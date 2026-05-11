// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Reader/writer lock backed by semaphores.
    /// Supports a single writer or multiple concurrent readers.
    /// </summary>
    public sealed class ReaderWriterLock : IDisposable
    {
        enum LockOperation
        {
            Reader,
            Writer
        }

#if NET9_0_OR_GREATER
        readonly Lock mutex;
#else
        readonly object mutex;
#endif
        readonly SemaphoreSlim readerSemaphore;
        readonly SemaphoreSlim writerSemaphore;

        // -1: writer holds the lock, 0: free, >0: number of active readers.
        int lockHeld;
        int waitingReaders;
        int waitingWriters;
        int grantedReaders;
        int grantedWriters;
        bool disposed;

        public ReaderWriterLock()
        {
            mutex = new();
            readerSemaphore = new(0);
            writerSemaphore = new(0);
        }

        public void Dispose()
        {
            lock (mutex)
            {
                if (disposed)
                    return;
                disposed = true;
            }

            readerSemaphore.Dispose();
            writerSemaphore.Dispose();
        }

        /// <summary>
        /// Acquires an exclusive write lock, granting sole access to the resource for write operations.
        /// </summary>
        public void WriteLock()
            => WriteLock(default);

        /// <summary>
        /// Acquires an exclusive write lock, granting sole access to the resource for write operations.
        /// </summary>
        /// <param name="token">A cancellation token that can be used to cancel the lock acquisition process.</param>
        public void WriteLock(CancellationToken token)
            => Acquire(LockOperation.Writer, token);

        /// <summary>
        /// Release writer lock and wake either one writer or all waiting readers.
        /// </summary>
        public void WriteUnlock()
            => Release();

        /// <summary>
        /// Acquires a reader lock, allowing concurrent read access to the resource.
        /// </summary>
        public void ReadLock()
            => ReadLock(default);

        /// <summary>
        /// Acquires a reader lock, allowing concurrent read access to the resource.
        /// </summary>
        /// <param name="token">The cancellation token used to signal the operation's cancellation.</param>
        public void ReadLock(CancellationToken token)
            => Acquire(LockOperation.Reader, token);

        /// <summary>
        /// Acquires a reader lock, allowing concurrent read access to the resource.
        /// </summary>
        /// <param name="token">The cancellation token used to signal the operation's cancellation.</param>
        [Obsolete("Use ReadLock(CancellationToken) instead.")]
        public void ReaderLock(CancellationToken token)
            => ReadLock(token);

        /// <summary>
        /// Release reader lock and wake one writer when this was the last active reader.
        /// </summary>
        public void ReadUnlock()
            => Release();

        void Acquire(LockOperation operation, CancellationToken cancellationToken)
        {
            SemaphoreSlim waitSemaphore;
            lock (mutex)
            {
                ObjectDisposedException.ThrowIf(disposed, nameof(ReaderWriterLock));

                switch (operation)
                {
                    case LockOperation.Reader:
                        // Acquire reader lock immediately
                        if (lockHeld >= 0 && waitingWriters == 0)
                        {
                            lockHeld++;
                            return;
                        }

                        // Prepare to wait because lock was not available for readers
                        waitingReaders++;
                        waitSemaphore = readerSemaphore;
                        break;
                    case LockOperation.Writer:
                        // Acquire writer lock immediately
                        if (lockHeld == 0)
                        {
                            lockHeld = -1;
                            return;
                        }

                        // Prepare to wait because lock was not available for writers
                        waitingWriters++;
                        waitSemaphore = writerSemaphore;
                        break;
                    default:
                        throw new InvalidOperationException();
                }
            }

            WaitForGrant(waitSemaphore, operation, cancellationToken);
        }

        void WaitForGrant(SemaphoreSlim waitSemaphore, LockOperation operation, CancellationToken cancellationToken)
        {
            try
            {
                waitSemaphore.Wait(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                var shouldRelease = false;
                lock (mutex)
                {
                    ObjectDisposedException.ThrowIf(disposed, nameof(ReaderWriterLock));

                    switch (operation)
                    {
                        case LockOperation.Reader:
                            if (grantedReaders > 0)
                            {
                                // lock was granted before cancellation
                                grantedReaders--;
                                shouldRelease = true;
                            }
                            else
                            {
                                // lock was not granted yet
                                waitingReaders--;
                            }
                            break;
                        case LockOperation.Writer:
                            if (grantedWriters > 0)
                            {
                                // lock was granted before cancellation
                                grantedWriters--;
                                shouldRelease = true;
                            }
                            else
                            {
                                // lock was not granted yet
                                waitingWriters--;
                            }
                            break;
                        default:
                            throw new InvalidOperationException();
                    }
                }

                if (shouldRelease)
                    Release();

                throw;
            }

            lock (mutex)
            {
                ObjectDisposedException.ThrowIf(disposed, nameof(ReaderWriterLock));

                switch (operation)
                {
                    case LockOperation.Reader:
                        if (grantedReaders <= 0)
                            throw new SynchronizationLockException("Reader wakeup observed without matching grant.");
                        // Release granted counters and not waiting counters since they have been decremented at release that granted the lock.
                        grantedReaders--;
                        break;
                    case LockOperation.Writer:
                        if (grantedWriters <= 0)
                            throw new SynchronizationLockException("Writer wakeup observed without matching grant.");
                        // Release granted counters and not waiting counters since they have been decremented at release that granted the lock.
                        grantedWriters--;
                        break;
                    default:
                        throw new InvalidOperationException();
                }
            }
        }

        void Release()
        {
            lock (mutex)
            {
                ObjectDisposedException.ThrowIf(disposed, nameof(ReaderWriterLock));

                if (lockHeld == 0)
                    throw new SynchronizationLockException("Unlock called when lock is not held.");

                // Update lock state first.
                if (lockHeld == -1)
                    lockHeld = 0;
                else
                    lockHeld--;

                // Still held by active readers.
                if (lockHeld != 0)
                    return;

                // Writers have priority.
                if (waitingWriters > 0)
                {
                    waitingWriters--;
                    lockHeld = -1;
                    grantedWriters++;
                    _ = writerSemaphore.Release();
                    return;
                }

                // Consume readers next
                if (waitingReaders > 0)
                {
                    var toRelease = waitingReaders;
                    waitingReaders = 0;
                    lockHeld = toRelease;
                    grantedReaders += toRelease;
                    _ = readerSemaphore.Release(toRelease);
                }
            }
        }
    }
}