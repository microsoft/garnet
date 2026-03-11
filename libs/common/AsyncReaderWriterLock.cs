// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.common
{
    /// <summary>
    /// Provides an asynchronous reader-writer lock that enables multiple concurrent readers or exclusive access for a
    /// single writer.
    /// </summary>
    public class AsyncReaderWriterLock : IDisposable
    {
        enum LockOperation
        {
            Reader,
            Writer
        }

        Queue<TaskCompletionSource<bool>> readerQueue;
        Queue<TaskCompletionSource<bool>> writerQueue;
        TaskCompletionSource<bool> tcs;
        int lockRequests;
        bool useFIFO;
        int maxReaderReleaseCount;
#if NET9_0_OR_GREATER
        readonly Lock mutex;
#else
        readonly object mutex;
#endif
        bool disposed = false;

        /// <summary>
        /// AsyncReaderWriterLock constructor
        /// </summary>
        /// <param name="useFIFO"></param>
        /// <param name="maxReaderReleaseCount"></param>
        public AsyncReaderWriterLock(bool useFIFO = false, int maxReaderReleaseCount = 10)
        {
            this.useFIFO = useFIFO;
            this.maxReaderReleaseCount = maxReaderReleaseCount;
            readerQueue = useFIFO ? new() : null;
            writerQueue = useFIFO ? new() : null;
            tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            mutex = new();
        }

        /// <summary>
        /// Releases all resources used by the current instance and cancels any pending operations.
        /// </summary>
        public void Dispose()
        {
            TaskCompletionSource<bool> release;
            lock (mutex)
            {
                if (disposed)
                    return;
                disposed = true;
                release = tcs;
                tcs = null;
            }
            release.SetCanceled();
        }

        /// <summary>
        /// Acquires a read lock asynchronously, allowing concurrent read access to the resource.
        /// </summary>
        /// <returns>A task that represents the asynchronous operation of acquiring the read lock. The task completes when the
        /// lock is granted.</returns>
        public async Task ReadLockAsync()
            => await ReadLockAsync(default);

        /// <summary>
        /// Acquires a read lock asynchronously, allowing concurrent read access to the resource.
        /// </summary>
        /// <param name="token">The cancellation token used to signal the cancellation of the operation.</param>
        /// <returns>A task that represents the asynchronous operation of acquiring the read lock. The task completes when the
        /// lock is granted.</returns>
        public async Task ReadLockAsync(CancellationToken token)
        {
            if (!useFIFO)
                await Acquire(LockOperation.Reader, token);
            else
                await AcquireFIFO(LockOperation.Reader, token);
        }

        /// <summary>
        /// Releases a read lock on the resource, allowing other operations to proceed.
        /// </summary>
        public void ReadUnlock()
        {
            if (!useFIFO)
                Release(LockOperation.Reader);
            else
                ReleaseFIFO(LockOperation.Reader);
        }

        /// <summary>
        /// Acquires an exclusive write lock on the resource asynchronously.
        /// </summary>
        /// <returns>A task that represents the asynchronous operation of acquiring the write lock. The task completes when the
        /// lock is granted.</returns>
        public async Task WriteLockAsync()
            => await WriteLockAsync(default);

        /// <summary>
        /// Acquires an exclusive write lock asynchronously, granting sole access to the protected resource.
        /// </summary>
        /// <param name="token">A cancellation token that can be used to cancel the operation before the write lock is acquired.</param>
        /// <returns>A task that represents the asynchronous operation of acquiring the write lock. The task completes when the
        /// lock is granted.</returns>
        public async Task WriteLockAsync(CancellationToken token)
        {
            if (!useFIFO)
                await Acquire(LockOperation.Writer, token);
            else
                await AcquireFIFO(LockOperation.Writer, token);
        }

        /// <summary>
        /// Releases the write lock held by the current instance, allowing other threads to acquire the lock.
        /// </summary>
        public void WriteUnlock()
        {
            if (!useFIFO)
                Release(LockOperation.Writer);
            else
                ReleaseFIFO(LockOperation.Writer);
        }

        private async Task Acquire(LockOperation operation, CancellationToken token)
        {
            Task task = default;
            while (true)
            {
                lock (mutex)
                {
                    ObjectDisposedException.ThrowIf(disposed, this);

                    // Lock immediately if lock is not taken
                    switch (operation)
                    {
                        case LockOperation.Reader:
                            // Reader logic
                            if (lockRequests >= 0)
                            {
                                lockRequests++;
                                return;
                            }
                            break;
                        case LockOperation.Writer:
                            // Writer logic
                            if (lockRequests == 0)
                            {
                                lockRequests = -1;
                                return;
                            }
                            break;
                        default:
                            throw new InvalidOperationException();
                    }

                    task = tcs.Task;
                }
                await task.WaitAsync(token).ConfigureAwait(false);
            }
        }

        private void Release(LockOperation operation)
        {
            TaskCompletionSource<bool> _cts;
            var release = false;
            lock (mutex)
            {
                ObjectDisposedException.ThrowIf(disposed, this);
                // Release lock
                switch (operation)
                {
                    case LockOperation.Reader:
                        lockRequests--;
                        break;
                    case LockOperation.Writer:
                        lockRequests = 0;
                        break;
                    default:
                        throw new InvalidOperationException();
                }

                release = lockRequests == 0;

                // Acquire previous tcs for release
                _cts = tcs;
                tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            if(release)
                _ = _cts.TrySetResult(true);
        }

        private async Task AcquireFIFO(LockOperation operation, CancellationToken token)
        {
            TaskCompletionSource<bool> completion;
            lock (mutex)
            {
                ObjectDisposedException.ThrowIf(disposed, this);

                // Lock immediately if lock is not taken.
                switch (operation)
                {
                    case LockOperation.Reader:
                        // Block new readers when a writer is already waiting.
                        if (lockRequests >= 0 && writerQueue.Count == 0)
                        {
                            lockRequests++;
                            return;
                        }

                        completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                        readerQueue.Enqueue(completion);
                        break;
                    case LockOperation.Writer:
                        if (lockRequests == 0)
                        {
                            lockRequests = -1;
                            return;
                        }

                        completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                        writerQueue.Enqueue(completion);
                        break;
                    default:
                        throw new InvalidOperationException();
                }
            }

            await WaitForGrantAsync(completion, operation, token).ConfigureAwait(false);
        }

        private void ReleaseFIFO(LockOperation operation)
        {
            lock (mutex)
            {
                ObjectDisposedException.ThrowIf(disposed, this);

                // Release lock.
                switch (operation)
                {
                    case LockOperation.Reader:
                        if (lockRequests <= 0)
                            throw new SynchronizationLockException("Read lock release attempted when no read lock is held.");
                        lockRequests--;
                        break;
                    case LockOperation.Writer:
                        if (lockRequests != -1)
                            throw new SynchronizationLockException("Write lock release attempted when no write lock is held.");
                        lockRequests = 0;
                        break;
                    default:
                        throw new InvalidOperationException();
                }

                if (lockRequests != 0)
                    return;

                // Prefer a waiting writer before releasing readers.
                while (writerQueue.Count > 0)
                {
                    var writerCompletion = writerQueue.Dequeue();
                    if (writerCompletion.Task.IsCompleted)
                        continue;

                    if (writerCompletion.TrySetResult(true))
                    {
                        lockRequests = -1;
                        return;
                    }
                }

                var maxReleaseCount = maxReaderReleaseCount <= 0 ? int.MaxValue : maxReaderReleaseCount;
                while (maxReleaseCount-- > 0 && readerQueue.Count > 0)
                {
                    var readerCompletion = readerQueue.Dequeue();
                    if (readerCompletion.Task.IsCompleted)
                        continue;

                    if (readerCompletion.TrySetResult(true))
                        lockRequests++;
                }
            }
        }

        private async Task WaitForGrantAsync(TaskCompletionSource<bool> completion, LockOperation operation, CancellationToken token)
        {
            using var registration = token.CanBeCanceled
                ? token.Register(() => CancelWaiter(completion, operation, token))
                : default;

            _ = await completion.Task.ConfigureAwait(false);
        }

        private void CancelWaiter(TaskCompletionSource<bool> completion, LockOperation operation, CancellationToken token)
        {
            var release = false;
            lock (mutex)
            {
                if (!completion.Task.IsCompleted)
                {
                    _ = completion.TrySetCanceled(token);
                    return;
                }

                release = completion.Task.Status == TaskStatus.RanToCompletion;
            }

            if (release)
                ReleaseFIFO(operation);
        }
    }
}