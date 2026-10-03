// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    // This structure uses a SemaphoreSlim as if it were a ManualResetEventSlim, because MRES does not support async waiting.
    internal struct CompletionEvent : IDisposable
    {
        /// <summary>
        /// Installed by <see cref="Dispose"/> in place of the live generation. It starts out fully signaled, so
        /// a wait issued after disposal falls straight through instead of blocking, and it is never replaced or
        /// released again.
        /// </summary>
        /// <remarks>
        /// Shared by every disposed event, which is safe because a wait on a disposed event only happens inside
        /// the teardown window before the waiter's own retry loop observes its owner's shutdown state and exits.
        /// Exhausting the permits would take <see cref="int.MaxValue"/> such waits.
        /// </remarks>
        private static readonly SemaphoreSlim Tombstone = new(int.MaxValue, int.MaxValue);

        private SemaphoreSlim semaphore;

        internal void Initialize() => semaphore = new SemaphoreSlim(0);

        internal void Set()
        {
            // If we have an existing semaphore, replace with a new one (to which any subequent waits will apply) and signal any waits on the existing one.
            SemaphoreSlim newSemaphore = null;
            while (true)
            {
                var tempSemaphore = semaphore;

                // Never replace the tombstone: that would un-signal a disposed event, leaving a later waiter to
                // block on a generation nothing will ever signal, and releasing the tombstone would throw
                // SemaphoreFullException because it is already at its maximum count.
                if (tempSemaphore is null || ReferenceEquals(tempSemaphore, Tombstone))
                    break;

                newSemaphore ??= new SemaphoreSlim(0);
                if (Interlocked.CompareExchange(ref semaphore, newSemaphore, tempSemaphore) == tempSemaphore)
                {
                    // Release all waiting threads. Only the thread that wins this CAS releases this instance, so
                    // it is released exactly once however many threads signal concurrently.
                    tempSemaphore.Release(int.MaxValue);
                    // tempSemaphore.Dispose();    TODO: We cannot Dispose() here because there may still be waiters that have not yet been released.
                    return;
                }
            }

            newSemaphore?.Dispose();
        }

        internal bool IsDefault() => semaphore is null;

        internal void Wait(CancellationToken token = default) => semaphore.Wait(token);

        internal bool Wait(TimeSpan timeout, CancellationToken token = default) => semaphore.Wait(timeout, token);

        internal Task WaitAsync(CancellationToken token = default) => semaphore.WaitAsync(token);

        internal Task WaitAsync(TimeSpan timeSpan, CancellationToken cancellationToken = default) => semaphore.WaitAsync(timeSpan, cancellationToken);

        /// <summary>
        /// Permanently signal the event: release everyone parked on the live generation, and install the
        /// <see cref="Tombstone"/> so that waits issued afterwards also fall straight through.
        /// </summary>
        /// <remarks>
        /// The retired semaphore is deliberately not disposed and the field is deliberately not cleared.
        /// Disposing it would make every later member throw <see cref="ObjectDisposedException"/>, including on
        /// a copy a caller is about to wait on from inside an operation with its epoch suspended; clearing the
        /// field would turn a wait on the live field into a <see cref="NullReferenceException"/>. Releasing
        /// instead lets every waiter, present and future, fall through so its own retry loop can observe its
        /// owner's shutdown state and terminate. The cost is one abandoned <see cref="SemaphoreSlim"/> per event
        /// at teardown: a managed object holding no kernel handle, because
        /// <see cref="SemaphoreSlim.AvailableWaitHandle"/> is never used.
        /// </remarks>
        public void Dispose()
        {
            while (true)
            {
                var tempSemaphore = semaphore;
                if (tempSemaphore is null || ReferenceEquals(tempSemaphore, Tombstone))
                    return;
                if (Interlocked.CompareExchange(ref semaphore, Tombstone, tempSemaphore) == tempSemaphore)
                {
                    tempSemaphore.Release(int.MaxValue);
                    return;
                }
            }
        }
    }
}