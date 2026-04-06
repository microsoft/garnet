// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Tracks the number of active workers within a critical region.
    /// On dispose, atomically prevents new workers from entering and efficiently blocks
    /// until all in-flight workers have exited, without spin-waiting.
    /// </summary>
    public sealed class ActiveWorkerMonitor
    {
        private int disposed = 0;

        /// <summary>
        /// Active worker count.
        /// </summary>
        int workerCount = 0;

        /// <summary>
        /// Signaled when the last worker exits after close.
        /// Starts unsignaled.
        /// </summary>
        readonly ManualResetEventSlim drainEvent = new(false);

        /// <summary>
        /// Gets the current active worker count (0 if closed).
        /// </summary>
        public int CurrentCount
        {
            get
            {
                var observedCount = workerCount;
                return observedCount < 0 ? 0 : observedCount;
            }
        }

        /// <summary>
        /// Closes the monitor and blocks until all active workers have exited.
        /// </summary>
        public void Dispose()
        {
            // Guard against dispose being called multiple times
            if (Interlocked.Exchange(ref disposed, 1) != 0)
            {
                return;
            }

            // Atomically flip the count negative; if no workers were active, skip wait
            TryClose();
            drainEvent.Dispose();
        }

        /// <summary>
        /// Attempts to open the resource if it is currently closed.
        /// </summary>
        /// <returns>true if the resource was successfully opened; otherwise, false.</returns>
        public bool TryOpen()
        {
            Debug.Assert(workerCount == int.MinValue);
            drainEvent.Reset();
            return Interlocked.CompareExchange(ref workerCount, 0, int.MinValue) == int.MinValue;
        }

        /// <summary>
        /// Attempts to close the resource gracefully, waiting for active workers to complete within the specified
        /// timeout period.
        /// </summary>
        /// <param name="timeout">The maximum time, in milliseconds, to wait for active workers to finish before closing. Specify -1 to wait
        /// indefinitely.</param>
        /// <param name="token">A cancellation token that can be used to cancel the close operation before the timeout elapses.</param>
        public void TryClose(int timeout = -1, CancellationToken token = default)
        {
            // Atomically flip the count negative; if no workers were active, skip wait
            if (Interlocked.Add(ref workerCount, int.MinValue) != int.MinValue)
                _ = drainEvent.Wait(timeout, token);
        }

        /// <summary>
        /// Attempts to register a new active worker.
        /// Returns <c>false</c> if the monitor has been closed.
        /// </summary>
        public bool TryEnter()
            => TryEnter(1, out _);

        /// <summary>
        /// Attempts to register a new active worker.
        /// Returns <c>false</c> if the monitor has been closed.
        /// When successful, <paramref name="count"/> contains the new worker count (always &gt; 0).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryEnter(int add, out int count)
        {
            var cnt = Interlocked.Add(ref workerCount, add);
            if (cnt < 0)
            {
                // Closed — undo the increment; if we happen to be the last, signal drain.
                if (Interlocked.Add(ref workerCount, -add) == int.MinValue)
                    drainEvent.Set();
                count = 0;
                return false;
            }

            count = cnt;
            return true;
        }

        /// <summary>
        /// Signals that a worker has finished. Returns the count after decrementing.
        /// If the monitor is closed and this was the last worker, signals the drain event.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Exit()
        {
            var cnt = Interlocked.Decrement(ref workerCount);

            // Signal drain when closed and all workers have exited
            if (cnt == int.MinValue)
                drainEvent.Set();

            return cnt;
        }
    }
}