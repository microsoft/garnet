// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Tracks the number of active workers within a critical region.
    /// On dispose, atomically prevents new workers from entering and efficiently blocks
    /// until all in-flight workers have exited, without spin-waiting.
    /// </summary>
    public sealed class WorkerMonitor
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
            if (Interlocked.Add(ref workerCount, int.MinValue) != int.MinValue)
                drainEvent.Wait();
            drainEvent.Dispose();
        }

        /// <summary>
        /// Attempts to register a new active worker.
        /// Returns <c>false</c> if the monitor has been closed.
        /// When successful, <paramref name="count"/> contains the new worker count (always &gt; 0).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryEnter(out int count)
        {
            var cnt = Interlocked.Increment(ref workerCount);
            if (cnt < 0)
            {
                // Closed — undo the increment; if we happen to be the last, signal drain.
                if (Interlocked.Decrement(ref workerCount) == int.MinValue)
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