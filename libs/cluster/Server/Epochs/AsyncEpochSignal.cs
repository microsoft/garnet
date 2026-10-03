// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.cluster
{
    /// <summary>
    /// Reusable, allocation-light async manual-reset signal used by the epoch wake barrier.
    /// A single <see cref="TaskCompletionSource{T}"/> is swapped on reset; all current waiters
    /// share one completion, so a single <see cref="Set"/> coalesces into one wake for every
    /// parked waiter.
    ///
    /// The <see cref="Set"/> path (the session-release waker hot path) performs no allocation and
    /// takes no lock. Allocation happens only on <see cref="Reset"/>, which runs on the slow
    /// bump-waiter path and is explicitly allowed to be slower.
    ///
    /// Multiple parked waiters are supported because epoch bumps may (rarely) overlap; see the
    /// concurrency note on <see cref="GarnetEpoch{TObserverSource}.BumpAndWaitForEpochTransitionWakeAsync"/> for when
    /// this shared-completion coalescing should be revisited.
    /// </summary>
    internal sealed class AsyncEpochSignal
    {
        TaskCompletionSource<bool> tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        /// Releases any parked waiter(s). Allocation-free and lock-free; a no-op when already set.
        /// </summary>
        public void Set() => Volatile.Read(ref tcs).TrySetResult(true);

        /// <summary>
        /// Rearms the signal so the next <see cref="WaitAsync"/> blocks until the following
        /// <see cref="Set"/>. Only swaps when the current source is already completed.
        /// </summary>
        public void Reset()
        {
            while (true)
            {
                var current = Volatile.Read(ref tcs);
                if (!current.Task.IsCompleted)
                    return;

                var next = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                if (Interlocked.CompareExchange(ref tcs, next, current) == current)
                    return;
            }
        }

        /// <summary>
        /// Waits for the signal, bounded by <paramref name="timeout"/>. Returns when signaled, the
        /// timeout elapses, or the token is canceled. The bounded wait is the backstop that turns any
        /// theoretically missed wake into a short re-scan rather than a hang.
        /// </summary>
        public async Task WaitAsync(TimeSpan timeout, CancellationToken token)
        {
            var waitTask = Volatile.Read(ref tcs).Task;
            if (waitTask.IsCompleted)
                return;

            using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(token);
            var delayTask = Task.Delay(timeout, delayCts.Token);
            var completed = await Task.WhenAny(waitTask, delayTask).ConfigureAwait(false);

            if (completed == waitTask)
            {
                await delayCts.CancelAsync().ConfigureAwait(false);
                try { await delayTask.ConfigureAwait(false); } catch (OperationCanceledException) { }
            }
        }
    }
}