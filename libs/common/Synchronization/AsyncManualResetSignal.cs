// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.common
{
    /// <summary>
    /// Reusable, allocation-light async manual-reset signal that coalesces a single wake across every
    /// current waiter. A single <see cref="TaskCompletionSource{T}"/> backs the signal and is swapped
    /// on <see cref="Reset"/>; all waiters awaiting between two resets share one completion, so a
    /// single <see cref="Set"/> releases every parked waiter at once.
    ///
    /// The <see cref="Set"/> path is the producer hot path: it performs no allocation and takes no
    /// lock. Allocation happens only on <see cref="Reset"/>, which runs on the (slower) waiter re-arm
    /// path and is explicitly allowed to be slower.
    ///
    /// Multiple concurrent waiters are supported via the shared completion. A design that needs each
    /// waiter woken independently, or that must avoid allocation on the waiter path, should instead
    /// use a pooled single-waiter primitive (e.g. a reusable <c>IValueTaskSource</c>); this
    /// type deliberately trades that for simplicity and a zero-cost producer.
    /// </summary>
    public sealed class AsyncManualResetSignal
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
        /// timeout elapses, or the token is canceled. The bounded wait is a backstop that turns any
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