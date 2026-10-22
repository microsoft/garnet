// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.common
{
    /// <summary>
    /// A one-shot rendezvous of a fixed number of participants whose waiters are parked rather than blocked.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Each participant calls <see cref="SignalAndWaitAsync"/> once. The arrival that completes the cohort returns an
    /// already-completed result and releases every parked participant; an arrival after that completes synchronously.
    /// </para>
    /// <para>
    /// A waiter's timeout releases that waiter alone, with a result of <c>false</c>, and its arrival still counts: the
    /// rendezvous reports that every participant arrived, not that every participant arrived in time. Cancelling the
    /// token faults that waiter with <see cref="OperationCanceledException"/>; its arrival counts as well.
    /// </para>
    /// <para>
    /// A parked waiter resumes on the thread pool, never inline on the thread that completed the cohort, fired the
    /// timeout or cancelled the token, so a caller that cancels while holding a lock does not run waiter code under it.
    /// </para>
    /// </remarks>
    public sealed class AsyncRendezvous
    {
        readonly int participantCount;
        readonly object gate = new();
        List<TaskCompletionSource<bool>> parked = [];
        int arrived;

        /// <summary>
        /// Creates a rendezvous for <paramref name="participantCount"/> participants.
        /// </summary>
        /// <param name="participantCount">Number of arrivals that complete the rendezvous. Must be at least 1.</param>
        public AsyncRendezvous(int participantCount)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(participantCount, 1);
            this.participantCount = participantCount;
        }

        /// <summary>
        /// Arrives at the rendezvous and waits for the rest of the cohort without blocking a thread.
        /// </summary>
        /// <param name="timeout">How long this participant waits for the rest. Either non-negative or <see cref="Timeout.InfiniteTimeSpan"/>.</param>
        /// <param name="cancellationToken">Token observed while waiting; cancelling it faults the returned task.</param>
        /// <returns>True when every participant arrived; false when this participant's timeout elapsed first.</returns>
        public ValueTask<bool> SignalAndWaitAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            TaskCompletionSource<bool> waiter = null;
            List<TaskCompletionSource<bool>> release = null;
            lock (gate)
            {
                if (++arrived < participantCount)
                {
                    waiter = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    parked.Add(waiter);
                }
                else
                {
                    release = parked;
                    parked = null;
                }
            }

            if (waiter != null)
                return new ValueTask<bool>(WaitAsync(waiter, timeout, cancellationToken));

            if (release != null)
            {
                foreach (var parkedWaiter in release)
                    _ = parkedWaiter.TrySetResult(true);
            }

            return new ValueTask<bool>(true);
        }

        static async Task<bool> WaitAsync(TaskCompletionSource<bool> waiter, TimeSpan timeout, CancellationToken cancellationToken)
        {
            using var timer = timeout == Timeout.InfiniteTimeSpan ? null :
                new Timer(static state => ((TaskCompletionSource<bool>)state).TrySetResult(false), waiter, timeout, Timeout.InfiniteTimeSpan);
            using var cancellation = cancellationToken.UnsafeRegister(static (state, token) => ((TaskCompletionSource<bool>)state).TrySetCanceled(token), waiter);
            return await waiter.Task.ConfigureAwait(false);
        }
    }
}