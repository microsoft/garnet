// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.common
{
    /// <summary>
    /// Synchronization primitive for coordinating a leader task with multiple participant tasks in a cyclic pattern.
    /// The leader signals work readiness, waits for all participants to complete, then the cycle repeats.
    /// Participants wait for work signal, process, signal completion, then wait for reset before next cycle.
    /// </summary>
    /// <param name="participantCount">Number of participant tasks that will process work.</param>
    public sealed class LeaderFollowerBarrier(int participantCount)
    {
        readonly int participantCount = participantCount;
        readonly SemaphoreSlim workReady = new(0);
        readonly SemaphoreSlim workCompleted = new(0);
        readonly SemaphoreSlim resetReady = new(0);

        /// <summary>
        /// Leader: Signals all participants that work is ready.
        /// </summary>
        public void SignalWorkReady()
        {
            workReady.Release(participantCount);
        }

        /// <summary>
        /// Leader: Waits for all participants to complete, then resets for next cycle.
        /// </summary>
        public bool WaitCompleted(TimeSpan timeout = default, CancellationToken cancellationToken = default)
            => WaitCompletedAsync(timeout == default ? Timeout.InfiniteTimeSpan : timeout, cancellationToken).GetAwaiter().GetResult();

        /// <summary>
        /// Leader: Waits for all participants to complete, then resets for next cycle.
        /// </summary>
        public async Task<bool> WaitCompletedAsync(TimeSpan timeout = default, CancellationToken cancellationToken = default)
        {
            var waitTimeout = timeout == default ? Timeout.InfiniteTimeSpan : timeout;
            
            // Wait for all participants to signal completion
            for (var i = 0; i < participantCount; i++)
            {
                if (!await workCompleted.WaitAsync(waitTimeout, cancellationToken))
                    return false;
            }

            // Reset the barrier by signaling all participants to proceed to next wait
            resetReady.Release(participantCount);
            return true;
        }

        /// <summary>
        /// Participant: Waits for work signal from leader.
        /// </summary>
        public void WaitReadyWork(CancellationToken cancellationToken = default)
            => WaitReadyWorkAsync(cancellationToken).GetAwaiter().GetResult();

        /// <summary>
        /// Participant: Waits for work signal from leader.
        /// </summary>
        public async Task WaitReadyWorkAsync(CancellationToken cancellationToken = default)
        {
            await workReady.WaitAsync(cancellationToken);
        }

        /// <summary>
        /// Participant: Signals completion and waits for leader to reset.
        /// </summary>
        public void SignalCompleted(CancellationToken cancellationToken = default)
            => SignalCompletedAsync(cancellationToken).GetAwaiter().GetResult();

        /// <summary>
        /// Participant: Signals completion and waits for leader to reset.
        /// </summary>
        public async Task SignalCompletedAsync(CancellationToken cancellationToken = default)
        {
            workCompleted.Release();
            await resetReady.WaitAsync(cancellationToken);
        }
    }


}
