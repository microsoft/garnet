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
    public sealed class LeaderFollowerBarrier
    {
        readonly int participantCount;
        readonly SemaphoreSlim workReady = new(0);
        readonly SemaphoreSlim workCompleted = new(0);
        readonly SemaphoreSlim resetReady = new(0);

        /// <summary>
        /// Initializes a new instance of the <see cref="LeaderFollowerBarrier"/> class.
        /// </summary>
        /// <param name="participantCount">Number of participant tasks that will process work.</param>
        public LeaderFollowerBarrier(int participantCount)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(participantCount, 1);
            this.participantCount = participantCount;
        }

        static TimeSpan ProcessTimeSpan(TimeSpan timeout)
            => timeout == default ? Timeout.InfiniteTimeSpan : timeout;

        /// <summary>
        /// Leader: Waits for all participants to complete, then resets for next cycle.
        /// </summary>
        public bool WaitCompleted(TimeSpan timeout = default, CancellationToken cancellationToken = default)
        {
            var waitTimeout = ProcessTimeSpan(timeout);
            for (var i = 0; i < participantCount; i++)
            {
                if (!workCompleted.Wait(waitTimeout, cancellationToken))
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Leader: Release participants that are waiting inside <see cref="SignalCompleted"/>
        /// so they can proceed to the next cycle.
        /// </summary>
        public void Release() => resetReady.Release(participantCount);

        /// <summary>
        /// Participant: Waits for work signal from leader.
        /// </summary>
        public async Task WaitReadyWorkAsync(CancellationToken cancellationToken = default)
        {
            await workReady.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Leader: Signals all participants that work is ready.
        /// </summary>
        public void SignalWorkReady()
        {
            workReady.Release(participantCount);
        }

        /// <summary>
        /// Participant: Signals completion and waits for leader to reset.
        /// </summary>
        public void SignalCompleted(CancellationToken cancellationToken = default)
        {
            workCompleted.Release();
            resetReady.Wait(cancellationToken);
        }
    }
}