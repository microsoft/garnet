// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.server
{
    /// <summary>
    /// A reusable timed wait for a suspending command body.
    /// </summary>
    /// <remarks>
    /// A single <see cref="Timer"/> is created per session that ever waits and rearmed thereafter, rather
    /// than the timer and promise <see cref="Task.Delay(TimeSpan)"/> allocates per call.
    /// </remarks>
    internal sealed class SessionDelay : SessionWaitSource
    {
        Timer timer;

        /// <summary>
        /// Starts a wait of the given length.
        /// </summary>
        /// <param name="delay">How long to wait. Must be positive.</param>
        /// <returns>A task that completes when the delay elapses, or faults if the session is torn down first.</returns>
        internal ValueTask WaitAsync(TimeSpan delay)
        {
            if (Cancelled)
                return ValueTask.FromException(new OperationCanceledException());

            Arm();

            // Created on first use so a session that never parks does not pay for a timer, and reused
            // afterwards because rearming an existing timer does not allocate.
            timer ??= new Timer(static self => _ = ((SessionDelay)self).Complete(canceled: false), this, Timeout.Infinite, Timeout.Infinite);
            timer.Change(delay, Timeout.InfiniteTimeSpan);

            // Cancel may have run between the check above and the arm, in which case nothing else will
            // complete this wait.
            if (Cancelled)
                _ = Complete(canceled: true);

            return Pending;
        }

        /// <inheritdoc/>
        protected override void OnCancelling() => timer?.Change(Timeout.Infinite, Timeout.Infinite);
    }
}