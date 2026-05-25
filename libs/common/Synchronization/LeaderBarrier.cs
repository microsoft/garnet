// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Synchronizes a group of participants, allowing one to act as the leader while others wait until released.
    /// </summary>
    /// <param name="participantCount"></param>
    public class LeaderBarrier(int participantCount)
    {
        readonly int participantCount = participantCount;
        int arrivedCount = participantCount;

        ManualResetEventSlim releaseFirst = new(false);
        ManualResetEventSlim releaseAll = new(false);

        /// <summary>
        /// Attempts to signal arrival and wait for other participants within the specified timeout and cancellation
        /// token.
        /// </summary>
        /// <param name="exception">When this method returns, contains the exception that occurred during the operation, or null if no exception
        /// was thrown.</param>
        /// <param name="timeout">The maximum time to wait for other participants. The default value is infinite.</param>
        /// <param name="cancellationToken">A cancellation token to observe while waiting.</param>
        /// <returns>true if the caller is the first participant to arrive; otherwise, false.</returns>
        public bool TrySignalOrWait(out Exception exception, TimeSpan timeout = default, CancellationToken cancellationToken = default)
        {
            exception = null;
            var newValue = Interlocked.Decrement(ref arrivedCount);

            try
            {
                // First participant to arrive
                if (newValue == participantCount - 1)
                {
                    // Wait only if there are more participants to arrive
                    if (newValue > 0)
                        _ = releaseFirst.Wait(timeout, cancellationToken);
                    return true;
                }

                // Last participant to arrive - release the first
                if (newValue == 0)
                    releaseFirst.Set();

                // All non-first participants wait for release
                if (newValue >= 0)
                {
                    _ = releaseAll.Wait(timeout, cancellationToken);
                    return false;
                }

                // Invalid state
                throw new Exception("Invalid count value < 0");
            }
            catch (Exception ex)
            {
                exception = ex;
                return false;
            }
        }

        /// <summary>
        /// Release all waiting participants
        /// </summary>
        public void Release() => releaseAll.Set();
    }
}