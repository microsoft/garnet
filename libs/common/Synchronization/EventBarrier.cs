// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Implementation of a barrier like primitive involving N participants for which N-1 will enter
    /// and wait for a signal from the N-th participant (i.e. leader)
    /// </summary>
    /// <param name="participantCount"></param>
    public class EventBarrier(int participantCount)
    {
        int participantCount = participantCount;
        int arrivedCount = participantCount;
        readonly ManualResetEventSlim eventSlim = new(false);

        readonly ManualResetEventSlim releaseFirst = new(false);
        readonly ManualResetEventSlim releaseAll = new(false);

        /// <summary>
        /// Decrements participant count but does not set signal.
        /// </summary>
        /// <param name="timeout"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public bool SignalAndWait(TimeSpan timeout = default, CancellationToken cancellationToken = default)
        {
            var newValue = Interlocked.Decrement(ref arrivedCount);
            if (newValue > 0)
            {
                if (!Wait(timeout, cancellationToken))
                    throw new TimeoutException();
                return false;
            }
            else if (newValue == 0)
                return true;
            else
                throw new Exception("Invalid count value < 0");
        }


        /// <summary>
        /// Try wait for all participants to join
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="timeout"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>True if it is the first participant that joined the group otherwise false</returns>
        public bool TrySignalAndWait(out Exception exception, TimeSpan timeout = default, CancellationToken cancellationToken = default)
        {
            exception = null;
            var newValue = Interlocked.Decrement(ref arrivedCount);
            var isFirst = false;

            try
            {
                if (newValue == participantCount - 1)
                {
                    isFirst = true;
                    // Wait only if there is at least one more participant
                    if (newValue > 0)
                        _ = releaseFirst.Wait(timeout, cancellationToken);
                }
                else if (newValue > 0)
                {
                    // Wait for first participant to release me or timeout/cancellation
                    _ = releaseAll.Wait(timeout, cancellationToken);
                    return false;
                }
                else if (newValue == 0)
                {
                    // Release first participant to perform the operation
                    releaseFirst.Set();
                    // Wait for first participant to release me or timeout/cancellation
                    _ = releaseAll.Wait(timeout, cancellationToken);
                    return false;
                }
                else
                {
                    throw new Exception("Invalid count value < 0");
                }
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            return isFirst;
        }

        /// <summary>
        /// Release all waiting participants
        /// </summary>
        public void Release() => releaseAll.Set();

        /// <summary>
        /// Set underlying event.
        /// </summary>
        public void Set() => eventSlim.Set();

        /// <summary>
        /// Wait for signal to be set.
        /// </summary>
        /// <param name="timeout"></param>
        /// <param name="cancellationToken"></param>
        bool Wait(TimeSpan timeout = default, CancellationToken cancellationToken = default)
            => eventSlim.Wait(timeout == default ? Timeout.InfiniteTimeSpan : timeout, cancellationToken);
    }
}