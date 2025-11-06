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
        int count = participantCount;
        readonly ManualResetEventSlim eventSlim = new(false);

        /// <summary>
        /// Decrements participant count but does not set signal.
        /// </summary>
        /// <param name="timeout"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public bool SignalAndWait(TimeSpan timeout = default, CancellationToken cancellationToken = default)
        {
            var newValue = Interlocked.Decrement(ref count);
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