// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;

namespace Garnet.server.Lua
{
    /// <summary>
    /// Central place to manage timeouts being injected into running Lua scripts.
    /// 
    /// We use this because each <see cref="LuaRunner"/> starting it's own timer
    /// or similar has substantial overhead.
    /// </summary>
    internal class LuaTimeoutManager : IDisposable
    {
        private readonly TimeSpan timeout;
        private readonly TimeSpan frequency;
        private readonly long timeoutTimestampTicks;

        // Shared timer for all timeouts
        private Timer timer;

        // Head of the intrusive linked list of active runners
        private LuaRunner activeRunners;

        // Provides unique values for timer registrations
        private long globalRegistrationCookie;

        private int processingTimeouts;

        internal LuaTimeoutManager(TimeSpan timeout)
        {
            if (timeout <= TimeSpan.Zero)
            {
                throw new ArgumentException($"Timeout must be >= 0, was {timeout}");
            }

            this.timeout = timeout;
            timeoutTimestampTicks = (long)(Stopwatch.Frequency * this.timeout.TotalSeconds);
            frequency = TimeSpan.FromTicks(timeout.Ticks / 10);
            if (frequency == TimeSpan.Zero)
            {
                frequency = timeout;
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        => timer?.Dispose();

        /// <summary>
        /// Start this <see cref="LuaTimeoutManager"/>.
        /// </summary>
        internal void Start()
        {
            timer = new Timer(OnFrequency, null, TimeSpan.Zero, frequency);
        }

        /// <summary>
        /// Register the given <see cref="LuaRunner"/> for a timeout notification.
        /// 
        /// A runner can only be registered a single time.
        /// 
        /// No two threads should be trying to register the same <see cref="LuaRunner"/>
        /// at the same time.
        /// </summary>
        internal long RegisterForTimeout(LuaRunner runner)
        {
            var now = Stopwatch.GetTimestamp();
            var timeoutAt = now + timeoutTimestampTicks;

            var curHead = Volatile.Read(ref activeRunners);

            var cookie = Interlocked.Increment(ref globalRegistrationCookie);

            // Special value, so we know to skip the runner if we read into it
            _ = Interlocked.Exchange(ref runner.currentTimeoutTicks, -1);
            _ = Interlocked.Exchange(ref runner.currentTimeoutToken, -1);

        tryAgain:
            _ = Interlocked.Exchange(ref runner.nextRunnerTimeoutChain, curHead);

            LuaRunner newCurHead;
            if ((newCurHead = Interlocked.CompareExchange(ref activeRunners, runner, curHead)) != curHead)
            {
                curHead = newCurHead;
                goto tryAgain;
            }

            var cookieRes = Interlocked.Exchange(ref runner.currentTimeoutToken, cookie);
            Debug.Assert(cookieRes == -1, "Nothing else should have updated cookie value");

            var timeoutRes = Interlocked.Exchange(ref runner.currentTimeoutTicks, timeoutAt);
            Debug.Assert(timeoutRes == -1, "Nothing else should have updated timeout value");

            return cookie;
        }

        /// <summary>
        /// Invoked approximately every <see cref="frequency"/>.
        /// 
        /// Checks all registered runners and informs them of timeouts if needed.
        /// </summary>
        private void OnFrequency(object state)
        {
            // Timer will keep firing even if this callback hasn't finished, so force an at-most-once behavior
            if (Interlocked.CompareExchange(ref processingTimeouts, 1, 0) != 0)
            {
                return;
            }

            try
            {
                var now = Stopwatch.GetTimestamp();

                var head = Volatile.Read(ref activeRunners);

            restart:
                LuaRunner prev = null;
                var cur = head;
                while (cur != null)
                {
                    var timeout = Interlocked.CompareExchange(ref cur.currentTimeoutTicks, 0, 0);
                    var cookie = Interlocked.CompareExchange(ref cur.currentTimeoutToken, 0, 0);

                    // Check to see if the timeout has occurred
                    if (timeout != -1 && cookie != -1 && timeout < now)
                    {
                        // Remove the runner first...

                        if (prev == null)
                        {
                            // Head of list

                            LuaRunner newHead;
                            if ((newHead = Interlocked.CompareExchange(ref activeRunners, cur.nextRunnerTimeoutChain, head)) != head)
                            {
                                // If contended, just start over - we will eventually succeed
                                head = newHead;
                                goto restart;
                            }
                        }
                        else
                        {
                            // Interior to list, shouldn't fail as no other thread should modify these

                            var exchangeRes = Interlocked.CompareExchange(ref prev.nextRunnerTimeoutChain, cur.nextRunnerTimeoutChain, cur);
                            Debug.Assert(exchangeRes == cur, "Should never fail, exchanges should order visibility of runners in chain");
                        }

                        // Actually request the timeout
                        cur.RequestTimeout(cookie);

                        // Do not update prev, it stays the same
                        cur = cur.nextRunnerTimeoutChain;
                    }
                    else
                    {
                        // No timeout, move on to next item

                        prev = cur;
                        cur = cur.nextRunnerTimeoutChain;
                    }
                }
            }
            finally
            {
                var unlockRes = Interlocked.Exchange(ref processingTimeouts, 0);
                Debug.Assert(unlockRes == 1, "No other thread should have modified this");
            }
        }
    }
}
