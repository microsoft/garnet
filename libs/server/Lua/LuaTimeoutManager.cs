// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Garnet.server.Lua
{
    /// <summary>
    /// Central place to manage timeouts being injected into running Lua scripts.
    /// 
    /// We use this because each <see cref="LuaRunner"/> starting it's own timer
    /// or similar has substantial overhead.
    /// 
    /// Timeouts are explicitly best effort, there's no guarantee we'll time something out _exactly_
    /// when it runs too long.  We will time it out _eventually_ once it has run too long.
    /// </summary>
    /// <remarks>
    /// This is complex functionality.
    /// 
    /// Complications are:
    ///  1. Timeouts are _rare_, so significant overhead must be avoided
    ///     - We cannot afford to allocate timers/tasks/etc. per-invocation due to this
    ///  2. Scripts can be active on many threads
    ///     - However, only 1 script will be active PER thread
    ///  3. Scripts can complete after we've decided to "time them out", so there's a natural race
    ///  
    /// The rough design is:
    ///  - <see cref="SessionScriptCache"/>s are registered with the <see cref="LuaTimeoutManager"/>
    ///  - <see cref="SessionScriptCache"/>s now track the active <see cref="LuaRunner"/> (if any)
    ///  - When a script starts, we get a unique token so we can distinguish the natural race
    ///  - A dedicate thread is ticking periodically, walking the <see cref="SessionScriptCache"/>s and triggering timeouts
    /// </remarks>
    internal sealed class LuaTimeoutManager : IDisposable
    {
        internal sealed class Registration : IDisposable
        {
            private readonly LuaTimeoutManager owner;

            // Bottom half is cookie, top half is count
            private ulong packedState;

            internal SessionScriptCache ScriptCache { get; }
            internal uint Cookie => (uint)(packedState >> 32);
            internal uint Count => (uint)packedState;

            internal Registration(LuaTimeoutManager owner, SessionScriptCache cache)
            {
                this.owner = owner;
                ScriptCache = cache;
            }

            /// <summary>
            /// Update value of <see cref="Cookie"/> atomically.
            /// </summary>
            internal void SetCookie(uint cookie)
            {
                _ = Interlocked.Exchange(ref packedState, cookie);
            }

            /// <summary>
            /// Advance count on current registration.
            /// 
            /// Returns true if should be cancelled.
            /// 
            /// If true, <paramref name="cookie"/> will be set to the value that identifies
            /// this registration.
            /// </summary>
            internal bool AdvanceTimeout(out uint cookie)
            {
                // Advances the top half of packedState by 1
                const ulong IncrBy = 1UL << 32;

                // +1 here because at the point of registration, the current tick is some % of the way
                // complete.  So we need to wait an additional one to make sure we don't cancel early.
                const ulong TriggerAfter = (ulong)(TimeoutDivisions + 1) << 32;

                var oldValue = Volatile.Read(ref packedState);
                if ((uint)oldValue == 0)
                {
                    // Current registration isn't active (cookie == 0)
                    Unsafe.SkipInit(out cookie);
                    return false;
                }

                var newValue = oldValue + IncrBy;
                if (Interlocked.CompareExchange(ref packedState, newValue, oldValue) != oldValue)
                {
                    // Cookie was set from some other thread, this registration cannot timeout yet
                    Unsafe.SkipInit(out cookie);
                    return false;
                }

                if (newValue >= TriggerAfter)
                {
                    // It's been the requisit number of ticks since the registration, timeout
                    cookie = (uint)newValue;
                    return true;
                }

                Unsafe.SkipInit(out cookie);
                return false;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                owner.RemoveRegistration(this);
            }
        }

        // Rather than track proper timestamps we just count up to some number
        // and use that to trigger timeouts.
        private const int TimeoutDivisions = 10;

        private readonly TimeSpan frequency;
        private readonly ILogger logger;

        // Shared thread for all timeouts
        //
        // Using a Thread instead of some variant of a Timer for promptness,
        // because the API fits better, and because going through the thread
        // pool has considerable overhead.
        private Thread timerThread;
        private CancellationTokenSource timerThreadCts;

        private Registration[] registrations;

        internal LuaTimeoutManager(TimeSpan timeout, ILogger logger = null)
        {
            if (timeout <= TimeSpan.Zero)
            {
                throw new ArgumentException($"Timeout must be >= 0, was {timeout}");
            }

            this.logger = logger;

            frequency = TimeSpan.FromTicks(timeout.Ticks / TimeoutDivisions); // Should get us to +/- 10% of the desired timeout, which is fine
            if (frequency < TimeSpan.FromMilliseconds(1))
            {
                // Below 1ms, it doesn't really make sense to try and be precise of timeouts - too much jitter
                frequency = TimeSpan.FromMilliseconds(1);
            }

            int initialRegistrationsSize;
#if DEBUG
            // In Debug, force growth of registrations frequently
            initialRegistrationsSize = 1;
#else
            // In Release, make a decent guess at the max for perf reasons
            initialRegistrationsSize = Environment.ProcessorCount;
#endif
            registrations = new Registration[initialRegistrationsSize];

            logger?.LogInformation("Created LuaTimeoutManager with space for {initialRegistrationSize} timeout registrations", initialRegistrationsSize);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            timerThreadCts?.Cancel();
            timerThread?.Join();

            timerThreadCts?.Dispose();
        }

        /// <summary>
        /// Start this <see cref="LuaTimeoutManager"/>.
        /// </summary>
        internal void Start()
        {
            timerThreadCts = new CancellationTokenSource();
            timerThread =
                new Thread(
                    () =>
                    {
                        var token = timerThreadCts.Token;

                        while (!token.IsCancellationRequested)
                        {
                            if (token.WaitHandle.WaitOne(frequency))
                            {
                                return;
                            }

                            // We will slip a bit here, but timeouts are a best effort thing anyway
                            TickTimeouts();
                        }
                    }
                )
                {
                    Name = $"{nameof(LuaTimeoutManager)}",
                    IsBackground = true,
                };
            timerThread.Start();
        }

        /// <summary>
        /// Register the given <see cref="SessionScriptCache"/> for a timeout notification.
        /// 
        /// A runner can only be registered a single time, by a single thread.
        /// 
        /// Returns a <see cref="Registration"/> that uniquely identifies identifies this registration.
        /// 
        /// Dispose the <see cref="Registration"/> to remove the cache from timeout notifications.
        /// </summary>
        internal Registration RegisterForTimeout(SessionScriptCache cache)
        {
            var ret = new Registration(this, cache);

            var potentiallyInserted = false;

            var curRegistrations = Volatile.Read(ref registrations);

        tryAgain:
            if (potentiallyInserted)
            {
                // If we're trying again, it's because registrations grew 
                for (var i = 0; i < curRegistrations.Length; i++)
                {
                    if (Interlocked.CompareExchange(ref curRegistrations[i], null, null) == ret)
                    {
                        return ret;
                    }
                }
            }

            // Scan for an open slot
            for (var i = 0; i < curRegistrations.Length; i++)
            {
                if (Interlocked.CompareExchange(ref curRegistrations[i], ret, null) == null)
                {
                    potentiallyInserted = true;
                    goto checkUnmodified;
                }
            }

            // Fell through, grow registrations and retry

            var newSize = curRegistrations.Length * 2;
            var newRegistrations = new Registration[newSize];
            for (var i = 0; i < curRegistrations.Length; i++)
            {
                newRegistrations[i] = Interlocked.CompareExchange(ref curRegistrations[i], null, null);
            }

            newRegistrations[curRegistrations.Length] = ret;

            Registration[] updatedRegistrations;
            if ((updatedRegistrations = Interlocked.CompareExchange(ref registrations, newRegistrations, curRegistrations)) == curRegistrations)
            {
                // This thread won, so we know we successfully inserted the registration
                logger?.LogInformation("Grew LuaTimeoutManager registration space to {newSize}", newSize);
                return ret;
            }
            else
            {
                // So other thread won, just update our reference and try again
                curRegistrations = updatedRegistrations;
                goto tryAgain;
            }

        // Other threads might update registrations, so check that before returning
        checkUnmodified:
            if ((updatedRegistrations = Interlocked.CompareExchange(ref registrations, curRegistrations, curRegistrations)) != curRegistrations)
            {
                // Another thread grew registrations, retry
                curRegistrations = updatedRegistrations;
                goto tryAgain;
            }

            return ret;
        }

        /// <summary>
        /// Remove a previously created registration
        /// </summary>
        private void RemoveRegistration(Registration toRemove)
        {
            var removedAtLeastOnce = false;
            var curRegistrations = Volatile.Read(ref registrations);

        tryAgain:
            for (var i = 0; i < curRegistrations.Length; i++)
            {
                if (Interlocked.CompareExchange(ref curRegistrations[i], null, toRemove) == toRemove)
                {
                    removedAtLeastOnce = true;
                    goto checkUnmodified;
                }
            }

            // Scanned all the way through and _didn't_ find our registration
            //
            // This implies we're retrying due to modification, and the thread doing that update
            // saw our previous removal.  That's fine.

            Debug.Assert(removedAtLeastOnce, "We should have seen at least one removal succeed");

            return;

        checkUnmodified:
            Registration[] updatedRegistrations;
            if ((updatedRegistrations = Interlocked.CompareExchange(ref registrations, null, null)) != curRegistrations)
            {
                // Some other thread modified registrations, we need to try again
                curRegistrations = updatedRegistrations;
                goto tryAgain;
            }
        }

        /// <summary>
        /// Invoked periodically to check timeouts on active registrations.
        /// </summary>
        private void TickTimeouts()
        {
            var curRegistrations = Volatile.Read(ref registrations);

            for (var i = 0; i < curRegistrations.Length; i++)
            {
                var reg = curRegistrations[i];
                if (reg == null)
                {
                    continue;
                }

                if (reg.AdvanceTimeout(out var cookie))
                {
                    reg.ScriptCache.RequestTimeout(cookie);
                    break;
                }
            }
        }
    }
}