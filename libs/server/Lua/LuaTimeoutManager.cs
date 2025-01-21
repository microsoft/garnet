// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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

        private Timer timer;

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
                frequency = TimeSpan.FromMicroseconds(1);
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
        /// A runner can only be registered a single time, it should be unregistered
        /// before being registered again.
        /// </summary>
        internal void RegisterForTimeout(LuaRunner runner)
        {
            var now = Stopwatch.GetTimestamp();
            var timeoutAt = now + timeoutTimestampTicks;

        }

        /// <summary>
        /// Remove the given <see cref="LuaRunner"/> from timeout notifications.
        /// 
        /// This should always be paired with a single <see cref="RegisterForTimeout(LuaRunner)"/> call.
        /// </summary>
        internal void UnregisterForTimeout(LuaRunner runner)
        {
        }

        /// <summary>
        /// Invoked approximately every <see cref="frequency"/>.
        /// 
        /// Checks all registered runners and informs them of timeouts if needed.
        /// </summary>
        private void OnFrequency(object state)
        {
            var now = Stopwatch.GetTimestamp();

            
        }
    }
}
