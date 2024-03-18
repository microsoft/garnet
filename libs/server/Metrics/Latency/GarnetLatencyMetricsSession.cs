// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Runtime.CompilerServices;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Latency metrics emitted from RespServerSession
    /// </summary>
    internal class GarnetLatencyMetricsSession
    {
        readonly GarnetServerMonitor monitor;
        static readonly LatencyMetricsType[] defaultLatencyTypes =
            Enum.GetValues(typeof(LatencyMetricsType)).Cast<LatencyMetricsType>().ToArray();
        int Version => (int)(monitor.monitor_iterations % 2);
        public int PriorVersion => 1 - Version;
        public LatencyMetricsEntrySession[] metrics;

        public GarnetLatencyMetricsSession(GarnetServerMonitor monitor)
        {
            this.monitor = monitor;
            Init();
        }

        private void Init()
        {
            metrics = new LatencyMetricsEntrySession[defaultLatencyTypes.Length];
            foreach (var cmd in defaultLatencyTypes)
                metrics[(int)cmd] = new LatencyMetricsEntrySession();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Start(LatencyMetricsType cmd)
        {
            int idx = (int)cmd;
            metrics[idx].Start();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void StopAndSwitch(LatencyMetricsType oldCmd, LatencyMetricsType newCmd)
        {
            int old_idx = (int)oldCmd;
            int new_idx = (int)newCmd;
            metrics[new_idx].startTimestamp = metrics[old_idx].startTimestamp;
            metrics[old_idx].startTimestamp = 0;
            metrics[new_idx].RecordValue(Version);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Stop(LatencyMetricsType cmd)
        {
            int idx = (int)cmd;
            metrics[idx].RecordValue(Version);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RecordValue(LatencyMetricsType cmd, long value)
        {
            int idx = (int)cmd;
            metrics[idx].RecordValue(Version, value);
        }

        public void ResetAll()
        {
            foreach (var cmd in defaultLatencyTypes)
                Reset(cmd);
        }

        public void Reset(LatencyMetricsType cmd)
        {
            int idx = (int)cmd;
            metrics[idx].latency[PriorVersion].Reset();
        }
    }
}