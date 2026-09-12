// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Latency metrics emitted from RespServerSession
    /// </summary>
    internal sealed class GarnetLatencyMetricsSession
    {
        readonly GarnetServerMonitor monitor;
        static readonly LatencyMetricsType[] defaultLatencyTypes = Enum.GetValues<LatencyMetricsType>();
        int Version => (int)(monitor.monitor_iterations % 2);
        public int PriorVersion => 1 - Version;
        public LatencyMetricsEntrySession[] metrics;

        private SingleWriterMultiReaderLock disposeLock;

        public GarnetLatencyMetricsSession(GarnetServerMonitor monitor)
        {
            this.monitor = monitor;
            Init();
        }

        int SignificantDigits => monitor.LatencyPrecision;

        public void Return()
        {
            LatencyMetricsEntrySession[] toRelease;
            try
            {
                disposeLock.WriteLock();
                toRelease = metrics;
                metrics = null;
            }
            finally
            {
                disposeLock.WriteUnlock();
            }

            // Published as unavailable before the arrays go back to the shared pool, so no reader can
            // still reach them through this session.
            if (toRelease == null)
                return;

            foreach (var cmd in defaultLatencyTypes)
                toRelease[(int)cmd].Return();
        }

        private void Init()
        {
            metrics = new LatencyMetricsEntrySession[defaultLatencyTypes.Length];
            foreach (var cmd in defaultLatencyTypes)
                metrics[(int)cmd] = new LatencyMetricsEntrySession(SignificantDigits);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Start(LatencyMetricsType cmd)
        {
            var m = metrics;
            if (m == null) return;
            m[(int)cmd].Start();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long Get(LatencyMetricsType cmd)
        {
            var m = metrics;
            return m == null ? 0 : m[(int)cmd].startTimestamp;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void StopAndSwitch(LatencyMetricsType oldCmd, LatencyMetricsType newCmd)
        {
            var m = metrics;
            if (m == null) return;
            int old_idx = (int)oldCmd;
            int new_idx = (int)newCmd;
            m[new_idx].startTimestamp = m[old_idx].startTimestamp;
            m[old_idx].startTimestamp = 0;
            m[new_idx].RecordValue(Version);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Stop(LatencyMetricsType cmd)
        {
            var m = metrics;
            if (m == null) return;
            m[(int)cmd].RecordValue(Version);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RecordValue(LatencyMetricsType cmd, long value)
        {
            var m = metrics;
            if (m == null) return;
            m[(int)cmd].RecordValue(Version, value);
        }

        public void ResetAll()
        {
            foreach (var cmd in defaultLatencyTypes)
                Reset(cmd);
        }

        /// <summary>
        /// Releases the histograms of every type that has recorded nothing for <paramref name="threshold"/>
        /// consecutive monitor windows. Returns the number of types released.
        /// </summary>
        /// <param name="threshold">Consecutive empty windows required before releasing a type.</param>
        /// <remarks>
        /// Called from the monitor sweep, which visits every session on a timer and so reaches connections
        /// that are quiet -- the state this reclaims. Takes the dispose lock so a session being torn down
        /// concurrently cannot have its metrics array replaced part way through; the lock does not, and is
        /// not intended to, exclude the record path, which
        /// <see cref="LatencyMetricsEntrySession.ReclaimIfQuiesced"/> is safe against by construction.
        /// </remarks>
        public int ReclaimQuiescedHistograms(int threshold)
        {
            var released = 0;
            try
            {
                disposeLock.WriteLock();
                if (metrics == null)
                    return 0;

                foreach (var cmd in defaultLatencyTypes)
                {
                    if (metrics[(int)cmd].ReclaimIfQuiesced(threshold))
                        released++;
                }
            }
            finally
            {
                disposeLock.WriteUnlock();
            }

            return released;
        }

        public void Reset(LatencyMetricsType cmd)
        {
            int idx = (int)cmd;
            try
            {
                disposeLock.WriteLock();
                if (metrics != null && metrics[idx].latency != null)
                {
                    metrics[idx].latency[PriorVersion].Reset();
                }
            }
            finally
            {
                disposeLock.WriteUnlock();
            }
        }
    }
}