// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Primary-side replication backpressure for the AOF. Always constructed; an internal
    /// <see cref="enabled"/> flag (driven by AofSyncMaxLagBytes, adjustable at runtime via CONFIG
    /// SET aof-sync-max-lag-bytes) gates the append path so a disabled gate returns immediately from
    /// <see cref="Wait"/>. Without it, an in-memory AOF (null device with fast-aof-truncate) recycles
    /// pages past unshipped records and silently drops them for a lagging replica.
    ///
    /// The gate is fail-safe by construction. For each physical sublog the shipping side publishes
    /// a watermark: the minimum shipped address across all attached replicas (long.MaxValue when
    /// none is attached). An appender, before reserving space on sublog s, computes its own lag
    /// as tail(s) - watermark(s) and stalls while it exceeds the per-sublog budget. Because the
    /// watermark is a past snapshot of a monotonically increasing shipped address, the computed
    /// lag is a conservative over-estimate: a stale or frozen watermark makes the appender stall
    /// sooner, never later. So if the shipping side slows or stops publishing, the appender's own
    /// advancing tail drives the computed lag up and it stalls itself: safety rests on the appender
    /// measuring its own tail against the watermark, not on any publish landing in time.
    ///
    /// The budget is AofSyncMaxLagBytes (a whole-log budget) divided evenly per sublog, matching the
    /// replica-side AofReplayMaxLagBytes convention. The watermark is read-mostly: appenders
    /// only read it, the shipping side writes it periodically, so its cache line stays shared in
    /// appender caches between publishes and coherence traffic scales with publish frequency, not
    /// append frequency. Publish frequency therefore trades throughput (a fresher watermark stalls
    /// less conservatively) for nothing on the safety side.
    /// </summary>
    public sealed class AofBackpressure : IDisposable
    {
        /// <summary>
        /// Sleep interval, in milliseconds, for a stalled appender re-checking its lag.
        /// </summary>
        public const int PollIntervalMs = 1;

        /// <summary>
        /// Byte-progress interval for refreshing the shipped watermark: the shipping side
        /// republishes only after shipping this many bytes since its last publish, so a caught-up
        /// sublog does no work and a busy one batches many chunks per publish. This only affects
        /// watermark freshness (hence throughput), never safety.
        /// </summary>
        public long PublishDeltaBytes { get; private set; }

        /// <summary>
        /// Whether the gate is currently active. Read on the append hot path as a plain field; a
        /// runtime enable/disable propagates by cache coherence (see <see cref="SetBudget"/>).
        /// </summary>
        public bool Enabled => enabled;

        // One padded watermark per physical sublog: the min shipped address across attached
        // replicas, written by the shipping side, read by appenders. Padded to a cache line so a
        // publish to one sublog does not invalidate an appender reading another sublog's slot.
        [StructLayout(LayoutKind.Explicit, Size = 64)]
        struct PaddedLong
        {
            [FieldOffset(0)] public long Value;
        }

        // Per-sublog lag budget and the active flag. Mutable plain fields (no volatile / readonly):
        // a runtime CONFIG SET updates them through SetBudget and appenders observe the change by
        // cache coherence. This is safe because the gate is best-effort and fail-safe — a briefly
        // stale value only over- or under-throttles, never permits a wrap — and an aligned long/bool
        // read is atomic on 64-bit, so no fence is taken on the append hot path.
        long perSublogBudget;
        bool enabled;
        readonly int sublogCount;
        readonly PaddedLong[] shippedWatermark;

        volatile bool disposed;

        readonly ILogger logger;

        // Back-reference to the owning log, set after construction, so the stall diagnostic can read
        // the live tail / safe-tail for a parked sublog. Diagnostic-only; never used for gating.
        GarnetLog log;

        public AofBackpressure(GarnetServerOptions serverOptions, ILogger logger = null)
        {
            this.logger = logger;
            sublogCount = serverOptions.AofPhysicalSublogCount;
            shippedWatermark = new PaddedLong[sublogCount];
            // No replica attached yet: a max watermark makes the computed lag non-positive, so
            // nothing stalls until the shipping side publishes real shipped addresses on attach.
            for (var i = 0; i < sublogCount; i++)
                shippedWatermark[i].Value = long.MaxValue;
            SetBudget(serverOptions.AofSyncMaxLagBytes);
        }

        /// <summary>
        /// Apply a new whole-log lag budget (bytes), evenly divided per sublog. A value &lt;= 0
        /// disables the gate — appenders then return immediately from <see cref="Wait"/> and any
        /// currently parked appender is released on its next poll. Called from the constructor and
        /// from a runtime CONFIG SET (via StoreWrapper). Uses plain-field writes: the budget is
        /// written before the enable flag so an appender that observes <see cref="enabled"/> also
        /// sees the matching budget, and any transient staleness only over/under-throttles (safe).
        /// </summary>
        /// <param name="aofSyncMaxLagBytes">New whole-log budget in bytes; &lt;= 0 disables the gate.</param>
        internal void SetBudget(long aofSyncMaxLagBytes)
        {
            if (aofSyncMaxLagBytes > 0)
            {
                perSublogBudget = Math.Max(1, aofSyncMaxLagBytes / sublogCount);
                PublishDeltaBytes = Math.Max(1, perSublogBudget / 8);
                enabled = true;
                logger?.LogInformation("AofBackpressure enabled: per-sublog budget {perSublogBudget} bytes across {sublogCount} sublogs (AofSyncMaxLagBytes {total} total), publish delta {PublishDeltaBytes} bytes", perSublogBudget, sublogCount, aofSyncMaxLagBytes, PublishDeltaBytes);
            }
            else
            {
                // Neutralize the budget before clearing the flag: an appender that still observes a
                // stale enabled==true then sees an effectively infinite budget and does not stall,
                // so disabling never briefly parks an appender.
                perSublogBudget = long.MaxValue;
                PublishDeltaBytes = 1;
                enabled = false;
                logger?.LogInformation("AofBackpressure disabled (AofSyncMaxLagBytes {total})", aofSyncMaxLagBytes);
            }
        }

        /// <summary>
        /// Stall the calling appender while sublog <paramref name="sublogIdx"/>'s lag
        /// (<paramref name="tailAddress"/> minus the published min-shipped watermark) exceeds the
        /// per-sublog budget. Callers must not hold sublog locks (transaction paths gate before
        /// LockSublogs).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Wait(int sublogIdx, long tailAddress)
        {
            if (!enabled)
                return;
            if (tailAddress - Volatile.Read(ref shippedWatermark[sublogIdx].Value) <= perSublogBudget)
                return;
            WaitSlow(sublogIdx, tailAddress);
        }

        void WaitSlow(int sublogIdx, long capturedTail)
        {
            while (!disposed)
            {
                // Gate on the live tail, not the value captured at entry. The AOF sublog tail can
                // settle below the entry snapshot (in-flight reservations that never finalize, or a
                // truncate/reset under fast-aof-truncate + null device), which would leave a frozen
                // capturedTail permanently above the reachable watermark and park the appender
                // forever. The correct backpressure measure is the current unshipped backlog,
                // liveTail - watermark; re-reading it each iteration self-heals against tail retreat
                // and lets the gate release the instant the shipper catches up.
                var liveTail = log?.GetTailAddress(sublogIdx) ?? capturedTail;
                var watermark = Volatile.Read(ref shippedWatermark[sublogIdx].Value);
                if (liveTail - watermark <= perSublogBudget)
                    break;
                Thread.Sleep(PollIntervalMs);
            }
        }

        /// <summary>
        /// Set the owning log back-reference used by the stall diagnostic. Called once during log
        /// construction; not used on any gating path.
        /// </summary>
        internal void SetLog(GarnetLog log) => this.log = log;

        /// <summary>
        /// Publish sublog <paramref name="sublogIdx"/>'s min shipped address across attached
        /// replicas (long.MaxValue when none is attached, which releases the sublog). A stale
        /// value is safe: it only over-estimates the appender's lag.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PublishShippedAddress(int sublogIdx, long minShippedAddress)
            => Volatile.Write(ref shippedWatermark[sublogIdx].Value, minShippedAddress);

        /// <summary>
        /// Release all stalled appenders permanently (server shutdown).
        /// </summary>
        public void Dispose() => disposed = true;
    }
}