// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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
    ///
    /// A stalled appender waits by sleeping on its own thread, which is a poor place to wait: the
    /// threads that serve other sessions run out, and where socket completions run inline the shipper's
    /// sends, which are what advance the watermark, can no longer complete. So a session that can park does
    /// not reach <see cref="Wait"/> in the first place. Before running a command that may append it asks
    /// <see cref="AdmissionClosed"/> and, when that holds, registers with <see cref="TryRegisterWaiter"/> and
    /// gives up its thread until admission reopens. Admission closes one publish interval before the budget
    /// does, so appenders that are already past it settle in the band between the two and <see cref="Wait"/>
    /// stays a fail-safe rather than becoming the throttle. It looks at every sublog and closes when any is
    /// over, because the sublog a command appends to is not known before it runs.
    ///
    /// <see cref="Wait"/> still guards every append, since admission cannot cover everything: writes inside
    /// transactions and scripts, which hold key locks once running; appends from a session that cannot park;
    /// appends that pass admission together; a watermark that drops when a lagging replica attaches; and the
    /// background commit markers.
    /// </summary>
    public sealed class AofBackpressure : IDisposable
    {
        /// <summary>
        /// Interval, in milliseconds, at which a stalled appender, or the poller that wakes parked sessions,
        /// re-checks the lag.
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

        // Sessions parked on the gate, all woken together when admission reopens. A shared one-shot timer polls
        // for that, because the shipper publishes only when it advances, and the live tail can retreat without
        // any publish. Created and armed on the first registration, so a gate nobody waits on costs nothing.
        readonly Lock waiterLock = new();
        List<IAofGateWaiter> waiters = [];
        List<IAofGateWaiter> spareWaiters;
        Timer pollTimer;
        bool pollTimerArmed;

        readonly ILogger logger;

        // Back-reference to the owning log, set after construction, so the stall diagnostic and the
        // read-only AnyStalled predicate can read the live tail for a sublog. Never used by the
        // append gating path itself (Wait receives the tail from its caller).
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
        }

        /// <summary>
        /// True if the gate is enabled and at least one sublog's current lag (live tail minus its
        /// published min-shipped watermark) exceeds the per-sublog budget.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool AnyStalled()
        {
            if (!enabled)
                return false;
            for (var i = 0; i < sublogCount; i++)
            {
                var tail = log?.GetTailAddress(i) ?? 0;
                if (tail - Volatile.Read(ref shippedWatermark[i].Value) > perSublogBudget)
                    return true;
            }
            return false;
        }

        /// <summary>
        /// True if the gate is enabled and at least one sublog's lag is within one publish interval of its
        /// budget, which is the point at which a session that can park should wait instead of appending.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The threshold sits below the budget that <see cref="Wait"/> stalls at. Under steady throttling the lag
        /// settles at whichever threshold stops writers first: if that were the budget, the writers admitted
        /// together would all run into <see cref="Wait"/> and it, rather than admission, would be doing the
        /// throttling. The watermark only moves in steps of <see cref="PublishDeltaBytes"/>, so there is nothing
        /// to be gained by waking a session sooner than that.
        /// </para>
        /// <para>
        /// Every sublog is checked, not only the one a command will append to, because that is not known until
        /// the command has run.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool AdmissionClosed()
        {
            if (!enabled)
                return false;

            var threshold = perSublogBudget - PublishDeltaBytes;
            for (var i = 0; i < sublogCount; i++)
            {
                var tail = log?.GetTailAddress(i) ?? 0;
                if (tail - Volatile.Read(ref shippedWatermark[i].Value) > threshold)
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Registers <paramref name="waiter"/> to be told when admission reopens, so its owner can stop waiting
        /// on a thread of its own.
        /// </summary>
        /// <param name="waiter">The waiter to notify, once.</param>
        /// <returns>
        /// False if admission is open already, or the gate has shut down, in which case nothing was registered
        /// and the caller proceeds instead of waiting.
        /// </returns>
        /// <remarks>
        /// The check and the registration happen under the lock the poller takes for its own check, so a
        /// reopening cannot fall between them: either this call sees admission open, or the poller finds the
        /// waiter registered.
        /// </remarks>
        internal bool TryRegisterWaiter(IAofGateWaiter waiter)
        {
            lock (waiterLock)
            {
                if (disposed || !AdmissionClosed())
                    return false;

                waiters.Add(waiter);
                if (!pollTimerArmed)
                {
                    pollTimerArmed = true;
                    if (pollTimer is null)
                        pollTimer = new Timer(static state => ((AofBackpressure)state).Poll(), this, PollIntervalMs, Timeout.Infinite);
                    else
                        _ = pollTimer.Change(PollIntervalMs, Timeout.Infinite);
                }
                return true;
            }
        }

        /// <summary>
        /// The number of waiters registered and not yet notified.
        /// </summary>
        internal int WaiterCount
        {
            get
            {
                lock (waiterLock)
                    return waiters.Count;
            }
        }

        void Poll()
        {
            List<IAofGateWaiter> woken;
            lock (waiterLock)
            {
                if (waiters.Count > 0 && AdmissionClosed())
                {
                    _ = pollTimer.Change(PollIntervalMs, Timeout.Infinite);
                    return;
                }

                pollTimerArmed = false;
                woken = TakeWaiters();
            }

            Notify(woken);
        }

        // Called with waiterLock held. The list handed back is the caller's alone until it returns it through Notify.
        List<IAofGateWaiter> TakeWaiters()
        {
            var taken = waiters;
            waiters = spareWaiters ?? [];
            spareWaiters = null;
            return taken;
        }

        // Runs outside the lock, because waking a waiter runs code of its owner. One waiter failing must not leave
        // the rest waiting, and an exception escaping a timer callback would end the process.
        void Notify(List<IAofGateWaiter> woken)
        {
            for (var i = 0; i < woken.Count; i++)
            {
                try
                {
                    woken[i].OnGateOpen();
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "A session waiting on the AOF gate failed to be woken");
                }
            }

            woken.Clear();
            lock (waiterLock)
                spareWaiters ??= woken;
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
        /// Release all stalled appenders permanently (server shutdown), including sessions parked on the gate.
        /// </summary>
        public void Dispose()
        {
            List<IAofGateWaiter> woken;
            lock (waiterLock)
            {
                // Nothing registers once disposed, so a poll that is still to run finds no waiter
                disposed = true;
                pollTimer?.Dispose();
                woken = TakeWaiters();
            }

            Notify(woken);
        }
    }
}