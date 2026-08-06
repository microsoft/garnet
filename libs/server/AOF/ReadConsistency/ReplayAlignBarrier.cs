// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Bounds inter-virtual-sublog replay drift on a replica. When a large drift is observed -- by a
    /// reader that is about to block on a lagging virtual sublog, or by a replay thread crossing its
    /// progress gate (see ReadConsistencyManager.BoundReplayDrift) -- a "round" is installed targeting
    /// the leading sublog's frontier sequence number. Each replay thread that reaches the target then
    /// arrives at the barrier and waits there (spinning, then sleeping, per the constructor's spin
    /// budget); when every participant has arrived, the last one releases them all together. The
    /// threads thus align at the target before any leader pulls further ahead, which bounds the drift.
    /// An arrived thread that has waited longer than the constructor's timeout (ReplicaSyncTimeout)
    /// abandons the round and proceeds unaligned, so a peer that never arrives (e.g. a stalled or dead
    /// sublog) cannot strand it indefinitely.
    /// The firing side only installs the round; it never tears it down. The barrier is a performance
    /// aid only -- prefix consistency is enforced by the reader's wait, so a round completing early,
    /// late, or abandoned on timeout never affects correctness.
    ///
    /// Arrivals are identified by virtual sublog and deduplicated within each round. Replayed records
    /// use blocking <see cref="CheckAndWait"/>, while progress signals for an idle sublog can use
    /// non-blocking <see cref="CheckAndArrive"/>. A participant that is about to stop calls
    /// <see cref="Disable"/>, which releases the active round and rejects new rounds until
    /// <see cref="Enable"/>, so peers are never stranded waiting for an arrival that cannot come.
    ///
    /// Fast path (no round active): a single Volatile.Read of a class field plus a long compare.
    /// </summary>
    public sealed class ReplayAlignBarrier
    {
        // Number of Thread.SpinWait iterations between release-flag checks while spinning.
        const int SpinWaitIterations = 16;

        sealed class Round
        {
            public long target;  // target frontier sequence number
            public int remaining;
            // Authoritative per-round release signal: set by the last arrival / Disable. Spinners poll
            // it directly; sleepers loop-gate on it after each wait on the shared releaseEvent below.
            public volatile bool released;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Release()
            {
                released = true;
            }
        }

        readonly int participantCount;

        // Written by the thread that owns each virtual sublog's replay.
        readonly Round[] lastArrivedRound;

        /// <summary>
        /// Single reusable kernel wakeup shared across all rounds, so no per-round wait handle is allocated.
        /// </summary>
        readonly ManualResetEventSlim releaseEvent = new(false);

        /// <summary>
        /// Controls how long a participant thread spin waits before falling back to kernel wait.
        /// </summary>
        readonly long aofReplayBarrierSpinUs;

        /// <summary>
        /// Blocking wait timeout for participant arrival.
        /// </summary>
        readonly TimeSpan replicaSyncTimeout;

        Round currentRound;

        public ReplayAlignBarrier(int participantCount, int aofReplayBarrierSpinUs, TimeSpan replicaSyncTimeout)
        {
            this.participantCount = participantCount;
            lastArrivedRound = new Round[participantCount];
            this.aofReplayBarrierSpinUs = aofReplayBarrierSpinUs < 0
                ? -1
                : (long)(aofReplayBarrierSpinUs * (Stopwatch.Frequency / 1_000_000.0));
            // ReplicaSyncTimeout is Timeout.InfiniteTimeSpan when disabled, which
            // ManualResetEventSlim.Wait treats as wait-forever.
            this.replicaSyncTimeout = replicaSyncTimeout;
        }

        /// <summary>
        /// True while a round is in progress. A disabled barrier reports active: <see cref="Disable"/>
        /// occupies the slot with a round that never completes.
        /// </summary>
        public bool InProgress => Volatile.Read(ref currentRound) != null;

        /// <summary>
        /// Called when a large cross-sublog drift is observed (by a reader about to wait, or by a
        /// replay thread at its progress gate). Installs a round at the given target that expects
        /// every participant to arrive. No-op if a round is already in progress (including the
        /// never-completing round installed by <see cref="Disable"/>).
        /// </summary>
        public void TryActivate(long target)
        {
            // Best effort read
            if (Volatile.Read(ref currentRound) != null) return;
            var round = new Round { target = target, remaining = participantCount };
            // Single round activation winner reset/release event
            if (Interlocked.CompareExchange(ref currentRound, round, null) == null)
                releaseEvent.Reset();
        }

        /// <summary>
        /// Called by a replay thread after advancing its virtual sublog's frontier. When a round is
        /// active and this participant has reached the target, it arrives once and blocks until every
        /// participant arrives. Lock-free fast path when no round is active.
        /// </summary>
        /// <param name="virtualSublogIdx">The arriving virtual sublog.</param>
        /// <param name="frontier">The virtual sublog's current frontier.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CheckAndWait(int virtualSublogIdx, long frontier)
        {
            var r = Volatile.Read(ref currentRound);
            if (r == null || frontier < r.target) return;
            if (lastArrivedRound[virtualSublogIdx] == r) return;
            lastArrivedRound[virtualSublogIdx] = r;
            Arrive(r);
        }

        /// <summary>
        /// Counts an idle virtual sublog as having reached an active round without blocking its caller.
        /// </summary>
        /// <param name="virtualSublogIdx">The arriving virtual sublog.</param>
        /// <param name="frontier">The virtual sublog's current frontier.</param>
        public void CheckAndArrive(int virtualSublogIdx, long frontier)
        {
            var r = Volatile.Read(ref currentRound);
            if (r == null || frontier < r.target) return;
            if (lastArrivedRound[virtualSublogIdx] == r) return;
            lastArrivedRound[virtualSublogIdx] = r;
            if (Interlocked.Decrement(ref r.remaining) > 0)
                return;
            ReleaseRound(r);
        }

        void Arrive(Round r)
        {
            if (Interlocked.Decrement(ref r.remaining) <= 0)
            {
                ReleaseRound(r);
                return;
            }

            if (aofReplayBarrierSpinUs < 0)  // Spin forever (never sleep).
            {
                while (!r.released)
                    Thread.SpinWait(SpinWaitIterations);
                return;
            }
            else if (aofReplayBarrierSpinUs > 0)
            {
                var spinDeadline = Stopwatch.GetTimestamp() + aofReplayBarrierSpinUs;
                while (Stopwatch.GetTimestamp() < spinDeadline)
                {
                    if (r.released)
                        return;
                    Thread.SpinWait(SpinWaitIterations);
                }
            }

            // Kernel wait bounded by ReplicaSyncTimeout: waitTimeout == Timeout.InfiniteTimeSpan blocks
            // until released; a finite timeout returning false means a peer failed to arrive, so we stop
            // looping and proceed unaligned -- safe because the barrier is a performance aid only.
            while (!r.released && releaseEvent.Wait(replicaSyncTimeout)) { }
        }

        /// <summary>
        /// Release spinners and sleepers, then tear the round down only if it is still current.
        /// </summary>
        /// <param name="r"></param>
        private void ReleaseRound(Round r)
        {
            r.released = true;
            releaseEvent.Set();
            _ = Interlocked.CompareExchange(ref currentRound, null, r);
        }

        /// <summary>
        /// Releases the active round and rejects new ones until <see cref="Enable"/>, by occupying
        /// the round slot with a round that can never complete: its target is long.MaxValue, which
        /// no frontier reaches, so no thread arrives at it, and <see cref="TryActivate"/> always
        /// finds a round in progress. Called by a participant that is about to stop arriving on the
        /// per-record replay path -- a replay worker exiting at end of run, workers pausing at a
        /// phase boundary (e.g. a benchmark warmup), or the owning
        /// <see cref="ReadConsistencyManager"/> being replaced -- so no peer is left stranded in a
        /// round that can no longer complete.
        /// </summary>
        public void Disable()
        {
            // The inert round is pre-released with effectively infinite remaining: if any path ever
            // did arrive at the unreachable target, it would return immediately instead of parking,
            // and the last-arrival teardown could never clear the slot.
            var inert = new Round { target = long.MaxValue, remaining = int.MaxValue, released = true };
            var r = Interlocked.Exchange(ref currentRound, inert);
            r?.Release();
            releaseEvent.Set();
        }

        /// <summary>
        /// Re-allows round activation after <see cref="Disable"/> by clearing the round slot,
        /// releasing whatever round it held (normally Disable's never-completing round, which no
        /// thread waits on). Called once every participant is again arriving on the per-record
        /// replay path (e.g. when a benchmark's measured pass starts after warmup).
        /// </summary>
        public void Enable()
        {
            var r = Interlocked.Exchange(ref currentRound, null);
            r?.Release();
            releaseEvent.Set();
        }
    }
}