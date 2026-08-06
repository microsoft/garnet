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
    /// use blocking <see cref="SignalArrivalAndWait"/>, while progress signals for an idle sublog can use
    /// non-blocking <see cref="SignalArrival"/>. A participant that is about to stop calls
    /// <see cref="Disable"/>, which releases the active round and rejects new rounds until
    /// <see cref="Enable"/>, so peers are never stranded waiting for an arrival that cannot come.
    ///
    /// Fast path (no round active): a single Volatile.Read of a class field plus a long compare.
    /// </summary>
    public sealed class ReplayAlignBarrier : IDisposable
    {
        // Number of Thread.SpinWait iterations between release-flag checks while spinning.
        const int SpinWaitIterations = 16;

        sealed class Round
        {
            public long target;  // target frontier sequence number
            public int remaining;
            // Authoritative per-round release signal: set by the last arrival / Disable. Spinners poll
            // it directly; a parked sleeper is woken by its own participant event being set on release.
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
        /// One reusable wakeup per participant (indexed by virtual sublog), allocated once for the
        /// barrier's lifetime -- no per-round allocation and a fixed, bounded set of wait handles.
        /// Each event has a single waiter: the thread that owns that virtual sublog's replay. Only
        /// that owner resets its event (immediately before parking); a releaser only ever sets events
        /// (never resets), so a set can never be lost to a concurrent reset the way a single shared,
        /// re-armed event can. <see cref="Round.released"/> remains the authoritative signal.
        /// </summary>
        readonly ManualResetEventSlim[] participantEvents;

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
            participantEvents = new ManualResetEventSlim[participantCount];
            for (var i = 0; i < participantCount; i++)
                participantEvents[i] = new ManualResetEventSlim(false);
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
        public void TryOpenRound(long target)
        {
            // Best effort read
            if (Volatile.Read(ref currentRound) != null) return;
            var round = new Round { target = target, remaining = participantCount };
            // No event to arm here: each participant resets its own event right before parking, so the
            // installer never touches a wait handle -- which is what makes a lost wakeup impossible.
            _ = Interlocked.CompareExchange(ref currentRound, round, null);
        }

        /// <summary>
        /// Called by a replay thread after advancing its virtual sublog's frontier. When a round is
        /// active and this participant has reached the target, it arrives once and blocks until every
        /// participant arrives. Lock-free fast path when no round is active.
        /// </summary>
        /// <param name="virtualSublogIdx">The arriving virtual sublog.</param>
        /// <param name="frontier">The virtual sublog's current frontier.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SignalArrivalAndWait(int virtualSublogIdx, long frontier)
        {
            var r = Volatile.Read(ref currentRound);
            if (r == null || frontier < r.target) return;
            if (lastArrivedRound[virtualSublogIdx] == r) return;
            lastArrivedRound[virtualSublogIdx] = r;
            WaitForAllArrivals(r, virtualSublogIdx);
        }

        /// <summary>
        /// Counts an idle virtual sublog as having reached an active round without blocking its caller.
        /// </summary>
        /// <param name="virtualSublogIdx">The arriving virtual sublog.</param>
        /// <param name="frontier">The virtual sublog's current frontier.</param>
        public void SignalArrival(int virtualSublogIdx, long frontier)
        {
            var r = Volatile.Read(ref currentRound);
            if (r == null || frontier < r.target) return;
            if (lastArrivedRound[virtualSublogIdx] == r) return;
            lastArrivedRound[virtualSublogIdx] = r;
            if (Interlocked.Decrement(ref r.remaining) > 0)
                return;
            ReleaseRound(r);
        }

        void WaitForAllArrivals(Round r, int virtualSublogIdx)
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

            // Kernel wait on this participant's own event. We are the sole waiter and the sole resetter
            // of this event, so reset it here (clearing any set left by a prior round) and then re-check
            // released. A releaser sets released BEFORE setting the event, and we reset the event BEFORE
            // reading released, so if our reset cleared a set then that set -- and thus released == true --
            // preceded it and is observed here; otherwise a release either already set released (observed
            // here) or sets the event after our reset and wakes the Wait below. The set is never lost.
            // The wait is bounded by ReplicaSyncTimeout (Timeout.InfiniteTimeSpan blocks until released);
            // a finite timeout just proceeds unaligned, safe because the barrier is a performance aid only.
            var ev = participantEvents[virtualSublogIdx];
            ev.Reset();
            if (r.released)
                return;
            _ = ev.Wait(replicaSyncTimeout);
        }

        /// <summary>
        /// Release spinners and sleepers, then tear the round down only if it is still current.
        /// </summary>
        /// <param name="r"></param>
        private void ReleaseRound(Round r)
        {
            r.released = true;
            SignalAll();
            _ = Interlocked.CompareExchange(ref currentRound, null, r);
        }

        // Wake every parked participant. Setting an event with no current waiter is harmless: the owner
        // resets its event before it next parks, so a stale set from a prior round or Disable is cleared.
        void SignalAll()
        {
            for (var i = 0; i < participantEvents.Length; i++)
                participantEvents[i].Set();
        }

        /// <summary>
        /// Releases the active round and rejects new ones until <see cref="Enable"/>, by occupying
        /// the round slot with a round that can never complete: its target is long.MaxValue, which
        /// no frontier reaches, so no thread arrives at it, and <see cref="TryOpenRound"/> always
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
            SignalAll();
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
            SignalAll();
        }

        /// <summary>
        /// Disposes the per-participant wait handles. The caller must ensure no replay thread is still
        /// parked in the barrier (e.g. after <see cref="Disable"/> and the owning manager has been
        /// replaced); disposing while a participant is inside Wait would surface an ObjectDisposedException.
        /// </summary>
        public void Dispose()
        {
            for (var i = 0; i < participantEvents.Length; i++)
                participantEvents[i].Dispose();
        }
    }
}