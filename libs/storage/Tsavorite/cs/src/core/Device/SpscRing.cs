// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Bounded multi-producer / single-consumer ring buffer with built-in,
    /// lost-wakeup-safe blocking wait on the consumer side. Lock-free on both
    /// sides; producers serialize only on a single fetch-and-add of the tail
    /// counter and otherwise write into per-slot cache lines independently.
    /// Cache-line padded to eliminate false sharing between producer-side,
    /// consumer-side, and signaling state.
    ///
    /// Algorithm: Vyukov-style sequence-numbered slots, with fetch-and-add
    /// (rather than CAS-loop) on the tail counter for guaranteed forward progress
    /// per producer and lower contention than CAS-retry under burst load.
    ///
    /// Each slot carries a 64-bit <c>Sequence</c> field:
    ///   - <c>seq == pos</c>      → empty (ready for a producer claiming index <c>pos</c>)
    ///   - <c>seq == pos + 1</c>  → full (ready for the consumer at head=<c>pos</c>)
    ///   - <c>seq == pos + N</c>  → consumed and waiting to be re-used for index <c>pos+N</c>
    /// where N = capacity.
    ///
    /// Producer steps (single Interlocked.Increment, no retry loop):
    ///   1. claimed = Interlocked.Increment(ref _tail) - 1
    ///   2. Spin while slot.Sequence != claimed     (ring full at this slot; consumer
    ///                                              hasn't drained the previous round yet)
    ///   3. slot.Value = item
    ///   4. Volatile.Write(slot.Sequence, claimed + 1)         (release; publishes to consumer)
    ///   5. Interlocked.MemoryBarrier(); if (wakeNeeded) Signal()
    ///
    /// Consumer steps:
    ///   1. slot = _buf[_head &amp; mask]
    ///   2. if Volatile.Read(slot.Sequence) != _head + 1: empty, return false
    ///   3. read slot.Value
    ///   4. Volatile.Write(slot.Sequence, _head + capacity)    (release; re-empties for next round)
    ///   5. _head++
    ///
    /// SPSC case: a single producer per ring pays one Interlocked.Increment (~5 ns
    /// on x86) instead of a plain store; the SPSC perf difference vs. a lock-free
    /// SPSC ring is small and disappears at any meaningful throughput.
    /// MPSC case: producers race only on the tail counter; each claims a unique
    /// slot and writes it independently in its own cache line. There is no
    /// per-Enqueue lock and no kernel futex, eliminating the
    /// Monitor.Enter_Slowpath cost (~12% of managed CPU in the prior design).
    /// Consumer head-of-line blocking is possible if a producer is delayed
    /// between claim and write, but the window is single-digit nanoseconds in
    /// practice.
    ///
    /// Signaling protocol (no busy spin once parked)
    /// ----------------------------------------------
    /// Consumer park sequence (called when <see cref="TryDequeue"/> returned false):
    ///   1. <c>_signal.Reset()</c>
    ///   2. <c>Interlocked.Exchange(ref _state.WakeNeeded, 1)</c>  (full StoreLoad fence)
    ///   3. Re-check work and <c>_state.Stopped</c>; if anything to do, return
    ///   4. <c>_signal.Wait()</c>  (parks on futex; 0% CPU at idle)
    /// Producer's wake path runs after publishing the slot sequence:
    ///   <c>Interlocked.MemoryBarrier();</c>
    ///   <c>if (Volatile.Read(ref _state.WakeNeeded) != 0) _signal.Set();</c>
    /// In steady state the consumer is awake (draining a batch) and <c>_state.WakeNeeded == 0</c>
    /// stays cache-stable in the producer's L1; the producer pays one
    /// Interlocked.Increment + one mfence (~10 ns total) per enqueue and never
    /// touches the OS. The consumer wakes once per idle→busy transition
    /// (~10-100 µs futex wake on Linux), then drains the batch via TryDequeue
    /// before re-parking — wake latency amortizes across the batch.
    /// </summary>
    internal sealed class SpscRing<T> where T : struct
    {
        // Each slot pairs an atomic 64-bit sequence number with the value payload.
        // The runtime lays the value type fields out contiguously; for small T
        // (e.g. IORequestLocalMemory at 48 B) the whole slot fits in a single
        // cache line so producer's "write data + publish sequence" stays local.
        private struct Slot
        {
            public long Sequence;
            public T Value;
        }

        private readonly Slot[] _buf;
        private readonly int _mask;
        private readonly long _capacity;

        // All mutable cross-thread counters live in one cache-line-isolated block (see SpscRingState)
        // so the producer's Tail writes, the consumer's Head writes, and the shared signaling word
        // never false-share with each other or with neighboring object fields.
        private SpscRingState _state;

        private readonly ManualResetEventSlim _signal = new(initialState: false, spinCount: 0);

        public SpscRing(int capacity)
        {
            if (capacity <= 0 || (capacity & (capacity - 1)) != 0)
                throw new System.ArgumentException("capacity must be a positive power of two", nameof(capacity));
            _buf = new Slot[capacity];
            _mask = capacity - 1;
            _capacity = capacity;
            // Initialize sequences so the first producer (claiming index 0) sees
            // slot 0's Sequence == 0 (empty), writes, and sets Sequence to 1
            // (full for consumer head=0).
            for (int i = 0; i < capacity; i++)
                _buf[i].Sequence = i;
        }

        public int Capacity => _buf.Length;

        /// <summary>True after <see cref="Stop"/> has been called.</summary>
        public bool IsStopped => _state.Stopped;

        /// <summary>
        /// Signal any consumer parked in <see cref="WaitForWork"/> to exit. The stop
        /// flag is written before the event is set so a consumer whose Reset wiped
        /// our Set will still observe <c>_state.Stopped == true</c> on its post-fence
        /// recheck and return without blocking.
        /// </summary>
        public void Stop()
        {
            _state.Stopped = true;
            _signal.Set();
        }

        /// <summary>
        /// Producer enqueue. Multi-producer-safe via a single Interlocked.Increment
        /// on <see cref="SpscRingState.Tail"/>. If the claimed slot has not yet been drained by
        /// the consumer (ring full at this slot), spins briefly with pure
        /// <see cref="Thread.SpinWait(int)"/>; under sustained backpressure the spin
        /// escalates to <see cref="SpinWait"/> (which yields/sleeps), giving
        /// the consumer CPU to drain.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Enqueue(in T item)
        {
            long claimed = Interlocked.Increment(ref _state.Tail) - 1;
            ref var slot = ref _buf[claimed & _mask];

            // Wait until this slot is empty for OUR claim (slot.Sequence == claimed).
            // The slot was last written by either the initial constructor (which set
            // Sequence = index) or the consumer's previous round (which set it to
            // index + N*capacity for N rounds back). When Sequence catches up to
            // exactly our claimed index, it's our turn to write.
            if (Volatile.Read(ref slot.Sequence) != claimed)
                WaitForSlotEmpty(ref slot, claimed);

            slot.Value = item;
            // Release-store: publishes data to the consumer.
            Volatile.Write(ref slot.Sequence, claimed + 1);

            WakeIfParked();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void WaitForSlotEmpty(ref Slot slot, long claimed)
        {
            // Phase 1: bounded pure busy spin — catches the common case where
            // consumer is one or two slots behind and is actively draining.
            for (int i = 0; i < 64; i++)
            {
                if (Volatile.Read(ref slot.Sequence) == claimed) return;
                Thread.SpinWait(16);
            }
            // Phase 2: pure Thread.Yield loop (no Sleep escalation). SpinWait.SpinOnce
            // escalates to Thread.Sleep(1) within ~20 iterations, which a CPU profile
            // showed dominated wall time (~21% of total stack-time at t=16) by stalling
            // the producer for 1 ms each time the ring filled — far longer than the
            // consumer needs to drain a slot (~167 ns). Thread.Yield drops the scheduler
            // quantum (~µs on Linux) without parking, so the producer comes back as
            // soon as it is re-scheduled and re-checks the slot, paying only scheduler
            // round-trip rather than 1 ms Sleep granularity.
            while (Volatile.Read(ref slot.Sequence) != claimed)
                Thread.Yield();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WakeIfParked()
        {
            // Lost-wakeup-safe wake. The StoreLoad barrier ensures the seq-write above is
            // globally visible before we read _state.WakeNeeded; pairs with the Interlocked.Exchange
            // on the consumer's park path.
            Interlocked.MemoryBarrier();
            if (Volatile.Read(ref _state.WakeNeeded) == 0)
                return;
            // CLAIM the wake atomically. Only the producer that successfully CASes
            // _state.WakeNeeded from 1 to 0 calls Set(), so a burst of N producers after a
            // consumer parks issues exactly ONE Set() instead of N. ManualResetEventSlim.Set
            // internally takes a Monitor; without this claim, redundant Sets dominated
            // the profile at ~10% of managed CPU on the read-pending path.
            if (Interlocked.CompareExchange(ref _state.WakeNeeded, 0, 1) == 1)
                Wake();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void Wake() => _signal.Set();

        /// <summary>Consumer dequeue. Returns false if empty.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue(out T item)
        {
            long pos = _state.Head;
            ref var slot = ref _buf[pos & _mask];
            if (Volatile.Read(ref slot.Sequence) != pos + 1)
            {
                item = default;
                return false;
            }
            item = slot.Value;
            slot.Value = default;
            // Release-store: re-empties slot for the NEXT round at this index.
            Volatile.Write(ref slot.Sequence, pos + _capacity);
            _state.Head = pos + 1;
            return true;
        }

        /// <summary>
        /// Consumer call when <see cref="TryDequeue"/> returned false. Spins briefly
        /// with pure <see cref="Thread.SpinWait(int)"/> (no escalation), then parks
        /// on the event indefinitely. The bounded spin catches back-to-back work
        /// without paying the event Reset / Wait / Set round-trip; when the spin
        /// expires we park on a futex at 0% CPU until the producer signals.
        /// </summary>
        public void WaitForWork(int spinIterations = 64)
        {
            // Phase 1: bounded BUSY spin. CRITICAL: Thread.SpinWait, NOT
            // SpinWait.SpinOnce — the latter escalates to Thread.Yield / Sleep(0) /
            // Sleep(1) within ~20 iterations, which a CPU profile showed was eating
            // ~12% of total managed CPU in Thread.Sleep on lightly-loaded rings
            // (drainer wakes briefly, sees empty, SpinOnce escalates and sleeps for
            // ~1 ms, wakes, re-spins → wasted CPU). Pure Thread.SpinWait at
            // ~8 cycles/iteration × 64 ≈ 1-2 µs total, never escalates.
            for (int i = 0; i < spinIterations; i++)
            {
                if (HasWork()) return;
                Thread.SpinWait(8);
            }

            // Phase 2: park. Order is critical for lost-wakeup safety:
            //   (a) Reset signal
            //   (b) Publish wakeNeeded = 1 with a full fence (Interlocked.Exchange)
            //   (c) Re-check work AND _state.Stopped — the fence at (b) ensures we see all
            //       stores globally ordered before it, including a concurrent Stop's
            //       _state.Stopped=true write and a concurrent Enqueue's sequence-write.
            //   (d) If still empty and not stopped, Wait.
            _signal.Reset();
            Interlocked.Exchange(ref _state.WakeNeeded, 1);

            if (HasWork() || _state.Stopped)
            {
                Volatile.Write(ref _state.WakeNeeded, 0);
                return;
            }

            _signal.Wait();
            Volatile.Write(ref _state.WakeNeeded, 0);
        }

        /// <summary>Cheap "is the next slot ready for consumer" check.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HasWork()
        {
            long pos = _state.Head;
            return Volatile.Read(ref _buf[pos & _mask].Sequence) == pos + 1;
        }

        /// <summary>Approximate count (consumer-relative). For diagnostics only.</summary>
        public long ApproximateCount => Volatile.Read(ref _state.Tail) - Volatile.Read(ref _state.Head);
    }

    /// <summary>
    /// Cache-line-isolated mutable state for <see cref="SpscRing{T}"/>. Declared as a non-generic top-level
    /// struct because explicit layout is not permitted on a type nested in a generic type. Five cache lines:
    /// line 0 and the trailing line are pad (isolating the block from adjacent object fields); <see cref="Tail"/>,
    /// <see cref="Head"/>, and the signaling word each sit alone on lines 1-3. <see cref="WakeNeeded"/> and
    /// <see cref="Stopped"/> intentionally share one line — the producer reads WakeNeeded on every enqueue, and
    /// co-locating the rarely-written Stopped there costs the producer nothing.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 5 * CacheLineBytes)]
    internal struct SpscRingState
    {
        private const int CacheLineBytes = 64;

        [FieldOffset(1 * CacheLineBytes)] public long Tail;          // producer claim counter (Interlocked.Increment)
        [FieldOffset(2 * CacheLineBytes)] public long Head;          // consumer read cursor (consumer-only writer)
        [FieldOffset(3 * CacheLineBytes)] public int WakeNeeded;     // shared park/unpark flag
        [FieldOffset(3 * CacheLineBytes + sizeof(int))] public volatile bool Stopped;
    }
}