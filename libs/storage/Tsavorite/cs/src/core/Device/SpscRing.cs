// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Bounded single-producer / single-consumer ring buffer with a built-in,
    /// lost-wakeup-safe blocking wait. Cache-line padded between producer and
    /// consumer state so the hot Enqueue/TryDequeue paths take no cross-core
    /// coherence traffic in the steady state, and the consumer parks on an event
    /// at true idle (0% CPU).
    ///
    /// Signaling protocol
    /// ------------------
    /// A consumer that has nothing to do does the following park sequence:
    ///   1. _signal.Reset()
    ///   2. Interlocked.Exchange(_wakeNeeded, 1)   // full fence
    ///   3. recheck tail; if non-empty, undo state and run
    ///   4. _signal.Wait()
    /// The producer's hot path does:
    ///   1. publish item + tail (Volatile.Write release)
    ///   2. Interlocked.MemoryBarrier()           // StoreLoad fence (mfence on x86)
    ///   3. if (_wakeNeeded != 0) _signal.Set()
    /// In the steady state the consumer is awake, _wakeNeeded == 0, and the line
    /// holding it is in S in the producer's L1 — the producer pays one L1 hit and
    /// one mfence (~5 ns) per enqueue, never touches the OS, and never calls Set().
    /// </summary>
    internal sealed class SpscRing<T> where T : struct
    {
        private readonly T[] _buf;
        private readonly int _mask;

        // ----- producer-only -----
#pragma warning disable CS0169
        private long _padA0, _padA1, _padA2, _padA3, _padA4, _padA5, _padA6, _padA7;
#pragma warning restore CS0169
        private long _tail;
        private long _cachedHead;
#pragma warning disable CS0169
        private long _padB0, _padB1, _padB2, _padB3, _padB4, _padB5, _padB6;
#pragma warning restore CS0169

        // ----- consumer-only -----
        private long _head;
        private long _cachedTail;
#pragma warning disable CS0169
        private long _padC0, _padC1, _padC2, _padC3, _padC4, _padC5, _padC6;
#pragma warning restore CS0169

        // ----- shared signaling state -----
        // _wakeNeeded is read by the producer on every Enqueue (cache-hot L1 hit when
        // the consumer is awake and the line is in S) and written by the consumer
        // only at park/unpark transitions. Kept on its own cache line so producer
        // hot-path reads do not bounce with anything the consumer writes frequently.
#pragma warning disable CS0169
        private long _padD0, _padD1, _padD2, _padD3, _padD4, _padD5, _padD6, _padD7;
#pragma warning restore CS0169
        private int _wakeNeeded;
        // Stop flag — set by Stop() to wake any parked consumer and tell it to exit.
        // Co-located with _wakeNeeded (same cache line) since both are read by the
        // consumer in WaitForWork() and written rarely.
        private volatile bool _stopped;
#pragma warning disable CS0169
        private long _padE0, _padE1, _padE2, _padE3, _padE4, _padE5, _padE6;
#pragma warning restore CS0169

        private readonly ManualResetEventSlim _signal = new(initialState: false, spinCount: 0);

        public SpscRing(int capacity)
        {
            if (capacity <= 0 || (capacity & (capacity - 1)) != 0)
                throw new System.ArgumentException("capacity must be a positive power of two", nameof(capacity));
            _buf = new T[capacity];
            _mask = capacity - 1;
        }

        public int Capacity => _buf.Length;

        /// <summary>Producer-side enqueue. Spins until space is available, then signals the consumer if it has parked.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Enqueue(in T item)
        {
            long t = _tail;
            if (t - _cachedHead >= _buf.Length)
            {
                _cachedHead = Volatile.Read(ref _head);
                while (t - _cachedHead >= _buf.Length)
                {
                    var sw = default(SpinWait);
                    sw.SpinOnce();
                    _cachedHead = Volatile.Read(ref _head);
                }
            }
            _buf[t & _mask] = item;
            Volatile.Write(ref _tail, t + 1);

            // Lost-wakeup-safe wake. Without the StoreLoad barrier, x86 TSO would
            // let the wakeNeeded-load below be globally ordered ahead of the tail-write
            // above, so we could miss a consumer that has just parked. Pairs with the
            // Interlocked.Exchange on the consumer side.
            Interlocked.MemoryBarrier();
            if (Volatile.Read(ref _wakeNeeded) != 0)
                Wake();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void Wake() => _signal.Set();

        /// <summary>Consumer-side dequeue. Returns false if empty.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue(out T item)
        {
            long h = _head;
            if (h >= _cachedTail)
            {
                _cachedTail = Volatile.Read(ref _tail);
                if (h >= _cachedTail)
                {
                    item = default;
                    return false;
                }
            }
            item = _buf[h & _mask];
            _buf[h & _mask] = default;
            Volatile.Write(ref _head, h + 1);
            return true;
        }

        /// <summary>True after <see cref="Stop"/> has been called.</summary>
        public bool IsStopped => _stopped;

        /// <summary>
        /// Signal any consumer parked in <see cref="WaitForWork"/> to exit. The
        /// stop flag is written before the event is set so a consumer whose Reset
        /// wiped our Set will still observe <c>_stopped == true</c> on its
        /// post-fence recheck and return without blocking.
        /// </summary>
        public void Stop()
        {
            _stopped = true;
            _signal.Set();
        }

        /// <summary>
        /// Consumer call when <see cref="TryDequeue"/> returned false. Spins briefly to
        /// catch back-to-back work, then parks on the event indefinitely until the
        /// producer signals on next Enqueue, or <see cref="Stop"/> is called.
        ///
        /// <para>
        /// Lost-wakeup-safe under x86 TSO via Interlocked.Exchange (full fence) on the
        /// wakeNeeded store + recheck of tail and the stop flag. The same fence
        /// guarantees that a concurrent Stop's <c>_stopped = true</c> store made before
        /// its Set is visible on our recheck — so even if our Reset wiped the Set,
        /// we observe stopped and return without parking.
        /// </para>
        /// </summary>
        public void WaitForWork(int initialSpins = 64)
        {
            // Phase 1: brief in-cache spin. Catches bursty workloads without paying
            // the event Reset / Wait / Set round-trip.
            var sw = default(SpinWait);
            for (int i = 0; i < initialSpins; i++)
            {
                if (Volatile.Read(ref _tail) != _head) return;
                sw.SpinOnce();
            }

            // Phase 2: park. Order is critical for lost-wakeup safety:
            //   (a) Reset signal
            //   (b) Publish wakeNeeded = 1 with a full fence (Interlocked.Exchange)
            //   (c) Re-check tail AND _stopped — the fence at (b) ensures we see all
            //       stores globally ordered before it (including a concurrent Stop's
            //       _stopped=true write).
            //   (d) If still empty and not stopped, Wait indefinitely.
            // Pairs with the producer's MemoryBarrier between tail-write and
            // wakeNeeded-read.
            _signal.Reset();
            Interlocked.Exchange(ref _wakeNeeded, 1);

            if (Volatile.Read(ref _tail) != _head || _stopped)
            {
                Volatile.Write(ref _wakeNeeded, 0);
                return;
            }

            _signal.Wait();
            Volatile.Write(ref _wakeNeeded, 0);
        }

        /// <summary>Approximate count (consumer-relative). For diagnostics only.</summary>
        public long ApproximateCount => Volatile.Read(ref _tail) - Volatile.Read(ref _head);
    }
}
