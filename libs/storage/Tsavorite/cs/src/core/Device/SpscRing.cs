// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Bounded single-producer / single-consumer ring buffer with no CAS on the hot path.
    /// Used by <see cref="LocalMemoryDevice"/> for per-processor IO request queues so that
    /// each submitter thread (cached routing) feeds exactly one ring, and exactly one IO
    /// processor thread drains it.
    ///
    /// Layout: head and tail counters are placed on dedicated cache lines to avoid
    /// false-sharing between producer and consumer.
    /// </summary>
    internal sealed class SpscRing<T> where T : struct
    {
        private readonly T[] _buf;
        private readonly int _mask;

        // Producer-only field, padded.
#pragma warning disable CS0169
        private long _pad0a, _pad0b, _pad0c, _pad0d, _pad0e, _pad0f, _pad0g;
#pragma warning restore CS0169
        private long _tail;     // next slot to write
#pragma warning disable CS0169
        private long _pad1a, _pad1b, _pad1c, _pad1d, _pad1e, _pad1f, _pad1g;
#pragma warning restore CS0169

        // Consumer-only field, padded.
        private long _head;     // next slot to read
#pragma warning disable CS0169
        private long _pad2a, _pad2b, _pad2c, _pad2d, _pad2e, _pad2f, _pad2g;
#pragma warning restore CS0169

        // Producer's cached view of head (refreshed only when full is detected). Avoids
        // touching the consumer's cache line on every successful enqueue.
        private long _cachedHead;
        // Consumer's cached view of tail.
        private long _cachedTail;

        public SpscRing(int capacity)
        {
            if (capacity <= 0 || (capacity & (capacity - 1)) != 0)
                throw new System.ArgumentException("capacity must be a positive power of two", nameof(capacity));
            _buf = new T[capacity];
            _mask = capacity - 1;
        }

        public int Capacity => _buf.Length;

        /// <summary>Producer-side enqueue. Spins until space is available.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Enqueue(in T item)
        {
            long t = _tail;
            // Fast path: use cached head; only re-read the shared head when the cache says full.
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
        }

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
            // Clear slot reference (so any reference fields inside T don't pin objects after dequeue).
            _buf[h & _mask] = default;
            Volatile.Write(ref _head, h + 1);
            return true;
        }

        /// <summary>Approximate count (consumer-relative). For diagnostics only.</summary>
        public long ApproximateCount => Volatile.Read(ref _tail) - Volatile.Read(ref _head);
    }
}
