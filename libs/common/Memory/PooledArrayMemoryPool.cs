// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// A <see cref="MemoryPool{T}"/> implementation that recycles the <see cref="IMemoryOwner{T}"/>
    /// wrapper objects in addition to the underlying byte arrays (which are still drawn from
    /// <see cref="ArrayPool{T}.Shared"/>).
    /// <para>
    /// The default <see cref="MemoryPool{T}.Shared"/> (System.Buffers.ArrayMemoryPool) heap-allocates
    /// a fresh wrapper instance (ArrayMemoryPoolBuffer) on every Rent call. On the Garnet pending-IO
    /// GET path that is &gt;1 wrapper allocation per disk-read, which dominates gen0/gen1 pressure at
    /// 500K+ ops/sec. This pool reuses the wrapper instance across Rent/Dispose cycles.
    /// </para>
    /// <para>
    /// Design is two-tier (mirrors <c>ArrayPool&lt;T&gt;.Shared</c>): a
    /// <see cref="Tsavorite.core.ThreadLocalCache{T}"/> fast path provides a per-thread
    /// lock-free LIFO; a process-wide <see cref="ConcurrentQueue{T}"/> serves as fallback
    /// when the per-thread cache is empty (Rent) or full (Return). In Garnet's
    /// <c>ProcessMessage</c> flow, Rent (CopyRespTo) and Dispose (SendAndReset) execute
    /// on the same worker thread synchronously, so the TLS hit rate is effectively 100%
    /// in steady state.
    /// </para>
    /// <para>
    /// Memory ceiling (wrappers only; underlying byte[] are owned by ArrayPool):
    /// <c>num_unique_threads × <see cref="Tsavorite.core.ThreadLocalCache{T}.Capacity"/> × sizeof(wrapper)</c>
    /// + <c><see cref="MaxRetainedWrappers"/> × sizeof(wrapper)</c>. With ~160 threads on an 80-core
    /// box that is roughly 660 KB — flat and independent of session count, batch size, or workload.
    /// </para>
    /// <para>
    /// The pool is exposed as a process-wide singleton via <see cref="Shared"/>. A single instance
    /// is intentional because TLS slots are static and would otherwise be shared across pool
    /// instances.
    /// </para>
    /// </summary>
    public sealed class PooledArrayMemoryPool : MemoryPool<byte>
    {
        /// <summary>
        /// Process-wide singleton instance.
        /// </summary>
        public static new readonly PooledArrayMemoryPool Shared = new();

        /// <summary>
        /// Upper bound on the number of wrapper instances retained in the global fallback queue.
        /// Excess wrappers returned beyond this cap are dropped (the underlying byte[] is still
        /// returned to <see cref="ArrayPool{T}.Shared"/>). The TLS cache absorbs the steady-state
        /// in-flight count; the global queue only sees spillover.
        /// </summary>
        private const int MaxRetainedWrappers = 4096;

        /// <summary>
        /// Default rent size matches the .NET ArrayMemoryPool default for byte (4 KB). Only used
        /// when <see cref="Rent"/> is called with the sentinel <c>-1</c>; Garnet's call sites all
        /// pass explicit sizes.
        /// </summary>
        private const int DefaultRentSize = 4096;

        private readonly ConcurrentQueue<PooledBuffer> globalPool = new();
        private int retainedCount;

        private PooledArrayMemoryPool() { }

        /// <inheritdoc/>
        public override int MaxBufferSize => Array.MaxLength;

        /// <inheritdoc/>
        public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
        {
            if (minBufferSize == -1)
                minBufferSize = DefaultRentSize;
            else if ((uint)minBufferSize > (uint)MaxBufferSize)
                ThrowArgumentOutOfRange();

            PooledBuffer buf;

            // Fast path: thread-local LIFO.
            if (Tsavorite.core.ThreadLocalCache<PooledBuffer>.TryRent(out buf))
            {
                // nothing — got it
            }
            else if (globalPool.TryDequeue(out buf))
            {
                Interlocked.Decrement(ref retainedCount);
            }
            else
            {
                buf = new PooledBuffer(this);
            }

            buf.Init(minBufferSize);
            return buf;
        }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            // Singleton; nothing to release.
        }

        /// <summary>
        /// Wrapper-recycle callback. Invoked exclusively from <see cref="PooledBuffer.Dispose"/>
        /// after the underlying byte[] has already been returned to <see cref="ArrayPool{T}.Shared"/>
        /// and the <c>array</c> field has been Interlocked-Exchanged to <c>null</c>. Do not call
        /// this directly from anywhere else — wrapper lifetime is managed by <c>Dispose</c>, and
        /// invoking it outside of that path would re-enqueue a wrapper while it is still
        /// considered "rented" by its current owner.
        /// </summary>
        /// <remarks>
        /// The cap-exceeded "drop" branch is safe: the <see cref="PooledBuffer"/> at that point
        /// owns no native or pooled resources (<c>array == null</c>, byte[] already returned).
        /// Letting GC reclaim the bare wrapper costs only one small managed object and avoids
        /// unbounded growth of <see cref="globalPool"/>. The wrapper does not implement a
        /// finalizer, so no finalization pressure is incurred either.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnPooledBufferDisposed(PooledBuffer buf)
        {
            // Fast path: thread-local LIFO.
            if (Tsavorite.core.ThreadLocalCache<PooledBuffer>.TryReturn(buf))
                return;

            // Slow path: global queue with hard cap. If we are over the cap, drop the wrapper
            // (see remarks above — its byte[] is already back in ArrayPool, so dropping leaks
            // nothing beyond the wrapper's own managed footprint, which the GC will reclaim).
            if (Interlocked.Increment(ref retainedCount) > MaxRetainedWrappers)
            {
                Interlocked.Decrement(ref retainedCount);
                return;
            }
            globalPool.Enqueue(buf);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowArgumentOutOfRange()
            => throw new ArgumentOutOfRangeException("minBufferSize");

        /// <summary>
        /// IMemoryOwner wrapper that returns its underlying byte[] to <see cref="ArrayPool{T}.Shared"/>
        /// and itself to the owning pool's wrapper queue on <see cref="Dispose"/>.
        /// </summary>
        internal sealed class PooledBuffer : IMemoryOwner<byte>
        {
            private readonly PooledArrayMemoryPool owner;
            private byte[] array;

            internal PooledBuffer(PooledArrayMemoryPool owner) => this.owner = owner;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Init(int minSize) => array = ArrayPool<byte>.Shared.Rent(minSize);

            public Memory<byte> Memory
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    var local = array;
                    if (local == null)
                        ThrowObjectDisposed();
                    return local;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose()
            {
                var local = Interlocked.Exchange(ref array, null);
                if (local != null)
                {
                    ArrayPool<byte>.Shared.Return(local);
                    // Recycle the wrapper itself. Must happen after the byte[] is back in
                    // ArrayPool so this wrapper holds no resources by the time it is enqueued.
                    owner.OnPooledBufferDisposed(this);
                }
            }

            [MethodImpl(MethodImplOptions.NoInlining)]
            private static void ThrowObjectDisposed()
                => throw new ObjectDisposedException(nameof(PooledBuffer));
        }
    }
}