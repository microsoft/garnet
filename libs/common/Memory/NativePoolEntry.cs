// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Threading;
using Tsavorite.core;

namespace Garnet.common
{
    /// <summary>
    /// A buffer that <see cref="LimitedFixedBufferPool"/> hands out above its largest size class, backed by demand-zero native memory rather than a
    /// pinned managed array.
    /// <para>
    /// Such a buffer is sized for one large message and released once the message is consumed. As a pinned array it would sit in the pinned object
    /// heap, which is never compacted, as garbage until a full collection, and leave behind holes that later pinned arrays of other sizes cannot use.
    /// Native memory is returned to the operating system as soon as it is released, and is resident only where it has been written.
    /// </para>
    /// </summary>
    internal sealed unsafe class NativePoolEntry : MemoryManager<byte>
    {
        /// <summary>
        /// Pointer to the buffer
        /// </summary>
        public readonly byte* entryPtr;

        /// <summary>
        /// Length of the buffer in bytes
        /// </summary>
        public readonly int length;

        readonly LimitedFixedBufferPool pool;
        readonly DirectVmBlock block;
        int returned;

        /// <summary>
        /// <see cref="ReportedToGC"/> and <see cref="Freed"/>, set atomically so that memory pressure reported to the GC is removed exactly once.
        /// </summary>
        int state;
        const int ReportedToGC = 1, Freed = 2;

        /// <summary>
        /// Packed source identifier, as for <see cref="PoolEntry"/>.
        /// </summary>
        internal int source;

        internal NativePoolEntry(int size, LimitedFixedBufferPool pool)
        {
            this.pool = pool;
            block = DirectVirtualMemory.Allocate(size, alignment: 64);
            entryPtr = (byte*)block.AlignedPtr;
            length = size;
        }

        /// <summary>
        /// Frees the memory of a buffer that was released with <see cref="ReleaseDeferred"/>.
        /// </summary>
        /// <remarks>
        /// CA2015 warns that a span over the memory does not keep this object alive. Every user of the buffer holds this object, a
        /// <see cref="Memory{T}"/> over it, or a handle from <see cref="Pin"/> while it uses the buffer: a socket receive holds its buffer, an
        /// <c>SslStream</c> read holds its destination, and the network handler that owns the buffer keeps referencing it after releasing it.
        /// </remarks>
#pragma warning disable CA2015
        ~NativePoolEntry() => FreeMemory();
#pragma warning restore CA2015

        /// <summary>
        /// Return the buffer to the pool and free its memory. Only call when no thread can still access the buffer.
        /// </summary>
        public void Release()
        {
            ReturnToPool();
            FreeMemory();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Return the buffer to the pool, but free its memory only once this object is unreachable, reporting it to the GC until then. For
        /// teardown paths where another thread may still be reading or writing the buffer through a reference that keeps this object alive.
        /// </summary>
        public void ReleaseDeferred()
        {
            ReturnToPool();

            // Only a collection reclaims the memory from here, so report it to the GC: a buildup of such buffers then prompts one. Reported
            // before the flag is set, and withdrawn if the memory was freed meanwhile or already reported, so it is removed exactly once.
            GC.AddMemoryPressure(length);
            if (Interlocked.CompareExchange(ref state, ReportedToGC, 0) != 0)
                GC.RemoveMemoryPressure(length);
        }

        void ReturnToPool()
        {
            if (Interlocked.Exchange(ref returned, 1) == 0)
                pool.ReturnNative(this);
        }

        void FreeMemory()
        {
            var previous = Interlocked.Or(ref state, Freed);
            if ((previous & Freed) != 0)
                return;
            DirectVirtualMemory.Free(block);
            if ((previous & ReportedToGC) != 0)
                GC.RemoveMemoryPressure(length);
        }

        /// <inheritdoc />
        public override Span<byte> GetSpan() => new(entryPtr, length);

        /// <inheritdoc />
        public override MemoryHandle Pin(int elementIndex = 0) => new(entryPtr + elementIndex, pinnable: this);

        /// <inheritdoc />
        public override void Unpin() { }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Release();
        }
    }
}