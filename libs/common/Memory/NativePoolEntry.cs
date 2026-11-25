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
    public sealed unsafe class NativePoolEntry : MemoryManager<byte>
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
        int returned, freed;

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
        /// CA2015 warns that a span over the memory does not keep this object alive. Every user of the buffer holds this object, or a
        /// <see cref="Memory{T}"/> over it, while it uses the buffer: a socket receive holds its buffer, an <c>SslStream</c> read holds its
        /// destination, and the network handler that owns the buffer keeps referencing it after releasing it.
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
        /// Return the buffer to the pool, but free its memory only once this object is unreachable. For teardown paths where another thread may
        /// still be reading or writing the buffer through a reference that keeps this object alive.
        /// </summary>
        public void ReleaseDeferred() => ReturnToPool();

        void ReturnToPool()
        {
            if (Interlocked.Exchange(ref returned, 1) == 0)
                pool.ReturnNative(this);
        }

        void FreeMemory()
        {
            if (Interlocked.Exchange(ref freed, 1) == 0)
                DirectVirtualMemory.Free(block);
        }

        /// <inheritdoc />
        public override Span<byte> GetSpan() => new(entryPtr, length);

        /// <inheritdoc />
        public override MemoryHandle Pin(int elementIndex = 0) => new(entryPtr + elementIndex);

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