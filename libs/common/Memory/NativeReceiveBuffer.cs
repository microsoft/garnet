// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using Tsavorite.core;

namespace Garnet.common
{
    /// <summary>
    /// The native memory behind a network receive buffer while the buffer is larger than <see cref="LimitedFixedBufferPool"/>'s largest size class.
    /// <para>
    /// Such a buffer is sized for one large message and released once the message is consumed. As a pinned array it would sit in the pinned object
    /// heap, which is never compacted, as garbage until a full collection, and leave behind holes that later pinned arrays of other sizes cannot use.
    /// Native memory is returned to the operating system as soon as it is released, and is resident only where it has been written.
    /// </para>
    /// <para>
    /// A network handler keeps one for each of its receive buffers and reuses it: growing the buffer maps a larger region and unmaps the one it
    /// replaces, and shrinking the buffer back into the pool's size classes unmaps it, so neither allocates anything managed. It is the
    /// <see cref="MemoryManager{T}"/> through which socket receives and <c>SslStream</c> reads, which take <see cref="Memory{T}"/>, write the
    /// buffer. The thread receiving into the buffer maps, installs and unmaps regions; disposal of the handler can run on another thread, so state
    /// changes take a lock on this object, which is only ever contended by that race.
    /// </para>
    /// </summary>
    internal sealed unsafe class NativeReceiveBuffer : MemoryManager<byte>
    {
        readonly LimitedFixedBufferPool pool;

        /// <summary>The installed region, or the empty block.</summary>
        DirectVmBlock block;
        int length;

        /// <summary>Whether <see cref="pool"/> accounts for the installed region.</summary>
        bool accounted;

        /// <summary>Bytes reported with <see cref="GC.AddMemoryPressure"/> for the installed region.</summary>
        int reportedToGC;

        internal NativeReceiveBuffer(LimitedFixedBufferPool pool) => this.pool = pool;

        /// <summary>
        /// Map a region of <paramref name="size"/> bytes, accounted to the pool, to <see cref="Install"/> once the buffered bytes are copied into it.
        /// </summary>
        internal DirectVmBlock Map(int size)
        {
            if (!pool.TryAcquireNative(size))
                throw new ObjectDisposedException(nameof(LimitedFixedBufferPool));
            try
            {
                return DirectVirtualMemory.Allocate(size, alignment: 64);
            }
            catch
            {
                pool.ReleaseNative(size);
                throw;
            }
        }

        /// <summary>
        /// Install a region from <see cref="Map"/> of <paramref name="size"/> bytes, unmapping the one it replaces. Only call when no thread can
        /// still access the replaced region.
        /// </summary>
        internal void Install(in DirectVmBlock region, int size)
        {
            lock (this)
            {
                UnmapInstalled();
                block = region;
                length = size;
                accounted = true;
            }
        }

        /// <summary>
        /// Unmap the installed region, if any, as the buffer moves back into the pool's size classes. Only call when no thread can still access it.
        /// </summary>
        internal void Unmap()
        {
            lock (this)
                UnmapInstalled();
        }

        /// <summary>
        /// Release the installed region to the pool, but unmap it only once this object is unreachable, reporting it to the GC until then. For
        /// disposal, where another thread may still be reading or writing the region through a reference that keeps this object alive.
        /// </summary>
        internal void ReleaseDeferred()
        {
            lock (this)
            {
                if (accounted)
                {
                    pool.ReleaseNative(length);
                    accounted = false;
                }

                // Only a collection reclaims the region from here, so report it to the GC: a buildup of such regions then prompts one.
                if (block.BasePtr != 0 && reportedToGC == 0)
                {
                    GC.AddMemoryPressure(length);
                    reportedToGC = length;
                }
            }
        }

        void UnmapInstalled()
        {
            if (accounted)
                pool.ReleaseNative(length);
            DirectVirtualMemory.Free(block);
            if (reportedToGC != 0)
                GC.RemoveMemoryPressure(reportedToGC);
            block = default;
            length = 0;
            accounted = false;
            reportedToGC = 0;
        }

        /// <summary>
        /// Unmaps a region left installed by <see cref="ReleaseDeferred"/>.
        /// </summary>
        /// <remarks>
        /// CA2015 warns that a span over the memory does not keep this object alive. Every user of the region holds this object, a
        /// <see cref="Memory{T}"/> over it, or a handle from <see cref="Pin"/> while it uses the region: a socket receive holds its buffer, an
        /// <c>SslStream</c> read holds its destination, and the network handler that owns the buffer references this object.
        /// </remarks>
#pragma warning disable CA2015
        ~NativeReceiveBuffer()
        {
            DirectVirtualMemory.Free(block);
            if (reportedToGC != 0)
                GC.RemoveMemoryPressure(reportedToGC);
        }
#pragma warning restore CA2015

        /// <inheritdoc />
        public override Span<byte> GetSpan() => new((void*)block.AlignedPtr, length);

        /// <inheritdoc />
        public override MemoryHandle Pin(int elementIndex = 0) => new((byte*)block.AlignedPtr + elementIndex, pinnable: this);

        /// <inheritdoc />
        public override void Unpin() { }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Unmap();
        }
    }
}