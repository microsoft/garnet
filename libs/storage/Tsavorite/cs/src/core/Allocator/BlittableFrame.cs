// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    using static Utility;

    /// <summary>
    /// A frame is an in-memory circular buffer of log pages
    /// </summary>
    internal sealed class BlittableFrame : IDisposable
    {
        public readonly int frameSize, pageSize, sectorSize;
        public readonly byte[][] frame;
        public readonly long[] pointers;

        // Per-slot direct-VM backing block when the Frames native surface is enabled ("full" mode); the parallel
        // entry in <see cref="frame"/> is null. Null array for the managed backend. Freed deterministically in
        // <see cref="Dispose"/> — safe because every BlittableFrame owner is a scan iterator that drains all
        // outstanding device reads (ScanIteratorBase.Dispose) before calling frame.Dispose(), so no device IO
        // references a frame slot when it is unmapped.
        readonly DirectVmBlock[] blocks;

        public BlittableFrame(int frameSize, int pageSize, int sectorSize)
        {
            this.frameSize = frameSize;
            this.pageSize = pageSize;
            this.sectorSize = sectorSize;

            frame = new byte[frameSize][];
            pointers = new long[frameSize];
            if ((NativeAllocatorInitializer.EnabledSurfaces & NativeAllocatorSurfaces.Frames) != 0)
                blocks = new DirectVmBlock[frameSize];
        }

        /// <summary>Whether frame slot <paramref name="index"/> is already backed (managed pinned array or
        /// direct-VM block). Backend-neutral so slots are reused across pages instead of re-allocated.</summary>
        public bool IsAllocated(int index) => pointers[index] != 0;

        public unsafe void Allocate(int index)
        {
            var adjustedSize = pageSize + 2 * sectorSize;

            if (blocks is not null)
            {
                // Direct-VM (mmap/VirtualAlloc): demand-zero, first-touch-placed, matching GC.AllocateArray.
                var block = DirectVirtualMemory.Allocate(adjustedSize, sectorSize);
                blocks[index] = block;
                pointers[index] = block.AlignedPtr;
                frame[index] = null;
                return;
            }

            var tmp = GC.AllocateArray<byte>(adjustedSize, pinned: true);
            var p = (long)Unsafe.AsPointer(ref tmp[0]);
            pointers[index] = RoundUp(p, sectorSize);
            frame[index] = tmp;
        }

        public void Clear(int pageIndex)
        {
            if (frame[pageIndex] is not null)
                Array.Clear(frame[pageIndex], 0, frame[pageIndex].Length);
            else
                DirectVirtualMemory.Clear((nint)pointers[pageIndex], pageSize + 2 * sectorSize);
        }

        public long GetPhysicalAddress(long frameNumber, long offset = 0)
        {
            return pointers[frameNumber % frameSize] + offset;
        }

        public unsafe (byte[] array, long offset) GetArrayAndUnalignedOffset(long frameNumber, long alignedOffset)
        {
            var frameIndex = frameNumber % frameSize;

            long ptr = (long)Unsafe.AsPointer(ref frame[frameIndex]);
            return (frame[frameIndex], alignedOffset + ptr - pointers[frameIndex]);
        }

        public void Dispose()
        {
            // Free native frame blocks deterministically: the owning scan iterator has already drained all
            // outstanding device reads (ScanIteratorBase.Dispose runs before this), so no IO references a slot.
            // Also clear pointers[] so that a post-Dispose reuse (ScanIteratorBase.Reset calls Dispose then keeps
            // the same frame) re-allocates via IsAllocated==false instead of dereferencing a freed block.
            // No-op for the managed backend (blocks is null; the pinned arrays are GC-reclaimed and a reused frame
            // legitimately keeps its still-live arrays).
            if (blocks is null)
                return;
            for (var i = 0; i < blocks.Length; i++)
            {
                DirectVirtualMemory.Free(blocks[i]);
                blocks[i] = default;
                pointers[i] = 0;
            }
        }
    }
}