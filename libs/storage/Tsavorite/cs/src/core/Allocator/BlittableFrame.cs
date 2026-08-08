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

        // Owns the lifetime of direct-VM frame blocks (Frames "full" mode); frees them at finalization (not
        // Dispose) so an in-flight device read into a frame is never unmapped underneath it. Null for managed.
        NativePageBlockRegistry nativeRegistry;

        public BlittableFrame(int frameSize, int pageSize, int sectorSize)
        {
            this.frameSize = frameSize;
            this.pageSize = pageSize;
            this.sectorSize = sectorSize;

            frame = new byte[frameSize][];
            pointers = new long[frameSize];
        }

        public unsafe void Allocate(int index)
        {
            var adjustedSize = pageSize + 2 * sectorSize;

            if ((NativeAllocatorInitializer.EnabledSurfaces & NativeAllocatorSurfaces.Frames) != 0)
            {
                var block = DirectVirtualMemory.Allocate(adjustedSize, sectorSize);
                (nativeRegistry ??= new NativePageBlockRegistry()).Register(block);
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
            // Direct-VM frame blocks are freed by nativeRegistry at finalization (not here): an in-flight device
            // read may still reference a frame, and the device outlives this Dispose. This matches the managed
            // frame lifetime (GC-reclaimed after the owner and its device are gone).
        }
    }
}
