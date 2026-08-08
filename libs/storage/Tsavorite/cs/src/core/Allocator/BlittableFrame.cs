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

        // Per-frame direct-VM backing block when the Frames native surface is enabled ("full" mode); the
        // parallel entry in <see cref="frame"/> is null. Empty (default) for the managed backend.
        readonly DirectVmBlock[] blocks;

        public BlittableFrame(int frameSize, int pageSize, int sectorSize)
        {
            this.frameSize = frameSize;
            this.pageSize = pageSize;
            this.sectorSize = sectorSize;

            frame = new byte[frameSize][];
            pointers = new long[frameSize];
            blocks = new DirectVmBlock[frameSize];
        }

        public unsafe void Allocate(int index)
        {
            var adjustedSize = pageSize + 2 * sectorSize;

            if ((NativeAllocatorInitializer.EnabledSurfaces & NativeAllocatorSurfaces.Frames) != 0)
            {
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
            if (blocks is null)
                return;
            for (var i = 0; i < blocks.Length; i++)
            {
                if (!blocks[i].IsEmpty)
                {
                    DirectVirtualMemory.Free(blocks[i]);
                    blocks[i] = default;
                }
            }
        }
    }
}