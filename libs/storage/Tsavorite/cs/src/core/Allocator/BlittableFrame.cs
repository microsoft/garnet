// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// A frame is an in-memory circular buffer of log pages
    /// </summary>
    internal sealed class BlittableFrame : IDisposable
    {
        public readonly int frameSize, pageSize, sectorSize;
        public readonly byte[][] frame;
        public readonly long[] pointers;

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

            byte[] tmp = GC.AllocateArray<byte>(adjustedSize, true);
            long p = (long)Unsafe.AsPointer(ref tmp[0]);
            Array.Clear(tmp, 0, adjustedSize);
            pointers[index] = (p + (sectorSize - 1)) & ~((long)sectorSize - 1);
            frame[index] = tmp;
        }

        public void Clear(int pageIndex)
        {
            Array.Clear(frame[pageIndex], 0, frame[pageIndex].Length);
        }

        public long GetPhysicalAddress(long frameNumber, long offset)
        {
            return pointers[frameNumber % frameSize] + offset;
        }

        public void Dispose()
        {
        }
    }
}