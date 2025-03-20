// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    /// <summary>
    /// A frame is an in-memory circular buffer of log pages
    /// </summary>
    internal sealed unsafe class BlittableFrame : IDisposable
    {
        public readonly int frameSize, pageSize, sectorSize;
        public readonly byte** pointers;

        public BlittableFrame(int frameSize, int pageSize, int sectorSize)
        {
            this.frameSize = frameSize;
            this.pageSize = pageSize;
            this.sectorSize = sectorSize;

            pointers = (byte**)NativeMemory.AllocZeroed((uint)frameSize, (uint)sizeof(byte*));
        }

        public bool IsAllocated(int pageIndex) => pointers[pageIndex] != null;

        public void Allocate(int pageIndex)
        {
            Debug.Assert(pageIndex < frameSize);
            Debug.Assert(!IsAllocated(pageIndex));

            pointers[pageIndex] = (byte*)NativeMemory.AlignedAlloc((uint)pageSize, alignment: (uint)sectorSize);
            GC.AddMemoryPressure(pageSize);
            Clear(pageIndex);
        }

        public void Clear(int pageIndex)
        {
            Debug.Assert(pageIndex < frameSize);
            Debug.Assert(IsAllocated(pageIndex));

            NativeMemory.Clear(pointers[pageIndex], (uint)frameSize);
        }

        public long GetPhysicalAddress(long frameNumber, long offset)
        {
            return (long)pointers[frameNumber % frameSize] + offset;
        }

        public void Dispose()
        {
            for (var i = 0; i < frameSize; i++)
            {
                var pagePtr = pointers[i];
                if (pagePtr != null)
                {
                    NativeMemory.AlignedFree(pagePtr);
                    GC.RemoveMemoryPressure(frameSize);
                    pointers[i] = null;
                }
            }
        }
    }
}