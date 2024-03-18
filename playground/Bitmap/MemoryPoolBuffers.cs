// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;

namespace Bitmap
{
    public unsafe class MemoryPoolBuffers
    {
        private IMemoryOwner<byte> memoryBuffer = null;
        private MemoryHandle memoryBufferHandle;

        public MemoryPoolBuffers(int numRecords)
        {
            memoryBuffer = MemoryPool<byte>.Shared.Rent(numRecords);
            memoryBufferHandle = memoryBuffer.Memory.Pin();
        }

        public void Dispose()
        {
            if (memoryBuffer != null)
            {
                memoryBufferHandle.Dispose();
                memoryBuffer.Dispose();
            }
        }

        public byte* GetValidPointer()
        {
            return (byte*)memoryBufferHandle.Pointer;
        }


    }
}