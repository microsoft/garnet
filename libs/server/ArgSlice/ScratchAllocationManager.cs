// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// <see cref="ScratchAllocationManager"/> is responsible for allocating and copying data into a buffer and returning an <see cref="ArgSlice"/>
    /// to the caller. Whenever the current buffer runs out of space, a new buffer is allocated, without copying the previous buffer data.
    /// The previous allocated buffers are kept rooted in a stack by the manager, so that each <see cref="ArgSlice"/> that wasn't explicitly
    /// re-winded is not going to be GCed.
    ///
    /// The manager is meant to be called from a single-threaded context (one per session).
    /// Each call to CreateArgSlice will copy the data to the current or new buffer that could contain the data in its entirety,
    /// so re-winding the <see cref="ArgSlice"/> (i.e. releasing the memory) should be called in reverse order to assignment.
    /// 
    /// Note: Use <see cref="ScratchBufferManager"/> if you need all data to remain in a continuous chunk of memory (which is not promised by
    /// <see cref="ScratchAllocationManager"/>) and you do not need to reuse previously returned <see cref="ArgSlice"/> structs
    /// (as consequent allocations may cause them to point to GCed areas in memory).
    /// </summary>
    internal sealed unsafe class ScratchAllocationManager
    {
        /// <summary>
        /// <see cref="ScratchBuffer"/> represents a buffer managed by <see cref="ScratchAllocationManager"/>
        /// </summary>
        private struct ScratchBuffer
        {
            /// <summary>
            /// Session-local scratch buffer to hold temporary arguments in transactions and GarnetApi
            /// </summary>
            internal byte[] scratchBuffer;

            /// <summary>
            /// Pointer to head of scratch buffer
            /// </summary>
            internal byte* scratchBufferHead;

            /// <summary>
            /// Current offset in scratch buffer
            /// </summary>
            internal int scratchBufferOffset;

            /// <summary>
            /// Length of the entire scratch buffer
            /// </summary>
            internal int Length => scratchBuffer.Length;

            /// <summary>
            /// True if buffer was not yet allocated
            /// </summary>
            internal bool IsDefault => scratchBuffer == null;
        }

        ScratchBuffer currScratchBuffer;

        readonly RefStack<ScratchBuffer> previousScratchBuffers = new();

        // Total offset of buffers in the stack
        int prevScratchBuffersOffset;

        /// <summary>
        /// Combined offset in all live scratch buffers
        /// </summary>
        internal int ScratchBufferOffset => prevScratchBuffersOffset + currScratchBuffer.scratchBufferOffset;

        /// <summary>
        /// Reset all scratch buffers - loses all ArgSlice instances created on the scratch buffers
        /// </summary>
        public void Reset()
        {
            currScratchBuffer.scratchBufferOffset = 0;

            while (previousScratchBuffers.Count > 0)
            {
                ref var buffer = ref previousScratchBuffers.Pop();
                prevScratchBuffersOffset -= buffer.scratchBufferOffset;
            }

            Debug.Assert(ScratchBufferOffset == 0);
        }

        /// <summary>
        /// Rewind (pop) the last entry of the current scratch buffer (rewinding the current scratch buffer offset),
        /// if it contains the given ArgSlice
        /// </summary>
        public bool RewindScratchBuffer(ref ArgSlice slice)
        {
            if (slice.ptr + slice.Length == currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset)
            {
                currScratchBuffer.scratchBufferOffset -= slice.Length;
                slice = default; // invalidate the given ArgSlice

                if (currScratchBuffer.scratchBufferOffset == 0 && previousScratchBuffers.Count > 0)
                {
                    ref var buffer = ref previousScratchBuffers.Pop();
                    prevScratchBuffersOffset -= buffer.scratchBufferOffset;
                    currScratchBuffer = buffer;
                }

                return true;
            }
            return false;
        }

        /// <summary>
        /// Create ArgSlice in scratch buffer, from given ReadOnlySpan
        /// </summary>
        public ArgSlice CreateArgSlice(ReadOnlySpan<byte> bytes)
        {
            ExpandScratchBufferIfNeeded(bytes.Length);

            var retVal = new ArgSlice(currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset, bytes.Length);
            bytes.CopyTo(retVal.Span);

            currScratchBuffer.scratchBufferOffset += bytes.Length;

            return retVal;
        }

        /// <summary>
        /// Create ArgSlice in UTF8 format in scratch buffer, from given string
        /// </summary>
        public ArgSlice CreateArgSlice(string str)
        {
            var length = Encoding.UTF8.GetByteCount(str);
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset, length);
            Encoding.UTF8.GetBytes(str, retVal.Span);
            currScratchBuffer.scratchBufferOffset += length;

            return retVal;
        }

        /// <summary>
        /// Create an ArgSlice of specified length, leaves contents as is
        /// </summary>
        public ArgSlice CreateArgSlice(int length)
        {
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset, length);
            currScratchBuffer.scratchBufferOffset += length;
            Debug.Assert(currScratchBuffer.scratchBufferOffset <= currScratchBuffer.Length);
            return retVal;
        }

        void ExpandScratchBufferIfNeeded(int newLength)
        {
            if (currScratchBuffer.IsDefault || newLength > currScratchBuffer.Length - currScratchBuffer.scratchBufferOffset)
                ExpandScratchBuffer(newLength);
        }

        void ExpandScratchBuffer(int newLength)
        {
            newLength = currScratchBuffer.IsDefault
                ? newLength < 64 ? 64 : (int)BitOperations.RoundUpToPowerOf2((uint)newLength + 1)
                : (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(currScratchBuffer.Length, newLength) + 1);

            var newBuffer = GC.AllocateArray<byte>(newLength, true);
            var newBufferHead = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(newBuffer));

            var newScratchBuffer = new ScratchBuffer
            {
                scratchBuffer = newBuffer,
                scratchBufferHead = newBufferHead
            };

            if (!currScratchBuffer.IsDefault)
            {
                previousScratchBuffers.Push(currScratchBuffer);
            }

            currScratchBuffer = newScratchBuffer;
        }
    }
}