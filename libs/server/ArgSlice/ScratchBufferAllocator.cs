// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// <see cref="ScratchBufferAllocator"/> is responsible for allocating sufficient memory and copying data into a buffer
    /// and returning an <see cref="PinnedSpanByte"/> to the caller.
    /// Whenever the current buffer runs out of space, a new buffer is allocated, without copying the previous buffer data.
    /// The previous allocated buffers are kept rooted in a stack by the manager, so that each <see cref="PinnedSpanByte"/> that wasn't explicitly
    /// rewound is not going to be GCed.
    ///
    /// The manager is meant to be called from a single-threaded context (i.e. one manager per session).
    /// Each call to CreateArgSlice will copy the data to the current or new buffer that could contain the data in its entirety,
    /// so rewinding the <see cref="PinnedSpanByte"/> (i.e. releasing the memory) should be called in reverse order to assignment.
    /// 
    /// Note: Use <see cref="ScratchBufferBuilder"/> if you need all data to remain in a continuous chunk of memory (which is not promised by
    /// <see cref="ScratchBufferAllocator"/>) and you do not need to reuse previously returned <see cref="PinnedSpanByte"/> structs
    /// (as consequent allocations may cause them to point to GCed areas in memory).
    /// </summary>
    internal sealed unsafe class ScratchBufferAllocator
    {
        /// <summary>
        /// <see cref="ScratchBuffer"/> represents a buffer managed by <see cref="ScratchBufferAllocator"/>
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
            internal readonly int Length => scratchBuffer.Length;

            /// <summary>
            /// True if buffer was not yet allocated
            /// </summary>
            internal readonly bool IsDefault => scratchBuffer == null;

            /// <summary>
            /// Initializes the scratch buffer to a specified length
            /// </summary>
            /// <param name="length">The length of the buffer</param>
            internal void Initialize(int length)
            {
                scratchBuffer = GC.AllocateArray<byte>(length, true);
                scratchBufferHead = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(scratchBuffer));
                scratchBufferOffset = 0;
            }
        }

        ScratchBuffer currScratchBuffer;

        readonly SimpleStack<ScratchBuffer> previousScratchBuffers = new();

        // Max size of previously allocated unused buffer that we can reuse upon reset
        readonly int maxInitialCapacity;

        // Min size that can be allocated for a single buffer
        readonly int minSizeBuffer;

        // Total offset of buffers in the stack
        int prevScratchBuffersOffset;

        // Total offset of buffers in the stack
        int totalLength;

        /// <summary>
        /// Combined offset in all managed scratch buffers
        /// </summary>
        internal int ScratchBufferOffset => prevScratchBuffersOffset + currScratchBuffer.scratchBufferOffset;

        /// <summary>
        /// Total length of all currently managed buffers
        /// </summary>
        internal int TotalLength => totalLength;

        /// <summary>
        /// Creates an instance of <see cref="ScratchBufferAllocator"/>
        /// </summary>
        /// <param name="minSizeBuffer">Min size that can be allocated for a single buffer (Default: 2)</param>
        /// <param name="maxInitialCapacity">Max size of previously allocated unused buffer to keep upon reset (Default: no limit)</param>
        public ScratchBufferAllocator(int minSizeBuffer = 2, int maxInitialCapacity = int.MaxValue)
        {
            this.minSizeBuffer = minSizeBuffer;
            this.maxInitialCapacity = maxInitialCapacity;
        }

        /// <summary>
        /// Reset all scratch buffers managed by the <see cref="ScratchBufferAllocator"/>.
        /// Loses all <see cref="PinnedSpanByte"/>s created on the scratch buffers.
        /// </summary>
        public void Reset()
        {
            if (currScratchBuffer.IsDefault) return;

            // Invalidate the current buffer
            currScratchBuffer.scratchBufferOffset = 0;

            var isCurrBufferSet = false;

            // If max capacity is not set or the current buffer is under the max capacity - 
            // we keep it as the current buffer
            if (currScratchBuffer.Length <= maxInitialCapacity)
            {
                isCurrBufferSet = true;
            }
            else
            {
                totalLength -= currScratchBuffer.Length;
            }

            // Pop and reset any previous buffers
            while (previousScratchBuffers.Count > 0)
            {
                var prevBuffer = previousScratchBuffers.Pop();
                prevScratchBuffersOffset -= prevBuffer.scratchBufferOffset;

                // Check if we need to set this scratch buffer as the current
                // i.e. if this is the largest buffer under the max capacity limit
                if (!isCurrBufferSet && prevBuffer.Length <= maxInitialCapacity)
                {
                    currScratchBuffer = prevBuffer;
                    currScratchBuffer.scratchBufferOffset = 0;
                    isCurrBufferSet = true;
                }
                else
                {
                    totalLength -= prevBuffer.Length;
                }
            }

            if (!isCurrBufferSet)
                currScratchBuffer = default;

            Debug.Assert(currScratchBuffer.IsDefault || ScratchBufferOffset == 0);
            Debug.Assert(TotalLength == (currScratchBuffer.IsDefault ? 0 : currScratchBuffer.Length));
        }

        /// <summary>
        /// Rewind (pop) the last entry of the current scratch buffer (rewinding the current scratch buffer offset),
        /// if it contains the given <see cref="PinnedSpanByte"/>
        /// </summary>
        /// <param name="slice">The <see cref="PinnedSpanByte"/> to rewind</param>
        /// <returns>True if successful</returns>
        public bool RewindScratchBuffer(ref PinnedSpanByte slice)
        {
            if (currScratchBuffer.IsDefault) return false;

            // If the current buffer is empty, try to pop a previous buffer
            if (currScratchBuffer.scratchBufferOffset == 0)
            {
                // No previous buffers to pop
                if (previousScratchBuffers.Count == 0)
                    return false;

                totalLength -= currScratchBuffer.Length;

                // Pop the previous buffer and set it as the current buffer
                var prevBuffer = previousScratchBuffers.Pop();
                prevScratchBuffersOffset -= prevBuffer.scratchBufferOffset;
                currScratchBuffer = prevBuffer;
            }

            if (slice.ptr + slice.Length == currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset)
            {
                currScratchBuffer.scratchBufferOffset -= slice.Length;
                slice = default; // Invalidate the given ArgSlice

                // If this buffer is now empty, and it is the only buffer remaining -
                // If the buffer is over the max initial capacity we get rid of it (similarly to Reset)
                if (currScratchBuffer.scratchBufferOffset == 0 &&
                    previousScratchBuffers.Count == 0 &&
                    currScratchBuffer.Length > maxInitialCapacity)
                {
                    totalLength -= currScratchBuffer.Length;
                    currScratchBuffer = default;
                }

                return true;
            }

            return false;
        }

        /// <summary>
        /// Create an <see cref="PinnedSpanByte"/> from the given ReadOnlySpan
        /// </summary>
        /// <param name="bytes">Input bytes</param>
        /// <returns>Created <see cref="PinnedSpanByte"/></returns>
        public PinnedSpanByte CreateArgSlice(ReadOnlySpan<byte> bytes)
        {
            ExpandScratchBufferIfNeeded(bytes.Length);

            var retVal = PinnedSpanByte.FromPinnedPointer(currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset, bytes.Length);
            bytes.CopyTo(retVal.Span);

            currScratchBuffer.scratchBufferOffset += bytes.Length;

            return retVal;
        }

        /// <summary>
        /// Create an <see cref="PinnedSpanByte"/> in UTF8 format from the given string
        /// </summary>
        /// <param name="str">Input string</param>
        /// <returns>Created <see cref="PinnedSpanByte"/></returns>
        public PinnedSpanByte CreateArgSlice(string str)
        {
            var length = Encoding.UTF8.GetByteCount(str);
            ExpandScratchBufferIfNeeded(length);

            var retVal = PinnedSpanByte.FromPinnedPointer(currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset, length);
            _ = Encoding.UTF8.GetBytes(str, retVal.Span);
            currScratchBuffer.scratchBufferOffset += length;

            return retVal;
        }

        /// <summary>
        /// Create an <see cref="PinnedSpanByte"/> of specified length, leaves contents as is
        /// </summary>
        /// <param name="length">Length of slice</param>
        /// <returns>Created <see cref="PinnedSpanByte"/></returns>
        public PinnedSpanByte CreateArgSlice(int length)
        {
            ExpandScratchBufferIfNeeded(length);

            var retVal = PinnedSpanByte.FromPinnedPointer(currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset, length);
            currScratchBuffer.scratchBufferOffset += length;
            Debug.Assert(currScratchBuffer.scratchBufferOffset <= currScratchBuffer.Length);
            return retVal;
        }

        void ExpandScratchBufferIfNeeded(int requiredLength)
        {
            if (currScratchBuffer.IsDefault || requiredLength > currScratchBuffer.Length - currScratchBuffer.scratchBufferOffset)
                ExpandScratchBuffer(requiredLength);
        }

        void ExpandScratchBuffer(int requiredLength)
        {
            var currLength = currScratchBuffer.IsDefault ? 0 : currScratchBuffer.Length;

            ScratchBuffer newScratchBuffer = default;
            InitializeScratchBuffer(ref newScratchBuffer, requiredLength: Math.Max(minSizeBuffer, requiredLength),
                currentLength: currLength);

            totalLength += newScratchBuffer.Length;

            if (!currScratchBuffer.IsDefault)
            {
                // If current buffer is not empty, we'll push it to the stack
                if (currScratchBuffer.scratchBufferOffset > 0)
                {
                    previousScratchBuffers.Push(currScratchBuffer);
                    prevScratchBuffersOffset += currScratchBuffer.scratchBufferOffset;
                }
                // If current buffer is empty, we'll just get rid of it
                else
                {
                    totalLength -= currScratchBuffer.Length;
                }
            }

            currScratchBuffer = newScratchBuffer;
        }

        private static void InitializeScratchBuffer(ref ScratchBuffer buffer, int requiredLength, int currentLength = 0)
        {
            // Length of new buffer is:
            // If there is no current buffer - the closest power of 2 to the data length
            // If there is a current buffer - the max between the closest power of 2 to the data length and twice the size of the current buffer.
            var newLength = currentLength == 0
                ? (int)BitOperations.RoundUpToPowerOf2((uint)requiredLength + 1)
                : (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(currentLength, requiredLength) + 1);

            buffer.Initialize(newLength);
        }
    }
}