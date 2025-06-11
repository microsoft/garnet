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

        // Max size of previously allocated unused buffer that we can reuse as necessary
        readonly int backupScratchBufferMaxSize;

        // A previously allocated unused buffer that we can reuse as necessary
        ScratchBuffer backupScratchBuffer;

        // Total offset of buffers in the stack
        int prevScratchBuffersOffset;

        // Total offset of buffers in the stack
        int totalLength;

        /// <summary>
        /// Combined offset in all managed scratch buffers
        /// </summary>
        internal int ScratchBufferOffset => prevScratchBuffersOffset + currScratchBuffer.scratchBufferOffset;

        /// <summary>
        /// Total length of all currently managed buffers (including backup buffer)
        /// </summary>
        internal int TotalLength => totalLength;

        /// <summary>
        /// Creates an instance of <see cref="ScratchAllocationManager"/>
        /// </summary>
        /// <param name="backupScratchBufferMaxSize">Max size of previously allocated unused buffer to keep for future use
        /// (Default: -1 to not use a backup buffer)</param>
        public ScratchAllocationManager(int backupScratchBufferMaxSize = -1)
        {
            this.backupScratchBufferMaxSize = backupScratchBufferMaxSize;
        }

        /// <summary>
        /// Reset all scratch buffers managed by the <see cref="ScratchAllocationManager"/>.
        /// Loses all <see cref="ArgSlice"/> instances created on the scratch buffers.
        /// </summary>
        public void Reset()
        {
            if (currScratchBuffer.IsDefault) return;

            // Invalidate the current buffer
            currScratchBuffer.scratchBufferOffset = 0;

            if (previousScratchBuffers.Count > 0)
            {
                totalLength -= currScratchBuffer.Length;

                // Save the current buffer for future use, if needed.
                SaveCurrentBufferAsBackupIfNeeded();

                // Pop and reset any previous buffers
                while (previousScratchBuffers.Count > 0)
                {
                    ref var prevBuffer = ref previousScratchBuffers.Pop();
                    prevScratchBuffersOffset -= prevBuffer.scratchBufferOffset;

                    // If we've reached the last buffer in the stack, we'll set it as the current.
                    if (previousScratchBuffers.Count == 0)
                    {
                        currScratchBuffer = prevBuffer;
                        currScratchBuffer.scratchBufferOffset = 0;
                    }
                    else
                    {
                        totalLength -= prevBuffer.Length;
                    }
                }
            }

            Debug.Assert(ScratchBufferOffset == 0);
            Debug.Assert(TotalLength == currScratchBuffer.Length + (backupScratchBuffer.IsDefault ? 0 : backupScratchBuffer.Length));
        }

        /// <summary>
        /// Rewind (pop) the last entry of the current scratch buffer (rewinding the current scratch buffer offset),
        /// if it contains the given <see cref="ArgSlice"/>
        /// </summary>
        /// <param name="slice">The <see cref="ArgSlice"/> to rewind</param>
        /// <returns>True if successful</returns>
        public bool RewindScratchBuffer(ref ArgSlice slice)
        {
            if (slice.ptr + slice.Length == currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset)
            {
                currScratchBuffer.scratchBufferOffset -= slice.Length;
                slice = default; // Invalidate the given ArgSlice

                // If the current buffer is now empty, try to pop a previous buffer
                if (currScratchBuffer.scratchBufferOffset == 0 && previousScratchBuffers.Count > 0)
                {
                    totalLength -= currScratchBuffer.Length;

                    // Save the current buffer for future use, if needed.
                    SaveCurrentBufferAsBackupIfNeeded();

                    // Pop the previous buffer and set it as the current buffer
                    ref var prevBuffer = ref previousScratchBuffers.Pop();
                    prevScratchBuffersOffset -= prevBuffer.scratchBufferOffset;
                    currScratchBuffer = prevBuffer;
                }

                return true;
            }

            return false;
        }

        /// <summary>
        /// Create an <see cref="ArgSlice"/> from the given ReadOnlySpan
        /// </summary>
        /// <param name="bytes">Input bytes</param>
        /// <returns>Created <see cref="ArgSlice"/></returns>
        public ArgSlice CreateArgSlice(ReadOnlySpan<byte> bytes)
        {
            ExpandScratchBufferIfNeeded(bytes.Length);

            var retVal = new ArgSlice(currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset, bytes.Length);
            bytes.CopyTo(retVal.Span);

            currScratchBuffer.scratchBufferOffset += bytes.Length;

            return retVal;
        }


        /// <summary>
        /// Create an <see cref="ArgSlice"/> in UTF8 format from the given string
        /// </summary>
        /// <param name="str">Input string</param>
        /// <returns>Created <see cref="ArgSlice"/></returns>
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
        /// Create an <see cref="ArgSlice"/> of specified length, leaves contents as is
        /// </summary>
        /// <param name="length">Length of slice</param>
        /// <returns>Created <see cref="ArgSlice"/></returns>
        public ArgSlice CreateArgSlice(int length)
        {
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset, length);
            currScratchBuffer.scratchBufferOffset += length;
            Debug.Assert(currScratchBuffer.scratchBufferOffset <= currScratchBuffer.Length);
            return retVal;
        }

        void SaveCurrentBufferAsBackupIfNeeded()
        {
            if (backupScratchBufferMaxSize != -1 && currScratchBuffer.Length <= backupScratchBufferMaxSize &&
                (backupScratchBuffer.IsDefault || backupScratchBuffer.Length < currScratchBuffer.Length))
            {
                totalLength += backupScratchBuffer.IsDefault
                    ? currScratchBuffer.Length
                    : (backupScratchBuffer.Length - currScratchBuffer.Length);

                backupScratchBuffer = currScratchBuffer;
            }
        }

        void ExpandScratchBufferIfNeeded(int requiredLength)
        {
            if (currScratchBuffer.IsDefault || requiredLength > currScratchBuffer.Length - currScratchBuffer.scratchBufferOffset)
                ExpandScratchBuffer(requiredLength);
        }

        void ExpandScratchBuffer(int requiredLength)
        {
            ScratchBuffer newScratchBuffer;

            // First, see if we can use the backup scratch buffer, if exists and can fit the data
            if (!backupScratchBuffer.IsDefault && backupScratchBuffer.Length >= requiredLength)
            {
                newScratchBuffer = backupScratchBuffer;
                backupScratchBuffer = default;
            }
            // If we cannot use the backup buffer, we'll allocate a new buffer
            else
            {
                // Length of new buffer is:
                // If there is no current buffer - the closest power of 2 to the data length that is at least 64
                // If there is a current buffer - the max between the closest power of 2 to the data length and twice the size of the current buffer.
                var newLength = currScratchBuffer.IsDefault
                    ? requiredLength < 64 ? 64 : (int)BitOperations.RoundUpToPowerOf2((uint)requiredLength + 1)
                    : (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(currScratchBuffer.Length, requiredLength) + 1);

                var newBuffer = GC.AllocateArray<byte>(newLength, true);
                var newBufferHead = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(newBuffer));

                newScratchBuffer = new ScratchBuffer
                {
                    scratchBuffer = newBuffer,
                    scratchBufferHead = newBufferHead
                };

                totalLength += newLength;
            }

            if (!currScratchBuffer.IsDefault)
            {
                // If current buffer is not empty, we'll push it to the stack
                if (currScratchBuffer.scratchBufferOffset > 0)
                {
                    previousScratchBuffers.Push(currScratchBuffer);
                    prevScratchBuffersOffset += currScratchBuffer.scratchBufferOffset;
                }
                // If current buffer is empty, we'll attempt to use it as a backup buffer
                else
                {
                    totalLength -= currScratchBuffer.Length;
                    SaveCurrentBufferAsBackupIfNeeded();
                }
            }

            currScratchBuffer = newScratchBuffer;
        }
    }
}