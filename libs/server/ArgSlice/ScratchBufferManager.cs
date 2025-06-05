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
    internal unsafe struct ScratchBuffer
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

        internal int Length => scratchBuffer.Length;

        internal bool IsDefault => scratchBuffer == null;
    }

    /// <summary>
    /// Utils for scratch buffer management - one per session (single threaded access)
    /// </summary>
    internal sealed unsafe class ScratchBufferManager
    {
        ScratchBuffer currScratchBuffer;

        readonly RefStack<ScratchBuffer> previousScratchBuffers = new();

        int prevScratchBuffersOffset = 0;

        /// <summary>
        /// Combined offset in all live scratch buffers
        /// </summary>
        internal int ScratchBufferOffset => prevScratchBuffersOffset + currScratchBuffer.scratchBufferOffset;

        public ScratchBufferManager()
        {
        }

        /// <summary>
        /// Reset scratch buffer - loses all ArgSlice instances created on the scratch buffer
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
        /// Return the full buffer managed by this <see cref="ScratchBufferManager"/>.
        /// </summary>
        public Span<byte> CurrentFullBuffer() => currScratchBuffer.scratchBuffer;

        /// <summary>
        /// Rewind (pop) the last entry of scratch buffer (rewinding the current scratch buffer offset),
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
        /// Resets scratch buffer offset to the specified offset.
        /// </summary>
        /// <param name="offset">Offset to reset to</param>
        /// <returns>True if successful, else false</returns>
        public bool ResetScratchBuffer(int offset)
        {
            if (offset < 0 || offset > ScratchBufferOffset)
                return false;

            while (prevScratchBuffersOffset > offset)
            {
                currScratchBuffer = previousScratchBuffers.Pop();
                prevScratchBuffersOffset -= currScratchBuffer.scratchBufferOffset;
            }

            currScratchBuffer.scratchBufferOffset = offset - prevScratchBuffersOffset;
            return true;
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
        /// Shift the scratch buffer offset
        /// </summary>
        /// <param name="length"></param>
        public void MoveOffset(int length)
        {
            currScratchBuffer.scratchBufferOffset += length;
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

        public ReadOnlySpan<byte> UTF8EncodeString(string str)
        {
            // We'll always need AT LEAST this many bytes
            ExpandScratchBufferIfNeeded(str.Length);

            var space = CurrentFullBuffer()[currScratchBuffer.scratchBufferOffset..];

            // Attempt to fit in the existing buffer first
            if (!Encoding.UTF8.TryGetBytes(str, space, out var written))
            {
                // If that fails, figure out exactly how much space we need
                var neededBytes = Encoding.UTF8.GetByteCount(str);
                ExpandScratchBufferIfNeeded(neededBytes);

                space = CurrentFullBuffer()[currScratchBuffer.scratchBufferOffset..];
                written = Encoding.UTF8.GetBytes(str, space);
            }

            return space[..written];
        }

        /// <summary>
        /// Create an ArgSlice that includes a header of specified size, followed by RESP Bulk-String formatted versions of the specified ArgSlice values (arg1 and arg2)
        /// </summary>
        public ArgSlice FormatScratchAsResp(int headerSize, ArgSlice arg1, ArgSlice arg2)
        {
            var length = headerSize + GetRespFormattedStringLength(arg1) + GetRespFormattedStringLength(arg2);
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset, length);
            retVal.Span[..headerSize].Clear(); // Clear the header

            var ptr = currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset + headerSize;
            var success = RespWriteUtils.TryWriteBulkString(arg1.Span, ref ptr, currScratchBuffer.scratchBufferHead + currScratchBuffer.Length);
            Debug.Assert(success);
            success = RespWriteUtils.TryWriteBulkString(arg2.Span, ref ptr, currScratchBuffer.scratchBufferHead + currScratchBuffer.Length);
            Debug.Assert(success);

            currScratchBuffer.scratchBufferOffset += length;
            Debug.Assert(currScratchBuffer.scratchBufferOffset <= currScratchBuffer.Length);
            return retVal;
        }

        /// <summary>
        /// Create an ArgSlice that includes a header of specified size, followed by RESP Bulk-String formatted versions of the specified ArgSlice value arg1
        /// </summary>
        public ArgSlice FormatScratchAsResp(int headerSize, ArgSlice arg1)
        {
            var length = headerSize + GetRespFormattedStringLength(arg1);
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset, length);
            retVal.Span[..headerSize].Clear(); // Clear the header

            var ptr = currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset + headerSize;
            var success = RespWriteUtils.TryWriteBulkString(arg1.Span, ref ptr, currScratchBuffer.scratchBufferHead + currScratchBuffer.Length);
            Debug.Assert(success);

            currScratchBuffer.scratchBufferOffset += length;
            Debug.Assert(currScratchBuffer.scratchBufferOffset <= currScratchBuffer.Length);
            return retVal;
        }

        /// <summary>
        /// Create an ArgSlice that includes a header of specified size, followed by the specified ArgSlice (arg)
        /// </summary>
        public ArgSlice FormatScratch(int headerSize, ArgSlice arg)
        {
            var length = headerSize + arg.Length;
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset, length);
            retVal.Span[..headerSize].Clear(); // Clear the header

            byte* ptr = currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset + headerSize;
            arg.ReadOnlySpan.CopyTo(new Span<byte>(ptr, arg.Length));

            currScratchBuffer.scratchBufferOffset += length;
            Debug.Assert(currScratchBuffer.scratchBufferOffset <= currScratchBuffer.Length);
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

        /// <summary>
        /// View remaining scratch space (of specified minimum length) as an ArgSlice
        /// Does NOT move the offset forward
        /// </summary>
        /// <returns></returns>
        public ArgSlice ViewRemainingArgSlice(int minLength = 0)
        {
            ExpandScratchBufferIfNeeded(minLength);

            return new ArgSlice(currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset,
                currScratchBuffer.Length - currScratchBuffer.scratchBufferOffset);
        }

        public ArgSlice ViewFullArgSlice()
        {
            return new ArgSlice(currScratchBuffer.scratchBufferHead, currScratchBuffer.scratchBufferOffset);
        }

        /// <summary>
        /// Create an ArgSlice that includes a header of specified size, followed by the specified Memory
        /// </summary>
        public ArgSlice FormatScratch(int headerSize, ReadOnlySpan<byte> arg)
        {
            var length = headerSize + arg.Length;
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset, length);
            retVal.Span[..headerSize].Clear(); // Clear the header

            var ptr = currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset + headerSize;
            arg.CopyTo(new Span<byte>(ptr, arg.Length));

            currScratchBuffer.scratchBufferOffset += length;
            Debug.Assert(currScratchBuffer.scratchBufferOffset <= currScratchBuffer.Length);
            return retVal;
        }

        /// <summary>
        /// Start a RESP array to hold a command and arguments.
        /// 
        /// Fill it with <paramref name="argCount"/> calls to <see cref="WriteNullArgument"/> and/or <see cref="WriteArgument(ReadOnlySpan{byte})"/>.
        /// </summary>
        public void StartCommand(ReadOnlySpan<byte> cmd, int argCount)
        {
            if (currScratchBuffer.IsDefault)
                ExpandScratchBuffer(64);

            var ptr = currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset;

            while (!RespWriteUtils.TryWriteArrayLength(argCount + 1, ref ptr, currScratchBuffer.scratchBufferHead + currScratchBuffer.Length))
            {
                ExpandScratchBuffer(currScratchBuffer.Length + 1);
                ptr = currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset;
            }
            currScratchBuffer.scratchBufferOffset = (int)(ptr - currScratchBuffer.scratchBufferHead);

            while (!RespWriteUtils.TryWriteBulkString(cmd, ref ptr, currScratchBuffer.scratchBufferHead + currScratchBuffer.Length))
            {
                ExpandScratchBuffer(currScratchBuffer.Length + 1);
                ptr = currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset;
            }
            currScratchBuffer.scratchBufferOffset = (int)(ptr - currScratchBuffer.scratchBufferHead);
        }

        /// <summary>
        /// Use to fill a RESP array with arguments after a call to <see cref="StartCommand(ReadOnlySpan{byte}, int)"/>.
        /// </summary>
        public void WriteNullArgument()
        {
            var ptr = currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset;

            while (!RespWriteUtils.TryWriteNull(ref ptr, currScratchBuffer.scratchBufferHead + currScratchBuffer.Length))
            {
                ExpandScratchBuffer(currScratchBuffer.Length + 1);
                ptr = currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset;
            }

            currScratchBuffer.scratchBufferOffset = (int)(ptr - currScratchBuffer.scratchBufferHead);
        }

        /// <summary>
        /// Use to fill a RESP array with arguments after a call to <see cref="StartCommand(ReadOnlySpan{byte}, int)"/>.
        /// </summary>
        public void WriteArgument(ReadOnlySpan<byte> arg)
        {
            var ptr = currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset;

            while (!RespWriteUtils.TryWriteBulkString(arg, ref ptr, currScratchBuffer.scratchBufferHead + currScratchBuffer.Length))
            {
                ExpandScratchBuffer(currScratchBuffer.Length + 1);
                ptr = currScratchBuffer.scratchBufferHead + currScratchBuffer.scratchBufferOffset;
            }

            currScratchBuffer.scratchBufferOffset = (int)(ptr - currScratchBuffer.scratchBufferHead);
        }

        /// <summary>
        /// Get length of a RESP Bulk-String formatted version of the specified ArgSlice
        /// RESP format: $[size]\r\n[value]\r\n
        /// Total size: 1 + [number of digits in the size value] + 2 + [size of value] + 2
        /// </summary>
        /// <param name="slice"></param>
        /// <returns></returns>
        static int GetRespFormattedStringLength(ArgSlice slice)
            => 1 + NumUtils.CountDigits(slice.Length) + 2 + slice.Length + 2;

        void ExpandScratchBufferIfNeeded(int newLength)
        {
            if (currScratchBuffer.IsDefault || newLength > currScratchBuffer.Length - currScratchBuffer.scratchBufferOffset)
                ExpandScratchBuffer(newLength);
        }

        void ExpandScratchBuffer(int newLength)
        {
            if (newLength < 64) newLength = 64;
            else newLength = (int)BitOperations.RoundUpToPowerOf2((uint)newLength + 1);

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

        /// <summary>
        /// Force backing buffer to grow.
        /// <paramref name="copyLengthOverride"/> provides a way to force a chunk at the start of the
        /// previous buffer be copied into the new buffer, even if this <see cref="ScratchBufferManager"/>
        /// doesn't consider that chunk in use.
        /// </summary>
        public void GrowBuffer(int? copyLengthOverride = null)
        {
            if (currScratchBuffer.IsDefault)
            {
                ExpandScratchBuffer(64);
            }
            else
            {
                ExpandScratchBuffer(currScratchBuffer.Length + 1);
            }
        }
    }
}