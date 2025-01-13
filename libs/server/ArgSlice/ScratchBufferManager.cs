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
    /// Utils for scratch buffer management - one per session (single threaded access)
    /// </summary>
    internal sealed unsafe class ScratchBufferManager
    {
        /// <summary>
        /// Session-local scratch buffer to hold temporary arguments in transactions and GarnetApi
        /// </summary>
        byte[] scratchBuffer;

        /// <summary>
        /// Pointer to head of scratch buffer
        /// </summary>
        byte* scratchBufferHead;

        /// <summary>
        /// Current offset in scratch buffer
        /// </summary>
        int scratchBufferOffset;

        /// <summary>Current offset in scratch buffer</summary>
        internal int ScratchBufferOffset => scratchBufferOffset;

        public ScratchBufferManager()
        {
        }

        /// <summary>
        /// Reset scratch buffer - loses all ArgSlice instances created on the scratch buffer
        /// </summary>
        public void Reset() => scratchBufferOffset = 0;

        /// <summary>
        /// Return the full buffer managed by this <see cref="ScratchBufferManager"/>.
        /// </summary>
        public Span<byte> FullBuffer()
        => scratchBuffer;

        /// <summary>
        /// Rewind (pop) the last entry of scratch buffer (rewinding the current scratch buffer offset),
        /// if it contains the given ArgSlice
        /// </summary>
        public bool RewindScratchBuffer(ref ArgSlice slice)
        {
            if (slice.ptr + slice.Length == scratchBufferHead + scratchBufferOffset)
            {
                scratchBufferOffset -= slice.Length;
                slice = default; // invalidate the given ArgSlice
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
            if (offset < 0 || offset > scratchBufferOffset)
                return false;

            scratchBufferOffset = offset;
            return true;
        }

        /// <summary>
        /// Create ArgSlice in scratch buffer, from given ReadOnlySpan
        /// </summary>
        public ArgSlice CreateArgSlice(ReadOnlySpan<byte> bytes)
        {
            ExpandScratchBufferIfNeeded(bytes.Length);

            var retVal = new ArgSlice(scratchBufferHead + scratchBufferOffset, bytes.Length);
            bytes.CopyTo(retVal.Span);
            scratchBufferOffset += bytes.Length;
            return retVal;
        }

        /// <summary>
        /// Shift the scratch buffer offset
        /// </summary>
        /// <param name="length"></param>
        public void MoveOffset(int length)
        {
            scratchBufferOffset += length;
        }

        /// <summary>
        /// Create ArgSlice in UTF8 format in scratch buffer, from given string
        /// </summary>
        public ArgSlice CreateArgSlice(string str)
        {
            int length = Encoding.UTF8.GetByteCount(str);
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(scratchBufferHead + scratchBufferOffset, length);
            Encoding.UTF8.GetBytes(str, retVal.Span);
            scratchBufferOffset += length;
            return retVal;
        }

        /// <summary>
        /// Create an ArgSlice that includes a header of specified size, followed by RESP Bulk-String formatted versions of the specified ArgSlice values (arg1 and arg2)
        /// </summary>
        public ArgSlice FormatScratchAsResp(int headerSize, ArgSlice arg1, ArgSlice arg2)
        {
            int length = headerSize + GetRespFormattedStringLength(arg1) + GetRespFormattedStringLength(arg2);
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(scratchBufferHead + scratchBufferOffset, length);
            retVal.Span[..headerSize].Clear(); // Clear the header

            byte* ptr = scratchBufferHead + scratchBufferOffset + headerSize;
            var success = RespWriteUtils.WriteBulkString(arg1.Span, ref ptr, scratchBufferHead + scratchBuffer.Length);
            Debug.Assert(success);
            success = RespWriteUtils.WriteBulkString(arg2.Span, ref ptr, scratchBufferHead + scratchBuffer.Length);
            Debug.Assert(success);

            scratchBufferOffset += length;
            Debug.Assert(scratchBufferOffset <= scratchBuffer.Length);
            return retVal;
        }

        /// <summary>
        /// Create an ArgSlice that includes a header of specified size, followed by RESP Bulk-String formatted versions of the specified ArgSlice value arg1
        /// </summary>
        public ArgSlice FormatScratchAsResp(int headerSize, ArgSlice arg1)
        {
            int length = headerSize + GetRespFormattedStringLength(arg1);
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(scratchBufferHead + scratchBufferOffset, length);
            retVal.Span[..headerSize].Clear(); // Clear the header

            byte* ptr = scratchBufferHead + scratchBufferOffset + headerSize;
            var success = RespWriteUtils.WriteBulkString(arg1.Span, ref ptr, scratchBufferHead + scratchBuffer.Length);
            Debug.Assert(success);

            scratchBufferOffset += length;
            Debug.Assert(scratchBufferOffset <= scratchBuffer.Length);
            return retVal;
        }

        /// <summary>
        /// Create an ArgSlice that includes a header of specified size, followed by the specified ArgSlice (arg)
        /// </summary>
        public ArgSlice FormatScratch(int headerSize, ArgSlice arg)
        {
            int length = headerSize + arg.Length;
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(scratchBufferHead + scratchBufferOffset, length);
            retVal.Span[..headerSize].Clear(); // Clear the header

            byte* ptr = scratchBufferHead + scratchBufferOffset + headerSize;
            arg.ReadOnlySpan.CopyTo(new Span<byte>(ptr, arg.Length));

            scratchBufferOffset += length;
            Debug.Assert(scratchBufferOffset <= scratchBuffer.Length);
            return retVal;
        }

        /// <summary>
        /// Create an ArgSlice of specified length, leaves contents as is
        /// </summary>
        public ArgSlice CreateArgSlice(int length)
        {
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(scratchBufferHead + scratchBufferOffset, length);
            scratchBufferOffset += length;
            Debug.Assert(scratchBufferOffset <= scratchBuffer.Length);
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
            return new ArgSlice(scratchBufferHead + scratchBufferOffset, scratchBuffer.Length - scratchBufferOffset);
        }

        public ArgSlice ViewFullArgSlice()
        {
            return new ArgSlice(scratchBufferHead, scratchBufferOffset);
        }

        /// <summary>
        /// Create an ArgSlice that includes a header of specified size, followed by the specified Memory
        /// </summary>
        public ArgSlice FormatScratch(int headerSize, ReadOnlySpan<byte> arg)
        {
            int length = headerSize + arg.Length;
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(scratchBufferHead + scratchBufferOffset, length);
            retVal.Span[..headerSize].Clear(); // Clear the header

            byte* ptr = scratchBufferHead + scratchBufferOffset + headerSize;
            arg.CopyTo(new Span<byte>(ptr, arg.Length));

            scratchBufferOffset += length;
            Debug.Assert(scratchBufferOffset <= scratchBuffer.Length);
            return retVal;
        }

        /// <summary>
        /// Start a RESP array to hold a command and arguments.
        /// 
        /// Fill it with <paramref name="argCount"/> calls to <see cref="WriteNullArgument"/> and/or <see cref="WriteArgument(ReadOnlySpan{byte})"/>.
        /// </summary>
        public void StartCommand(ReadOnlySpan<byte> cmd, int argCount)
        {
            if (scratchBuffer == null)
                ExpandScratchBuffer(64);

            var ptr = scratchBufferHead + scratchBufferOffset;

            while (!RespWriteUtils.WriteArrayLength(argCount + 1, ref ptr, scratchBufferHead + scratchBuffer.Length))
            {
                ExpandScratchBuffer(scratchBuffer.Length + 1);
                ptr = scratchBufferHead + scratchBufferOffset;
            }
            scratchBufferOffset = (int)(ptr - scratchBufferHead);

            while (!RespWriteUtils.WriteBulkString(cmd, ref ptr, scratchBufferHead + scratchBuffer.Length))
            {
                ExpandScratchBuffer(scratchBuffer.Length + 1);
                ptr = scratchBufferHead + scratchBufferOffset;
            }
            scratchBufferOffset = (int)(ptr - scratchBufferHead);
        }

        /// <summary>
        /// Use to fill a RESP array with arguments after a call to <see cref="StartCommand(ReadOnlySpan{byte}, int)"/>.
        /// </summary>
        public void WriteNullArgument()
        {
            var ptr = scratchBufferHead + scratchBufferOffset;

            while (!RespWriteUtils.WriteNull(ref ptr, scratchBufferHead + scratchBuffer.Length))
            {
                ExpandScratchBuffer(scratchBuffer.Length + 1);
                ptr = scratchBufferHead + scratchBufferOffset;
            }

            scratchBufferOffset = (int)(ptr - scratchBufferHead);
        }

        /// <summary>
        /// Use to fill a RESP array with arguments after a call to <see cref="StartCommand(ReadOnlySpan{byte}, int)"/>.
        /// </summary>
        public void WriteArgument(ReadOnlySpan<byte> arg)
        {
            var ptr = scratchBufferHead + scratchBufferOffset;

            while (!RespWriteUtils.WriteBulkString(arg, ref ptr, scratchBufferHead + scratchBuffer.Length))
            {
                ExpandScratchBuffer(scratchBuffer.Length + 1);
                ptr = scratchBufferHead + scratchBufferOffset;
            }

            scratchBufferOffset = (int)(ptr - scratchBufferHead);
        }

        /// <summary>
        /// Get length of a RESP Bulk-String formatted version of the specified ArgSlice
        /// RESP format: $[size]\r\n[value]\r\n
        /// Total size: 1 + [number of digits in the size value] + 2 + [size of value] + 2
        /// </summary>
        /// <param name="slice"></param>
        /// <returns></returns>
        static int GetRespFormattedStringLength(ArgSlice slice)
            => 1 + NumUtils.NumDigits(slice.Length) + 2 + slice.Length + 2;

        void ExpandScratchBufferIfNeeded(int newLength)
        {
            if (scratchBuffer == null || newLength > scratchBuffer.Length - scratchBufferOffset)
                ExpandScratchBuffer(scratchBufferOffset + newLength);
        }

        void ExpandScratchBuffer(int newLength, int? copyLengthOverride = null)
        {
            if (newLength < 64) newLength = 64;
            else newLength = (int)BitOperations.RoundUpToPowerOf2((uint)newLength + 1);

            var _scratchBuffer = GC.AllocateArray<byte>(newLength, true);
            var _scratchBufferHead = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_scratchBuffer));

            var copyLength = copyLengthOverride ?? scratchBufferOffset;
            if (copyLength > 0)
            {
                new ReadOnlySpan<byte>(scratchBufferHead, copyLength).CopyTo(new Span<byte>(_scratchBufferHead, copyLength));
            }
            scratchBuffer = _scratchBuffer;
            scratchBufferHead = _scratchBufferHead;
        }

        /// <summary>
        /// Returns a new <see cref="ArgSlice"/>
        /// with the <paramref name="length"/> bytes of the buffer;
        /// these are the most recently added bytes.
        /// </summary>
        /// <param name="length">Length for the new slice</param>
        /// <remarks>This is called by functions that add multiple items to the buffer,
        /// after all items have been added and all reallocations have been done.
        /// </remarks>
        public ArgSlice GetSliceFromTail(int length)
        {
            return new ArgSlice(scratchBufferHead + scratchBufferOffset - length, length);
        }

        /// <summary>
        /// Force backing buffer to grow.
        /// 
        /// <paramref name="copyLengthOverride"/> provides a way to force a chunk at the start of the
        /// previous buffer be copied into the new buffer, even if this <see cref="ScratchBufferManager"/>
        /// doesn't consider that chunk in use.
        /// </summary>
        public void GrowBuffer(int? copyLengthOverride = null)
        {
            if (scratchBuffer == null)
            {
                ExpandScratchBuffer(64);
            }
            else
            {
                ExpandScratchBuffer(scratchBuffer.Length + 1, copyLengthOverride);
            }
        }
    }
}