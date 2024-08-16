// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using Garnet.common;
using NLua;

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

        public ScratchBufferManager()
        {
        }

        /// <summary>
        /// Reset scratch buffer - loses all ArgSlice instances created on the scratch buffer
        /// </summary>
        public void Reset() => scratchBufferOffset = 0;

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
        /// Format specified command with arguments, as a RESP command. Lua state
        /// can be specified to handle Lua tables as arguments.
        /// </summary>
        public ArgSlice FormatCommandAsResp(string cmd, object[] args, Lua state)
        {
            if (scratchBuffer == null)
                ExpandScratchBuffer(64);

            scratchBufferOffset += 10; // Reserve space for the array length if it is larger than expected
            int commandStartOffset = scratchBufferOffset;
            byte* ptr = scratchBufferHead + scratchBufferOffset;

            while (!RespWriteUtils.WriteArrayLength(args.Length + 1, ref ptr, scratchBufferHead + scratchBuffer.Length))
            {
                ExpandScratchBuffer(scratchBuffer.Length + 1);
                ptr = scratchBufferHead + scratchBufferOffset;
            }
            scratchBufferOffset = (int)(ptr - scratchBufferHead);

            while (!RespWriteUtils.WriteAsciiBulkString(cmd, ref ptr, scratchBufferHead + scratchBuffer.Length))
            {
                ExpandScratchBuffer(scratchBuffer.Length + 1);
                ptr = scratchBufferHead + scratchBufferOffset;
            }
            scratchBufferOffset = (int)(ptr - scratchBufferHead);

            int count = 1;
            foreach (var item in args)
            {
                if (item is string str)
                {
                    count++;
                    while (!RespWriteUtils.WriteAsciiBulkString(str, ref ptr, scratchBufferHead + scratchBuffer.Length))
                    {
                        ExpandScratchBuffer(scratchBuffer.Length + 1);
                        ptr = scratchBufferHead + scratchBufferOffset;
                    }
                    scratchBufferOffset = (int)(ptr - scratchBufferHead);
                }
                else if (item is LuaTable t)
                {
                    var d = state.GetTableDict(t);
                    foreach (var value in d.Values)
                    {
                        count++;
                        while (!RespWriteUtils.WriteAsciiBulkString(Convert.ToString(value), ref ptr, scratchBufferHead + scratchBuffer.Length))
                        {
                            ExpandScratchBuffer(scratchBuffer.Length + 1);
                            ptr = scratchBufferHead + scratchBufferOffset;
                        }
                        scratchBufferOffset = (int)(ptr - scratchBufferHead);
                    }
                }
                else if (item is long i)
                {
                    count++;
                    while (!RespWriteUtils.WriteIntegerAsBulkString((int)i, ref ptr, scratchBufferHead + scratchBuffer.Length))
                    {
                        ExpandScratchBuffer(scratchBuffer.Length + 1);
                        ptr = scratchBufferHead + scratchBufferOffset;
                    }
                    scratchBufferOffset = (int)(ptr - scratchBufferHead);
                }
                else
                {
                    count++;
                    while (!RespWriteUtils.WriteAsciiBulkString(Convert.ToString(item), ref ptr, scratchBufferHead + scratchBuffer.Length))
                    {
                        ExpandScratchBuffer(scratchBuffer.Length + 1);
                        ptr = scratchBufferHead + scratchBufferOffset;
                    }
                    scratchBufferOffset = (int)(ptr - scratchBufferHead);
                }
            }
            if (count != args.Length + 1)
            {
                int extraSpace = NumUtils.NumDigits(count) - NumUtils.NumDigits(args.Length + 1);
                if (commandStartOffset < extraSpace)
                    throw new InvalidOperationException("Invalid number of arguments");
                commandStartOffset -= extraSpace;
                var head = scratchBufferHead + commandStartOffset;
                // There should be space as we have reserved it
                _ = RespWriteUtils.WriteArrayLength(count, ref head, scratchBufferHead + scratchBuffer.Length);
            }

            var retVal = new ArgSlice(scratchBufferHead + commandStartOffset, scratchBufferOffset - commandStartOffset);
            Debug.Assert(scratchBufferOffset <= scratchBuffer.Length);
            return retVal;
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

        void ExpandScratchBuffer(int newLength)
        {
            if (newLength < 64) newLength = 64;
            else newLength = (int)BitOperations.RoundUpToPowerOf2((uint)newLength + 1);

            var _scratchBuffer = GC.AllocateArray<byte>(newLength, true);
            var _scratchBufferHead = (byte*)Unsafe.AsPointer(ref _scratchBuffer[0]);
            if (scratchBufferOffset > 0)
                new ReadOnlySpan<byte>(scratchBufferHead, scratchBufferOffset).CopyTo(new Span<byte>(_scratchBufferHead, scratchBufferOffset));
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
    }
}