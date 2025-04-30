// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Garnet.common
{
    /// <summary>
    /// RESP output to SpanByteAndMemory.
    /// </summary>
    public unsafe ref struct RespMemoryWriter : IDisposable
    {
        byte* curr;
        byte* end;
        byte* ptr;
        MemoryHandle ptrHandle;
        ref SpanByteAndMemory output;
        bool isMemory;
        public readonly bool resp3;

        public unsafe RespMemoryWriter(byte respVersion, ref SpanByteAndMemory output)
        {
            this.output = ref output;
            resp3 = respVersion >= 3;
            ptr = output.SpanByte.ToPointer();
            curr = ptr;
            end = curr + output.Length;
        }

        /// <summary>
        /// Encodes the <paramref name="chars"/> as ASCII bulk string to memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteAsciiBulkString(string chars)
        {
            while (!RespWriteUtils.TryWriteAsciiBulkString(chars, ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Encodes the <paramref name="span"/> as ASCII to memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteAsciiDirect(ReadOnlySpan<char> span)
        {
            while (!RespWriteUtils.TryWriteAsciiDirect(span, ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Writes an array item to memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteArrayItem(long item)
        {
            while (!RespWriteUtils.TryWriteArrayItem(item, ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Writes an array length to memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteArrayLength(int len)
        {
            while (!RespWriteUtils.TryWriteArrayLength(len, ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Writes an array length to memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteArrayLength(int len, out int numDigits, out int totalLen)
        {
            while (!RespWriteUtils.TryWriteArrayLength(len, ref curr, end, out numDigits, out totalLen))
                ReallocateOutput();
        }

        /// <summary>
        /// Write bulk string to memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteBulkString(scoped ReadOnlySpan<byte> item)
        {
            while (!RespWriteUtils.TryWriteBulkString(item, ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Writes the contents of <paramref name="span"/> as byte array to memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteDirect(scoped ReadOnlySpan<byte> span)
        {
            while (!RespWriteUtils.TryWriteDirect(span, ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Writes a double-precision floating-point <paramref name="value"/> as bulk string to memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteDoubleBulkString(double value)
        {
            while (!RespWriteUtils.TryWriteDoubleBulkString(value, ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Write a double-precision floating-point <paramref name="value"/> to memory.
        /// If RESP2, write as BulkString. If RESP3 write as Double.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteDoubleNumeric(double value)
        {
            if (resp3)
            {
                while (!RespWriteUtils.TryWriteDoubleNumeric(value, ref curr, end))
                    ReallocateOutput();
            }
            else
            {
                while (!RespWriteUtils.TryWriteDoubleBulkString(value, ref curr, end))
                    ReallocateOutput();
            }
        }

        /// <summary>
        /// Write empty array to memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteEmptyArray()
        {
            while (!RespWriteUtils.TryWriteEmptyArray(ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Write empty array to memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteEmptyMap()
        {
            if (resp3)
            {
                while (!RespWriteUtils.TryWriteMapLength(0, ref curr, end))
                    ReallocateOutput();
            }
            else
            {
                while (!RespWriteUtils.TryWriteEmptyArray(ref curr, end))
                    ReallocateOutput();
            }
        }

        /// <summary>
        /// Write simple error to memory.
        /// </summary>
        /// <param name="errorString">An ASCII encoded error string. The string mustn't contain a CR (\r) or LF (\n) bytes.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteError(scoped ReadOnlySpan<byte> errorString)
        {
            while (!RespWriteUtils.TryWriteError(errorString, ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Write simple error
        /// </summary>
        /// <param name="errorString">An ASCII error string. The string mustn't contain a CR (\r) or LF (\n) characters.</param>
        public void WriteError(ReadOnlySpan<char> errorString)
        {
            while (!RespWriteUtils.TryWriteError(errorString, ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Write integer to memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteInt32(int value)
        {
            while (!RespWriteUtils.TryWriteInt32(value, ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Write a signed 64-bit integer as bulk string
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteInt64AsBulkString(long value)
        {
            while (!RespWriteUtils.TryWriteInt64AsBulkString(value, ref curr, end, out _))
                ReallocateOutput();
        }

        /// <summary>
        /// Write long to memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteInt64(long value)
        {
            while (!RespWriteUtils.TryWriteInt64(value, ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Write integer from bytes to memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteIntegerFromBytes(byte[] resultBytes)
        {
            while (!RespWriteUtils.TryWriteIntegerFromBytes(resultBytes, ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Writes a map length to memory.
        /// If RESP2, write as (doubled) Array length. If RESP3, write as Map length.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteMapLength(int len)
        {
            if (resp3)
            {
                while (!RespWriteUtils.TryWriteMapLength(len, ref curr, end))
                    ReallocateOutput();
            }
            else
            {
                while (!RespWriteUtils.TryWriteArrayLength(len * 2, ref curr, end))
                    ReallocateOutput();
            }
        }

        /// <summary>
        /// Writes a null to memory, using proper protocol representation.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteNull()
        {
            if (resp3)
            {
                while (!RespWriteUtils.TryWriteResp3Null(ref curr, end))
                    ReallocateOutput();
            }
            else
            {
                while (!RespWriteUtils.TryWriteNull(ref curr, end))
                    ReallocateOutput();
            }
        }

        /// <summary>
        /// Writes a null array to memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteNullArray()
        {
            while (!RespWriteUtils.TryWriteNullArray(ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Writes a push type length
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePushLength(int len)
        {
            while (!RespWriteUtils.TryWritePushLength(len, ref curr, end))
                ReallocateOutput();
        }

        /// <summary>
        /// Writes a set length to memory.
        /// If RESP2, write as Array length. If RESP3, write as Set length.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteSetLength(int len)
        {
            if (resp3)
            {
                while (!RespWriteUtils.TryWriteSetLength(len, ref curr, end))
                    ReallocateOutput();
            }
            else
            {
                while (!RespWriteUtils.TryWriteArrayLength(len, ref curr, end))
                    ReallocateOutput();
            }
        }

        /// <summary>
        /// Write simple string to memory.
        /// </summary>
        /// <param name="simpleString">An ASCII simple string. The string mustn't contain a CR (\r) or LF (\n) characters.</param>

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteSimpleString(ReadOnlySpan<char> simpleString)
        {
            while (!RespWriteUtils.TryWriteSimpleString(simpleString, ref curr, end))
                ReallocateOutput();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReallocateOutput()
        {
            ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
        }

        public static unsafe void ReallocateOutput(ref SpanByteAndMemory output, ref bool isMemory, ref byte* ptr, ref MemoryHandle ptrHandle, ref byte* curr, ref byte* end)
        {
            var length = Math.Max(output.Length * 2, 1024);
            var newMem = MemoryPool<byte>.Shared.Rent(length);
            var newPtrHandle = newMem.Memory.Pin();
            var newPtr = (byte*)newPtrHandle.Pointer;
            var bytesWritten = (int)(curr - ptr);
            Buffer.MemoryCopy(ptr, newPtr, length, bytesWritten);
            if (isMemory)
            {
                ptrHandle.Dispose();
                output.Memory.Dispose();
            }
            else
            {
                isMemory = true;
                output.ConvertToHeap();
            }
            ptrHandle = newPtrHandle;
            ptr = newPtr;
            output.Memory = newMem;
            output.Length = length;
            curr = ptr + bytesWritten;
            end = ptr + output.Length;
        }

        /// <summary>
        /// Decrease array length.
        /// </summary>
        /// <param name="newCount">New count of array items</param>
        /// <param name="oldTotalArrayHeaderLen">Array Header length</param>
        public void DecreaseArrayLength(int newCount, int oldTotalArrayHeaderLen)
        {
            var startOutputStartptr = ptr;

            // ReallocateOutput is not needed here as there should be always be available space in the output buffer as we have already written the max array length
            _ = RespWriteUtils.TryWriteArrayLength(newCount, ref startOutputStartptr, end, out _, out var newTotalArrayHeaderLen);
            Debug.Assert(oldTotalArrayHeaderLen >= newTotalArrayHeaderLen, "newTotalArrayHeaderLen can't be bigger than totalArrayHeaderLen as we have already written max array length in the buffer");

            if (oldTotalArrayHeaderLen != newTotalArrayHeaderLen)
            {
                var remainingLength = curr - ptr - oldTotalArrayHeaderLen;
                Buffer.MemoryCopy(ptr + oldTotalArrayHeaderLen, ptr + newTotalArrayHeaderLen, remainingLength, remainingLength);
                curr += newTotalArrayHeaderLen - oldTotalArrayHeaderLen;
            }
        }

        /// <summary>
        /// As a span of the contained data.
        /// </summary>
        public readonly ReadOnlySpan<byte> AsReadOnlySpan() => new(ptr, (int)(curr - ptr));

        /// <summary>
        /// Get position
        /// </summary>
        /// <returns></returns>
        public readonly long GetPosition() => curr - ptr;

        /// <summary>
        /// Reset position to starting position
        /// </summary>
        public void ResetPosition()
        {
            curr = ptr;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (isMemory) ptrHandle.Dispose();
            output.Length = (int)(curr - ptr);
        }
    }
}