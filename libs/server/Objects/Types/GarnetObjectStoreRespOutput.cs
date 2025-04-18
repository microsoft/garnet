// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal unsafe ref struct GarnetObjectStoreRespOutput : IDisposable
    {
        RespMemoryWriter writer;
        ObjectOutputHeader outputHeader;

        internal unsafe GarnetObjectStoreRespOutput(byte respVersion, ref SpanByteAndMemory output)
        {
            writer = new RespMemoryWriter(respVersion, ref output);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteAsciiBulkString(string bulkString) => writer.WriteAsciiBulkString(bulkString);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteAsciiDirect(ReadOnlySpan<char> asciiString) => writer.WriteAsciiDirect(asciiString);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteArrayItem(long item) => writer.WriteArrayItem(item);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteArrayLength(int len) => writer.WriteArrayLength(len);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteArrayLength(int len, out int numDigits, out int totalLen) => writer.WriteArrayLength(len, out numDigits, out totalLen);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteBulkString(ReadOnlySpan<byte> item) => writer.WriteBulkString(item);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteDirect(ReadOnlySpan<byte> span) => writer.WriteDirect(span);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteDoubleBulkString(double value) => writer.WriteDoubleBulkString(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteDoubleNumeric(double value) => writer.WriteDoubleNumeric(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteEmptyArray() => writer.WriteEmptyArray();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteError(ReadOnlySpan<byte> errorString) => writer.WriteError(errorString);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteInt32(int value) => writer.WriteInt32(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteInt64(long value) => writer.WriteInt64(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteIntegerFromBytes(byte[] resultBytes) => writer.WriteIntegerFromBytes(resultBytes);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteMapLength(int len) => writer.WriteMapLength(len);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteNull() => writer.WriteNull();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteNullArray() => writer.WriteNullArray();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteSetLength(int len) => writer.WriteSetLength(len);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ResetPosition() => writer.ResetPosition();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DecreaseArrayLength(int newCount, int oldArrayLen, int arrayPos = 0) =>
            writer.DecreaseArrayLength(newCount, oldArrayLen, arrayPos);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void IncResult1()
        {
            outputHeader.result1++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetResult1(int result1)
        {
            outputHeader.result1 = result1;
        }

        public void Dispose()
        {
            while (!RespWriteUtils.TryWriteDirect(ref outputHeader, ref writer.curr, writer.end))
                RespMemoryWriter.ReallocateOutput(ref writer.output, ref writer.isMemory, ref writer.ptr, ref writer.ptrHandle, ref writer.curr, ref writer.end);

            writer.Dispose();
        }
    }
}