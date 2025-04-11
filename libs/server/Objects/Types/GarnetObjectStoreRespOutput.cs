// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    public unsafe ref struct GarnetObjectStoreRespOutput : IDisposable
    {
        byte* curr;
        byte* end;
        byte* ptr;
        MemoryHandle ptrHandle;
        ref SpanByteAndMemory output;
        ObjectOutputHeader outputHeader;
        bool isMemory;
        readonly bool resp3;

        public unsafe GarnetObjectStoreRespOutput(ref GarnetObjectStoreOutput outputFooter)
        {
            isMemory = false;
            ptrHandle = default;
            output = ref outputFooter.SpanByteAndMemory;
            resp3 = outputFooter.IsResp3;
            ptr = output.SpanByte.ToPointer();
            curr = ptr;
            end = curr + output.Length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteAsciiBulkString(string bulkString)
        {
            while (!RespWriteUtils.TryWriteAsciiBulkString(bulkString, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteArrayItem(long item)
        {
            while (!RespWriteUtils.TryWriteArrayItem(item, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteArrayLength(int len)
        {
            while (!RespWriteUtils.TryWriteArrayLength(len, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteBulkString(ReadOnlySpan<byte> item)
        {
            while (!RespWriteUtils.TryWriteBulkString(item, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteDoubleBulkString(double distanceValue)
        {
            while (!RespWriteUtils.TryWriteDoubleBulkString(distanceValue, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteDoubleNumeric(double value)
        {
            if (resp3)
            {
                while (!RespWriteUtils.TryWriteDoubleNumeric(value, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
            else
            {
                while (!RespWriteUtils.TryWriteDoubleBulkString(value, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteEmptyArray()
        {
            while (!RespWriteUtils.TryWriteEmptyArray(ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteError(ReadOnlySpan<byte> error)
        {
            while (!RespWriteUtils.TryWriteError(error, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteInt32(int result)
        {
            while (!RespWriteUtils.TryWriteInt32(result, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteInt64(long result)
        {
            while (!RespWriteUtils.TryWriteInt64(result, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteIntegerFromBytes(byte[] resultBytes)
        {
            while (!RespWriteUtils.TryWriteIntegerFromBytes(resultBytes, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr,
                    ref end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteMapLength(int len)
        {
            while (!RespWriteUtils.TryWriteMapLength(len, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteNull()
        {
            if (resp3)
            {
                while (!RespWriteUtils.TryWriteResp3Null(ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
            else
            {
                while (!RespWriteUtils.TryWriteNull(ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
            }
        }

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
            while (!RespWriteUtils.TryWriteDirect(ref outputHeader, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

            if (isMemory) ptrHandle.Dispose();
            output.Length = (int)(curr - ptr);
        }
    }
}