// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Manage parameters to Operate, as well as consolidates output protocol writing.
    /// </summary>
    public unsafe ref struct OperateParams : IDisposable
    {
        ref ObjectInput input;
        ref GarnetObjectStoreOutput output;
        readonly byte respProtocolVersion;

        byte* curr;
        byte* end;
        byte* ptr;
        MemoryHandle ptrHandle;
        bool isMemory;

        internal unsafe OperateParams(ref ObjectInput input, ref GarnetObjectStoreOutput output, byte respProtocolVersion)
        {
            this.input = ref input;
            this.output = ref output;
            this.respProtocolVersion = respProtocolVersion;
            ptr = output.SpanByteAndMemory.SpanByte.ToPointer();
            curr = ptr;
            end = curr + output.SpanByteAndMemory.Length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteAsciiBulkString(string bulkString)
        {
            while (!RespWriteUtils.TryWriteAsciiBulkString(bulkString, ref curr, end))
                ObjectUtils.ReallocateOutput(ref output.SpanByteAndMemory, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteMapLength(int len)
        {
            switch (respProtocolVersion)
            {
                case 3:
                    while (!RespWriteUtils.TryWriteMapLength(len, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output.SpanByteAndMemory, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    break;
                default:
                    while (!RespWriteUtils.TryWriteArrayLength(len * 2, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output.SpanByteAndMemory, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    break;
            }
        }

        public void Dispose()
        {
            if (isMemory) ptrHandle.Dispose();
            output.SpanByteAndMemory.Length = (int)(curr - ptr);
        }
    }
}