// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool SingleReader(ref SpanByte key, ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
        {
            if (value.MetadataSize != 0 && CheckExpiry(ref value))
                return false;

            var cmd = input.header.cmd;
            if ((byte)cmd >= CustomCommandManager.StartOffset)
            {
                var valueLength = value.LengthWithoutMetadata;
                (IMemoryOwner<byte> Memory, int Length) output = (dst.Memory, 0);
                var ret = functionsState.customCommands[(byte)cmd - CustomCommandManager.StartOffset].functions
                    .Reader(key.AsReadOnlySpan(), ref input, value.AsReadOnlySpan(), ref output, ref readInfo);
                Debug.Assert(valueLength <= value.LengthWithoutMetadata);
                dst.Memory = output.Memory;
                dst.Length = output.Length;
                return ret;
            }

            if (cmd == RespCommand.NONE)
                CopyRespTo(ref value, ref dst);
            else
                CopyRespToWithInput(ref input, ref value, ref dst, readInfo.IsFromPending);

            return true;
        }

        /// <inheritdoc />
        public bool ConcurrentReader(ref SpanByte key, ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            if (value.MetadataSize != 0 && CheckExpiry(ref value))
            {
                // TODO: we can proactively expire if we wish, but if we do, we need to write WAL entry
                // readInfo.Action = ReadAction.Expire;
                return false;
            }

            var cmd = input.header.cmd;
            if ((byte)cmd >= CustomCommandManager.StartOffset)
            {
                var valueLength = value.LengthWithoutMetadata;
                (IMemoryOwner<byte> Memory, int Length) output = (dst.Memory, 0);
                var ret = functionsState.customCommands[(byte)cmd - CustomCommandManager.StartOffset].functions
                    .Reader(key.AsReadOnlySpan(), ref input, value.AsReadOnlySpan(), ref output, ref readInfo);
                Debug.Assert(valueLength <= value.LengthWithoutMetadata);
                dst.Memory = output.Memory;
                dst.Length = output.Length;
                return ret;
            }

            if (cmd == RespCommand.NONE)
                CopyRespTo(ref value, ref dst);
            else
            {
                CopyRespToWithInput(ref input, ref value, ref dst, readInfo.IsFromPending);
            }

            return true;
        }
    }
}