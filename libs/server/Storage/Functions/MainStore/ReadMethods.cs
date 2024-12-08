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

            var isEtagCmd = cmd is RespCommand.GETWITHETAG or RespCommand.GETIFNOTMATCH;

            if (isEtagCmd && cmd == RespCommand.GETIFNOTMATCH)
            {
                long existingEtag = *(long*)value.ToPointer();
                long etagToMatchAgainst = *(long*)(input.ToPointer() + RespInputHeader.Size);
                if (existingEtag == etagToMatchAgainst)
                {
                    // write the value not changed message to dst, and early return
                    CopyDefaultResp(CmdStrings.RESP_VALNOTCHANGED, ref dst);
                    return true;
                }
            }
            else if ((ushort)cmd >= CustomCommandManager.StartOffset)
            {
                var valueLength = value.LengthWithoutMetadata;
                (IMemoryOwner<byte> Memory, int Length) output = (dst.Memory, 0);
                var ret = functionsState.customCommands[(ushort)cmd - CustomCommandManager.StartOffset].functions
                    .Reader(key.AsReadOnlySpan(), ref input, value.AsReadOnlySpan(), ref output, ref readInfo);
                Debug.Assert(valueLength <= value.LengthWithoutMetadata);
                dst.Memory = output.Memory;
                dst.Length = output.Length;
                return ret;
            }

            // Unless the command explicitly asks for the ETag in response, we do not write back the ETag 
            var start = 0;
            var end = -1;
            if (!isEtagCmd && readInfo.RecordInfo.ETag)
            {
                start = Constants.EtagSize;
                end = value.LengthWithoutMetadata;
            }

            if (cmd == RespCommand.NONE)
                CopyRespTo(ref value, ref dst, start, end);
            else
                CopyRespToWithInput(ref input, ref value, ref dst, readInfo.IsFromPending, start, end, readInfo.RecordInfo.ETag);

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

            var isEtagCmd = cmd is RespCommand.GETWITHETAG or RespCommand.GETIFNOTMATCH;

            if (isEtagCmd && cmd == RespCommand.GETIFNOTMATCH)
            {
                long existingEtag = *(long*)value.ToPointer();
                long etagToMatchAgainst = *(long*)(input.ToPointer() + RespInputHeader.Size);
                if (existingEtag == etagToMatchAgainst)
                {
                    // write the value not changed message to dst, and early return
                    CopyDefaultResp(CmdStrings.RESP_VALNOTCHANGED, ref dst);
                    return true;
                }
            }
            else if ((ushort)cmd >= CustomCommandManager.StartOffset)
            {
                var valueLength = value.LengthWithoutMetadata;
                (IMemoryOwner<byte> Memory, int Length) output = (dst.Memory, 0);
                var ret = functionsState.customCommands[(ushort)cmd - CustomCommandManager.StartOffset].functions
                    .Reader(key.AsReadOnlySpan(), ref input, value.AsReadOnlySpan(), ref output, ref readInfo);
                Debug.Assert(valueLength <= value.LengthWithoutMetadata);
                dst.Memory = output.Memory;
                dst.Length = output.Length;
                return ret;
            }

            // Unless the command explicitly asks for the ETag in response, we do not write back the ETag 
            var start = 0;
            var end = -1;
            if (!isEtagCmd && recordInfo.ETag)
            {
                start = Constants.EtagSize;
                end = value.LengthWithoutMetadata;
            }

            if (cmd == RespCommand.NONE)
                CopyRespTo(ref value, ref dst, start, end);
            else
            {
                CopyRespToWithInput(ref input, ref value, ref dst, readInfo.IsFromPending, start, end, recordInfo.ETag);
            }

            return true;
        }
    }
}