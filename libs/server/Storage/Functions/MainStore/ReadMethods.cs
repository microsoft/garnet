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
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
        {
            if (value.MetadataSize != 0 && CheckExpiry(ref value))
                return false;

            var cmd = ((RespInputHeader*)input.ToPointer())->cmd;

            var isEtagCmd = cmd is RespCommand.GETWITHETAG or RespCommand.GETIFNOTMATCH;

            // ETAG Read command on non-ETag data should early exit but indicate the wrong type
            if (isEtagCmd && !readInfo.RecordInfo.ETag)
            {
                // Used to indicate wrong type operation
                readInfo.Action = ReadAction.CancelOperation;
                return false;
            }

            if ((byte)cmd >= CustomCommandManager.StartOffset)
            {
                int valueLength = value.LengthWithoutMetadata;
                (IMemoryOwner<byte> Memory, int Length) outp = (dst.Memory, 0);
                var ret = functionsState.customCommands[(byte)cmd - CustomCommandManager.StartOffset].functions.Reader(key.AsReadOnlySpan(), input.AsReadOnlySpan()[RespInputHeader.Size..], value.AsReadOnlySpan(), ref outp, ref readInfo);
                Debug.Assert(valueLength <= value.LengthWithoutMetadata);
                dst.Memory = outp.Memory;
                dst.Length = outp.Length;
                return ret;
            }

            if (cmd == RespCommand.GETIFNOTMATCH)
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

            // Unless the command explicitly asks for the ETag in response, we do not write back the ETag 
            var start = 0;
            var end = -1;
            if (!isEtagCmd && readInfo.RecordInfo.ETag)
            {
                start = sizeof(long);
                end = value.LengthWithoutMetadata;
            }

            if (input.Length == 0)
                CopyRespTo(ref value, ref dst, start, end);
            else
                CopyRespToWithInput(ref input, ref value, ref dst, readInfo.IsFromPending, start, end);

            return true;
        }

        /// <inheritdoc />
        public bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            if (value.MetadataSize != 0 && CheckExpiry(ref value))
            {
                // TODO: we can proactively expire if we wish, but if we do, we need to write WAL entry
                // readInfo.Action = ReadAction.Expire;
                return false;
            }

            var cmd = ((RespInputHeader*)input.ToPointer())->cmd;

            var isEtagCmd = cmd is RespCommand.GETWITHETAG or RespCommand.GETIFNOTMATCH;

            // ETAG Read command on non-ETag data should early exit but indicate the wrong type
            if (isEtagCmd && !recordInfo.ETag)
            {
                // Used to indicate wrong type operation
                readInfo.Action = ReadAction.CancelOperation;
                return false;
            }

            if ((byte)cmd >= CustomCommandManager.StartOffset)
            {
                int valueLength = value.LengthWithoutMetadata;
                (IMemoryOwner<byte> Memory, int Length) outp = (dst.Memory, 0);
                var ret = functionsState.customCommands[(byte)cmd - CustomCommandManager.StartOffset].functions.Reader(key.AsReadOnlySpan(), input.AsReadOnlySpan()[RespInputHeader.Size..], value.AsReadOnlySpan(), ref outp, ref readInfo);
                Debug.Assert(valueLength <= value.LengthWithoutMetadata);
                dst.Memory = outp.Memory;
                dst.Length = outp.Length;
                return ret;
            }

            if (cmd == RespCommand.GETIFNOTMATCH)
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

            // Unless the command explicitly asks for the ETag in response, we do not write back the ETag 
            var start = 0;
            var end = -1;
            if (!isEtagCmd && recordInfo.ETag)
            {
                start = sizeof(long);
                end = value.LengthWithoutMetadata;
            }

            if (input.Length == 0)
                CopyRespTo(ref value, ref dst, start, end);
            else
            {
                CopyRespToWithInput(ref input, ref value, ref dst, readInfo.IsFromPending, start, end);
            }

            return true;
        }
    }
}