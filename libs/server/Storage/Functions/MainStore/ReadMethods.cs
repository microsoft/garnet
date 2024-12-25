﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool SingleReader(
            ref SpanByte key, ref RawStringInput input,
            ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
        {
            if (value.MetadataSize != 0 && CheckExpiry(ref value))
                return false;

            var cmd = input.header.cmd;

            var etagMultiplier = readInfo.RecordInfo.HasETagMultiplier;

            if (cmd == RespCommand.GETIFNOTMATCH)
            {
                long etagToMatchAgainst = input.parseState.GetLong(0);
                // Any value without an etag is treated the same as a value with an etag
                long existingEtag = etagMultiplier * *(long*)value.ToPointer();
                if (existingEtag == etagToMatchAgainst)
                {
                    // write back array of the format [etag, nil]
                    var nilResp = CmdStrings.RESP_ERRNOTFOUND;
                    // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                    var numDigitsInEtag = NumUtils.NumDigitsInLong(existingEtag);
                    WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + nilResp.Length, ref nilResp, existingEtag, ref dst, writeDirect: true);
                    return true;
                }
            }
            else if (cmd > RespCommandExtensions.LastValidCommand)
            {
                var valueLength = value.LengthWithoutMetadata;
                (IMemoryOwner<byte> Memory, int Length) output = (dst.Memory, 0);
                var ret = functionsState.GetCustomCommandFunctions((ushort)cmd)
                    .Reader(key.AsReadOnlySpan(), ref input, value.AsReadOnlySpan(), ref output, ref readInfo);
                Debug.Assert(valueLength <= value.LengthWithoutMetadata);
                dst.Memory = output.Memory;
                dst.Length = output.Length;
                return ret;
            }

            // Unless the command explicitly asks for the ETag in response, we do not write back the ETag 
            bool isNotEtagCmdAndRecordHasEtag = cmd is not (RespCommand.GETWITHETAG or RespCommand.GETIFNOTMATCH);
            int isNotEtagCmdAndRecordHasEtagMultiplier = etagMultiplier * Unsafe.As<bool, byte>(ref isNotEtagCmdAndRecordHasEtag);

            int start = isNotEtagCmdAndRecordHasEtagMultiplier * Constants.EtagSize;
            int end = isNotEtagCmdAndRecordHasEtagMultiplier * (value.LengthWithoutMetadata + 1);
            end--;

            if (cmd == RespCommand.NONE)
                CopyRespTo(ref value, ref dst, start, end);
            else
            {
                CopyRespToWithInput(ref input, ref value, ref dst, readInfo.IsFromPending, start, end, readInfo.RecordInfo.ETag);
            }

            return true;
        }

        /// <inheritdoc />
        public bool ConcurrentReader(
            ref SpanByte key, ref RawStringInput input, ref SpanByte value,
            ref SpanByteAndMemory dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            if (value.MetadataSize != 0 && CheckExpiry(ref value))
                return false;

            var cmd = input.header.cmd;

            var etagMultiplier = readInfo.RecordInfo.HasETagMultiplier;

            if (cmd == RespCommand.GETIFNOTMATCH)
            {
                long etagToMatchAgainst = input.parseState.GetLong(0);
                // Any value without an etag is treated the same as a value with an etag
                long existingEtag = etagMultiplier * *(long*)value.ToPointer();
                if (existingEtag == etagToMatchAgainst)
                {
                    // write back array of the format [etag, nil]
                    var nilResp = CmdStrings.RESP_ERRNOTFOUND;
                    // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                    var numDigitsInEtag = NumUtils.NumDigitsInLong(existingEtag);
                    WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + nilResp.Length, ref nilResp, existingEtag, ref dst, writeDirect: true);
                    return true;
                }
            }
            else if (cmd > RespCommandExtensions.LastValidCommand)
            {
                var valueLength = value.LengthWithoutMetadata;
                (IMemoryOwner<byte> Memory, int Length) output = (dst.Memory, 0);
                var ret = functionsState.GetCustomCommandFunctions((ushort)cmd)
                    .Reader(key.AsReadOnlySpan(), ref input, value.AsReadOnlySpan(), ref output, ref readInfo);
                Debug.Assert(valueLength <= value.LengthWithoutMetadata);
                dst.Memory = output.Memory;
                dst.Length = output.Length;
                return ret;
            }

            // Unless the command explicitly asks for the ETag in response, we do not write back the ETag 
            bool isNotEtagCmdAndRecordHasEtag = cmd is not (RespCommand.GETWITHETAG or RespCommand.GETIFNOTMATCH);
            int isNotEtagCmdAndRecordHasEtagMultiplier = etagMultiplier * Unsafe.As<bool, byte>(ref isNotEtagCmdAndRecordHasEtag);

            int start = isNotEtagCmdAndRecordHasEtagMultiplier * Constants.EtagSize;
            int end = isNotEtagCmdAndRecordHasEtagMultiplier * (value.LengthWithoutMetadata + 1);
            end--;

            if (cmd == RespCommand.NONE)
                CopyRespTo(ref value, ref dst, start, end);
            else
            {
                CopyRespToWithInput(ref input, ref value, ref dst, readInfo.IsFromPending, start, end, readInfo.RecordInfo.ETag);
            }

            return true;
        }
    }
}