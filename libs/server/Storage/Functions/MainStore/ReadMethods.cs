// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Diagnostics;
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
            ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo) => Reader(ref key, ref input, ref value, ref dst, ref readInfo, readInfo.RecordInfo.ETag);


        /// <inheritdoc />
        public bool ConcurrentReader(
            ref SpanByte key, ref RawStringInput input, ref SpanByte value,
            ref SpanByteAndMemory dst, ref ReadInfo readInfo, ref RecordInfo recordInfo) => Reader(ref key, ref input, ref value, ref dst, ref readInfo, recordInfo.ETag);

        private bool Reader(ref SpanByte key, ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo, bool hasEtag)
        {
            if (value.MetadataSize != 0 && CheckExpiry(ref value))
                return false;

            var cmd = input.header.cmd;

            var isEtagCmd = cmd is RespCommand.GETWITHETAG or RespCommand.GETIFNOTMATCH;

            if (cmd == RespCommand.GETIFNOTMATCH)
            {
                long etagToMatchAgainst = input.parseState.GetLong(0);
                // Any value without an etag is treated the same as a value with an etag
                long existingEtag = hasEtag ? *(long*)value.ToPointer() : 0;
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
            var start = 0;
            var end = -1;
            if (!isEtagCmd && hasEtag)
            {
                start = Constants.EtagSize;
                end = value.LengthWithoutMetadata;
            }

            if (cmd == RespCommand.NONE)
                CopyRespTo(ref value, ref dst, start, end);
            else
            {
                CopyRespToWithInput(ref input, ref value, ref dst, readInfo.IsFromPending, start, end, hasEtag);
            }

            return true;
        }
    }
}