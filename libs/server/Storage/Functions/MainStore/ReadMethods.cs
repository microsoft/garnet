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
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
        {
            if (CheckExpiry(ref srcLogRecord))
            {
                srcLogRecord.InfoRef.ClearHasETag();
                return false;
            }

            var cmd = input.header.cmd;
            var value = srcLogRecord.ValueSpan; // reduce redundant length calculations
            if (cmd == RespCommand.GETIFNOTMATCH)
            {
                if (handleGetIfNotMatch(ref srcLogRecord, ref input, ref output, ref readInfo))
                    return true;
            }
            else if (cmd > RespCommandExtensions.LastValidCommand)
            {
                if (srcLogRecord.Info.HasETag)
                {
                    functionsState.CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref output);
                    return true;
                }

                var valueLength = value.Length;
                (IMemoryOwner<byte> Memory, int Length) memoryAndLength = (output.Memory, 0);
                var ret = functionsState.GetCustomCommandFunctions((ushort)cmd)
                    .Reader(srcLogRecord.Key.AsReadOnlySpan(), ref input, value.AsReadOnlySpan(), ref memoryAndLength, ref readInfo);
                Debug.Assert(valueLength <= value.Length);
                (output.Memory, output.Length) = memoryAndLength;
                return ret;
            }

            if (srcLogRecord.Info.HasETag)
                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref srcLogRecord);

            // Unless the command explicitly asks for the ETag in response, we do not write back the ETag 
            if (cmd is RespCommand.GETWITHETAG or RespCommand.GETIFNOTMATCH)
            {
                CopyRespWithEtagData(value, ref output, srcLogRecord.Info.HasETag, functionsState.memoryPool);
                ETagState.ResetState(ref functionsState.etagState);
                return true;
            }

            if (cmd == RespCommand.NONE)
                CopyRespTo(value, ref output);
            else
                CopyRespToWithInput(ref srcLogRecord, ref input, ref output, readInfo.IsFromPending);

            if (srcLogRecord.Info.HasETag)
                ETagState.ResetState(ref functionsState.etagState);

            return true;
        }

        /// <inheritdoc />
        public bool ConcurrentReader(ref LogRecord<SpanByte> srcLogRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            if (CheckExpiry(ref srcLogRecord))
            {
                srcLogRecord.RemoveETag();
                return false;
            }

            var cmd = input.header.cmd;
            var value = srcLogRecord.ValueSpan; // reduce redundant length calculations
            if (cmd == RespCommand.GETIFNOTMATCH)
            {
                if (handleGetIfNotMatch(ref srcLogRecord, ref input, ref output, ref readInfo))
                    return true;
            }
            else if (cmd > RespCommandExtensions.LastValidCommand)
            {
                if (srcLogRecord.Info.HasETag)
                {
                    functionsState.CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref output);
                    return true;
                }

                var valueLength = value.Length;
                (IMemoryOwner<byte> Memory, int Length) memoryAndLength = (output.Memory, 0);
                var ret = functionsState.GetCustomCommandFunctions((ushort)cmd)
                    .Reader(srcLogRecord.Key.AsReadOnlySpan(), ref input, value.AsReadOnlySpan(), ref memoryAndLength, ref readInfo);
                Debug.Assert(valueLength <= value.Length);
                (output.Memory, output.Length) = memoryAndLength;
                return ret;
            }

            if (srcLogRecord.Info.HasETag)
                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref srcLogRecord);

            // Unless the command explicitly asks for the ETag in response, we do not write back the ETag 
            if (cmd is (RespCommand.GETWITHETAG or RespCommand.GETIFNOTMATCH))
            {
                CopyRespWithEtagData(value, ref output, srcLogRecord.Info.HasETag, functionsState.memoryPool);
                ETagState.ResetState(ref functionsState.etagState);
                return true;
            }


            if (cmd == RespCommand.NONE)
                CopyRespTo(value, ref output);
            else
                CopyRespToWithInput(ref srcLogRecord, ref input, ref output, readInfo.IsFromPending);

            if (srcLogRecord.Info.HasETag)
                ETagState.ResetState(ref functionsState.etagState);
            return true;
        }

        private bool handleGetIfNotMatch<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref RawStringInput input, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
        {
            // Any value without an etag is treated the same as a value with an etag
            long etagToMatchAgainst = input.parseState.GetLong(0);

            long existingEtag = srcLogRecord.ETag;

            if (existingEtag == etagToMatchAgainst)
            {
                // write back array of the format [etag, nil]
                var nilResp = CmdStrings.RESP_ERRNOTFOUND;
                // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                var numDigitsInEtag = NumUtils.CountDigits(existingEtag);
                WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + nilResp.Length, nilResp, existingEtag, ref dst, functionsState.memoryPool, writeDirect: true);
                return true;
            }

            return false;
        }
    }
}