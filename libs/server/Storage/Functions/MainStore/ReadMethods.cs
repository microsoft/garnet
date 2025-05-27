﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (CheckExpiry(ref srcLogRecord))
                return false;

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

                var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output);
                try
                {
                    var ret = functionsState.GetCustomCommandFunctions((ushort)cmd)
                        .Reader(srcLogRecord.Key, ref input, value, ref writer, ref readInfo);
                    Debug.Assert(valueLength <= value.Length);
                    return ret;
                }
                finally
                {
                    writer.Dispose();
                }
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

        private bool handleGetIfNotMatch<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref RawStringInput input, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // Any value without an etag is treated the same as a value with an etag
            long etagToMatchAgainst = input.parseState.GetLong(0);

            long existingEtag = srcLogRecord.ETag;

            if (existingEtag == etagToMatchAgainst)
            {
                // write back array of the format [etag, nil]
                var nilResp = functionsState.nilResp;
                // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                var numDigitsInEtag = NumUtils.CountDigits(existingEtag);
                WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + nilResp.Length, nilResp, existingEtag, ref dst, functionsState.memoryPool, writeDirect: true);
                return true;
            }

            return false;
        }
    }
}