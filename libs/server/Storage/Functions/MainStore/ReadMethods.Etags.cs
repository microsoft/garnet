// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// ETag-specific Read callback methods for main store, kept in a separate file
    /// with NoInlining to minimize hot-path method footprint.
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<StringInput, StringOutput, long>
    {
        /// <summary>
        /// Handles GETWITHETAG and GETIFNOTMATCH commands. Called from Reader via early delegation.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private bool HandleEtagReader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref StringInput input, ref StringOutput output, ref ReadInfo readInfo,
            RespCommand cmd, ReadOnlySpan<byte> value)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (cmd == RespCommand.GETIFNOTMATCH)
            {
                // Check if the client's ETag matches; if so, return [etag, nil] without the value
                long etagToMatchAgainst = input.parseState.GetLong(0);
                long existingEtag = srcLogRecord.ETag;

                if (existingEtag == etagToMatchAgainst)
                {
                    // Write back array of the format [etag, nil]
                    var nilResp = functionsState.nilResp;
                    var numDigitsInEtag = NumUtils.CountDigits(existingEtag);
                    WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + nilResp.Length, nilResp, existingEtag, ref output, functionsState.memoryPool, writeDirect: true);
                    return true;
                }
            }

            // For both GETWITHETAG and GETIFNOTMATCH (when etag didn't match), return [etag, value]
            long etag = srcLogRecord.Info.HasETag ? srcLogRecord.ETag : LogRecord.NoETag;
            CopyRespWithEtagData(value, ref output, srcLogRecord.Info.HasETag, etag, functionsState.memoryPool);
            return true;
        }
    }
}