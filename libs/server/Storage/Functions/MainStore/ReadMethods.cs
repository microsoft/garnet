// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
            ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
        {
            if (value.MetadataSize == 8 && CheckExpiry(ref value))
            {
                readInfo.RecordInfo.ClearHasETag();
                return false;
            }

            var cmd = input.header.cmd;

            // Ignore special Vector Set logic if we're scanning, detected with cmd == NONE
            if (cmd != RespCommand.NONE)
            {
                // Vector sets are reachable (key not mangled) and hidden.
                // So we can use that to detect type mismatches.
                if (readInfo.RecordInfo.VectorSet && !cmd.IsLegalOnVectorSet())
                {
                    // Attempted an illegal op on a VectorSet
                    readInfo.Action = ReadAction.CancelOperation;
                    return false;
                }
                else if (!readInfo.RecordInfo.VectorSet && cmd.IsLegalOnVectorSet())
                {
                    // Attempted a vector set op on a non-VectorSet
                    readInfo.Action = ReadAction.CancelOperation;
                    return false;
                }
            }

            if (cmd == RespCommand.GETIFNOTMATCH)
            {
                if (handleGetIfNotMatch(ref input, ref value, ref dst, ref readInfo))
                    return true;
            }
            else if (cmd > RespCommandExtensions.LastValidCommand)
            {
                if (readInfo.RecordInfo.ETag)
                {
                    CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref dst);
                    return true;
                }

                var valueLength = value.LengthWithoutMetadata;

                var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref dst);
                try
                {
                    var ret = functionsState.GetCustomCommandFunctions((ushort)cmd)
                        .Reader(key.AsReadOnlySpan(), ref input, value.AsReadOnlySpan(), ref writer, ref readInfo);
                    Debug.Assert(valueLength <= value.LengthWithoutMetadata);
                    return ret;
                }
                finally
                {
                    writer.Dispose();
                }
            }

            if (readInfo.RecordInfo.ETag)
            {
                EtagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref value);
            }

            // Unless the command explicitly asks for the ETag in response, we do not write back the ETag
            if (cmd is (RespCommand.GETWITHETAG or RespCommand.GETIFNOTMATCH))
            {
                CopyRespWithEtagData(ref value, ref dst, readInfo.RecordInfo.ETag, functionsState.etagState.etagSkippedStart, functionsState.memoryPool);
                EtagState.ResetState(ref functionsState.etagState);
                return true;
            }

            if (cmd == RespCommand.NONE)
                CopyRespTo(ref value, ref dst, functionsState.etagState.etagSkippedStart, functionsState.etagState.etagAccountedLength);
            else
            {
                CopyRespToWithInput(ref input, ref value, ref dst, readInfo.IsFromPending);
            }

            if (readInfo.RecordInfo.ETag)
            {
                EtagState.ResetState(ref functionsState.etagState);
            }

            return true;
        }

        /// <inheritdoc />
        public bool ConcurrentReader(
            ref SpanByte key, ref RawStringInput input, ref SpanByte value,
            ref SpanByteAndMemory dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            if (value.MetadataSize == 8 && CheckExpiry(ref value))
            {
                recordInfo.ClearHasETag();
                return false;
            }

            var cmd = input.header.cmd;

            // Ignore special Vector Set logic if we're scanning, detected with cmd == NONE
            if (cmd != RespCommand.NONE)
            {
                // Vector sets are reachable (key not mangled) and hidden.
                // So we can use that to detect type mismatches.
                if (recordInfo.VectorSet && !cmd.IsLegalOnVectorSet())
                {
                    // Attempted an illegal op on a VectorSet
                    readInfo.Action = ReadAction.CancelOperation;
                    return false;
                }
                else if (!recordInfo.VectorSet && cmd.IsLegalOnVectorSet())
                {
                    // Attempted a vector set op on a non-VectorSet
                    readInfo.Action = ReadAction.CancelOperation;
                    return false;
                }
            }

            if (cmd == RespCommand.GETIFNOTMATCH)
            {
                if (handleGetIfNotMatch(ref input, ref value, ref dst, ref readInfo))
                    return true;
            }
            else if (cmd > RespCommandExtensions.LastValidCommand)
            {
                if (readInfo.RecordInfo.ETag)
                {
                    CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref dst);
                    return true;
                }

                var valueLength = value.LengthWithoutMetadata;

                var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref dst);
                try
                {
                    var ret = functionsState.GetCustomCommandFunctions((ushort)cmd)
                        .Reader(key.AsReadOnlySpan(), ref input, value.AsReadOnlySpan(), ref writer, ref readInfo);
                    Debug.Assert(valueLength <= value.LengthWithoutMetadata);
                    return ret;
                }
                finally
                {
                    writer.Dispose();
                }
            }

            if (readInfo.RecordInfo.ETag)
            {
                EtagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref value);
            }

            // Unless the command explicitly asks for the ETag in response, we do not write back the ETag
            if (cmd is (RespCommand.GETWITHETAG or RespCommand.GETIFNOTMATCH))
            {
                CopyRespWithEtagData(ref value, ref dst, readInfo.RecordInfo.ETag, functionsState.etagState.etagSkippedStart, functionsState.memoryPool);
                EtagState.ResetState(ref functionsState.etagState);
                return true;
            }

            if (cmd == RespCommand.NONE)
                CopyRespTo(ref value, ref dst, functionsState.etagState.etagSkippedStart, functionsState.etagState.etagAccountedLength);
            else
            {
                CopyRespToWithInput(ref input, ref value, ref dst, readInfo.IsFromPending);
            }

            if (readInfo.RecordInfo.ETag)
            {
                EtagState.ResetState(ref functionsState.etagState);
            }

            return true;
        }

        private bool handleGetIfNotMatch(ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
        {
            // Any value without an etag is treated the same as a value with an etag
            long etagToMatchAgainst = input.parseState.GetLong(0);

            long existingEtag = readInfo.RecordInfo.ETag ? value.GetEtagInPayload() : EtagConstants.NoETag;

            if (existingEtag == etagToMatchAgainst)
            {
                // write back array of the format [etag, nil]
                var nilResp = functionsState.nilResp;
                // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                var numDigitsInEtag = NumUtils.CountDigits(existingEtag);
                WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + nilResp.Length, ref nilResp, existingEtag, ref dst, functionsState.memoryPool, writeDirect: true);
                return true;
            }

            return false;
        }
    }
}