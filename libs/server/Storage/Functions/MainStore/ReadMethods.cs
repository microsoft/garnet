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
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<StringInput, StringOutput, long>
    {
        /// <inheritdoc />
        public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref StringInput input, ref StringOutput output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.Info.ValueIsObject)
            {
                readInfo.Action = ReadAction.WrongType;
                return false;
            }

            if (LogRecordUtils.CheckExpiry(in srcLogRecord))
                return false;

            var cmd = input.header.cmd;

            // Type safety: prevent cross-type access (e.g., GET on RI key, RI.SET on string key).
            // Hot path (normal GET/SET on normal string): RecordType == 0, cmd is not special →
            // hits only the first condition (one byte compare), skips everything.
            if (cmd != RespCommand.NONE)
            {
                var recordType = srcLogRecord.RecordType;
                if (recordType != 0)
                {
                    // Record has a special type (RI or Vector) — check if the command is allowed
                    if (CheckRecordTypeMismatch(recordType, cmd, ref readInfo))
                        return false;
                }
                else if (cmd != RespCommand.GET && (cmd.IsRangeIndexCommand() || cmd.IsVectorSetCommand()))
                {
                    // RI/Vector command on a normal string key
                    readInfo.Action = ReadAction.WrongType;
                    return false;
                }
            }

            // GET is used in a number of non-RESP contexts, which messes up existing logic
            //
            // Easiest to mark the actually-RESP commands with a < 0 arg1 and roll back to old logic
            // after the Vector Set checks
            //
            // TODO: This is quite hacky, but requires a bunch of non-Vector Set changes - do those and remove
            if (input.arg1 < 0 && cmd == RespCommand.GET)
            {
                cmd = RespCommand.NONE;
            }

            var value = srcLogRecord.ValueSpan; // reduce redundant length calculations
            if (cmd == RespCommand.GETIFNOTMATCH)
            {
                if (handleGetIfNotMatch(in srcLogRecord, ref input, ref output, ref readInfo))
                    return true;
            }
            else if (cmd > RespCommandExtensions.LastValidCommand)
            {
                if (srcLogRecord.Info.HasETag)
                {
                    functionsState.CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref output.SpanByteAndMemory);
                    return true;
                }

                var valueLength = value.Length;

                var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
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
                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in srcLogRecord);

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
                CopyRespToWithInput(in srcLogRecord, ref input, ref output, readInfo.IsFromPending);

            if (srcLogRecord.Info.HasETag)
                ETagState.ResetState(ref functionsState.etagState);

            return true;
        }

        private bool handleGetIfNotMatch<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref StringInput input, ref StringOutput dst, ref ReadInfo readInfo)
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

        /// <summary>
        /// Checks for type mismatches between the record's RecordType and the command.
        /// Called only when RecordType != 0 (RI or Vector key). Separated from Reader
        /// to keep the hot path compact.
        /// </summary>
        private static bool CheckRecordTypeMismatch(byte recordType, RespCommand cmd, ref ReadInfo readInfo)
        {
            // RangeIndex type safety
            if (recordType == RangeIndexManager.RangeIndexRecordType && !cmd.IsLegalOnRangeIndex())
            {
                readInfo.Action = ReadAction.WrongType;
                return true;
            }
            if (recordType != RangeIndexManager.RangeIndexRecordType && cmd.IsRangeIndexCommand())
            {
                readInfo.Action = ReadAction.WrongType;
                return true;
            }

            // Vector set type safety
            if (recordType == VectorManager.RecordType && !cmd.IsLegalOnVectorSet())
            {
                readInfo.Action = ReadAction.WrongType;
                return true;
            }
            if (recordType != VectorManager.RecordType && cmd.IsVectorSetCommand())
            {
                readInfo.Action = ReadAction.WrongType;
                return true;
            }

            return false;
        }
    }
}