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
            var info = srcLogRecord.Info;

            // Fast path for simple GET on a normal inline string key with no optional fields.
            // HasOptionalOrObjectFields is false iff: KeyIsInline, ValueIsInline, !HasETag, !HasExpiration (implies !ValueIsObject).
            // RecordType 0 means normal string (not VectorSet or RangeIndex).
            // This avoids expiry checks (no expiration), type-safety checks, ETag handling, and custom command dispatch.
            if (input.arg1 < 0 && !info.HasOptionalOrObjectFields && srcLogRecord.RecordType == 0)
            {
                CopyRespTo(srcLogRecord.ValueSpan, ref output);
                return true;
            }

            if (info.ValueIsObject)
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

            if (cmd > RespCommandExtensions.LastValidCommand)
            {
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

            switch (cmd)
            {
                case RespCommand.GETIFNOTMATCH:
                case RespCommand.GETWITHETAG:
                    return HandleEtagReader(in srcLogRecord, ref input, ref output, ref readInfo, cmd, value);
                case RespCommand.NONE:
                    CopyRespTo(value, ref output);
                    break;
                default:
                    CopyRespToWithInput(in srcLogRecord, ref input, ref output, readInfo.IsFromPending);
                    break;
            }

            return true;
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