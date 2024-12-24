// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool SingleWriter(ref LogRecord logRecord, ref RawStringInput input, SpanByte srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            => logRecord.StringLogRecord.TrySetValue(srcValue);

        /// <inheritdoc />
        public void PostSingleWriter(ref LogRecord logRecord, ref RawStringInput input, ref SpanByte srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (reason == WriteReason.Upsert && functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.StringLogRecord.Key, ref input, ref srcValue, upsertInfo.Version, upsertInfo.SessionID);
        }

        /// <inheritdoc />
        public bool ConcurrentWriter(ref LogRecord logRecord, ref RawStringInput input, SpanByte srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            if (ConcurrentWriterWorker(ref logRecord.StringLogRecord, srcValue, ref input, ref upsertInfo, ref recordInfo))
            {
                if (!logRecord.StringLogRecord.Info.Modified)
                    functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
                if (functionsState.appendOnlyFile != null)
                    WriteLogUpsert(logRecord.StringLogRecord.Key, ref input, ref srcValue, upsertInfo.Version, upsertInfo.SessionID);
                return true;
            }
            return false;
        }

        static bool ConcurrentWriterWorker(ref StringLogRecord logRecord, SpanByte srcValue, ref RawStringInput input, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            => logRecord.TrySetValue(srcValue);
    }
}