// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool SingleWriter(ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, SpanByte srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            if (!dstLogRecord.TrySetValueSpan(srcValue, ref sizeInfo))
                return false;
            if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                return false;
            sizeInfo.AssertOptionals(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public bool SingleCopyWriter<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
            => dstLogRecord.TryCopyRecordValues(ref srcLogRecord, ref sizeInfo);

        /// <inheritdoc />
        public void PostSingleWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, SpanByte srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (reason == WriteReason.Upsert && functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, ref srcValue, upsertInfo.Version, upsertInfo.SessionID);
        }

        /// <inheritdoc />
        public bool ConcurrentWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, SpanByte srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            if (ConcurrentWriterWorker(ref logRecord, ref sizeInfo, srcValue, ref input, ref output, ref upsertInfo))
            {
                if (!logRecord.Info.Modified)
                    functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
                if (functionsState.appendOnlyFile != null)
                    WriteLogUpsert(logRecord.Key, ref input, ref srcValue, upsertInfo.Version, upsertInfo.SessionID);
                return true;
            }
            return false;
        }

        bool ConcurrentWriterWorker(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, SpanByte srcValue, ref RawStringInput input, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            if (!logRecord.TrySetValueSpan(srcValue, ref sizeInfo))
                return false;
            var ok = input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1);
            if (ok)
            {
                if (input.header.CheckWithETagFlag())
                {
                    var newETag = functionsState.etagState.ETag + 1;
                    ok = logRecord.TrySetETag(newETag);
                    if (ok)
                        functionsState.CopyRespNumber(newETag, ref output);
                }
                else
                    ok = logRecord.RemoveETag();
            }
            if (ok)
                sizeInfo.AssertOptionals(logRecord.Info);
            return ok;
        }
    }
}