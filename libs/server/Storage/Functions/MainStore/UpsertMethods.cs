﻿// Copyright (c) Microsoft Corporation.
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
        public bool SingleWriter(ref LogRecord<SpanByte> dstLogRecord, ref RawStringInput input, SpanByte srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
            => dstLogRecord.TrySetValueSpan(srcValue);

        /// <inheritdoc />
        public bool SingleCopyWriter<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
            => dstLogRecord.TrySetValueSpan(srcLogRecord.ValueSpan);

        /// <inheritdoc />
        public void PostSingleWriter(ref LogRecord<SpanByte> logRecord, ref RawStringInput input, SpanByte srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (reason == WriteReason.Upsert && functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, ref srcValue, upsertInfo.Version, upsertInfo.SessionID);
        }

        /// <inheritdoc />
        public bool ConcurrentWriter(ref LogRecord<SpanByte> logRecord, ref RawStringInput input, SpanByte srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            if (ConcurrentWriterWorker(ref logRecord, srcValue, ref input, ref upsertInfo))
            {
                if (!logRecord.Info.Modified)
                    functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
                if (functionsState.appendOnlyFile != null)
                    WriteLogUpsert(logRecord.Key, ref input, ref srcValue, upsertInfo.Version, upsertInfo.SessionID);
                return true;
            }
            return false;
        }

        static bool ConcurrentWriterWorker(ref LogRecord<SpanByte> logRecord, SpanByte srcValue, ref RawStringInput input, ref UpsertInfo upsertInfo)
            => logRecord.TrySetValueSpan(srcValue);
    }
}