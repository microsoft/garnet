﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    internal sealed class LogCompactionFunctions<TValue, TInput, TOutput, TContext, TFunctions> : ISessionFunctions<TValue, TInput, TOutput, TContext>
        where TFunctions : ISessionFunctions<TValue, TInput, TOutput, TContext>
    {
        readonly TFunctions _functions;

        public LogCompactionFunctions(TFunctions functions)
        {
            _functions = functions;
        }

        /// <summary>
        /// No reads during compaction
        /// </summary>
        public bool ConcurrentReader(ref LogRecord<TValue> logRecord, ref TInput input, ref TOutput output, ref ReadInfo readInfo) => true;

        public bool SingleDeleter(ref LogRecord<TValue> logRecord, ref DeleteInfo deleteInfo) => true;

        public void PostSingleDeleter(ref LogRecord<TValue> logRecord, ref DeleteInfo deleteInfo) { }

        /// <summary>
        /// No ConcurrentDeleter needed for compaction
        /// </summary>
        public bool ConcurrentDeleter(ref LogRecord<TValue> logRecord, ref DeleteInfo deleteInfo) => true;

        /// <summary>
        /// For compaction, we never perform concurrent writes as rolled over data defers to newly inserted data for the same key.
        /// </summary>
        public bool ConcurrentWriter(ref LogRecord<TValue> logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo) => true;

        public bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TValue> dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<TValue>
            => true;

        public bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TValue> dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<TValue>
            => true;

        public bool InitialUpdater(ref LogRecord<TValue> logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;
        public void PostInitialUpdater(ref LogRecord<TValue> logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) { }

        public bool InPlaceUpdater(ref LogRecord<TValue> logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;

        public bool NeedInitialUpdate(SpanByte key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;

        public bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<TValue>
            => true;

        public void ReadCompletionCallback(ref DiskLogRecord<TValue> diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }

        public void RMWCompletionCallback(ref DiskLogRecord<TValue> diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }

        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input)
            where TSourceLogRecord : ISourceLogRecord<TValue>
            => default;
        public RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref TInput input) => default;
        public RecordFieldInfo GetUpsertFieldInfo(SpanByte key, TValue value, ref TInput input) => _functions.GetUpsertFieldInfo(key, value, ref input);

        /// <summary>
        /// No reads during compaction
        /// </summary>
        public bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput dst, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord<TValue>
            => true;

        /// <summary>
        /// Write compacted live value to store
        /// </summary>
        public bool SingleWriter(ref LogRecord<TValue> logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            => _functions.SingleWriter(ref logRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo, reason);

        public bool SingleCopyWriter<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TValue> dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord<TValue>
            => _functions.SingleCopyWriter(ref srcLogRecord, ref dstLogRecord, ref sizeInfo, ref input, ref output, ref upsertInfo, reason);

        public void PostSingleWriter(ref LogRecord<TValue> logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        public void ConvertOutputToHeap(ref TInput input, ref TOutput output) { }
    }
}