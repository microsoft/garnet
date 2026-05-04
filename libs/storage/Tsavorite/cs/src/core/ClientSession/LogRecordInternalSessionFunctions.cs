// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Session functions for internal log record copy operations (compaction, iteration).
    /// Implements Upsert callbacks that copy source log records to destination records via TryCopyFrom.
    /// Also implements Delete (return true) for compaction. All other methods are no-op or throw.
    /// </summary>
    /// <typeparam name="TSourceLogRecord">The source log record type (e.g. ITsavoriteScanIterator for compaction/iteration)</typeparam>
    internal struct LogRecordInternalSessionFunctions<TSourceLogRecord> : ISessionFunctions<LogRecordInput<TSourceLogRecord>, Empty, Empty>
        where TSourceLogRecord : ISourceLogRecord
    {
        public readonly bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) => true;

        public readonly void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) { }

        public readonly bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) => true;

        public readonly bool InPlaceWriter(ref LogRecord dstLogRecord, ref LogRecordInput<TSourceLogRecord> input, ref Empty output, ref UpsertInfo upsertInfo)
        {
            var sizeInfo = new RecordSizeInfo() { FieldInfo = GetUpsertFieldInfo(key: dstLogRecord, ref input) };
            dstLogRecord.PopulateRecordSizeInfoForIPU(ref sizeInfo);
            return dstLogRecord.TryCopyFrom(in input.SourceRecord, in sizeInfo);
        }

        public readonly bool CopyUpdater<TCopySourceLogRecord>(in TCopySourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref LogRecordInput<TSourceLogRecord> input, ref Empty output, ref RMWInfo rmwInfo)
            where TCopySourceLogRecord : ISourceLogRecord
            => true;

        public readonly bool PostCopyUpdater<TCopySourceLogRecord>(in TCopySourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref LogRecordInput<TSourceLogRecord> input, ref Empty output, ref RMWInfo rmwInfo)
            where TCopySourceLogRecord : ISourceLogRecord
            => true;

        public readonly bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref LogRecordInput<TSourceLogRecord> input, ref Empty output, ref RMWInfo rmwInfo) => true;
        public readonly void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref LogRecordInput<TSourceLogRecord> input, ref Empty output, ref RMWInfo rmwInfo) { }

        public readonly bool InPlaceUpdater(ref LogRecord logRecord, ref LogRecordInput<TSourceLogRecord> input, ref Empty output, ref RMWInfo rmwInfo) => true;

        public readonly bool NeedInitialUpdate<TKey>(TKey key, ref LogRecordInput<TSourceLogRecord> input, ref Empty output, ref RMWInfo rmwInfo)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => true;

        public readonly bool NeedCopyUpdate<TCopySourceLogRecord>(in TCopySourceLogRecord srcLogRecord, ref LogRecordInput<TSourceLogRecord> input, ref Empty output, ref RMWInfo rmwInfo)
            where TCopySourceLogRecord : ISourceLogRecord
            => true;

        public readonly void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref LogRecordInput<TSourceLogRecord> input, ref Empty output, Empty ctx, Status status, RecordMetadata recordMetadata) { }

        public readonly void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref LogRecordInput<TSourceLogRecord> input, ref Empty output, Empty ctx, Status status, RecordMetadata recordMetadata) { }

        public readonly RecordFieldInfo GetRMWModifiedFieldInfo<TCopySourceLogRecord>(in TCopySourceLogRecord srcLogRecord, ref LogRecordInput<TSourceLogRecord> input)
            where TCopySourceLogRecord : ISourceLogRecord
             => throw new NotImplementedException("GetRMWModifiedFieldInfo is not supported in LogRecordInternalSessionFunctions");
        public readonly RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref LogRecordInput<TSourceLogRecord> input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => throw new NotImplementedException("GetRMWInitialFieldInfo is not supported in LogRecordInternalSessionFunctions");

        public readonly RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ref LogRecordInput<TSourceLogRecord> input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => new() { KeySize = key.KeyBytes.Length, ValueSize = input.SourceRecord.Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : input.SourceRecord.ValueSpan.Length, ValueIsObject = input.SourceRecord.Info.ValueIsObject };

        /// <summary>
        /// No reads during compaction
        /// </summary>
        public readonly bool Reader<TCopySourceLogRecord>(in TCopySourceLogRecord srcLogRecord, ref LogRecordInput<TSourceLogRecord> input, ref Empty dst, ref ReadInfo readInfo)
            where TCopySourceLogRecord : ISourceLogRecord
            => true;

        /// <summary>
        /// Write compacted live value to store
        /// </summary>
        public readonly bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref LogRecordInput<TSourceLogRecord> input, ref Empty output, ref UpsertInfo upsertInfo)
        {
            return dstLogRecord.TryCopyFrom(in input.SourceRecord, in sizeInfo);
        }

        public readonly void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref LogRecordInput<TSourceLogRecord> input, ref Empty output, ref UpsertInfo upsertInfo) { }

        public readonly void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref LogRecordInput<TSourceLogRecord> input, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        { }
        public readonly void PostRMWOperation<TKey, TEpochAccessor>(TKey key, ref LogRecordInput<TSourceLogRecord> input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        { }
        public readonly void PostDeleteOperation<TKey, TEpochAccessor>(TKey key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        { }

        public readonly void ConvertOutputToHeap(ref LogRecordInput<TSourceLogRecord> input, ref Empty output) { }

        public readonly void BeforeConsistentReadCallback(long hash) { }

        public readonly void AfterConsistentReadKeyCallback() { }

        /// <inheritdoc />
        public readonly void BeforeConsistentReadKeyBatchCallback(ReadOnlySpan<PinnedSpanByte> parameters) { }

        public readonly bool AfterConsistentReadKeyBatchCallback(int keyCount) => true;
    }
}
