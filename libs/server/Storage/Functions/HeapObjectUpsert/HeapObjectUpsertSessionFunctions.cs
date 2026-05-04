// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Session functions for heap object upsert operations with optional AOF logging.
    /// Used when inserting IHeapObject instances directly into the store.
    /// </summary>
    internal readonly unsafe partial struct HeapObjectUpsertSessionFunctions : ISessionFunctions<HeapObjectInput, Empty, Empty>
    {
        const byte NeedAofLog = 0x1;
        readonly FunctionsState functionsState;

        internal HeapObjectUpsertSessionFunctions(FunctionsState functionsState)
        {
            this.functionsState = functionsState;
        }

        /// <inheritdoc />
        public bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref HeapObjectInput input, ref Empty output, ref UpsertInfo upsertInfo)
            => dstLogRecord.TrySetValueObjectAndPrepareOptionals(input.heapObject, in sizeInfo);

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref HeapObjectInput input, ref Empty output, ref UpsertInfo upsertInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog;
        }

        /// <inheritdoc />
        public bool InPlaceWriter(ref LogRecord logRecord, ref HeapObjectInput input, ref Empty output, ref UpsertInfo upsertInfo)
            => false; // Force copy-to-tail for object replacement

        /// <inheritdoc />
        public void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref HeapObjectInput input, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        {
            if ((upsertInfo.UserData & NeedAofLog) == NeedAofLog)
                WriteLogUpsert(key.KeyBytes, input, upsertInfo.Version, upsertInfo.SessionID, epochAccessor);
        }

        void WriteLogUpsert<TEpochAccessor>(ReadOnlySpan<byte> key, HeapObjectInput input, long version, int sessionId, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if (functionsState.StoredProcMode)
                return;

            GarnetObjectSerializer.Serialize((IGarnetObject)input.heapObject, out var valueBytes);
            fixed (byte* valPtr = valueBytes)
            {
                functionsState.appendOnlyFile.Log.Enqueue(
                    AofEntryType.UnifiedStoreObjectUpsert,
                    version,
                    sessionId,
                    key,
                    new ReadOnlySpan<byte>(valPtr, valueBytes.Length),
                    epochAccessor,
                    out _);
            }
        }

        /// <inheritdoc />
        public RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ref HeapObjectInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => new()
            {
                KeySize = key.KeyBytes.Length,
                ValueSize = ObjectIdMap.ObjectIdSize,
                ValueIsObject = true
            };

        /// <inheritdoc />
        public readonly bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) => true;

        /// <inheritdoc />
        public readonly void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) { }

        /// <inheritdoc />
        public readonly bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) => true;

        /// <inheritdoc />
        public readonly void PostDeleteOperation<TKey, TEpochAccessor>(TKey key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        { }

        /// <inheritdoc />
        public readonly bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref HeapObjectInput input, ref Empty output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        /// <inheritdoc />
        public readonly void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref HeapObjectInput input, ref Empty output, Empty ctx, Status status, RecordMetadata recordMetadata) { }

        /// <inheritdoc />
        public readonly bool NeedInitialUpdate<TKey>(TKey key, ref HeapObjectInput input, ref Empty output, ref RMWInfo rmwInfo)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => true;

        /// <inheritdoc />
        public readonly bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref HeapObjectInput input, ref Empty output, ref RMWInfo rmwInfo) => true;

        /// <inheritdoc />
        public readonly void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref HeapObjectInput input, ref Empty output, ref RMWInfo rmwInfo) { }

        /// <inheritdoc />
        public readonly bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref HeapObjectInput input, ref Empty output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        /// <inheritdoc />
        public readonly bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref HeapObjectInput input, ref Empty output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        /// <inheritdoc />
        public readonly bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref HeapObjectInput input, ref Empty output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        /// <inheritdoc />
        public readonly bool InPlaceUpdater(ref LogRecord logRecord, ref HeapObjectInput input, ref Empty output, ref RMWInfo rmwInfo) => true;

        /// <inheritdoc />
        public readonly void PostRMWOperation<TKey, TEpochAccessor>(TKey key, ref HeapObjectInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        { }

        /// <inheritdoc />
        public readonly void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref HeapObjectInput input, ref Empty output, Empty ctx, Status status, RecordMetadata recordMetadata) { }

        /// <inheritdoc />
        public readonly RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref HeapObjectInput input)
            where TSourceLogRecord : ISourceLogRecord
            => throw new NotImplementedException("GetRMWModifiedFieldInfo is not supported in HeapObjectUpsertSessionFunctions");

        /// <inheritdoc />
        public readonly RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref HeapObjectInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => throw new NotImplementedException("GetRMWInitialFieldInfo is not supported in HeapObjectUpsertSessionFunctions");

        /// <inheritdoc />
        public readonly void ConvertOutputToHeap(ref HeapObjectInput input, ref Empty output) { }

        /// <inheritdoc />
        public readonly void BeforeConsistentReadCallback(long hash) { }

        /// <inheritdoc />
        public readonly void AfterConsistentReadKeyCallback() { }

        /// <inheritdoc />
        public readonly void BeforeConsistentReadKeyBatchCallback(ReadOnlySpan<PinnedSpanByte> parameters) { }

        /// <inheritdoc />
        public readonly bool AfterConsistentReadKeyBatchCallback(int keyCount) => true;
    }
}
