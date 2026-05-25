// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable 1591

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of ISessionFunctions
    /// </summary>
    public abstract class SessionFunctionsBase<TInput, TOutput, TContext> : ISessionFunctions<TInput, TOutput, TContext>
    {
        /// <inheritdoc/>
        public virtual bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        /// <inheritdoc/>
        public virtual bool InPlaceWriter(ref LogRecord logRecord, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration, which will come from TInput in fuller implementations.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = GetUpsertFieldInfo(key: logRecord, srcValue, ref input) };
            logRecord.PopulateRecordSizeInfoForIPU(ref sizeInfo);
            return logRecord.TrySetValueSpanAndPrepareOptionals(srcValue, in sizeInfo);
        }

        /// <inheritdoc/>
        public virtual bool InPlaceWriter(ref LogRecord logRecord, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration, which will come from TInput in fuller implementations.
            var sizeInfo = new RecordSizeInfo() { FieldInfo = GetUpsertFieldInfo(key: logRecord, srcValue, ref input) };
            logRecord.PopulateRecordSizeInfoForIPU(ref sizeInfo);
            return logRecord.TrySetValueObjectAndPrepareOptionals(srcValue, in sizeInfo);
        }

        public virtual bool InPlaceWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // This includes ETag and Expiration
            var sizeInfo = new RecordSizeInfo() { FieldInfo = GetUpsertFieldInfo(key: dstLogRecord, inputLogRecord, ref input) };
            dstLogRecord.PopulateRecordSizeInfoForIPU(ref sizeInfo);
            return dstLogRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);
        }

        /// <inheritdoc/>
        public virtual bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration, which will come from TInput in fuller implementations.
            return dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcValue, in sizeInfo);
        }

        /// <inheritdoc/>
        public virtual bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration, which will come from TInput in fuller implementations.
            return dstLogRecord.TrySetValueObjectAndPrepareOptionals(srcValue, in sizeInfo);
        }

        /// <inheritdoc/>
        public virtual bool InitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // This includes ETag and Expiration
            return dstLogRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);
        }

        public virtual void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref TInput input, ReadOnlySpan<byte> valueSpan, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        { }
        public virtual void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref TInput input, IHeapObject valueObject, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        { }

        /// <inheritdoc/>
        public virtual void PostInitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo) { }
        /// <inheritdoc/>
        public virtual void PostInitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo) { }
        /// <inheritdoc/>
        public virtual void PostInitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        { }

        /// <inheritdoc/>
        public virtual bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;
        /// <inheritdoc/>
        public virtual void PostInitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) { }
        /// <inheritdoc/>
        public virtual bool NeedInitialUpdate<TKey>(TKey key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => true;

        /// <inheritdoc/>
        public virtual bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        /// <inheritdoc/>
        public virtual void PostRMWOperation<TKey, TEpochAccessor>(TKey key, ref TInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        { }

        /// <inheritdoc/>
        public virtual bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;
        /// <inheritdoc/>
        public virtual bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;
        /// <inheritdoc/>
        public virtual bool InPlaceUpdater(ref LogRecord logRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;

        /// <inheritdoc/>
        public virtual bool InitialDeleter(ref LogRecord dstLogRecord, ref DeleteInfo deleteInfo)
        {
            dstLogRecord.ClearValueIfHeap();
            return true;
        }
        public virtual void PostInitialDeleter(ref LogRecord dstLogRecord, ref DeleteInfo deleteInfo) { }
        public virtual bool InPlaceDeleter(ref LogRecord dstLogRecord, ref DeleteInfo deleteInfo) => true;

        /// <inheritdoc/>
        public virtual void PostDeleteOperation<TKey, TEpochAccessor>(TKey key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        { }


        public virtual void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }
        /// <inheritdoc/>
        public virtual void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }

        // *FieldInfo require an implementation that knows what is in IInput
        /// <inheritdoc/>
        public virtual RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input)
            where TSourceLogRecord : ISourceLogRecord
            => throw new NotImplementedException("GetRMWModifiedFieldInfo requires knowledge of TInput");
        /// <inheritdoc/>
        public virtual RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref TInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => throw new NotImplementedException("GetRMWInitialFieldInfo requires knowledge of TInput");
        /// <inheritdoc/>
        public virtual RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ReadOnlySpan<byte> value, ref TInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            // TODO: Namespace!
            => new() { KeySize = key.KeyBytes.Length, ValueSize = value.Length, ValueIsObject = false };
        /// <inheritdoc/>
        public virtual RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, IHeapObject value, ref TInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            // TODO: Namespace!
            => new() { KeySize = key.KeyBytes.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        /// <inheritdoc/>
        public virtual RecordFieldInfo GetUpsertFieldInfo<TKey, TSourceLogRecord>(TKey key, in TSourceLogRecord inputLogRecord, ref TInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord
            // TODO: Namespace!
            => new() { KeySize = key.KeyBytes.Length, ValueSize = inputLogRecord.Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : inputLogRecord.ValueSpan.Length, ValueIsObject = inputLogRecord.Info.ValueIsObject };

        /// <inheritdoc/>
        public virtual void ConvertOutputToHeap(ref TInput input, ref TOutput output) { }

        public virtual void BeforeConsistentReadCallback(long hash) { }

        public virtual void AfterConsistentReadKeyCallback() { }

        public virtual void BeforeConsistentReadKeyBatchCallback(ReadOnlySpan<PinnedSpanByte> parameters) { }

        public virtual bool AfterConsistentReadKeyBatchCallback(int keyCount) => true;
    }
}