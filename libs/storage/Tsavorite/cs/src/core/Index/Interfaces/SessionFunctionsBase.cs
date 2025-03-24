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
        public virtual bool ConcurrentReader(ref LogRecord logRecord, ref TInput input, ref TOutput output, ref ReadInfo readInfo) => true;

        /// <inheritdoc/>
        public virtual bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        /// <inheritdoc/>
        public virtual bool ConcurrentWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration, which will come from TInput in fuller implementations.
            return logRecord.TrySetValueSpan(srcValue, ref sizeInfo);
        }

        /// <inheritdoc/>
        public virtual bool ConcurrentWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration, which will come from TInput in fuller implementations.
            return logRecord.TrySetValueObject(srcValue, ref sizeInfo);
        }

        /// <inheritdoc/>
        public virtual bool SingleWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            // This does not try to set ETag or Expiration, which will come from TInput in fuller implementations.
            return dstLogRecord.TrySetValueSpan(srcValue, ref sizeInfo);
        }

        /// <inheritdoc/>
        public virtual bool SingleWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            // This does not try to set ETag or Expiration, which will come from TInput in fuller implementations.
            return dstLogRecord.TrySetValueObject(srcValue, ref sizeInfo);
        }

        /// <inheritdoc/>
        public virtual bool SingleCopyWriter<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord
        {
            var ok = srcLogRecord.ValueIsObject
                ? dstLogRecord.TrySetValueObject(srcLogRecord.ValueObject, ref sizeInfo)
                : dstLogRecord.TrySetValueSpan(srcLogRecord.ValueSpan, ref sizeInfo);
            if (!ok)
                return false;
            if (srcLogRecord.Info.HasETag && !dstLogRecord.TrySetETag(srcLogRecord.ETag))
                return false;
            if (srcLogRecord.Info.HasExpiration && !dstLogRecord.TrySetExpiration(srcLogRecord.Expiration))
                return false;
            return true;
        }

        /// <inheritdoc/>
        public virtual void PostSingleWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        /// <inheritdoc/>
        public virtual void PostSingleWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        /// <inheritdoc/>
        public virtual bool InitialUpdater(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;
        /// <inheritdoc/>
        public virtual void PostInitialUpdater(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) { }
        /// <inheritdoc/>
        public virtual bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;

        /// <inheritdoc/>
        public virtual bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;
        /// <inheritdoc/>
        public virtual bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;
        /// <inheritdoc/>
        public virtual bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) 
            where TSourceLogRecord : ISourceLogRecord
            => true;
        /// <inheritdoc/>
        public virtual bool InPlaceUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;

        /// <inheritdoc/>
        public virtual bool SingleDeleter(ref LogRecord dstLogRecord, ref DeleteInfo deleteInfo)
        {
            if (dstLogRecord.ValueIsObject)
                dstLogRecord.ClearValueObject(_ => { });
            return true;
        }
        public virtual void PostSingleDeleter(ref LogRecord dstLogRecord, ref DeleteInfo deleteInfo) { }
        public virtual bool ConcurrentDeleter(ref LogRecord dstLogRecord, ref DeleteInfo deleteInfo) => true;

        public virtual void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }
        /// <inheritdoc/>
        public virtual void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }

        // *FieldInfo require an implementation that knows what is in IInput
        /// <inheritdoc/>
        public virtual RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input) where TSourceLogRecord : ISourceLogRecord
            => throw new NotImplementedException("GetRMWModifiedFieldInfo");
        /// <inheritdoc/>
        public virtual RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref TInput input) => throw new NotImplementedException("GetRMWInitialFieldInfo");
        /// <inheritdoc/>
        public virtual RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref TInput input) => throw new NotImplementedException("GetUpsertFieldInfo(Span<byte>)");
        /// <inheritdoc/>
        public virtual RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref TInput input) => throw new NotImplementedException("GetUpsertFieldInfo(IHeapObject)");

        /// <inheritdoc/>
        public virtual void ConvertOutputToHeap(ref TInput input, ref TOutput output) { }
    }
}