﻿// Copyright (c) Microsoft Corporation.
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
        public virtual bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        /// <inheritdoc/>
        public virtual bool InPlaceWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration, which will come from TInput in fuller implementations.
            return logRecord.TrySetValueSpan(srcValue, ref sizeInfo);
        }

        /// <inheritdoc/>
        public virtual bool InPlaceWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration, which will come from TInput in fuller implementations.
            return logRecord.TrySetValueObject(srcValue, ref sizeInfo);
        }

        public virtual bool InPlaceWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // This includes ETag and Expiration
            return dstLogRecord.TryCopyFrom(ref inputLogRecord, ref sizeInfo);
        }

        /// <inheritdoc/>
        public virtual bool InitialWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration, which will come from TInput in fuller implementations.
            return dstLogRecord.TrySetValueSpan(srcValue, ref sizeInfo);
        }

        /// <inheritdoc/>
        public virtual bool InitialWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration, which will come from TInput in fuller implementations.
            return dstLogRecord.TrySetValueObject(srcValue, ref sizeInfo);
        }

        /// <inheritdoc/>
        public virtual bool InitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // This includes ETag and Expiration
            return dstLogRecord.TryCopyFrom(ref inputLogRecord, ref sizeInfo);
        }

        /// <inheritdoc/>
        public virtual void PostInitialWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo) { }
        /// <inheritdoc/>
        public virtual void PostInitialWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo) { }
        /// <inheritdoc/>
        public virtual void PostInitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        { }

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
        public virtual bool InitialDeleter(ref LogRecord dstLogRecord, ref DeleteInfo deleteInfo)
        {
            if (dstLogRecord.ValueIsObject)
                dstLogRecord.ClearValueObject(_ => { });
            return true;
        }
        public virtual void PostInitialDeleter(ref LogRecord dstLogRecord, ref DeleteInfo deleteInfo) { }
        public virtual bool InPlaceDeleter(ref LogRecord dstLogRecord, ref DeleteInfo deleteInfo) => true;

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
        public virtual RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key, ref TSourceLogRecord inputLogRecord, ref TInput input)
            where TSourceLogRecord : ISourceLogRecord
        => throw new NotImplementedException("GetUpsertFieldInfo(ref TSourceLogRecord)");

        /// <inheritdoc/>
        public virtual void ConvertOutputToHeap(ref TInput input, ref TOutput output) { }
    }
}