// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable 1591

using System;
using System.Runtime.CompilerServices;

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
        public virtual bool ConcurrentWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, Span<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
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
        public virtual bool SingleWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, Span<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
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
        public virtual void PostSingleWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, Span<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason) { }

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
        public virtual RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, Span<byte> value, ref TInput input) => throw new NotImplementedException("GetUpsertFieldInfo(Span<byte>)");
        /// <inheritdoc/>
        public virtual RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref TInput input) => throw new NotImplementedException("GetUpsertFieldInfo(IHeapObject)");

        /// <inheritdoc/>
        public virtual void ConvertOutputToHeap(ref TInput input, ref TOutput output) { }
    }

    /// <summary>
    /// Default simple functions base class with TInput and TOutput the same as TValue.
    /// </summary>
    public struct SimpleSessionFunctions<TContext> : ISessionFunctions<Span<byte>, Span<byte>, TContext>
    {
        /// <inheritdoc/>
        public override bool ConcurrentReader(ref LogRecord logRecord, ref TValue input, ref TValue output, ref ReadInfo readInfo)
        {
            if (logRecord.ValueIsObject)
            {
                output = logRecord.ValueObject;
                return true;
            }
            return logRecord.ValueSpan.TryCopyTo(Unsafe.As<SpanByte>(ref output));
        }

        /// <inheritdoc/>
        public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TValue input, ref TValue output, ref ReadInfo readInfo)
        {
            if (srcLogRecord.ValueIsObject)
            {
                output = srcLogRecord.ValueObject;
                return true;
            }
            return srcLogRecord.ValueSpan.TryCopyTo(Unsafe.As<SpanByte>(ref output));
        }

        public override bool SingleWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TValue input, TValue srcValue, ref TValue output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            var result = base.SingleWriter(ref dstLogRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo, reason);
            if (result)
                output = srcValue;
            return result;
        }

        public override bool ConcurrentWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TValue input, TValue srcValue, ref TValue output, ref UpsertInfo upsertInfo)
        {
            var result = base.ConcurrentWriter(ref logRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            if (result)
                output = srcValue;
            return result;
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TValue input, ref TValue output, ref RMWInfo rmwInfo)
        {
            var ok = input is IHeapObject heapObj
               ? dstLogRecord.TrySetValueObject((TValue)heapObj, ref sizeInfo)
               : dstLogRecord.TrySetValueSpan(Unsafe.As<Span<byte>>(ref input), ref sizeInfo);
            if (ok)
                output = input;
            return ok;
        }
        /// <inheritdoc/>
        public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TValue input, ref TValue output, ref RMWInfo rmwInfo)
        {
            // Simple base implementation does not use upsertInfo or WriteReason
            var upsertInfo = new UpsertInfo();
            return base.SingleCopyWriter(ref srcLogRecord, ref dstLogRecord, ref sizeInfo, ref input, ref output, ref upsertInfo, WriteReason.Upsert);
        }
        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TValue input, ref TValue output, ref RMWInfo rmwInfo)
        {
            // Simple base implementation does not use upsertInfo
            var upsertInfo = new UpsertInfo();
            return ConcurrentWriter(ref logRecord, ref sizeInfo, ref input, input, ref output, ref upsertInfo);
        }
    }
}