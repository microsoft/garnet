// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable 1591

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of ISessionFunctions
    /// </summary>
    public abstract class SessionFunctionsBase<TValue, TInput, TOutput, TContext> : ISessionFunctions<TValue, TInput, TOutput, TContext>
    {
        /// <inheritdoc/>
        public virtual bool ConcurrentReader(ref LogRecord<TValue> logRecord, ref TInput input, ref TOutput output, ref ReadInfo readInfo) => true;
        /// <inheritdoc/>
        public virtual bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord<TValue>
            => true;

        /// <inheritdoc/>
        public virtual bool ConcurrentWriter(ref LogRecord<TValue> logRecord, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            var ok = logRecord.IsObjectRecord
                ? logRecord.TrySetValueObject(srcValue)
                : logRecord.TrySetValueSpan(Unsafe.As<TValue, SpanByte>(ref srcValue));
            // This does not try to set ETag or Expiration, which will come from TInput in fuller implementations.
            return ok;
        }

        /// <inheritdoc/>
        public virtual bool SingleWriter(ref LogRecord<TValue> dstLogRecord, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            var ok = srcValue is IHeapObject heapObj
                ? dstLogRecord.TrySetValueObject(heapObj)
                : dstLogRecord.TrySetValueSpan(Unsafe.As<TValue, SpanByte>(ref srcValue));
            // This does not try to set ETag or Expiration, which will come from TInput in fuller implementations.
            return ok;
        }

        /// <inheritdoc/>
        public virtual bool SingleCopyWriter<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TValue> dstLogRecord, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord<TValue>
        {
            var ok = srcLogRecord.IsObjectRecord
                ? dstLogRecord.TrySetValueObject(srcLogRecord.ValueObject)
                : dstLogRecord.TrySetValueSpan(srcLogRecord.ValueSpan);
            if (!ok)
                return false;
            if (srcLogRecord.Info.HasETag && !dstLogRecord.TrySetETag(srcLogRecord.ETag))
                return false;
            if (srcLogRecord.Info.HasExpiration && !dstLogRecord.TrySetExpiration(srcLogRecord.Expiration))
                return false;
            return true;
        }

        /// <inheritdoc/>
        public virtual void PostSingleWriter(ref LogRecord<TValue> dstLogRecord, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        /// <inheritdoc/>
        public virtual bool InitialUpdater(ref LogRecord<TValue> dstLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;
        /// <inheritdoc/>
        public virtual void PostInitialUpdater(ref LogRecord<TValue> dstLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) { }
        /// <inheritdoc/>
        public virtual bool NeedInitialUpdate(SpanByte key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;

        /// <inheritdoc/>
        public virtual bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<TValue>
            => true;
        /// <inheritdoc/>
        public virtual bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TValue> dstLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<TValue>
            => true;
        /// <inheritdoc/>
        public virtual bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TValue> dstLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) 
            where TSourceLogRecord : ISourceLogRecord<TValue>
            => true;
        /// <inheritdoc/>
        public virtual bool InPlaceUpdater(ref LogRecord<TValue> logRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;

        /// <inheritdoc/>
        public virtual bool SingleDeleter(ref LogRecord<TValue> dstLogRecord, ref DeleteInfo deleteInfo)
        {
            _ = dstLogRecord.IsObjectRecord ? dstLogRecord.TrySetValueObject(default) : dstLogRecord.TrySetValueSpan(default);
            return true;
        }
        public virtual void PostSingleDeleter(ref LogRecord<TValue> dstLogRecord, ref DeleteInfo deleteInfo) { }
        public virtual bool ConcurrentDeleter(ref LogRecord<TValue> dstLogRecord, ref DeleteInfo deleteInfo) => true;

        public virtual void ReadCompletionCallback(ref LogRecord<TValue> logRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }
        /// <inheritdoc/>
        public virtual void RMWCompletionCallback(ref LogRecord<TValue> logRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }

        // *FieldInfo require an implementation that knows what is in IInput
        /// <inheritdoc/>
        public virtual RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input)
            where TSourceLogRecord : ISourceLogRecord<TValue>
            => throw new NotImplementedException("GetRMWModifiedFieldInfo");
        /// <inheritdoc/>
        public virtual RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref TInput input)
            => throw new NotImplementedException("GetRMWInitialFieldInfo");
        /// <inheritdoc/>
        public virtual RecordFieldInfo GetUpsertFieldInfo(SpanByte key, TValue value, ref TInput input)
            => throw new NotImplementedException("GetUpsertFieldInfo");

        /// <inheritdoc/>
        public virtual void ConvertOutputToHeap(ref TInput input, ref TOutput output) { }
    }

    /// <summary>
    /// Default simple functions base class with TInput and TOutput the same as TValue.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TContext"></typeparam>
    public class SimpleSessionFunctions<TValue, TContext> : SessionFunctionsBase<TValue, TValue, TValue, TContext>
    {
        /// <inheritdoc/>
        public virtual bool ConcurrentReader(ref LogRecord<TValue> logRecord, ref SpanByte input, ref SpanByte output, ref ReadInfo readInfo)
        {
            Debug.Assert(!logRecord.IsObjectRecord, "IHeapObject form of ConcurrentReader should not be called for SpanByte LogRecord");
            return logRecord.ValueSpan.TryCopyTo(ref output);
        }
        /// <inheritdoc/>
        public virtual bool ConcurrentReader(ref LogRecord<TValue> logRecord, ref IHeapObject input, ref TValue output, ref ReadInfo readInfo)
        {
            Debug.Assert(logRecord.IsObjectRecord, "SpanByte form of ConcurrentReader should not be called for Object LogRecord");
            output = logRecord.ValueObject;
            return true;
        }
        /// <inheritdoc/>
        public override bool ConcurrentReader(ref LogRecord<TValue> logRecord, ref TValue input, ref TValue output, ref ReadInfo readInfo)
        {
            if (logRecord.IsObjectRecord)
            {
                output = logRecord.ValueObject;
                return true;
            }
            return logRecord.ValueSpan.TryCopyTo(ref Unsafe.As<TValue, SpanByte>(ref output));
        }

        /// <inheritdoc/>
        public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TValue input, ref TValue output, ref ReadInfo readInfo)
        {
            if (srcLogRecord.IsObjectRecord)
            {
                output = srcLogRecord.ValueObject;
                return true;
            }
            return srcLogRecord.ValueSpan.TryCopyTo(ref Unsafe.As<TValue, SpanByte>(ref output));
        }

        public override bool SingleWriter(ref LogRecord<TValue> dstLogRecord, ref TValue input, TValue srcValue, ref TValue output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            var result = base.SingleWriter(ref dstLogRecord, ref input, srcValue, ref output, ref upsertInfo, reason);
            if (result)
                output = srcValue;
            return result;
        }

        public override bool ConcurrentWriter(ref LogRecord<TValue> logRecord, ref TValue input, TValue srcValue, ref TValue output, ref UpsertInfo upsertInfo)
        {
            var result = base.ConcurrentWriter(ref logRecord, ref input, srcValue, ref output, ref upsertInfo);
            if (result)
                output = srcValue;
            return result;
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref LogRecord<TValue> dstLogRecord, ref TValue input, ref TValue output, ref RMWInfo rmwInfo)
        {
            var ok = input is IHeapObject heapObj
               ? dstLogRecord.TrySetValueObject(heapObj)
               : dstLogRecord.TrySetValueSpan(Unsafe.As<TValue, SpanByte>(ref input));
            if (ok)
                output = input;
            return ok;
        }
        /// <inheritdoc/>
        public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TValue> dstLogRecord, ref TValue input, ref TValue output, ref RMWInfo rmwInfo)
        {
            // Simple base implementation does not use upsertInfo or WriteReason
            var upsertInfo = new UpsertInfo();
            return base.SingleCopyWriter(ref srcLogRecord, ref dstLogRecord, ref upsertInfo, WriteReason.Upsert);
        }
        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref LogRecord<TValue> logRecord, ref TValue input, ref TValue output, ref RMWInfo rmwInfo)
        {
            // Simple base implementation does not use upsertInfo
            var upsertInfo = new UpsertInfo();
            return ConcurrentWriter(ref logRecord, ref input, input, ref output, ref upsertInfo);
        }
    }
}