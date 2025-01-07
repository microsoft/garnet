// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable 1591

using System;
using System.Diagnostics;

namespace Tsavorite.core
{
    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of ISessionFunctions
    /// </summary>
    public abstract class SessionFunctionsBase<TValue, TInput, TOutput, TContext> : ISessionFunctions<TValue, TInput, TOutput, TContext>
    {
        /// <inheritdoc/>
        public virtual bool ConcurrentReader(ref LogRecord logRecord, ref TInput input, ref TOutput dst, ref ReadInfo readInfo) => true;
        /// <inheritdoc/>
        public virtual bool SingleReader(ref LogRecord logRecord, ref TInput input, ref TOutput dst, ref ReadInfo readInfo) => true;

        /// <inheritdoc/>
        public virtual bool ConcurrentWriter(ref LogRecord logRecord, ref TInput input, SpanByte srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            Debug.Assert(!logRecord.IsObjectRecord, "SpanByte form of ConcurrentWriter should not be called for Object LogRecord");
            return logRecord.TrySetValueSpan(srcValue);
        }
        /// <inheritdoc/>
        public virtual bool ConcurrentWriter(ref LogRecord logRecord, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            Debug.Assert(logRecord.IsObjectRecord, "IHeapObject form of ConcurrentWriter should not be called for String LogRecord");
            return logRecord.TrySetValueObject(srcValue);
        }
        /// <inheritdoc/>
        public virtual bool ConcurrentWriter(ref LogRecord logRecord, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            Debug.Fail("Generic form of ConcurrentWriter should not be called");
            return false;
        }

        /// <inheritdoc/>
        public virtual bool SingleWriter(ref LogRecord logRecord, ref TInput input, SpanByte srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            Debug.Assert(logRecord.IsObjectRecord, "IHeapObject form of SingleWriter should not be called for String LogRecord");
            return logRecord.TrySetValueSpan(srcValue);
        }
        /// <inheritdoc/>
        public virtual bool SingleWriter(ref LogRecord logRecord, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            Debug.Assert(logRecord.IsObjectRecord, "IHeapObject form of SingleWriter should not be called for String LogRecord");
            return logRecord.TrySetValueObject(srcValue);
        }
        /// <inheritdoc/>
        public virtual bool SingleWriter(ref LogRecord logRecord, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            Debug.Fail("Generic form of SingleWriter should not be called");
            return false;
        }

        /// <inheritdoc/>
        public virtual void PostSingleWriter(ref LogRecord logRecord, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        /// <inheritdoc/>
        public virtual bool InitialUpdater(ref LogRecord logRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;
        /// <inheritdoc/>
        public virtual void PostInitialUpdater(ref LogRecord logRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) { }
        /// <inheritdoc/>
        public virtual bool NeedInitialUpdate(SpanByte key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;

        /// <inheritdoc/>
        public virtual bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : IReadOnlyLogRecord
            => true;
        /// <inheritdoc/>
        public virtual bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref TInput input, ref TValue newValue, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : IReadOnlyLogRecord
            => true;
        /// <inheritdoc/>
        public virtual bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) 
            where TSourceLogRecord : IReadOnlyLogRecord
            => true;
        /// <inheritdoc/>
        public virtual bool InPlaceUpdater(ref LogRecord logRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;

        /// <inheritdoc/>
        public virtual bool SingleDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            _ = logRecord.IsObjectRecord ? logRecord.TrySetValueObject(default) : logRecord.TrySetValueSpan(default);
            return true;
        }
        public virtual void PostSingleDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) { }
        public virtual bool ConcurrentDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo) => true;

        public virtual void ReadCompletionCallback(ref LogRecord logRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }
        /// <inheritdoc/>
        public virtual void RMWCompletionCallback(ref LogRecord logRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }

        // *FieldInfo require an implementation that knows what is in IInput
        /// <inheritdoc/>
        public abstract RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input) where TSourceLogRecord : IReadOnlyLogRecord;
        /// <inheritdoc/>
        public abstract RecordFieldInfo GetRMWInitialFieldInfo(ref TInput input);
        /// <inheritdoc/>
        public abstract RecordFieldInfo GetUpsertFieldInfo(TValue value, ref TInput input);

        /// <inheritdoc/>
        public virtual void ConvertOutputToHeap(ref TInput input, ref TOutput output) { }
    }

    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of FunctionsBase
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TContext"></typeparam>
    public class SimpleSessionFunctions<TValue, TContext> : SessionFunctionsBase<TValue, TValue, TValue, TContext>
    {
        private readonly Func<TValue, TValue, TValue> merger;
        public SimpleSessionFunctions() => merger = (l, r) => l;
        public SimpleSessionFunctions(Func<TValue, TValue, TValue> merger) => this.merger = merger;

        /// <inheritdoc/>
        public virtual bool ConcurrentReader(ref LogRecord logRecord, ref SpanByte input, ref SpanByte output, ref ReadInfo readInfo)
        {
            Debug.Assert(!logRecord.IsObjectRecord, "IHeapObject form of ConcurrentReader should not be called for SpanByte LogRecord");
            return logRecord.ValueSpan.TryCopyTo(ref output);
        }
        /// <inheritdoc/>
        public virtual bool ConcurrentReader(ref LogRecord logRecord, ref IHeapObject input, ref IHeapObject output, ref ReadInfo readInfo)
        {
            Debug.Assert(logRecord.IsObjectRecord, "SpanByte form of ConcurrentReader should not be called for Object LogRecord");
            output = logRecord.ValueObject;
            return true;
        }
        /// <inheritdoc/>
        public override bool ConcurrentReader(ref LogRecord logRecord, ref TValue input, ref TValue output, ref ReadInfo readInfo)
        {
            Debug.Fail("Generic form of ConcurrentReader should not be called");
            return false;
        }





        /// <inheritdoc/>
        public override bool SingleReader(ref TKey key, ref TValue input, ref TValue value, ref TValue dst, ref ReadInfo readInfo)
        {
            dst = value;
            return true;
        }

        public override bool SingleWriter(ref TKey key, ref TValue input, ref TValue src, ref TValue dst, ref TValue output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            var result = base.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);
            if (result)
                output = dst;
            return result;
        }

        public override bool ConcurrentWriter(ref TKey key, ref TValue input, ref TValue src, ref TValue dst, ref TValue output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            var result = base.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, ref recordInfo);
            if (result)
                output = dst;
            return result;
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref TKey key, ref TValue input, ref TValue value, ref TValue output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) { value = output = input; return true; }
        /// <inheritdoc/>
        public override bool CopyUpdater(ref TKey key, ref TValue input, ref TValue oldValue, ref TValue newValue, ref TValue output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) { newValue = output = merger(input, oldValue); return true; }
        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref TKey key, ref TValue input, ref TValue value, ref TValue output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) { value = output = merger(input, value); return true; }
    }

    public class SimpleSimpleFunctions<TKey, TValue> : SimpleSessionFunctions<TKey, TValue, Empty>
    {
        public SimpleSimpleFunctions() : base() { }
        public SimpleSimpleFunctions(Func<TValue, TValue, TValue> merger) : base(merger) { }
    }
}