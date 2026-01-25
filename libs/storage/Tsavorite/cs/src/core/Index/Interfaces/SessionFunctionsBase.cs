// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable 1591

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of ISessionFunctions
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TInput"></typeparam>
    /// <typeparam name="TOutput"></typeparam>
    /// <typeparam name="TContext"></typeparam>
    public abstract class SessionFunctionsBase<TKey, TValue, TInput, TOutput, TContext> : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
    {
        /// <inheritdoc/>
        public virtual bool ConcurrentReader(ref TKey key, ref TInput input, ref TValue value, ref TOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo) => true;
        /// <inheritdoc/>
        public virtual bool SingleReader(ref TKey key, ref TInput input, ref TValue value, ref TOutput dst, ref ReadInfo readInfo) => true;

        /// <inheritdoc/>
        public virtual bool ConcurrentWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo) { dst = src; return true; }
        /// <inheritdoc/>
        public virtual bool SingleWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo) { dst = src; return true; }
        /// <inheritdoc/>
        public virtual void PostSingleWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        /// <inheritdoc/>
        public virtual void PostUpsertOperation<TEpochAccessor>(ref TKey key, ref TInput input, ref TValue src, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        { }

        /// <inheritdoc/>
        public virtual bool InitialUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;
        /// <inheritdoc/>
        public virtual void PostInitialUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo) { }
        /// <inheritdoc/>
        public virtual bool NeedInitialUpdate(ref TKey key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;
        /// <inheritdoc/>
        public virtual bool NeedCopyUpdate(ref TKey key, ref TInput input, ref TValue oldValue, ref TOutput output, ref RMWInfo rmwInfo) => true;
        /// <inheritdoc/>
        public virtual bool CopyUpdater(ref TKey key, ref TInput input, ref TValue oldValue, ref TValue newValue, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;
        /// <inheritdoc/>
        public virtual bool PostCopyUpdater(ref TKey key, ref TInput input, ref TValue oldValue, ref TValue newValue, ref TOutput output, ref RMWInfo rmwInfo) => true;
        /// <inheritdoc/>
        public virtual bool InPlaceUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;

        /// <inheritdoc/>
        public virtual bool SingleDeleter(ref TKey key, ref TValue value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) { value = default; return true; }
        public virtual void PostSingleDeleter(ref TKey key, ref DeleteInfo deleteInfo) { }
        public virtual bool ConcurrentDeleter(ref TKey key, ref TValue value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;

        /// <inheritdoc/>
        public virtual void PostDeleteOperation<TEpochAccessor>(ref TKey key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        { }

        public virtual void ReadCompletionCallback(ref TKey key, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }

        /// <inheritdoc/>
        public virtual void PostRMWOperation<TEpochAccessor>(ref TKey key, ref TInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        { }

        /// <inheritdoc/>
        public virtual void RMWCompletionCallback(ref TKey key, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }

        /// <inheritdoc/>
        public virtual int GetRMWModifiedValueLength(ref TValue value, ref TInput input) => throw new TsavoriteException("GetRMWModifiedValueLength is only available for SpanByte Functions");
        /// <inheritdoc/>
        public virtual int GetRMWInitialValueLength(ref TInput input) => throw new TsavoriteException("GetRMWInitialValueLength is only available for SpanByte Functions");
        /// <inheritdoc/>
        public virtual int GetUpsertValueLength(ref TValue value, ref TInput input) => throw new TsavoriteException("GetUpsertValueLength is only available for SpanByte Functions");

        /// <inheritdoc/>
        public virtual void ConvertOutputToHeap(ref TInput input, ref TOutput output) { }
    }

    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of FunctionsBase
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TContext"></typeparam>
    public class SimpleSessionFunctions<TKey, TValue, TContext> : SessionFunctionsBase<TKey, TValue, TValue, TValue, TContext>
    {
        private readonly Func<TValue, TValue, TValue> merger;
        public SimpleSessionFunctions() => merger = (l, r) => l;
        public SimpleSessionFunctions(Func<TValue, TValue, TValue> merger) => this.merger = merger;

        /// <inheritdoc/>
        public override bool ConcurrentReader(ref TKey key, ref TValue input, ref TValue value, ref TValue dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst = value;
            return true;
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