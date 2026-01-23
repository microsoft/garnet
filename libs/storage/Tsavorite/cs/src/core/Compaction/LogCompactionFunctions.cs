// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    internal sealed class LogCompactionFunctions<TKey, TValue, TInput, TOutput, TContext, TFunctions> : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
    {
        readonly TFunctions _functions;

        public LogCompactionFunctions(TFunctions functions)
        {
            _functions = functions;
        }

        /// <summary>
        /// No reads during compaction
        /// </summary>
        public bool ConcurrentReader(ref TKey key, ref TInput input, ref TValue value, ref TOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo) => true;

        public bool SingleDeleter(ref TKey key, ref TValue value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;

        public void PostSingleDeleter(ref TKey key, ref DeleteInfo deleteInfo) { }

        /// <summary>
        /// No ConcurrentDeleter needed for compaction
        /// </summary>
        public bool ConcurrentDeleter(ref TKey key, ref TValue value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;

        public void PostDeleteOperation<TEpochAccessor>(ref TKey key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        { }

        /// <summary>
        /// For compaction, we never perform concurrent writes as rolled over data defers to
        /// newly inserted data for the same key.
        /// </summary>
        public bool ConcurrentWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo) => true;

        public void PostUpsertOperation<TEpochAccessor>(ref TKey key, ref TInput input, ref TValue src, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        { }

        public bool CopyUpdater(ref TKey key, ref TInput input, ref TValue oldValue, ref TValue newValue, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;

        public bool PostCopyUpdater(ref TKey key, ref TInput input, ref TValue oldValue, ref TValue newValue, ref TOutput output, ref RMWInfo rmwInfo) => true;

        public bool InitialUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;
        public void PostInitialUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo) { }

        public bool InPlaceUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;

        public bool NeedInitialUpdate(ref TKey key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;

        public bool NeedCopyUpdate(ref TKey key, ref TInput input, ref TValue oldValue, ref TOutput output, ref RMWInfo rmwInfo) => true;

        public void ReadCompletionCallback(ref TKey key, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }

        public void PostRMWOperation<TEpochAccessor>(ref TKey key, ref TInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        { }

        public void RMWCompletionCallback(ref TKey key, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata) { }

        public int GetRMWModifiedValueLength(ref TValue value, ref TInput input) => 0;
        public int GetRMWInitialValueLength(ref TInput input) => 0;
        public int GetUpsertValueLength(ref TValue value, ref TInput input) => _functions.GetUpsertValueLength(ref value, ref input);

        /// <summary>
        /// No reads during compaction
        /// </summary>
        public bool SingleReader(ref TKey key, ref TInput input, ref TValue value, ref TOutput dst, ref ReadInfo readInfo) => true;

        /// <summary>
        /// Write compacted live value to store
        /// </summary>
        public bool SingleWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            => _functions.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);

        public void PostSingleWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        public void ConvertOutputToHeap(ref TInput input, ref TOutput output) { }
    }
}