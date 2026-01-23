// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Provides thread management and all callbacks. A wrapper for IFunctions and additional methods called by TsavoriteImpl; the wrapped
    /// IFunctions methods provide additional parameters to support the wrapper functionality, then call through to the user implementations. 
    /// </summary>
    internal interface ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator> : IVariableLengthInput<TValue, TInput>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        bool IsManualLocking { get; }
        TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> Store { get; }

        #region Reads
        bool SingleReader(ref TKey key, ref TInput input, ref TValue value, ref TOutput dst, ref ReadInfo readInfo);
        bool ConcurrentReader(ref TKey key, ref TInput input, ref TValue value, ref TOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo);
        void ReadCompletionCallback(ref TKey key, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata);
        #endregion reads

        #region Upserts
        bool SingleWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo);
        void PostSingleWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo);
        bool ConcurrentWriter(long physicalAddress, ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo);
        void PostUpsertOperation<TEpochAccessor>(ref TKey key, ref TInput input, ref TValue src, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor;
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
            bool NeedInitialUpdate(ref TKey key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo);
            bool InitialUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo);
            void PostInitialUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rMWInfo, ref RecordInfo recordInfo);
        #endregion InitialUpdater

        #region CopyUpdater
        bool NeedCopyUpdate(ref TKey key, ref TInput input, ref TValue oldValue, ref TOutput output, ref RMWInfo rmwInfo);
        bool CopyUpdater(ref TKey key, ref TInput input, ref TValue oldValue, ref TValue newValue, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo);
        bool PostCopyUpdater(ref TKey key, ref TInput input, ref TValue oldValue, ref TValue newValue, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo);
        #endregion CopyUpdater

        #region InPlaceUpdater
        bool InPlaceUpdater(long physicalAddress, ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, out OperationStatus status, ref RecordInfo recordInfo);
        #endregion InPlaceUpdater

        void PostRMWOperation<TEpochAccessor>(ref TKey key, ref TInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor;
        void RMWCompletionCallback(ref TKey key, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata);
        #endregion RMWs

        #region Deletes
        bool SingleDeleter(ref TKey key, ref TValue value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo);
        void PostSingleDeleter(ref TKey key, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo);
        bool ConcurrentDeleter(long physicalAddress, ref TKey key, ref TValue value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo, out int fullRecordLength);
        void PostDeleteOperation<TEpochAccessor>(ref TKey key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor;
        #endregion Deletes

        #region Utilities
        /// <inheritdoc/>
        void ConvertOutputToHeap(ref TInput input, ref TOutput output);
        #endregion Utilities

        #region Transient locking
        bool TryLockTransientExclusive(ref TKey key, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx);
        bool TryLockTransientShared(ref TKey key, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx);
        void UnlockTransientExclusive(ref TKey key, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx);
        void UnlockTransientShared(ref TKey key, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx);
        #endregion

        #region Epoch control
        void UnsafeResumeThread();
        void UnsafeSuspendThread();
        #endregion

        bool CompletePendingWithOutputs(out CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false);

        TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.TsavoriteExecutionContext<TInput, TOutput, TContext> Ctx { get; }

        IHeapContainer<TInput> GetHeapContainer(ref TInput input);
    }
}