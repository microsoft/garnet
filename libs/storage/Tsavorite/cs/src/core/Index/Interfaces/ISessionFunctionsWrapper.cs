// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Provides thread management and all callbacks. A wrapper for IFunctions and additional methods called by TsavoriteImpl; the wrapped
    /// IFunctions methods provide additional parameters to support the wrapper functionality, then call through to the user implementations. 
    /// </summary>
    internal interface ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator> : IVariableLengthInput<TValue, TInput>
        where TStoreFunctions : IStoreFunctions<TValue>
        where TAllocator : IAllocator<TValue, TStoreFunctions>
    {
        bool IsTransactionalLocking { get; }
        TsavoriteKV<TValue, TStoreFunctions, TAllocator> Store { get; }

        #region Reads
        bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput dst, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord<TValue>;
        bool ConcurrentReader(ref LogRecord<TValue> logRecord, ref TInput input, ref TOutput dst, ref ReadInfo readInfo);
        void ReadCompletionCallback(ref DiskLogRecord<TValue> diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata);
        #endregion reads

        #region Upserts
        bool SingleWriter(ref LogRecord<TValue> logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason);
        bool SingleCopyWriter<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TValue> dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord<TValue>;
        void PostSingleWriter(ref LogRecord<TValue> logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason);
        bool ConcurrentWriter(ref LogRecord<TValue> logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo);
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        bool NeedInitialUpdate(SpanByte key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo);
        bool InitialUpdater(ref LogRecord<TValue> logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo);
        void PostInitialUpdater(ref LogRecord<TValue> logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rMWInfo);
        #endregion InitialUpdater

        #region CopyUpdater
        bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<TValue>;
        bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TValue> dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<TValue>;
        bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TValue> logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<TValue>;
        #endregion CopyUpdater

        #region InPlaceUpdater
        bool InPlaceUpdater(ref LogRecord<TValue> logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo, out OperationStatus status);
        #endregion InPlaceUpdater

        void RMWCompletionCallback(ref DiskLogRecord<TValue> diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata);
        #endregion RMWs

        #region Deletes
        bool SingleDeleter(ref LogRecord<TValue> logRecord, ref DeleteInfo deleteInfo);
        void PostSingleDeleter(ref LogRecord<TValue> logRecord, ref DeleteInfo deleteInfo);
        bool ConcurrentDeleter(ref LogRecord<TValue> logRecord, ref DeleteInfo deleteInfo);
        #endregion Deletes

        #region Utilities
        /// <inheritdoc/>
        void ConvertOutputToHeap(ref TInput input, ref TOutput output);
        #endregion Utilities

        #region Ephemeral locking
        bool TryLockEphemeralExclusive(ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx);
        bool TryLockEphemeralShared(ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx);
        void UnlockEphemeralExclusive(ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx);
        void UnlockEphemeralShared(ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx);
        #endregion 

        #region Epoch control
        void UnsafeResumeThread();
        void UnsafeSuspendThread();
        #endregion

        bool CompletePendingWithOutputs(out CompletedOutputIterator<TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false);

        TsavoriteKV<TValue, TStoreFunctions, TAllocator>.TsavoriteExecutionContext<TInput, TOutput, TContext> Ctx { get; }

        IHeapContainer<TInput> GetHeapContainer(ref TInput input);
    }
}