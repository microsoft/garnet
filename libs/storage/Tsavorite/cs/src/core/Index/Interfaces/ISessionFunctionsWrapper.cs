// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Provides thread management and all callbacks. A wrapper for IFunctions and additional methods called by TsavoriteImpl; the wrapped
    /// IFunctions methods provide additional parameters to support the wrapper functionality, then call through to the user implementations. 
    /// </summary>
    internal interface ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator> : IVariableLengthInput<TInput>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        bool IsTransactionalLocking { get; }
        TsavoriteKV<TStoreFunctions, TAllocator> Store { get; }

        #region Reads
        bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput dst, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord;
        bool ConcurrentReader(ref LogRecord logRecord, ref TInput input, ref TOutput dst, ref ReadInfo readInfo);
        void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata);
        #endregion reads

        #region Upserts
        bool SingleWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason);
        bool SingleWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason);
        bool SingleCopyWriter<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord;
        void PostSingleWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason);
        void PostSingleWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason);
        bool ConcurrentWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo);
        bool ConcurrentWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo);
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo);
        bool InitialUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo);
        void PostInitialUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rMWInfo);
        #endregion InitialUpdater

        #region CopyUpdater
        bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord;
        bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord;
        bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord;
        #endregion CopyUpdater

        #region InPlaceUpdater
        bool InPlaceUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo, out OperationStatus status);
        #endregion InPlaceUpdater

        void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata);
        #endregion RMWs

        #region Deletes
        bool SingleDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo);
        void PostSingleDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo);
        bool ConcurrentDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo);
        #endregion Deletes

        #region Utilities
        /// <inheritdoc/>
        void ConvertOutputToHeap(ref TInput input, ref TOutput output);
        #endregion Utilities

        #region Ephemeral locking
        bool TryLockEphemeralExclusive(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx);
        bool TryLockEphemeralShared(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx);
        void UnlockEphemeralExclusive(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx);
        void UnlockEphemeralShared(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx);
        #endregion 

        #region Epoch control
        void UnsafeResumeThread();
        void UnsafeSuspendThread();
        #endregion

        bool CompletePendingWithOutputs(out CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false);

        TsavoriteKV<TStoreFunctions, TAllocator>.TsavoriteExecutionContext<TInput, TOutput, TContext> Ctx { get; }
    }
}