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
        bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput dst, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord;
        void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata);
        #endregion reads

        #region Upserts
        bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo);
        bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo);
        bool InitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord;
        void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo);
        void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo);
        void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord;
        bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo);
        bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo);
        bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord;
        void PostUpsertOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref TInput input, ReadOnlySpan<byte> srcValueSpan, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor;
        void PostUpsertOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref TInput input, IHeapObject srcValueObject, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor;
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo);
        bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo);
        void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rMWInfo);
        #endregion InitialUpdater

        #region CopyUpdater
        bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord;
        bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord;
        bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord;
        #endregion CopyUpdater

        #region InPlaceUpdater
        bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo, out OperationStatus status);
        #endregion InPlaceUpdater

        void PostRMWOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref TInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor;
        void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata);
        #endregion RMWs

        #region Deletes
        bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo);
        void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo);
        bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo);
        void PostDeleteOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor;
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