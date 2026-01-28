// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    internal readonly struct SessionFunctionsWrapper<TInput, TOutput, TContext, TFunctions, TSessionLocker, TStoreFunctions, TAllocator>
            : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        where TFunctions : ISessionFunctions<TInput, TOutput, TContext>
        where TSessionLocker : struct, ISessionLocker<TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        private readonly ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> _clientSession;
        private readonly TSessionLocker _sessionLocker;  // Has no data members

        public SessionFunctionsWrapper(ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession)
        {
            _clientSession = clientSession;
            _sessionLocker = new TSessionLocker();
        }

        public TsavoriteKV<TStoreFunctions, TAllocator> Store => _clientSession.store;
        public OverflowBucketLockTable<TStoreFunctions, TAllocator> LockTable => _clientSession.store.LockTable;

        #region Reads
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput dst, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
            => _clientSession.functions.Reader(in srcLogRecord, ref input, ref dst, ref readInfo);

        public void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata)
            => _clientSession.functions.ReadCompletionCallback(ref diskLogRecord, ref input, ref output, ctx, status, recordMetadata);

        #endregion Reads

        #region Upserts
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
            => _clientSession.functions.InitialWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
            => _clientSession.functions.InitialWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
            => _clientSession.functions.InitialWriter(ref dstLogRecord, in sizeInfo, ref input, in inputLogRecord, ref output, ref upsertInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            logRecord.InfoRef.SetDirtyAndModified();
            _clientSession.functions.PostInitialWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            logRecord.InfoRef.SetDirtyAndModified();
            _clientSession.functions.PostInitialWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void PostInitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            dstLogRecord.InfoRef.SetDirtyAndModified();
            _clientSession.functions.PostInitialWriter(ref dstLogRecord, in sizeInfo, ref input, in inputLogRecord, ref output, ref upsertInfo);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ReadOnlySpan<byte> srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            if (!_clientSession.functions.InPlaceWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo))
                return false;
            logRecord.InfoRef.SetDirtyAndModified();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, IHeapObject srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            if (!_clientSession.functions.InPlaceWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo))
                return false;
            logRecord.InfoRef.SetDirtyAndModified();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (!_clientSession.functions.InPlaceWriter(ref logRecord, in sizeInfo, ref input, in inputLogRecord, ref output, ref upsertInfo))
                return false;
            logRecord.InfoRef.SetDirtyAndModified();
            return true;
        }
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            => _clientSession.functions.NeedInitialUpdate(key, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            => _clientSession.functions.InitialUpdater(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
        {
            logRecord.InfoRef.SetDirtyAndModified();
            _clientSession.functions.PostInitialUpdater(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
        }
        #endregion InitialUpdater

        #region CopyUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => _clientSession.functions.NeedCopyUpdate(in srcLogRecord, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => _clientSession.functions.CopyUpdater(in srcLogRecord, ref dstLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            dstLogRecord.InfoRef.SetDirtyAndModified();
            return _clientSession.functions.PostCopyUpdater(in srcLogRecord, ref dstLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
        }
        #endregion CopyUpdater

        #region InPlaceUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo, out OperationStatus status)
        {
            // This wraps the ISessionFunctions call to provide expiration logic.
            if (_clientSession.functions.InPlaceUpdater(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo))
            {
                rmwInfo.Action = RMWAction.Default;
                logRecord.InfoRef.SetDirtyAndModified();

                // MarkPage is done in InternalRMW
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                return true;
            }

            if (rmwInfo.Action == RMWAction.ExpireAndResume)
            {
                // This inserts the tombstone if appropriate
                return _clientSession.store.ReinitializeExpiredRecord<TInput, TOutput, TContext, SessionFunctionsWrapper<TInput, TOutput, TContext, TFunctions, TSessionLocker, TStoreFunctions, TAllocator>>(
                                                    ref logRecord, ref input, ref output, ref rmwInfo, rmwInfo.Address, this, isIpu: true, out status);
            }

            if (rmwInfo.Action == RMWAction.CancelOperation)
            {
                status = OperationStatus.CANCELED;
            }
            else if (rmwInfo.Action == RMWAction.ExpireAndStop)
            {
                logRecord.InfoRef.SetTombstone();
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord | StatusCode.Expired);
            }
            else if (rmwInfo.Action == RMWAction.WrongType)
            {
                logRecord.InfoRef.SetTombstone();
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.WrongType);
            }
            else
                status = OperationStatus.SUCCESS;
            return false;
        }
        #endregion InPlaceUpdater

        public void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata)
            => _clientSession.functions.RMWCompletionCallback(ref diskLogRecord, ref input, ref output, ctx, status, recordMetadata);

        #endregion RMWs

        #region Deletes
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
            => _clientSession.functions.InitialDeleter(ref logRecord, ref deleteInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            logRecord.InfoRef.SetDirtyAndModified();
            _clientSession.functions.PostInitialDeleter(ref logRecord, ref deleteInfo);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            if (!_clientSession.functions.InPlaceDeleter(ref logRecord, ref deleteInfo))
                return false;
            logRecord.InfoRef.SetTombstone();
            logRecord.InfoRef.SetDirtyAndModified();
            return true;
        }
        #endregion Deletes

        #region Utilities
        /// <inheritdoc/>
        public void ConvertOutputToHeap(ref TInput input, ref TOutput output) => _clientSession.functions.ConvertOutputToHeap(ref input, ref output);
        #endregion Utilities

        #region Ephemeral locking
        public bool IsTransactionalLocking => _sessionLocker.IsTransactionalLocking;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockEphemeralExclusive(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx) =>
            _sessionLocker.TryLockEphemeralExclusive(Store, ref stackCtx);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockEphemeralShared(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
            => _sessionLocker.TryLockEphemeralShared(Store, ref stackCtx);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockEphemeralExclusive(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
            => _sessionLocker.UnlockEphemeralExclusive(Store, ref stackCtx);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockEphemeralShared(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
            => _sessionLocker.UnlockEphemeralShared(Store, ref stackCtx);
        #endregion Ephemeral locking

        #region Internal utilities
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref TInput input) => _clientSession.functions.GetRMWInitialFieldInfo(key, ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TInput input)
            where TSourceLogRecord : ISourceLogRecord
            => _clientSession.functions.GetRMWModifiedFieldInfo(in srcLogRecord, ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref TInput input) => _clientSession.functions.GetUpsertFieldInfo(key, value, ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref TInput input) => _clientSession.functions.GetUpsertFieldInfo(key, value, ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordFieldInfo GetUpsertFieldInfo<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord inputLogRecord, ref TInput input)
            where TSourceLogRecord : ISourceLogRecord
            => _clientSession.functions.GetUpsertFieldInfo(key, in inputLogRecord, ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeResumeThread() => _clientSession.UnsafeResumeThread(this);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeSuspendThread() => _clientSession.UnsafeSuspendThread();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            => _clientSession.CompletePendingWithOutputs(this, out completedOutputs, wait, spinWaitForCommit);

        public TsavoriteKV<TStoreFunctions, TAllocator>.TsavoriteExecutionContext<TInput, TOutput, TContext> Ctx => _clientSession.ctx;
        #endregion Internal utilities
    }
}