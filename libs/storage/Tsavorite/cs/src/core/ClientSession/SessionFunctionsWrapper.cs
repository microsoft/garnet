// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    internal readonly struct SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TFunctions, TSessionLocker, TStoreFunctions, TAllocator>
            : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        where TFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TSessionLocker : struct, ISessionLocker<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        private readonly ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> _clientSession;
        private readonly TSessionLocker _sessionLocker;  // Has no data members

        public SessionFunctionsWrapper(ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession)
        {
            _clientSession = clientSession;
            _sessionLocker = new TSessionLocker();
        }

        public TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> Store => _clientSession.store;
        public OverflowBucketLockTable<TKey, TValue, TStoreFunctions, TAllocator> LockTable => _clientSession.store.LockTable;

        #region Reads
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleReader(long logicalAddress, long physicalAddress, ref TInput input, ref TOutput dst, ref ReadInfo readInfo)
        {
            var logRecord = _clientSession.store.hlog.CreateLogRecord(logicalAddress, physicalAddress);
            return _clientSession.functions.SingleReader(ref logRecord, ref input, ref dst, ref readInfo);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentReader(long logicalAddress, long physicalAddress, ref TInput input, ref TOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            var logRecord = _clientSession.store.hlog.CreateLogRecord(logicalAddress, physicalAddress);
            return _clientSession.functions.ConcurrentReader(ref logRecord, ref input, ref dst, ref readInfo, ref recordInfo);
        }

        public void ReadCompletionCallback(long logicalAddress, long physicalAddress, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata)
        {
            var logRecord = _clientSession.store.hlog.CreateLogRecord(logicalAddress, physicalAddress);
            _clientSession.functions.ReadCompletionCallback(ref logRecord, ref input, ref output, ctx, status, recordMetadata);
        }

        #endregion Reads

        #region Upserts
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleWriter(ref LogRecord logRecord, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            => _clientSession.functions.SingleWriter(ref logRecord, ref input, srcValue, ref output, ref upsertInfo, reason);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostSingleWriter(ref LogRecord logRecord, ref TInput input, TValue srcValue, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            logRecord.InfoRef.SetDirtyAndModified();
            _clientSession.functions.PostSingleWriter(ref logRecord, ref input, srcValue, ref output, ref upsertInfo, reason);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(long physicalAddress, ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            if (!_clientSession.functions.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo))
                return false;
            logRecord.InfoRef.SetDirtyAndModified();
            return true;
        }
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool NeedInitialUpdate(ref TKey key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            => _clientSession.functions.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InitialUpdater(ref LogRecord logRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            => _clientSession.functions.InitialUpdater(ref logRecord, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostInitialUpdater(ref LogRecord logRecord, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            recordInfo.SetDirtyAndModified();
            _clientSession.functions.PostInitialUpdater(ref logRecord, ref input, ref value, ref output, ref rmwInfo);
        }
        #endregion InitialUpdater

        #region CopyUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : IReadOnlyLogRecord
            => _clientSession.functions.NeedCopyUpdate(ref srcLogRecord, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : IReadOnlyLogRecord
            => _clientSession.functions.CopyUpdater(ref srcLogRecord, ref dstLogRecord, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : IReadOnlyLogRecord
        {
            dstLogRecord.InfoRef.SetDirtyAndModified();
            return _clientSession.functions.PostCopyUpdater(ref srcLogRecord, ref dstLogRecord, ref input, ref output, ref rmwInfo);
        }
        #endregion CopyUpdater

        #region InPlaceUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(long physicalAddress, ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, out OperationStatus status, ref RecordInfo recordInfo)
        {
            // This wraps the ISessionFunctions call to provide expiration logic.
            if (_clientSession.functions.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo))
            {
                rmwInfo.Action = RMWAction.Default;
                recordInfo.SetDirtyAndModified();

                // MarkPage is done in InternalRMW
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                return true;
            }

            if (rmwInfo.Action == RMWAction.ExpireAndResume)
            {
                // This inserts the tombstone if appropriate
                return _clientSession.store.ReinitializeExpiredRecord<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TFunctions, TSessionLocker, TStoreFunctions, TAllocator>>(
                                                    ref key, ref input, ref value, ref output, ref recordInfo, ref rmwInfo, rmwInfo.Address, this, isIpu: true, out status);
            }

            if (rmwInfo.Action == RMWAction.CancelOperation)
            {
                status = OperationStatus.CANCELED;
            }
            else if (rmwInfo.Action == RMWAction.ExpireAndStop)
            {
                recordInfo.SetTombstone();
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord | StatusCode.Expired);
            }
            else
                status = OperationStatus.SUCCESS;
            return false;
        }
        #endregion InPlaceUpdater

        public void RMWCompletionCallback(ref TKey key, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata)
            => _clientSession.functions.RMWCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

        #endregion RMWs

        #region Deletes
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleDeleter(ref TKey key, ref TValue value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
            => _clientSession.functions.SingleDeleter(ref key, ref value, ref deleteInfo, ref recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostSingleDeleter(ref TKey key, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
        {
            recordInfo.SetDirtyAndModified();
            _clientSession.functions.PostSingleDeleter(ref key, ref deleteInfo);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentDeleter(long physicalAddress, ref TKey key, ref TValue value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo, out int allocatedSize)
        {
            (deleteInfo.UsedValueLength, deleteInfo.FullValueLength, allocatedSize) = _clientSession.store.GetRecordLengths(physicalAddress, ref value, ref recordInfo);
            if (!_clientSession.functions.ConcurrentDeleter(ref key, ref value, ref deleteInfo, ref recordInfo))
                return false;
            _clientSession.store.SetTombstoneAndExtraValueLength(ref value, ref recordInfo, deleteInfo.UsedValueLength, deleteInfo.FullValueLength);
            recordInfo.SetDirtyAndModified();
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
        public bool TryLockEphemeralExclusive(ref TKey key, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx) =>
            _sessionLocker.TryLockEphemeralExclusive(Store, ref stackCtx);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockEphemeralShared(ref TKey key, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
            => _sessionLocker.TryLockEphemeralShared(Store, ref stackCtx);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockEphemeralExclusive(ref TKey key, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
            => _sessionLocker.UnlockEphemeralExclusive(Store, ref stackCtx);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockEphemeralShared(ref TKey key, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
            => _sessionLocker.UnlockEphemeralShared(Store, ref stackCtx);
        #endregion Ephemeral locking

        #region Internal utilities
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordFieldInfo GetRMWInitialFieldInfo(ref TInput input) => _clientSession.functions.GetRMWInitialValueLength(ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetRMWModifiedValueLength(ref TValue t, ref TInput input) => _clientSession.functions.GetRMWModifiedValueLength(ref t, ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetUpsertFieldInfo(ref TValue t, ref TInput input) => _clientSession.functions.GetUpsertValueLength(ref t, ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IHeapContainer<TInput> GetHeapContainer(ref TInput input)
        {
            if (typeof(TInput) == typeof(SpanByte))
                return new SpanByteHeapContainer(ref Unsafe.As<TInput, SpanByte>(ref input), _clientSession.store.hlogBase.bufferPool) as IHeapContainer<TInput>;
            return new StandardHeapContainer<TInput>(ref input);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DisposeRecord(DisposeReason reason)
        {
            // TODO: If ObjectLogRecord, get IHeapObject Value and call StoreFunctions.DisposeObjectValue(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeResumeThread() => _clientSession.UnsafeResumeThread(this);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeSuspendThread() => _clientSession.UnsafeSuspendThread();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            => _clientSession.CompletePendingWithOutputs(this, out completedOutputs, wait, spinWaitForCommit);

        public TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.TsavoriteExecutionContext<TInput, TOutput, TContext> Ctx => _clientSession.ctx;
        #endregion Internal utilities
    }
}