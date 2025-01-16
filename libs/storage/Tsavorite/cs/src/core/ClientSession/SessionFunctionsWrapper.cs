// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    internal readonly struct SessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TFunctions, TSessionLocker, TStoreFunctions, TAllocator>
            : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        where TFunctions : ISessionFunctions<TValue, TInput, TOutput, TContext>
        where TSessionLocker : struct, ISessionLocker<TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TValue>
        where TAllocator : IAllocator<TValue, TStoreFunctions>
    {
        private readonly ClientSession<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> _clientSession;
        private readonly TSessionLocker _sessionLocker;  // Has no data members

        public SessionFunctionsWrapper(ClientSession<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession)
        {
            _clientSession = clientSession;
            _sessionLocker = new TSessionLocker();
        }

        public TsavoriteKV<TValue, TStoreFunctions, TAllocator> Store => _clientSession.store;
        public OverflowBucketLockTable<TValue, TStoreFunctions, TAllocator> LockTable => _clientSession.store.LockTable;

        #region Reads
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput dst, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
            => _clientSession.functions.SingleReader(ref srcLogRecord, ref input, ref dst, ref readInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentReader(ref LogRecord logRecord, ref TInput input, ref TOutput dst, ref ReadInfo readInfo)
            => _clientSession.functions.ConcurrentReader(ref logRecord, ref input, ref dst, ref readInfo);

        public void ReadCompletionCallback(ref LogRecord logRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata)
            => _clientSession.functions.ReadCompletionCallback(ref logRecord, ref input, ref output, ctx, status, recordMetadata);

        #endregion Reads

        #region Upserts
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleWriter(ref LogRecord logRecord, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            => _clientSession.functions.SingleWriter(ref logRecord, ref input, srcValue, ref output, ref upsertInfo, reason);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleCopyWriter<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord
            => _clientSession.functions.SingleCopyWriter(ref srcLogRecord, ref dstLogRecord, ref upsertInfo, reason);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostSingleWriter(ref LogRecord logRecord, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            logRecord.InfoRef.SetDirtyAndModified();
            _clientSession.functions.PostSingleWriter(ref logRecord, ref input, srcValue, ref output, ref upsertInfo, reason);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref LogRecord logRecord, ref TInput input, TValue srcValue, ref TOutput output, ref UpsertInfo upsertInfo)
        {
            if (!_clientSession.functions.ConcurrentWriter(ref logRecord, ref input, srcValue, ref output, ref upsertInfo))
                return false;
            logRecord.InfoRef.SetDirtyAndModified();
            return true;
        }
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool NeedInitialUpdate(SpanByte key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            => _clientSession.functions.NeedInitialUpdate(key, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InitialUpdater(ref LogRecord logRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            => _clientSession.functions.InitialUpdater(ref logRecord, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostInitialUpdater(ref LogRecord logRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
        {
            logRecord.InfoRef.SetDirtyAndModified();
            _clientSession.functions.PostInitialUpdater(ref logRecord, ref input, ref output, ref rmwInfo);
        }
        #endregion InitialUpdater

        #region CopyUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => _clientSession.functions.NeedCopyUpdate(ref srcLogRecord, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => _clientSession.functions.CopyUpdater(ref srcLogRecord, ref dstLogRecord, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            dstLogRecord.InfoRef.SetDirtyAndModified();
            return _clientSession.functions.PostCopyUpdater(ref srcLogRecord, ref dstLogRecord, ref input, ref output, ref rmwInfo);
        }
        #endregion CopyUpdater

        #region InPlaceUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref LogRecord logRecord, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo, out OperationStatus status)
        {
            // This wraps the ISessionFunctions call to provide expiration logic.
            if (_clientSession.functions.InPlaceUpdater(ref logRecord, ref input, ref output, ref rmwInfo))
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
                return _clientSession.store.ReinitializeExpiredRecord<TInput, TOutput, TContext, SessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TFunctions, TSessionLocker, TStoreFunctions, TAllocator>>(
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
            else
                status = OperationStatus.SUCCESS;
            return false;
        }
        #endregion InPlaceUpdater

        public void RMWCompletionCallback(ref LogRecord logRecord, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata)
            => _clientSession.functions.RMWCompletionCallback(ref logRecord, ref input, ref output, ctx, status, recordMetadata);

        #endregion RMWs

        #region Deletes
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
            => _clientSession.functions.SingleDeleter(ref logRecord, ref deleteInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostSingleDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            logRecord.InfoRef.SetDirtyAndModified();
            _clientSession.functions.PostSingleDeleter(ref logRecord, ref deleteInfo);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            if (!_clientSession.functions.ConcurrentDeleter(ref logRecord, ref deleteInfo))
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
        public bool TryLockEphemeralExclusive(ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx) =>
            _sessionLocker.TryLockEphemeralExclusive(Store, ref stackCtx);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockEphemeralShared(ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx)
            => _sessionLocker.TryLockEphemeralShared(Store, ref stackCtx);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockEphemeralExclusive(ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx)
            => _sessionLocker.UnlockEphemeralExclusive(Store, ref stackCtx);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockEphemeralShared(ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx)
            => _sessionLocker.UnlockEphemeralShared(Store, ref stackCtx);
        #endregion Ephemeral locking

        #region Internal utilities
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref TInput input) => _clientSession.functions.GetRMWInitialFieldInfo(key, ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TInput input)
            where TSourceLogRecord : ISourceLogRecord
            => _clientSession.functions.GetRMWModifiedFieldInfo(ref srcLogRecord, ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordFieldInfo GetUpsertFieldInfo(SpanByte key, TValue value, ref TInput input) => _clientSession.functions.GetUpsertFieldInfo(key, value, ref input);

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
            // TODO: If Object LogRecord, get IHeapObject Value and call StoreFunctions.DisposeObjectValue(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeResumeThread() => _clientSession.UnsafeResumeThread(this);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeSuspendThread() => _clientSession.UnsafeSuspendThread();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            => _clientSession.CompletePendingWithOutputs(this, out completedOutputs, wait, spinWaitForCommit);

        public TsavoriteKV<TValue, TStoreFunctions, TAllocator>.TsavoriteExecutionContext<TInput, TOutput, TContext> Ctx => _clientSession.ctx;
        #endregion Internal utilities
    }
}