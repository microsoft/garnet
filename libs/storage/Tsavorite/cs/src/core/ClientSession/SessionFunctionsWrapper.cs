// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    internal readonly struct SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        private readonly ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> _clientSession;

        public SessionFunctionsWrapper(ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> clientSession, bool isDual)
        {
            _clientSession = clientSession;
            this.IsDual = isDual;
        }

        public readonly bool IsDual { get; init; }

        public TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> Store => _clientSession.Store;
        public HashBucketLockTable LockTable => _clientSession.Store.LockTable;

        #region Reads
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleReader(ref TKey key, ref TInput input, ref TValue value, ref TOutput dst, ref ReadInfo readInfo)
            => _clientSession.functions.SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentReader(ref TKey key, ref TInput input, ref TValue value, ref TOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            => _clientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref readInfo, ref recordInfo);

        public void ReadCompletionCallback(ref TKey key, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata)
            => _clientSession.functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

        #endregion Reads

        #region Upserts
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            => _clientSession.functions.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostSingleWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            recordInfo.SetDirtyAndModified();
            _clientSession.functions.PostSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(long physicalAddress, ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            (upsertInfo.UsedValueLength, upsertInfo.FullValueLength, _) = _clientSession.Store.GetRecordLengths(physicalAddress, ref dst, ref recordInfo);
            if (!_clientSession.functions.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, ref recordInfo))
                return false;
            _clientSession.Store.SetExtraValueLength(ref dst, ref recordInfo, upsertInfo.UsedValueLength, upsertInfo.FullValueLength);
            recordInfo.SetDirtyAndModified();
            return true;
        }
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool NeedInitialUpdate(ref TKey key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            => _clientSession.functions.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InitialUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            => _clientSession.functions.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostInitialUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            recordInfo.SetDirtyAndModified();
            _clientSession.functions.PostInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
        }
        #endregion InitialUpdater

        #region CopyUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool NeedCopyUpdate(ref TKey key, ref TInput input, ref TValue oldValue, ref TOutput output, ref RMWInfo rmwInfo)
            => _clientSession.functions.NeedCopyUpdate(ref key, ref input, ref oldValue, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CopyUpdater(ref TKey key, ref TInput input, ref TValue oldValue, ref TValue newValue, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            => _clientSession.functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo, ref recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool PostCopyUpdater(ref TKey key, ref TInput input, ref TValue oldValue, ref TValue newValue, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            recordInfo.SetDirtyAndModified();
            return _clientSession.functions.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
        }
        #endregion CopyUpdater

        #region InPlaceUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(long physicalAddress, ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, out OperationStatus status, ref RecordInfo recordInfo)
        {
            (rmwInfo.UsedValueLength, rmwInfo.FullValueLength, _) = _clientSession.Store.GetRecordLengths(physicalAddress, ref value, ref recordInfo);

            if (_clientSession.functions.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo))
            {
                rmwInfo.Action = RMWAction.Default;
                _clientSession.Store.SetExtraValueLength(ref value, ref recordInfo, rmwInfo.UsedValueLength, rmwInfo.FullValueLength);
                recordInfo.SetDirtyAndModified();

                // MarkPage is done in InternalRMW
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                return true;
            }

            if (rmwInfo.Action == RMWAction.ExpireAndResume)
            {
                // This inserts the tombstone if appropriate
                return _clientSession.Store.ReinitializeExpiredRecord<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>>(
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
            (deleteInfo.UsedValueLength, deleteInfo.FullValueLength, allocatedSize) = _clientSession.Store.GetRecordLengths(physicalAddress, ref value, ref recordInfo);
            if (!_clientSession.functions.ConcurrentDeleter(ref key, ref value, ref deleteInfo, ref recordInfo))
                return false;
            _clientSession.Store.SetTombstoneAndExtraValueLength(ref value, ref recordInfo, deleteInfo.UsedValueLength, deleteInfo.FullValueLength);
            recordInfo.SetDirtyAndModified();
            return true;
        }
        #endregion Deletes

        #region Utilities
        /// <inheritdoc/>
        public void ConvertOutputToHeap(ref TInput input, ref TOutput output) => _clientSession.functions.ConvertOutputToHeap(ref input, ref output);
        #endregion Utilities

        #region Internal utilities
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetRMWInitialValueLength(ref TInput input) => _clientSession.functions.GetRMWInitialValueLength(ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetRMWModifiedValueLength(ref TValue t, ref TInput input) => _clientSession.functions.GetRMWModifiedValueLength(ref t, ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IHeapContainer<TInput> GetHeapContainer(ref TInput input)
        {
            if (typeof(TInput) == typeof(SpanByte))
                return new SpanByteHeapContainer(ref Unsafe.As<TInput, SpanByte>(ref input), _clientSession.Store.hlogBase.bufferPool) as IHeapContainer<TInput>;
            return new StandardHeapContainer<TInput>(ref input);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeResumeThread() => _clientSession.UnsafeResumeThread();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeSuspendThread() => _clientSession.UnsafeSuspendThread();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            => _clientSession.CompletePendingWithOutputs(this, out completedOutputs, wait, spinWaitForCommit);

        public TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.ExecutionContext<TInput, TOutput, TContext> ExecutionCtx => _clientSession.ExecutionCtx;
        #endregion Internal utilities
    }
}