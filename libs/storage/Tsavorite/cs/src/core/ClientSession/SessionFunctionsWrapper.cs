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
        internal readonly ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> ClientSession;

        public SessionFunctionsWrapper(ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> clientSession, bool isDual)
        {
            ClientSession = clientSession;
            IsDual = isDual;
        }

        public readonly bool IsDual { get; init; }

        public readonly TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> Store => ClientSession.Store;
        public readonly HashBucketLockTable LockTable => ClientSession.Store.LockTable;

        #region Reads
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool SingleReader(ref TKey key, ref TInput input, ref TValue value, ref TOutput dst, ref ReadInfo readInfo)
            => ClientSession.functions.SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool ConcurrentReader(ref TKey key, ref TInput input, ref TValue value, ref TOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            => ClientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref readInfo, ref recordInfo);

        public readonly void ReadCompletionCallback(ref TKey key, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata)
            => ClientSession.functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

        #endregion Reads

        #region Upserts
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool SingleWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            => ClientSession.functions.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void PostSingleWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            recordInfo.SetDirtyAndModified();
            ClientSession.functions.PostSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool ConcurrentWriter(long physicalAddress, ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            (upsertInfo.UsedValueLength, upsertInfo.FullValueLength, _) = ClientSession.Store.GetRecordLengths(physicalAddress, ref dst, ref recordInfo);
            if (!ClientSession.functions.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, ref recordInfo))
                return false;
            ClientSession.Store.SetExtraValueLength(ref dst, ref recordInfo, upsertInfo.UsedValueLength, upsertInfo.FullValueLength);
            recordInfo.SetDirtyAndModified();
            return true;
        }
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool NeedInitialUpdate(ref TKey key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            => ClientSession.functions.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InitialUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            => ClientSession.functions.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void PostInitialUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            recordInfo.SetDirtyAndModified();
            ClientSession.functions.PostInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
        }
        #endregion InitialUpdater

        #region CopyUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool NeedCopyUpdate(ref TKey key, ref TInput input, ref TValue oldValue, ref TOutput output, ref RMWInfo rmwInfo)
            => ClientSession.functions.NeedCopyUpdate(ref key, ref input, ref oldValue, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool CopyUpdater(ref TKey key, ref TInput input, ref TValue oldValue, ref TValue newValue, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            => ClientSession.functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo, ref recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool PostCopyUpdater(ref TKey key, ref TInput input, ref TValue oldValue, ref TValue newValue, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            recordInfo.SetDirtyAndModified();
            return ClientSession.functions.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
        }
        #endregion CopyUpdater

        #region InPlaceUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool InPlaceUpdater(long physicalAddress, ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, out OperationStatus status, ref RecordInfo recordInfo)
        {
            (rmwInfo.UsedValueLength, rmwInfo.FullValueLength, _) = ClientSession.Store.GetRecordLengths(physicalAddress, ref value, ref recordInfo);

            if (ClientSession.functions.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo))
            {
                rmwInfo.Action = RMWAction.Default;
                ClientSession.Store.SetExtraValueLength(ref value, ref recordInfo, rmwInfo.UsedValueLength, rmwInfo.FullValueLength);
                recordInfo.SetDirtyAndModified();

                // MarkPage is done in InternalRMW
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                return true;
            }

            if (rmwInfo.Action == RMWAction.ExpireAndResume)
            {
                // This inserts the tombstone if appropriate
                return ClientSession.Store.ReinitializeExpiredRecord<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>>(
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

        public readonly void RMWCompletionCallback(ref TKey key, ref TInput input, ref TOutput output, TContext ctx, Status status, RecordMetadata recordMetadata)
            => ClientSession.functions.RMWCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

        #endregion RMWs

        #region Deletes
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool SingleDeleter(ref TKey key, ref TValue value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
            => ClientSession.functions.SingleDeleter(ref key, ref value, ref deleteInfo, ref recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void PostSingleDeleter(ref TKey key, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
        {
            recordInfo.SetDirtyAndModified();
            ClientSession.functions.PostSingleDeleter(ref key, ref deleteInfo);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool ConcurrentDeleter(long physicalAddress, ref TKey key, ref TValue value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo, out int allocatedSize)
        {
            (deleteInfo.UsedValueLength, deleteInfo.FullValueLength, allocatedSize) = ClientSession.Store.GetRecordLengths(physicalAddress, ref value, ref recordInfo);
            if (!ClientSession.functions.ConcurrentDeleter(ref key, ref value, ref deleteInfo, ref recordInfo))
                return false;
            ClientSession.Store.SetTombstoneAndExtraValueLength(ref value, ref recordInfo, deleteInfo.UsedValueLength, deleteInfo.FullValueLength);
            recordInfo.SetDirtyAndModified();
            return true;
        }
        #endregion Deletes

        #region Utilities
        /// <inheritdoc/>
        public readonly void ConvertOutputToHeap(ref TInput input, ref TOutput output) => ClientSession.functions.ConvertOutputToHeap(ref input, ref output);
        #endregion Utilities

        #region Internal utilities
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly int GetRMWInitialValueLength(ref TInput input) => ClientSession.functions.GetRMWInitialValueLength(ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly int GetRMWModifiedValueLength(ref TValue t, ref TInput input) => ClientSession.functions.GetRMWModifiedValueLength(ref t, ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly IHeapContainer<TInput> GetHeapContainer(ref TInput input)
        {
            if (typeof(TInput) == typeof(SpanByte))
                return new SpanByteHeapContainer(ref Unsafe.As<TInput, SpanByte>(ref input), ClientSession.Store.hlogBase.bufferPool) as IHeapContainer<TInput>;
            return new StandardHeapContainer<TInput>(ref input);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void UnsafeResumeThread() => ClientSession.UnsafeResumeThread();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void UnsafeSuspendThread() => ClientSession.UnsafeSuspendThread();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool CompletePendingWithOutputs<TKeyLocker>(out CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            where TKeyLocker : struct, ISessionLocker
            => ClientSession.CompletePendingWithOutputs<SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(this, out completedOutputs, wait, spinWaitForCommit);

        public readonly TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.ExecutionContext<TInput, TOutput, TContext> ExecutionCtx => ClientSession.ExecutionCtx;
        #endregion Internal utilities
    }
}