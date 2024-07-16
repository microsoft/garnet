// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    internal readonly struct SessionFunctionsWrapper<Key, Value, Input, Output, Context, Functions, TSessionLocker, TStoreFunctions, TAllocator> 
            : ISessionFunctionsWrapper<Key, Value, Input, Output, Context, TStoreFunctions, TAllocator>
        where Functions : ISessionFunctions<Key, Value, Input, Output, Context>
        where TSessionLocker : struct, ISessionLocker<Key, Value, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<Key, Value>
        where TAllocator : IAllocator<Key, Value, TStoreFunctions>
    {
        private readonly ClientSession<Key, Value, Input, Output, Context, Functions, TStoreFunctions, TAllocator> _clientSession;
        private readonly TSessionLocker _sessionLocker;  // Has no data members

        public SessionFunctionsWrapper(ClientSession<Key, Value, Input, Output, Context, Functions, TStoreFunctions, TAllocator> clientSession)
        {
            _clientSession = clientSession;
            _sessionLocker = new TSessionLocker();
        }

        public TsavoriteKV<Key, Value, TStoreFunctions, TAllocator> Store => _clientSession.store;
        public OverflowBucketLockTable<Key, Value, TStoreFunctions, TAllocator> LockTable => _clientSession.store.LockTable;

        #region Reads
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo)
            => _clientSession.functions.SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            => _clientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref readInfo, ref recordInfo);

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata)
            => _clientSession.functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

        #endregion Reads

        #region Upserts
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            => _clientSession.functions.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            recordInfo.SetDirtyAndModified();
            _clientSession.functions.PostSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(long physicalAddress, ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            (upsertInfo.UsedValueLength, upsertInfo.FullValueLength, _) = _clientSession.store.GetRecordLengths(physicalAddress, ref dst, ref recordInfo);
            if (!_clientSession.functions.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, ref recordInfo))
                return false;
            _clientSession.store.SetExtraValueLength(ref dst, ref recordInfo, upsertInfo.UsedValueLength, upsertInfo.FullValueLength);
            recordInfo.SetDirtyAndModified();
            return true;
        }
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            => _clientSession.functions.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            => _clientSession.functions.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            recordInfo.SetDirtyAndModified();
            _clientSession.functions.PostInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
        }
        #endregion InitialUpdater

        #region CopyUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref RMWInfo rmwInfo)
            => _clientSession.functions.NeedCopyUpdate(ref key, ref input, ref oldValue, ref output, ref rmwInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            => _clientSession.functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo, ref recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            recordInfo.SetDirtyAndModified();
            return _clientSession.functions.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
        }
        #endregion CopyUpdater

        #region InPlaceUpdater
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(long physicalAddress, ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, out OperationStatus status, ref RecordInfo recordInfo)
        {
            (rmwInfo.UsedValueLength, rmwInfo.FullValueLength, _) = _clientSession.store.GetRecordLengths(physicalAddress, ref value, ref recordInfo);
            if (!_clientSession.InPlaceUpdater(this, ref key, ref input, ref value, ref output, ref recordInfo, ref rmwInfo, out status))
                return false;
            _clientSession.store.SetExtraValueLength(ref value, ref recordInfo, rmwInfo.UsedValueLength, rmwInfo.FullValueLength);
            recordInfo.SetDirtyAndModified();
            return true;
        }
        #endregion InPlaceUpdater

        public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata)
            => _clientSession.functions.RMWCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

        #endregion RMWs

        #region Deletes
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
            => _clientSession.functions.SingleDeleter(ref key, ref value, ref deleteInfo, ref recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostSingleDeleter(ref Key key, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
        {
            recordInfo.SetDirtyAndModified();
            _clientSession.functions.PostSingleDeleter(ref key, ref deleteInfo);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentDeleter(long physicalAddress, ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo, out int allocatedSize)
        {
            (deleteInfo.UsedValueLength, deleteInfo.FullValueLength, allocatedSize) = _clientSession.store.GetRecordLengths(physicalAddress, ref value, ref recordInfo);
            if (!_clientSession.functions.ConcurrentDeleter(ref key, ref value, ref deleteInfo, ref recordInfo))
                return false;
            _clientSession.store.SetTombstoneAndExtraValueLength(ref value, ref recordInfo, deleteInfo.UsedValueLength, deleteInfo.FullValueLength);
            recordInfo.SetDirtyAndModified();
            return true;
        }
        #endregion Deletes

        #region Dispose
        public void DisposeSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason)
            => _clientSession.functions.DisposeSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
        public void DisposeCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo)
            => _clientSession.functions.DisposeCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
        public void DisposeInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo)
            => _clientSession.functions.DisposeInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
        public void DisposeSingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo)
            => _clientSession.functions.DisposeSingleDeleter(ref key, ref value, ref deleteInfo);
        public void DisposeDeserializedFromDisk(ref Key key, ref Value value, ref RecordInfo recordInfo)
            => _clientSession.functions.DisposeDeserializedFromDisk(ref key, ref value);
        public void DisposeForRevivification(ref Key key, ref Value value, int newKeySize, ref RecordInfo recordInfo)
            => _clientSession.functions.DisposeForRevivification(ref key, ref value, newKeySize);
        #endregion Dispose

        #region Utilities
        /// <inheritdoc/>
        public void ConvertOutputToHeap(ref Input input, ref Output output) => _clientSession.functions.ConvertOutputToHeap(ref input, ref output);
        #endregion Utilities

        #region Transient locking
        public bool IsManualLocking => _sessionLocker.IsManualLocking;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockTransientExclusive(ref Key key, ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx) =>
            _sessionLocker.TryLockTransientExclusive(Store, ref stackCtx);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockTransientShared(ref Key key, ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx)
            => _sessionLocker.TryLockTransientShared(Store, ref stackCtx);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockTransientExclusive(ref Key key, ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx)
            => _sessionLocker.UnlockTransientExclusive(Store, ref stackCtx);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockTransientShared(ref Key key, ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx)
            => _sessionLocker.UnlockTransientShared(Store, ref stackCtx);
        #endregion Transient locking

        #region Internal utilities
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetRMWInitialValueLength(ref Input input) => _clientSession.functions.GetRMWInitialValueLength(ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetRMWModifiedValueLength(ref Value t, ref Input input) => _clientSession.functions.GetRMWModifiedValueLength(ref t, ref input);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IHeapContainer<Input> GetHeapContainer(ref Input input)
        {
            if (typeof(Input) == typeof(SpanByte))
                return new SpanByteHeapContainer(ref Unsafe.As<Input, SpanByte>(ref input), _clientSession.store.hlogBase.bufferPool) as IHeapContainer<Input>;
            return new StandardHeapContainer<Input>(ref input);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeResumeThread() => _clientSession.UnsafeResumeThread(this);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeSuspendThread() => _clientSession.UnsafeSuspendThread();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            => _clientSession.CompletePendingWithOutputs(this, out completedOutputs, wait, spinWaitForCommit);

        public TsavoriteKV<Key, Value, TStoreFunctions, TAllocator>.TsavoriteExecutionContext<Input, Output, Context> Ctx => _clientSession.ctx;
        #endregion Internal utilities
    }
}