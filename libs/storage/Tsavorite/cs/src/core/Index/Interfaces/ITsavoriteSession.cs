// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Provides thread management and callback to checkpoint completion (called state machine).
    /// </summary>
    /// <remarks>This is broken out into a non-generic base interfaces to allow the use of <see cref="NullTsavoriteSession"/> 
    /// in <see cref="TsavoriteKV{Key, Value}.ThreadStateMachineStep"/>.</remarks>
    internal interface ITsavoriteSession
    {
        void UnsafeResumeThread();
        void UnsafeSuspendThread();
        void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint);
    }

    /// <summary>
    /// Provides thread management and all callbacks. A wrapper for IFunctions and additional methods called by TsavoriteImpl; the wrapped
    /// IFunctions methods provide additional parameters to support the wrapper functionality, then call through to the user implementations. 
    /// </summary>
    internal interface ITsavoriteSession<Key, Value, Input, Output, Context> : ITsavoriteSession, IVariableLengthInput<Value, Input>
    {
        bool IsManualLocking { get; }
        TsavoriteKV<Key, Value> Store { get; }

        #region Reads
        bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo);
        bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo, ref RecordInfo recordInfo);
        void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata);
        #endregion reads

        #region Upserts
        bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo);
        void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo);
        bool ConcurrentWriter(long physicalAddress, ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo);
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref RMWInfo rmwInfo);
        bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo);
        void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rMWInfo, ref RecordInfo recordInfo);
        #endregion InitialUpdater

        #region CopyUpdater
        bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref RMWInfo rmwInfo);
        bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo);
        void PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo);
        #endregion CopyUpdater

        #region InPlaceUpdater
        bool InPlaceUpdater(long physicalAddress, ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, out OperationStatus status, ref RecordInfo recordInfo);
        #endregion InPlaceUpdater

        void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata);
        #endregion RMWs

        #region Deletes
        bool SingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo);
        void PostSingleDeleter(ref Key key, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo);
        bool ConcurrentDeleter(long physicalAddress, ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo, out int fullRecordLength);
        #endregion Deletes

        #region Disposal
        void DisposeSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason);
        void DisposeCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo);
        void DisposeInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo);
        void DisposeSingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo);
        void DisposeDeserializedFromDisk(ref Key key, ref Value value, ref RecordInfo recordInfo);
        void DisposeForRevivification(ref Key key, ref Value value, int newKeySize, ref RecordInfo recordInfo);
        #endregion Disposal

        #region Transient locking
        bool TryLockTransientExclusive(ref Key key, ref OperationStackContext<Key, Value> stackCtx);
        bool TryLockTransientShared(ref Key key, ref OperationStackContext<Key, Value> stackCtx);
        void UnlockTransientExclusive(ref Key key, ref OperationStackContext<Key, Value> stackCtx);
        void UnlockTransientShared(ref Key key, ref OperationStackContext<Key, Value> stackCtx);
        #endregion 

        bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false);

        TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> Ctx { get; }

        IHeapContainer<Input> GetHeapContainer(ref Input input);
    }
}