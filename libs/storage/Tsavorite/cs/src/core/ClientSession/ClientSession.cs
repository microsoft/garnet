// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Thread-independent session interface to Tsavorite
    /// </summary>
    public sealed class ClientSession<Key, Value, Input, Output, Context, Functions> : IClientSession, IDisposable
        where Functions : ISessionFunctions<Key, Value, Input, Output, Context>
    {
        internal readonly TsavoriteKV<Key, Value> store;

        internal readonly TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> ctx;

        internal readonly Functions functions;

        internal CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs;

        readonly UnsafeContext<Key, Value, Input, Output, Context, Functions> uContext;
        readonly LockableUnsafeContext<Key, Value, Input, Output, Context, Functions> luContext;
        readonly LockableContext<Key, Value, Input, Output, Context, Functions> lContext;
        readonly BasicContext<Key, Value, Input, Output, Context, Functions> bContext;

        internal const string NotAsyncSessionErr = "Session does not support async operations";

        readonly ILoggerFactory loggerFactory;
        readonly ILogger logger;

        internal ulong TotalLockCount => sharedLockCount + exclusiveLockCount;
        internal ulong sharedLockCount;
        internal ulong exclusiveLockCount;

        bool isAcquiredLockable;

        ScanCursorState<Key, Value> scanCursorState;

        internal void AcquireLockable<TSessionFunctions>(TSessionFunctions sessionFunctions)
            where TSessionFunctions : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            CheckIsNotAcquiredLockable();

            while (true)
            {
                // Checkpoints cannot complete while we have active locking sessions.
                while (IsInPreparePhase())
                {
                    if (store.epoch.ThisInstanceProtected())
                        store.InternalRefresh<Input, Output, Context, TSessionFunctions>(sessionFunctions);
                    Thread.Yield();
                }

                store.IncrementNumLockingSessions();
                isAcquiredLockable = true;

                if (!IsInPreparePhase())
                    break;
                InternalReleaseLockable();
                Thread.Yield();
            }
        }

        internal void ReleaseLockable()
        {
            CheckIsAcquiredLockable();
            if (TotalLockCount > 0)
                throw new TsavoriteException($"EndLockable called with locks held: {sharedLockCount} shared locks, {exclusiveLockCount} exclusive locks");
            InternalReleaseLockable();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InternalReleaseLockable()
        {
            isAcquiredLockable = false;
            store.DecrementNumLockingSessions();
        }

        internal void CheckIsAcquiredLockable()
        {
            if (!isAcquiredLockable)
                throw new TsavoriteException("Lockable method call when BeginLockable has not been called");
        }

        void CheckIsNotAcquiredLockable()
        {
            if (isAcquiredLockable)
                throw new TsavoriteException("BeginLockable cannot be called twice (call EndLockable first)");
        }

        internal ClientSession(
            TsavoriteKV<Key, Value> store,
            TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            Functions functions,
            ILoggerFactory loggerFactory = null)
        {
            bContext = new(this);
            uContext = new(this);
            lContext = new(this);
            luContext = new(this);

            this.loggerFactory = loggerFactory;
            logger = loggerFactory?.CreateLogger($"ClientSession-{GetHashCode():X8}");
            this.store = store;
            this.ctx = ctx;
            this.functions = functions;
        }

        /// <summary>
        /// Get session ID
        /// </summary>
        public int ID { get { return ctx.sessionID; } }

        /// <summary>
        /// Current version number of the session
        /// </summary>
        public long Version => ctx.version;

        /// <summary>
        /// Dispose session
        /// </summary>
        public void Dispose()
        {
            completedOutputs?.Dispose();

            // By the time Dispose is called, we should have no outstanding locks, so can use the BasicContext's sessionFunctions.
            _ = CompletePending(bContext.sessionFunctions, true);
            store.DisposeClientSession(ID, ctx.phase);
        }

        /// <summary>
        /// Return a new interface to Tsavorite operations that supports manual epoch control.
        /// </summary>
        public UnsafeContext<Key, Value, Input, Output, Context, Functions> UnsafeContext => uContext;

        /// <summary>
        /// Return a new interface to Tsavorite operations that supports manual locking and epoch control.
        /// </summary>
        public LockableUnsafeContext<Key, Value, Input, Output, Context, Functions> LockableUnsafeContext => luContext;

        /// <summary>
        /// Return a session wrapper that supports manual locking.
        /// </summary>
        public LockableContext<Key, Value, Input, Output, Context, Functions> LockableContext => lContext;

        /// <summary>
        /// Return a session wrapper struct that passes through to client session
        /// </summary>
        public BasicContext<Key, Value, Input, Output, Context, Functions> BasicContext => bContext;

        #region ITsavoriteContext

        /// <inheritdoc/>
        public long GetKeyHash(Key key) => store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public long GetKeyHash(ref Key key) => store.GetKeyHash(ref key);

        /// <inheritdoc/>
        internal void Refresh<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            UnsafeResumeThread(sessionFunctions);
            try
            {
                store.InternalRefresh<Input, Output, Context, TSessionFunctionsWrapper>(sessionFunctions);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        internal void ResetModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref Key key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            UnsafeResumeThread(sessionFunctions);
            try
            {
                UnsafeResetModified(sessionFunctions, ref key);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public int CompareKeyHashes<TLockableKey>(TLockableKey key1, TLockableKey key2) where TLockableKey : ILockableKey => store.LockTable.CompareKeyHashes(key1, key2);

        /// <inheritdoc/>
        public int CompareKeyHashes<TLockableKey>(ref TLockableKey key1, ref TLockableKey key2) where TLockableKey : ILockableKey => store.LockTable.CompareKeyHashes(ref key1, ref key2);

        /// <inheritdoc/>
        public void SortKeyHashes<TLockableKey>(TLockableKey[] keys) where TLockableKey : ILockableKey => store.LockTable.SortKeyHashes(keys);

        /// <inheritdoc/>
        public void SortKeyHashes<TLockableKey>(TLockableKey[] keys, int start, int count) where TLockableKey : ILockableKey => store.LockTable.SortKeyHashes(keys, start, count);

        #endregion ITsavoriteContext

        #region Pending Operations

        /// <inheritdoc/>
        internal bool CompletePending<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, bool wait = false, bool spinWaitForCommit = false)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
            => CompletePending(sessionFunctions, getOutputs: false, wait, spinWaitForCommit);

        /// <inheritdoc/>
        internal bool CompletePendingWithOutputs<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            InitializeCompletedOutputs();
            var result = CompletePending(sessionFunctions, getOutputs: true, wait, spinWaitForCommit);
            completedOutputs = this.completedOutputs;
            return result;
        }

        /// <summary>
        /// Synchronously complete outstanding pending synchronous operations, returning outputs for the completed operations.
        /// Assumes epoch protection is managed by user. Async operations must be completed individually.
        /// </summary>
        internal bool UnsafeCompletePendingWithOutputs<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            InitializeCompletedOutputs();
            var result = UnsafeCompletePending(sessionFunctions, true, wait, spinWaitForCommit);
            completedOutputs = this.completedOutputs;
            return result;
        }

        private void InitializeCompletedOutputs()
        {
            if (completedOutputs is null)
                completedOutputs = new CompletedOutputIterator<Key, Value, Input, Output, Context>();
            else
                completedOutputs.Dispose();
        }

        internal bool CompletePending<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, bool getOutputs, bool wait, bool spinWaitForCommit)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            UnsafeResumeThread(sessionFunctions);
            try
            {
                return UnsafeCompletePending(sessionFunctions, getOutputs, wait, spinWaitForCommit);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        internal bool UnsafeCompletePending<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, bool getOutputs, bool wait, bool spinWaitForCommit)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            var requestedOutputs = getOutputs ? completedOutputs : default;
            var result = store.InternalCompletePending(sessionFunctions, wait, requestedOutputs);
            if (spinWaitForCommit)
            {
                if (!wait)
                    throw new TsavoriteException("Can spin-wait for commit (checkpoint completion) only if wait is true");
                do
                {
                    _ = store.InternalCompletePending(sessionFunctions, wait, requestedOutputs);
                    if (store.InRestPhase())
                    {
                        _ = store.InternalCompletePending(sessionFunctions, wait, requestedOutputs);
                        return true;
                    }
                } while (wait);
            }
            return result;
        }

        /// <inheritdoc/>
        internal ValueTask CompletePendingAsync<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, bool waitForCommit = false, CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
            => CompletePendingAsync(sessionFunctions, getOutputs: false, waitForCommit, token);

        /// <inheritdoc/>
        internal async ValueTask<CompletedOutputIterator<Key, Value, Input, Output, Context>> CompletePendingWithOutputsAsync<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                bool waitForCommit = false, CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            InitializeCompletedOutputs();
            await CompletePendingAsync(sessionFunctions, getOutputs: true, waitForCommit, token).ConfigureAwait(false);
            return completedOutputs;
        }

        private async ValueTask CompletePendingAsync<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, bool getOutputs, bool waitForCommit = false, CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            token.ThrowIfCancellationRequested();

            if (store.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            // Complete all pending operations on session
            await store.CompletePendingAsync(sessionFunctions, token, getOutputs ? completedOutputs : null).ConfigureAwait(false);

            // Wait for commit if necessary
            if (waitForCommit)
                await WaitForCommitAsync(sessionFunctions, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Check if at least one synchronous request is ready for CompletePending to be called on
        /// Returns completed immediately if there are no outstanding synchronous requests
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async ValueTask ReadyToCompletePendingAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (store.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            await TsavoriteKV<Key, Value>.ReadyToCompletePendingAsync(ctx, token).ConfigureAwait(false);
        }

        #endregion Pending Operations

        #region Other Operations

        internal void UnsafeResetModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref Key key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            OperationStatus status;
            do
                status = store.InternalModifiedBitOperation(ref key, out _);
            while (store.HandleImmediateNonPendingRetryStatus<Input, Output, Context, TSessionFunctionsWrapper>(status, sessionFunctions));
        }

        /// <inheritdoc/>
        internal unsafe void ResetModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, Key key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
            => ResetModified(sessionFunctions, ref key);

        /// <inheritdoc/>
        internal bool IsModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref Key key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            UnsafeResumeThread(sessionFunctions);
            try
            {
                return UnsafeIsModified(sessionFunctions, ref key);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        internal bool UnsafeIsModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref Key key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            RecordInfo modifiedInfo;
            OperationStatus status;
            do
                status = store.InternalModifiedBitOperation(ref key, out modifiedInfo, false);
            while (store.HandleImmediateNonPendingRetryStatus<Input, Output, Context, TSessionFunctionsWrapper>(status, sessionFunctions));
            return modifiedInfo.Modified;
        }

        /// <inheritdoc/>
        internal unsafe bool IsModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, Key key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
            => IsModified(sessionFunctions, ref key);

        /// <summary>
        /// Wait for commit of all operations completed until the current point in session.
        /// Does not itself issue checkpoint/commits.
        /// </summary>
        /// <returns></returns>
        private async ValueTask WaitForCommitAsync<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            token.ThrowIfCancellationRequested();

            if (!ctx.prevCtx.pendingReads.IsEmpty || !ctx.pendingReads.IsEmpty)
                throw new TsavoriteException("Make sure all async operations issued on this session are awaited and completed first");

            // Complete all pending sync operations on session
            await CompletePendingAsync(sessionFunctions, token: token).ConfigureAwait(false);

            var task = store.CheckpointTask;

            while (true)
            {
                _ = await task.WithCancellationAsync(token).ConfigureAwait(false);
                Refresh(sessionFunctions);
                task = store.CheckpointTask;
            }
        }

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="compactUntilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact(long compactUntilAddress, CompactionType compactionType = CompactionType.Scan)
            => Compact(compactUntilAddress, compactionType, default(DefaultCompactionFunctions<Key, Value>));

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="input">Input for SingleWriter</param>
        /// <param name="output">Output from SingleWriter; it will be called all records that are moved, before Compact() returns, so the user must supply buffering or process each output completely</param>
        /// <param name="compactUntilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact(ref Input input, ref Output output, long compactUntilAddress, CompactionType compactionType = CompactionType.Scan)
            => Compact(ref input, ref output, compactUntilAddress, compactionType, default(DefaultCompactionFunctions<Key, Value>));

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <param name="compactionFunctions">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<CompactionFunctions>(long untilAddress, CompactionType compactionType, CompactionFunctions compactionFunctions)
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            Input input = default;
            Output output = default;
            return store.Compact<Input, Output, Context, Functions, CompactionFunctions>(functions, compactionFunctions, ref input, ref output, untilAddress, compactionType);
        }

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="input">Input for SingleWriter</param>
        /// <param name="output">Output from SingleWriter; it will be called all records that are moved, before Compact() returns, so the user must supply buffering or process each output completely</param>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <param name="compactionFunctions">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<CompactionFunctions>(ref Input input, ref Output output, long untilAddress, CompactionType compactionType, CompactionFunctions compactionFunctions)
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            return store.Compact<Input, Output, Context, Functions, CompactionFunctions>(functions, compactionFunctions, ref input, ref output, untilAddress, compactionType);
        }

        /// <summary>
        /// Pull iterator for all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>Tsavorite iterator</returns>
        public ITsavoriteScanIterator<Key, Value> Iterate(long untilAddress = -1)
            => store.Iterate<Input, Output, Context, Functions>(functions, untilAddress);

        /// <summary>
        /// Push iteration of all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="scanFunctions">Functions receiving pushed records</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>True if Iteration completed; false if Iteration ended early due to one of the TScanIterator reader functions returning false</returns>
        public bool Iterate<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>
            => store.Iterate<Input, Output, Context, Functions, TScanFunctions>(functions, ref scanFunctions, untilAddress);

        /// <summary>
        /// Push-scan the log from <paramref name="cursor"/> (which should be a valid address) and push up to <paramref name="count"/> records
        /// to the caller via <paramref name="scanFunctions"/> for each Key that is not found at a higher address.
        /// </summary>
        /// <param name="cursor">The cursor of the scan. If 0, start at BeginAddress, else this should be the value this was updated with on the previous call,
        ///     which is the next address to return. If this is some other value, then for variable length records it must be validated by iterating from the 
        ///     start of the page, and iteration will start from the first valid logical address &lt;= this value. This is expensive.</param>
        /// <param name="count">The number of records to push.</param>
        /// <param name="scanFunctions">Caller functions called to push records. For this variant of Scan, this is not a ref param, because it will likely be copied through
        ///     the pending IO process.</param>
        /// <param name="endAddress">A specific end address; otherwise we scan until we hit the current TailAddress, which may yield duplicates in the event of RCUs.
        ///     This may be set to the TailAddress at the start of the scan, which may lose records that are RCU'd during the scan (because they are moved above the starting
        ///     TailAddress). A snapshot can be taken by calling ShiftReadOnlyToTail() and then using that TailAddress as endAddress.</param>
        /// <param name="validateCursor">If true, validate that the cursor is on a valid address boundary, and snap it to the highest lower address if it is not.</param>
        /// <returns>True if Scan completed and pushed <paramref name="count"/> records; false if Scan ended early due to finding less than <paramref name="count"/> records
        /// or one of the TScanIterator reader functions returning false</returns>
        public bool ScanCursor<TScanFunctions>(ref long cursor, long count, TScanFunctions scanFunctions, long endAddress = long.MaxValue, bool validateCursor = false)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>
            => store.hlog.ScanCursor(store, scanCursorState ??= new(), ref cursor, count, scanFunctions, endAddress, validateCursor);

        /// <summary>
        /// Resume session on current thread. IMPORTANT: Call SuspendThread before any async op.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeResumeThread<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            // We do not track any "acquired" state here; if someone mixes calls between safe and unsafe contexts, they will 
            // get the "trying to acquire already-acquired epoch" error.
            store.epoch.Resume();
            store.InternalRefresh<Input, Output, Context, TSessionFunctionsWrapper>(sessionFunctions);
        }

        /// <summary>
        /// Suspend session on current thread
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeSuspendThread()
        {
            Debug.Assert(store.epoch.ThisInstanceProtected());
            store.epoch.Suspend();
        }

        void IClientSession.AtomicSwitch(long version)
        {
            _ = TsavoriteKV<Key, Value>.AtomicSwitch(ctx, ctx.prevCtx, version);
        }

        /// <inheritdoc/>
        public void MergeRevivificationStatsTo(ref RevivificationStats to, bool reset) => ctx.MergeRevivificationStatsTo(ref to, reset);

        /// <inheritdoc/>
        public void ResetRevivificationStats() => ctx.ResetRevivificationStats();

        /// <summary>
        /// Return true if Tsavorite State Machine is in PREPARE state
        /// </summary>
        internal bool IsInPreparePhase()
        {
            return store.SystemState.Phase == Phase.PREPARE || store.SystemState.Phase == Phase.PREPARE_GROW;
        }

        #endregion Other Operations

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool InPlaceUpdater<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo, out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            // Note: KeyIndexes do not need notification of in-place updates because the key does not change.
            if (functions.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo))
            {
                rmwInfo.Action = RMWAction.Default;
                // MarkPage is done in InternalRMW
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                return true;
            }
            if (rmwInfo.Action == RMWAction.CancelOperation)
            {
                status = OperationStatus.CANCELED;
                return false;
            }
            if (rmwInfo.Action == RMWAction.ExpireAndResume)
            {
                // This inserts the tombstone if appropriate
                return store.ReinitializeExpiredRecord<Input, Output, Context, TSessionFunctionsWrapper>(ref key, ref input, ref value, ref output, ref recordInfo,
                                                    ref rmwInfo, rmwInfo.Address, sessionFunctions, isIpu: true, out status);
            }
            if (rmwInfo.Action == RMWAction.ExpireAndStop)
            {
                recordInfo.Tombstone = true;
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord | StatusCode.Expired);
                return false;
            }

            status = OperationStatus.SUCCESS;
            return false;
        }
    }
}