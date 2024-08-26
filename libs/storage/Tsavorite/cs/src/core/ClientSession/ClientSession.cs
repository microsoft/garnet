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
    public sealed class ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> : IClientSession, IDisposable
        where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        internal readonly TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store;

        internal readonly TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.TsavoriteExecutionContext<TInput, TOutput, TContext> ctx;

        internal readonly TSessionFunctions functions;

        internal CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs;

        readonly UnsafeContext<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> uContext;
        readonly LockableUnsafeContext<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> luContext;
        readonly LockableContext<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> lContext;
        readonly BasicContext<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> bContext;

        readonly ILoggerFactory loggerFactory;
        readonly ILogger logger;

        internal ulong TotalLockCount => sharedLockCount + exclusiveLockCount;
        internal ulong sharedLockCount;
        internal ulong exclusiveLockCount;

        bool isAcquiredLockable;

        ScanCursorState<TKey, TValue> scanCursorState;

        internal void AcquireLockable<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctionsWrapper)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            CheckIsNotAcquiredLockable();

            while (true)
            {
                // Checkpoints cannot complete while we have active locking sessions.
                while (IsInPreparePhase())
                {
                    if (store.kernel.epoch.ThisInstanceProtected())
                        store.InternalRefresh<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctionsWrapper);
                    Thread.Yield();
                }

                store.IncrementNumLockingSessions();
                isAcquiredLockable = true;

                if (!IsInPreparePhase())
                    break;
                InternalReleaseLockable();
                _ = Thread.Yield();
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
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store,
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.TsavoriteExecutionContext<TInput, TOutput, TContext> ctx,
            TSessionFunctions functions,
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
        public UnsafeContext<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> UnsafeContext => uContext;

        /// <summary>
        /// Return a new interface to Tsavorite operations that supports manual locking and epoch control.
        /// </summary>
        public LockableUnsafeContext<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> LockableUnsafeContext => luContext;

        /// <summary>
        /// Return a session wrapper that supports manual locking.
        /// </summary>
        public LockableContext<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> LockableContext => lContext;

        /// <summary>
        /// Return a session wrapper struct that passes through to client session
        /// </summary>
        public BasicContext<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> BasicContext => bContext;

        #region ITsavoriteContext

        /// <inheritdoc/>
        public long GetKeyHash(TKey key) => store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public long GetKeyHash(ref TKey key) => store.GetKeyHash(ref key);

        /// <inheritdoc/>
        internal void Refresh<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            UnsafeResumeThread(sessionFunctions);
            try
            {
                store.InternalRefresh<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        internal void ResetModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
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
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            => CompletePending(sessionFunctions, getOutputs: false, wait, spinWaitForCommit);

        /// <inheritdoc/>
        internal bool CompletePendingWithOutputs<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, out CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
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
        internal bool UnsafeCompletePendingWithOutputs<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, out CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            InitializeCompletedOutputs();
            var result = UnsafeCompletePending(sessionFunctions, true, wait, spinWaitForCommit);
            completedOutputs = this.completedOutputs;
            return result;
        }

        private void InitializeCompletedOutputs()
        {
            if (completedOutputs is null)
                completedOutputs = new CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext>();
            else
                completedOutputs.Dispose();
        }

        internal bool CompletePending<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, bool getOutputs, bool wait, bool spinWaitForCommit)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
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
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
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
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            => CompletePendingAsync(sessionFunctions, getOutputs: false, waitForCommit, token);

        /// <inheritdoc/>
        internal async ValueTask<CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext>> CompletePendingWithOutputsAsync<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                bool waitForCommit = false, CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            InitializeCompletedOutputs();
            await CompletePendingAsync(sessionFunctions, getOutputs: true, waitForCommit, token).ConfigureAwait(false);
            return completedOutputs;
        }

        private async ValueTask CompletePendingAsync<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, bool getOutputs, bool waitForCommit = false, CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            token.ThrowIfCancellationRequested();

            if (store.kernel.epoch.ThisInstanceProtected())
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

            if (store.kernel.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            await TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.ReadyToCompletePendingAsync(ctx, token).ConfigureAwait(false);
        }

        #endregion Pending Operations

        #region Other Operations

        internal void UnsafeResetModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            OperationStatus status;
            do
                status = store.InternalModifiedBitOperation(ref key, out _);
            while (store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(status, sessionFunctions));
        }

        /// <inheritdoc/>
        internal unsafe void ResetModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, TKey key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            => ResetModified(sessionFunctions, ref key);

        /// <inheritdoc/>
        internal bool IsModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
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

        internal bool UnsafeIsModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            RecordInfo modifiedInfo;
            OperationStatus status;
            do
                status = store.InternalModifiedBitOperation(ref key, out modifiedInfo, false);
            while (store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(status, sessionFunctions));
            return modifiedInfo.Modified;
        }

        /// <inheritdoc/>
        internal unsafe bool IsModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, TKey key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            => IsModified(sessionFunctions, ref key);

        /// <summary>
        /// Wait for commit of all operations completed until the current point in session.
        /// Does not itself issue checkpoint/commits.
        /// </summary>
        /// <returns></returns>
        private async ValueTask WaitForCommitAsync<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
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
            => Compact(compactUntilAddress, compactionType, default(DefaultCompactionFunctions<TKey, TValue>));

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="input">Input for SingleWriter</param>
        /// <param name="output">Output from SingleWriter; it will be called all records that are moved, before Compact() returns, so the user must supply buffering or process each output completely</param>
        /// <param name="compactUntilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact(ref TInput input, ref TOutput output, long compactUntilAddress, CompactionType compactionType = CompactionType.Scan)
            => Compact(ref input, ref output, compactUntilAddress, compactionType, default(DefaultCompactionFunctions<TKey, TValue>));

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <param name="compactionFunctions">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<CompactionFunctions>(long untilAddress, CompactionType compactionType, CompactionFunctions compactionFunctions)
            where CompactionFunctions : ICompactionFunctions<TKey, TValue>
        {
            TInput input = default;
            TOutput output = default;
            return store.Compact<TInput, TOutput, TContext, TSessionFunctions, CompactionFunctions>(functions, compactionFunctions, ref input, ref output, untilAddress, compactionType);
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
        public long Compact<CompactionFunctions>(ref TInput input, ref TOutput output, long untilAddress, CompactionType compactionType, CompactionFunctions compactionFunctions)
            where CompactionFunctions : ICompactionFunctions<TKey, TValue>
        {
            return store.Compact<TInput, TOutput, TContext, TSessionFunctions, CompactionFunctions>(functions, compactionFunctions, ref input, ref output, untilAddress, compactionType);
        }

        /// <summary>
        /// Pull iterator for all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>Tsavorite iterator</returns>
        public ITsavoriteScanIterator<TKey, TValue> Iterate(long untilAddress = -1)
            => store.Iterate<TInput, TOutput, TContext, TSessionFunctions>(functions, untilAddress);

        /// <summary>
        /// Push iteration of all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="scanFunctions">Functions receiving pushed records</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>True if Iteration completed; false if Iteration ended early due to one of the TScanIterator reader functions returning false</returns>
        public bool Iterate<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
            => store.Iterate<TInput, TOutput, TContext, TSessionFunctions, TScanFunctions>(functions, ref scanFunctions, untilAddress);

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
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
            => store.hlogBase.ScanCursor(store, scanCursorState ??= new(), ref cursor, count, scanFunctions, endAddress, validateCursor);

        /// <summary>
        /// Resume session on current thread. IMPORTANT: Call SuspendThread before any async op.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeResumeThread<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // We do not track any "acquired" state here; if someone mixes calls between safe and unsafe contexts, they will 
            // get the "trying to acquire already-acquired epoch" error.
            store.kernel.epoch.Resume();
            store.InternalRefresh<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions);
        }

        /// <summary>
        /// Suspend session on current thread
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeSuspendThread()
        {
            Debug.Assert(store.kernel.epoch.ThisInstanceProtected());
            store.kernel.epoch.Suspend();
        }

        void IClientSession.AtomicSwitch(long version)
        {
            _ = TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.AtomicSwitch(ctx, ctx.prevCtx, version);
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
    }
}