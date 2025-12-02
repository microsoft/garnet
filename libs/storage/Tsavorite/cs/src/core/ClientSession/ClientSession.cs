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
    public sealed class ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> : IClientSession, IDisposable
        where TFunctions : ISessionFunctions<TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        internal readonly TsavoriteKV<TStoreFunctions, TAllocator> store;

        internal readonly TsavoriteKV<TStoreFunctions, TAllocator>.TsavoriteExecutionContext<TInput, TOutput, TContext> ctx;

        internal readonly TFunctions functions;

        internal CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs;

        readonly UnsafeContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> uContext;
        readonly TransactionalUnsafeContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> luContext;
        readonly TransactionalContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> lContext;
        readonly BasicContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> bContext;
        readonly ConsistentReadContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> crContext;
        readonly TransactionalConsistentReadContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> tcrContext;

        internal const string NotAsyncSessionErr = "Session does not support async operations";

        internal readonly ConsistentReadContextCallbacks consistentReadProtocolCallbacks = null;

        readonly ILoggerFactory loggerFactory;
        readonly ILogger logger;

        internal ulong TotalLockCount => sharedLockCount + exclusiveLockCount;
        internal ulong sharedLockCount;
        internal ulong exclusiveLockCount;

        ScanCursorState scanCursorState;

        internal void AcquireTransactional<TSessionFunctions>(TSessionFunctions sessionFunctions)
            where TSessionFunctions : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            CheckIsNotAcquiredTransactional(sessionFunctions);
            sessionFunctions.Ctx.isAcquiredTransactional = true;
        }

        internal void LocksAcquired<TSessionFunctions>(TSessionFunctions sessionFunctions, long txnVersion)
            where TSessionFunctions : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            CheckIsAcquiredTransactional(sessionFunctions);
            sessionFunctions.Ctx.txnVersion = txnVersion;
        }

        internal void ReleaseTransactional<TSessionFunctions>(TSessionFunctions sessionFunctions)
            where TSessionFunctions : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            CheckIsAcquiredTransactional(sessionFunctions);
            if (TotalLockCount > 0)
                throw new TsavoriteException($"EndTransactional called with locks held: {sharedLockCount} shared locks, {exclusiveLockCount} exclusive locks");
            sessionFunctions.Ctx.isAcquiredTransactional = false;
            sessionFunctions.Ctx.txnVersion = 0;
        }

        internal void CheckIsAcquiredTransactional<TSessionFunctions>(TSessionFunctions sessionFunctions)
            where TSessionFunctions : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (!sessionFunctions.Ctx.isAcquiredTransactional)
                throw new TsavoriteException("Transactional method call when BeginTransactional has not been called");
        }

        void CheckIsNotAcquiredTransactional<TSessionFunctions>(TSessionFunctions sessionFunctions)
            where TSessionFunctions : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (sessionFunctions.Ctx.isAcquiredTransactional)
                throw new TsavoriteException("BeginTransactional cannot be called twice (call EndTransactional first)");
        }

        internal ClientSession(
            TsavoriteKV<TStoreFunctions, TAllocator> store,
            TsavoriteKV<TStoreFunctions, TAllocator>.TsavoriteExecutionContext<TInput, TOutput, TContext> ctx,
            TFunctions functions,
            ILoggerFactory loggerFactory = null)
        {
            bContext = new(this);
            uContext = new(this);
            lContext = new(this);
            luContext = new(this);
            crContext = new(this);
            tcrContext = new(this);

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

            store.DisposeClientSession(ID);
        }

        /// <summary>
        /// Return a new interface to Tsavorite operations that supports manual epoch control.
        /// </summary>
        public UnsafeContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> UnsafeContext => uContext;

        /// <summary>
        /// Return a new interface to Tsavorite operations that supports Transactional locking and manual epoch control.
        /// </summary>
        public TransactionalUnsafeContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> TransactionalUnsafeContext => luContext;

        /// <summary>
        /// Return a session wrapper that supports Transactional locking.
        /// </summary>
        public TransactionalContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> TransactionalContext => lContext;

        /// <summary>
        /// Return a session wrapper struct that passes through to client session
        /// </summary>
        public BasicContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> BasicContext => bContext;

        /// <summary>
        /// Return the consistent read context;
        /// </summary>
        public ConsistentReadContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> ConsistentReadContext => crContext;

        /// <summary>
        /// Return the transactional consistent read context
        /// </summary>
        public TransactionalConsistentReadContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> TransactionalConsistentReadContext => tcrContext;

        #region ITsavoriteContext

        /// <inheritdoc/>
        public long GetKeyHash(ReadOnlySpan<byte> key) => store.GetKeyHash(key);

        /// <inheritdoc/>
        public long GetKeyHash(ref ReadOnlySpan<byte> key) => store.GetKeyHash(key);

        /// <inheritdoc/>
        internal void Refresh<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
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
        internal void ResetModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ReadOnlySpan<byte> key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            UnsafeResumeThread(sessionFunctions);
            try
            {
                UnsafeResetModified(sessionFunctions, key);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public int CompareKeyHashes<TTransactionalKey>(TTransactionalKey key1, TTransactionalKey key2) where TTransactionalKey : ITransactionalKey => store.LockTable.CompareKeyHashes(key1, key2);

        /// <inheritdoc/>
        public int CompareKeyHashes<TTransactionalKey>(ref TTransactionalKey key1, ref TTransactionalKey key2) where TTransactionalKey : ITransactionalKey => store.LockTable.CompareKeyHashes(ref key1, ref key2);

        /// <inheritdoc/>
        public void SortKeyHashes<TTransactionalKey>(Span<TTransactionalKey> keys) where TTransactionalKey : ITransactionalKey => store.LockTable.SortKeyHashes(keys);

        #endregion ITsavoriteContext

        #region Pending Operations

        /// <inheritdoc/>
        internal bool CompletePending<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, bool wait = false, bool spinWaitForCommit = false)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            => CompletePending(sessionFunctions, getOutputs: false, wait, spinWaitForCommit);

        /// <inheritdoc/>
        internal bool CompletePendingWithOutputs<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, out CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
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
        internal bool UnsafeCompletePendingWithOutputs<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, out CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            InitializeCompletedOutputs();
            var result = UnsafeCompletePending(sessionFunctions, true, wait, spinWaitForCommit);
            completedOutputs = this.completedOutputs;
            return result;
        }

        private void InitializeCompletedOutputs()
        {
            if (completedOutputs is null)
                completedOutputs = new CompletedOutputIterator<TInput, TOutput, TContext>();
            else
                completedOutputs.Dispose();
        }

        internal bool CompletePending<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, bool getOutputs, bool wait, bool spinWaitForCommit)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
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
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
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
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            => CompletePendingAsync(sessionFunctions, getOutputs: false, waitForCommit, token);

        /// <inheritdoc/>
        internal async ValueTask<CompletedOutputIterator<TInput, TOutput, TContext>> CompletePendingWithOutputsAsync<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                bool waitForCommit = false, CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            InitializeCompletedOutputs();
            await CompletePendingAsync(sessionFunctions, getOutputs: true, waitForCommit, token).ConfigureAwait(false);
            return completedOutputs;
        }

        private async ValueTask CompletePendingAsync<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, bool getOutputs, bool waitForCommit = false, CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
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

            await TsavoriteKV<TStoreFunctions, TAllocator>.ReadyToCompletePendingAsync(ctx, token).ConfigureAwait(false);
        }

        #endregion Pending Operations

        #region Other Operations

        internal void UnsafeResetModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ReadOnlySpan<byte> key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            OperationStatus status;
            do
                status = store.InternalModifiedBitOperation(key, out _);
            while (store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(status, sessionFunctions));
        }

        /// <inheritdoc/>
        internal bool IsModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ReadOnlySpan<byte> key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            UnsafeResumeThread(sessionFunctions);
            try
            {
                return UnsafeIsModified(sessionFunctions, key);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        internal bool UnsafeIsModified<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ReadOnlySpan<byte> key)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            RecordInfo modifiedInfo;
            OperationStatus status;
            do
                status = store.InternalModifiedBitOperation(key, out modifiedInfo, false);
            while (store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(status, sessionFunctions));
            return modifiedInfo.Modified;
        }

        /// <summary>
        /// Wait for commit of all operations completed until the current point in session.
        /// Does not itself issue checkpoint/commits.
        /// </summary>
        /// <returns></returns>
        private async ValueTask WaitForCommitAsync<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            token.ThrowIfCancellationRequested();

            if (!ctx.pendingReads.IsEmpty)
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
            => Compact(compactUntilAddress, compactionType, default(DefaultCompactionFunctions));

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <param name="compactionFunctions">User provided compaction functions (see <see cref="ICompactionFunctions"/>).</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<CompactionFunctions>(long untilAddress, CompactionType compactionType, CompactionFunctions compactionFunctions)
            where CompactionFunctions : ICompactionFunctions
            => store.Compact<TInput, TOutput, TContext, CompactionFunctions>(compactionFunctions, untilAddress, compactionType);

        /// <summary>
        /// Pull iterator for all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>Tsavorite iterator</returns>
        public ITsavoriteScanIterator Iterate(long untilAddress = -1)
            => store.Iterate<TInput, TOutput, TContext, TFunctions>(functions, untilAddress);

        /// <summary>
        /// Push iteration of all (distinct) live key-values stored in Tsavorite, using a temporary TsavoriteKV to ensure uniqueness
        /// </summary>
        /// <param name="scanFunctions">Functions receiving pushed records</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>True if Iteration completed; false if Iteration ended early due to one of the TScanIterator reader functions returning false</returns>
        public bool Iterate<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions
            => store.Iterate<TInput, TOutput, TContext, TFunctions, TScanFunctions>(functions, ref scanFunctions, untilAddress);

        /// <summary>
        /// Push iteration of all (distinct) live key-values stored in Tsavorite, using a lookup strategy to ensure uniqueness
        /// </summary>
        /// <param name="scanFunctions">Functions receiving pushed records</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <param name="cursor">Cursor to scan from</param>
        /// <param name="validateCursor">Cursor to scan from</param>
        /// <param name="maxAddress">Max address to search for keys when conditionally pushing</param>
        /// <param name="resetCursor">Whether to reset cursor at the end of the iteration</param>
        /// <param name="includeTombstones">Whether to include tombstoned record when iterating</param>
        /// <returns>True if Iteration completed; false if Iteration ended early due to one of the TScanIterator reader functions returning false</returns>
        public bool IterateLookup<TScanFunctions>(ref TScanFunctions scanFunctions, ref long cursor, long untilAddress = -1, bool validateCursor = false, long maxAddress = long.MaxValue,
                bool resetCursor = true, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions
        {
            if (untilAddress == -1)
                untilAddress = store.Log.TailAddress;
            return ScanCursor(ref cursor, count: long.MaxValue, scanFunctions, endAddress: untilAddress, validateCursor: validateCursor, maxAddress: maxAddress,
                resetCursor: resetCursor, includeTombstones: includeTombstones);
        }

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
        ///     TailAddress). A snapshot can be taken by calling ShiftReadOnlyToTail() and then using that TailAddress as endAddress and maxAddress.</param>
        /// <param name="validateCursor">If true, validate that the cursor is on a valid address boundary, and snap it to the highest lower address if it is not.</param>
        /// <param name="maxAddress">Maximum address for determining liveness, records after this address are not considered when checking validity.</param>
        /// <param name="resetCursor">Whether to set cursor to zero at the end of iteration.</param>
        /// <param name="includeTombstones">Whether to include tombstoned records while iterating.</param>
        /// <returns>True if Scan completed and pushed <paramref name="count"/> records and there may be more records; false if Scan ended early due to finding less than <paramref name="count"/> records
        /// or one of the TScanIterator reader functions returning false, or if we determined that there are no records remaining. In other words, if this returns true,
        /// there may be more records satisfying the iteration criteria beyond <paramref name="count"/>.</returns>
        public bool ScanCursor<TScanFunctions>(ref long cursor, long count, TScanFunctions scanFunctions, long endAddress = long.MaxValue, bool validateCursor = false,
                long maxAddress = long.MaxValue, bool resetCursor = true, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions
            => store.hlogBase.ScanCursor(store, scanCursorState ??= new(), ref cursor, count, scanFunctions, endAddress, validateCursor, maxAddress,
                resetCursor: resetCursor, includeTombstones: includeTombstones);

        /// <summary>
        /// Resume session on current thread. IMPORTANT: Call SuspendThread before any async op.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeResumeThread<TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // We do not track any "acquired" state here; if someone mixes calls between safe and unsafe contexts, they will 
            // get the "trying to acquire already-acquired epoch" error.
            store.epoch.Resume();
            store.InternalRefresh<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions);
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

        /// <inheritdoc/>
        public void MergeRevivificationStatsTo(ref RevivificationStats to, bool reset) => ctx.MergeRevivificationStatsTo(ref to, reset);

        /// <inheritdoc/>
        public void ResetRevivificationStats() => ctx.ResetRevivificationStats();
        #endregion Other Operations
    }
}