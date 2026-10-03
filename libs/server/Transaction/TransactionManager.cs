// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    [Flags]
    public enum TransactionStoreTypes : byte
    {
        None = 0,
        Main = 1,
        Object = 1 << 1,
        Unified = 1 << 2,
    }

    /// <summary>
    /// Transaction manager
    /// </summary>
    public sealed unsafe partial class TransactionManager
    {
        /// <summary>
        /// Disposable to allow <see cref="PromoteToTransaction"/> to be handled with a using block.
        /// </summary>
        public readonly struct TransactionGuard : IDisposable
        {
            internal static readonly TransactionGuard Null = new();

            private readonly TransactionManager txnManager;

            internal TransactionGuard(TransactionManager txnManager)
            {
                this.txnManager = txnManager;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                txnManager?.Commit(true);
            }
        }

        internal bool AofEnabled => appendOnlyFile != null;

        /// <summary>
        /// Basic context for main store
        /// </summary>
        readonly StringBasicContext stringBasicContext;

        /// <summary>
        /// Transactional context for main store
        /// </summary>
        readonly StringTransactionalContext stringTransactionalContext;

        /// <summary>
        /// Basic context for object store
        /// </summary>
        readonly ObjectBasicContext objectBasicContext;

        /// <summary>
        /// Transactional context for object store
        /// </summary>
        readonly ObjectTransactionalContext objectTransactionalContext;

        /// <summary>
        /// Basic context for unified store
        /// </summary>
        readonly UnifiedBasicContext unifiedBasicContext;

        /// <summary>
        /// Transactional context for unified store
        /// </summary>
        readonly UnifiedTransactionalContext unifiedTransactionalContext;

        // Not readonly to avoid defensive copy
        GarnetWatchApi<BasicGarnetApi> garnetTxPrepareApi;

        // Not readonly to avoid defensive copy
        TransactionalGarnetApi garnetTxMainApi;

        // Not readonly to avoid defensive copy
        BasicGarnetApi garnetTxFinalizeApi;

        // Not readonly to avoid defensive copy
        GarnetWatchApi<ConsistentReadGarnetApi> garnetConsistentTxPrepareApi;

        // Not readonly to avoid defensive copy
        TransactionalConsistentReadGarnetApi garnetConsistentTxRunApi;

        // Not readonly to avoid defensive copy
        ConsistentReadGarnetApi garnetConsistentTxFinalizeApi;

        readonly bool enableConsistentRead;

        private readonly RespServerSession respSession;
        readonly FunctionsState functionsState;
        internal readonly ScratchBufferAllocator scratchBufferAllocator;
        internal readonly ScratchBufferAllocator txnScratchBufferAllocator;
        internal SessionParseState txnKeysParseState;
        private readonly GarnetAppendOnlyFile appendOnlyFile;
        internal readonly WatchedKeysContainer watchContainer;
        private readonly StateMachineDriver stateMachineDriver;
        readonly GarnetServerOptions serverOptions;
        internal int txnStartHead;
        internal int operationCntTxn;

        // Track whether transaction contains write operations
        internal bool PerformWrites;

        /// <summary>
        /// State
        /// </summary>
        public TxnState state;

        /// <summary>
        /// True once <see cref="Run"/> has acquired the transactional contexts, locks and store version,
        /// and until <see cref="Reset(bool)"/> has released them.
        /// </summary>
        /// <remarks>
        /// <para>
        /// <see cref="state"/> cannot be used to decide whether this manager owns a transaction, for two
        /// reasons. Lua's transactional mode assigns <see cref="TxnState.Running"/> directly through
        /// <c>SetTransactionMode</c> while owning a *different* lock set and holding its store version in a
        /// local, so a manager in that state owns nothing and must not release anything. And
        /// <see cref="Run"/> acquires the contexts and locks well before it assigns
        /// <see cref="TxnState.Running"/>, so a failure in between -- the <c>TxnStart</c> append is the
        /// reachable one -- leaves a transaction that is owned but not yet <c>Running</c>.
        /// </para>
        /// <para>
        /// Ownership answers "must this be released", which is what teardown needs.
        /// <see cref="TxnState.Running"/> answers "is the AOF group open and is <c>EXEC</c> mid-replay",
        /// which is what the closing marker and the commit pass need. They are not the same question.
        /// </para>
        /// </remarks>
        internal bool OwnsTransaction { get; private set; }

        /// <summary>
        /// Whether this transaction has appended its opening <see cref="AofEntryType.TxnStart"/> marker,
        /// and so has a group in the log that something must terminate.
        /// </summary>
        bool txnStartPublished;
        private const int initialSliceBufferSize = 1 << 10;
        private const int initialKeyBufferSize = 1 << 10;
        readonly ILogger logger;
        long txnVersion;
        private TransactionStoreTypes storeTypes;

        internal StringTransactionalContext StringTransactionalContext
            => stringTransactionalContext;
        internal StringTransactionalUnsafeContext TransactionalUnsafeContext
            => stringBasicContext.Session.TransactionalUnsafeContext;
        internal ObjectTransactionalContext ObjectTransactionalContext
            => objectTransactionalContext;
        internal UnifiedTransactionalContext UnifiedTransactionalContext
            => unifiedTransactionalContext;

        bool IsReplaying { get; set; } = false;

        /// <summary>
        /// Array to keep pointer keys in keyBuffer
        /// </summary>
        private TxnKeyEntries keyEntries;

        internal TransactionManager(
            StoreWrapper storeWrapper,
            RespServerSession respSession,
            BasicGarnetApi garnetApi,
            TransactionalGarnetApi transactionalGarnetApi,
            StorageSession storageSession,
            ScratchBufferAllocator scratchBufferAllocator,
            bool clusterEnabled,
            bool enableConsistentRead = false,
            ConsistentReadGarnetApi garnetConsistentApi = default,
            TransactionalConsistentReadGarnetApi transactionalConsistentGarnetApi = default,
            ILogger logger = null,
            int dbId = 0)
        {
            serverOptions = storeWrapper.serverOptions;
            var session = storageSession.stringBasicContext.Session;
            stringBasicContext = session.BasicContext;
            stringTransactionalContext = session.TransactionalContext;

            if (!storeWrapper.serverOptions.DisableObjects)
            {
                var objectSession = storageSession.objectBasicContext.Session;
                objectBasicContext = objectSession.BasicContext;
                objectTransactionalContext = objectSession.TransactionalContext;
            }

            var unifiedStoreSession = storageSession.unifiedBasicContext.Session;
            unifiedBasicContext = unifiedStoreSession.BasicContext;
            unifiedTransactionalContext = unifiedStoreSession.TransactionalContext;

            this.functionsState = storageSession.functionsState;
            this.appendOnlyFile = functionsState.appendOnlyFile;
            this.logger = logger;

            this.respSession = respSession;

            txnScratchBufferAllocator = new ScratchBufferAllocator(
                maxInitialCapacity: storeWrapper.serverOptions.GetSessionScratchBufferMaxRetainedSize());
            watchContainer = new WatchedKeysContainer(initialSliceBufferSize, functionsState.watchVersionMap, txnScratchBufferAllocator);
            keyEntries = new TxnKeyEntries(initialSliceBufferSize, unifiedTransactionalContext);
            this.scratchBufferAllocator = scratchBufferAllocator;

            var dbFound = storeWrapper.TryGetDatabase(dbId, out var db);
            Debug.Assert(dbFound);
            this.stateMachineDriver = db.StateMachineDriver;

            garnetTxMainApi = transactionalGarnetApi;
            garnetTxPrepareApi = new GarnetWatchApi<BasicGarnetApi>(garnetApi);
            garnetTxFinalizeApi = garnetApi;

            this.enableConsistentRead = enableConsistentRead;
            if (enableConsistentRead)
            {
                garnetConsistentTxPrepareApi = new GarnetWatchApi<ConsistentReadGarnetApi>(garnetConsistentApi);
                garnetConsistentTxRunApi = transactionalConsistentGarnetApi;
                garnetConsistentTxFinalizeApi = garnetConsistentApi;
            }

            this.clusterEnabled = clusterEnabled;
            if (clusterEnabled)
            {
                txnKeysParseState.Initialize(initialKeyBufferSize);
                txnKeysParseState.Count = 0;
            }

            Reset(false);
        }

        internal void Reset() => Reset(state == TxnState.Running);

        /// <summary>
        /// Releases the transaction scratch allocator if it is over its cap, idle, and holds nothing live.
        /// Driven from the session's batch boundary; WATCHed key slices span batches, so the allocator
        /// itself declines to shrink while anything is outstanding.
        /// </summary>
        internal void TrimScratchBuffer() => txnScratchBufferAllocator.Trim();

        /// <summary>
        /// Releases both scratch allocators on the AOF replay path. The caller counts the interval, so this
        /// trims every time it is called.
        /// </summary>
        /// <remarks>
        /// AOF replay reaches this manager through replayed transaction procedures, which grow both scratch
        /// allocators: the prepare phase runs against the watch API, copying every watched key into the
        /// transaction allocator, and the procedure itself builds arguments from the session allocator. The
        /// replay session never enters the network batch boundary, so without this the buffers one wide
        /// procedure grew stay pinned for the lifetime of a replica.
        /// <para>
        /// The transaction allocator is safe to trim while a transaction is in flight, because it
        /// shrinks only when nothing is outstanding. The session allocator has no such guard and is reset at
        /// the start of every procedure rather than at the end, so it is quiescent only between
        /// procedures: it is reset here, as the network batch boundary does, and only outside a transaction.
        /// </para>
        /// </remarks>
        internal void TrimReplayBuffers()
        {
            txnScratchBufferAllocator.Trim();

            // Between records nothing the previous one allocated is live, so the reset that makes the
            // trim effective is safe. A transaction still running owns live slices, so skip it.
            if (state != TxnState.Running)
            {
                scratchBufferAllocator.Reset();
                scratchBufferAllocator.Trim();
            }
        }

        internal void Reset(bool isRunning)
        {
            // Reset releases a transaction; it does not terminate the AOF group one opened. Reaching here
            // with a group still open means some exit released the transaction without writing the marker
            // that closes it, and replay will attribute every later record from this session to a group
            // that never commits and drop them all. Commit and Abandon clear the flag as they emit.
            Debug.Assert(!txnStartPublished,
                "A transaction was released with its AOF group still open; terminate it via Commit or Abandon.");

            if (isRunning)
            {
                try
                {
                    keyEntries.UnlockAllKeys();

                    // Release contexts
                    if ((storeTypes & TransactionStoreTypes.Main) == TransactionStoreTypes.Main)
                        stringTransactionalContext.EndTransaction();
                    if ((storeTypes & TransactionStoreTypes.Object) == TransactionStoreTypes.Object && !objectBasicContext.IsNull)
                        objectTransactionalContext.EndTransaction();
                    unifiedTransactionalContext.EndTransaction();
                }
                finally
                {
                    stateMachineDriver.EndTransaction(txnVersion);
                }
            }
            this.txnVersion = 0;
            this.txnStartHead = 0;
            this.operationCntTxn = 0;
            this.state = TxnState.None;
            this.OwnsTransaction = false;
            this.txnStartPublished = false;
            this.storeTypes = TransactionStoreTypes.None;
            functionsState.StoredProcMode = false;
            this.PerformWrites = false;

            // Reset cluster key parse state
            if (clusterEnabled)
            {
                txnKeysParseState.Count = 0;
                saveKeyRecvBufferPtr = null;
                txnScratchBufferAllocator.Reset();
            }
        }

        internal bool RunTransactionProc(byte id, ref CustomProcedureInput procInput, CustomTransactionProcedure proc, ref MemoryResult<byte> output, bool isReplaying = false)
        {
            if (enableConsistentRead)
            {
                return RunTransactionProcInternal(
                    ref garnetConsistentTxPrepareApi,
                    ref garnetConsistentTxRunApi,
                    ref garnetConsistentTxFinalizeApi,
                    id,
                    ref procInput,
                    proc,
                    ref output,
                    isReplaying);
            }
            else
            {
                return RunTransactionProcInternal(
                    ref garnetTxPrepareApi,
                    ref garnetTxMainApi,
                    ref garnetTxFinalizeApi,
                    id,
                    ref procInput,
                    proc,
                    ref output,
                    isReplaying);
            }
        }

        private bool RunTransactionProcInternal<TPrepareApi, TRunApi, TFinalizeApi>(
            ref TPrepareApi garnetTxPrepareApi,
            ref TRunApi garnetTxRunApi,
            ref TFinalizeApi garnetTxFinalizeApi,
            byte id,
            ref CustomProcedureInput procInput,
            CustomTransactionProcedure proc,
            ref MemoryResult<byte> output,
            bool isReplaying = false)
            where TPrepareApi : IGarnetReadApi
            where TRunApi : IGarnetApi
            where TFinalizeApi : IGarnetApi
        {
            var running = false;
            scratchBufferAllocator.Reset();
            IsReplaying = isReplaying;
            try
            {
                // If cluster is enabled reset slot verification state cache
                ResetCacheSlotVerificationResult();

                // Reset logAccess for sharded log
                if (serverOptions.MultiLogEnabled)
                {
                    proc.physicalSublogAccessVector = 0UL;
                    proc.virtualSublogParticipantCount = 0;
                    if (proc.replayTaskAccessVector != null)
                    {
                        foreach (var vector in proc.replayTaskAccessVector)
                            vector.Clear();
                    }
                }

                functionsState.StoredProcMode = true;

                // Prepare phase
                if (!proc.Prepare(garnetTxPrepareApi, ref procInput))
                {
                    Reset(running);
                    return false;
                }

                if (state == TxnState.Aborted)
                {
                    WriteCachedSlotVerificationMessage(ref output);
                    Reset(running);
                    return false;
                }

                // Start the TransactionManager
                if (!Run(fail_fast_on_lock: proc.FailFastOnKeyLockFailure, lock_timeout: proc.KeyLockTimeout))
                {
                    Reset(running);
                    return false;
                }

                running = true;

                // Run main procedure on locked data
                proc.Main(garnetTxRunApi, ref procInput, ref output);

                // Log the transaction to AOF
                Log(id, ref procInput, proc);

                // Transaction Commit
                Commit();
            }
            catch (Exception ex)
            {
                Reset(running);
                logger?.LogError(ex, "TransactionManager.RunTransactionProc error in running transaction proc");
                return false;
            }
            finally
            {
                try
                {
                    // Run finalize procedure at the end.
                    // If the transaction was invoked during AOF replay skip the finalize step altogether
                    // Finalize logs to AOF accordingly, so let the replay pick up the commits from AOF as
                    // part of normal AOF replay.
                    if (!isReplaying)
                    {
                        proc.Finalize(garnetTxFinalizeApi, ref procInput, ref output);
                    }
                }
                catch { }

                // Reset scratch buffer for next txn invocation
                scratchBufferAllocator.Reset();
            }

            return true;
        }

        void Log(byte id, ref CustomProcedureInput procInput, CustomTransactionProcedure proc)
        {
            Debug.Assert(functionsState.StoredProcMode);

            if (PerformWrites && appendOnlyFile != null)
                appendOnlyFile.Log.EnqueueStoredProc(AofEntryType.StoredProcedure, id, txnVersion, stringBasicContext.Session.ID, ref procInput, proc);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsSkippingOperations()
        {
            return state == TxnState.Started || state == TxnState.Aborted;
        }

        internal void Abort()
        {
            state = TxnState.Aborted;
        }

        internal void Commit(bool internal_txn = false)
        {
            if (txnStartPublished)
            {
                ComputeSublogAccessVector(out var physicalSublogAccessVector, out var virtualSublogAccessVector, out var virtualSublogParticipantCount);
                appendOnlyFile.Log.EnqueueTxn(AofEntryType.TxnCommit, txnVersion, stringBasicContext.Session.ID, physicalSublogAccessVector, virtualSublogAccessVector, virtualSublogParticipantCount);
                txnStartPublished = false;
            }
            if (!internal_txn)
                watchContainer.Reset();
            Reset(true);
        }

        /// <summary>
        /// Ends a transaction that a failed batch left behind.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Guarded on <see cref="OwnsTransaction"/> rather than on <see cref="TxnState.Running"/>, because
        /// the two differ at both ends. Lua's transactional mode sets <c>Running</c> on a manager that owns
        /// nothing, and releasing Lua's contexts from here unbalances the store's transaction counters and
        /// ends contexts whose locks Lua still holds. In the other direction, <see cref="Run"/> owns the
        /// contexts and locks before it reaches <c>Running</c>, so a failure to append <c>TxnStart</c>
        /// would otherwise leave them held with nothing to release them.
        /// </para>
        /// <para>
        /// Once <see cref="Run"/> has appended <see cref="AofEntryType.TxnStart"/>, the log holds an open
        /// group for this session. Replay attributes every subsequent record to that group until something
        /// terminates it, and a later <c>TxnStart</c> on the same session fails replay outright with
        /// "No nested transactions expected". Ending the group is as much a part of abandoning the
        /// transaction as releasing its locks -- but only when there is a group to end, which is why the
        /// marker is conditioned on the opening append while the release below is not.
        /// </para>
        /// <para>
        /// The marker is <see cref="AofEntryType.TxnCommit"/>, not <see cref="AofEntryType.TxnAbort"/>, because
        /// <c>EXEC</c> does not roll back: the commands that ran before the failure mutated the store and were
        /// acknowledged to the client, and the ones after it never ran. Committing the group is what makes a
        /// recovered store match the live one. Aborting would discard writes the client was told had landed.
        /// </para>
        /// <para>
        /// The watch container is reset with the rest of the attempt. A transaction's WATCHes belong to the
        /// attempt that was about to consume them, and both paths that refuse to start a transaction
        /// without throwing already clear them. The path that throws can now leave the session alive, and a
        /// registration that outlives its attempt silently aborts the *next*, unrelated transaction on that
        /// connection once the watched key is modified.
        /// </para>
        /// </remarks>
        internal void Abandon()
        {
            if (!OwnsTransaction)
                return;

            try
            {
                // Keyed off the opening marker rather than off state: Run appends TxnStart before it
                // reaches TxnState.Running, so a failure in between leaves a published group that state
                // says nothing about. The flag is also raised before the append rather than after, because
                // the append publishes its record before the commit that can throw. Both choices err the
                // same way, and the asymmetry justifies it: replay ignores a transaction end with no start
                // outright -- a truncating checkpoint produces them routinely -- while a start with no end
                // swallows every record after it, from every session, and fails the next transaction on
                // this session with "No nested transactions expected".
                if (txnStartPublished)
                {
                    ComputeSublogAccessVector(out var physicalSublogAccessVector, out var virtualSublogAccessVector, out var virtualSublogParticipantCount);
                    appendOnlyFile.Log.EnqueueTxn(AofEntryType.TxnCommit, txnVersion, stringBasicContext.Session.ID, physicalSublogAccessVector, virtualSublogAccessVector, virtualSublogParticipantCount);
                }
            }
            finally
            {
                // Cleared here rather than after the append, because the append can throw: the group is no
                // longer this transaction's to terminate either way, and leaving the flag raised would only
                // make the release below trip the assertion that guards against exactly that.
                txnStartPublished = false;

                // Appending the marker can fail -- a full or failed device surfaces as an exception from
                // the append path -- and the transaction's locks, contexts and store version have to come
                // off regardless. Losing the group's terminator costs the tail of the log, which a device
                // that is already failing was going to cost anyway. Losing the release strands them with no
                // owner: keys stay exclusively locked, and the store's count of active transactions in the
                // version never drops, so no checkpoint can complete again.
                //
                // Demonstrated rather than assumed: injecting a throw at the append above and removing this
                // finally wedges the server, and wedges it past shutdown. That severity is also why there is
                // no test for it -- a regression would hang the test host rather than fail it.
                watchContainer.Reset();
                Reset(true);
            }
        }

        internal void Watch(PinnedSpanByte key)
        {
            watchContainer.AddWatch(key);

            // Release context
            if ((storeTypes & TransactionStoreTypes.Main) == TransactionStoreTypes.Main)
                stringTransactionalContext.ResetModified((FixedSpanByteKey)key);
            if ((storeTypes & TransactionStoreTypes.Object) == TransactionStoreTypes.Object && !objectBasicContext.IsNull)
                objectTransactionalContext.ResetModified((FixedSpanByteKey)key);
            unifiedTransactionalContext.ResetModified((FixedSpanByteKey)key);
        }

        internal void AddTransactionStoreTypes(TransactionStoreTypes transactionStoreTypes)
        {
            this.storeTypes |= transactionStoreTypes;
        }

        internal void AddTransactionStoreType(StoreType storeType)
        {
            var transactionStoreTypes = storeType switch
            {
                StoreType.Main => TransactionStoreTypes.Main,
                StoreType.Object => TransactionStoreTypes.Object,
                StoreType.All => TransactionStoreTypes.Unified,
                _ => TransactionStoreTypes.None
            };

            this.storeTypes |= transactionStoreTypes;
        }

        internal string GetLockset() => keyEntries.GetLockset();

        internal void GetSlotVerificationInput(byte* recvBufferPtr, byte sessionAsking, out ClusterSlotVerificationInput clusterSlotVerificationInput)
        {
            // Copy keys if buffer changed since last queued command
            if (recvBufferPtr != saveKeyRecvBufferPtr)
            {
                CopyExistingKeysToScratchBuffer();
                saveKeyRecvBufferPtr = recvBufferPtr;
            }

            watchContainer.SaveKeysToKeyList(this);
            clusterSlotVerificationInput = new ClusterSlotVerificationInput
            {
                readOnly = keyEntries.IsReadOnly,
                sessionAsking = sessionAsking,
                // We don't specify key specs here as slot verification will know to iterate over all keys in this context
            };
        }

        void BeginTransaction()
        {
            if ((storeTypes & TransactionStoreTypes.Main) == TransactionStoreTypes.Main)
                stringTransactionalContext.BeginTransaction();
            if ((storeTypes & TransactionStoreTypes.Object) == TransactionStoreTypes.Object && !objectBasicContext.IsNull)
                objectTransactionalContext.BeginTransaction();
            unifiedTransactionalContext.BeginTransaction();
        }

        void LocksAcquired(long txnVersion)
        {
            if ((storeTypes & TransactionStoreTypes.Main) == TransactionStoreTypes.Main)
                stringTransactionalContext.LocksAcquired(txnVersion);
            if ((storeTypes & TransactionStoreTypes.Object) == TransactionStoreTypes.Object && !objectBasicContext.IsNull)
                objectTransactionalContext.LocksAcquired(txnVersion);
            unifiedTransactionalContext.LocksAcquired(txnVersion);
        }

        internal bool Run(bool internal_txn = false, bool fail_fast_on_lock = false, TimeSpan lock_timeout = default)
        {
            // Save watch keys to lock list
            if (!internal_txn)
                watchContainer.SaveKeysToLock(this);

            // Acquire transaction version
            txnVersion = stateMachineDriver.AcquireTransactionVersion();

            // Acquire lock sessions
            BeginTransaction();

            // From here the contexts, the locks taken below and the store version all have to be released,
            // whether or not this call reaches TxnState.Running.
            OwnsTransaction = true;

            bool lockSuccess;
            if (fail_fast_on_lock)
            {
                lockSuccess = keyEntries.TryLockAllKeys(lock_timeout);
            }
            else
            {
                keyEntries.LockAllKeys();
                lockSuccess = true;
            }

            if (!lockSuccess ||
                (!internal_txn && !watchContainer.ValidateWatchVersion()))
            {
                if (!lockSuccess)
                {
                    this.logger?.LogError("Transaction failed to acquire all the locks on keys to proceed.");
                }
                Reset(true);
                if (!internal_txn)
                    watchContainer.Reset();
                return false;
            }

            // Verify transaction version
            txnVersion = stateMachineDriver.VerifyTransactionVersion(txnVersion);

            // Update sessions with transaction version
            LocksAcquired(txnVersion);

            // Add TxnStart Marker
            if (PerformWrites && appendOnlyFile != null && !functionsState.StoredProcMode)
            {
                ComputeSublogAccessVector(out var physicalSublogAccessVector, out var virtualSublogAccessVector, out var virtualSublogParticipantCount);

                // Before the append, not after: the append publishes the record and can then throw from
                // the commit behind it, so a flag raised afterwards would miss a group that is already in
                // the log. See Abandon for why erring this way is the safe direction.
                txnStartPublished = true;

                ExceptionInjectionHelper.TriggerRecoverableException(ExceptionInjectionType.Transaction_Fail_Before_TxnStart_Append);

                appendOnlyFile.Log.EnqueueTxn(AofEntryType.TxnStart, txnVersion, stringBasicContext.Session.ID, physicalSublogAccessVector, virtualSublogAccessVector, virtualSublogParticipantCount);

                ExceptionInjectionHelper.TriggerRecoverableException(ExceptionInjectionType.Transaction_Fail_After_TxnStart_Append);
            }

            state = TxnState.Running;
            return true;
        }

        /// <summary>
        /// Compute metadata required for sharded log custom transaction replay
        /// </summary>
        /// <param name="key"></param>
        /// <param name="proc"></param>
        public void ComputeCustomProcShardedLogAccess(PinnedSpanByte key, CustomTransactionProcedure proc)
        {
            // Skip if AOF is disabled
            if (appendOnlyFile == null)
                return;

            // Skip if singleLog
            if (!serverOptions.MultiLogEnabled)
                return;

            var keyHash = GarnetLog.HASH(key);
            if (proc.customProcKeyHashCollection == null)
            {
                // Used with parallel replay, this BitVector will track which replay tasks should participate in the parallel replay of this custom proc.
                proc.replayTaskAccessVector ??= [.. Enumerable.Range(0, appendOnlyFile.Log.Size).Select(_ => new BitVector(AofShardedLogTransactionHeader.ReplayTaskAccessVectorBytes))];
                var physicalSublogIdx = appendOnlyFile.Log.GetPhysicalSublogIdx(keyHash);
                var replayIdx = appendOnlyFile.Log.GetReplayTaskIdx(keyHash);

                // Mark physical sublog participating in custom txn proc to help with replay coordination.
                proc.physicalSublogAccessVector |= 1UL << physicalSublogIdx;
                // Mark replay task participation and update count replay tasks participating in replay.
                proc.virtualSublogParticipantCount += proc.replayTaskAccessVector[physicalSublogIdx].SetBit(replayIdx) ? 1 : 0;
            }
            else
                // Keep track of key hashes to update sequence numbers of keys at end of replay
                proc.customProcKeyHashCollection.AddHash(keyHash);
        }

        /// <summary>
        /// Compute metadata required for sharded log transaction replay
        /// </summary>
        /// <param name="physicalSublogAccessVector"></param>
        /// <param name="virtualSublogAccessVector"></param>
        /// <param name="participantCount"></param>
        void ComputeSublogAccessVector(out ulong physicalSublogAccessVector, out BitVector[] virtualSublogAccessVector, out int participantCount)
        {
            physicalSublogAccessVector = 0UL;
            virtualSublogAccessVector = null;
            participantCount = 0;
            // Skip if AOF is disabled
            if (appendOnlyFile == null)
                return;

            // If singleLog no computation is necessary
            if (appendOnlyFile.Log.Size == 1 && appendOnlyFile.Log.ReplayTaskCount == 1)
                return;

            // Initialize only for multi-log
            virtualSublogAccessVector = [.. Enumerable.Range(0, appendOnlyFile.Log.Size).Select(_ => new BitVector(AofShardedLogTransactionHeader.ReplayTaskAccessVectorBytes))];

            // Compute the sublog access bitmap from the transaction's locked keys. keyEntries is populated in both
            // standalone and cluster mode (via SaveKeyEntryToLock), unlike txnKeysParseState which is only filled during
            // cluster slot verification (TxnClusterSlotCheck.SaveKeyArgSlice returns early when !clusterEnabled). The
            // stored keyHash equals GarnetLog.HASH of the key, so it is used directly (no re-hashing).
            for (var i = 0; i < keyEntries.Count; i++)
            {
                var keyHash = keyEntries.GetKeyHash(i);
                var physicalSublogIdx = appendOnlyFile.Log.GetPhysicalSublogIdx(keyHash);
                var replayIdx = appendOnlyFile.Log.GetReplayTaskIdx(keyHash);
                physicalSublogAccessVector |= 1UL << physicalSublogIdx;
                // Calculate sublog access vector for participating replay tasks
                participantCount += virtualSublogAccessVector[physicalSublogIdx].SetBit(replayIdx) ? 1 : 0;
            }
        }

        /// <summary>
        /// Helper to DRY-up the common pattern of promoting to a transaction when two (or more) subcommands need to be made atomic.
        /// 
        /// Returns a disposable that, when <see cref="IDisposable.Dispose"/> is called will commit the transaction if it promoted.
        /// </summary>
        internal TransactionGuard PromoteToTransaction(TransactionStoreTypes storeTypes, PinnedSpanByte key, LockType lockType)
        {
            // We're already in a transaction
            if (state == TxnState.Running)
            {
                return TransactionGuard.Null;
            }

            AddTransactionStoreTypes(storeTypes);
            SaveKeyEntryToLock(key, lockType);

            _ = Run(true);

            return new(this);
        }
    }
}