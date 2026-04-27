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

            txnScratchBufferAllocator = new ScratchBufferAllocator();
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

        internal void Reset(bool isRunning)
        {
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
            if (PerformWrites && appendOnlyFile != null && !functionsState.StoredProcMode)
            {
                ComputeSublogAccessVector(out var physicalSublogAccessVector, out var virtualSublogAccessVector, out var virtualSublogParticipantCount);
                appendOnlyFile.Log.EnqueueTxn(AofEntryType.TxnCommit, txnVersion, stringBasicContext.Session.ID, physicalSublogAccessVector, virtualSublogAccessVector, virtualSublogParticipantCount);
            }
            if (!internal_txn)
                watchContainer.Reset();
            Reset(true);
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
                appendOnlyFile.Log.EnqueueTxn(AofEntryType.TxnStart, txnVersion, stringBasicContext.Session.ID, physicalSublogAccessVector, virtualSublogAccessVector, virtualSublogParticipantCount);
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
                proc.replayTaskAccessVector ??= [.. Enumerable.Range(0, appendOnlyFile.Log.Size).Select(_ => new BitVector(AofTransactionHeader.ReplayTaskAccessVectorBytes))];
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
            virtualSublogAccessVector = [.. Enumerable.Range(0, appendOnlyFile.Log.Size).Select(_ => new BitVector(AofTransactionHeader.ReplayTaskAccessVectorBytes))];

            // If sharded log is enabled calculate sublog access bitmap
            for (var i = 0; i < txnKeysParseState.Count; i++)
            {
                ref var key = ref txnKeysParseState.GetArgSliceByRef(i);
                var keyHash = GarnetLog.HASH(key.ReadOnlySpan);
                var physicalSublogIdx = appendOnlyFile.Log.GetPhysicalSublogIdx(keyHash);
                var replayIdx = appendOnlyFile.Log.GetReplayTaskIdx(keyHash);
                physicalSublogAccessVector |= 1UL << physicalSublogIdx;
                // Calculate sublog access vector for participating replay tasks
                participantCount += virtualSublogAccessVector[physicalSublogIdx].SetBit(replayIdx) ? 1 : 0;
            }
        }
    }
}