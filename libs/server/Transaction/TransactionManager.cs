// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using BasicGarnetApi = GarnetApi<BasicContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        BasicContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        BasicContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>>;
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;
    using TransactionalGarnetApi = GarnetApi<TransactionalContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        TransactionalContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        TransactionalContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>>;

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
        readonly BasicContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> stringBasicContext;

        /// <summary>
        /// Transactional context for main store
        /// </summary>
        readonly TransactionalContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> stringTransactionalContext;

        /// <summary>
        /// Basic context for object store
        /// </summary>
        readonly BasicContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectBasicContext;

        /// <summary>
        /// Transactional context for object store
        /// </summary>
        readonly TransactionalContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectTransactionalContext;

        /// <summary>
        /// Basic context for unified store
        /// </summary>
        readonly BasicContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedBasicContext;

        /// <summary>
        /// Transactional context for unified store
        /// </summary>
        readonly TransactionalContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedTransactionalContext;

        // Not readonly to avoid defensive copy
        GarnetWatchApi<BasicGarnetApi> garnetTxPrepareApi;

        // Not readonly to avoid defensive copy
        TransactionalGarnetApi garnetTxMainApi;

        // Not readonly to avoid defensive copy
        BasicGarnetApi garnetTxFinalizeApi;

        private readonly RespServerSession respSession;
        readonly FunctionsState functionsState;
        internal readonly ScratchBufferAllocator scratchBufferAllocator;
        private readonly TsavoriteLog appendOnlyFile;
        internal readonly WatchedKeysContainer watchContainer;
        private readonly StateMachineDriver stateMachineDriver;
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

        internal TransactionalContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> StringTransactionalContext
            => stringTransactionalContext;
        internal TransactionalUnsafeContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> TransactionalUnsafeContext
            => stringBasicContext.Session.TransactionalUnsafeContext;
        internal TransactionalContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> ObjectTransactionalContext
            => objectTransactionalContext;
        internal TransactionalContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> UnifiedTransactionalContext
            => unifiedTransactionalContext;

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
            ILogger logger = null,
            int dbId = 0)
        {
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

            watchContainer = new WatchedKeysContainer(initialSliceBufferSize, functionsState.watchVersionMap);
            keyEntries = new TxnKeyEntries(initialSliceBufferSize, unifiedTransactionalContext);
            this.scratchBufferAllocator = scratchBufferAllocator;

            var dbFound = storeWrapper.TryGetDatabase(dbId, out var db);
            Debug.Assert(dbFound);
            this.stateMachineDriver = db.StateMachineDriver;

            garnetTxMainApi = transactionalGarnetApi;
            garnetTxPrepareApi = new GarnetWatchApi<BasicGarnetApi>(garnetApi);
            garnetTxFinalizeApi = garnetApi;

            this.clusterEnabled = clusterEnabled;
            if (clusterEnabled)
                keys = new PinnedSpanByte[initialKeyBufferSize];

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

            // Reset cluster variables used for slot verification
            this.saveKeyRecvBufferPtr = null;
            this.keyCount = 0;
        }

        internal bool RunTransactionProc(byte id, ref CustomProcedureInput procInput, CustomTransactionProcedure proc, ref MemoryResult<byte> output, bool isRecovering = false)
        {
            var running = false;
            scratchBufferAllocator.Reset();
            try
            {
                // If cluster is enabled reset slot verification state cache
                ResetCacheSlotVerificationResult();

                functionsState.StoredProcMode = true;
                // Prepare phase
                if (!proc.Prepare(garnetTxPrepareApi, ref procInput))
                {
                    Reset(running);
                    return false;
                }

                // State will never be Aborted at this point for non-cluster mode. This code path is only for TransactionProcedure.
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
                proc.Main(garnetTxMainApi, ref procInput, ref output);

                // Log the transaction to AOF
                Log(id, ref procInput);

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
                    if (!isRecovering)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsSkippingOperations()
        {
            return state == TxnState.Started || state == TxnState.Aborted;
        }

        internal void Abort()
        {
            state = TxnState.Aborted;
        }

        internal void Log(byte id, ref CustomProcedureInput procInput)
        {
            Debug.Assert(functionsState.StoredProcMode);

            if (PerformWrites)
            {
                appendOnlyFile?.Enqueue(
                    new AofHeader { opType = AofEntryType.StoredProcedure, procedureId = id, storeVersion = txnVersion, sessionID = stringBasicContext.Session.ID },
                    ref procInput,
                    out _);
            }
        }

        internal void Commit(bool internal_txn = false)
        {
            if (PerformWrites && appendOnlyFile != null && !functionsState.StoredProcMode)
            {
                appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.TxnCommit, storeVersion = txnVersion, sessionID = stringBasicContext.Session.ID }, out _);
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
                stringTransactionalContext.ResetModified(key.ReadOnlySpan);
            if ((storeTypes & TransactionStoreTypes.Object) == TransactionStoreTypes.Object && !objectBasicContext.IsNull)
                objectTransactionalContext.ResetModified(key.ReadOnlySpan);
            unifiedTransactionalContext.ResetModified(key.ReadOnlySpan);
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

        internal void GetKeysForValidation(byte* recvBufferPtr, out PinnedSpanByte[] keys, out int keyCount, out bool readOnly)
        {
            UpdateRecvBufferPtr(recvBufferPtr);
            watchContainer.SaveKeysToKeyList(this);
            keys = this.keys;
            keyCount = this.keyCount;
            readOnly = keyEntries.IsReadOnly;
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

            // Acquire transaction version.
            // Version is associated to the version the state machine runs in. Version is incremented everytime Checkpointing state machine goes from Prepare to in-progress
            // We acquire the version here to ensure that we have the latest version before acquiring locks.
            // This call may block if the system is in prepare_grow phase of the index resizing state machine till it moves to In-progress_grow phase.
            txnVersion = stateMachineDriver.AcquireTransactionVersion();
            //stateMachineDriver.WaitForPrepareGrowComplete();

            // For the ongoing session, mark it as in-transaction. This marking is only used to handle cases during index resizing.
            // See further explanation in TsavoriteThread.cs [InternalRefresh].
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

            // verify and possibly update txn version after locks are acquired
            txnVersion = stateMachineDriver.VerifyTransactionVersion(txnVersion);

            // acquire txn version after locks are acquired
            //txnVersion = stateMachineDriver.AcquireTransactionVersionFastNoBarrier();

            // Update sessions with transaction version
            LocksAcquired(txnVersion);

            // Do not write to AOF if no write operations
            if (PerformWrites && appendOnlyFile != null && !functionsState.StoredProcMode)
            {
                appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.TxnStart, storeVersion = txnVersion, sessionID = stringBasicContext.Session.ID }, out _);
            }

            state = TxnState.Running;
            return true;
        }
    }
}