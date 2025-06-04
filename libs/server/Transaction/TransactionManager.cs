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
    using BasicGarnetApi = GarnetApi<BasicContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>>>,
        BasicContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>>;
    using TransactionalGarnetApi = GarnetApi<TransactionalContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>>>,
        TransactionalContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>>;

    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using ObjectStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Transaction manager
    /// </summary>
    public sealed unsafe partial class TransactionManager
    {
        /// <summary>
        /// Basic context for main store
        /// </summary>
        readonly BasicContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext;

        /// <summary>
        /// Transactional context for main store
        /// </summary>
        readonly TransactionalContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> transactionalContext;

        /// <summary>
        /// Basic context for object store
        /// </summary>
        readonly BasicContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreBasicContext;

        /// <summary>
        /// Transactional context for object store
        /// </summary>
        readonly TransactionalContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreTransactionalContext;

        // Not readonly to avoid defensive copy
        GarnetWatchApi<BasicGarnetApi> garnetTxPrepareApi;

        // Not readonly to avoid defensive copy
        TransactionalGarnetApi garnetTxMainApi;

        // Not readonly to avoid defensive copy
        BasicGarnetApi garnetTxFinalizeApi;

        private readonly RespServerSession respSession;
        readonly FunctionsState functionsState;
        internal readonly ScratchBufferManager scratchBufferManager;
        private readonly TsavoriteLog appendOnlyFile;
        internal readonly WatchedKeysContainer watchContainer;
        private readonly StateMachineDriver stateMachineDriver;
        internal int txnStartHead;
        internal int operationCntTxn;

        /// <summary>
        /// State
        /// </summary>
        public TxnState state;
        private const int initialSliceBufferSize = 1 << 10;
        private const int initialKeyBufferSize = 1 << 10;
        StoreType transactionStoreType;
        readonly ILogger logger;
        long txnVersion;

        internal TransactionalContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> TransactionalContext
            => transactionalContext;
        internal TransactionalUnsafeContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> TransactionalUnsafeContext
            => basicContext.Session.TransactionalUnsafeContext;
        internal TransactionalContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> ObjectStoreTransactionalContext
            => objectStoreTransactionalContext;

        /// <summary>
        /// Array to keep pointer keys in keyBuffer
        /// </summary>
        private TxnKeyEntries keyEntries;

        internal TransactionManager(StoreWrapper storeWrapper,
            RespServerSession respSession,
            BasicGarnetApi garnetApi,
            TransactionalGarnetApi transactionalGarnetApi,
            StorageSession storageSession,
            ScratchBufferManager scratchBufferManager,
            bool clusterEnabled,
            ILogger logger = null,
            int dbId = 0)
        {
            var session = storageSession.basicContext.Session;
            basicContext = session.BasicContext;
            transactionalContext = session.TransactionalContext;

            var objectStoreSession = storageSession.objectStoreBasicContext.Session;
            if (objectStoreSession != null)
            {
                objectStoreBasicContext = objectStoreSession.BasicContext;
                objectStoreTransactionalContext = objectStoreSession.TransactionalContext;
            }

            this.functionsState = storageSession.functionsState;
            this.appendOnlyFile = functionsState.appendOnlyFile;
            this.logger = logger;

            this.respSession = respSession;

            watchContainer = new WatchedKeysContainer(initialSliceBufferSize, functionsState.watchVersionMap);
            keyEntries = new TxnKeyEntries(initialSliceBufferSize, transactionalContext, objectStoreTransactionalContext);
            this.scratchBufferManager = scratchBufferManager;

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

        internal void Reset(bool isRunning)
        {
            if (isRunning)
            {
                try
                {
                    keyEntries.UnlockAllKeys();

                    // Release context
                    if (transactionStoreType == StoreType.Main || transactionStoreType == StoreType.All)
                        transactionalContext.EndTransaction();
                    if (transactionStoreType == StoreType.Object || transactionStoreType == StoreType.All)
                    {
                        if (objectStoreBasicContext.IsNull)
                            throw new Exception("Trying to perform object store transaction with object store disabled");
                        objectStoreTransactionalContext.EndTransaction();
                    }
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
            this.transactionStoreType = 0;
            functionsState.StoredProcMode = false;

            // Reset cluster variables used for slot verification
            this.saveKeyRecvBufferPtr = null;
            this.keyCount = 0;
        }

        internal bool RunTransactionProc(byte id, ref CustomProcedureInput procInput, CustomTransactionProcedure proc, ref MemoryResult<byte> output)
        {
            var running = false;
            scratchBufferManager.Reset();
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
                    // Run finalize procedure at the end
                    proc.Finalize(garnetTxFinalizeApi, ref procInput, ref output);
                }
                catch { }

                // Reset scratch buffer for next txn invocation
                scratchBufferManager.Reset();
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

            appendOnlyFile?.Enqueue(new AofHeader { opType = AofEntryType.StoredProcedure, procedureId = id, storeVersion = basicContext.Session.Version, sessionID = basicContext.Session.ID }, ref procInput, out _);
        }

        internal void Commit(bool internal_txn = false)
        {
            if (appendOnlyFile != null && !functionsState.StoredProcMode)
            {
                appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.TxnCommit, storeVersion = basicContext.Session.Version, sessionID = basicContext.Session.ID }, out _);
            }
            if (!internal_txn)
                watchContainer.Reset();
            Reset(true);
        }

        internal void Watch(PinnedSpanByte key, StoreType type)
        {
            // Update watch type if object store is disabled
            if (type == StoreType.All && objectStoreBasicContext.IsNull)
                type = StoreType.Main;

            UpdateTransactionStoreType(type);
            watchContainer.AddWatch(key, type);

            if (type == StoreType.Main || type == StoreType.All)
                basicContext.ResetModified(key.ReadOnlySpan);
            if ((type == StoreType.Object || type == StoreType.All) && !objectStoreBasicContext.IsNull)
                objectStoreBasicContext.ResetModified(key.ReadOnlySpan);
        }

        void UpdateTransactionStoreType(StoreType type)
        {
            if (transactionStoreType != StoreType.All)
            {
                if (transactionStoreType == 0)
                    transactionStoreType = type;
                else
                {
                    if (transactionStoreType != type)
                        transactionStoreType = StoreType.All;
                }
            }
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

        void BeginTransaction(StoreType transactionStoreType)
        {
            if (transactionStoreType is StoreType.All or StoreType.Main)
            {
                transactionalContext.BeginTransaction();
            }
            if (transactionStoreType is StoreType.All or StoreType.Object)
            {
                if (objectStoreBasicContext.IsNull)
                    throw new Exception("Trying to perform object store transaction with object store disabled");
                objectStoreTransactionalContext.BeginTransaction();
            }
        }

        void LocksAcquired(StoreType transactionStoreType, long txnVersion)
        {
            if (transactionStoreType is StoreType.All or StoreType.Main)
            {
                transactionalContext.LocksAcquired(txnVersion);
            }
            if (transactionStoreType is StoreType.All or StoreType.Object)
            {
                if (objectStoreBasicContext.IsNull)
                    throw new Exception("Trying to perform object store transaction with object store disabled");
                objectStoreTransactionalContext.LocksAcquired(txnVersion);
            }
        }

        internal bool Run(bool internal_txn = false, bool fail_fast_on_lock = false, TimeSpan lock_timeout = default)
        {
            // Save watch keys to lock list
            if (!internal_txn)
                watchContainer.SaveKeysToLock(this);

            // Acquire transaction version
            txnVersion = stateMachineDriver.AcquireTransactionVersion();

            // Acquire lock sessions
            BeginTransaction(transactionStoreType);

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
            LocksAcquired(transactionStoreType, txnVersion);

            if (appendOnlyFile != null && !functionsState.StoredProcMode)
            {
                appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.TxnStart, storeVersion = basicContext.Session.Version, sessionID = basicContext.Session.ID }, out _);
            }

            state = TxnState.Running;
            return true;
        }
    }
}