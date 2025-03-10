﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
        BasicContext<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>,
            ObjectAllocator<IGarnetObject, StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>>>>;
    using TransactionalGarnetApi = GarnetApi<TransactionalContext<SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
        TransactionalContext<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>,
            ObjectAllocator<IGarnetObject, StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>>>>;

    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = ObjectAllocator<IGarnetObject, StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>;

    /// <summary>
    /// Transaction manager
    /// </summary>
    public sealed unsafe partial class TransactionManager
    {
        /// <summary>
        /// Basic context for main store
        /// </summary>
        readonly BasicContext<SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext;

        /// <summary>
        /// Transactional context for main store
        /// </summary>
        readonly TransactionalContext<SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> transactionalContext;

        /// <summary>
        /// Basic context for object store
        /// </summary>
        readonly BasicContext<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreBasicContext;

        /// <summary>
        /// Transactional context for object store
        /// </summary>
        readonly TransactionalContext<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreTransactionalContext;

        // Not readonly to avoid defensive copy
        GarnetWatchApi<BasicGarnetApi> garnetTxPrepareApi;

        // Not readonly to avoid defensive copy
        TransactionalGarnetApi garnetTxMainApi;

        // Not readonly to avoid defensive copy
        BasicGarnetApi garnetTxFinalizeApi;

        private readonly RespServerSession respSession;
        readonly FunctionsState functionsState;
        internal readonly ScratchBufferManager scratchBufferManager;
        private readonly TsavoriteAof appendOnlyFile;
        internal readonly WatchedKeysContainer watchContainer;
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

        internal TransactionalContext<SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> TransactionalContext
            => transactionalContext;
        internal TransactionalUnsafeContext<SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> TransactionalUnsafeContext
            => basicContext.Session.TransactionalUnsafeContext;
        internal TransactionalContext<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> ObjectStoreTransactionalContext
            => objectStoreTransactionalContext;

        /// <summary>
        /// Array to keep pointer keys in keyBuffer
        /// </summary>
        private TxnKeyEntries keyEntries;

        internal TransactionManager(
            RespServerSession respSession,
            StorageSession storageSession,
            ScratchBufferManager scratchBufferManager,
            bool clusterEnabled,
            ILogger logger = null)
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

            garnetTxMainApi = respSession.transactionalGarnetApi;
            garnetTxPrepareApi = new GarnetWatchApi<BasicGarnetApi>(respSession.basicGarnetApi);
            garnetTxFinalizeApi = respSession.basicGarnetApi;

            this.clusterEnabled = clusterEnabled;
            if (clusterEnabled)
                keys = new ArgSlice[initialKeyBufferSize];

            Reset(false);
        }

        internal void Reset(bool isRunning)
        {
            if (isRunning)
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

        internal void Watch(ArgSlice key, StoreType type)
        {
            // Update watch type if object store is disabled
            if (type == StoreType.All && objectStoreBasicContext.IsNull)
                type = StoreType.Main;

            UpdateTransactionStoreType(type);
            watchContainer.AddWatch(key, type);

            if (type == StoreType.Main || type == StoreType.All)
                basicContext.ResetModified(key.SpanByte);
            if ((type == StoreType.Object || type == StoreType.All) && !objectStoreBasicContext.IsNull)
                objectStoreBasicContext.ResetModified(key.SpanByte);
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

        internal void GetKeysForValidation(byte* recvBufferPtr, out ArgSlice[] keys, out int keyCount, out bool readOnly)
        {
            UpdateRecvBufferPtr(recvBufferPtr);
            watchContainer.SaveKeysToKeyList(this);
            keys = this.keys;
            keyCount = this.keyCount;
            readOnly = keyEntries.IsReadOnly;
        }

        internal bool Run(bool internal_txn = false, bool fail_fast_on_lock = false, TimeSpan lock_timeout = default)
        {
            // Save watch keys to lock list
            if (!internal_txn)
                watchContainer.SaveKeysToLock(this);

            // Acquire lock sessions
            if (transactionStoreType == StoreType.All || transactionStoreType == StoreType.Main)
            {
                transactionalContext.BeginTransaction();
            }
            if (transactionStoreType == StoreType.All || transactionStoreType == StoreType.Object)
            {
                if (objectStoreBasicContext.IsNull)
                    throw new Exception("Trying to perform object store transaction with object store disabled");
                objectStoreTransactionalContext.BeginTransaction();
            }

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

            if (appendOnlyFile != null && !functionsState.StoredProcMode)
            {
                appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.TxnStart, storeVersion = basicContext.Session.Version, sessionID = basicContext.Session.ID }, out _);
            }

            state = TxnState.Running;
            return true;
        }
    }
}