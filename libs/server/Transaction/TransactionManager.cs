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
    using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
        BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>>;
    using LockableGarnetApi = GarnetApi<LockableContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
        LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>>;

    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    /// <summary>
    /// Transaction manager
    /// </summary>
    public sealed unsafe partial class TransactionManager
    {
        internal bool AofEnabled => appendOnlyFile != null;

        /// <summary>
        /// Basic context for main store
        /// </summary>
        readonly BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext;

        /// <summary>
        /// Lockable context for main store
        /// </summary>
        readonly LockableContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> lockableContext;

        /// <summary>
        /// Basic context for object store
        /// </summary>
        readonly BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreBasicContext;

        /// <summary>
        /// Lockable context for object store
        /// </summary>
        readonly LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreLockableContext;

        // Not readonly to avoid defensive copy
        GarnetWatchApi<BasicGarnetApi> garnetTxPrepareApi;

        // Not readonly to avoid defensive copy
        LockableGarnetApi garnetTxMainApi;

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
        StoreType transactionStoreType;
        readonly ILogger logger;
        long txnVersion;

        internal LockableContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> LockableContext
            => lockableContext;
        internal LockableUnsafeContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> LockableUnsafeContext
            => basicContext.Session.LockableUnsafeContext;
        internal LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> ObjectStoreLockableContext
            => objectStoreLockableContext;

        /// <summary>
        /// Array to keep pointer keys in keyBuffer
        /// </summary>
        private TxnKeyEntries keyEntries;

        internal TransactionManager(
            StoreWrapper storeWrapper,
            RespServerSession respSession,
            BasicGarnetApi garnetApi,
            LockableGarnetApi lockableGarnetApi,
            StorageSession storageSession,
            ScratchBufferAllocator scratchBufferAllocator,
            bool clusterEnabled,
            ILogger logger = null,
            int dbId = 0)
        {
            var session = storageSession.basicContext.Session;
            basicContext = session.BasicContext;
            lockableContext = session.LockableContext;

            var objectStoreSession = storageSession.objectStoreBasicContext.Session;
            if (objectStoreSession != null)
            {
                objectStoreBasicContext = objectStoreSession.BasicContext;
                objectStoreLockableContext = objectStoreSession.LockableContext;
            }

            this.functionsState = storageSession.functionsState;
            this.appendOnlyFile = functionsState.appendOnlyFile;
            this.logger = logger;

            this.respSession = respSession;

            watchContainer = new WatchedKeysContainer(initialSliceBufferSize, functionsState.watchVersionMap);
            keyEntries = new TxnKeyEntries(initialSliceBufferSize, lockableContext, objectStoreLockableContext);
            this.scratchBufferAllocator = scratchBufferAllocator;

            var dbFound = storeWrapper.TryGetDatabase(dbId, out var db);
            Debug.Assert(dbFound);
            this.stateMachineDriver = db.StateMachineDriver;

            garnetTxMainApi = lockableGarnetApi;
            garnetTxPrepareApi = new GarnetWatchApi<BasicGarnetApi>(garnetApi);
            garnetTxFinalizeApi = garnetApi;

            this.clusterEnabled = clusterEnabled;
            if (clusterEnabled)
                keys = new ArgSlice[initialKeyBufferSize];

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
                        lockableContext.EndLockable();
                    if (transactionStoreType == StoreType.Object || transactionStoreType == StoreType.All)
                    {
                        if (objectStoreBasicContext.IsNull)
                            throw new Exception("Trying to perform object store transaction with object store disabled");
                        objectStoreLockableContext.EndLockable();
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
                    new AofHeader { opType = AofEntryType.StoredProcedure, procedureId = id, storeVersion = txnVersion, sessionID = basicContext.Session.ID },
                    ref procInput,
                    out _);
            }
        }

        internal void Commit(bool internal_txn = false)
        {
            if (PerformWrites && appendOnlyFile != null && !functionsState.StoredProcMode)
            {
                appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.TxnCommit, storeVersion = txnVersion, sessionID = basicContext.Session.ID }, out _);
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
                objectStoreBasicContext.ResetModified(key.ToArray());
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

        void BeginLockable(StoreType transactionStoreType)
        {
            if (transactionStoreType is StoreType.All or StoreType.Main)
            {
                lockableContext.BeginLockable();
            }
            if (transactionStoreType is StoreType.All or StoreType.Object)
            {
                if (objectStoreBasicContext.IsNull)
                    throw new Exception("Trying to perform object store transaction with object store disabled");
                objectStoreLockableContext.BeginLockable();
            }
        }

        void LocksAcquired(StoreType transactionStoreType, long txnVersion)
        {
            if (transactionStoreType is StoreType.All or StoreType.Main)
            {
                lockableContext.LocksAcquired(txnVersion);
            }
            if (transactionStoreType is StoreType.All or StoreType.Object)
            {
                if (objectStoreBasicContext.IsNull)
                    throw new Exception("Trying to perform object store transaction with object store disabled");
                objectStoreLockableContext.LocksAcquired(txnVersion);
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
            BeginLockable(transactionStoreType);

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

            // Do not write to AOF if no write operations
            if (PerformWrites && appendOnlyFile != null && !functionsState.StoredProcMode)
            {
                appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.TxnStart, storeVersion = txnVersion, sessionID = basicContext.Session.ID }, out _);
            }

            state = TxnState.Running;
            return true;
        }
    }
}