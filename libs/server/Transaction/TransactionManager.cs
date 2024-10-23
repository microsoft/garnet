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
    using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
        BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>>;
    using LockableGarnetApi = GarnetApi<LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
        LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>>;
    using DualGarnetApi = GarnetApi<DualItemContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
        DualItemContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
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
        /// <summary>
        /// Basic context for main store
        /// </summary>
        readonly BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext;

        /// <summary>
        /// Lockable context for main store
        /// </summary>
        readonly LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> lockableContext;

        /// <summary>
        /// Dual context for main store
        /// </summary>
        readonly DualItemContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> dualContext;

        /// <summary>
        /// Basic context for object store
        /// </summary>
        readonly BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreBasicContext;

        /// <summary>
        /// Lockable context for object store
        /// </summary>
        readonly LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreLockableContext;

        /// <summary>
        /// Dual context for object store
        /// </summary>
        readonly DualItemContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreDualContext;

        // Not readonly to avoid defensive copy
        GarnetWatchApi<BasicGarnetApi> garnetTxPrepareApi;

        // Cluster session
        IClusterSession clusterSession;

        // Not readonly to avoid defensive copy
        LockableGarnetApi garnetTxMainApi;

        // Not readonly to avoid defensive copy
        BasicGarnetApi garnetTxFinalizeApi;

        private readonly RespServerSession respSession;
        readonly FunctionsState functionsState;
        internal readonly ScratchBufferManager scratchBufferManager;
        private readonly TsavoriteLog appendOnlyFile;
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

        internal LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> LockableContext
            => lockableContext;
        internal LockableUnsafeContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> LockableUnsafeContext
            => basicContext.Session.LockableUnsafeContext;
        internal LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> ObjectStoreLockableContext
            => objectStoreLockableContext;

        /// <summary>
        /// Array to keep pointer keys in keyBuffer
        /// </summary>
        private TxnKeyEntries keyEntries;

        internal TsavoriteKernel TsavoriteKernel => keyEntries.kernel;

        internal TransactionManager(
            TsavoriteKernel kernel,
            RespServerSession respSession,
            StorageSession storageSession,
            ScratchBufferManager scratchBufferManager,
            bool clusterEnabled,
            ILogger logger = null)
        {
            var session = storageSession.basicContext.Session;
            basicContext = session.BasicContext;
            lockableContext = session.LockableContext;
            dualContext = session.DualContext;

            var objectStoreSession = storageSession.objectStoreBasicContext.Session;
            if (objectStoreSession != null)
            {
                objectStoreBasicContext = objectStoreSession.BasicContext;
                objectStoreLockableContext = objectStoreSession.LockableContext;
                objectStoreDualContext = objectStoreSession.DualContext;
            }

            this.functionsState = storageSession.functionsState;
            this.appendOnlyFile = functionsState.appendOnlyFile;
            this.logger = logger;

            this.respSession = respSession;
            this.clusterSession = respSession.clusterSession;

            watchContainer = new WatchedKeysContainer(initialSliceBufferSize, functionsState.watchVersionMap);
            keyEntries = new TxnKeyEntries(kernel, initialSliceBufferSize);
            this.scratchBufferManager = scratchBufferManager;

            garnetTxMainApi = respSession.lockableGarnetApi;
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
                var acquiredEpoch = respSession.KernelSession.EnsureBeginUnsafe();
                try
                {
                    keyEntries.UnlockAllKeys(ref respSession.KernelSession);
                    respSession.KernelSession.EndTransaction();
                }
                finally
                {
                    if (acquiredEpoch)
                        respSession.KernelSession.EndUnsafe();
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

        internal bool RunTransactionProc(byte id, ArgSlice input, CustomTransactionProcedure proc, ref MemoryResult<byte> output)
        {
            bool running = false;
            scratchBufferManager.Reset();
            try
            {
                functionsState.StoredProcMode = true;
                // Prepare phase
                if (!proc.Prepare(garnetTxPrepareApi, input))
                {
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
                proc.Main(garnetTxMainApi, input, ref output);

                // Log the transaction to AOF
                Log(id, input);

                // Commit
                Commit();
            }
            catch
            {
                Reset(running);
                return false;
            }
            finally
            {
                try
                {
                    // Run finalize procedure at the end
                    proc.Finalize(garnetTxFinalizeApi, input, ref output);
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

        internal void Log(byte id, ArgSlice input)
        {
            Debug.Assert(functionsState.StoredProcMode);
            SpanByte sb = new SpanByte(input.Length, (nint)input.ptr);
            appendOnlyFile?.Enqueue(new AofHeader { opType = AofEntryType.StoredProcedure, type = id, version = basicContext.Session.Version, sessionID = basicContext.Session.ID }, ref sb, out _);
        }

        internal void Commit(bool internal_txn = false)
        {
            if (appendOnlyFile != null && !functionsState.StoredProcMode)
            {
                appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.TxnCommit, version = basicContext.Session.Version, sessionID = basicContext.Session.ID }, out _);
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

            _ = respSession.storageSession.Watch(key, type, in dualContext, in objectStoreDualContext);
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

            respSession.KernelSession.BeginUnsafe();
            try
            {
                respSession.KernelSession.BeginTransaction();

                bool lockSuccess = true;
                if (fail_fast_on_lock)
                { 
                    lockSuccess = keyEntries.TryLockAllKeys(ref respSession.KernelSession, lock_timeout);
                    if (!lockSuccess)
                        this.logger?.LogError("Transaction failed to acquire all the locks on keys to proceed.");
                }
                else
                    keyEntries.LockAllKeys(ref respSession.KernelSession);

                if (!lockSuccess || (!internal_txn && !watchContainer.ValidateWatchVersion()))
                {
                    Reset(true);
                    if (!internal_txn)
                        watchContainer.Reset();
                    return false;
                }
            }
            finally
            {
                respSession.KernelSession.EndUnsafe();
            }

            if (appendOnlyFile != null && !functionsState.StoredProcMode)
            {
                appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.TxnStart, version = basicContext.Session.Version, sessionID = basicContext.Session.ID }, out _);
            }

            state = TxnState.Running;
            return true;
        }
    }
}