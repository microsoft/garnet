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
    using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;
    using LockableGarnetApi = GarnetApi<LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;

    /// <summary>
    /// Transaction manager
    /// </summary>
    public sealed unsafe partial class TransactionManager
    {
        /// <summary>
        /// Basic context for main store
        /// </summary>
        readonly BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> basicContext;

        /// <summary>
        /// Lockable context for main store
        /// </summary>
        readonly LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> lockableContext;

        /// <summary>
        /// Basic context for object store
        /// </summary>
        readonly BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreBasicContext;

        /// <summary>
        /// Lockable context for object store
        /// </summary>
        readonly LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreLockableContext;

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

        internal LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> LockableContext
            => lockableContext;
        internal LockableUnsafeContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> LockableUnsafeContext
            => basicContext.Session.LockableUnsafeContext;
        internal LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> ObjectStoreLockableContext
            => objectStoreLockableContext;

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
            this.clusterSession = respSession.clusterSession;

            watchContainer = new WatchedKeysContainer(initialSliceBufferSize, functionsState.watchVersionMap);
            keyEntries = new TxnKeyEntries(initialSliceBufferSize, lockableContext, objectStoreLockableContext);
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

        internal bool Run(bool internal_txn = false, bool fail_fast_on_lock = false, TimeSpan lock_timeout = default)
        {
            // Save watch keys to lock list
            if (!internal_txn)
                watchContainer.SaveKeysToLock(this);

            // Acquire lock sessions
            if (transactionStoreType == StoreType.All || transactionStoreType == StoreType.Main)
            {
                lockableContext.BeginLockable();
            }
            if (transactionStoreType == StoreType.All || transactionStoreType == StoreType.Object)
            {
                if (objectStoreBasicContext.IsNull)
                    throw new Exception("Trying to perform object store transaction with object store disabled");
                objectStoreLockableContext.BeginLockable();
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
                appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.TxnStart, version = basicContext.Session.Version, sessionID = basicContext.Session.ID }, out _);
            }

            state = TxnState.Running;
            return true;
        }
    }
}