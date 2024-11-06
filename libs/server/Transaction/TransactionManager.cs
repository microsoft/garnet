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
    using BasicGarnetApi = GarnetApi<TransientKeyLocker, GarnetSafeEpochGuard>;
    using LockableGarnetApi = GarnetApi<TransactionalKeyLocker, GarnetSafeEpochGuard>;
    using PrepareGarnetApi = GarnetWatchApi<TransientKeyLocker, GarnetSafeEpochGuard, /* BasicGarnetApi */ GarnetApi<TransientKeyLocker, GarnetSafeEpochGuard>>;

    /// <summary>
    /// Transaction manager
    /// </summary>
    public sealed unsafe partial class TransactionManager
    {
        // Not readonly to avoid defensive copy
        PrepareGarnetApi garnetTxPrepareApi;

        // Not readonly to avoid defensive copy
        LockableGarnetApi garnetTxMainApi;

        // Not readonly to avoid defensive copy
        BasicGarnetApi garnetTxFinalizeApi;

        // Cluster session
        readonly IClusterSession clusterSession;

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
        private const int InitialSliceBufferSize = 1 << 10;
        private const int InitialKeyBufferSize = 1 << 10;
        StoreType transactionStoreType;
        readonly ILogger logger;

        /// <summary>
        /// Array to keep pointer keys in keyBuffer
        /// </summary>
        private readonly TxnKeyEntries keyEntries;

        internal TransactionManager(
            TsavoriteKernel kernel,
            RespServerSession respSession,
            StorageSession storageSession,
            ScratchBufferManager scratchBufferManager,
            bool clusterEnabled,
            ILogger logger = null)
        {
            functionsState = storageSession.functionsState;
            appendOnlyFile = functionsState.appendOnlyFile;
            this.logger = logger;

            this.respSession = respSession;
            clusterSession = respSession.clusterSession;

            watchContainer = new WatchedKeysContainer(InitialSliceBufferSize, functionsState.watchVersionMap);
            keyEntries = new TxnKeyEntries(kernel, InitialSliceBufferSize);
            this.scratchBufferManager = scratchBufferManager;

            garnetTxMainApi = respSession.lockableGarnetApi;
            garnetTxPrepareApi = new GarnetWatchApi<TransientKeyLocker, GarnetSafeEpochGuard, BasicGarnetApi>(respSession.basicGarnetApi);
            garnetTxFinalizeApi = respSession.basicGarnetApi;

            this.clusterEnabled = clusterEnabled;
            if (clusterEnabled)
                clusterKeys = new ArgSlice[InitialKeyBufferSize];

            Reset(false);
        }

        internal void Reset(bool isRunning)
        {
            if (isRunning)
            {
                var acquiredEpoch = respSession.storageSession.EnsureBeginUnsafe();
                try
                {
                    keyEntries.UnlockAllKeys(respSession.storageSession);
                    respSession.storageSession.EndTransaction();
                }
                finally
                {
                    if (acquiredEpoch)
                        respSession.storageSession.EndUnsafe();
                }
            }
            txnStartHead = 0;
            operationCntTxn = 0;
            state = TxnState.None;
            transactionStoreType = 0;
            functionsState.StoredProcMode = false;

            // Reset cluster variables used for slot verification
            saveKeyRecvBufferPtr = null;
            clusterKeyCount = 0;
        }

        internal bool RunTransactionProc(byte id, ArgSlice input, CustomTransactionProcedure proc, ref MemoryResult<byte> output)
        {
            var running = false;
            scratchBufferManager.Reset();
            try
            {
                functionsState.StoredProcMode = true;
                // Prepare phase
                if (!proc.Prepare<TransientKeyLocker, GarnetSafeEpochGuard, PrepareGarnetApi>(garnetTxPrepareApi, input))
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
                proc.Main<TransactionalKeyLocker, GarnetSafeEpochGuard, LockableGarnetApi>(garnetTxMainApi, input, ref output);

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
                    proc.Finalize<TransientKeyLocker, GarnetSafeEpochGuard, BasicGarnetApi>(garnetTxFinalizeApi, input, ref output);
                }
                catch { }

                // Reset scratch buffer for next txn invocation
                scratchBufferManager.Reset();
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsSkippingOperations() => state is TxnState.Started or TxnState.Aborted;

        internal void Abort() => state = TxnState.Aborted;

        internal void Log(byte id, ArgSlice input)
        {
            Debug.Assert(functionsState.StoredProcMode);
            var sb = new SpanByte(input.Length, (nint)input.ptr);
            appendOnlyFile?.Enqueue(new AofHeader { opType = AofEntryType.StoredProcedure, type = id, version = respSession.storageSession.SessionVersion, sessionID = respSession.storageSession.SessionID }, ref sb, out _);
        }

        internal void Commit(bool internal_txn = false)
        {
            if (appendOnlyFile != null && !functionsState.StoredProcMode)
            {
                appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.TxnCommit, version = respSession.storageSession.SessionVersion, sessionID = respSession.storageSession.SessionID }, out _);
            }
            if (!internal_txn)
                watchContainer.Reset();
            Reset(true);
        }

        internal void Watch<TKeyLocker, TEpochGuard>(ArgSlice key, StoreType type)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            // Update watch type if object store is disabled
            if (type == StoreType.All && !respSession.storageSession.IsDual)
                type = StoreType.Main;

            UpdateTransactionStoreType(type);
            watchContainer.AddWatch(key, type);

            _ = respSession.storageSession.Watch<TKeyLocker, TEpochGuard>(key, type);
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
            _ = watchContainer.SaveKeysToKeyList(this);
            keys = clusterKeys;
            keyCount = clusterKeyCount;
            readOnly = keyEntries.IsReadOnly;
        }

        internal bool Run(bool internal_txn = false, bool fail_fast_on_lock = false, TimeSpan lock_timeout = default)
        {
            // Save watch keys to lock list
            if (!internal_txn)
                _ = watchContainer.SaveKeysToLock(this);

            respSession.storageSession.BeginUnsafe();
            try
            {
                respSession.storageSession.BeginTransaction();

                var lockSuccess = true;
                if (fail_fast_on_lock)
                { 
                    lockSuccess = keyEntries.TryLockAllKeys(respSession.storageSession, lock_timeout);
                    if (!lockSuccess)
                        logger?.LogError("Transaction failed to acquire all the locks on keys to proceed.");
                }
                else
                    keyEntries.LockAllKeys(respSession.storageSession);

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
                respSession.storageSession.EndUnsafe();
            }

            if (appendOnlyFile != null && !functionsState.StoredProcMode)
            {
                appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.TxnStart, version = respSession.storageSession.SessionVersion, sessionID = respSession.storageSession.SessionID }, out _);
            }

            state = TxnState.Running;
            return true;
        }
    }
}