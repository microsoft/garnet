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
        /// Basic context for main store
        /// </summary>
        readonly BasicContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> basicContext;

        /// <summary>
        /// Transactional context for main store
        /// </summary>
        readonly TransactionalContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> transactionalContext;

        /// <summary>
        /// Basic context for object store
        /// </summary>
        readonly BasicContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectStoreBasicContext;

        /// <summary>
        /// Transactional context for object store
        /// </summary>
        readonly TransactionalContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectStoreTransactionalContext;

        /// <summary>
        /// Basic context for unified store
        /// </summary>
        readonly BasicContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedStoreBasicContext;

        /// <summary>
        /// Transactional context for unified store
        /// </summary>
        readonly TransactionalContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedStoreTransactionalContext;

        // Not readonly to avoid defensive copy
        GarnetWatchApi<BasicGarnetApi> garnetTxPrepareApi;

        // Not readonly to avoid defensive copy
        TransactionalGarnetApi garnetTxRunApi;

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
        private readonly GarnetAppendOnlyFile appendOnlyFile;
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

        internal TransactionalContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> TransactionalContext
            => transactionalContext;
        internal TransactionalUnsafeContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> TransactionalUnsafeContext
            => basicContext.Session.TransactionalUnsafeContext;
        internal TransactionalContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> ObjectStoreTransactionalContext
            => objectStoreTransactionalContext;
        internal TransactionalContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> UnifiedStoreTransactionalContext
            => unifiedStoreTransactionalContext;

        /// <summary>
        /// Array to keep pointer keys in keyBuffer
        /// </summary>
        private TxnKeyEntries keyEntries;

        internal TransactionManager(StoreWrapper storeWrapper,
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
            var session = storageSession.basicContext.Session;
            basicContext = session.BasicContext;
            transactionalContext = session.TransactionalContext;

            var objectStoreSession = storageSession.objectStoreBasicContext.Session;
            objectStoreBasicContext = objectStoreSession.BasicContext;
            objectStoreTransactionalContext = objectStoreSession.TransactionalContext;

            var unifiedStoreSession = storageSession.unifiedStoreBasicContext.Session;
            unifiedStoreBasicContext = unifiedStoreSession.BasicContext;
            unifiedStoreTransactionalContext = unifiedStoreSession.TransactionalContext;

            this.functionsState = storageSession.functionsState;
            this.appendOnlyFile = functionsState.appendOnlyFile;
            this.logger = logger;

            this.respSession = respSession;

            watchContainer = new WatchedKeysContainer(initialSliceBufferSize, functionsState.watchVersionMap);
            keyEntries = new TxnKeyEntries(initialSliceBufferSize, transactionalContext, objectStoreTransactionalContext, unifiedStoreTransactionalContext);
            this.scratchBufferAllocator = scratchBufferAllocator;

            var dbFound = storeWrapper.TryGetDatabase(dbId, out var db);
            Debug.Assert(dbFound);
            this.stateMachineDriver = db.StateMachineDriver;

            garnetTxRunApi = transactionalGarnetApi;
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
                        transactionalContext.EndTransaction();
                    if ((storeTypes & TransactionStoreTypes.Object) == TransactionStoreTypes.Object)
                        objectStoreTransactionalContext.EndTransaction();
                    unifiedStoreTransactionalContext.EndTransaction();
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
                    isRecovering);
            }
            else
            {
                return RunTransactionProcInternal(
                    ref garnetTxPrepareApi,
                    ref garnetTxRunApi,
                    ref garnetTxFinalizeApi,
                    id,
                    ref procInput,
                    proc,
                    ref output,
                    isRecovering
                    );
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
            bool isRecovering = false)
            where TPrepareApi : IGarnetReadApi
            where TRunApi : IGarnetApi
            where TFinalizeApi : IGarnetApi
        {
            var running = false;
            scratchBufferAllocator.Reset();
            try
            {
                // If cluster is enabled reset slot verification state cache
                ResetCacheSlotVerificationResult();

                // Reset logAccess for sharded log
                proc.sublogAccessVector = 0UL;

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
                Log(id, ref procInput, proc.sublogAccessVector, proc.replayTaskAccessVector);

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

        void Log(byte id, ref CustomProcedureInput procInput, ulong sublogAccessVector, BitVector[] replayTaskAccessVector)
        {
            Debug.Assert(functionsState.StoredProcMode);

            if (PerformWrites && appendOnlyFile != null)
            {
                if (!appendOnlyFile.serverOptions.MultiLogEnabled)
                {
                    var header = new AofHeader
                    {
                        opType = AofEntryType.StoredProcedure,
                        procedureId = id,
                        storeVersion = txnVersion,
                        sessionID = basicContext.Session.ID,
                    };
                    appendOnlyFile.Log.GetSubLog(0).Enqueue(header, ref procInput, out _);
                }
                else
                {
                    try
                    {
                        appendOnlyFile.Log.LockSublogs(sublogAccessVector);
                        var _sublogAccessVector = sublogAccessVector;
                        var header = new AofTransactionHeader
                        {
                            shardedHeader = new AofShardedHeader
                            {
                                basicHeader = new AofHeader
                                {
                                    padding = (byte)AofHeaderType.TransactionHeader,
                                    opType = AofEntryType.StoredProcedure,
                                    procedureId = id,
                                    storeVersion = txnVersion,
                                    sessionID = basicContext.Session.ID,
                                },
                                sequenceNumber = functionsState.appendOnlyFile.seqNumGen.GetSequenceNumber(),
                            },
                            participantCount = (short)replayTaskAccessVector.Select(bitVector => bitVector.PopCount()).Sum()
                        };

                        while (_sublogAccessVector > 0)
                        {
                            var sublogIdx = _sublogAccessVector.GetNextOffset();
                            // Update corresponding sublog participating vector before enqueue to related physical sublog
                            replayTaskAccessVector[sublogIdx].CopyTo(new Span<byte>(header.replayTaskAccessVector, AofTransactionHeader.ReplayTaskAccessVectorSize));
                            appendOnlyFile.Log.GetSubLog(sublogIdx).Enqueue(header, ref procInput, out _);
                        }
                    }
                    finally
                    {
                        appendOnlyFile.Log.UnlockSublogs(sublogAccessVector);
                    }
                }
            }
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
                if (!appendOnlyFile.serverOptions.MultiLogEnabled)
                {
                    var header = new AofHeader
                    {
                        opType = AofEntryType.TxnCommit,
                        storeVersion = txnVersion,
                        txnID = basicContext.Session.ID,
                    };
                    appendOnlyFile.Log.GetSubLog(0).Enqueue(header, out _);
                }
                else
                {
                    ComputeSublogAccessVector(out var sublogAccessVector, out var replayTaskAccessVector);

                    try
                    {
                        appendOnlyFile.Log.LockSublogs(sublogAccessVector);
                        var _logAccessBitmap = sublogAccessVector;
                        var header = new AofTransactionHeader
                        {
                            shardedHeader = new AofShardedHeader
                            {
                                basicHeader = new AofHeader
                                {
                                    padding = (byte)AofHeaderType.TransactionHeader,
                                    opType = AofEntryType.TxnCommit,
                                    storeVersion = txnVersion,
                                    txnID = basicContext.Session.ID,
                                },
                                sequenceNumber = functionsState.appendOnlyFile.seqNumGen.GetSequenceNumber()
                            },
                            participantCount = (short)replayTaskAccessVector.Select(bitVector => bitVector.PopCount()).Sum()
                        };

                        while (_logAccessBitmap > 0)
                        {
                            var sublogIdx = _logAccessBitmap.GetNextOffset();
                            // Update corresponding sublog participating vector before enqueue to related physical sublog
                            replayTaskAccessVector[sublogIdx].CopyTo(new Span<byte>(header.replayTaskAccessVector, AofTransactionHeader.ReplayTaskAccessVectorSize));
                            appendOnlyFile.Log.GetSubLog(sublogIdx).Enqueue(header, out _);
                        }
                    }
                    finally
                    {
                        appendOnlyFile.Log.UnlockSublogs(sublogAccessVector);
                    }
                }
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
                transactionalContext.ResetModified(key.ReadOnlySpan);
            if ((storeTypes & TransactionStoreTypes.Object) == TransactionStoreTypes.Object)
                objectStoreTransactionalContext.ResetModified(key.ReadOnlySpan);
            unifiedStoreTransactionalContext.ResetModified(key.ReadOnlySpan);
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
                transactionalContext.BeginTransaction();
            if ((storeTypes & TransactionStoreTypes.Object) == TransactionStoreTypes.Object)
                objectStoreTransactionalContext.BeginTransaction();
            unifiedStoreTransactionalContext.BeginTransaction();
        }

        void LocksAcquired(long txnVersion)
        {
            if ((storeTypes & TransactionStoreTypes.Main) == TransactionStoreTypes.Main)
                transactionalContext.LocksAcquired(txnVersion);
            if ((storeTypes & TransactionStoreTypes.Object) == TransactionStoreTypes.Object)
                objectStoreTransactionalContext.LocksAcquired(txnVersion);
            unifiedStoreTransactionalContext.LocksAcquired(txnVersion);
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
                if (!appendOnlyFile.serverOptions.MultiLogEnabled)
                {
                    var header = new AofHeader
                    {
                        opType = AofEntryType.TxnStart,
                        storeVersion = txnVersion,
                        txnID = basicContext.Session.ID
                    };
                    appendOnlyFile.Log.GetSubLog(0).Enqueue(header, out _);
                }
                else
                {
                    ComputeSublogAccessVector(out var sublogAccessVector, out var replayTaskAccessVector);

                    try
                    {
                        appendOnlyFile.Log.LockSublogs(sublogAccessVector);
                        var _sublogAccessVector = sublogAccessVector;
                        var header = new AofTransactionHeader
                        {
                            shardedHeader = new AofShardedHeader
                            {
                                basicHeader = new AofHeader
                                {
                                    padding = (byte)AofHeaderType.TransactionHeader,
                                    opType = AofEntryType.TxnStart,
                                    storeVersion = txnVersion,
                                    txnID = basicContext.Session.ID
                                },
                                sequenceNumber = functionsState.appendOnlyFile.seqNumGen.GetSequenceNumber()
                            },
                            participantCount = (short)replayTaskAccessVector.Select(bitVector => bitVector.PopCount()).Sum()
                        };

                        while (_sublogAccessVector > 0)
                        {
                            var sublogIdx = _sublogAccessVector.GetNextOffset();
                            replayTaskAccessVector[sublogIdx].CopyTo(new Span<byte>(header.replayTaskAccessVector, AofTransactionHeader.ReplayTaskAccessVectorSize));
                            appendOnlyFile.Log.GetSubLog(sublogIdx).Enqueue(header, out _);
                        }
                    }
                    finally
                    {
                        appendOnlyFile.Log.UnlockSublogs(sublogAccessVector);
                    }
                }
            }

            state = TxnState.Running;
            return true;
        }

        public void IterativeShardedLogAccess(PinnedSpanByte key, ref ulong sublogAccessVector, CustomTransactionProcedure proc, out BitVector[] replayTaskAccessVector)
        {
            replayTaskAccessVector = null;

            // Skip if AOF is disabled
            if (appendOnlyFile == null)
                return;

            // Skip if singleLog
            if (appendOnlyFile.Log.Size == 1)
                return;

            replayTaskAccessVector = [.. Enumerable.Range(0, appendOnlyFile.Log.Size).Select(_ => new BitVector(AofTransactionHeader.ReplayTaskAccessVectorSize))];
            var hash = GarnetLog.HASH(key);
            if (proc.customProcTimestampBitmap == null)
            {
                var sublogIdx = (int)(hash % appendOnlyFile.Log.Size);
                var replayIdx = (int)(hash % appendOnlyFile.Log.ReplayTaskCount);
                // Mark sublog participating in custom txn proc to help with replay coordination
                sublogAccessVector |= 1UL << sublogIdx;
                replayTaskAccessVector[sublogIdx].SetBit(replayIdx);
            }
            else
                // Keep track of key hashes to update sequence numbers of keys at end of replay
                proc.customProcTimestampBitmap.AddHash(hash);
        }

        void ComputeSublogAccessVector(out ulong sublogAccessVector, out BitVector[] replayTaskAccessVector)
        {
            sublogAccessVector = 0UL;
            replayTaskAccessVector = [.. Enumerable.Range(0, appendOnlyFile.Log.Size).Select(_ => new BitVector(AofTransactionHeader.ReplayTaskAccessVectorSize))];
            // Skip if AOF is disabled
            if (appendOnlyFile == null)
                return;

            // If singleLog no computation is necessary
            if (appendOnlyFile.Log.Size == 1)
                return;

            // If sharded log is enabled calculate sublog access bitmap
            for (var i = 0; i < keyCount; i++)
            {
                var hash = GarnetLog.HASH(keys[i]);
                var sublogIdx = (int)(hash % appendOnlyFile.Log.Size);
                var replayIdx = (int)(hash % appendOnlyFile.Log.ReplayTaskCount);
                sublogAccessVector |= 1UL << sublogIdx;
                // Calculate sublog access vector for participating replay tasks
                replayTaskAccessVector[sublogIdx].SetBit(replayIdx);
            }
        }
    }
}