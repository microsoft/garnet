// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Used to index barriers for coordinated replay operations in the AofReplayCoordinator
    /// NOTE: Use only negative numbers because sessionIDs will be positive values
    /// </summary>
    internal enum LeaderBarrierType : int
    {
        CHECKPOINT = -1,
        STREAMING_CHECKPOINT = -2,
        FLUSH_DB = -3,
        FLUSH_DB_ALL = -4,
        CUSTOM_STORED_PROC = -5,
    }

    struct BarrierKey : IEquatable<BarrierKey>
    {
        /// <summary>
        /// Session Id
        /// </summary>
        public int SessionId;

        /// <summary>
        /// Transaction Id
        /// </summary>
        public long txnId;

        public bool Equals(BarrierKey other)
            => SessionId == other.SessionId && txnId == other.txnId;

        public override bool Equals(object obj)
            => obj is BarrierKey other && Equals(other);

        public override int GetHashCode()
            => HashCode.Combine(SessionId, txnId);
    }

    public sealed unsafe partial class AofProcessor
    {
        /// <summary>
        /// Coordinates the replay of Append-Only File (AOF) operations, including transaction processing, fuzzy region
        /// handling, and stored procedure execution.
        /// </summary>
        /// <remarks>This class is responsible for managing the replay context, processing transaction
        /// groups, and handling operations within fuzzy regions. It provides methods to add, replay, and process
        /// transactions and operations, ensuring consistency and correctness during AOF replay.  The <see
        /// cref="AofReplayCoordinator"/> is designed to work with an <see cref="AofProcessor"/> to facilitate the
        /// replay of operations.</remarks>
        /// <param name="serverOptions"></param>
        /// <param name="aofProcessor"></param>
        /// <param name="logger"></param>
        public class AofReplayCoordinator(GarnetServerOptions serverOptions, AofProcessor aofProcessor, ILogger logger = null) : IDisposable
        {
            readonly GarnetServerOptions serverOptions = serverOptions;
            readonly ConcurrentDictionary<BarrierKey, LeaderBarrier> leaderBarriers = [];
            readonly AofProcessor aofProcessor = aofProcessor;
            readonly AofReplayContext[] aofReplayContext = InitializeReplayContext(serverOptions.AofVirtualSublogCount, aofProcessor);
            SingleWriterMultiReaderLock disposed = new();
            readonly ILogger logger = logger;

            /// <summary>
            /// Replay context for replay subtask
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <returns></returns>
            internal AofReplayContext GetReplayContext(int sublogIdx) => aofReplayContext[sublogIdx];

            internal static AofReplayContext[] InitializeReplayContext(int AofVirtualSublogCount, AofProcessor aofProcessor)
            {
                var virtualSublogReplayContext = new AofReplayContext[AofVirtualSublogCount];
                for (var i = 0; i < virtualSublogReplayContext.Length; i++)
                    virtualSublogReplayContext[i] = new(aofProcessor.ObtainServerSession());
                return virtualSublogReplayContext;
            }

            /// <summary>
            /// Dispose
            /// </summary>
            public void Dispose()
            {
                if (!disposed.TryWriteLock()) return;
                foreach (var replayContext in aofReplayContext)
                    replayContext.Dispose();
            }

            /// <summary>
            /// Get fuzzy region buffer count
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <returns></returns>
            internal int FuzzyRegionBufferCount(int sublogIdx) => aofReplayContext[sublogIdx].fuzzyRegionOps.Count;

            /// <summary>
            /// Clear fuzzy region buffer
            /// </summary>
            /// <param name="sublogIdx"></param>
            internal void ClearFuzzyRegionBuffer(int sublogIdx) => aofReplayContext[sublogIdx].fuzzyRegionOps.Clear();

            /// <summary>
            /// Add single operation to fuzzy region buffer
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <param name="entry"></param>
            internal unsafe void AddFuzzyRegionOperation(int sublogIdx, ReadOnlySpan<byte> entry) => aofReplayContext[sublogIdx].fuzzyRegionOps.Add(entry.ToArray());

            /// <summary>
            /// This method will perform one of the followin
            ///     1. TxnStart: Create a new transaction group
            ///     2. TxnCommit: Replay or buffer transaction group depending if we are in fuzzyRegion. 
            ///     3. TxnAbort: Clear corresponding sublog replay buffer.
            ///     4. Default: Add an operation to an existing transaction group
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <param name="ptr"></param>
            /// <param name="length"></param>
            /// <param name="asReplica"></param>
            /// <returns>Returns true if a txn operation was processed and added otherwise false</returns>
            /// <exception cref="GarnetException"></exception>
            internal unsafe bool AddOrReplayTransactionOperation(int sublogIdx, byte* ptr, int length, bool asReplica, long entryAddress = 0)
            {
                var header = *(AofHeader*)ptr;
                var replayContext = GetReplayContext(sublogIdx);
                // First try to process this as an existing transaction
                if (aofReplayContext[sublogIdx].activeTxns.TryGetValue(header.sessionID, out var group))
                {
                    switch (header.opType)
                    {
                        case AofEntryType.TxnStart:
                            throw new GarnetException("No nested transactions expected");
                        case AofEntryType.TxnAbort:
                            ClearSessionTxn();
                            UpdateMaxSequenceNumberFromHeader(sublogIdx, ptr, entryAddress);
                            break;
                        case AofEntryType.TxnCommit:
                            if (replayContext.inFuzzyRegion)
                            {
                                // If in fuzzy region we want to record the commit marker and
                                // buffer the transaction group for later replay
                                var commitMarker = new ReadOnlySpan<byte>(ptr, length);
                                aofReplayContext[sublogIdx].AddToFuzzyRegionBuffer(group, commitMarker);
                            }
                            else
                            {
                                // Otherwise process transaction group immediately
                                ProcessTransactionGroup(sublogIdx, ptr, asReplica, group, entryAddress);
                            }

                            // We want to clear and remove in both cases to make space for next txn from session
                            ClearSessionTxn();
                            break;
                        case AofEntryType.StoredProcedure:
                            throw new GarnetException($"Unexpected AOF header operation type {header.opType} within transaction");
                        default:
                            group.Operations.Add(new ReadOnlySpan<byte>(ptr, length).ToArray());
                            break;
                    }

                    void ClearSessionTxn()
                    {
                        aofReplayContext[sublogIdx].activeTxns[header.sessionID].Clear();
                        _ = aofReplayContext[sublogIdx].activeTxns.Remove(header.sessionID);
                    }

                    return true;
                }

                // See if you have detected a txn
                switch (header.opType)
                {
                    case AofEntryType.TxnStart:
                        var headerType = (AofHeaderType)header.HeaderType;
                        short logAccessCount = 0;
                        long startSeqNum = 0;
                        if (serverOptions.MultiLogEnabled)
                        {
                            logAccessCount = headerType == AofHeaderType.SingleLogTransactionHeader
                                ? (*(AofSingleLogTransactionHeader*)ptr).participantCount
                                : (*(AofTransactionHeader*)ptr).participantCount;

                            startSeqNum = headerType == AofHeaderType.SingleLogTransactionHeader
                                ? entryAddress
                                : (*(AofShardedHeader*)ptr).sequenceNumber;
                        }
                        aofReplayContext[sublogIdx].AddTransactionGroup(header.sessionID, sublogIdx, (byte)logAccessCount, startSeqNum);
                        break;
                    case AofEntryType.TxnAbort:
                    case AofEntryType.TxnCommit:
                        // We encountered a transaction end without start - this could happen because we truncated the AOF
                        // after a checkpoint, and the transaction belonged to the previous version. It can safely
                        // be ignored.
                        UpdateMaxSequenceNumberFromHeader(sublogIdx, ptr, entryAddress);
                        break;
                    default:
                        // Continue processing
                        return false;
                }

                // Processed this record succesfully
                return true;
            }

            /// <summary>
            /// Updates the max sequence number for the given sublog from the entry header.
            /// For single-physical-log + multi-replay, uses the entry address; for multi-physical-log, uses embedded sequence number.
            /// </summary>
            void UpdateMaxSequenceNumberFromHeader(int sublogIdx, byte* ptr, long entryAddress)
            {
                var headerType = (AofHeaderType)(*(AofHeader*)ptr).HeaderType;
                long sequenceNumber;
                switch (headerType)
                {
                    case AofHeaderType.BasicHeader:
                    case AofHeaderType.SingleLogTransactionHeader:
                        sequenceNumber = entryAddress;
                        break;
                    case AofHeaderType.ShardedHeader:
                        sequenceNumber = (*(AofShardedHeader*)ptr).sequenceNumber;
                        break;
                    case AofHeaderType.MultiLogTransactionHeader:
                        sequenceNumber = (*(AofTransactionHeader*)ptr).shardedHeader.sequenceNumber;
                        break;
                    default:
                        throw new GarnetException($"Unexpected header type {headerType}");
                }
                aofProcessor.storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(sublogIdx, sequenceNumber);
            }

            /// <summary>
            /// Process fuzzy region operations if any
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <param name="storeVersion"></param>
            /// <param name="asReplica"></param>
            internal void ProcessFuzzyRegionOperations(int sublogIdx, long storeVersion, bool asReplica)
            {
                var fuzzyRegionOps = aofReplayContext[sublogIdx].fuzzyRegionOps;
                if (fuzzyRegionOps.Count > 0)
                    logger?.LogInformation("Replaying sublogIdx: {sublogIdx} - {fuzzyRegionBufferCount} records from fuzzy region for checkpoint {newVersion}", sublogIdx, fuzzyRegionOps.Count, storeVersion);
                var replayContext = GetReplayContext(sublogIdx);
                foreach (var entry in fuzzyRegionOps)
                {
                    fixed (byte* entryPtr = entry)
                    {
                        var header = *(AofHeader*)entryPtr;
                        _ = aofProcessor.ReplayOpDispatch(
                            sublogIdx,
                            header,
                            replayContext,
                            replayContext.StringBasicContext,
                            replayContext.ObjectBasicContext,
                            replayContext.UnifiedBasicContext,
                            entryPtr,
                            entry.Length,
                            asReplica);
                    }
                }
            }

            /// <summary>
            /// Process fuzzy region transaction groups
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <param name="ptr"></param>
            /// <param name="asReplica"></param>
            /// <param name="entryAddress">Log address of the commit entry</param>
            internal void ProcessFuzzyRegionTransactionGroup(int sublogIdx, byte* ptr, bool asReplica, long entryAddress = 0)
            {
                Debug.Assert(aofReplayContext[sublogIdx].txnGroupBuffer != null);
                // Process transaction groups in FIFO order
                var txnGroup = aofReplayContext[sublogIdx].txnGroupBuffer.Dequeue();
                ProcessTransactionGroup(sublogIdx, ptr, asReplica, txnGroup, entryAddress);
            }

            /// <summary>
            /// Process provided transaction group
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <param name="ptr"></param>
            /// <param name="asReplica"></param>
            /// <param name="txnGroup"></param>
            /// <param name="entryAddress">Log address of the commit entry</param>
            internal void ProcessTransactionGroup(int sublogIdx, byte* ptr, bool asReplica, TransactionGroup txnGroup, long entryAddress = 0)
            {
                var replayContext = GetReplayContext(sublogIdx);
                if (!asReplica)
                {
                    // If recovering reads will not expose partial transactions so we can replay without locking.
                    // Also we don't have to synchronize replay of sublogs because write ordering has been established at the time of enqueue.
                    ProcessTransactionGroupOperations(
                        aofProcessor,
                        replayContext.StringBasicContext,
                        replayContext.ObjectBasicContext,
                        replayContext.UnifiedBasicContext,
                        txnGroup,
                        asReplica,
                        entryAddress);
                }
                else
                {
                    var txnManager = replayContext.respServerSession.txnManager;

                    // Start by saving transaction keys for locking
                    SaveTransactionGroupKeysToLock(txnManager, txnGroup);

                    if (serverOptions.MultiLogEnabled)
                    {
                        var headerType = (AofHeaderType)(*(AofHeader*)ptr).HeaderType;
                        long commitSeqNum;
                        short partCount;
                        var sessionId = (*(AofHeader*)ptr).sessionID;

                        if (headerType == AofHeaderType.SingleLogTransactionHeader)
                        {
                            commitSeqNum = entryAddress;
                            partCount = (*(AofSingleLogTransactionHeader*)ptr).participantCount;
                        }
                        else
                        {
                            var shardedHeader = *(AofShardedHeader*)ptr;
                            commitSeqNum = shardedHeader.sequenceNumber;
                            partCount = (*(AofTransactionHeader*)ptr).participantCount;
                        }

                        // Acquire-barrier: synchronize all participants before locking using TxnStart sequence number
                        ProcessSynchronizedOperation(
                            sublogIdx,
                            txnGroup.StartSequenceNumber,
                            partCount,
                            sessionId,
                            null);

                        // Start transaction (acquires locks)
                        _ = txnManager.Run(internal_txn: true);

                        // Process transaction group operations
                        ProcessTransactionGroupOperations(
                            aofProcessor,
                            txnManager.StringTransactionalContext,
                            txnManager.ObjectTransactionalContext,
                            txnManager.UnifiedTransactionalContext,
                            txnGroup,
                            asReplica,
                            entryAddress);

                        // Release-barrier: synchronize all participants before committing using TxnCommit sequence number
                        ProcessSynchronizedOperation(
                            sublogIdx,
                            commitSeqNum,
                            partCount,
                            sessionId,
                            null);
                    }
                    else
                    {
                        // Single-log: no synchronization needed
                        _ = txnManager.Run(internal_txn: true);

                        ProcessTransactionGroupOperations(
                            aofProcessor,
                            txnManager.StringTransactionalContext,
                            txnManager.ObjectTransactionalContext,
                            txnManager.UnifiedTransactionalContext,
                            txnGroup,
                            asReplica,
                            entryAddress);
                    }

                    // Commit (NOTE: need to ensure that we do not write to log here)
                    txnManager.Commit(true);
                }

                // Helper to iterate of transaction keys and add them to lockset
                static void SaveTransactionGroupKeysToLock(TransactionManager txnManager, TransactionGroup txnGroup)
                {
                    foreach (var entry in txnGroup.Operations)
                    {
                        fixed (byte* entryPtr = entry)
                        {
                            var curr = AofHeader.SkipHeader(entryPtr);
                            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
                            txnManager.SaveKeyEntryToLock(key, LockType.Exclusive);
                        }
                    }
                }

                // Process transaction
                static void ProcessTransactionGroupOperations<TStringContext, TObjectContext, TUnifiedContext>(AofProcessor aofProcessor,
                        TStringContext stringContext, TObjectContext objectContext, TUnifiedContext unifiedContext,
                        TransactionGroup txnGroup, bool asReplica, long entryAddress = 0)
                    where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
                    where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
                    where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
                {
                    var replayContext = aofProcessor.aofReplayCoordinator.GetReplayContext(txnGroup.VirtualSublogIdx);
                    foreach (var entry in txnGroup.Operations)
                    {
                        fixed (byte* entryPtr = entry)
                        {
                            var header = *(AofHeader*)entryPtr;
                            _ = aofProcessor.ReplayOpDispatch(
                                txnGroup.VirtualSublogIdx,
                                header,
                                replayContext,
                                stringContext,
                                objectContext,
                                unifiedContext,
                                entryPtr,
                                entry.Length,
                                asReplica: asReplica,
                                entryAddress: entryAddress);
                        }
                    }
                }
            }

            /// <summary>
            /// Replay StoredProc wrapper for single and sharded logs
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <param name="id"></param>
            /// <param name="ptr"></param>
            /// <param name="entryAddress">Log address of the entry, used for single-physical-log mode</param>
            internal void ReplayStoredProc(int sublogIdx, byte id, byte* ptr, long entryAddress = 0)
            {
                if (!serverOptions.MultiLogEnabled)
                {
                    StoredProcRunnerBase(0, id, ptr, shardedLog: false, null);
                }
                else
                {
                    var headerType = (AofHeaderType)(*(AofHeader*)ptr).HeaderType;
                    long sequenceNumber;
                    short participantCount;
                    int sessionId = (*(AofHeader*)ptr).sessionID;

                    if (headerType == AofHeaderType.SingleLogTransactionHeader)
                    {
                        var singleLogHeader = *(AofSingleLogTransactionHeader*)ptr;
                        sequenceNumber = entryAddress;
                        participantCount = singleLogHeader.participantCount;
                    }
                    else
                    {
                        var shardedHeader = *(AofShardedHeader*)ptr;
                        sequenceNumber = shardedHeader.sequenceNumber;
                        participantCount = (*(AofTransactionHeader*)ptr).participantCount;
                    }

                    // Synchronized processing of stored proc operation
                    ProcessSynchronizedOperation(
                        sublogIdx,
                        sequenceNumber,
                        participantCount,
                        sessionId,
                        () => { StoredProcRunnerWrapper(sublogIdx, id, ptr, sequenceNumber); return Task.CompletedTask; }
                    );

                    // Wrapper for store proc runner used for multi-log synchronization
                    void StoredProcRunnerWrapper(int sublogIdx, byte id, byte* ptr, long seqNum)
                    {
                        // Initialize custom proc collection to keep track of hashes for keys for which their timestamp needs to be updated
                        CustomProcedureKeyHashCollection customProcKeyHashTracker = new(aofProcessor.storeWrapper.appendOnlyFile);

                        // Update timestamps for associated keys
                        customProcKeyHashTracker?.UpdateSequenceNumber(seqNum);

                        // Replay StoredProc
                        StoredProcRunnerBase(sublogIdx, id, ptr, shardedLog: true, customProcKeyHashTracker);
                    }
                }

                // Based run stored proc method used of legacy single log implementation
                void StoredProcRunnerBase(int sublogIdx, byte id, byte* entryPtr, bool shardedLog, CustomProcedureKeyHashCollection customProcKeyHashTracker)
                {
                    var curr = AofHeader.SkipHeader(entryPtr);

                    var replayContext = aofReplayContext[sublogIdx];
                    // Reconstructing CustomProcedureInput
                    _ = replayContext.customProcInput.DeserializeFrom(curr);

                    // Run the stored procedure with the reconstructed input
                    var output = replayContext.output;
                    _ = replayContext.respServerSession.RunCustomTxnProcAtReplica(id, ref replayContext.customProcInput, ref output, isRecovering: true, customProcKeyHashTracker);
                }
            }

            /// <summary>
            /// Unified method to process operations that require synchronization across sublogs
            /// </summary>
            /// <param name="sublogIdx">SublogIdx</param>
            /// <param name="sequenceNumber">Sequence number or entry address for ordering</param>
            /// <param name="participantCount">Number of participating replay tasks</param>
            /// <param name="barrierId">Unique barrier ID for this operation type</param>
            /// <param name="operation">The operation to execute</param>
            internal void ProcessSynchronizedOperation(int sublogIdx, long sequenceNumber, short participantCount, int barrierId, Func<Task> operation)
            {
                Debug.Assert(serverOptions.MultiLogEnabled);

                // Synchronize execution across sublogs
                var leaderBarrier = GetBarrier(barrierId, sequenceNumber, participantCount);
                var isLeader = leaderBarrier.TrySignalOrWait(out var signalException, serverOptions.ReplicaSyncTimeout);
                Exception removeBarrierException = null;

                // We execute the synchronized operation iff
                // 1. Task is the first that joined and
                // 2. No exception was triggered or we allow data loss (see cref serverOptions.AllowDataLoss).
                // In the event of an exception with the possibility of data loss we follow a best effort approach to guarantee
                // the integrity of the replication stream
                var execute = isLeader && (signalException == null || serverOptions.AllowDataLoss);
                // Here either all participants joined or timeout exception happened
                // We can guarantee only one leader since at least one replay task has entered this method.

                try
                {
                    if (execute)
                    {
                        // Only one replay task will win and execute the following operation
                        if (operation != null)
                        {
                            var opTask = operation();

                            // No choice but to block here, cannot move off thread
                            AsyncUtils.BlockingWait(opTask);
                        }
                    }
                }
                finally
                {
                    // The leader will always perform a cleanup
                    if (isLeader)
                    {
                        if (!TryRemoveBarrier(barrierId, sequenceNumber))
                            removeBarrierException = new GarnetException($"RemoveBarrier failed when processing {barrierId}");

                        // Release participants if any
                        leaderBarrier.Release();
                    }
                }

                // Throw exception if data loss is not allowed and replay failed due to exception (possibly timeout)
                if (signalException != null && serverOptions.AllowDataLoss)
                    throw signalException;

                // Need to always fail here otherwise next operations could not create a barrier if the last operation was not
                // able to remove it.
                if (removeBarrierException != null)
                    throw removeBarrierException;

                // Update timestamp
                aofProcessor.storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(sublogIdx, sequenceNumber);

                // Get barrier helper
                LeaderBarrier GetBarrier(int sessionId, long seqNum, short partCount)
                {
                    var barrierID = new BarrierKey() { SessionId = sessionId, txnId = seqNum };
                    return leaderBarriers.GetOrAdd(barrierID, _ => new LeaderBarrier(partCount));
                }

                // Remove barrier helper
                bool TryRemoveBarrier(int sessionId, long seqNum)
                {
                    var barrierID = new BarrierKey() { SessionId = sessionId, txnId = seqNum };
                    return leaderBarriers.TryRemove(barrierID, out _);
                }
            }
        }
    }
}