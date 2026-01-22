// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Used to index barriers for coordinated replay operations in the AofReplayCoordinator
    /// NOTE: Use only negative numbers because sessionIDs will be positive values
    /// </summary>
    internal enum EventBarrierType : int
    {
        CHECKPOINT = -1,
        STREAMING_CHECKPOINT = -2,
        FLUSH_DB = -3,
        FLUSH_DB_ALL = -4,
        CUSTOM_STORED_PROC = -5,
    }

    struct BarrierId : IEquatable<BarrierId>
    {
        public int sessionId;
        public long sequenceNumber;

        public bool Equals(BarrierId other)
            => sessionId == other.sessionId && sequenceNumber == other.sequenceNumber;

        public override bool Equals(object obj)
            => obj is BarrierId other && Equals(other);

        public override int GetHashCode()
            => HashCode.Combine(sessionId, sequenceNumber);
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
            readonly ConcurrentDictionary<BarrierId, EventBarrier> eventBarriers = [];
            readonly AofProcessor aofProcessor = aofProcessor;
            readonly AofReplayContext[] aofReplayContext = InitializeReplayContext(serverOptions.AofVirtualSublogCount);

            /// <summary>
            /// Replay context for replay subtask
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <returns></returns>
            public AofReplayContext GetReplayContext(int sublogIdx) => aofReplayContext[sublogIdx];
            readonly ILogger logger = logger;

            internal static AofReplayContext[] InitializeReplayContext(int AofVirtualSublogCount)
            {
                var virtualSublogReplayContext = new AofReplayContext[AofVirtualSublogCount];
                for (var i = 0; i < virtualSublogReplayContext.Length; i++)
                    virtualSublogReplayContext[i] = new();
                return virtualSublogReplayContext;
            }

            /// <summary>
            /// Dispose
            /// </summary>
            public void Dispose()
            {
                foreach (var replayContext in aofReplayContext)
                    replayContext.output.MemoryOwner?.Dispose();
            }

            EventBarrier GetBarrier(int sessionId, long sequenceNumber, int participantCount)
            {
                var barrierID = new BarrierId() { sessionId = sessionId, sequenceNumber = sequenceNumber };
                return eventBarriers.GetOrAdd(barrierID, _ => new EventBarrier(participantCount));
            }

            bool TryRemoveBarrier(int sessionId, long sequenceNumber, out EventBarrier eventBarrier)
            {
                var barrierID = new BarrierId() { sessionId = sessionId, sequenceNumber = sequenceNumber };
                return eventBarriers.TryRemove(barrierID, out eventBarrier);
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
            internal unsafe bool AddOrReplayTransactionOperation(int sublogIdx, byte* ptr, int length, bool asReplica)
            {
                var header = *(AofHeader*)ptr;
                var shardedHeader = default(AofShardedHeader);
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
                            shardedHeader = *(AofShardedHeader*)ptr;
                            aofProcessor.storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(sublogIdx, shardedHeader.sequenceNumber);
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
                                ProcessTransactionGroup(sublogIdx, ptr, asReplica, group);
                            }

                            // We want to clear and remove in both cases to make space for next txn from session
                            ClearSessionTxn();
                            break;
                        case AofEntryType.StoredProcedure:
                            throw new GarnetException($"Unexpected AOF header operation type {header.opType} within transaction");
                        default:
                            group.operations.Add(new ReadOnlySpan<byte>(ptr, length).ToArray());
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
                        var logAccessCount = !serverOptions.MultiLogEnabled ? 0 : (*(AofTransactionHeader*)ptr).participantCount;
                        aofReplayContext[sublogIdx].AddTransactionGroup(header.sessionID, sublogIdx, (byte)logAccessCount);
                        break;
                    case AofEntryType.TxnAbort:
                    case AofEntryType.TxnCommit:
                        // We encountered a transaction end without start - this could happen because we truncated the AOF
                        // after a checkpoint, and the transaction belonged to the previous version. It can safely
                        // be ignored.
                        shardedHeader = *(AofShardedHeader*)ptr;
                        aofProcessor.storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(sublogIdx, shardedHeader.sequenceNumber);
                        break;
                    default:
                        // Continue processing
                        return false;
                }

                // Processed this record succesfully
                return true;
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
                foreach (var entry in fuzzyRegionOps)
                {
                    fixed (byte* entryPtr = entry)
                        _ = aofProcessor.ReplayOp(sublogIdx, aofProcessor.stringBasicContext, aofProcessor.objectBasicContext, aofProcessor.unifiedBasicContext, entryPtr, entry.Length, asReplica);
                }
            }

            /// <summary>
            /// Process fuzzy region transaction groups
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <param name="ptr"></param>
            /// <param name="asReplica"></param>
            internal void ProcessFuzzyRegionTransactionGroup(int sublogIdx, byte* ptr, bool asReplica)
            {
                Debug.Assert(aofReplayContext[sublogIdx].txnGroupBuffer != null);
                // Process transaction groups in FIFO order
                var txnGroup = aofReplayContext[sublogIdx].txnGroupBuffer.Dequeue();
                ProcessTransactionGroup(sublogIdx, ptr, asReplica, txnGroup);
            }

            /// <summary>
            /// Process provided transaction group
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <param name="asReplica"></param>
            /// <param name="txnGroup"></param>
            internal void ProcessTransactionGroup(int sublogIdx, byte* ptr, bool asReplica, TransactionGroup txnGroup)
            {
                if (!asReplica)
                {
                    // If recovering reads will not expose partial transactions so we can replay without locking.
                    // Also we don't have to synchronize replay of sublogs because write ordering has been established at the time of enqueue.
                    ProcessTransactionGroupOperations(aofProcessor, aofProcessor.stringBasicContext, aofProcessor.objectBasicContext, aofProcessor.unifiedBasicContext, txnGroup, asReplica);
                }
                else
                {
                    var txnManager = aofProcessor.respServerSessions[sublogIdx].txnManager;

                    // Start by saving transaction keys for locking
                    SaveTransactionGroupKeysToLock(txnManager, txnGroup);

                    // Start transaction
                    _ = txnManager.Run(internal_txn: true);

                    // Process in parallel transaction group
                    ProcessTransactionGroupOperations(
                        aofProcessor,
                        txnManager.StringTransactionalContext,
                        txnManager.ObjectTransactionalContext,
                        txnManager.UnifiedTransactionalContext,
                        txnGroup,
                        asReplica);

                    // Wait for all participating subtasks to complete replay unless singleLog
                    if (serverOptions.MultiLogEnabled)
                    {
                        var shardedHeader = *(AofShardedHeader*)ptr;
                        // Synchronize replay of txn
                        ProcessSynchronizedOperation(
                            sublogIdx,
                            ptr,
                            shardedHeader.basicHeader.sessionID,
                            () => { });
                    }

                    // Commit (NOTE: need to ensure that we do not write to log here)
                    txnManager.Commit(true);
                }

                // Helper to iterate of transaction keys and add them to lockset
                static unsafe void SaveTransactionGroupKeysToLock(TransactionManager txnManager, TransactionGroup txnGroup)
                {
                    foreach (var entry in txnGroup.operations)
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
                        TransactionGroup txnGroup, bool asReplica)
                    where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
                    where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
                    where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
                {
                    foreach (var entry in txnGroup.operations)
                    {
                        fixed (byte* entryPtr = entry)
                            _ = aofProcessor.ReplayOp(txnGroup.sublogIdx, stringContext, objectContext, unifiedContext, entryPtr, entry.Length, asReplica: asReplica);
                    }
                }
            }

            /// <summary>
            /// Replay StoredProc wrapper for single and sharded logs
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <param name="id"></param>
            /// <param name="ptr"></param>
            internal void ReplayStoredProc(int sublogIdx, byte id, byte* ptr)
            {
                if (!serverOptions.MultiLogEnabled)
                {
                    StoredProcRunnerBase(0, id, ptr, shardedLog: false, null);
                }
                else
                {
                    var shardedHeader = *(AofShardedHeader*)ptr;
                    // Initialize custom proc collection to keep track of hashes for keys for which their timestamp needs to be updated
                    CustomProcedureKeyHashCollection customProcKeyHashTracker = new(aofProcessor.storeWrapper.appendOnlyFile);

                    logger?.LogError("> sublogIdx: {sublogIdx}", sublogIdx);
                    // Synchronized processing of stored proc operation
                    ProcessSynchronizedOperation(
                        sublogIdx,
                        ptr,
                        shardedHeader.basicHeader.sessionID,
                        () => StoredProcRunnerWrapper(sublogIdx, id, ptr));
                    logger?.LogError("< sublogIdx: {sublogIdx}", sublogIdx);

                    // Wrapper for store proc runner used for multi-log synchronization
                    void StoredProcRunnerWrapper(int sublogIdx, byte id, byte* ptr)
                    {
                        // Initialize custom proc collection to keep track of hashes for keys for which their timestamp needs to be updated
                        CustomProcedureKeyHashCollection customProcKeyHashTracker = new(aofProcessor.storeWrapper.appendOnlyFile);

                        // Replay StoredProc
                        StoredProcRunnerBase(sublogIdx, id, ptr, shardedLog: true, customProcKeyHashTracker);

                        // Update timestamps for associated keys
                        customProcKeyHashTracker?.UpdateSequenceNumber(shardedHeader.sequenceNumber);
                    }
                }

                // Based run stored proc method used of legacy single log implementation
                void StoredProcRunnerBase(int sublogIdx, byte id, byte* entryPtr, bool shardedLog, CustomProcedureKeyHashCollection customProcKeyHashTracker)
                {
                    var curr = AofHeader.SkipHeader(entryPtr);

                    // Reconstructing CustomProcedureInput
                    _ = aofReplayContext[sublogIdx].customProcInput.DeserializeFrom(curr);

                    // Run the stored procedure with the reconstructed input                    
                    var output = aofReplayContext[sublogIdx].output;
                    _ = aofProcessor.respServerSessions[sublogIdx].RunCustomTxnProcAtReplica(id, ref aofReplayContext[sublogIdx].customProcInput, ref output, isRecovering: true, customProcKeyHashTracker);
                }
            }

            /// <summary>
            /// Unified method to process operations that require synchronization across sublogs
            /// </summary>
            /// <param name="sublogIdx">SublogIdx</param>
            /// <param name="ptr">Pointer to the AOF entry</param>
            /// <param name="barrierId">Unique barrier ID for this operation type</param>
            /// <param name="operation">The operation to execute</param>
            internal void ProcessSynchronizedOperation(int sublogIdx, byte* ptr, int barrierId, Action operation)
            {
                Debug.Assert(serverOptions.MultiLogEnabled);

                // Extract extended header info and validate header
                var txnHeader = *(AofTransactionHeader*)ptr;

                // Synchronize execution across sublogs
                var eventBarrier = GetBarrier(barrierId, txnHeader.shardedHeader.sequenceNumber, txnHeader.participantCount);
                var isLeader = eventBarrier.TrySignalAndWait(out var signalException, serverOptions.ReplicaSyncTimeout);
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
                        operation();
                    }
                }
                finally
                {
                    // The leader will always perform a cleanup
                    if (isLeader)
                    {
                        if (!TryRemoveBarrier(barrierId, txnHeader.shardedHeader.sequenceNumber, out _))
                            removeBarrierException = new GarnetException($"RemoveBarrier failed when processing {barrierId}");

                        // Release participants if any
                        eventBarrier.Release();
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
                aofProcessor.storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(sublogIdx, txnHeader.shardedHeader.sequenceNumber);
            }
        }
    }
}