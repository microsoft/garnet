// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    public sealed unsafe partial class AofProcessor
    {
        public class AofReplayCoordinator(AofProcessor aofProcessor, ILogger logger = null) : IDisposable
        {
            const int CHECKPOINT_BARRIER_ID = -1;

            readonly ConcurrentDictionary<int, EventBarrier> eventBarriers = [];
            readonly AofProcessor aofProcessor = aofProcessor;
            readonly AofReplayContext[] aofReplayContext = InitializeReplayContext(aofProcessor.storeWrapper.serverOptions.AofSublogCount);
            public AofReplayContext GetReplayContext(int sublogIdx) => aofReplayContext[sublogIdx];
            readonly ILogger logger = logger;

            internal static AofReplayContext[] InitializeReplayContext(int AofSublogCount)
            {
                var sublogReplayBuffers = new AofReplayContext[AofSublogCount];
                for (var i = 0; i < sublogReplayBuffers.Length; i++)
                    sublogReplayBuffers[i] = new();
                return sublogReplayBuffers;
            }

            public void Dispose()
            {
                foreach (var replayContext in aofReplayContext)
                    replayContext.output.MemoryOwner?.Dispose();
            }

            EventBarrier GetBarrier(int id, int participantCount)
                => eventBarriers.GetOrAdd(id, _ => new EventBarrier(participantCount));

            bool RemoveBarrier(int id, out EventBarrier eventBarrier)
                => eventBarriers.TryRemove(id, out eventBarrier);

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
                        aofReplayContext[sublogIdx].activeTxns.Remove(header.sessionID);
                    }

                    return true;
                }

                // See if you have detected a txn
                switch (header.opType)
                {
                    case AofEntryType.TxnStart:
                        var logAccessCount = aofProcessor.storeWrapper.serverOptions.AofSublogCount == 1 ? 0 : (*(AofExtendedHeader*)ptr).logAccessCount;
                        aofReplayContext[sublogIdx].AddTransactionGroup(header.sessionID, sublogIdx, (byte)logAccessCount);
                        break;
                    case AofEntryType.TxnAbort:
                    case AofEntryType.TxnCommit:
                        // We encountered a transaction end without start - this could happen because we truncated the AOF
                        // after a checkpoint, and the transaction belonged to the previous version. It can safely
                        // be ignored.
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
                        _ = aofProcessor.ReplayOp(sublogIdx, aofProcessor.basicContext, aofProcessor.objectStoreBasicContext, entryPtr, entry.Length, asReplica);
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
                    ProcessTransactionGroupOperations(aofProcessor, aofProcessor.basicContext, aofProcessor.objectStoreBasicContext, txnGroup, asReplica);
                }
                else
                {
                    var txnManager = aofProcessor.respServerSessions[sublogIdx].txnManager;
                    var usingShardedLog = aofProcessor.storeWrapper.serverOptions.AofSublogCount > 1;

                    // Start by saving transaction keys for locking
                    SaveTransactionGroupKeysToLock(txnManager, txnGroup, usingShardedLog: usingShardedLog);

                    // Start transaction
                    _ = txnManager.Run(internal_txn: true);

                    // Process in parallel transaction group
                    ProcessTransactionGroupOperations(aofProcessor, txnManager.LockableContext, txnManager.ObjectStoreLockableContext, txnGroup, asReplica);

                    // Wait for all participating subtasks to complete replay unless singleLog
                    if (usingShardedLog)
                        Synchronize(aofProcessor, txnGroup, ptr);

                    // Commit (NOTE: need to ensure that we do not write to log here)
                    txnManager.Commit(true);
                }

                // Helper to iterate of transaction keys and add them to lockset
                static unsafe void SaveTransactionGroupKeysToLock(TransactionManager txnManager, TransactionGroup txnGroup, bool usingShardedLog)
                {
                    foreach (var entry in txnGroup.operations)
                    {
                        ref var key = ref Unsafe.NullRef<SpanByte>();
                        fixed (byte* entryPtr = entry)
                        {
                            var header = *(AofHeader*)entryPtr;
                            var isObject = false;
                            switch (header.opType)
                            {
                                case AofEntryType.StoreUpsert:
                                case AofEntryType.StoreRMW:
                                case AofEntryType.StoreDelete:
                                    key = ref Unsafe.AsRef<SpanByte>(entryPtr + HeaderSize(usingShardedLog));
                                    isObject = false;
                                    break;
                                case AofEntryType.ObjectStoreUpsert:
                                case AofEntryType.ObjectStoreRMW:
                                case AofEntryType.ObjectStoreDelete:
                                    key = ref Unsafe.AsRef<SpanByte>(entryPtr + HeaderSize(usingShardedLog));
                                    isObject = true;
                                    break;
                                default:
                                    throw new GarnetException($"Invalid replay operation {header.opType} within transaction");
                            }

                            // Add key to the lockset
                            txnManager.SaveKeyEntryToLock(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()), isObject: isObject, LockType.Exclusive);
                        }
                    }
                }

                // Use eventBarrier to synchronize between replay subtasks
                unsafe void Synchronize(AofProcessor aofProcessor, TransactionGroup txnGroup, byte* ptr)
                {
                    var extendedHeader = *(AofExtendedHeader*)ptr;
                    // Add coordinator group if does not exist and add to that the txnGroup that needs to be replayed
                    var participantCount = txnGroup.logAccessCount;
                    var eventBarrier = eventBarriers.GetOrAdd(extendedHeader.header.sessionID, _ => new EventBarrier(participantCount));
                    if (eventBarrier.SignalAndWait(aofProcessor.storeWrapper.serverOptions.ReplicaSyncTimeout))
                    {
                        try
                        {
                            if (!eventBarriers.Remove(extendedHeader.header.sessionID, out _))
                                throw new GarnetException("Failed to remove EventBarrier");
                        }
                        finally
                        {
                            eventBarrier.Set();
                        }
                    }
                }

                // Process transaction 
                static void ProcessTransactionGroupOperations<TContext, TObjectContext>(AofProcessor aofProcessor, TContext context, TObjectContext objectContext, TransactionGroup txnGroup, bool asReplica)
                    where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
                    where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
                {
                    foreach (var entry in txnGroup.operations)
                    {
                        fixed (byte* entryPtr = entry)
                            _ = aofProcessor.ReplayOp(txnGroup.sublogIdx, context, objectContext, entryPtr, entry.Length, asReplica: asReplica);
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
                if (aofProcessor.storeWrapper.serverOptions.AofSublogCount == 1)
                {
                    RunStoredProc(0, id, ptr, shardedLog: false, null);
                }
                else
                {
                    var extendedHeader = *(AofExtendedHeader*)ptr;
                    var participantCount = extendedHeader.logAccessCount;
                    var eventBarrier = eventBarriers.GetOrAdd(extendedHeader.header.sessionID, _ => new EventBarrier(participantCount));

                    // Wait for leader to replay CustomProc
                    if (!eventBarrier.SignalAndWait(aofProcessor.storeWrapper.serverOptions.ReplicaSyncTimeout))
                        return;

                    // Only leader will execute this
                    try
                    {
                        // Initialize custom proc bitmap to keep track of hashes for keys for which their timestamp needs to be updated
                        CustomProcedureKeyHashCollection customProcKeyHashTracker = new(aofProcessor.storeWrapper.appendOnlyFile);

                        // Replay StoredProc
                        RunStoredProc(sublogIdx, id, ptr, shardedLog: true, customProcKeyHashTracker);

                        // Update timestamps for associated keys
                        customProcKeyHashTracker?.UpdateTimestamps(extendedHeader.sequenceNumber);
                    }
                    finally
                    {
                        _ = eventBarriers.Remove(extendedHeader.header.sessionID, out _);
                        eventBarrier.Set();
                    }
                }

                void RunStoredProc(int sublogIdx, byte id, byte* ptr, bool shardedLog, CustomProcedureKeyHashCollection customProcKeyHashTracker)
                {
                    var curr = ptr + HeaderSize(shardedLog);

                    // Reconstructing CustomProcedureInput
                    _ = aofReplayContext[sublogIdx].customProcInput.DeserializeFrom(curr);

                    // Run the stored procedure with the reconstructed input                    
                    var output = aofReplayContext[sublogIdx].output;
                    _ = aofProcessor.respServerSessions[sublogIdx].RunCustomTxnProcAtReplica(id, ref aofReplayContext[sublogIdx].customProcInput, ref output, isRecovering: true, customProcKeyHashTracker);
                }
            }

            internal void ProcessCheckpointMarker(byte* ptr)
            {
                // If single log don't have to synchronize, take checkpoint immediately
                if (aofProcessor.storeWrapper.serverOptions.AofSublogCount == 1)
                {
                    _ = aofProcessor.storeWrapper.TakeCheckpoint(false, logger);
                    return;
                }

                // Sychronize checkpoint execution
                var extendedHeader = *(AofExtendedHeader*)ptr;
                var eventBarrier = GetBarrier(CHECKPOINT_BARRIER_ID, extendedHeader.logAccessCount);
                if (eventBarrier.SignalAndWait(aofProcessor.storeWrapper.serverOptions.ReplicaSyncTimeout))
                {
                    _ = aofProcessor.storeWrapper.TakeCheckpoint(false, logger);
                    RemoveBarrier(CHECKPOINT_BARRIER_ID, out _);
                    eventBarrier.Set();
                }
            }
        }
    }
}