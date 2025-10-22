// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading;
using System.Diagnostics;

namespace Garnet.server
{
    public sealed unsafe partial class AofProcessor
    {
        readonly ConcurrentDictionary<int, TransactionGroupReplayCoordinator> txnReplayCoordinators = [];

        /// <summary>
        /// Replay StoredProc wrapper for single and sharded logs
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="id"></param>
        /// <param name="customProcInput"></param>
        /// <param name="ptr"></param>
        void ReplayStoredProc(int sublogIdx, byte id, CustomProcedureInput customProcInput, byte* ptr)
        {
            if (storeWrapper.serverOptions.AofSublogCount == 1)
            {
                RunStoredProc(id, customProcInput, ptr, false);
            }
            else
            {
                var extendedHeader = *(AofExtendedHeader*)ptr;
                var participantCount = extendedHeader.logAccessCount;
                var txnReplayCoordinator = txnReplayCoordinators.GetOrAdd(extendedHeader.header.sessionID, _ => new TransactionGroupReplayCoordinator(participantCount));

                var IsLeader = txnReplayCoordinator.AddTransactionGroup(null);

                try
                {
                    if (!IsLeader)
                    {
                        // Wait for leader to complete replay
                        txnReplayCoordinator.Wait();
                        return;
                    }

                    // Replay StoredProc
                    RunStoredProc(id, customProcInput, ptr, false);
                }
                finally
                {
                    if (IsLeader)
                    {
                        _ = txnReplayCoordinators.Remove(extendedHeader.header.sessionID, out _);
                        txnReplayCoordinator.Set();
                    }

                    // Update timestamps
                    storeWrapper.appendOnlyFile.replayedTimestampProgress.UpdateSublogTimestamp(sublogIdx, extendedHeader.timestamp);
                }
            }

            void RunStoredProc(byte id, CustomProcedureInput customProcInput, byte* ptr, bool shardedLog)
            {
                var curr = ptr + HeaderSize(shardedLog);

                // Reconstructing CustomProcedureInput

                // input
                customProcInput.DeserializeFrom(curr);

                // Run the stored procedure with the reconstructed input
                respServerSession.RunTransactionProc(id, ref customProcInput, ref output, isRecovering: true);
            }
        }

        internal class EventBarrier(int participantCount)
        {
            int count = participantCount;
            ManualResetEventSlim eventSlim = new(false);

            /// <summary>
            /// Decrements participant count but does not set signal
            /// </summary>
            /// <returns>True if participant count reaches zero otherwise false</returns>
            /// <exception cref="Exception"></exception>
            public bool Signal(out int pos)
            {
                pos = Interlocked.Decrement(ref count);
                if (pos > 0)
                    return false;
                else if (pos == 0)
                    return true;
                else
                    throw new Exception("Invalid count value < 0");
            }

            /// <summary>
            /// Set underlying event
            /// </summary>
            public void Set() => eventSlim.Set();

            /// <summary>
            /// Wait for signal to be set
            /// </summary>
            public void Wait() => eventSlim.Wait();
        }

        /// <summary>
        /// Transaction group replay coordinator is used to coordinate replay of transactions between sublogs
        /// </summary>
        /// <param name="participantCount"></param>
        internal class TransactionGroupReplayCoordinator(int participantCount)
        {
            EventBarrier eventBarrier = new(participantCount);
            /// <summary>
            /// Allocating enough space to store the transaction groups from all participating sublogs
            /// </summary>
            readonly TransactionGroup[] participatingTxnGroups = new TransactionGroup[participantCount];
            public TransactionGroup[] TxnGroups => participatingTxnGroups;

            /// <summary>
            /// Add transaction group to be replayed
            /// </summary>
            /// <param name="txnGroup"></param>
            /// <returns>True if add operation was the last one to be added otherwise true</returns>
            public bool AddTransactionGroup(TransactionGroup txnGroup)
            {
                var isLeader = eventBarrier.Signal(out var offset);
                Debug.Assert(offset <= participatingTxnGroups.Length);
                participatingTxnGroups[offset] = txnGroup;
                return isLeader;
            }

            /// <summary>
            /// Set underlying event
            /// </summary>
            public void Set() => eventBarrier.Set();

            /// <summary>
            /// Wait for signal to be set
            /// </summary>
            public void Wait() => eventBarrier.Wait();
        }

        /// <summary>
        /// Transaction group contains logAccessMap and list of operations associated with this Txn
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="logAccessMap"></param>
        internal class TransactionGroup(int sublogIdx, byte logAccessMap)
        {
            public readonly int sublogIdx = sublogIdx;
            public readonly byte logAccessCount = logAccessMap;

            public List<byte[]> operations = [];

            public void Clear() => operations.Clear();
        }

        /// <summary>
        /// Sublog replay buffer (one for each sublog)
        /// </summary>
        internal struct SublogReplayBuffer()
        {
            public readonly List<byte[]> fuzzyRegionOps = [];
            public readonly Queue<TransactionGroup> txnGroupBuffer = [];
            public readonly Dictionary<int, TransactionGroup> activeTxns = [];

            /// <summary>
            /// Add transaction group to this replay buffer
            /// </summary>
            /// <param name="sessionID"></param>
            /// <param name="sublogIdx"></param>
            /// <param name="logAccessBitmap"></param>
            public void AddTransactionGroup(int sessionID, int sublogIdx, byte logAccessBitmap)
                => activeTxns[sessionID] = new(sublogIdx, logAccessBitmap);

            /// <summary>
            /// Add transaction group to fuzzy region buffer
            /// </summary>
            /// <param name="group"></param>
            /// <param name="commitMarker"></param>
            public void AddToFuzzyRegionBuffer(TransactionGroup group, ReadOnlySpan<byte> commitMarker)
            {
                // Add commit marker operation and enqueue transaction group
                fuzzyRegionOps.Add(commitMarker.ToArray());
                txnGroupBuffer.Enqueue(group);
            }
        }

        readonly AofReplayBuffer aofReplayBuffer;

        public class AofReplayBuffer(AofProcessor aofProcessor, ILogger logger = null)
        {
            readonly AofProcessor aofProcessor = aofProcessor;
            readonly SublogReplayBuffer[] sublogReplayBuffers = InitializeSublogBuffers(aofProcessor.storeWrapper.serverOptions.AofSublogCount);
            readonly ILogger logger = logger;

            internal static SublogReplayBuffer[] InitializeSublogBuffers(int AofSublogCount)
            {
                var sublogReplayBuffers = new SublogReplayBuffer[AofSublogCount];
                for (var i = 0; i < sublogReplayBuffers.Length; i++)
                    sublogReplayBuffers[i] = new();
                return sublogReplayBuffers;
            }

            /// <summary>
            /// Get fuzzy region buffer count
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <returns></returns>
            internal int FuzzyRegionBufferCount(int sublogIdx) => sublogReplayBuffers[sublogIdx].fuzzyRegionOps.Count;

            /// <summary>
            /// Clear fuzzy region buffer
            /// </summary>
            /// <param name="sublogIdx"></param>
            internal void ClearFuzzyRegionBuffer(int sublogIdx) => sublogReplayBuffers[sublogIdx].fuzzyRegionOps.Clear();

            /// <summary>
            /// Add single operation to fuzzy region buffer
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <param name="entry"></param>
            internal unsafe void AddFuzzyRegionOperation(int sublogIdx, ReadOnlySpan<byte> entry) => sublogReplayBuffers[sublogIdx].fuzzyRegionOps.Add(entry.ToArray());

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
                // First try to process this as an existing transaction
                if (sublogReplayBuffers[sublogIdx].activeTxns.TryGetValue(header.sessionID, out var group))
                {
                    switch (header.opType)
                    {
                        case AofEntryType.TxnStart:
                            throw new GarnetException("No nested transactions expected");
                        case AofEntryType.TxnAbort:
                            ClearSessionTxn();
                            break;
                        case AofEntryType.TxnCommit:
                            if (aofProcessor.inFuzzyRegion)
                            {
                                // If in fuzzy region we want to record the commit marker and
                                // buffer the transaction group for later replay
                                var commitMarker = new ReadOnlySpan<byte>(ptr, length);
                                sublogReplayBuffers[sublogIdx].AddToFuzzyRegionBuffer(group, commitMarker);
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
                        sublogReplayBuffers[sublogIdx].activeTxns[header.sessionID].Clear();
                        sublogReplayBuffers[sublogIdx].activeTxns.Remove(header.sessionID);
                    }

                    return true;
                }

                // See if you have detected a txn
                switch (header.opType)
                {
                    case AofEntryType.TxnStart:
                        var logAccessCount = aofProcessor.storeWrapper.serverOptions.AofSublogCount == 1 ? 0 : (*(AofExtendedHeader*)ptr).logAccessCount;
                        sublogReplayBuffers[sublogIdx].AddTransactionGroup(header.sessionID, sublogIdx, (byte)logAccessCount);
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
                var fuzzyRegionOps = sublogReplayBuffers[sublogIdx].fuzzyRegionOps;
                if (fuzzyRegionOps.Count > 0)
                    logger?.LogInformation("Replaying sublogIdx: {sublogIdx} - {fuzzyRegionBufferCount} records from fuzzy region for checkpoint {newVersion}", sublogIdx, fuzzyRegionOps.Count, storeVersion);
                foreach (var entry in fuzzyRegionOps)
                {
                    fixed (byte* entryPtr = entry)
                        _ = aofProcessor.ReplayOp(sublogIdx, entryPtr, entry.Length, asReplica);
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
                var txnGroup = sublogReplayBuffers[sublogIdx].txnGroupBuffer.Dequeue();
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
                // No need to coordinate replay of transaction if operating with single sublog
                if (aofProcessor.storeWrapper.serverOptions.AofSublogCount == 1)
                {
                    ProcessTransactionGroupOperations(txnGroup);
                    return;
                }

                var extendedHeader = *(AofExtendedHeader*)ptr;

                // Add coordinator group if does not exist and add to that the txnGroup that needs to be replayed
                var participantCount = txnGroup.logAccessCount;
                var txnReplayCoordinator = aofProcessor.txnReplayCoordinators.GetOrAdd(extendedHeader.header.sessionID, _ => new TransactionGroupReplayCoordinator(participantCount));
                var IsLeader = txnReplayCoordinator.AddTransactionGroup(txnGroup);

                try
                {
                    if (!IsLeader)
                    {
                        // Wait for leader to complete replay
                        txnReplayCoordinator.Wait();
                        return;
                    }

                    // Iterate over transaction groups and apply their operations
                    foreach (var nextTxnGroup in txnReplayCoordinator.TxnGroups)
                        ProcessTransactionGroupOperations(nextTxnGroup);
                }
                finally
                {
                    if (IsLeader)
                    {
                        _ = aofProcessor.txnReplayCoordinators.Remove(extendedHeader.header.sessionID, out _);
                        txnReplayCoordinator.Set();
                    }

                    aofProcessor.storeWrapper.appendOnlyFile.replayedTimestampProgress.UpdateSublogTimestamp(sublogIdx, extendedHeader.timestamp);
                }

                // Process transaction 
                void ProcessTransactionGroupOperations(TransactionGroup txnGroup)
                {
                    foreach (var entry in txnGroup.operations)
                    {
                        fixed (byte* entryPtr = entry)
                            _ = aofProcessor.ReplayOp(txnGroup.sublogIdx, entryPtr, entry.Length, asReplica);
                    }
                }
            }
        }
    }
}