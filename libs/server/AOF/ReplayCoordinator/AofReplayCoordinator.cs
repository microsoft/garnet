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
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

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
        /// <param name="aofProcessor"></param>
        /// <param name="logger"></param>
        public class AofReplayCoordinator(AofProcessor aofProcessor, ILogger logger = null) : IDisposable
        {
            readonly AofProcessor aofProcessor = aofProcessor;
            readonly AofReplayContext aofReplayContext = InitializeReplayContext();
            public AofReplayContext GetReplayContext() => aofReplayContext;
            readonly ILogger logger = logger;

            internal static AofReplayContext InitializeReplayContext()
            {
                return new AofReplayContext();
            }

            /// <summary>
            /// Dispose
            /// </summary>
            public void Dispose()
            {
                aofReplayContext.output.MemoryOwner?.Dispose();
            }

            /// <summary>
            /// Get fuzzy region buffer count
            /// </summary>
            /// <returns></returns>
            internal int FuzzyRegionBufferCount() => aofReplayContext.fuzzyRegionOps.Count;

            /// <summary>
            /// Clear fuzzy region buffer
            /// </summary>
            internal void ClearFuzzyRegionBuffer() => aofReplayContext.fuzzyRegionOps.Clear();

            /// <summary>
            /// Add single operation to fuzzy region buffer
            /// </summary>
            /// <param name="entry"></param>
            internal unsafe void AddFuzzyRegionOperation(ReadOnlySpan<byte> entry) => aofReplayContext.fuzzyRegionOps.Add(entry.ToArray());

            /// <summary>
            /// This method will perform one of the following
            ///     1. TxnStart: Create a new transaction group
            ///     2. TxnCommit: Replay or buffer transaction group depending if we are in fuzzyRegion. 
            ///     3. TxnAbort: Clear corresponding sublog replay buffer.
            ///     4. Default: Add an operation to an existing transaction group
            /// </summary>
            /// <param name="ptr"></param>
            /// <param name="length"></param>
            /// <param name="asReplica"></param>
            /// <returns>Returns true if a txn operation was processed and added otherwise false</returns>
            /// <exception cref="GarnetException"></exception>
            internal unsafe bool AddOrReplayTransactionOperation(byte* ptr, int length, bool asReplica)
            {
                var header = *(AofHeader*)ptr;
                var replayContext = GetReplayContext();
                // First try to process this as an existing transaction
                if (aofReplayContext.activeTxns.TryGetValue(header.sessionID, out var group))
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
                                aofReplayContext.AddToFuzzyRegionBuffer(group, commitMarker);
                            }
                            else
                            {
                                // Otherwise process transaction group immediately
                                ProcessTransactionGroup(ptr, asReplica, group);
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
                        aofReplayContext.activeTxns[header.sessionID].Clear();
                        _ = aofReplayContext.activeTxns.Remove(header.sessionID);
                    }

                    return true;
                }

                // See if you have detected a txn
                switch (header.opType)
                {
                    case AofEntryType.TxnStart:
                        aofReplayContext.AddTransactionGroup(header.sessionID);
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

                // Processed this record successfully
                return true;
            }

            /// <summary>
            /// Process fuzzy region operations if any
            /// </summary>
            /// <param name="storeVersion"></param>
            /// <param name="asReplica"></param>
            internal void ProcessFuzzyRegionOperations(long storeVersion, bool asReplica)
            {
                var fuzzyRegionOps = aofReplayContext.fuzzyRegionOps;
                if (fuzzyRegionOps.Count > 0)
                    logger?.LogInformation("Replaying {fuzzyRegionBufferCount} records from fuzzy region for checkpoint {newVersion}", fuzzyRegionOps.Count, storeVersion);
                foreach (var entry in fuzzyRegionOps)
                {
                    fixed (byte* entryPtr = entry)
                        _ = aofProcessor.ReplayOp(aofProcessor.basicContext, aofProcessor.objectStoreBasicContext, entryPtr, entry.Length, asReplica);
                }
            }

            /// <summary>
            /// Process fuzzy region transaction groups
            /// </summary>
            /// <param name="ptr"></param>
            /// <param name="asReplica"></param>
            internal void ProcessFuzzyRegionTransactionGroup(byte* ptr, bool asReplica)
            {
                Debug.Assert(aofReplayContext.txnGroupBuffer != null);
                // Process transaction groups in FIFO order
                if (aofReplayContext.txnGroupBuffer.Count > 0)
                {
                    var txnGroup = aofReplayContext.txnGroupBuffer.Dequeue();
                    ProcessTransactionGroup(ptr, asReplica, txnGroup);
                }
            }

            /// <summary>
            /// Process provided transaction group
            /// </summary>
            /// <param name="asReplica"></param>
            /// <param name="txnGroup"></param>
            internal void ProcessTransactionGroup(byte* ptr, bool asReplica, TransactionGroup txnGroup)
            {
                if (!asReplica)
                {
                    // If recovering reads will not expose partial transactions so we can replay without locking.
                    // Also we don't have to synchronize replay of sublogs because write ordering has been established at the time of enqueue.
                    ProcessTransactionGroupOperations(aofProcessor, aofProcessor.basicContext, aofProcessor.objectStoreBasicContext, txnGroup, asReplica);
                }
                else
                {
                    var txnManager = aofProcessor.respServerSession.txnManager;

                    // Start by saving transaction keys for locking
                    SaveTransactionGroupKeysToLock(txnManager, txnGroup);

                    // Start transaction
                    _ = txnManager.Run(internal_txn: true);

                    // Process in parallel transaction group
                    ProcessTransactionGroupOperations(aofProcessor, txnManager.LockableContext, txnManager.ObjectStoreLockableContext, txnGroup, asReplica);

                    // Commit (NOTE: need to ensure that we do not write to log here)
                    txnManager.Commit(true);
                }

                // Helper to iterate of transaction keys and add them to lockset
                static unsafe void SaveTransactionGroupKeysToLock(TransactionManager txnManager, TransactionGroup txnGroup)
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
                                    key = ref Unsafe.AsRef<SpanByte>(entryPtr + sizeof(AofHeader));
                                    isObject = false;
                                    break;
                                case AofEntryType.ObjectStoreUpsert:
                                case AofEntryType.ObjectStoreRMW:
                                case AofEntryType.ObjectStoreDelete:
                                    key = ref Unsafe.AsRef<SpanByte>(entryPtr + sizeof(AofHeader));
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

                // Process transaction 
                static void ProcessTransactionGroupOperations<TContext, TObjectContext>(AofProcessor aofProcessor, TContext context, TObjectContext objectContext, TransactionGroup txnGroup, bool asReplica)
                    where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
                    where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
                {
                    foreach (var entry in txnGroup.operations)
                    {
                        fixed (byte* entryPtr = entry)
                            _ = aofProcessor.ReplayOp(context, objectContext, entryPtr, entry.Length, asReplica: asReplica);
                    }
                }
            }

            /// <summary>
            /// Replay StoredProc wrapper for single and sharded logs
            /// </summary>
            /// <param name="id"></param>
            /// <param name="ptr"></param>
            internal void ReplayStoredProc(byte id, byte* ptr)
            {
                StoredProcRunnerBase(id, ptr);

                // Based run stored proc method used of legacy single log implementation
                void StoredProcRunnerBase(byte id, byte* ptr)
                {
                    var curr = ptr + sizeof(AofHeader);

                    // Reconstructing CustomProcedureInput
                    _ = aofReplayContext.customProcInput.DeserializeFrom(curr);

                    // Run the stored procedure with the reconstructed input                    
                    var output = aofReplayContext.output;
                    _ = aofProcessor.respServerSession.RunTransactionProc(id, ref aofReplayContext.customProcInput, ref output, isRecovering: true);
                }
            }
        }
    }
}