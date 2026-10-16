// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Runtime.ExceptionServices;
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
            sealed class CoordinatedTransaction
            {
                internal readonly ConcurrentBag<TransactionGroup> Groups = [];
                internal bool Replayed;
                internal ExceptionDispatchInfo Failure;
            }

            readonly GarnetServerOptions serverOptions = serverOptions;
            readonly ConcurrentDictionary<BarrierKey, LeaderBarrier> leaderBarriers = [];
            readonly ConcurrentDictionary<BarrierKey, CoordinatedTransaction> coordinatedTransactions = [];
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
                coordinatedTransactions.Clear();
            }

            /// <summary>
            /// Get fuzzy region buffer count
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <returns></returns>
            internal int FuzzyRegionBufferCount(int sublogIdx) => aofReplayContext[sublogIdx].fuzzyRegionOps.Count;

            /// <summary>
            /// Discard the fuzzy region buffer, returning the pooled chunk buffers of any chunked operation it holds.
            /// </summary>
            /// <param name="sublogIdx"></param>
            internal void ClearFuzzyRegionBuffer(int sublogIdx) => aofReplayContext[sublogIdx].DiscardFuzzyRegionBuffer();

            /// <summary>
            /// Add single operation to fuzzy region buffer
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <param name="entry"></param>
            internal void AddFuzzyRegionOperation(int sublogIdx, ReadOnlySpan<byte> entry) => aofReplayContext[sublogIdx].fuzzyRegionOps.Add(new ReplayOperation(entry.ToArray()));

            /// <summary>
            /// Buffer a completed chunked-record accumulator as a fuzzy-region operation.
            /// </summary>
            internal void AddFuzzyRegionOperation(int sublogIdx, ChunkedAccumulator acc) => aofReplayContext[sublogIdx].fuzzyRegionOps.Add(new ReplayOperation(acc));

            /// <summary>
            /// This method will perform one of the following
            ///     1. TxnStart: Create a new transaction group
            ///     2. TxnCommit: Replay or buffer transaction group depending if we are in fuzzyRegion. 
            ///     3. TxnAbort: Clear corresponding sublog replay buffer.
            ///     4. Default: Add an operation to an existing transaction group
            /// </summary>
            /// <param name="virtualSublogIdx"></param>
            /// <param name="ptr"></param>
            /// <param name="length"></param>
            /// <param name="asReplica"></param>
            /// <param name="logAddressSequenceNumber"></param>
            /// <returns>Returns true if a txn operation was processed and added otherwise false</returns>
            /// <exception cref="GarnetException"></exception>
            internal bool AddOrReplayTransactionOperation(int virtualSublogIdx, byte* ptr, int length, bool asReplica, long logAddressSequenceNumber = 0)
            {
                var header = *(AofHeader*)ptr;
                var replayContext = GetReplayContext(virtualSublogIdx);
                // Process operation as part of a transaction if it belongs to the same sessionId and
                // there is already a transaction group associated with it.
                if (aofReplayContext[virtualSublogIdx].activeTxns.TryGetValue(header.sessionID, out var group))
                {
                    switch (header.opType)
                    {
                        case AofEntryType.TxnStart:
                            throw new GarnetException("No nested transactions expected");
                        case AofEntryType.TxnAbort:
                            ClearSessionTxn();
                            UpdateMaxSequenceNumberFromHeader();
                            break;
                        case AofEntryType.TxnCommit:
                            // Mirror the single-record decision in ShouldSkipRecord: inside the fuzzy region a replica
                            // defers only the records of the NEW version, because the checkpoint being taken captures
                            // the old one. A transaction is atomic, so the whole group is deferred or replayed as a
                            // unit, keyed on the version of its commit record. Deferring an old-version group instead
                            // loses it: the replica takes its own checkpoint at the end marker, and the group's
                            // operations are then stale and skipped when it replays what it buffered.
                            if (asReplica && replayContext.inFuzzyRegion
                                && header.storeVersion > aofProcessor.storeWrapper.store.CurrentVersion)
                            {
                                // Record the commit marker, which fixes where in the buffered stream the group is
                                // replayed, and buffer the group itself for later replay
                                var commitMarker = new ReadOnlySpan<byte>(ptr, length);
                                aofReplayContext[virtualSublogIdx].AddToFuzzyRegionBuffer(group, commitMarker, logAddressSequenceNumber);

                                // The group's operations are still needed: the fuzzy-region buffer now owns them and
                                // replays them at the end of the region. Drop this session's reference WITHOUT
                                // discarding them, and make space for the session's next transaction.
                                _ = aofReplayContext[virtualSublogIdx].activeTxns.Remove(header.sessionID);
                            }
                            else
                            {
                                // Otherwise process transaction group immediately, then release it
                                ProcessTransactionGroup(virtualSublogIdx, ptr, asReplica, group, logAddressSequenceNumber);
                                ClearSessionTxn();
                            }
                            break;
                        case AofEntryType.StoredProcedure:
                            throw new GarnetException($"Unexpected AOF header operation type {header.opType} within transaction");
                        default:
                            var sequenceNumber = header.HeaderType == AofHeaderType.ShardedHeader
                                ? (*(AofShardedHeader*)ptr).sequenceNumber : logAddressSequenceNumber;
                            group.Operations.Add(new ReplayOperation(new ReadOnlySpan<byte>(ptr, length).ToArray()));
                            break;
                    }

                    // Discard, rather than merely drop, the group's operations: a chunked one holds pooled buffers that
                    // are returned to the pool here. A committed group's operations have already been dispatched, which
                    // returns those buffers, and returning an already-returned chunk list is a no-op.
                    void ClearSessionTxn()
                    {
                        aofReplayContext[virtualSublogIdx].activeTxns[header.sessionID].Discard();
                        _ = aofReplayContext[virtualSublogIdx].activeTxns.Remove(header.sessionID);
                    }

                    return true;
                }

                // See if you have detected a txn
                switch (header.opType)
                {
                    case AofEntryType.TxnStart:
                        var headerType = header.HeaderType;
                        short logAccessCount = 0;
                        long startSeqNum = 0;
                        if (serverOptions.MultiLogEnabled)
                        {
                            switch (headerType)
                            {
                                case AofHeaderType.SingleLogTransactionHeader:
                                    logAccessCount = (*(AofSingleLogTransactionHeader*)ptr).participantCount;
                                    startSeqNum = logAddressSequenceNumber;
                                    break;
                                case AofHeaderType.ShardedLogTransactionHeader:
                                    logAccessCount = (*(AofShardedLogTransactionHeader*)ptr).participantCount;
                                    startSeqNum = (*(AofShardedHeader*)ptr).sequenceNumber;
                                    break;
                                default:
                                    // BasicHeader from SL-era AOF: all replay tasks participate.
                                    Debug.Assert(headerType is not AofHeaderType.BasicChunkHeader and not AofHeaderType.ShardedChunkHeader, "Chunked headers should not be encountered as they were added after *LogTransactionHeader");
                                    logAccessCount = (short)serverOptions.AofReplayTaskCount;
                                    startSeqNum = logAddressSequenceNumber;
                                    break;
                            }
                        }
                        aofReplayContext[virtualSublogIdx].AddTransactionGroup(header.sessionID, virtualSublogIdx, (byte)logAccessCount, startSeqNum);
                        break;
                    case AofEntryType.TxnAbort:
                    case AofEntryType.TxnCommit:
                        // We encountered a transaction end without start - this could happen because we truncated the AOF
                        // after a checkpoint, and the transaction belonged to the previous version. It can safely
                        // be ignored.
                        UpdateMaxSequenceNumberFromHeader();
                        break;
                    default:
                        // Continue processing
                        return false;
                }

                // Processed this record successfully
                return true;

                // U
                void UpdateMaxSequenceNumberFromHeader()
                {
                    var headerType = (*(AofHeader*)ptr).HeaderType;
                    long sequenceNumber;
                    switch (headerType)
                    {
                        case AofHeaderType.BasicHeader:
                        case AofHeaderType.BasicChunkHeader:
                        case AofHeaderType.SingleLogTransactionHeader:
                            sequenceNumber = logAddressSequenceNumber;
                            break;
                        case AofHeaderType.ShardedHeader:
                        case AofHeaderType.ShardedChunkHeader:
                            sequenceNumber = (*(AofShardedHeader*)ptr).sequenceNumber;
                            break;
                        case AofHeaderType.ShardedLogTransactionHeader:
                            sequenceNumber = (*(AofShardedLogTransactionHeader*)ptr).shardedHeader.sequenceNumber;
                            break;
                        default:
                            throw new GarnetException($"Unexpected header type {headerType}");
                    }
                    aofProcessor.storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, sequenceNumber);
                }
            }

            /// <summary>
            /// Buffer a completed chunked-record operation into the active transaction group for its session, if any. A chunked
            /// record is only ever a data op (never a Txn{Start,Commit,Abort} marker), so this only handles the "add to an
            /// existing group" case; standalone chunked ops return false and are replayed by the caller.
            /// </summary>
            /// <returns>True if the op was buffered into an active transaction group; otherwise false.</returns>
            internal bool AddOrReplayTransactionOperation(int virtualSublogIdx, ChunkedAccumulator acc, long logAddressSequenceNumber = 0)
            {
                if (aofReplayContext[virtualSublogIdx].activeTxns.TryGetValue(acc.sessionID, out var group))
                {
                    group.Operations.Add(new ReplayOperation(acc));
                    return true;
                }
                return false;
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
                    if (entry.IsChunked)
                    {
                        _ = aofProcessor.ReplayOpDispatch(
                            sublogIdx,
                            entry.Chunk,
                            replayContext,
                            replayContext.StringBasicContext,
                            replayContext.ObjectBasicContext,
                            replayContext.UnifiedBasicContext,
                            asReplica);
                    }
                    else
                    {
                        fixed (byte* entryPtr = entry.Record)
                        {
                            var header = *(AofHeader*)entryPtr;

                            // A buffered TxnCommit is a marker, not a replayable op: the operations it commits were
                            // set aside as a transaction group when the commit arrived inside the fuzzy region. Replay
                            // the group here, at the marker's position in the buffered stream, so the transaction is
                            // applied in the order it was committed. Dispatching the marker itself would instead fall
                            // through ReplayOp's switch, which has no case for it.
                            if (header.opType == AofEntryType.TxnCommit)
                            {
                                ProcessFuzzyRegionTransactionGroup(sublogIdx, entryPtr, asReplica, entry.SequenceNumber);
                                continue;
                            }

                            _ = aofProcessor.ReplayOpDispatch(
                                sublogIdx,
                                header,
                                replayContext,
                                replayContext.StringBasicContext,
                                replayContext.ObjectBasicContext,
                                replayContext.UnifiedBasicContext,
                                entryPtr,
                                entry.Record.Length,
                                asReplica);
                        }
                    }
                }
            }

            /// <summary>
            /// Replay the next buffered fuzzy-region transaction group, in the order the groups were buffered. Called
            /// when the buffered stream reaches the commit marker that was recorded alongside the group.
            /// </summary>
            /// <param name="sublogIdx"></param>
            /// <param name="ptr">The TxnCommit record the group is committed by.</param>
            /// <param name="asReplica"></param>
            /// <param name="entryAddress">Log address of the commit entry</param>
            internal void ProcessFuzzyRegionTransactionGroup(int sublogIdx, byte* ptr, bool asReplica, long entryAddress = 0)
            {
                var txnGroupBuffer = aofReplayContext[sublogIdx].txnGroupBuffer;
                Debug.Assert(txnGroupBuffer != null);

                // Every buffered commit marker is recorded together with its group, so the two stay in step. If they
                // do not, a transaction's operations have been lost; fail loudly rather than silently skipping them.
                if (txnGroupBuffer.Count == 0)
                    throw new GarnetException($"Fuzzy region commit marker on sublog {sublogIdx} has no buffered transaction group");

                // Process transaction groups in FIFO order
                var txnGroup = txnGroupBuffer.Dequeue();
                try
                {
                    ProcessTransactionGroup(sublogIdx, ptr, asReplica, txnGroup, entryAddress);
                }
                finally
                {
                    // The group has been replayed (or abandoned by a throw) and is owned by nobody else, so release
                    // what its operations hold.
                    txnGroup.Discard();
                }
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
                        else if (headerType == AofHeaderType.ShardedLogTransactionHeader)
                        {
                            var shardedHeader = *(AofShardedHeader*)ptr;
                            commitSeqNum = shardedHeader.sequenceNumber;
                            partCount = (*(AofShardedLogTransactionHeader*)ptr).participantCount;
                        }
                        else
                        {
                            // BasicHeader from SL-era AOF: all replay tasks participate
                            commitSeqNum = entryAddress;
                            partCount = (short)serverOptions.AofReplayTaskCount;
                        }

                        var transactionKey = new BarrierKey { SessionId = sessionId, txnId = txnGroup.StartSequenceNumber };
                        var coordinated = coordinatedTransactions.GetOrAdd(transactionKey, static _ => new CoordinatedTransaction());
                        coordinated.Groups.Add(txnGroup);

                        // Acquire-barrier: synchronize all participants before locking using TxnStart sequence number
                        ProcessSynchronizedOperation(
                            sublogIdx,
                            txnGroup.StartSequenceNumber,
                            partCount,
                            sessionId,
                            ReplayOrderedTransactionAsync);

                        coordinated.Failure?.Throw();
                        if (coordinated.Replayed)
                        {
                            ProcessSynchronizedOperation(sublogIdx, commitSeqNum, partCount, sessionId, null);
                            return;
                        }

                        // Start transaction (acquires locks)
                        SaveTransactionGroupKeysToLock(txnManager, txnGroup);
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

                        Task ReplayOrderedTransactionAsync()
                        {
                            try
                            {
                                if (coordinated.Groups.Any(group => group.Operations.Any(RequiresOrderedReplay)))
                                {
                                    coordinated.Replayed = true;
                                    var orderedGroup = new TransactionGroup(sublogIdx, txnGroup.LogAccessCount, txnGroup.StartSequenceNumber)
                                    {
                                        Operations = coordinated.Groups.SelectMany(group => group.Operations).OrderBy(operation => operation.SequenceNumber).ToList()
                                    };
                                    SaveTransactionGroupKeysToLock(txnManager, orderedGroup);
                                    _ = txnManager.Run(internal_txn: true);
                                    try
                                    {
                                        ProcessTransactionGroupOperations(
                                            aofProcessor,
                                            txnManager.StringTransactionalContext,
                                            txnManager.ObjectTransactionalContext,
                                            txnManager.UnifiedTransactionalContext,
                                            orderedGroup,
                                            asReplica,
                                            entryAddress);
                                    }
                                    finally
                                    {
                                        txnManager.Commit(true);
                                    }
                                }
                            }
                            catch (Exception exception)
                            {
                                coordinated.Failure = ExceptionDispatchInfo.Capture(exception);
                            }
                            finally
                            {
                                _ = coordinatedTransactions.TryRemove(transactionKey, out _);
                            }
                            return Task.CompletedTask;
                        }
                    }
                    else
                    {
                        // Single-log: no synchronization needed
                        SaveTransactionGroupKeysToLock(txnManager, txnGroup);
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

                static bool RequiresOrderedReplay(ReplayOperation operation)
                {
                    if (operation.IsChunked)
                    {
                        if (operation.Chunk.opType != AofEntryType.UnifiedStoreStringUpsert)
                            return false;
                        fixed (byte* inputPtr = operation.Chunk.input)
                            return IsVectorRename(inputPtr);
                    }

                    fixed (byte* recordPtr = operation.Record)
                    {
                        if (((AofHeader*)recordPtr)->opType != AofEntryType.UnifiedStoreStringUpsert)
                            return false;
                        var inputPtr = AofHeader.SkipHeader(recordPtr);
                        inputPtr += PinnedSpanByte.FromLengthPrefixedPinnedPointer(inputPtr).TotalSize;
                        inputPtr += PinnedSpanByte.FromLengthPrefixedPinnedPointer(inputPtr).TotalSize;
                        return IsVectorRename(inputPtr, ((AofHeader*)recordPtr)->aofHeaderVersion < 4);
                    }

                    static bool IsVectorRename(byte* inputPtr, bool legacyCmdFormat = false)
                    {
                        UnifiedInput input = default;
                        _ = input.DeserializeFrom(inputPtr);
                        if (legacyCmdFormat)
                            input.header.cmd = LegacyRespCommand.FromV3(input.header.cmd);
                        return input.header.cmd == RespCommand.RENAME && input.arg1 == VectorManager.RecordType;
                    }
                }

                // Helper to iterate of transaction keys and add them to lockset
                static void SaveTransactionGroupKeysToLock(TransactionManager txnManager, TransactionGroup txnGroup)
                {
                    foreach (var op in txnGroup.Operations)
                    {
                        if (op.IsChunked)
                        {
                            var acc = op.Chunk;
                            fixed (byte* keyPtr = acc.key)
                                txnManager.SaveKeyEntryToLock(PinnedSpanByte.FromPinnedPointer(keyPtr, acc.keyOffset), LockType.Exclusive);
                        }
                        else
                        {
                            fixed (byte* entryPtr = op.Record)
                            {
                                var curr = AofHeader.SkipHeader(entryPtr);
                                var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
                                txnManager.SaveKeyEntryToLock(key, LockType.Exclusive);
                            }
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
                    foreach (var op in txnGroup.Operations)
                    {
                        if (op.IsChunked)
                        {
                            _ = aofProcessor.ReplayOpDispatch(
                                txnGroup.VirtualSublogIdx,
                                op.Chunk,
                                replayContext,
                                stringContext,
                                objectContext,
                                unifiedContext,
                                asReplica: asReplica,
                                logAddressSequenceNumber: entryAddress);
                        }
                        else
                        {
                            fixed (byte* entryPtr = op.Record)
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
                                    op.Record.Length,
                                    asReplica: asReplica,
                                    logAddressSequenceNumber: entryAddress);
                            }
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
                    else if (headerType == AofHeaderType.ShardedLogTransactionHeader)
                    {
                        var shardedHeader = *(AofShardedHeader*)ptr;
                        sequenceNumber = shardedHeader.sequenceNumber;
                        participantCount = (*(AofShardedLogTransactionHeader*)ptr).participantCount;
                    }
                    else
                    {
                        // BasicHeader from SL-era AOF: all replay tasks participate
                        sequenceNumber = entryAddress;
                        participantCount = (short)serverOptions.AofReplayTaskCount;
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

                // Transaction replay consistency invariant:
                // Updating the sequence number before the operation executes preserves prefix consistency —
                // it signals that replay has reached this log position, matching the standalone operation model.
                // Atomicity is currently preserved through coordinated locking (acquire-barrier before writes,
                // release-barrier after commit), preventing readers from observing partial transaction state.
                // Alternatively, atomicity could be preserved without locking by relying on the read protocol
                // to re-read keys as they are updated; in that model, write-set replay must NOT advance the
                // sequence number — only the commit marker should update it.
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