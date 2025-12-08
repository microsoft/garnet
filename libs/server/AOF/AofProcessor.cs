// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Wrapper for store and store-specific information
    /// </summary>
    public sealed unsafe partial class AofProcessor
    {
        readonly StoreWrapper storeWrapper;
        //readonly RespServerSession respServerSession;

        //private readonly RawStringInput storeInput;
        //private readonly ObjectInput objectStoreInput;
        //private readonly UnifiedStoreInput unifiedStoreInput;
        //private readonly CustomProcedureInput customProcInput;
        //private readonly SessionParseState parseState;
        readonly RespServerSession[] respServerSessions;
        readonly AofReplayCoordinator aofReplayCoordinator;

        int activeDbId;

        /// <summary>
        /// Set ReadWriteSession on the cluster session (NOTE: used for replaying stored procedures only)
        /// </summary>
        public void SetReadWriteSession()
        {
            foreach (var respServerSession in respServerSessions)
                respServerSession.clusterSession.SetReadWriteSession();
        }

        /// <summary>
        /// Session for main store
        /// </summary>
        BasicContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> basicContext;

        /// <summary>
        /// Session for object store
        /// </summary>
        BasicContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectStoreBasicContext;

        /// <summary>
        /// Session for unified store
        /// </summary>
        BasicContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedStoreBasicContext;

        readonly ILogger logger;

        /// <summary>
        /// Create new AOF processor
        /// </summary>
        public AofProcessor(
            StoreWrapper storeWrapper,
            IClusterProvider clusterProvider = null,
            bool recordToAof = false,
            ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;

            var replayAofStoreWrapper = new StoreWrapper(storeWrapper, recordToAof);

            this.activeDbId = 0;
            this.respServerSessions = [ .. Enumerable.Range(0, storeWrapper.serverOptions.AofSublogCount).Select(
                    _ => new RespServerSession(
                        0,
                        networkSender: null,
                        storeWrapper: replayAofStoreWrapper,
                        subscribeBroker: null,
                        authenticator: null,
                        enableScripts: false,
                        clusterProvider: clusterProvider))];

            // Switch current contexts to match the default database
            SwitchActiveDatabaseContext(storeWrapper.DefaultDatabase, true);

            aofReplayCoordinator = new AofReplayCoordinator(this, logger);
            this.logger = logger;
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            foreach (var respServerSession in respServerSessions)
            {
                var databaseSessionsSnapshot = respServerSession.GetDatabaseSessionsSnapshot();
                foreach (var dbSession in databaseSessionsSnapshot)
                {
                    dbSession.StorageSession.basicContext.Session?.Dispose();
                    dbSession.StorageSession.objectStoreBasicContext.Session?.Dispose();
                }
            }
        }

        /// <summary>
        /// Recover store using AOF
        /// </summary>
        /// <param name="db">Database to recover</param>
        /// <param name="untilAddress">Tail address for recovery</param>
        /// <returns>Tail address</returns>
        public AofAddress Recover(GarnetDatabase db, AofAddress untilAddress)
        {
            Stopwatch swatch = new();
            swatch.Start();
            var total_number_of_replayed_records = 0L;
            try
            {
                storeWrapper.appendOnlyFile.CreateOrUpdateTimestampManager();
                logger?.LogInformation("Begin AOF recovery for DB ID: {id}", db.Id);
                return RecoverReplay(db, untilAddress);
            }
            finally
            {
                storeWrapper.appendOnlyFile.ResetSeqNumberGen();
                var seconds = swatch.ElapsedMilliseconds / 1000.0;
                var aofSize = db.AppendOnlyFile.TotalSize();
                var recordsPerSec = total_number_of_replayed_records / seconds;
                var GiBperSecs = (aofSize / seconds) / (double)1_000_000_000;

                logger?.LogInformation("AOF Recovery in {seconds} secs", seconds);
                logger?.LogInformation("Total number of replayed records {total_number_of_replayed_records:N0} bytes", total_number_of_replayed_records);
                logger?.LogInformation("Throughput {recordsPerSec:N2} records/sec", recordsPerSec);
                logger?.LogInformation("AOF Recovery size {aofSize:N0}", aofSize);
                logger?.LogInformation("AOF Recovery throughput {GiBperSecs:N2} GiB/secs", GiBperSecs);
            }

            AofAddress RecoverReplay(GarnetDatabase db, AofAddress untilAddress)
            {
                // Begin replay for specified database
                logger?.LogInformation("Begin AOF replay for DB ID: {id}", db.Id);
                try
                {
                    // Fetch the database AOF and update the current database context for the processor
                    var appendOnlyFile = db.AppendOnlyFile;
                    SwitchActiveDatabaseContext(db);

                    // Set the tail address for replay recovery to the tail address of the AOF if none specified
                    untilAddress.SetValueIf(appendOnlyFile.Log.TailAddress, -1);

                    var tasks = new Task[untilAddress.Length];
                    for (var i = 0; i < untilAddress.Length; i++)
                    {
                        var sublogIdx = i;
                        tasks[i] = Task.Run(() => RecoverReplayTask(sublogIdx, untilAddress));
                    }

                    Task.WaitAll(tasks);

                    void RecoverReplayTask(int sublogIdx, AofAddress untilAddress)
                    {
                        var count = 0;
                        using var scan = appendOnlyFile.Scan(sublogIdx, appendOnlyFile.Log.GetBeginAddress(sublogIdx), untilAddress[sublogIdx]);

                        // Replay each AOF record in the current database context
                        while (scan.GetNext(MemoryPool<byte>.Shared, out var entry, out var length, out _, out long nextAofAddress))
                        {
                            count++;
                            ProcessAofRecord(sublogIdx, entry, length);
                            if (count % 100_000 == 0)
                                logger?.LogTrace("Completed AOF replay of {count} records, until AOF address {nextAofAddress} (DB ID: {id})", count, nextAofAddress, db.Id);
                        }

                        logger?.LogInformation("Completed full AOF sublog {taskId} replay of {count:N0} records (DB ID: {id})", sublogIdx, count, db.Id);
                        _ = Interlocked.Add(ref total_number_of_replayed_records, count);
                    }

                    unsafe void ProcessAofRecord(int sublogIdx, IMemoryOwner<byte> entry, int length)
                    {
                        fixed (byte* ptr = entry.Memory.Span)
                        {
                            ProcessAofRecordInternal(sublogIdx, ptr, length, asReplica: false, out _);
                        }
                        entry.Dispose();
                    }

                    return untilAddress;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "An error occurred AofProcessor.RecoverReplay");

                    if (storeWrapper.serverOptions.FailOnRecoveryError)
                        throw;
                }
                finally
                {
                    aofReplayCoordinator.Dispose();
                    foreach (var respServerSession in respServerSessions)
                        respServerSession.Dispose();
                }

                return AofAddress.Create(storeWrapper.serverOptions.AofSublogCount, -1);
            }
        }

        /// <summary>
        /// Process AOF record internal
        /// NOTE: This method is shared between recover replay and replication replay
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="ptr"></param>
        /// <param name="length"></param>
        /// <param name="asReplica"></param>
        /// <param name="isCheckpointStart"></param>
        public unsafe void ProcessAofRecordInternal(int sublogIdx, byte* ptr, int length, bool asReplica, out bool isCheckpointStart)
        {
            var header = *(AofHeader*)ptr;
            var extendedHeader = default(AofExtendedHeader);
            var replayContext = aofReplayCoordinator.GetReplayContext(sublogIdx);
            isCheckpointStart = false;
            var shardedLog = storeWrapper.serverOptions.AofSublogCount > 1;

            // Handle transactions
            if (aofReplayCoordinator.AddOrReplayTransactionOperation(sublogIdx, ptr, length, asReplica))
                return;

            switch (header.opType)
            {
                case AofEntryType.CheckpointStartCommit:
                    // Inform caller that we processed a checkpoint start marker so that it can record ReplicationCheckpointStartOffset if this is a replica replay
                    isCheckpointStart = true;
                    if (header.aofHeaderVersion > 1)
                    {
                        if (replayContext.inFuzzyRegion)
                        {
                            logger?.LogInformation("Encountered new CheckpointStartCommit before prior CheckpointEndCommit. Clearing {fuzzyRegionBufferCount} records from previous fuzzy region", aofReplayCoordinator.FuzzyRegionBufferCount(sublogIdx));
                            aofReplayCoordinator.ClearFuzzyRegionBuffer(sublogIdx);
                        }
                        Debug.Assert(!replayContext.inFuzzyRegion);
                        replayContext.inFuzzyRegion = true;
                    }

                    if (shardedLog)
                    {
                        extendedHeader = *(AofExtendedHeader*)ptr;
                        storeWrapper.appendOnlyFile.replicaReadConsistencyManager.UpdateSublogSequencenumber(sublogIdx, extendedHeader.sequenceNumber);
                    }
                    break;
                case AofEntryType.CheckpointEndCommit:
                    if (header.aofHeaderVersion > 1)
                    {
                        if (!replayContext.inFuzzyRegion)
                        {
                            logger?.LogInformation("Encountered CheckpointEndCommit without a prior CheckpointStartCommit - ignoring");
                        }
                        else
                        {
                            Debug.Assert(replayContext.inFuzzyRegion);
                            replayContext.inFuzzyRegion = false;
                            // Take checkpoint after the fuzzy region
                            if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                            {
                                if (!shardedLog)
                                {
                                    _ = storeWrapper.TakeCheckpoint(background: false, logger);
                                }
                                else
                                {
                                    aofReplayCoordinator.ProcessSynchronizedOperation(
                                        sublogIdx,
                                        ptr,
                                        (int)EventBarrierType.CHECKPOINT,
                                        () => storeWrapper.TakeCheckpoint(background: false, logger));
                                }
                            }

                            // Process buffered records
                            aofReplayCoordinator.ProcessFuzzyRegionOperations(sublogIdx, storeWrapper.store.CurrentVersion, asReplica);
                            aofReplayCoordinator.ClearFuzzyRegionBuffer(sublogIdx);
                        }
                    }
                    break;
                case AofEntryType.MainStoreStreamingCheckpointStartCommit:
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                    {
                        if (!shardedLog)
                        {
                            storeWrapper.store.SetVersion(header.storeVersion);
                        }
                        else
                        {
                            aofReplayCoordinator.ProcessSynchronizedOperation(
                                sublogIdx,
                                ptr,
                                (int)EventBarrierType.STREAMING_CHECKPOINT,
                                () => storeWrapper.store.SetVersion(header.storeVersion));
                        }
                    }
                    break;
                case AofEntryType.ObjectStoreStreamingCheckpointStartCommit:
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                    {
                        if (!shardedLog)
                        {
                            storeWrapper.store.SetVersion(header.storeVersion);
                        }
                        else
                        {
                            aofReplayCoordinator.ProcessSynchronizedOperation(
                                sublogIdx,
                                ptr,
                                (int)EventBarrierType.STREAMING_CHECKPOINT,
                                () => storeWrapper.store.SetVersion(header.storeVersion));
                        }
                    }
                    break;
                case AofEntryType.MainStoreStreamingCheckpointEndCommit:
                case AofEntryType.ObjectStoreStreamingCheckpointEndCommit:
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    if (shardedLog)
                    {
                        extendedHeader = *(AofExtendedHeader*)ptr;
                        storeWrapper.appendOnlyFile.replicaReadConsistencyManager.UpdateSublogSequencenumber(sublogIdx, extendedHeader.sequenceNumber);
                    }
                    break;
                case AofEntryType.FlushAll:
                    if (!shardedLog)
                    {
                        storeWrapper.FlushAllDatabases(unsafeTruncateLog: header.unsafeTruncateLog == 1);
                    }
                    else
                    {
                        aofReplayCoordinator.ProcessSynchronizedOperation(
                            sublogIdx,
                            ptr,
                            (int)EventBarrierType.FLUSH_DB_ALL,
                            () => storeWrapper.FlushAllDatabases(unsafeTruncateLog: header.unsafeTruncateLog == 1));
                    }
                    break;
                case AofEntryType.FlushDb:
                    if (!shardedLog)
                    {
                        storeWrapper.FlushDatabase(unsafeTruncateLog: header.unsafeTruncateLog == 1, dbId: header.databaseId);
                    }
                    else
                    {
                        aofReplayCoordinator.ProcessSynchronizedOperation(
                            sublogIdx,
                            ptr,
                            (int)EventBarrierType.FLUSH_DB,
                            () => storeWrapper.FlushDatabase(unsafeTruncateLog: header.unsafeTruncateLog == 1, dbId: header.databaseId));
                    }
                    break;
                case AofEntryType.RefreshSublogTail:
                    extendedHeader = *(AofExtendedHeader*)ptr;
                    //logger?.LogDebug("RefreshSublogTail {sublogIdx} {idx}", sublogIdx, extendedHeader.sequenceNumber);
                    storeWrapper.appendOnlyFile.replicaReadConsistencyManager.UpdateSublogSequencenumber(sublogIdx, extendedHeader.sequenceNumber);
                    break;
                default:
                    _ = ReplayOp(sublogIdx, basicContext, objectStoreBasicContext, unifiedStoreBasicContext, ptr, length, asReplica);
                    break;
            }
        }

        private unsafe bool ReplayOp<TContext, TObjectContext, TUnifiedContext>(int sublogIdx, TContext storeContext, TObjectContext objectStoreContext, TUnifiedContext unifiedStoreContext, byte* entryPtr, int length, bool asReplica)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            where TUnifiedContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var header = *(AofHeader*)entryPtr;
            var replayContext = aofReplayCoordinator.GetReplayContext(sublogIdx);

            // Skips (1) entries with versions that were part of prior checkpoint; and (2) future entries in fuzzy region
            if (SkipRecord(sublogIdx, replayContext.inFuzzyRegion, entryPtr, length, asReplica))
                return false;

            var bufferPtr = (byte*)Unsafe.AsPointer(ref replayContext.objectOutputBuffer[0]);
            var bufferLength = replayContext.objectOutputBuffer.Length;
            var isSharded = storeWrapper.serverOptions.AofSublogCount > 1;
            var updateKey = true;
            switch (header.opType)
            {
                case AofEntryType.StoreUpsert:
                    StoreUpsert(storeContext, replayContext.storeInput, entryPtr + HeaderSize(isSharded));
                    break;
                case AofEntryType.StoreRMW:
                    StoreRMW(storeContext, replayContext.storeInput, entryPtr + HeaderSize(isSharded));
                    break;
                case AofEntryType.StoreDelete:
                    StoreDelete(storeContext, entryPtr + HeaderSize(isSharded));
                    break;
                case AofEntryType.ObjectStoreRMW:
                    ObjectStoreRMW(objectStoreContext, replayContext.objectStoreInput, entryPtr + HeaderSize(isSharded), bufferPtr, bufferLength);
                    break;
                case AofEntryType.ObjectStoreUpsert:
                    ObjectStoreUpsert(objectStoreContext, storeWrapper.GarnetObjectSerializer, entryPtr + HeaderSize(isSharded), bufferPtr, bufferLength);
                    break;
                case AofEntryType.ObjectStoreDelete:
                    ObjectStoreDelete(objectStoreContext, entryPtr + HeaderSize(isSharded));
                    break;
                case AofEntryType.UnifiedStoreRMW:
                    UnifiedStoreRMW(unifiedStoreContext, replayContext.unifiedStoreInput, entryPtr + HeaderSize(isSharded), bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreStringUpsert:
                    UnifiedStoreStringUpsert(unifiedStoreContext, replayContext.unifiedStoreInput, entryPtr + HeaderSize(isSharded), bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreObjectUpsert:
                    UnifiedStoreObjectUpsert(unifiedStoreContext, storeWrapper.GarnetObjectSerializer, entryPtr + HeaderSize(isSharded), bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreDelete:
                    UnifiedStoreDelete(unifiedStoreContext, entryPtr + HeaderSize(isSharded));
                    break;
                case AofEntryType.StoredProcedure:
                    updateKey = false;
                    aofReplayCoordinator.ReplayStoredProc(sublogIdx, header.procedureId, entryPtr);
                    break;
                case AofEntryType.TxnCommit:
                    updateKey = false;
                    aofReplayCoordinator.ProcessFuzzyRegionTransactionGroup(sublogIdx, entryPtr, asReplica);
                    break;
                default:
                    throw new GarnetException($"Unknown AOF header operation type {header.opType}");
            }
            if (isSharded && updateKey)
                UpdateKeySequenceNumber(sublogIdx, entryPtr);
            return true;
        }

        private void SwitchActiveDatabaseContext(GarnetDatabase db, bool initialSetup = false)
        {
            foreach (var respServerSession in respServerSessions)
            {
                // Switch the session's context to match the specified database, if necessary
                if (respServerSession.activeDbId != db.Id)
                {
                    var switchDbSuccessful = respServerSession.TrySwitchActiveDatabaseSession(db.Id);
                    Debug.Assert(switchDbSuccessful);
                }
                // Switch the storage context to match the session, if necessary
                if (this.activeDbId != db.Id || initialSetup)
                {
                    basicContext = respServerSession.storageSession.basicContext.Session.BasicContext;
                    objectStoreBasicContext = respServerSession.storageSession.objectStoreBasicContext.Session.BasicContext;
                    unifiedStoreBasicContext = respServerSession.storageSession.unifiedStoreBasicContext.Session.BasicContext;
                    this.activeDbId = db.Id;
                }
            }
        }

        static int HeaderSize(bool useShardedLog) => useShardedLog ? sizeof(AofExtendedHeader) : sizeof(AofHeader);

        void UpdateKeySequenceNumber(int sublogIdx, byte* ptr)
        {
            Debug.Assert(storeWrapper.serverOptions.AofSublogCount > 1);
            var extendedHeader = *(AofExtendedHeader*)ptr;
            var curr = ptr + sizeof(AofExtendedHeader);
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            storeWrapper.appendOnlyFile.replicaReadConsistencyManager.UpdateKeySequenceNumber(sublogIdx, ref key, extendedHeader.sequenceNumber);
        }

        static void StoreUpsert<TContext>(
            TContext basicContext,
            RawStringInput storeInput,
            byte* ptr)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = ptr;
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += key.TotalSize;

            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += value.TotalSize;

            // Reconstructing RawStringInput
            _ = storeInput.DeserializeFrom(curr);

            SpanByteAndMemory output = default;
            basicContext.Upsert(key.ReadOnlySpan, ref storeInput, value.ReadOnlySpan, ref output);
            output.Dispose();
        }

        static void StoreRMW<TContext>(
            TContext context,
            RawStringInput storeInput,
            byte* ptr)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = ptr;
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += key.TotalSize;

            // Reconstructing RawStringInput
            _ = storeInput.DeserializeFrom(curr);

            var pbOutput = stackalloc byte[32];
            var output = SpanByteAndMemory.FromPinnedPointer(pbOutput, 32);

            if (context.RMW(key.ReadOnlySpan, ref storeInput, ref output).IsPending)
                context.CompletePending(true);
            output.Dispose();
        }

        static void StoreDelete<TContext>(
            TContext context,
            byte* ptr)
            where TContext : ITsavoriteContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(ptr);
            context.Delete(key);
        }

        static void ObjectStoreUpsert<TContext>(
            TContext context,
            GarnetObjectSerializer garnetObjectSerializer,
            byte* ptr,
            byte* outputPtr,
            int outputLength)
            where TContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(ptr);
            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(ptr + key.TotalSize);
            var valB = garnetObjectSerializer.Deserialize(value.ToArray());

            // input
            // TODOMigrate: _ = objectStoreInput.DeserializeFrom(curr); // TODO - need to serialize this as well

            var output = GarnetObjectStoreOutput.FromPinnedPointer(outputPtr, outputLength);
            context.Upsert(key.ReadOnlySpan, valB);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Memory.Dispose();
        }

        static void ObjectStoreRMW<TContext>(
            TContext context,
            ObjectInput objectStoreInput, byte* ptr, byte* outputPtr, int outputLength)
            where TContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = ptr;
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += key.TotalSize;

            // Reconstructing ObjectInput
            _ = objectStoreInput.DeserializeFrom(curr);

            // Call RMW with the reconstructed key & ObjectInput
            var output = GarnetObjectStoreOutput.FromPinnedPointer(outputPtr, outputLength);
            if (context.RMW(key.ReadOnlySpan, ref objectStoreInput, ref output).IsPending)
                context.CompletePending(true);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Memory.Dispose();
        }

        static void ObjectStoreDelete<TContext>(
            TContext context,
            byte* ptr)
            where TContext : ITsavoriteContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(ptr);
            context.Delete(key);
        }

        static void UnifiedStoreStringUpsert<TContext>(
            TContext basicContext,
            UnifiedStoreInput storeInput,
            byte* ptr,
            byte* outputPtr,
            int outputLength)
            where TContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = ptr;
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += key.TotalSize;

            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += value.TotalSize;

            // Reconstructing UnifiedStoreInput
            _ = storeInput.DeserializeFrom(curr);

            var output = GarnetUnifiedStoreOutput.FromPinnedPointer(outputPtr, outputLength);
            basicContext.Upsert(key.ReadOnlySpan, ref storeInput, value.ReadOnlySpan, ref output);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Memory.Dispose();
        }

        static void UnifiedStoreObjectUpsert<TContext>(
            TContext basicContext,
            GarnetObjectSerializer garnetObjectSerializer,
            byte* ptr,
            byte* outputPtr,
            int outputLength)
            where TContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(ptr);

            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(ptr + key.TotalSize);
            var valB = garnetObjectSerializer.Deserialize(value.ToArray());

            // input
            // TODOMigrate: _ = unifiedStoreInput.DeserializeFrom(curr); // TODO - need to serialize this as well

            var output = GarnetUnifiedStoreOutput.FromPinnedPointer(outputPtr, outputLength);
            basicContext.Upsert(key.ReadOnlySpan, valB);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Memory.Dispose();
        }

        static void UnifiedStoreRMW<TContext>(
            TContext context,
            UnifiedStoreInput unifiedStoreInput,
            byte* ptr,
            byte* outputPtr,
            int outputLength)
            where TContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = ptr;
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += key.TotalSize;

            // Reconstructing UnifiedStoreInput
            _ = unifiedStoreInput.DeserializeFrom(curr);

            // Call RMW with the reconstructed key & UnifiedStoreInput
            var output = GarnetUnifiedStoreOutput.FromPinnedPointer(outputPtr, outputLength);
            if (context.RMW(key.ReadOnlySpan, ref unifiedStoreInput, ref output).IsPending)
                context.CompletePending(true);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Memory.Dispose();
        }

        static void UnifiedStoreDelete<TContext>(
            TContext context,
            byte* ptr)
            where TContext : ITsavoriteContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(ptr);
            context.Delete(key);
        }

        /// <summary>
        /// On recovery apply records with header.version greater than CurrentVersion.
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="inFuzzyRegion"></param>
        /// <param name="entryPtr"></param>
        /// <param name="length"></param>
        /// <param name="asReplica"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        bool SkipRecord(int sublogIdx, bool inFuzzyRegion, byte* entryPtr, int length, bool asReplica)
        {
            var header = *(AofHeader*)entryPtr;
            return (asReplica && inFuzzyRegion) ? // Buffer logic only for AOF version > 1
                BufferNewVersionRecord(sublogIdx, header, entryPtr, length) :
                IsOldVersionRecord(header);

            bool BufferNewVersionRecord(int sublogIdx, AofHeader header, byte* entryPtr, int length)
            {
                if (IsNewVersionRecord(header))
                {
                    aofReplayCoordinator.AddFuzzyRegionOperation(sublogIdx, new ReadOnlySpan<byte>(entryPtr, length));
                    return true;
                }
                return false;
            }

            bool IsOldVersionRecord(AofHeader header)
                => header.storeVersion < storeWrapper.store.CurrentVersion;

            bool IsNewVersionRecord(AofHeader header)
                => header.storeVersion > storeWrapper.store.CurrentVersion;
        }

        /// <summary>
        /// This updates maxtimestamp seen in primary's shipped records
        /// TODO: evaluate performance implications
        /// </summary>
        /// <param name="maxSendTimestamp"></param>
        /// <param name="record"></param>
        /// <param name="recordLength"></param>
        /// <param name="storeWrapper"></param>
        public static void UpdateMaxSequenceNumber(ref long maxSendTimestamp, byte* record, int recordLength, StoreWrapper storeWrapper)
        {
            var ptr = record;
            while (ptr < record + recordLength)
            {
                var entryLength = storeWrapper.appendOnlyFile.HeaderSize;
                var payloadLength = storeWrapper.appendOnlyFile.Log.GetSubLog(0).UnsafeGetLength(ptr);
                if (payloadLength > 0)
                {
                    var header = *(AofExtendedHeader*)(ptr + entryLength);
                    var timestamp = header.sequenceNumber;
                    maxSendTimestamp = Math.Max(maxSendTimestamp, timestamp);
                    entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
                }
                else
                {
                    entryLength += TsavoriteLog.UnsafeAlign(-payloadLength);
                }
                ptr += entryLength;
            }
        }
    }
}