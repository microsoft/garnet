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

        /// <summary>Basic (Ephemeral locking) Session Context for main store</summary>
        BasicContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> stringBasicContext;

        /// <summary>Basic (Ephemeral locking) Session Context for object store</summary>
        BasicContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectBasicContext;

        /// <summary>Basic (Ephemeral locking) Session Context for unified store</summary>
        BasicContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedBasicContext;

        readonly StoreWrapper replayAofStoreWrapper;
        readonly IClusterProvider clusterProvider;
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

            this.clusterProvider = clusterProvider;
            replayAofStoreWrapper = new StoreWrapper(storeWrapper, recordToAof);

            this.activeDbId = 0;
            this.respServerSessions = [.. Enumerable.Range(0, storeWrapper.serverOptions.AofVirtualSublogCount).Select(_ => ObtainServerSession())];

            // Switch current contexts to match the default database
            SwitchActiveDatabaseContext(storeWrapper.DefaultDatabase, true);
            aofReplayCoordinator = new AofReplayCoordinator(storeWrapper.serverOptions, this, logger);
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
                    dbSession.StorageSession.stringBasicContext.Session?.Dispose();
                    dbSession.StorageSession.objectBasicContext.Session?.Dispose();
                }
            }
        }

        private RespServerSession ObtainServerSession()
            => new(0, networkSender: null, storeWrapper: replayAofStoreWrapper, subscribeBroker: null, authenticator: null, enableScripts: false, clusterProvider: clusterProvider);

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
                if (activeDbId != db.Id || initialSetup)
                {
                    stringBasicContext = respServerSession.storageSession.stringBasicContext;
                    unifiedBasicContext = respServerSession.storageSession.unifiedBasicContext;

                    if (!storeWrapper.serverOptions.DisableObjects)
                        objectBasicContext = respServerSession.storageSession.objectBasicContext.Session.BasicContext;
                    activeDbId = db.Id;
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
                storeWrapper.appendOnlyFile.CreateOrUpdateKeySequenceManager();
                logger?.LogInformation("Begin AOF recovery for DB ID: {id}", db.Id);
                return RecoverReplay(db, untilAddress);
            }
            finally
            {
                storeWrapper.appendOnlyFile.ResetSequenceNumberGenerator();
                var seconds = swatch.ElapsedMilliseconds / 1000.0;
                var aofSize = db.AppendOnlyFile.TotalSize();
                var recordsPerSec = total_number_of_replayed_records / seconds;
                var GiBperSecs = aofSize / seconds / 1_000_000_000;

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

                return AofAddress.Create(storeWrapper.serverOptions.AofPhysicalSublogCount, -1);
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
        public void ProcessAofRecordInternal(int sublogIdx, byte* ptr, int length, bool asReplica, out bool isCheckpointStart)
        {
            var header = *(AofHeader*)ptr;
            var shardedHeader = default(AofShardedHeader);
            var replayContext = aofReplayCoordinator.GetReplayContext(sublogIdx);
            isCheckpointStart = false;
            var shardedLog = storeWrapper.serverOptions.AofPhysicalSublogCount > 1;

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
                        shardedHeader = *(AofShardedHeader*)ptr;
                        storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(sublogIdx, shardedHeader.sequenceNumber);
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
                        shardedHeader = *(AofShardedHeader*)ptr;
                        storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(sublogIdx, shardedHeader.sequenceNumber);
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
                    shardedHeader = *(AofShardedHeader*)ptr;
                    //logger?.LogDebug("RefreshSublogTail {sublogIdx} {idx}", sublogIdx, extendedHeader.sequenceNumber);
                    storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(sublogIdx, shardedHeader.sequenceNumber);
                    break;
                default:
                    _ = ReplayOp(sublogIdx, stringBasicContext, objectBasicContext, unifiedBasicContext, ptr, length, asReplica);
                    break;
            }
        }

        private unsafe bool ReplayOp<TStringContext, TObjectContext, TUnifiedContext>(
                int sublogIdx,
                TStringContext stringContext, TObjectContext objectContext, TUnifiedContext unifiedContext,
                byte* entryPtr, int length, bool asReplica)
            where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var header = *(AofHeader*)entryPtr;
            var replayContext = aofReplayCoordinator.GetReplayContext(sublogIdx);

            // Skips (1) entries with versions that were part of prior checkpoint; and (2) future entries in fuzzy region
            if (SkipRecord(sublogIdx, replayContext.inFuzzyRegion, entryPtr, length, asReplica))
                return false;

            var bufferPtr = (byte*)Unsafe.AsPointer(ref replayContext.objectOutputBuffer[0]);
            var bufferLength = replayContext.objectOutputBuffer.Length;

            var isSharded = storeWrapper.serverOptions.AofPhysicalSublogCount > 1;
            var updateKey = true;
            switch (header.opType)
            {
                case AofEntryType.StoreUpsert:
                    StoreUpsert(stringContext, AofHeader.SkipHeader(entryPtr));
                    break;
                case AofEntryType.StoreRMW:
                    StoreRMW(stringContext, AofHeader.SkipHeader(entryPtr));
                    break;
                case AofEntryType.StoreDelete:
                    StoreDelete(stringContext, AofHeader.SkipHeader(entryPtr));
                    break;
                case AofEntryType.ObjectStoreRMW:
                    ObjectStoreRMW(objectContext, AofHeader.SkipHeader(entryPtr), bufferPtr, bufferLength);
                    break;
                case AofEntryType.ObjectStoreUpsert:
                    ObjectStoreUpsert(objectContext, storeWrapper.GarnetObjectSerializer, AofHeader.SkipHeader(entryPtr), bufferPtr, bufferLength);
                    break;
                case AofEntryType.ObjectStoreDelete:
                    ObjectStoreDelete(objectContext, AofHeader.SkipHeader(entryPtr));
                    break;
                case AofEntryType.UnifiedStoreRMW:
                    UnifiedStoreRMW(unifiedContext, AofHeader.SkipHeader(entryPtr), bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreStringUpsert:
                    UnifiedStoreStringUpsert(unifiedContext, AofHeader.SkipHeader(entryPtr), bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreObjectUpsert:
                    UnifiedStoreObjectUpsert(unifiedContext, storeWrapper.GarnetObjectSerializer, AofHeader.SkipHeader(entryPtr), bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreDelete:
                    UnifiedStoreDelete(unifiedContext, AofHeader.SkipHeader(entryPtr));
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

        private void UpdateKeySequenceNumber(int sublogIdx, byte* ptr)
        {
            Debug.Assert(storeWrapper.serverOptions.AofPhysicalSublogCount > 1);
            var shardedHeader = *(AofShardedHeader*)ptr;
            var curr = ptr + sizeof(AofShardedHeader);
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr).ReadOnlySpan;
            storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogKeySequenceNumber(sublogIdx, key, shardedHeader.sequenceNumber);
        }

        static void StoreUpsert<TStringContext>(TStringContext stringContext, byte* keyPtr)
            where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
            var curr = keyPtr + key.TotalSize();

            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += value.TotalSize;

            var stringInput = new StringInput();
            _ = stringInput.DeserializeFrom(curr);

            SpanByteAndMemory output = default;
            _ = stringContext.Upsert(key, ref stringInput, value, ref output);
            if (!output.IsSpanByte)
                output.Dispose();
        }

        static void StoreRMW<TStringContext>(TStringContext stringContext, byte* keyPtr)
            where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
            var curr = keyPtr + key.TotalSize();

            var stringInput = new StringInput();
            _ = stringInput.DeserializeFrom(curr);

            const int stackAllocSize = 32;
            var pbOutput = stackalloc byte[stackAllocSize];
            var output = SpanByteAndMemory.FromPinnedPointer(pbOutput, stackAllocSize);

            var status = stringContext.RMW(key, ref stringInput, ref output);
            if (status.IsPending)
                StorageSession.CompletePendingForSession(ref status, ref output, ref stringContext);
            if (!output.IsSpanByte)
                output.Dispose();
        }

        static void StoreDelete<TStringContext>(TStringContext stringContext, byte* keyPtr)
            where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            => stringContext.Delete(SpanByte.FromLengthPrefixedPinnedPointer(keyPtr));

        static void ObjectStoreUpsert<TObjectContext>(TObjectContext objectContext, GarnetObjectSerializer garnetObjectSerializer, byte* keyPtr, byte* outputPtr, int outputLength)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
            var curr = keyPtr + key.TotalSize();

            var valueSpan = SpanByte.FromLengthPrefixedPinnedPointer(curr);
            var valueObject = garnetObjectSerializer.Deserialize(valueSpan.ToArray()); // TODO native deserializer to avoid alloc and copy

            var output = ObjectOutput.FromPinnedPointer(outputPtr, outputLength);
            _ = objectContext.Upsert(key, valueObject);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void ObjectStoreRMW<TObjectContext>(TObjectContext objectContext, byte* keyPtr, byte* outputPtr, int outputLength)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
            var curr = keyPtr + key.TotalSize();

            var objectInput = new ObjectInput();
            _ = objectInput.DeserializeFrom(curr);

            // Call RMW with the reconstructed key & ObjectInput
            var output = ObjectOutput.FromPinnedPointer(outputPtr, outputLength);
            var status = objectContext.RMW(key, ref objectInput, ref output);
            if (status.IsPending)
                StorageSession.CompletePendingForObjectStoreSession(ref status, ref output, ref objectContext);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void ObjectStoreDelete<TObjectContext>(TObjectContext objectContext, byte* keyPtr)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => objectContext.Delete(SpanByte.FromLengthPrefixedPinnedPointer(keyPtr));

        static void UnifiedStoreStringUpsert<TUnifiedContext>(TUnifiedContext unifiedContext, byte* keyPtr, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
            var curr = keyPtr + key.TotalSize();

            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += value.TotalSize;

            var unifiedInput = new UnifiedInput();
            _ = unifiedInput.DeserializeFrom(curr);

            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            _ = unifiedContext.Upsert(key, ref unifiedInput, value, ref output);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void UnifiedStoreObjectUpsert<TUnifiedContext>(TUnifiedContext unifiedContext, GarnetObjectSerializer garnetObjectSerializer, byte* keyPtr, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
            var curr = keyPtr + key.TotalSize();

            var valueSpan = SpanByte.FromLengthPrefixedPinnedPointer(curr);
            var valueObject = garnetObjectSerializer.Deserialize(valueSpan.ToArray()); // TODO native deserializer to avoid alloc and copy

            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            _ = unifiedContext.Upsert(key, valueObject);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void UnifiedStoreRMW<TUnifiedContext>(TUnifiedContext unifiedContext, byte* keyPtr, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr);
            var curr = keyPtr + key.TotalSize();

            var unifiedInput = new UnifiedInput();
            _ = unifiedInput.DeserializeFrom(curr);

            // Call RMW with the reconstructed key & UnifiedInput
            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            var status = unifiedContext.RMW(key, ref unifiedInput, ref output);
            if (status.IsPending)
                StorageSession.CompletePendingForUnifiedStoreSession(ref status, ref output, ref unifiedContext);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void UnifiedStoreDelete<TUnifiedContext>(TUnifiedContext unifiedContext, byte* keyPtr)
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
            => unifiedContext.Delete(SpanByte.FromLengthPrefixedPinnedPointer(keyPtr));

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
        /// Check if the calling parallel replay task should replay this entry
        /// </summary>
        /// <param name="ptr"></param>
        /// <param name="replayTaskIdx"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        public bool ShouldReplay(byte* ptr, int replayTaskIdx)
        {
            var header = *(AofHeader*)ptr;
            var replayHeaderType = (AofHeaderType)header.padding;
            switch (replayHeaderType)
            {
                // Check if should replay entry by inspecting key
                case AofHeaderType.ShardedHeader:
                    var curr = AofHeader.SkipHeader(ptr);
                    var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr).ReadOnlySpan;
                    var hash = GarnetLog.HASH(key);
                    var _replayTaskIdx = hash % storeWrapper.serverOptions.AofReplayTaskCount;
                    return replayTaskIdx == _replayTaskIdx;
                // If no key to inspect, check bit vector for participating replay tasks in the transaction
                // NOTE: HeaderType transactions include MULTI-EXEC transactions, custom txn procedures, and any operation that executes across physical and virtual sublogs (e.g. checkpoint, flushdb)
                case AofHeaderType.TransactionHeader:
                    var txnHeader = *(AofTransactionHeader*)ptr;
                    var bitVector = BitVector.CopyFrom(new Span<byte>(ptr, AofTransactionHeader.ReplayTaskAccessVectorSize));
                    return bitVector.IsSet(replayTaskIdx);
                default:
                    throw new GarnetException($"Replay header type {replayHeaderType} not supported!");
            }
        }
    }
}