// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Wrapper for store and store-specific information
    /// </summary>
    public sealed unsafe partial class AofProcessor
    {
        readonly StoreWrapper storeWrapper;
        readonly RespServerSession respServerSession;
        readonly AofReplayCoordinator aofReplayCoordinator;

        int activeDbId;

        /// <summary>
        /// Set ReadWriteSession on the cluster session (NOTE: used for replaying stored procedures only)
        /// </summary>
        public void SetReadWriteSession() => respServerSession.clusterSession.SetReadWriteSession();

        /// <summary>Basic (Ephemeral locking) Session Context for main store</summary>
        StoreBasicContext stringBasicContext;
        /// <summary>Transactional Session Context for main store</summary>
        StoreTransactionalContext stringTransactionalContext;

        /// <summary>Basic (Ephemeral locking) Session Context for object store</summary>
        ObjectBasicContext objectBasicContext;
        /// <summary>Transactional Session Context for object store</summary>
        ObjectTransactionalContext objectTransactionalContext;

        /// <summary>Basic (Ephemeral locking) Session Context for unified store</summary>
        UnifiedBasicContext unifiedBasicContext;
        /// <summary>Transactional Session Context for unified store</summary>
        UnifiedTransactionalContext unifiedTransactionalContext;

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

            activeDbId = 0;
            respServerSession = ObtainServerSession();

            // Switch current contexts to match the default database
            SwitchActiveDatabaseContext(storeWrapper.DefaultDatabase, true);

            aofReplayCoordinator = new AofReplayCoordinator(this, logger);
            this.logger = logger;
        }

        private RespServerSession ObtainServerSession()
            => new(0, networkSender: null, storeWrapper: replayAofStoreWrapper, subscribeBroker: null, authenticator: null, enableScripts: false, clusterProvider: clusterProvider);

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            var databaseSessionsSnapshot = respServerSession.GetDatabaseSessionsSnapshot();
            foreach (var dbSession in databaseSessionsSnapshot)
            {
                dbSession.StorageSession.stringBasicContext.Session?.Dispose();
                dbSession.StorageSession.objectBasicContext.Session?.Dispose();
                dbSession.StorageSession.unifiedBasicContext.Session?.Dispose();
            }
        }

        /// <summary>
        /// Recover store using AOF
        /// </summary>
        /// <param name="db">Database to recover</param>
        /// <param name="untilAddress">Tail address for recovery</param>
        /// <returns>Tail address</returns>
        public long Recover(GarnetDatabase db, long untilAddress = -1)
        {
            var start = Stopwatch.GetTimestamp();
            var total_number_of_replayed_records = 0L;
            try
            {
                logger?.LogInformation("Begin AOF recovery for DB ID: {id}", db.Id);
                return RecoverReplay(db, untilAddress);
            }
            finally
            {
                var end = Stopwatch.GetTimestamp();
                var elapsed = Stopwatch.GetElapsedTime(start, end);
                var seconds = elapsed.TotalMilliseconds / 1000.0;
                var aofSize = db.AppendOnlyFile.TailAddress - db.AppendOnlyFile.BeginAddress;
                var recordsPerSec = total_number_of_replayed_records / seconds;
                var gigabytesPerSec = (aofSize / seconds) / (double)1_000_000_000;

                logger?.LogInformation("AOF Recovery in {seconds} secs", seconds);
                logger?.LogInformation("Total number of replayed records {total_number_of_replayed_records:N0} bytes", total_number_of_replayed_records);
                logger?.LogInformation("Throughput {recordsPerSec:N2} records/sec", recordsPerSec);
                logger?.LogInformation("AOF Recovery size {aofSize:N0}", aofSize);
                logger?.LogInformation("AOF Recovery throughput {GiBperSecs:N2} GiB/secs", gigabytesPerSec);
            }

            long RecoverReplay(GarnetDatabase db, long untilAddress)
            {
                // Begin replay for specified database
                logger?.LogInformation("Begin AOF replay for DB ID: {id}", db.Id);
                try
                {
                    // Fetch the database AOF and update the current database context for the processor
                    var appendOnlyFile = db.AppendOnlyFile;
                    SwitchActiveDatabaseContext(db);

                    // Set the tail address for replay recovery to the tail address of the AOF if none specified
                    if (untilAddress == -1)
                        untilAddress = appendOnlyFile.TailAddress;

                    // Run recover replay task
                    RecoverReplayTask(untilAddress);

                    void RecoverReplayTask(long untilAddress)
                    {
                        var count = 0;
                        using var scan = appendOnlyFile.Scan(appendOnlyFile.BeginAddress, untilAddress);

                        // Replay each AOF record in the current database context
                        while (scan.GetNext(MemoryPool<byte>.Shared, out var entry, out var length, out _, out var nextAofAddress))
                        {
                            count++;
                            ProcessAofRecord(entry, length);
                            if (count % 100_000 == 0)
                                logger?.LogTrace("Completed AOF replay of {count} records, until AOF address {nextAofAddress} (DB ID: {id})", count, nextAofAddress, db.Id);
                        }

                        logger?.LogInformation("Completed full AOF sublog replay of {count:N0} records (DB ID: {id})", count, db.Id);
                        _ = Interlocked.Add(ref total_number_of_replayed_records, count);
                    }

                    unsafe void ProcessAofRecord(IMemoryOwner<byte> entry, int length)
                    {
                        fixed (byte* ptr = entry.Memory.Span)
                            ProcessAofRecordInternal(ptr, length, asReplica: false, out _);
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
                    respServerSession.Dispose();
                }

                return -1;
            }
        }

        /// <summary>
        /// Process AOF record internal
        /// </summary>
        /// <param name="ptr"></param>
        /// <param name="length"></param>
        /// <param name="asReplica"></param>
        /// <param name="isCheckpointStart"></param>
        public unsafe void ProcessAofRecordInternal(byte* ptr, int length, bool asReplica, out bool isCheckpointStart)
        {
            var header = *(AofHeader*)ptr;
            var replayContext = aofReplayCoordinator.GetReplayContext();
            isCheckpointStart = false;

            // Handle transactions
            if (aofReplayCoordinator.AddOrReplayTransactionOperation(ptr, length, asReplica))
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
                            logger?.LogInformation("Encountered new CheckpointStartCommit before prior CheckpointEndCommit. Clearing {fuzzyRegionBufferCount} records from previous fuzzy region",
                                aofReplayCoordinator.FuzzyRegionBufferCount());
                            aofReplayCoordinator.ClearFuzzyRegionBuffer();
                        }
                        Debug.Assert(!replayContext.inFuzzyRegion);
                        replayContext.inFuzzyRegion = true;
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
                            replayContext.inFuzzyRegion = false;
                            // Take checkpoint after the fuzzy region
                            if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                            {
                                _ = storeWrapper.TakeCheckpoint(background: false, logger);
                            }

                            // Process buffered records
                            aofReplayCoordinator.ProcessFuzzyRegionOperations(storeWrapper.store.CurrentVersion, asReplica);
                            aofReplayCoordinator.ClearFuzzyRegionBuffer();
                        }
                    }
                    break;
                case AofEntryType.MainStoreStreamingCheckpointStartCommit:
                case AofEntryType.ObjectStoreStreamingCheckpointStartCommit:
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                        storeWrapper.store.SetVersion(header.storeVersion);
                    break;
                case AofEntryType.MainStoreStreamingCheckpointEndCommit:
                case AofEntryType.ObjectStoreStreamingCheckpointEndCommit:
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    break;
                case AofEntryType.FlushAll:
                    storeWrapper.FlushAllDatabases(unsafeTruncateLog: header.unsafeTruncateLog == 1);
                    break;
                case AofEntryType.FlushDb:
                    storeWrapper.FlushDatabase(unsafeTruncateLog: header.unsafeTruncateLog == 1, dbId: header.databaseId);
                    break;
                default:
                    _ = ReplayOp(stringBasicContext, objectBasicContext, unifiedBasicContext, ptr, length, asReplica);
                    break;
            }
        }

        private unsafe bool ReplayOp<TStringContext, TObjectContext, TUnifiedContext>(
                TStringContext stringContext, TObjectContext objectContext, TUnifiedContext unifiedContext,
                byte* entryPtr, int length, bool asReplica)
            where TStringContext : ITsavoriteContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            where TUnifiedContext : ITsavoriteContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var header = *(AofHeader*)entryPtr;
            var replayContext = aofReplayCoordinator.GetReplayContext();

            // Skips (1) entries with versions that were part of prior checkpoint; and (2) future entries in fuzzy region
            if (SkipRecord(replayContext.inFuzzyRegion, entryPtr, length, asReplica))
                return false;

            var bufferPtr = (byte*)Unsafe.AsPointer(ref replayContext.objectOutputBuffer[0]);
            var bufferLength = replayContext.objectOutputBuffer.Length;
            var keyPtr = entryPtr + sizeof(AofHeader);
            switch (header.opType)
            {
                case AofEntryType.StoreUpsert:
                    StoreUpsert(stringContext, keyPtr);
                    break;
                case AofEntryType.StoreRMW:
                    StoreRMW(stringContext, keyPtr);
                    break;
                case AofEntryType.StoreDelete:
                    StoreDelete(stringContext, keyPtr);
                    break;
                case AofEntryType.ObjectStoreRMW:
                    ObjectStoreRMW(objectContext, keyPtr, bufferPtr, bufferLength);
                    break;
                case AofEntryType.ObjectStoreUpsert:
                    ObjectStoreUpsert(objectContext, storeWrapper.GarnetObjectSerializer, keyPtr, bufferPtr, bufferLength);
                    break;
                case AofEntryType.ObjectStoreDelete:
                    ObjectStoreDelete(objectContext, keyPtr);
                    break;
                case AofEntryType.UnifiedStoreRMW:
                    UnifiedStoreRMW(unifiedContext, keyPtr, bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreStringUpsert:
                    UnifiedStoreStringUpsert(unifiedContext, keyPtr, bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreObjectUpsert:
                    UnifiedStoreObjectUpsert(unifiedContext, storeWrapper.GarnetObjectSerializer, keyPtr, bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreDelete:
                    UnifiedStoreDelete(unifiedContext, keyPtr);
                    break;
                case AofEntryType.StoredProcedure:
                    aofReplayCoordinator.ReplayStoredProc(header.procedureId, entryPtr);
                    break;
                case AofEntryType.TxnCommit:
                    aofReplayCoordinator.ProcessFuzzyRegionTransactionGroup(entryPtr, asReplica);
                    break;
                default:
                    throw new GarnetException($"Unknown AOF header operation type {header.opType}");
            }

            return true;
        }

        private void SwitchActiveDatabaseContext(GarnetDatabase db, bool initialSetup = false)
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
                stringTransactionalContext = respServerSession.storageSession.stringTransactionalContext;
                unifiedBasicContext = respServerSession.storageSession.unifiedBasicContext;
                unifiedTransactionalContext = respServerSession.storageSession.unifiedTransactionalContext;

                if (!storeWrapper.serverOptions.DisableObjects)
                {
                    objectBasicContext = respServerSession.storageSession.objectBasicContext.Session.BasicContext;
                    objectTransactionalContext = respServerSession.storageSession.objectTransactionalContext.Session.TransactionalContext;
                }
                activeDbId = db.Id;
            }
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
        /// <param name="inFuzzyRegion"></param>
        /// <param name="entryPtr"></param>
        /// <param name="length"></param>
        /// <param name="asReplica"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        bool SkipRecord(bool inFuzzyRegion, byte* entryPtr, int length, bool asReplica)
        {
            var header = *(AofHeader*)entryPtr;
            return (asReplica && inFuzzyRegion)     // Buffer logic only for AOF version > 1
                ? BufferNewVersionRecord(header, entryPtr, length)
                : IsOldVersionRecord(header);

            bool BufferNewVersionRecord(AofHeader header, byte* entryPtr, int length)
            {
                if (IsNewVersionRecord(header))
                {
                    aofReplayCoordinator.AddFuzzyRegionOperation(new ReadOnlySpan<byte>(entryPtr, length));
                    return true;
                }
                return false;
            }
        }

        bool IsOldVersionRecord(AofHeader header) => header.storeVersion < storeWrapper.store.CurrentVersion;

        bool IsNewVersionRecord(AofHeader header) => header.storeVersion > storeWrapper.store.CurrentVersion;
    }
}