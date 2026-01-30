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
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    /// <summary>
    /// Wrapper for store and store-specific information
    /// </summary>
    public sealed unsafe partial class AofProcessor
    {
        readonly StoreWrapper storeWrapper;
        readonly RespServerSession respServerSession;
        readonly AofReplayCoordinator aofReplayCoordinator;

        int activeDbId;
        VectorManager activeVectorManager;

        /// <summary>
        /// Set ReadWriteSession on the cluster session (NOTE: used for replaying stored procedures only)
        /// </summary>
        public void SetReadWriteSession()
        {
            respServerSession.clusterSession.SetReadWriteSession();
        }

        /// <summary>
        /// Session for main store
        /// </summary>
        BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext;

        /// <summary>
        /// Session for object store
        /// </summary>
        BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreBasicContext;

        readonly IClusterProvider clusterProvider;

        readonly ILogger logger;

        readonly Func<RespServerSession> obtainServerSession;

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
            var replayAofStoreWrapper = new StoreWrapper(storeWrapper, recordToAof);

            obtainServerSession = () => new(0, networkSender: null, storeWrapper: replayAofStoreWrapper, subscribeBroker: null, authenticator: null, enableScripts: false, clusterProvider: clusterProvider);

            this.activeDbId = 0;
            this.respServerSession = obtainServerSession();

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
            var databaseSessionsSnapshot = respServerSession.GetDatabaseSessionsSnapshot();
            foreach (var dbSession in databaseSessionsSnapshot)
            {
                dbSession.StorageSession.basicContext.Session?.Dispose();
                dbSession.StorageSession.objectStoreBasicContext.Session?.Dispose();
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
                        while (scan.GetNext(MemoryPool<byte>.Shared, out var entry, out var length, out _, out long nextAofAddress))
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
                        {
                            ProcessAofRecordInternal(ptr, length, asReplica: false, out _);
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

            // Aggressively do not move data if VADD are being replayed
            if (header.opType != AofEntryType.StoreRMW)
            {
                activeVectorManager.WaitForVectorOperationsToComplete();
            }

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
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                    {
                        storeWrapper.store.SetVersion(header.storeVersion);
                    }
                    break;
                case AofEntryType.ObjectStoreStreamingCheckpointStartCommit:
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                    {
                        storeWrapper.objectStore.SetVersion(header.storeVersion);
                    }
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
                    _ = ReplayOp(basicContext, objectStoreBasicContext, ptr, length, asReplica);
                    break;
            }
        }

        private unsafe bool ReplayOp<TContext, TObjectContext>(TContext storeContext, TObjectContext objectStoreContext, byte* entryPtr, int length, bool asReplica)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var header = *(AofHeader*)entryPtr;
            var replayContext = aofReplayCoordinator.GetReplayContext();

            // StoreRMW can queue VADDs onto different threads
            // but everything else needs to WAIT for those to complete
            // otherwise we might loose consistency
            if (header.opType != AofEntryType.StoreRMW)
            {
                activeVectorManager.WaitForVectorOperationsToComplete();
            }

            // Skips (1) entries with versions that were part of prior checkpoint; and (2) future entries in fuzzy region
            if (SkipRecord(replayContext.inFuzzyRegion, entryPtr, length, asReplica))
                return false;

            var bufferPtr = (byte*)Unsafe.AsPointer(ref replayContext.objectOutputBuffer[0]);
            var bufferLength = replayContext.objectOutputBuffer.Length;
            switch (header.opType)
            {
                case AofEntryType.StoreUpsert:
                    StoreUpsert(storeContext, replayContext.storeInput, entryPtr + sizeof(AofHeader));
                    break;
                case AofEntryType.StoreRMW:
                    StoreRMW(storeContext, replayContext.storeInput, activeVectorManager, respServerSession, obtainServerSession, entryPtr + sizeof(AofHeader));
                    break;
                case AofEntryType.StoreDelete:
                    StoreDelete(storeContext, activeVectorManager, respServerSession.storageSession, entryPtr + sizeof(AofHeader));
                    break;
                case AofEntryType.ObjectStoreRMW:
                    ObjectStoreRMW(objectStoreContext, replayContext.objectStoreInput, entryPtr + sizeof(AofHeader), bufferPtr, bufferLength);
                    break;
                case AofEntryType.ObjectStoreUpsert:
                    ObjectStoreUpsert(objectStoreContext, storeWrapper.GarnetObjectSerializer, entryPtr + sizeof(AofHeader), bufferPtr, bufferLength);
                    break;
                case AofEntryType.ObjectStoreDelete:
                    ObjectStoreDelete(objectStoreContext, entryPtr + sizeof(AofHeader));
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
            if (this.activeDbId != db.Id || initialSetup)
            {
                var session = respServerSession.storageSession.basicContext.Session;
                basicContext = session.BasicContext;
                var objectStoreSession = respServerSession.storageSession.objectStoreBasicContext.Session;
                if (objectStoreSession is not null)
                    objectStoreBasicContext = objectStoreSession.BasicContext;
                this.activeDbId = db.Id;
            }

            activeVectorManager = db.VectorManager;
        }

        static void StoreUpsert<TContext>(
            TContext context,
            RawStringInput storeInput,
            byte* ptr)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var curr = ptr;
            ref var key = ref Unsafe.AsRef<SpanByte>(curr);
            curr += key.TotalSize;

            ref var value = ref Unsafe.AsRef<SpanByte>(curr);
            curr += value.TotalSize;

            // Reconstructing RawStringInput
            _ = storeInput.DeserializeFrom(curr);

            SpanByteAndMemory output = default;
            context.Upsert(ref key, ref storeInput, ref value, ref output);
            if (!output.IsSpanByte)
                output.Memory.Dispose();
        }

        static void StoreRMW<TContext>(
            TContext context,
            RawStringInput storeInput,
            VectorManager vectorManager,
            RespServerSession currentSession,
            Func<RespServerSession> obtainServerSession,
            byte* ptr)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var curr = ptr;
            ref var key = ref Unsafe.AsRef<SpanByte>(curr);
            curr += key.TotalSize;

            // Reconstructing RawStringInput
            _ = storeInput.DeserializeFrom(curr);

            // VADD requires special handling, shove it over to the VectorManager
            if (storeInput.header.cmd == RespCommand.VADD)
            {
                vectorManager.HandleVectorSetAddReplication(currentSession.storageSession, obtainServerSession, ref key, ref storeInput);
                return;
            }
            else
            {
                // Any other op (include other vector ops) need to wait for pending VADDs to complete
                vectorManager.WaitForVectorOperationsToComplete();

                // VREM and VSETATTR are also read-like, so require special handling - shove it over to the VectorManager
                if (storeInput.header.cmd == RespCommand.VREM)
                {
                    vectorManager.HandleVectorSetRemoveReplication(currentSession.storageSession, ref key, ref storeInput);
                    return;
                }
                else if (storeInput.header.cmd == RespCommand.VSETATTR)
                {
                    vectorManager.HandleVectorUpdateAttributesReplication(currentSession.storageSession, ref key, ref storeInput);
                    return;
                }
            }

            var pbOutput = stackalloc byte[32];
            var output = new SpanByteAndMemory(pbOutput, 32);

            if (context.RMW(ref key, ref storeInput, ref output).IsPending)
                _ = context.CompletePending(true);

            if (!output.IsSpanByte)
                output.Memory.Dispose();
        }

        static void StoreDelete<TContext>(
            TContext context,
            VectorManager vectorManager,
            StorageSession storageSession,
            byte* ptr)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr);
            var res = context.Delete(ref key);

            if (res.IsCanceled)
            {
                // Might be a vector set
                res = vectorManager.TryDeleteVectorSet(storageSession, ref key, out _);
                if (res.IsPending)
                    _ = context.CompletePending(true);
            }
        }

        static void ObjectStoreUpsert<TObjectContext>(
            TObjectContext objectContext,
            GarnetObjectSerializer garnetObjectSerializer,
            byte* ptr,
            byte* outputPtr,
            int outputLength)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr);
            var keyB = key.ToByteArray();

            ref var value = ref Unsafe.AsRef<SpanByte>(ptr + key.TotalSize);
            var valB = garnetObjectSerializer.Deserialize(value.ToByteArray());

            var output = new GarnetObjectStoreOutput(new(outputPtr, outputLength));
            _ = objectContext.Upsert(ref keyB, ref valB);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Memory.Dispose();
        }

        static void ObjectStoreRMW<TObjectContext>(
            TObjectContext objectContext,
            ObjectInput objectStoreInput,
            byte* ptr,
            byte* outputPtr,
            int outputLength)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var curr = ptr;
            ref var key = ref Unsafe.AsRef<SpanByte>(curr);
            curr += key.TotalSize;
            var keyB = key.ToByteArray();

            // Reconstructing ObjectInput
            _ = objectStoreInput.DeserializeFrom(curr);

            // Call RMW with the reconstructed key & ObjectInput
            var output = new GarnetObjectStoreOutput(new(outputPtr, outputLength));
            if (objectContext.RMW(ref keyB, ref objectStoreInput, ref output).IsPending)
                _ = objectContext.CompletePending(true);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Memory.Dispose();
        }

        static void ObjectStoreDelete<TObjectContext>(
            TObjectContext objectContext,
            byte* ptr)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr);
            var keyB = key.ToByteArray();
            _ = objectContext.Delete(ref keyB);
        }

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
            return (asReplica && inFuzzyRegion) ? // Buffer logic only for AOF version > 1
                BufferNewVersionRecord(header, entryPtr, length) :
                IsOldVersionRecord(header);

            bool BufferNewVersionRecord(AofHeader header, byte* entryPtr, int length)
            {
                if (IsNewVersionRecord(header))
                {
                    aofReplayCoordinator.AddFuzzyRegionOperation(new ReadOnlySpan<byte>(entryPtr, length));
                    return true;
                }
                return false;
            }

            bool IsOldVersionRecord(AofHeader header)
            {
                var storeType = ToAofStoreType(header.opType);

                return storeType switch
                {
                    AofStoreType.MainStoreType => header.storeVersion < storeWrapper.store.CurrentVersion,
                    AofStoreType.ObjectStoreType => header.storeVersion < storeWrapper.objectStore.CurrentVersion,
                    AofStoreType.TxnType => header.storeVersion < storeWrapper.objectStore.CurrentVersion,
                    _ => throw new GarnetException($"Unexpected AOF header store type {storeType}"),
                };
            }

            bool IsNewVersionRecord(AofHeader header)
            {
                var storeType = ToAofStoreType(header.opType);
                return storeType switch
                {
                    AofStoreType.MainStoreType => header.storeVersion > storeWrapper.store.CurrentVersion,
                    AofStoreType.ObjectStoreType => header.storeVersion > storeWrapper.objectStore.CurrentVersion,
                    AofStoreType.TxnType => header.storeVersion > storeWrapper.objectStore.CurrentVersion,
                    _ => throw new GarnetException($"Unknown AOF header store type {storeType}"),
                };
            }

            static AofStoreType ToAofStoreType(AofEntryType type)
            {
                return type switch
                {
                    AofEntryType.StoreUpsert or AofEntryType.StoreRMW or AofEntryType.StoreDelete => AofStoreType.MainStoreType,
                    AofEntryType.ObjectStoreUpsert or AofEntryType.ObjectStoreRMW or AofEntryType.ObjectStoreDelete => AofStoreType.ObjectStoreType,
                    AofEntryType.TxnStart or AofEntryType.TxnCommit or AofEntryType.TxnAbort or AofEntryType.StoredProcedure => AofStoreType.TxnType,
                    AofEntryType.CheckpointStartCommit or AofEntryType.ObjectStoreStreamingCheckpointStartCommit => AofStoreType.CheckpointType,
                    AofEntryType.CheckpointEndCommit or AofEntryType.MainStoreStreamingCheckpointEndCommit or AofEntryType.ObjectStoreStreamingCheckpointEndCommit => AofStoreType.CheckpointType,
                    AofEntryType.FlushAll or AofEntryType.FlushDb => AofStoreType.FlushDbType,
                    _ => throw new GarnetException($"Conversion to AofStoreType not possible for {type}"),
                };
            }
        }
    }
}