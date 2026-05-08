// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    unsafe ref struct PreparedParameters
    {
        public Span<byte> Key;
        public long KeyHash;
        public byte* PayloadPtr;
    }

    interface IPreprocessKey
    {
        public abstract unsafe void PrepareKey(int virtualSublogIdx, byte* entryPtr, out PreparedParameters preparedParameters);
    }

    struct SingleLogPreprocessKey : IPreprocessKey
    {
        public unsafe void PrepareKey(int virtualSublogIdx, byte* entryPtr, out PreparedParameters preparedParameters)
        {
            var keyPtr = entryPtr + sizeof(AofHeader);
            preparedParameters = new()
            {
                Key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr)
            };
            preparedParameters.KeyHash = GarnetLog.HASH(preparedParameters.Key);
            preparedParameters.PayloadPtr = keyPtr + preparedParameters.Key.TotalSize();
        }
    }

    struct ShardedLogPreprocessKey : IPreprocessKey
    {
        public GarnetAppendOnlyFile appendOnlyFile;

        public unsafe void PrepareKey(int virtualSublogIdx, byte* entryPtr, out PreparedParameters preparedParameters)
        {
            var shardedHeader = *(AofShardedHeader*)entryPtr;
            var keyPtr = entryPtr + sizeof(AofShardedHeader);
            preparedParameters = new()
            {
                Key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr)
            };
            preparedParameters.KeyHash = GarnetLog.HASH(preparedParameters.Key);
            preparedParameters.PayloadPtr = keyPtr + preparedParameters.Key.TotalSize();
            appendOnlyFile.readConsistencyManager.UpdateVirtualSublogKeySequenceNumber(
                virtualSublogIdx,
                preparedParameters.KeyHash,
                shardedHeader.sequenceNumber);
        }
    }

    /// <summary>
    /// Wrapper for store and store-specific information
    /// </summary>
    public sealed unsafe partial class AofProcessor
    {
        readonly StoreWrapper storeWrapper;
        readonly AofReplayCoordinator aofReplayCoordinator;

        int activeDbId;
        internal VectorManager activeVectorManager;
        RangeIndexManager activeRangeIndexManager;

        /// <summary>
        /// Set ReadWriteSession on the cluster session (NOTE: used for replaying stored procedures only)
        /// </summary>
        public void SetReadWriteSession()
        {
            for (var i = 0; i < storeWrapper.serverOptions.AofVirtualSublogCount; i++)
            {
                var respServerSession = aofReplayCoordinator.GetReplayContext(i).respServerSession;
                respServerSession.clusterSession.SetReadWriteSession();
            }
        }

        readonly StoreWrapper replayAofStoreWrapper;
        readonly IClusterProvider clusterProvider;

        readonly Func<RespServerSession> obtainServerSession;

        readonly ILogger logger;
        readonly bool usingShardedLog;
        SingleLogPreprocessKey singleLogPreprocessKey;
        ShardedLogPreprocessKey shardedLogPreprocessKey;

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
            this.replayAofStoreWrapper = new StoreWrapper(storeWrapper, recordToAof);

            this.activeDbId = 0;
            this.usingShardedLog = storeWrapper.serverOptions.AofPhysicalSublogCount > 1 || storeWrapper.serverOptions.AofReplayTaskCount > 1;
            if (usingShardedLog)
                this.shardedLogPreprocessKey = new ShardedLogPreprocessKey() { appendOnlyFile = storeWrapper.appendOnlyFile };
            else
                this.singleLogPreprocessKey = new SingleLogPreprocessKey();
            this.obtainServerSession = () => new(0, networkSender: null, storeWrapper: replayAofStoreWrapper, subscribeBroker: null, authenticator: null, enableScripts: false, clusterProvider: clusterProvider);

            this.aofReplayCoordinator = new AofReplayCoordinator(storeWrapper.serverOptions, this, logger);
            this.logger = logger;

            // Switch current contexts to match the default database
            SwitchActiveDatabaseContext(storeWrapper.DefaultDatabase, true);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            activeVectorManager?.WaitForVectorOperationsToComplete();
            activeVectorManager?.ShutdownReplayTasks();
            aofReplayCoordinator?.Dispose();
        }

        private RespServerSession ObtainServerSession()
            => new(0, networkSender: null, storeWrapper: replayAofStoreWrapper, subscribeBroker: null, authenticator: null, enableScripts: false, clusterProvider: clusterProvider);

        private void SwitchActiveDatabaseContext(GarnetDatabase db, bool initialSetup = false)
        {
            for (var i = 0; i < storeWrapper.serverOptions.AofVirtualSublogCount; i++)
            {
                var respServerSession = aofReplayCoordinator.GetReplayContext(i).respServerSession;
                // Switch the session's context to match the specified database, if necessary
                if (respServerSession.activeDbId != db.Id)
                {
                    var switchDbSuccessful = respServerSession.TrySwitchActiveDatabaseSession(db.Id);
                    Debug.Assert(switchDbSuccessful);
                }

                // Switch the storage context to match the session, if necessary
                if (activeDbId != db.Id || initialSetup)
                {
                    activeDbId = db.Id;
                    activeVectorManager = db.VectorManager;
                    activeRangeIndexManager = db.RangeIndexManager;
                }
            }
        }

        /// <summary>
        /// Wait for any queued Vector Set operations to complete.
        /// </summary>
        public void WaitForVectorOperationsToComplete()
            => activeVectorManager?.WaitForVectorOperationsToComplete();

        /// <summary>
        /// Process AOF record internal
        /// NOTE: This method is shared between recover replay and replication replay
        /// </summary>
        /// <param name="virtualSublogIdx"></param>
        /// <param name="ptr"></param>
        /// <param name="length"></param>
        /// <param name="asReplica"></param>
        /// <param name="isCheckpointStart"></param>
        public void ProcessAofRecordInternal(int virtualSublogIdx, byte* ptr, int length, bool asReplica, out bool isCheckpointStart)
        {
            var header = *(AofHeader*)ptr;
            var shardedHeader = default(AofShardedHeader);
            var replayContext = aofReplayCoordinator.GetReplayContext(virtualSublogIdx);
            isCheckpointStart = false;

            // StoreRMW can queue VADDs onto different threads
            // but everything else needs to WAIT for those to complete
            // otherwise we might loose consistency
            if (header.opType != AofEntryType.StoreRMW)
            {
                activeVectorManager.WaitForVectorOperationsToComplete();
            }

            // Handle transactions
            if (aofReplayCoordinator.AddOrReplayTransactionOperation(virtualSublogIdx, ptr, length, asReplica))
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
                            logger?.LogInformation("Encountered new CheckpointStartCommit before prior CheckpointEndCommit. Clearing {fuzzyRegionBufferCount} records from previous fuzzy region", aofReplayCoordinator.FuzzyRegionBufferCount(virtualSublogIdx));
                            aofReplayCoordinator.ClearFuzzyRegionBuffer(virtualSublogIdx);
                        }
                        Debug.Assert(!replayContext.inFuzzyRegion);
                        replayContext.inFuzzyRegion = true;
                    }

                    if (usingShardedLog)
                    {
                        shardedHeader = *(AofShardedHeader*)ptr;
                        storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, shardedHeader.sequenceNumber);
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
                                if (!usingShardedLog)
                                {
                                    // Must block here, cannot move off the thread
                                    _ = AsyncUtils.BlockingWait(storeWrapper.TakeCheckpointAsync(background: false, logger: logger));
                                }
                                else
                                {
                                    aofReplayCoordinator.ProcessSynchronizedOperation(
                                        virtualSublogIdx,
                                        ptr,
                                        (int)LeaderBarrierType.CHECKPOINT,
                                        () => storeWrapper.TakeCheckpointAsync(background: false, logger: logger));
                                }
                            }

                            // Process buffered records
                            aofReplayCoordinator.ProcessFuzzyRegionOperations(virtualSublogIdx, storeWrapper.store.CurrentVersion, asReplica);
                            aofReplayCoordinator.ClearFuzzyRegionBuffer(virtualSublogIdx);
                        }
                    }
                    break;
                case AofEntryType.MainStoreStreamingCheckpointStartCommit:
                case AofEntryType.ObjectStoreStreamingCheckpointStartCommit:
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                    {
                        if (!usingShardedLog)
                        {
                            storeWrapper.store.SetVersion(header.storeVersion);
                        }
                        else
                        {
                            aofReplayCoordinator.ProcessSynchronizedOperation(
                                virtualSublogIdx,
                                ptr,
                                (int)LeaderBarrierType.STREAMING_CHECKPOINT,
                                () => { storeWrapper.store.SetVersion(header.storeVersion); return Task.CompletedTask; }
                            );
                        }
                    }
                    break;
                case AofEntryType.MainStoreStreamingCheckpointEndCommit:
                case AofEntryType.ObjectStoreStreamingCheckpointEndCommit:
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    if (usingShardedLog)
                    {
                        shardedHeader = *(AofShardedHeader*)ptr;
                        storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, shardedHeader.sequenceNumber);
                    }
                    break;
                case AofEntryType.FlushAll:
                    if (!usingShardedLog)
                    {
                        storeWrapper.FlushAllDatabases(unsafeTruncateLog: header.UnsafeTruncateLog);
                    }
                    else
                    {
                        aofReplayCoordinator.ProcessSynchronizedOperation(
                            virtualSublogIdx,
                            ptr,
                            (int)LeaderBarrierType.FLUSH_DB_ALL,
                            () => { storeWrapper.FlushAllDatabases(unsafeTruncateLog: header.UnsafeTruncateLog); return Task.CompletedTask; }
                        );
                    }
                    break;
                case AofEntryType.FlushDb:
                    if (!usingShardedLog)
                    {
                        storeWrapper.FlushDatabase(unsafeTruncateLog: header.UnsafeTruncateLog, dbId: header.databaseId);
                    }
                    else
                    {
                        aofReplayCoordinator.ProcessSynchronizedOperation(
                            virtualSublogIdx,
                            ptr,
                            (int)LeaderBarrierType.FLUSH_DB,
                            () => { storeWrapper.FlushDatabase(unsafeTruncateLog: header.UnsafeTruncateLog, dbId: header.databaseId); return Task.CompletedTask; }
                        );
                    }
                    break;
                case AofEntryType.StoredProcedure:
                    aofReplayCoordinator.ReplayStoredProc(virtualSublogIdx, header.procedureId, ptr);
                    break;
                case AofEntryType.TxnCommit:
                    aofReplayCoordinator.ProcessFuzzyRegionTransactionGroup(virtualSublogIdx, ptr, asReplica);
                    break;
                default:
                    _ = ReplayOpDispatch(
                        virtualSublogIdx,
                        header,
                        replayContext,
                        replayContext.StringBasicContext,
                        replayContext.ObjectBasicContext,
                        replayContext.UnifiedBasicContext,
                        ptr,
                        length,
                        asReplica);
                    break;
            }
        }

        internal bool ReplayOpDispatch<TStringContext, TObjectContext, TUnifiedContext>(
                int virtualSublogIdx,
                AofHeader header,
                AofReplayContext replayContext,
                TStringContext stringContext,
                TObjectContext objectContext,
                TUnifiedContext unifiedContext,
                byte* entryPtr,
                int length,
                bool asReplica)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            if (usingShardedLog)
                return ReplayOp(virtualSublogIdx, header, replayContext, shardedLogPreprocessKey, stringContext, objectContext, unifiedContext, entryPtr, length, asReplica);
            else
                return ReplayOp(virtualSublogIdx, header, replayContext, singleLogPreprocessKey, stringContext, objectContext, unifiedContext, entryPtr, length, asReplica);
        }

        private bool ReplayOp<TPreprocessKey, TStringContext, TObjectContext, TUnifiedContext>(
                int virtualSublogIdx,
                AofHeader header,
                AofReplayContext replayContext,
                TPreprocessKey preprocessKey,
                TStringContext stringContext,
                TObjectContext objectContext,
                TUnifiedContext unifiedContext,
                byte* entryPtr,
                int length,
                bool asReplica)
            where TPreprocessKey : IPreprocessKey
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            // StoreRMW can queue VADDs onto different threads
            // but everything else needs to WAIT for those to complete
            // otherwise we might loose consistency
            if (header.opType != AofEntryType.StoreRMW)
            {
                activeVectorManager.WaitForVectorOperationsToComplete();
            }

            // Skips (1) entries with versions that were part of prior checkpoint; and (2) future entries in fuzzy region
            if (SkipRecord(virtualSublogIdx, replayContext.inFuzzyRegion, entryPtr, length, asReplica))
                return false;

            var bufferPtr = (byte*)Unsafe.AsPointer(ref replayContext.objectOutputBuffer[0]);
            var bufferLength = replayContext.objectOutputBuffer.Length;
            preprocessKey.PrepareKey(virtualSublogIdx, entryPtr, out var preparedParameters);
            switch (header.opType)
            {
                case AofEntryType.StoreUpsert:
                    StoreUpsert(preparedParameters, stringContext, ref replayContext.parseState);
                    break;
                case AofEntryType.StoreRMW:
                    StoreRMW(preparedParameters, stringContext, ref replayContext.parseState, activeVectorManager, activeRangeIndexManager, replayContext.respServerSession, obtainServerSession);
                    break;
                case AofEntryType.StoreDelete:
                    StoreDelete(preparedParameters, stringContext);
                    break;
                case AofEntryType.ObjectStoreUpsert:
                    ObjectStoreUpsert(preparedParameters, objectContext, storeWrapper.GarnetObjectSerializer, bufferPtr, bufferLength);
                    break;
                case AofEntryType.ObjectStoreRMW:
                    ObjectStoreRMW(preparedParameters, objectContext, ref replayContext.parseState, bufferPtr, bufferLength);
                    break;
                case AofEntryType.ObjectStoreDelete:
                    ObjectStoreDelete(preparedParameters, objectContext);
                    break;
                case AofEntryType.UnifiedStoreStringUpsert:
                    UnifiedStoreStringUpsert(preparedParameters, unifiedContext, ref replayContext.parseState, bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreRMW:
                    UnifiedStoreRMW(preparedParameters, unifiedContext, ref replayContext.parseState, bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreObjectUpsert:
                    UnifiedStoreObjectUpsert(preparedParameters, unifiedContext, storeWrapper.GarnetObjectSerializer, bufferPtr, bufferLength);
                    break;
                case AofEntryType.UnifiedStoreDelete:
                    UnifiedStoreDelete(preparedParameters, unifiedContext, activeVectorManager, replayContext.respServerSession.storageSession);
                    break;
                default:
                    throw new GarnetException($"Unknown AOF header operation type {header.opType}");
            }

            return true;
        }

        static void StoreUpsert<TStringContext>(PreparedParameters preparedParameters, TStringContext stringContext, ref SessionParseState parseState)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = preparedParameters.PayloadPtr;
            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += value.TotalSize;

            var stringInput = new StringInput { parseState = parseState };
            _ = stringInput.DeserializeFrom(curr);

            StringOutput output = default;
            var upsertOptions = new UpsertOptions() { KeyHash = preparedParameters.KeyHash };
            _ = stringContext.Upsert((FixedSpanByteKey)preparedParameters.Key, ref stringInput, value, ref output, ref upsertOptions);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void StoreRMW<TStringContext>(
            PreparedParameters preparedParameters,
            TStringContext stringContext,
            ref SessionParseState parseState,
            VectorManager vectorManager,
            RangeIndexManager rangeIndexManager,
            RespServerSession activeServerSession,
            Func<RespServerSession> obtainServerSession)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = preparedParameters.PayloadPtr;
            var stringInput = new StringInput { parseState = parseState };
            _ = stringInput.DeserializeFrom(curr);

            // VADD requires special handling, shove it over to the VectorManager
            if (stringInput.header.cmd == RespCommand.VADD)
            {
                vectorManager.HandleVectorSetAddReplication(activeServerSession.storageSession, obtainServerSession, preparedParameters.Key, ref stringInput);
                return;
            }
            else
            {
                // Any other op (include other vector ops) need to wait for pending VADDs to complete
                vectorManager.WaitForVectorOperationsToComplete();

                // VREM is also read-like, so requires special handling - shove it over to the VectorManager
                if (stringInput.header.cmd == RespCommand.VREM)
                {
                    vectorManager.HandleVectorSetRemoveReplication(activeServerSession.storageSession, preparedParameters.Key, ref stringInput);
                    return;
                }
            }

            // RangeIndex commands need actual execution on replay
            if (stringInput.header.cmd == RespCommand.RICREATE)
            {
                rangeIndexManager?.HandleRangeIndexCreateReplay(activeServerSession.storageSession, preparedParameters.Key, ref stringInput);
                return;
            }
            if (stringInput.header.cmd == RespCommand.RISET)
            {
                rangeIndexManager.HandleRangeIndexSetReplay(activeServerSession.storageSession, preparedParameters.Key, ref stringInput);
                return;
            }
            if (stringInput.header.cmd == RespCommand.RIDEL)
            {
                rangeIndexManager.HandleRangeIndexDelReplay(activeServerSession.storageSession, preparedParameters.Key, ref stringInput);
                return;
            }

            var output = StringOutput.FromPinnedSpan(stackalloc byte[32]);
            var rmwOptions = new RMWOptions { KeyHash = preparedParameters.KeyHash };
            var status = stringContext.RMW((FixedSpanByteKey)preparedParameters.Key, ref stringInput, ref output, ref rmwOptions);
            if (status.IsPending)
                StorageSession.CompletePendingForSession(ref status, ref output, ref stringContext);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void StoreDelete<TStringContext>(PreparedParameters preparedParameters, TStringContext stringContext)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            => stringContext.Delete((FixedSpanByteKey)preparedParameters.Key);

        static void ObjectStoreUpsert<TObjectContext>(PreparedParameters preparedParameters, TObjectContext objectContext, GarnetObjectSerializer garnetObjectSerializer, byte* outputPtr, int outputLength)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = preparedParameters.PayloadPtr;
            var valueSpan = SpanByte.FromLengthPrefixedPinnedPointer(curr);
            var valueObject = garnetObjectSerializer.Deserialize(valueSpan.ToArray()); // TODO native deserializer to avoid alloc and copy

            var output = ObjectOutput.FromPinnedPointer(outputPtr, outputLength);
            var upsertOptions = new UpsertOptions() { KeyHash = preparedParameters.KeyHash };
            _ = objectContext.Upsert((FixedSpanByteKey)preparedParameters.Key, valueObject, ref upsertOptions);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void ObjectStoreRMW<TObjectContext>(PreparedParameters preparedParameters, TObjectContext objectContext, ref SessionParseState parseState, byte* outputPtr, int outputLength)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = preparedParameters.PayloadPtr;

            var objectInput = new ObjectInput { parseState = parseState };
            _ = objectInput.DeserializeFrom(curr);

            // Call RMW with the reconstructed key & ObjectInput
            var output = ObjectOutput.FromPinnedPointer(outputPtr, outputLength);
            var rmwOptions = new RMWOptions { KeyHash = preparedParameters.KeyHash };
            var status = objectContext.RMW((FixedSpanByteKey)preparedParameters.Key, ref objectInput, ref output, ref rmwOptions);
            if (status.IsPending)
                StorageSession.CompletePendingForObjectStoreSession(ref status, ref output, ref objectContext);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void ObjectStoreDelete<TObjectContext>(PreparedParameters preparedParameters, TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => objectContext.Delete((FixedSpanByteKey)preparedParameters.Key);

        static void UnifiedStoreStringUpsert<TUnifiedContext>(PreparedParameters preparedParameters, TUnifiedContext unifiedContext, ref SessionParseState parseState, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = preparedParameters.PayloadPtr;

            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += value.TotalSize;

            var unifiedInput = new UnifiedInput { parseState = parseState };
            _ = unifiedInput.DeserializeFrom(curr);

            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            var upsertOptions = new UpsertOptions() { KeyHash = preparedParameters.KeyHash };
            _ = unifiedContext.Upsert((FixedSpanByteKey)preparedParameters.Key, ref unifiedInput, value, ref output, ref upsertOptions);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void UnifiedStoreObjectUpsert<TUnifiedContext>(PreparedParameters preparedParameters, TUnifiedContext unifiedContext, GarnetObjectSerializer garnetObjectSerializer, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = preparedParameters.PayloadPtr;

            var valueSpan = SpanByte.FromLengthPrefixedPinnedPointer(curr);
            var valueObject = garnetObjectSerializer.Deserialize(valueSpan.ToArray()); // TODO native deserializer to avoid alloc and copy

            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            var upsertOptions = new UpsertOptions() { KeyHash = preparedParameters.KeyHash };
            _ = unifiedContext.Upsert((FixedSpanByteKey)preparedParameters.Key, valueObject, ref upsertOptions);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void UnifiedStoreRMW<TUnifiedContext>(PreparedParameters preparedParameters, TUnifiedContext unifiedContext, ref SessionParseState parseState, byte* outputPtr, int outputLength)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = preparedParameters.PayloadPtr;
            var unifiedInput = new UnifiedInput { parseState = parseState };
            _ = unifiedInput.DeserializeFrom(curr);

            // Call RMW with the reconstructed key & UnifiedInput
            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            var rmwOptions = new RMWOptions { KeyHash = preparedParameters.KeyHash };
            var status = unifiedContext.RMW((FixedSpanByteKey)preparedParameters.Key, ref unifiedInput, ref output, ref rmwOptions);
            if (status.IsPending)
                StorageSession.CompletePendingForUnifiedStoreSession(ref status, ref output, ref unifiedContext);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        static void UnifiedStoreDelete<TUnifiedContext>(PreparedParameters preparedParameters, TUnifiedContext unifiedContext, VectorManager vectorManager, StorageSession storageSession)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var res = unifiedContext.Delete((FixedSpanByteKey)preparedParameters.Key);

            if (res.IsCanceled)
            {
                // Might be a vector set
                res = vectorManager.TryDeleteVectorSet(storageSession, preparedParameters.Key, out _);
                if (res.IsPending)
                    _ = unifiedContext.CompletePending(true);
            }
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
        /// Check if the calling parallel replay task should replay this entry
        /// </summary>
        /// <param name="ptr"></param>
        /// <param name="replayTaskIdx"></param>
        /// <param name="sequenceNumber"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        public bool CanReplay(byte* ptr, int replayTaskIdx, out long sequenceNumber)
        {
            var header = *(AofHeader*)ptr;
            var replayHeaderType = header.HeaderType;
            sequenceNumber = 0L;
            switch (replayHeaderType)
            {
                // Check if should replay entry by inspecting key
                case AofHeaderType.ShardedHeader:
                    var shardedHeader = *(AofShardedHeader*)ptr;
                    sequenceNumber = shardedHeader.sequenceNumber;
                    var curr = AofHeader.SkipHeader(ptr);
                    var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr).ReadOnlySpan;
                    var _replayTaskIdx = storeWrapper.appendOnlyFile.Log.GetReplayTaskIdx(key);
                    return replayTaskIdx == _replayTaskIdx;
                // If no key to inspect, check bit vector for participating replay tasks in the transaction
                // NOTE: HeaderType transactions include MULTI-EXEC transactions, custom txn procedures, and any operation that executes across physical and virtual sublogs (e.g. checkpoint, flushdb)
                case AofHeaderType.TransactionHeader:
                    var txnHeader = *(AofTransactionHeader*)ptr;
                    sequenceNumber = txnHeader.shardedHeader.sequenceNumber;
                    var bitVector = BitVector.CopyFrom(new Span<byte>(txnHeader.replayTaskAccessVector, AofTransactionHeader.ReplayTaskAccessVectorBytes));
                    return bitVector.IsSet(replayTaskIdx);
                default:
                    throw new GarnetException($"Replay header type {replayHeaderType} not supported!");
            }
        }

        /// <summary>
        /// Calculates the index of the replay task associated with the specified AOF header pointer.
        /// </summary>
        /// <param name="ptr">A pointer to a byte array representing the AOF header.</param>
        /// <returns>The zero-based index of the replay task to which the entry should be assigned. Returns -1 if the header type
        /// does not contain a key for task assignment.</returns>
        /// <exception cref="GarnetException">Thrown when the AOF header type referenced by <paramref name="ptr"/> is not supported.</exception>
        public int GetReplayTaskIdx(byte* ptr)
        {
            var header = *(AofHeader*)ptr;
            var replayHeaderType = header.HeaderType;
            switch (replayHeaderType)
            {
                // Check if should replay entry by inspecting key
                case AofHeaderType.ShardedHeader:
                    var shardedHeader = *(AofShardedHeader*)ptr;
                    var curr = AofHeader.SkipHeader(ptr);
                    var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr).ReadOnlySpan;
                    var _replayTaskIdx = storeWrapper.appendOnlyFile.Log.GetReplayTaskIdx(key);
                    return _replayTaskIdx;
                // If no key to inspect, check bit vector for participating replay tasks in the transaction
                // NOTE: HeaderType transactions include MULTI-EXEC transactions, custom txn procedures, and any operation that executes across physical and virtual sublogs (e.g. checkpoint, flushdb)
                case AofHeaderType.TransactionHeader:
                    return -1;
                default:
                    throw new GarnetException($"Replay header type {replayHeaderType} not supported!");
            }
        }

        /// <summary>
        /// Determines whether the specified log entry should be skipped during replay based on its sequence number.
        /// </summary>
        /// <param name="ptr">A pointer to the start of the log entry header in memory. Must point to a valid header structure.</param>
        /// <param name="untilSequenceNumber">The sequence number threshold. Entries with a sequence number greater than this value will be skipped.
        /// Specify -1 to skip all entries.</param>
        /// <param name="entrySequenceNumber">When this method returns, contains the sequence number of the current log entry, or -1 if unavailable.</param>
        /// <returns>true if the log entry should be skipped; otherwise, false.</returns>
        /// <exception cref="GarnetException">Thrown if the log entry header type is not supported.</exception>
        public bool SkipReplay(byte* ptr, long untilSequenceNumber, out long entrySequenceNumber)
        {
            entrySequenceNumber = -1;
            if (untilSequenceNumber == -1)
                return true;
            var header = *(AofHeader*)ptr;
            var replayHeaderType = header.HeaderType;
            switch (replayHeaderType)
            {
                case AofHeaderType.ShardedHeader:
                    var shardedHeader = *(AofShardedHeader*)ptr;
                    entrySequenceNumber = shardedHeader.sequenceNumber;
                    return shardedHeader.sequenceNumber > untilSequenceNumber;
                case AofHeaderType.TransactionHeader:
                    var txnHeader = *(AofTransactionHeader*)ptr;
                    entrySequenceNumber = txnHeader.shardedHeader.sequenceNumber;
                    return txnHeader.shardedHeader.sequenceNumber > untilSequenceNumber;
                default:
                    throw new GarnetException($"Replay header type {replayHeaderType} not supported!");
            }
        }
    }
}