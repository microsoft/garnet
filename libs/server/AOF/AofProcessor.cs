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
        public abstract unsafe void PrepareKey(int virtualSublogIdx, byte* entryPtr, long logAddressSequenceNumber, out PreparedParameters preparedParameters);
    }

    struct SingleLogPreprocessKey : IPreprocessKey
    {
        public unsafe void PrepareKey(int virtualSublogIdx, byte* entryPtr, long logAddressSequenceNumber, out PreparedParameters preparedParameters)
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

    struct SinglePhysicalLogPreprocessKey : IPreprocessKey
    {
        public GarnetAppendOnlyFile appendOnlyFile;

        public unsafe void PrepareKey(int virtualSublogIdx, byte* entryPtr, long logAddressSequenceNumber, out PreparedParameters preparedParameters)
        {
            // Single physical log + multi-replay: entries use BasicHeader, ordering via log address
            var keyPtr = entryPtr + sizeof(AofHeader);
            preparedParameters = new()
            {
                Key = SpanByte.FromLengthPrefixedPinnedPointer(keyPtr)
            };
            preparedParameters.KeyHash = GarnetLog.HASH(preparedParameters.Key);
            preparedParameters.PayloadPtr = keyPtr + preparedParameters.Key.TotalSize();
            Debug.Assert(logAddressSequenceNumber > 0, "Entry address must be positive for single-physical-log consistency updates");
            appendOnlyFile.readConsistencyManager.UpdateVirtualSublogKeySequenceNumber(
                virtualSublogIdx,
                preparedParameters.KeyHash,
                logAddressSequenceNumber);
        }
    }

    struct ShardedLogPreprocessKey : IPreprocessKey
    {
        public GarnetAppendOnlyFile appendOnlyFile;

        public unsafe void PrepareKey(int virtualSublogIdx, byte* entryPtr, long logAddressSequenceNumber, out PreparedParameters preparedParameters)
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
        readonly bool usingSinglePhysicalLogMultiReplay;
        SingleLogPreprocessKey singleLogPreprocessKey;
        SinglePhysicalLogPreprocessKey singlePhysicalLogPreprocessKey;
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
            this.usingSinglePhysicalLogMultiReplay = storeWrapper.serverOptions.AofPhysicalSublogCount == 1 && storeWrapper.serverOptions.AofReplayTaskCount > 1;
            if (storeWrapper.serverOptions.AofPhysicalSublogCount > 1)
                this.shardedLogPreprocessKey = new ShardedLogPreprocessKey() { appendOnlyFile = storeWrapper.appendOnlyFile };
            else if (usingSinglePhysicalLogMultiReplay)
                this.singlePhysicalLogPreprocessKey = new SinglePhysicalLogPreprocessKey() { appendOnlyFile = storeWrapper.appendOnlyFile };
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
            activeRangeIndexManager?.DisposeIncompleteStreamReassembly();
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
        /// Extracts sequence number and participant count from a transaction header entry.
        /// For single-physical-log + multi-replay, uses entry address; for multi-physical-log, uses embedded sequence number.
        /// </summary>
        void GetSynchronizedOperationParams(byte* ptr, long entryAddress, out long sequenceNumber, out short participantCount)
        {
            var headerType = (*(AofHeader*)ptr).HeaderType;
            switch (headerType)
            {
                case AofHeaderType.SingleLogTransactionHeader:
                    sequenceNumber = entryAddress;
                    participantCount = (*(AofSingleLogTransactionHeader*)ptr).participantCount;
                    break;
                case AofHeaderType.ShardedLogTransactionHeader:
                    var txnHeader = *(AofShardedLogTransactionHeader*)ptr;
                    sequenceNumber = txnHeader.shardedHeader.sequenceNumber;
                    participantCount = txnHeader.participantCount;
                    break;
                case AofHeaderType.BasicHeader:
                    sequenceNumber = entryAddress;
                    participantCount = (short)storeWrapper.serverOptions.AofReplayTaskCount;
                    break;
                case AofHeaderType.ShardedHeader:
                    sequenceNumber = (*(AofShardedHeader*)ptr).sequenceNumber;
                    participantCount = (short)storeWrapper.serverOptions.AofReplayTaskCount;
                    break;
                default:
                    throw new GarnetException($"Unsupported header type: {headerType}");
            }
        }

        /// <summary>
        /// Process AOF record internal
        /// NOTE: This method is shared between recover replay and replication replay
        /// </summary>
        /// <param name="virtualSublogIdx"></param>
        /// <param name="ptr"></param>
        /// <param name="length"></param>
        /// <param name="asReplica"></param>
        /// <param name="isCheckpointStart"></param>
        /// <param name="logAddressSequenceNumber"></param>
        public void ProcessAofRecordInternal(int virtualSublogIdx, byte* ptr, int length, bool asReplica, out bool isCheckpointStart, long logAddressSequenceNumber = 0)
        {
            var header = *(AofHeader*)ptr;

            // Reject entries written by a newer Garnet version (higher AOF header version) rather than
            // silently mis-interpreting them under this version's format.
            if (header.aofHeaderVersion > AofHeader.MaxSupportedAofHeaderVersion)
                throw new GarnetException($"Unsupported AOF header version {header.aofHeaderVersion}; this build supports up to version {AofHeader.MaxSupportedAofHeaderVersion}. The data may have been written by a newer Garnet version.");

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
            if (aofReplayCoordinator.AddOrReplayTransactionOperation(virtualSublogIdx, ptr, length, asReplica, logAddressSequenceNumber))
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
                        // For single-physical-log + multi-replay, use entry address; otherwise use embedded sequence number
                        var checkpointSequenceNumber = usingSinglePhysicalLogMultiReplay ? logAddressSequenceNumber : (*(AofShardedHeader*)ptr).sequenceNumber;
                        storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, checkpointSequenceNumber);
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
                                    GetSynchronizedOperationParams(ptr, logAddressSequenceNumber, out var seqNum, out var partCount);
                                    aofReplayCoordinator.ProcessSynchronizedOperation(
                                        virtualSublogIdx,
                                        seqNum,
                                        partCount,
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
                            GetSynchronizedOperationParams(ptr, logAddressSequenceNumber, out var seqNum, out var partCount);
                            aofReplayCoordinator.ProcessSynchronizedOperation(
                                virtualSublogIdx,
                                seqNum,
                                partCount,
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
                        // For single-physical-log + multi-replay, use entry address; otherwise use embedded sequence number
                        var streamingSequenceNumber = usingSinglePhysicalLogMultiReplay ? logAddressSequenceNumber : (*(AofShardedHeader*)ptr).sequenceNumber;
                        storeWrapper.appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, streamingSequenceNumber);
                    }
                    break;
                case AofEntryType.FlushAll:
                    if (!usingShardedLog)
                    {
                        storeWrapper.FlushAllDatabases(unsafeTruncateLog: header.UnsafeTruncateLog);
                    }
                    else
                    {
                        GetSynchronizedOperationParams(ptr, logAddressSequenceNumber, out var seqNum, out var partCount);
                        aofReplayCoordinator.ProcessSynchronizedOperation(
                            virtualSublogIdx,
                            seqNum,
                            partCount,
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
                        GetSynchronizedOperationParams(ptr, logAddressSequenceNumber, out var seqNum, out var partCount);
                        aofReplayCoordinator.ProcessSynchronizedOperation(
                            virtualSublogIdx,
                            seqNum,
                            partCount,
                            (int)LeaderBarrierType.FLUSH_DB,
                            () => { storeWrapper.FlushDatabase(unsafeTruncateLog: header.UnsafeTruncateLog, dbId: header.databaseId); return Task.CompletedTask; }
                        );
                    }
                    break;
                case AofEntryType.StoredProcedure:
                    aofReplayCoordinator.ReplayStoredProc(virtualSublogIdx, header.procedureId, ptr, logAddressSequenceNumber);
                    break;
                case AofEntryType.TxnCommit:
                    aofReplayCoordinator.ProcessFuzzyRegionTransactionGroup(virtualSublogIdx, ptr, asReplica, logAddressSequenceNumber);
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
                        asReplica,
                        logAddressSequenceNumber);
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
                bool asReplica,
                long logAddressSequenceNumber = 0)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            if (storeWrapper.serverOptions.AofPhysicalSublogCount > 1)
                return ReplayOp(virtualSublogIdx, header, replayContext, shardedLogPreprocessKey, stringContext, objectContext, unifiedContext, entryPtr, length, asReplica, logAddressSequenceNumber);
            else if (usingSinglePhysicalLogMultiReplay)
                return ReplayOp(virtualSublogIdx, header, replayContext, singlePhysicalLogPreprocessKey, stringContext, objectContext, unifiedContext, entryPtr, length, asReplica, logAddressSequenceNumber);
            else
                return ReplayOp(virtualSublogIdx, header, replayContext, singleLogPreprocessKey, stringContext, objectContext, unifiedContext, entryPtr, length, asReplica, logAddressSequenceNumber);
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
                bool asReplica,
                long logAddressSequenceNumber)
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
            preprocessKey.PrepareKey(virtualSublogIdx, entryPtr, logAddressSequenceNumber, out var preparedParameters);

            // Entries written before AOF header version 4 used the legacy RespCommand numbering
            // (reads-first) and packed the object sub-operation id into the low 5 bits of the flags
            // byte. Translate those on replay: remap header.cmd for string/unified entries and
            // relocate the sub-op id for object RMW entries.
            var legacyCmdFormat = header.aofHeaderVersion < 4;
            switch (header.opType)
            {
                case AofEntryType.StoreUpsert:
                    StoreUpsert(preparedParameters, stringContext, ref replayContext.parseState, legacyCmdFormat);
                    break;
                case AofEntryType.StoreRMW:
                    StoreRMW(preparedParameters, stringContext, ref replayContext.parseState, activeVectorManager, activeRangeIndexManager, replayContext.respServerSession, obtainServerSession, legacyCmdFormat);
                    break;
                case AofEntryType.RangeIndexStreamChunk:
                    HandleRangeIndexStreamChunk(preparedParameters, ref replayContext.parseState, activeRangeIndexManager, replayContext.respServerSession);
                    break;
                case AofEntryType.StoreDelete:
                    StoreDelete(preparedParameters, stringContext);
                    break;
                case AofEntryType.ObjectStoreUpsert:
                    ObjectStoreUpsert(preparedParameters, objectContext, storeWrapper.GarnetObjectSerializer, bufferPtr, bufferLength);
                    break;
                case AofEntryType.ObjectStoreRMW:
                    ObjectStoreRMW(preparedParameters, objectContext, ref replayContext.parseState, bufferPtr, bufferLength, legacyCmdFormat);
                    break;
                case AofEntryType.ObjectStoreDelete:
                    ObjectStoreDelete(preparedParameters, objectContext);
                    break;
                case AofEntryType.UnifiedStoreStringUpsert:
                    UnifiedStoreStringUpsert(preparedParameters, unifiedContext, ref replayContext.parseState, bufferPtr, bufferLength, legacyCmdFormat);
                    break;
                case AofEntryType.UnifiedStoreRMW:
                    UnifiedStoreRMW(preparedParameters, unifiedContext, ref replayContext.parseState, bufferPtr, bufferLength, legacyCmdFormat);
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

        static void StoreUpsert<TStringContext>(PreparedParameters preparedParameters, TStringContext stringContext, ref SessionParseState parseState, bool legacyCmdFormat)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = preparedParameters.PayloadPtr;
            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += value.TotalSize;

            var stringInput = new StringInput { parseState = parseState };
            _ = stringInput.DeserializeFrom(curr);
            if (legacyCmdFormat)
                stringInput.header.cmd = LegacyRespCommand.FromV3(stringInput.header.cmd);

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
            Func<RespServerSession> obtainServerSession,
            bool legacyCmdFormat)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = preparedParameters.PayloadPtr;
            var stringInput = new StringInput { parseState = parseState };
            _ = stringInput.DeserializeFrom(curr);
            if (legacyCmdFormat)
                stringInput.header.cmd = LegacyRespCommand.FromV3(stringInput.header.cmd);

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
                if (rangeIndexManager == null)
                    ExceptionUtils.ThrowException(new GarnetException("RangeIndexPreview disabled; Replay failed"));
                rangeIndexManager.HandleRangeIndexCreateReplay(activeServerSession.storageSession, preparedParameters.Key, ref stringInput);
                return;
            }
            if (stringInput.header.cmd == RespCommand.RISET)
            {
                if (rangeIndexManager == null)
                    ExceptionUtils.ThrowException(new GarnetException("RangeIndexPreview disabled; Replay failed"));
                rangeIndexManager.HandleRangeIndexSetReplay(activeServerSession.storageSession, preparedParameters.Key, ref stringInput);
                return;
            }
            if (stringInput.header.cmd == RespCommand.RIDEL)
            {
                if (rangeIndexManager == null)
                    ExceptionUtils.ThrowException(new GarnetException("RangeIndexPreview disabled; Replay failed"));
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

        static void HandleRangeIndexStreamChunk(PreparedParameters preparedParameters, ref SessionParseState parseState, RangeIndexManager rangeIndexManager, RespServerSession activeServerSession)
        {
            if (!activeServerSession.StoreWrapper.serverOptions.EnableRangeIndexPreview)
                ExceptionUtils.ThrowException(new GarnetException("RangeIndexPreview disabled; Replay failed"));

            var stringInput = new StringInput { parseState = parseState };
            _ = stringInput.DeserializeFrom(preparedParameters.PayloadPtr);

            rangeIndexManager.HandleRangeIndexStreamReplay(activeServerSession.storageSession, preparedParameters.Key, ref stringInput);
        }

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

        static void ObjectStoreRMW<TObjectContext>(PreparedParameters preparedParameters, TObjectContext objectContext, ref SessionParseState parseState, byte* outputPtr, int outputLength, bool legacyCmdFormat)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = preparedParameters.PayloadPtr;

            var objectInput = new ObjectInput { parseState = parseState };
            _ = objectInput.DeserializeFrom(curr);
            if (legacyCmdFormat)
                RelocateLegacyObjectSubId(ref objectInput.header);

            // Call RMW with the reconstructed key & ObjectInput
            var output = ObjectOutput.FromPinnedPointer(outputPtr, outputLength);
            var rmwOptions = new RMWOptions { KeyHash = preparedParameters.KeyHash };
            var status = objectContext.RMW((FixedSpanByteKey)preparedParameters.Key, ref objectInput, ref output, ref rmwOptions);
            if (status.IsPending)
                StorageSession.CompletePendingForObjectStoreSession(ref status, ref output, ref objectContext);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Dispose();
        }

        /// <summary>
        /// Pre-v4 object entries packed the sub-operation id into the low 5 bits of the flags byte
        /// (RespInputHeader byte 2), with byte 1 unused. v4 moves the sub-op id into its own byte
        /// (byte 1). When replaying a legacy object header, split the flags byte: low 5 bits become
        /// the sub-op id, high 3 bits remain RespInputFlags.
        /// </summary>
        static void RelocateLegacyObjectSubId(ref RespInputHeader header)
        {
            const byte LegacySubIdMask = 0x1F; // low 5 bits held the sub-op id
            var legacyFlags = (byte)header.flags;
            header.subId = (byte)(legacyFlags & LegacySubIdMask);
            header.flags = (RespInputFlags)(legacyFlags & ~LegacySubIdMask);
        }

        static void ObjectStoreDelete<TObjectContext>(PreparedParameters preparedParameters, TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => objectContext.Delete((FixedSpanByteKey)preparedParameters.Key);

        static void UnifiedStoreStringUpsert<TUnifiedContext>(PreparedParameters preparedParameters, TUnifiedContext unifiedContext, ref SessionParseState parseState, byte* outputPtr, int outputLength, bool legacyCmdFormat)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = preparedParameters.PayloadPtr;

            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += value.TotalSize;

            var unifiedInput = new UnifiedInput { parseState = parseState };
            _ = unifiedInput.DeserializeFrom(curr);
            if (legacyCmdFormat)
                unifiedInput.header.cmd = LegacyRespCommand.FromV3(unifiedInput.header.cmd);

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

        static void UnifiedStoreRMW<TUnifiedContext>(PreparedParameters preparedParameters, TUnifiedContext unifiedContext, ref SessionParseState parseState, byte* outputPtr, int outputLength, bool legacyCmdFormat)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var curr = preparedParameters.PayloadPtr;
            var unifiedInput = new UnifiedInput { parseState = parseState };
            _ = unifiedInput.DeserializeFrom(curr);
            if (legacyCmdFormat)
                unifiedInput.header.cmd = LegacyRespCommand.FromV3(unifiedInput.header.cmd);

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
        => unifiedContext.Delete((FixedSpanByteKey)preparedParameters.Key);

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
        /// <param name="entryAddress">Log address of the entry, used as ordering value for single-physical-log parallel replay mode</param>
        /// <param name="logAddressSequenceNumber"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        public bool CanReplay(byte* ptr, int replayTaskIdx, long entryAddress, out long logAddressSequenceNumber)
        {
            var header = *(AofHeader*)ptr;
            var replayHeaderType = header.HeaderType;
            logAddressSequenceNumber = 0L;
            switch (replayHeaderType)
            {
                // Single-physical-log + multi-replay: BasicHeader entries, use entry address for ordering
                case AofHeaderType.BasicHeader:
                    logAddressSequenceNumber = entryAddress;
                    // Keyless entries (transactions, checkpoints, flush, stored procedures) are processed by all tasks
                    // because they may participate in barriers via ProcessSynchronizedOperation
                    if (!header.opType.HasKey())
                        return true;
                    var basicCurr = AofHeader.SkipHeader(ptr);
                    var basicKey = PinnedSpanByte.FromLengthPrefixedPinnedPointer(basicCurr).ReadOnlySpan;
                    return replayTaskIdx == storeWrapper.appendOnlyFile.Log.GetReplayTaskIdx(basicKey);
                // Multi-physical-log: ShardedHeader entries with embedded sequence number
                case AofHeaderType.ShardedHeader:
                    var shardedHeader = *(AofShardedHeader*)ptr;
                    logAddressSequenceNumber = shardedHeader.sequenceNumber;
                    // Keyless entries are processed by task 0 only
                    if (!header.opType.HasKey())
                        return replayTaskIdx == 0;
                    var curr = AofHeader.SkipHeader(ptr);
                    var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr).ReadOnlySpan;
                    return replayTaskIdx == storeWrapper.appendOnlyFile.Log.GetReplayTaskIdx(key);
                // Single-physical-log + multi-replay: transaction header without sequence number
                case AofHeaderType.SingleLogTransactionHeader:
                    var singleLogTxnHeader = *(AofSingleLogTransactionHeader*)ptr;
                    logAddressSequenceNumber = entryAddress;
                    var singleLogBitVector = BitVector.CopyFrom(new Span<byte>(singleLogTxnHeader.replayTaskAccessVector, AofShardedLogTransactionHeader.ReplayTaskAccessVectorBytes));
                    return singleLogBitVector.IsSet(replayTaskIdx);
                // Multi-physical-log: transaction header with embedded sequence number
                case AofHeaderType.ShardedLogTransactionHeader:
                    var txnHeader = *(AofShardedLogTransactionHeader*)ptr;
                    logAddressSequenceNumber = txnHeader.shardedHeader.sequenceNumber;
                    var bitVector = BitVector.CopyFrom(new Span<byte>(txnHeader.replayTaskAccessVector, AofShardedLogTransactionHeader.ReplayTaskAccessVectorBytes));
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
                // Single-physical-log + multi-replay: BasicHeader entries
                case AofHeaderType.BasicHeader:
                    var basicCurr = AofHeader.SkipHeader(ptr);
                    var basicKey = PinnedSpanByte.FromLengthPrefixedPinnedPointer(basicCurr).ReadOnlySpan;
                    return storeWrapper.appendOnlyFile.Log.GetReplayTaskIdx(basicKey);
                // Multi-physical-log: ShardedHeader entries
                case AofHeaderType.ShardedHeader:
                    var curr = AofHeader.SkipHeader(ptr);
                    var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr).ReadOnlySpan;
                    return storeWrapper.appendOnlyFile.Log.GetReplayTaskIdx(key);
                // Transaction headers (both types) don't have a single key for task assignment
                case AofHeaderType.ShardedLogTransactionHeader:
                case AofHeaderType.SingleLogTransactionHeader:
                    return -1;
                default:
                    throw new GarnetException($"Replay header type {replayHeaderType} not supported!");
            }
        }

        /// <summary>
        /// Determines whether the specified log entry should be skipped during replay based on its sequence number or address.
        /// </summary>
        /// <param name="ptr">A pointer to the start of the log entry header in memory.</param>
        /// <param name="untilSequenceNumber">The sequence number/address threshold. Entries beyond this value will be skipped.
        /// Specify -1 to skip all entries.</param>
        /// <param name="logAddressSequenceNumber">Log address of the entry, used for single-physical-log mode.</param>
        /// <param name="sequenceNumber">When this method returns, contains the sequence number/address of the current log entry, or -1 if unavailable.</param>
        /// <returns>true if the log entry should be skipped; otherwise, false.</returns>
        /// <exception cref="GarnetException">Thrown if the log entry header type is not supported.</exception>
        public bool SkipReplay(byte* ptr, long untilSequenceNumber, long logAddressSequenceNumber, out long sequenceNumber)
        {
            sequenceNumber = -1;
            if (untilSequenceNumber == -1)
                return true;
            var header = *(AofHeader*)ptr;
            var replayHeaderType = header.HeaderType;
            switch (replayHeaderType)
            {
                // Single-physical-log + multi-replay: use entry address
                case AofHeaderType.BasicHeader:
                case AofHeaderType.SingleLogTransactionHeader:
                    sequenceNumber = logAddressSequenceNumber;
                    return logAddressSequenceNumber > untilSequenceNumber;
                // Multi-physical-log: use embedded sequence number
                case AofHeaderType.ShardedHeader:
                    var shardedHeader = *(AofShardedHeader*)ptr;
                    sequenceNumber = shardedHeader.sequenceNumber;
                    return shardedHeader.sequenceNumber > untilSequenceNumber;
                case AofHeaderType.ShardedLogTransactionHeader:
                    var txnHeader = *(AofShardedLogTransactionHeader*)ptr;
                    sequenceNumber = txnHeader.shardedHeader.sequenceNumber;
                    return txnHeader.shardedHeader.sequenceNumber > untilSequenceNumber;
                default:
                    throw new GarnetException($"Replay header type {replayHeaderType} not supported!");
            }
        }
    }
}