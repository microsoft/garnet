// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Garnet.common;
using Garnet.networking;
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
        readonly RespServerSession respServerSession;

        private readonly StringInput stringInput;
        private readonly ObjectInput objectInput;
        private readonly UnifiedInput unifiedInput;
        private readonly CustomProcedureInput customProcInput;
        private readonly SessionParseState parseState;

        int activeDbId;

        /// <summary>
        /// Set ReadWriteSession on the cluster session (NOTE: used for replaying stored procedures only)
        /// </summary>
        public void SetReadWriteSession() => respServerSession.clusterSession.SetReadWriteSession();

        /// <summary>
        /// Session for main store
        /// </summary>
        BasicContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> stringBasicContext;

        /// <summary>
        /// Session for object store
        /// </summary>
        BasicContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectBasicContext;

        /// <summary>
        /// Session for unified store
        /// </summary>
        BasicContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedBasicContext;

        readonly Dictionary<int, List<byte[]>> inflightTxns;
        readonly byte[] buffer;
        readonly GCHandle handle;
        readonly byte* bufferPtr;

        readonly ILogger logger;

        MemoryResult<byte> output;

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
            this.respServerSession = new RespServerSession(0, networkSender: null, storeWrapper: replayAofStoreWrapper, subscribeBroker: null, authenticator: null, enableScripts: false, clusterProvider: clusterProvider);

            // Switch current contexts to match the default database
            SwitchActiveDatabaseContext(storeWrapper.DefaultDatabase, true);

            parseState.Initialize();
            stringInput.parseState = parseState;
            objectInput.parseState = parseState;
            unifiedInput.parseState = parseState;
            customProcInput.parseState = parseState;

            inflightTxns = [];
            buffer = new byte[BufferSizeUtils.ServerBufferSize(new MaxSizeSettings())];
            handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            bufferPtr = (byte*)handle.AddrOfPinnedObject();
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
                dbSession.StorageSession.stringBasicContext.Session?.Dispose();
                dbSession.StorageSession.objectBasicContext.Session?.Dispose();
                dbSession.StorageSession.unifiedBasicContext.Session?.Dispose();
            }

            handle.Free();
        }

        /// <summary>
        /// Recover store using AOF
        /// </summary>
        /// <param name="db">Database to recover</param>
        /// <param name="untilAddress">Tail address for recovery</param>
        /// <returns>Tail address</returns>
        public long Recover(GarnetDatabase db, long untilAddress = -1)
        {
            logger?.LogInformation("Begin AOF recovery for DB ID: {id}", db.Id);
            return RecoverReplay(db, untilAddress);
        }

        private long RecoverReplay(GarnetDatabase db, long untilAddress)
        {
            // Begin replay for specified database
            logger?.LogInformation("Begin AOF replay for DB ID: {id}", db.Id);
            try
            {
                int count = 0;

                // Fetch the database AOF and update the current database context for the processor
                var appendOnlyFile = db.AppendOnlyFile;
                SwitchActiveDatabaseContext(db);

                // Set the tail address for replay recovery to the tail address of the AOF if none specified
                if (untilAddress == -1) untilAddress = appendOnlyFile.TailAddress;

                // Scan the AOF up to the tail address
                using var scan = appendOnlyFile.Scan(appendOnlyFile.BeginAddress, untilAddress);

                // Replay each AOF record in the current database context
                while (scan.GetNext(MemoryPool<byte>.Shared, out var entry, out var length, out _, out long nextAofAddress))
                {
                    count++;
                    ProcessAofRecord(entry, length);
                    if (count % 100_000 == 0)
                        logger?.LogInformation("Completed AOF replay of {count} records, until AOF address {nextAofAddress} (DB ID: {id})", count, nextAofAddress, db.Id);
                }

                logger?.LogInformation("Completed full AOF log replay of {count} records (DB ID: {id})", count, db.Id);
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
                output.MemoryOwner?.Dispose();
                respServerSession.Dispose();
            }

            return -1;
        }

        internal unsafe void ProcessAofRecord(IMemoryOwner<byte> entry, int length)
        {
            fixed (byte* ptr = entry.Memory.Span)
            {
                ProcessAofRecordInternal(ptr, length, false, out _);
            }
            entry.Dispose();
        }

        /// <summary>
        /// Fuzzy region of AOF is the region between the checkpoint start and end commit markers.
        /// This regions can contain entries in both (v) and (v+1) versions. The processing logic is:
        /// 1) Process (v) entries as is.
        /// 2) Store aware the (v+1) entries in a buffer.
        /// 3) At the end of the fuzzy region, take a checkpoint
        /// 4) Finally, replay the buffered (v+1) entries.
        /// </summary>
        bool inFuzzyRegion = false;
        List<byte[]> fuzzyRegionBuffer = new();

        /// <summary>
        /// Process AOF record
        /// </summary>
        public unsafe void ProcessAofRecordInternal(byte* ptr, int length, bool asReplica, out bool isCheckpointStart)
        {
            AofHeader header = *(AofHeader*)ptr;
            isCheckpointStart = false;

            if (inflightTxns.ContainsKey(header.sessionID))
            {
                switch (header.opType)
                {
                    case AofEntryType.TxnAbort:
                        inflightTxns[header.sessionID].Clear();
                        inflightTxns.Remove(header.sessionID);
                        break;
                    case AofEntryType.TxnCommit:
                        if (inFuzzyRegion)
                        {
                            fuzzyRegionBuffer.Add(new ReadOnlySpan<byte>(ptr, length).ToArray());
                        }
                        else
                        {
                            ProcessTxn(inflightTxns[header.sessionID], asReplica);
                            inflightTxns[header.sessionID].Clear();
                            inflightTxns.Remove(header.sessionID);
                        }
                        break;
                    case AofEntryType.StoredProcedure:
                        throw new GarnetException($"Unexpected AOF header operation type {header.opType} within transaction");
                    default:
                        inflightTxns[header.sessionID].Add(new ReadOnlySpan<byte>(ptr, length).ToArray());
                        break;
                }
                return;
            }

            switch (header.opType)
            {
                case AofEntryType.TxnStart:
                    inflightTxns[header.sessionID] = [];
                    break;
                case AofEntryType.TxnAbort:
                case AofEntryType.TxnCommit:
                    // We encountered a transaction end without start - this could happen because we truncated the AOF
                    // after a checkpoint, and the transaction belonged to the previous version. It can safely
                    // be ignored.
                    break;
                case AofEntryType.CheckpointStartCommit:
                    // Inform caller that we processed a checkpoint start marker so that it can record ReplicationCheckpointStartOffset if this is a replica replay
                    isCheckpointStart = true;
                    if (header.aofHeaderVersion > 1)
                    {
                        if (inFuzzyRegion)
                        {
                            logger?.LogInformation("Encountered new CheckpointStartCommit before prior CheckpointEndCommit. Clearing {fuzzyRegionBufferCount} records from previous fuzzy region", fuzzyRegionBuffer.Count);
                            fuzzyRegionBuffer.Clear();
                        }
                        inFuzzyRegion = true;
                    }
                    else
                    {
                        // We are parsing the old AOF format: take checkpoint immediately as we do not have a fuzzy region
                        // Note: we will not truncate the AOF as ReplicationCheckpointStartOffset is not set
                        // Once a new checkpoint is transferred, the replica will truncate the AOF.
                        if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                            _ = storeWrapper.TakeCheckpoint(false, logger);
                    }
                    break;
                case AofEntryType.ObjectStoreCheckpointStartCommit:
                    // With unified checkpoint, we do not need to handle object store checkpoint separately
                    break;
                case AofEntryType.CheckpointEndCommit:
                    if (header.aofHeaderVersion > 1)
                    {
                        if (!inFuzzyRegion)
                        {
                            logger?.LogInformation("Encountered CheckpointEndCommit without a prior CheckpointStartCommit - ignoring");
                        }
                        else
                        {
                            inFuzzyRegion = false;
                            // Take checkpoint after the fuzzy region
                            if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                                _ = storeWrapper.TakeCheckpoint(false, logger);
                            // Process buffered records
                            if (fuzzyRegionBuffer.Count > 0)
                            {
                                logger?.LogInformation("Replaying {fuzzyRegionBufferCount} records from fuzzy region for checkpoint {newVersion}", fuzzyRegionBuffer.Count, storeWrapper.store.CurrentVersion);
                            }
                            foreach (var entry in fuzzyRegionBuffer)
                            {
                                fixed (byte* entryPtr = entry)
                                    ReplayOp(entryPtr, entry.Length, asReplica);
                            }
                            fuzzyRegionBuffer.Clear();
                        }
                    }
                    break;
                case AofEntryType.ObjectStoreCheckpointEndCommit:
                    // With unified checkpoint, we do not need to handle object store checkpoint separately
                    break;
                case AofEntryType.MainStoreStreamingCheckpointStartCommit:
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                        storeWrapper.store.SetVersion(header.storeVersion);
                    break;
                case AofEntryType.MainStoreStreamingCheckpointEndCommit:
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    break;
                case AofEntryType.ObjectStoreStreamingCheckpointStartCommit:
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    if (asReplica && header.storeVersion > storeWrapper.store.CurrentVersion)
                        storeWrapper.store.SetVersion(header.storeVersion);
                    break;
                case AofEntryType.FlushAll:
                    storeWrapper.FlushAllDatabases(unsafeTruncateLog: header.unsafeTruncateLog == 1);
                    break;
                case AofEntryType.FlushDb:
                    storeWrapper.FlushDatabase(unsafeTruncateLog: header.unsafeTruncateLog == 1, dbId: header.databaseId);
                    break;
                case AofEntryType.ObjectStoreStreamingCheckpointEndCommit:
                    Debug.Assert(storeWrapper.serverOptions.ReplicaDisklessSync);
                    break;
                default:
                    ReplayOp(ptr, length, asReplica);
                    break;
            }
        }

        /// <summary>
        /// Method to process a batch of entries as a single txn.
        /// Assumes that operations arg does not contain transaction markers (i.e. TxnStart,TxnCommit,TxnAbort)
        /// </summary>
        /// <param name="operations"></param>
        /// <param name="asReplica"></param>
        private unsafe void ProcessTxn(List<byte[]> operations, bool asReplica)
        {
            foreach (byte[] entry in operations)
            {
                fixed (byte* ptr = entry)
                    ReplayOp(ptr, entry.Length, asReplica);
            }
        }

        private unsafe bool ReplayOp(byte* entryPtr, int length, bool replayAsReplica)
        {
            AofHeader header = *(AofHeader*)entryPtr;

            // Skips (1) entries with versions that were part of prior checkpoint; and (2) future entries in fuzzy region
            if (SkipRecord(entryPtr, length, replayAsReplica)) return false;

            switch (header.opType)
            {
                case AofEntryType.StoreUpsert:
                    StoreUpsert(stringBasicContext, stringInput, entryPtr);
                    break;
                case AofEntryType.StoreRMW:
                    StoreRMW(stringBasicContext, stringInput, entryPtr);
                    break;
                case AofEntryType.StoreDelete:
                    StoreDelete(stringBasicContext, entryPtr);
                    break;
                case AofEntryType.ObjectStoreRMW:
                    ObjectStoreRMW(objectBasicContext, objectInput, entryPtr, bufferPtr, buffer.Length);
                    break;
                case AofEntryType.ObjectStoreUpsert:
                    ObjectStoreUpsert(objectBasicContext, storeWrapper.GarnetObjectSerializer, entryPtr, bufferPtr, buffer.Length);
                    break;
                case AofEntryType.ObjectStoreDelete:
                    ObjectStoreDelete(objectBasicContext, entryPtr);
                    break;
                case AofEntryType.UnifiedStoreRMW:
                    UnifiedStoreRMW(unifiedBasicContext, unifiedInput, entryPtr, bufferPtr, buffer.Length);
                    break;
                case AofEntryType.UnifiedStoreStringUpsert:
                    UnifiedStoreStringUpsert(unifiedBasicContext, unifiedInput, entryPtr, bufferPtr, buffer.Length);
                    break;
                case AofEntryType.UnifiedStoreObjectUpsert:
                    UnifiedStoreObjectUpsert(unifiedBasicContext, storeWrapper.GarnetObjectSerializer, entryPtr, bufferPtr, buffer.Length);
                    break;
                case AofEntryType.UnifiedStoreDelete:
                    UnifiedStoreDelete(unifiedBasicContext, entryPtr);
                    break;
                case AofEntryType.StoredProcedure:
                    RunStoredProc(header.procedureId, customProcInput, entryPtr);
                    break;
                default:
                    throw new GarnetException($"Unknown AOF header operation type {header.opType}");
            }
            return true;

            void RunStoredProc(byte id, CustomProcedureInput customProcInput, byte* ptr)
            {
                var curr = ptr + sizeof(AofHeader);

                // Reconstructing CustomProcedureInput

                // input
                customProcInput.DeserializeFrom(curr);

                // Run the stored procedure with the reconstructed input
                respServerSession.RunTransactionProc(id, ref customProcInput, ref output, isRecovering: true);
            }
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
                stringBasicContext = respServerSession.storageSession.stringBasicContext.Session.BasicContext;
                unifiedBasicContext = respServerSession.storageSession.unifiedBasicContext.Session.BasicContext;

                if (!storeWrapper.serverOptions.DisableObjects)
                    objectBasicContext = respServerSession.storageSession.objectBasicContext.Session.BasicContext;

                this.activeDbId = db.Id;
            }
        }

        static void StoreUpsert(BasicContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> basicContext,
            StringInput stringInput, byte* ptr)
        {
            var curr = ptr + sizeof(AofHeader);
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += key.TotalSize;

            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += value.TotalSize;

            // Reconstructing StringInput

            // input
            stringInput.DeserializeFrom(curr);

            SpanByteAndMemory output = default;
            basicContext.Upsert(key.ReadOnlySpan, ref stringInput, value.ReadOnlySpan, ref output);
            output.Dispose();
        }

        static void StoreRMW(BasicContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> basicContext, StringInput stringInput, byte* ptr)
        {
            var curr = ptr + sizeof(AofHeader);
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += key.TotalSize;

            // Reconstructing StringInput

            // input
            _ = stringInput.DeserializeFrom(curr);

            var pbOutput = stackalloc byte[32];
            var output = SpanByteAndMemory.FromPinnedPointer(pbOutput, 32);

            if (basicContext.RMW(key.ReadOnlySpan, ref stringInput, ref output).IsPending)
                basicContext.CompletePending(true);
            output.Dispose();
        }

        static void StoreDelete(BasicContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> basicContext, byte* ptr)
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(ptr + sizeof(AofHeader));
            basicContext.Delete(key);
        }

        static void ObjectStoreUpsert(BasicContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> basicContext,
                GarnetObjectSerializer garnetObjectSerializer, byte* ptr, byte* outputPtr, int outputLength)
        {
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(ptr + sizeof(AofHeader));

            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(ptr + sizeof(AofHeader) + key.TotalSize);
            var valB = garnetObjectSerializer.Deserialize(value.ToArray());

            // input
            // TODOMigrate: _ = objectInput.DeserializeFrom(curr); // TODO - need to serialize this as well

            var output = ObjectOutput.FromPinnedPointer(outputPtr, outputLength);
            basicContext.Upsert(key.ReadOnlySpan, valB);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Memory.Dispose();
        }

        static void ObjectStoreRMW(BasicContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> basicContext,
            ObjectInput objectInput, byte* ptr, byte* outputPtr, int outputLength)
        {
            var curr = ptr + sizeof(AofHeader);
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += key.TotalSize;

            // Reconstructing ObjectInput

            // input
            _ = objectInput.DeserializeFrom(curr);

            // Call RMW with the reconstructed key & ObjectInput
            var output = ObjectOutput.FromPinnedPointer(outputPtr, outputLength);
            if (basicContext.RMW(key.ReadOnlySpan, ref objectInput, ref output).IsPending)
                basicContext.CompletePending(true);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Memory.Dispose();
        }

        static void ObjectStoreDelete(BasicContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> basicContext, byte* ptr)
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(ptr + sizeof(AofHeader));
            basicContext.Delete(key);
        }

        static void UnifiedStoreStringUpsert(BasicContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> basicContext,
            UnifiedInput unifiedInput, byte* ptr, byte* outputPtr, int outputLength)
        {
            var curr = ptr + sizeof(AofHeader);
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += key.TotalSize;

            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += value.TotalSize;

            // Reconstructing UnifiedInput

            // input
            _ = unifiedInput.DeserializeFrom(curr);

            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            basicContext.Upsert(key.ReadOnlySpan, ref unifiedInput, value.ReadOnlySpan, ref output);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Memory.Dispose();
        }

        static void UnifiedStoreObjectUpsert(BasicContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> basicContext,
                GarnetObjectSerializer garnetObjectSerializer, byte* ptr, byte* outputPtr, int outputLength)
        {
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(ptr + sizeof(AofHeader));

            var value = PinnedSpanByte.FromLengthPrefixedPinnedPointer(ptr + sizeof(AofHeader) + key.TotalSize);
            var valB = garnetObjectSerializer.Deserialize(value.ToArray());

            // input
            // TODOMigrate: _ = unifiedInput.DeserializeFrom(curr); // TODO - need to serialize this as well

            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            basicContext.Upsert(key.ReadOnlySpan, valB);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Memory.Dispose();
        }

        static void UnifiedStoreRMW(BasicContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> basicContext,
            UnifiedInput unifiedInput, byte* ptr, byte* outputPtr, int outputLength)
        {
            var curr = ptr + sizeof(AofHeader);
            var key = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            curr += key.TotalSize;

            // Reconstructing UnifiedInput

            // input
            _ = unifiedInput.DeserializeFrom(curr);

            // Call RMW with the reconstructed key & UnifiedInput
            var output = UnifiedOutput.FromPinnedPointer(outputPtr, outputLength);
            if (basicContext.RMW(key.ReadOnlySpan, ref unifiedInput, ref output).IsPending)
                basicContext.CompletePending(true);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Memory.Dispose();
        }

        static void UnifiedStoreDelete(
            BasicContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> basicContext, byte* ptr)
        {
            var key = SpanByte.FromLengthPrefixedPinnedPointer(ptr + sizeof(AofHeader));
            basicContext.Delete(key);
        }

        /// <summary>
        /// On recovery apply records with header.version greater than CurrentVersion.
        /// </summary>
        /// <param name="entryPtr"></param>
        /// <param name="length"></param>
        /// <param name="asReplica"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        bool SkipRecord(byte* entryPtr, int length, bool asReplica)
        {
            var header = *(AofHeader*)entryPtr;
            return (asReplica && inFuzzyRegion) ? // Buffer logic only for AOF version > 1
                BufferNewVersionRecord(header, entryPtr, length) :
                IsOldVersionRecord(header);
        }

        bool BufferNewVersionRecord(AofHeader header, byte* entryPtr, int length)
        {
            if (IsNewVersionRecord(header))
            {
                fuzzyRegionBuffer.Add(new ReadOnlySpan<byte>(entryPtr, length).ToArray());
                return true;
            }
            return false;
        }

        bool IsOldVersionRecord(AofHeader header)
            => header.storeVersion < storeWrapper.store.CurrentVersion;

        bool IsNewVersionRecord(AofHeader header)
            => header.storeVersion > storeWrapper.store.CurrentVersion;
    }
}