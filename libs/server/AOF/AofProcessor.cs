﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Garnet.networking;
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

        private readonly RawStringInput storeInput;
        private readonly ObjectInput objectStoreInput;
        private readonly CustomProcedureInput customProcInput;
        private readonly SessionParseState parseState;

        int activeDbId;

        /// <summary>
        /// Session for main store
        /// </summary>
        BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext;

        /// <summary>
        /// Session for object store
        /// </summary>
        BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreBasicContext;

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
            bool recordToAof = false,
            ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;

            var replayAofStoreWrapper = new StoreWrapper(storeWrapper, recordToAof);

            this.activeDbId = 0;
            this.respServerSession = new RespServerSession(0, networkSender: null, storeWrapper: replayAofStoreWrapper, subscribeBroker: null, authenticator: null, enableScripts: false);

            // Switch current contexts to match the default database
            SwitchActiveDatabaseContext(storeWrapper.DefaultDatabase, true);

            parseState.Initialize();
            storeInput.parseState = parseState;
            objectStoreInput.parseState = parseState;
            customProcInput.parseState = parseState;

            inflightTxns = new Dictionary<int, List<byte[]>>();
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
                dbSession.StorageSession.basicContext.Session?.Dispose();
                dbSession.StorageSession.objectStoreBasicContext.Session?.Dispose();
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
                        storeWrapper.objectStore.SetVersion(header.storeVersion);
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

        private unsafe bool ReplayOp(byte* entryPtr, int length, bool asReplica)
        {
            AofHeader header = *(AofHeader*)entryPtr;

            // Skips (1) entries with versions that were part of prior checkpoint; and (2) future entries in fuzzy region
            if (SkipRecord(entryPtr, length, asReplica)) return false;

            switch (header.opType)
            {
                case AofEntryType.StoreUpsert:
                    StoreUpsert(basicContext, storeInput, entryPtr);
                    break;
                case AofEntryType.StoreRMW:
                    StoreRMW(basicContext, storeInput, entryPtr);
                    break;
                case AofEntryType.StoreDelete:
                    StoreDelete(basicContext, entryPtr);
                    break;
                case AofEntryType.ObjectStoreRMW:
                    ObjectStoreRMW(objectStoreBasicContext, objectStoreInput, entryPtr, bufferPtr, buffer.Length);
                    break;
                case AofEntryType.ObjectStoreUpsert:
                    ObjectStoreUpsert(objectStoreBasicContext, storeWrapper.GarnetObjectSerializer, entryPtr, bufferPtr, buffer.Length);
                    break;
                case AofEntryType.ObjectStoreDelete:
                    ObjectStoreDelete(objectStoreBasicContext, entryPtr);
                    break;
                case AofEntryType.StoredProcedure:
                    RunStoredProc(header.procedureId, customProcInput, entryPtr);
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
        }

        void RunStoredProc(byte id, CustomProcedureInput customProcInput, byte* ptr)
        {
            var curr = ptr + sizeof(AofHeader);

            // Reconstructing CustomProcedureInput

            // input
            customProcInput.DeserializeFrom(curr);

            respServerSession.RunTransactionProc(id, ref customProcInput, ref output);
        }

        static void StoreUpsert(BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext,
            RawStringInput storeInput, byte* ptr)
        {
            var curr = ptr + sizeof(AofHeader);
            ref var key = ref Unsafe.AsRef<SpanByte>(curr);
            curr += key.TotalSize;

            ref var value = ref Unsafe.AsRef<SpanByte>(curr);
            curr += value.TotalSize;

            // Reconstructing RawStringInput

            // input
            storeInput.DeserializeFrom(curr);

            SpanByteAndMemory output = default;
            basicContext.Upsert(ref key, ref storeInput, ref value, ref output);
            if (!output.IsSpanByte)
                output.Memory.Dispose();
        }

        static void StoreRMW(BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext, RawStringInput storeInput, byte* ptr)
        {
            var curr = ptr + sizeof(AofHeader);
            ref var key = ref Unsafe.AsRef<SpanByte>(curr);
            curr += key.TotalSize;

            // Reconstructing RawStringInput

            // input
            storeInput.DeserializeFrom(curr);

            var pbOutput = stackalloc byte[32];
            var output = new SpanByteAndMemory(pbOutput, 32);

            if (basicContext.RMW(ref key, ref storeInput, ref output).IsPending)
                basicContext.CompletePending(true);
            if (!output.IsSpanByte)
                output.Memory.Dispose();
        }

        static void StoreDelete(BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext, byte* ptr)
        {
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader));
            basicContext.Delete(ref key);
        }

        static void ObjectStoreUpsert(BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> basicContext,
                GarnetObjectSerializer garnetObjectSerializer, byte* ptr, byte* outputPtr, int outputLength)
        {
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader));
            var keyB = key.ToByteArray();

            ref var value = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader) + key.TotalSize);
            var valB = garnetObjectSerializer.Deserialize(value.ToByteArray());

            var output = new GarnetObjectStoreOutput { SpanByteAndMemory = new(outputPtr, outputLength) };
            basicContext.Upsert(ref keyB, ref valB);
            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Memory.Dispose();
        }

        static void ObjectStoreRMW(BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> basicContext,
            ObjectInput objectStoreInput, byte* ptr, byte* outputPtr, int outputLength)
        {
            var curr = ptr + sizeof(AofHeader);
            ref var key = ref Unsafe.AsRef<SpanByte>(curr);
            curr += key.TotalSize;
            var keyB = key.ToByteArray();

            // Reconstructing ObjectInput

            // input
            objectStoreInput.DeserializeFrom(curr);

            // Call RMW with the reconstructed key & ObjectInput
            var output = new GarnetObjectStoreOutput { SpanByteAndMemory = new(outputPtr, outputLength) };
            if (basicContext.RMW(ref keyB, ref objectStoreInput, ref output).IsPending)
                basicContext.CompletePending(true);

            if (!output.SpanByteAndMemory.IsSpanByte)
                output.SpanByteAndMemory.Memory.Dispose();
        }

        static void ObjectStoreDelete(BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> basicContext, byte* ptr)
        {
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader));
            var keyB = key.ToByteArray();
            basicContext.Delete(ref keyB);
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
                AofEntryType.CheckpointStartCommit or AofEntryType.ObjectStoreCheckpointStartCommit or AofEntryType.MainStoreStreamingCheckpointStartCommit or AofEntryType.ObjectStoreStreamingCheckpointStartCommit => AofStoreType.CheckpointType,
                AofEntryType.CheckpointEndCommit or AofEntryType.ObjectStoreCheckpointEndCommit or AofEntryType.MainStoreStreamingCheckpointEndCommit or AofEntryType.ObjectStoreStreamingCheckpointEndCommit => AofStoreType.CheckpointType,
                AofEntryType.FlushAll or AofEntryType.FlushDb => AofStoreType.FlushDbType,
                _ => throw new GarnetException($"Conversion to AofStoreType not possible for {type}"),
            };
        }
    }
}