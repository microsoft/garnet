// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
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

        /// <summary>
        /// Replication offset
        /// </summary>
        internal long ReplicationOffset { get; private set; }

        /// <summary>
        /// Session for main store
        /// </summary>
        readonly BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext;

        /// <summary>
        /// Session for object store
        /// </summary>
        readonly BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreBasicContext;

        readonly Dictionary<int, List<byte[]>> inflightTxns;
        readonly byte[] buffer;
        readonly GCHandle handle;
        readonly byte* bufferPtr;

        readonly ILogger logger;
        readonly bool recordToAof;
        readonly bool replayFromLegacyAof;

        // 1000 0000
        const byte MsbMask = (1 << 7);

        /// <summary>
        /// Create new AOF processor
        /// </summary>
        public AofProcessor(
            StoreWrapper storeWrapper,
            bool recordToAof = false,
            ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;
            this.recordToAof = recordToAof;

            ReplicationOffset = 0;

            var replayAofStoreWrapper = new StoreWrapper(
                storeWrapper.version,
                storeWrapper.redisProtocolVersion,
                null,
                storeWrapper.store,
                storeWrapper.objectStore,
                storeWrapper.objectStoreSizeTracker,
                storeWrapper.customCommandManager,
                recordToAof ? storeWrapper.appendOnlyFile : null,
                storeWrapper.serverOptions,
                accessControlList: storeWrapper.accessControlList,
                loggerFactory: storeWrapper.loggerFactory);

            this.respServerSession = new RespServerSession(0, networkSender: null, storeWrapper: replayAofStoreWrapper, subscribeBroker: null, authenticator: null, enableScripts: false);

            var session = respServerSession.storageSession.basicContext.Session;
            basicContext = session.BasicContext;
            var objectStoreSession = respServerSession.storageSession.objectStoreBasicContext.Session;
            if (objectStoreSession is not null)
                objectStoreBasicContext = objectStoreSession.BasicContext;

            parseState.Initialize();
            storeInput.parseState = parseState;
            objectStoreInput.parseState = parseState;
            customProcInput.parseState = parseState;

            inflightTxns = new Dictionary<int, List<byte[]>>();
            buffer = new byte[BufferSizeUtils.ServerBufferSize(new MaxSizeSettings())];
            handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            bufferPtr = (byte*)handle.AddrOfPinnedObject();

            replayFromLegacyAof = storeWrapper.serverOptions.ReplayFromLegacyAof;

            this.logger = logger;
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            basicContext.Session?.Dispose();
            objectStoreBasicContext.Session?.Dispose();
            handle.Free();
        }

        /// <summary>
        /// Recover store using AOF
        /// </summary>
        public unsafe bool Recover(long untilAddress = -1)
        {
            logger?.LogInformation("Begin AOF recovery");
            return RecoverReplay(untilAddress);
        }

        MemoryResult<byte> output = default;
        private unsafe bool RecoverReplay(long untilAddress)
        {
            logger?.LogInformation("Begin AOF replay");
            try
            {
                int count = 0;
                if (untilAddress == -1) untilAddress = storeWrapper.appendOnlyFile.TailAddress;
                using TsavoriteLogScanIterator scan = storeWrapper.appendOnlyFile.Scan(storeWrapper.appendOnlyFile.BeginAddress, untilAddress);

                while (scan.GetNext(MemoryPool<byte>.Shared, out IMemoryOwner<byte> entry, out int length, out _, out long nextAofAddress))
                {
                    count++;
                    ProcessAofRecord(entry, length);
                    if (count % 100_000 == 0)
                        logger?.LogInformation("Completed AOF replay of {count} records, until AOF address {nextAofAddress}", count, nextAofAddress);
                }

                // Update ReplicationOffset
                ReplicationOffset = untilAddress;

                logger?.LogInformation("Completed full AOF log replay of {count} records", count);
                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error occurred AofProcessor.RecoverReplay");
                return false;
            }
            finally
            {
                output.MemoryOwner?.Dispose();
                respServerSession.Dispose();
            }
        }

        internal unsafe void ProcessAofRecord(IMemoryOwner<byte> entry, int length, bool asReplica = false)
        {
            fixed (byte* ptr = entry.Memory.Span)
            {
                if (replayFromLegacyAof && IsLegacyFormat(ptr))
                {
                    LegacyAofHeader aofHeader = *(LegacyAofHeader*)ptr;
                    ProcessAofRecordInternal(aofHeader.sessionID, aofHeader.opType, aofHeader.version, aofHeader.type, ptr, length, isLegacyFormat: true, asReplica);
                }
                else
                {
                    AofHeader aofHeader = *(AofHeader*)ptr;
                    ProcessAofRecordInternal(aofHeader.sessionID, aofHeader.opType, aofHeader.version, aofHeader.type, ptr, length, isLegacyFormat: false, asReplica);
                }
            }
            entry.Dispose();
        }

        /// <summary>
        /// Checks if the MSB is not set of the first pointer, this indicates it is an older AOF format
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        private static unsafe bool IsLegacyFormat(byte* ptr) => (*ptr & MsbMask) == 0;

        public unsafe void ProcessAofRecordInternal(byte* ptr, int length, bool asReplica = false)
        {
            AofHeader aofHeader = *(AofHeader*)ptr;
            ProcessAofRecordInternal(aofHeader.sessionID, aofHeader.opType, aofHeader.version, aofHeader.type, ptr, length, isLegacyFormat: false, asReplica);
        }

        /// <summary>
        /// Process AOF record
        /// </summary>
        internal unsafe void ProcessAofRecordInternal(int sessionID, AofEntryType opType, long aofHeaderRecordVersion, byte type, byte* ptr, int length, bool isLegacyFormat, bool asReplica = false)
        {
            if (inflightTxns.ContainsKey(sessionID))
            {
                switch (opType)
                {
                    case AofEntryType.TxnAbort:
                        inflightTxns[sessionID].Clear();
                        inflightTxns.Remove(sessionID);
                        break;
                    case AofEntryType.TxnCommit:
                        ProcessTxn(inflightTxns[sessionID]);
                        inflightTxns[sessionID].Clear();
                        inflightTxns.Remove(sessionID);
                        break;
                    case AofEntryType.StoredProcedure:
                        throw new GarnetException($"Unexpected AOF header operation type {opType} within transaction");
                    default:
                        inflightTxns[sessionID].Add(new ReadOnlySpan<byte>(ptr, length).ToArray());
                        break;
                }
                return;
            }

            switch (opType)
            {
                case AofEntryType.TxnStart:
                    inflightTxns[sessionID] = new List<byte[]>();
                    break;
                case AofEntryType.TxnAbort:
                case AofEntryType.TxnCommit:
                    // We encountered a transaction end without start - this could happen because we truncated the AOF
                    // after a checkpoint, and the transaction belonged to the previous version. It can safely
                    // be ignored.
                    break;
                case AofEntryType.MainStoreCheckpointCommit:
                    if (asReplica)
                    {
                        if (aofHeaderRecordVersion > storeWrapper.store.CurrentVersion)
                            storeWrapper.TakeCheckpoint(false, StoreType.Main, logger);
                    }
                    break;
                case AofEntryType.ObjectStoreCheckpointCommit:
                    if (asReplica)
                    {
                        if (aofHeaderRecordVersion > storeWrapper.objectStore.CurrentVersion)
                            storeWrapper.TakeCheckpoint(false, StoreType.Object, logger);
                    }
                    break;
                default:
                    if (isLegacyFormat)
                    {
                        ReplayLegacyOp(opType, aofHeaderRecordVersion, type, ptr);
                    }
                    else
                    {
                        ReplayOp(opType, aofHeaderRecordVersion, type, ptr);
                    }
                    break;
            }
        }

        /// <summary>
        /// Method to process a batch of entries as a single txn.
        /// Assumes that operations arg does not contain transaction markers (i.e. TxnStart,TxnCommit,TxnAbort)
        /// </summary>
        /// <param name="operations"></param>
        private unsafe void ProcessTxn(List<byte[]> operations)
        {
            foreach (byte[] entry in operations)
            {
                fixed (byte* ptr = entry)
                {
                    if (!IsLegacyFormat(ptr))
                    {
                        AofHeader aofHeader = *(AofHeader*)ptr;
                        ReplayOp(aofHeader.opType, aofHeader.version, aofHeader.type, ptr);
                    }
                    else
                    {
                        LegacyAofHeader aofHeader = *(LegacyAofHeader*)ptr;
                        ReplayOp(aofHeader.opType, aofHeader.version, aofHeader.type, ptr);
                    }
                }
            }
        }

        private unsafe bool ReplayOp(AofEntryType opType, long aofHeaderRecordVersion, byte type, byte* entryPtr)
        {
            // Skips versions that were part of checkpoint
            if (SkipRecord(opType, aofHeaderRecordVersion)) return false;

            switch (opType)
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
                    RunStoredProc(type, customProcInput, entryPtr, isHistoricMode: false);
                    break;
                default:
                    throw new GarnetException($"Unknown AOF header operation type {opType}");
            }
            return true;
        }

        private unsafe bool ReplayLegacyOp(AofEntryType opType, long aofHeaderRecordVersion, byte type, byte* entryPtr)
        {
            // Skips versions that were part of checkpoint
            if (SkipRecord(opType, aofHeaderRecordVersion)) return false;

            switch (opType)
            {
                case AofEntryType.StoreUpsert:
                    LegacyUpsert(entryPtr, storeInput);
                    break;
                case AofEntryType.StoredProcedure:
                    RunStoredProc(type, customProcInput, entryPtr, isHistoricMode: true);
                    break;
                default:
                    throw new GarnetException($"Unknown AOF header operation type {opType} for AOF legacy processing");
            }

            return true;
        }

        private unsafe void LegacyUpsert(byte* ptr, RawStringInput storeInput)
        {
            ref SpanByte key = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(LegacyAofHeader));
            // input is willingly being ignored from older format
            ref SpanByte input = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(LegacyAofHeader) + key.TotalSize);
            ref SpanByte value = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(LegacyAofHeader) + key.TotalSize + input.TotalSize);

            /* Reconstructing RawStringInput based on the following invariant:
               - extra metadata on value tells us there was an expiration
            */
            long expiration = value.ExtraMetadata;
            if (value.MetadataSize == 0)
            {
                storeInput = new RawStringInput(RespCommand.SETEX, 0);
            }
            else
            {
                storeInput = new RawStringInput(RespCommand.SETEX, 0, expiration);
            }

            StoreUpsertPostParsing(basicContext, ref key, storeInput, ref value);
        }

        unsafe void RunStoredProc(byte id, CustomProcedureInput customProcInput, byte* ptr, bool isHistoricMode)
        {
            var curr = ptr + (isHistoricMode ? sizeof(LegacyAofHeader) : sizeof(AofHeader));

            // Reconstructing CustomProcedureInput
            if (isHistoricMode)
            {
                customProcInput.DeserializeFromHistoricAof(curr);
            }
            else
            {
                customProcInput.DeserializeFrom(curr);
            }

            respServerSession.RunTransactionProc(id, ref customProcInput, ref output);
        }

        static unsafe void StoreUpsert(BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext,
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

            StoreUpsertPostParsing(basicContext, ref key, storeInput, ref value);
        }

        static unsafe void StoreUpsertPostParsing(BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext,
            ref SpanByte key, RawStringInput storeInput, ref SpanByte value)
        {
            SpanByteAndMemory output = default;
            basicContext.Upsert(ref key, ref storeInput, ref value, ref output);
            if (!output.IsSpanByte)
                output.Memory.Dispose();
        }

        static unsafe void StoreRMW(BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext, RawStringInput storeInput, byte* ptr)
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

        static unsafe void StoreDelete(BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext, byte* ptr)
        {
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader));
            basicContext.Delete(ref key);
        }

        static unsafe void ObjectStoreUpsert(BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> basicContext,
                GarnetObjectSerializer garnetObjectSerializer, byte* ptr, byte* outputPtr, int outputLength)
        {
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader));
            var keyB = key.ToByteArray();

            ref var value = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader) + key.TotalSize);
            var valB = garnetObjectSerializer.Deserialize(value.ToByteArray());

            var output = new GarnetObjectStoreOutput { spanByteAndMemory = new(outputPtr, outputLength) };
            basicContext.Upsert(ref keyB, ref valB);
            if (!output.spanByteAndMemory.IsSpanByte)
                output.spanByteAndMemory.Memory.Dispose();
        }

        static unsafe void ObjectStoreRMW(BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> basicContext,
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
            var output = new GarnetObjectStoreOutput { spanByteAndMemory = new(outputPtr, outputLength) };
            if (basicContext.RMW(ref keyB, ref objectStoreInput, ref output).IsPending)
                basicContext.CompletePending(true);

            if (!output.spanByteAndMemory.IsSpanByte)
                output.spanByteAndMemory.Memory.Dispose();
        }

        static unsafe void ObjectStoreDelete(BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> basicContext, byte* ptr)
        {
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader));
            var keyB = key.ToByteArray();
            basicContext.Delete(ref keyB);
        }

        /// <summary>
        /// On recovery apply records with header.version greater than CurrentVersion.
        /// </summary>
        /// <exception cref="GarnetException"></exception>
        bool SkipRecord(AofEntryType opType, long recordVersion)
        {
            AofStoreType storeType = ToAofStoreType(opType);

            return storeType switch
            {
                AofStoreType.MainStoreType => recordVersion <= storeWrapper.store.CurrentVersion - 1,
                AofStoreType.ObjectStoreType => recordVersion <= storeWrapper.objectStore.CurrentVersion - 1,
                AofStoreType.TxnType => false,
                AofStoreType.ReplicationType => false,
                AofStoreType.CheckpointType => false,
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
                AofEntryType.MainStoreCheckpointCommit or AofEntryType.ObjectStoreCheckpointCommit => AofStoreType.CheckpointType,
                _ => throw new GarnetException($"Conversion to AofStoreType not possible for {type}"),
            };
        }
    }
}