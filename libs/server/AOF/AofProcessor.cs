// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Garnet.networking;
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
        readonly CustomCommand[] customCommands;
        readonly CustomObjectCommandWrapper[] customObjectCommands;
        readonly RespServerSession respServerSession;

        /// <summary>
        /// Replication offset
        /// </summary>
        internal long ReplicationOffset { get; private set; }

        /// <summary>
        /// Session for main store
        /// </summary>
        readonly ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> session = null;

        /// <summary>
        /// Session for object store
        /// </summary>
        readonly ClientSession<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreSession = null;

        readonly Dictionary<int, List<byte[]>> inflightTxns;
        readonly byte[] buffer;
        readonly GCHandle handle;
        readonly byte* bufferPtr;

        readonly ILogger logger;
        readonly bool recordToAof;

        /// <summary>
        /// Create new AOF processor
        /// </summary>
        public AofProcessor(
            StoreWrapper storeWrapper,
            bool recordToAof = false,
            ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;
            this.customCommands = storeWrapper.customCommandManager.commandMap;
            this.customObjectCommands = storeWrapper.customCommandManager.objectCommandMap;
            this.recordToAof = recordToAof;

            ReplicationOffset = 0;

            var replayAofStoreWrapper = new StoreWrapper(
                storeWrapper.version,
                null,
                storeWrapper.store,
                storeWrapper.objectStore,
                storeWrapper.objectStoreSizeTracker,
                storeWrapper.customCommandManager,
                recordToAof ? storeWrapper.appendOnlyFile : null,
                storeWrapper.serverOptions,
                accessControlList: storeWrapper.accessControlList,
                loggerFactory: storeWrapper.loggerFactory);

            this.respServerSession = new RespServerSession(null, replayAofStoreWrapper, null);

            session = respServerSession.storageSession.session;
            objectStoreSession = respServerSession.storageSession.objectStoreSession;

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
            session?.Dispose();
            objectStoreSession?.Dispose();
            handle.Free();
        }

        /// <summary>
        /// Recover store using AOF
        /// </summary>
        public unsafe void Recover(long untilAddress = -1)
        {
            logger?.LogInformation("Begin AOF recovery");
            RecoverReplay(untilAddress);
        }

        MemoryResult<byte> output = default;
        private unsafe void RecoverReplay(long untilAddress)
        {
            logger?.LogInformation("Begin AOF replay");
            try
            {
                int count = 0;
                if (untilAddress == -1) untilAddress = storeWrapper.appendOnlyFile.TailAddress;
                using var scan = storeWrapper.appendOnlyFile.Scan(storeWrapper.appendOnlyFile.BeginAddress, untilAddress);

                while (scan.GetNext(out byte[] entry, out int _, out _, out long nextAofAddress))
                {
                    count++;

                    ProcessAofRecord(entry);

                    if (count % 100_000 == 0)
                        logger?.LogInformation("Completed AOF replay of {count} records, until AOF address {nextAofAddress}", count, nextAofAddress);
                }

                // Update ReplicationOffset
                ReplicationOffset = untilAddress;

                logger?.LogInformation("Completed full AOF log replay of {count} records", count);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error occurred AofProcessor.RecoverReplay");
            }
            finally
            {
                output.MemoryOwner?.Dispose();
                respServerSession.Dispose();
            }
        }

        internal unsafe void ProcessAofRecord(byte[] record, bool asReplica = false)
        {
            fixed (byte* ptr = record)
            {
                ProcessAofRecordInternal(record, ptr, record.Length, asReplica);
            }
        }

        /// <summary>
        /// Process AOF record
        /// </summary>
        public unsafe void ProcessAofRecordInternal(byte[] record, byte* ptr, int length, bool asReplica = false)
        {
            AofHeader header = *(AofHeader*)ptr;

            if (inflightTxns.ContainsKey(header.sessionID))
            {
                switch (header.opType)
                {
                    case AofEntryType.TxnAbort:
                        inflightTxns[header.sessionID].Clear();
                        inflightTxns.Remove(header.sessionID);
                        break;
                    case AofEntryType.TxnCommit:
                        ProcessTxn(inflightTxns[header.sessionID]);
                        inflightTxns[header.sessionID].Clear();
                        inflightTxns.Remove(header.sessionID);
                        break;
                    case AofEntryType.StoredProcedure:
                        throw new GarnetException($"Unexpected AOF header operation type {header.opType} within transaction");
                    default:
                        inflightTxns[header.sessionID].Add(record ?? new ReadOnlySpan<byte>(ptr, length).ToArray());
                        break;
                }
                return;
            }

            switch (header.opType)
            {
                case AofEntryType.TxnStart:
                    inflightTxns[header.sessionID] = new List<byte[]>();
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
                        if (header.version > storeWrapper.store.CurrentVersion)
                            storeWrapper.TakeCheckpoint(false, StoreType.Main, logger);
                    }
                    break;
                case AofEntryType.ObjectStoreCheckpointCommit:
                    if (asReplica)
                    {
                        if (header.version > storeWrapper.objectStore.CurrentVersion)
                            storeWrapper.TakeCheckpoint(false, StoreType.Object, logger);
                    }
                    break;
                default:
                    ReplayOp(ptr);
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
                    ReplayOp(ptr);
            }
        }

        private unsafe bool ReplayOp(byte* entryPtr)
        {
            AofHeader header = *(AofHeader*)entryPtr;

            // Skips versions that were part of checkpoint
            if (SkipRecord(header)) return false;

            switch (header.opType)
            {
                case AofEntryType.StoreUpsert:
                    StoreUpsert(session, entryPtr);
                    break;
                case AofEntryType.StoreRMW:
                    StoreRMW(session, entryPtr);
                    break;
                case AofEntryType.StoreDelete:
                    StoreDelete(session, entryPtr);
                    break;
                case AofEntryType.ObjectStoreRMW:
                    ObjectStoreRMW(objectStoreSession, entryPtr, bufferPtr, buffer.Length);
                    break;
                case AofEntryType.ObjectStoreUpsert:
                    ObjectStoreUpsert(objectStoreSession, entryPtr, bufferPtr, buffer.Length, storeWrapper);
                    break;
                case AofEntryType.ObjectStoreDelete:
                    ObjectStoreDelete(objectStoreSession, entryPtr);
                    break;
                case AofEntryType.StoredProcedure:
                    ref var input = ref Unsafe.AsRef<SpanByte>(entryPtr + sizeof(AofHeader));
                    respServerSession.RunTransactionProc(header.type, new ArgSlice(ref input), ref output);
                    break;
                default:
                    throw new GarnetException($"Unknown AOF header operation type {header.opType}");
            }
            return true;
        }

        static unsafe void StoreUpsert(ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> session, byte* ptr)
        {
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader));
            ref var input = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader) + key.TotalSize);
            ref var value = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader) + key.TotalSize + input.TotalSize);

            SpanByteAndMemory output = default;
            session.Upsert(ref key, ref input, ref value, ref output);
            if (!output.IsSpanByte)
                output.Memory.Dispose();
        }

        static unsafe void StoreRMW(ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> session, byte* ptr)
        {
            byte* pbOutput = stackalloc byte[32];
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader));
            ref var input = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader) + key.TotalSize);
            var output = new SpanByteAndMemory(pbOutput, 32);
            if (session.RMW(ref key, ref input, ref output).IsPending)
                session.CompletePending(true);
            if (!output.IsSpanByte)
                output.Memory.Dispose();
        }

        static unsafe void StoreDelete(ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> session, byte* ptr)
        {
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader));
            session.Delete(ref key);
        }

        static unsafe void ObjectStoreUpsert(ClientSession<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> session, byte* ptr, byte* outputPtr, int outputLength, StoreWrapper storeWrapper)
        {
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader));
            var keyB = key.ToByteArray();
            ref var input = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader) + key.TotalSize);
            ref var value = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader) + key.TotalSize + input.TotalSize);

            var valB = storeWrapper.DeserializeGarnetObject(value.ToByteArray());

            var output = new GarnetObjectStoreOutput { spanByteAndMemory = new(outputPtr, outputLength) };
            session.Upsert(ref keyB, ref valB);
            if (!output.spanByteAndMemory.IsSpanByte)
                output.spanByteAndMemory.Memory.Dispose();
        }

        static unsafe void ObjectStoreRMW(ClientSession<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> session, byte* ptr, byte* outputPtr, int outputLength)
        {
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader));
            var keyB = key.ToByteArray();

            ref var input = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader) + key.TotalSize);
            var output = new GarnetObjectStoreOutput { spanByteAndMemory = new(outputPtr, outputLength) };
            if (session.RMW(ref keyB, ref input, ref output).IsPending)
                session.CompletePending(true);
            if (!output.spanByteAndMemory.IsSpanByte)
                output.spanByteAndMemory.Memory.Dispose();
        }

        static unsafe void ObjectStoreDelete(ClientSession<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> session, byte* ptr)
        {
            ref var key = ref Unsafe.AsRef<SpanByte>(ptr + sizeof(AofHeader));
            var keyB = key.ToByteArray();
            session.Delete(ref keyB);
        }

        /// <summary>
        /// On recovery apply records with header.version greater than CurrentVersion.
        /// </summary>
        /// <param name="header"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        bool SkipRecord(AofHeader header)
        {
            AofStoreType storeType = ToAofStoreType(header.opType);

            return storeType switch
            {
                AofStoreType.MainStoreType => header.version <= storeWrapper.store.CurrentVersion - 1,
                AofStoreType.ObjectStoreType => header.version <= storeWrapper.objectStore.CurrentVersion - 1,
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