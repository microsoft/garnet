// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        long lastLog = 0;
        long totalKeyCount = 0;

        // Per-connection reassembly state for MigrationRecordSpanType.ChunkedLogRecord records whose chunks may span commands.
        ChunkedRecordReassembler chunkedRecordReassembler;

        /// <summary>
        /// Complete a reassembled <see cref="MigrationRecordSpanType.ChunkedLogRecord"/>. A non-inline object value is streamed
        /// from a <see cref="System.Buffers.ReadOnlySequence{T}"/> and deserialized with the object serializer (so it can exceed
        /// 2 GB), returned pre-deserialized alongside the small inline+key header; every other record is returned as one
        /// contiguous buffer. Returns true when the record has a streamed object value.
        /// </summary>
        unsafe bool CompleteChunkedRecord(StoreWrapper storeWrapper, out byte[] contiguous, out byte[] header, out IHeapObject valueObject)
        {
            var sequence = chunkedRecordReassembler.AsSequence();

            // Peek a prefix covering the inline portion to learn whether the object value must be streamed and where it starts.
            Span<byte> prefix = stackalloc byte[256];
            var prefixLen = (int)Math.Min(prefix.Length, chunkedRecordReassembler.Length);
            sequence.Slice(0, prefixLen).CopyTo(prefix);
            var objectValueStart = DiskLogRecord.GetChunkedObjectValueStart(prefix[..prefixLen], out var isObjectRecord);

            if (!isObjectRecord)
            {
                // Inline or overflow-value record (<= 2 GB): reassemble contiguously for the standard deserialize.
                contiguous = sequence.ToArray();
                header = null;
                valueObject = null;
                return false;
            }

            // Object value (possibly > 2 GB): keep the small inline+key header contiguous; stream the object value from the tail.
            header = sequence.Slice(0, objectValueStart).ToArray();
            valueObject = (IHeapObject)storeWrapper.GarnetObjectSerializer.Deserialize(sequence.Slice(objectValueStart));
            contiguous = null;
            return true;
        }

        /// <summary>
        /// Logging of migrate session status
        /// </summary>
        /// <param name="keyCount"></param>
        /// <param name="completed"></param>
        private void TrackImportProgress(int keyCount, bool completed = false)
        {
            totalKeyCount += keyCount;
            var duration = TimeSpan.FromTicks(Stopwatch.GetTimestamp() - lastLog);
            if (completed || lastLog == 0 || duration >= clusterProvider.storeWrapper.loggingFrequency)
            {
                logger?.LogTrace("[{op}]: totalKeyCount:({totalKeyCount})", completed ? "COMPLETED" : "IMPORTING", totalKeyCount.ToString("N0"));
                lastLog = Stopwatch.GetTimestamp();
            }
        }

        /// <summary>
        /// Implements CLUSTER MIGRATE command (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private bool NetworkClusterMigrate(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 4 arguments
            if (parseState.Count != 4)
            {
                invalidParameters = true;
                return true;
            }

            var replace = parseState.GetArgSliceByRef(1).ReadOnlySpan;
            var vectorSet = parseState.GetArgSliceByRef(2).ReadOnlySpan;
            var payloadStartPtr = parseState.GetArgSliceByRef(3).ToPointer();
            var lastParam = parseState.GetArgSliceByRef(parseState.Count - 1);

            var payloadEndPtr = lastParam.ToPointer() + lastParam.Length;

            var replaceOption = replace.EqualsUpperCaseSpanIgnoringCase("T"u8);
            var vectorSetOption = vectorSet.EqualsUpperCaseSpanIgnoringCase("T"u8);

            var buffer = new Span<byte>(payloadStartPtr, (int)(payloadEndPtr - payloadStartPtr)).ToArray();

            if (clusterProvider.serverOptions.FastMigrate)
                _ = Task.Run(() => Process(basicGarnetApi, buffer, replaceOption, vectorSetOption));
            else
                Process(basicGarnetApi, buffer, replaceOption, vectorSetOption);

            void Process(BasicGarnetApi basicGarnetApi, byte[] input, bool replaceOption, bool vectorSetOption)
            {
                var currentConfig = clusterProvider.clusterManager.CurrentConfig;
                byte migrateState = 0;

                fixed (byte* ptr = input)
                {
                    var payloadPtr = ptr;
                    var payloadEndPtr = ptr + input.Length;

                    var keyCount = *(int*)payloadPtr;
                    payloadPtr += sizeof(int);
                    var i = 0;

                    TrackImportProgress(keyCount, keyCount == 0);
                    var storeWrapper = clusterProvider.storeWrapper;
                    var transientObjectIdMap = storeWrapper.store.Log.TransientObjectIdMap;

                    DiskLogRecord diskLogRecord = default;
                    try
                    {
                        if (vectorSetOption)
                        {
                            // Vector Sets need special handling
                            while (i < keyCount)
                            {
                                var kind = (MigrationRecordSpanType)(*payloadPtr);
                                payloadPtr++;

                                if (!RespReadUtils.GetSerializedRecordSpan(out var payloadRaw, ref payloadPtr, payloadEndPtr))
                                    return;

                                if (kind != MigrationRecordSpanType.VectorSetIndex)
                                    throw new InvalidOperationException($"Unexpected {nameof(MigrationRecordSpanType)}: {kind}");

                                var payload = payloadRaw.ReadOnlySpan;

                                VectorManager.DeserializeMigratedIndexKey(payload, out var keyBytes, out var valueBytes);

                                // An error has occurred
                                if (migrateState > 0)
                                {
                                    i++;
                                    continue;
                                }

                                clusterProvider.storeWrapper.DefaultDatabase.VectorManager.HandleMigratedIndexKey(clusterProvider.storeWrapper.DefaultDatabase, clusterProvider.storeWrapper, keyBytes, valueBytes);
                                i++;
                            }
                        }
                        else
                        {
                            while (i < keyCount)
                            {
                                var kind = (MigrationRecordSpanType)(*payloadPtr);
                                payloadPtr++;

                                if (kind == MigrationRecordSpanType.ChunkedLogRecord)
                                {
                                    // A record too large for one send buffer arrives as chunks (possibly across commands):
                                    // [int chunkLength | continuation][chunk bytes]. GetSerializedRecordSpan cannot read these
                                    // because the continuation flag makes the length read as negative.
                                    if (payloadPtr + sizeof(int) > payloadEndPtr)
                                        return;
                                    var rawChunkLength = *(int*)payloadPtr;
                                    payloadPtr += sizeof(int);
                                    var moreChunksFollow = (rawChunkLength & ChunkedRecordConstants.ContinuationFlag) != 0;
                                    var chunkLength = rawChunkLength & ~ChunkedRecordConstants.ContinuationFlag;
                                    if (chunkLength < 0 || payloadPtr + chunkLength > payloadEndPtr)
                                        return;
                                    var chunkSpan = new ReadOnlySpan<byte>(payloadPtr, chunkLength);
                                    payloadPtr += chunkLength;

                                    // An error has occurred; keep consuming chunks but do not process.
                                    if (migrateState > 0)
                                    {
                                        chunkedRecordReassembler?.Reset();
                                        i++;
                                        continue;
                                    }

                                    chunkedRecordReassembler ??= new();
                                    if (chunkedRecordReassembler.Append(chunkSpan, moreChunksFollow))
                                    {
                                        var isObject = CompleteChunkedRecord(storeWrapper, out var contiguous, out var header, out var valueObject);
                                        chunkedRecordReassembler.Reset();
                                        var recordBytes = isObject ? header : contiguous;
                                        fixed (byte* recordPtr = recordBytes)
                                        {
                                            var recordSpan = PinnedSpanByte.FromPinnedPointer(recordPtr, recordBytes.Length);
                                            diskLogRecord = isObject
                                                ? DiskLogRecord.DeserializeChunkedObject(recordSpan, valueObject, transientObjectIdMap)
                                                : DiskLogRecord.DeserializeChunked(recordSpan, storeWrapper.GarnetObjectSerializer, transientObjectIdMap, storeWrapper.storeFunctions);

                                            var slot = HashSlotUtils.HashSlot(diskLogRecord.Key);
                                            if (!currentConfig.IsImportingSlot(slot)) // Slot is not in importing state
                                            {
                                                migrateState = 1;
                                            }
                                            else
                                            {
                                                // Set if key replace flag is set or key does not exist
                                                var keySlice = PinnedSpanByte.FromPinnedSpan(diskLogRecord.Key);
                                                if (replaceOption || !Exists(keySlice))
                                                    _ = basicGarnetApi.SET(in diskLogRecord);
                                            }

                                            storeWrapper.storeFunctions.OnDisposeDiskRecord(ref diskLogRecord, DisposeReason.DeserializedFromDisk);
                                            diskLogRecord.Dispose();
                                            diskLogRecord = default; // prevent double-trigger in finally
                                        }
                                    }

                                    i++;
                                    continue;
                                }

                                if (!RespReadUtils.GetSerializedRecordSpan(out var payloadRaw, ref payloadPtr, payloadEndPtr))
                                    return;

                                // An error has occurred
                                if (migrateState > 0)
                                {
                                    i++;
                                    continue;
                                }

                                // Protocol enforcement: while receiving a RangeIndex stream, only SerializedRangeIndexStream records are valid
                                if (clusterProvider.serverOptions.EnableRangeIndexPreview && rangeIndexMigrationState.IsReceiving && kind != MigrationRecordSpanType.SerializedRangeIndexStream)
                                {
                                    logger?.LogError("Protocol violation: expected SerializedRangeIndexStream continuation after {ChunkCount} chunks, got {Kind}", rangeIndexMigrationState.CurrentChunkCount, kind);
                                    migrateState = 1;
                                    i++;
                                    continue;
                                }

                                if (kind == MigrationRecordSpanType.VectorSetElement)
                                {
                                    // This is a Vector Set namespace key being migrated - it won't necessarily look like it's "in" a hash slot
                                    // because it's dependent on some other key (the index key) being migrated which itself is in a moving hash slot

                                    // Vector Set elements are Namespace + Key + Value

                                    var payload = payloadRaw.Span;

                                    VectorManager.DeserializeMigratedElementKey(payload, out var namespaceBytes, out var keyBytes, out var valueBytes);

                                    // An error has occurred
                                    if (migrateState > 0)
                                    {
                                        i++;
                                        continue;
                                    }

                                    clusterProvider.storeWrapper.DefaultDatabase.VectorManager.HandleMigratedElementKey(ref stringBasicContext, ref vectorBasicContext, namespaceBytes, keyBytes, valueBytes);
                                }
                                else if (kind == MigrationRecordSpanType.SerializedRangeIndexStream)
                                {
                                    if (!clusterProvider.serverOptions.EnableRangeIndexPreview)
                                    {
                                        logger?.LogError("Received RangeIndex migration data but RangeIndex feature is not enabled");
                                        migrateState = 1;
                                        i++;
                                        continue;
                                    }

                                    if (!rangeIndexMigrationState.ProcessRecord(payloadRaw.ReadOnlySpan, currentConfig, ref stringBasicContext, replaceOption))
                                    {
                                        logger?.LogError("Failed to process RangeIndex migration record");
                                        migrateState = 1;
                                        i++;
                                        continue;
                                    }
                                }
                                else if (kind == MigrationRecordSpanType.LogRecord)
                                {
                                    // An error has occurred
                                    if (migrateState > 0)
                                    {
                                        i++;
                                        continue;
                                    }

                                    diskLogRecord = DiskLogRecord.Deserialize(payloadRaw, storeWrapper.GarnetObjectSerializer,
                                        transientObjectIdMap, storeWrapper.storeFunctions);

                                    var slot = HashSlotUtils.HashSlot(diskLogRecord.Key);
                                    if (!currentConfig.IsImportingSlot(slot)) // Slot is not in importing state
                                    {
                                        migrateState = 1;
                                        i++;
                                        continue;
                                    }

                                    // Set if key replace flag is set or key does not exist
                                    var keySlice = PinnedSpanByte.FromPinnedSpan(diskLogRecord.Key);
                                    if (replaceOption || !Exists(keySlice))
                                        _ = basicGarnetApi.SET(in diskLogRecord);

                                    storeWrapper.storeFunctions.OnDisposeDiskRecord(ref diskLogRecord, DisposeReason.DeserializedFromDisk);
                                    diskLogRecord.Dispose();
                                    diskLogRecord = default; // prevent double-trigger in finally
                                }
                                else
                                {
                                    throw new InvalidOperationException($"Unexpected {nameof(MigrationRecordSpanType)}: {kind}");
                                }

                                i++;
                            }
                        }
                    }
                    finally
                    {
                        if (diskLogRecord.IsSet)
                        {
                            storeWrapper.storeFunctions.OnDisposeDiskRecord(ref diskLogRecord, DisposeReason.DeserializedFromDisk);
                            diskLogRecord.Dispose();
                        }
                    }
                }
            }

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER MTASKS command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterMTasks(out bool invalidParameters)
        {
            invalidParameters = false;

            if (parseState.Count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var mtasks = clusterProvider.migrationManager.GetMigrationTaskCount();
            while (!RespWriteUtils.TryWriteInt32(mtasks, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}