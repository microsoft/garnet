// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
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

                    // Use try/finally instead of "using" because we don't want the boxing that an interface call would entail. Double-Dispose() is OK for DiskLogRecord.
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
                                {
                                    throw new InvalidOperationException($"Unexpected {nameof(MigrationRecordSpanType)}: {kind}");
                                }

                                var payload = payloadRaw.ReadOnlySpan;

                                // Vector Set indexes are Key + Value
                                var keyLen = BinaryPrimitives.ReadInt32LittleEndian(payload);
                                var keyBytes = payload.Slice(sizeof(int), keyLen);
                                var valueLen = BinaryPrimitives.ReadInt32LittleEndian(payload[(sizeof(int) + keyBytes.Length)..]);
                                var valueBytes = payload.Slice(sizeof(int) + keyBytes.Length + sizeof(int), valueLen);

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

                                if (!RespReadUtils.GetSerializedRecordSpan(out var payloadRaw, ref payloadPtr, payloadEndPtr))
                                    return;

                                if (kind == MigrationRecordSpanType.VectorSetElement)
                                {
                                    // This is a Vector Set namespace key being migrated - it won't necessarily look like it's "in" a hash slot
                                    // because it's dependent on some other key (the index key) being migrated which itself is in a moving hash slot

                                    // Vector Set elements are Namespace + Key + Value

                                    var payload = payloadRaw.ReadOnlySpan;

                                    var namespaceLen = BinaryPrimitives.ReadInt32LittleEndian(payload);
                                    var namespaceBytes = payload.Slice(sizeof(int), namespaceLen);
                                    var keyLen = BinaryPrimitives.ReadInt32LittleEndian(payload[(sizeof(int) + namespaceBytes.Length)..]);
                                    var keyBytes = payload.Slice(sizeof(int) + namespaceLen + sizeof(int), keyLen);
                                    var valueLen = BinaryPrimitives.ReadInt32LittleEndian(payload[(sizeof(int) + namespaceBytes.Length + sizeof(int) + keyBytes.Length)..]);
                                    var valueBytes = payload.Slice(sizeof(int) + namespaceLen + sizeof(int) + keyBytes.Length + sizeof(int), valueLen);

                                    // An error has occurred
                                    if (migrateState > 0)
                                    {
                                        i++;
                                        continue;
                                    }

                                    clusterProvider.storeWrapper.DefaultDatabase.VectorManager.HandleMigratedElementKey(ref stringBasicContext, ref vectorBasicContext, namespaceBytes, keyBytes, valueBytes);
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