// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    using BasicGarnetApi = GarnetApi<BasicContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        BasicContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        BasicContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>>;

    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        long lastLog = 0;
        long totalKeyCount = 0;

        /// <summary>
        /// Logging of migrate session status
        /// </summary>
        /// <param name="keyCount"></param>
        /// <param name="isMainStore"></param>
        /// <param name="completed"></param>
        private void TrackImportProgress(int keyCount, bool isMainStore, bool completed = false)
        {
            totalKeyCount += keyCount;
            var duration = TimeSpan.FromTicks(Stopwatch.GetTimestamp() - lastLog);
            if (completed || lastLog == 0 || duration >= clusterProvider.storeWrapper.loggingFrequency)
            {
                logger?.LogTrace("[{op}]: isMainStore:({storeType}) totalKeyCount:({totalKeyCount})", completed ? "COMPLETED" : "IMPORTING", isMainStore, totalKeyCount.ToString("N0"));
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
            var storeType = parseState.GetArgSliceByRef(2).ReadOnlySpan;
            var payloadStartPtr = parseState.GetArgSliceByRef(3).ToPointer();
            var lastParam = parseState.GetArgSliceByRef(parseState.Count - 1);
            var payloadEndPtr = lastParam.ToPointer() + lastParam.Length;
            var replaceOption = replace.EqualsUpperCaseSpanIgnoringCase("T"u8);

            var storeTypeStr = Encoding.ASCII.GetString(storeType);
            var buffer = new Span<byte>(payloadStartPtr, (int)(payloadEndPtr - payloadStartPtr)).ToArray();

            if (clusterProvider.serverOptions.FastMigrate)
                _ = Task.Run(() => Process(basicGarnetApi, buffer, storeTypeStr, replaceOption));
            else
                Process(basicGarnetApi, buffer, storeTypeStr, replaceOption);

            void Process(BasicGarnetApi basicGarnetApi, byte[] input, string storeTypeSpan, bool replaceOption)
            {
                var currentConfig = clusterProvider.clusterManager.CurrentConfig;
                byte migrateState = 0;

                fixed (byte* ptr = input)
                {
                    var payloadPtr = ptr;
                    var payloadEndPtr = ptr + input.Length;
                    if (storeTypeSpan.Equals("SSTORE", StringComparison.OrdinalIgnoreCase))
                    {
                        var keyCount = *(int*)payloadPtr;
                        payloadPtr += 4;
                        var i = 0;

                        TrackImportProgress(keyCount, isMainStore: true, keyCount == 0);
                        while (i < keyCount)
                        {
                            if (!RespReadUtils.TryReadSerializedRecord(out var startAddress, out var length, ref payloadPtr, payloadEndPtr))
                                return;

                            // An error has occurred
                            if (migrateState > 0)
                            {
                                i++;
                                continue;
                            }

                            var diskLogRecord = new DiskLogRecord(startAddress, length);
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
                                _ = basicGarnetApi.SET(in diskLogRecord, StoreType.Main);
                            i++;
                        }
                    }
                    else if (storeTypeSpan.Equals("OSTORE", StringComparison.OrdinalIgnoreCase))
                    {
                        var keyCount = *(int*)payloadPtr;
                        payloadPtr += 4;
                        var i = 0;
                        TrackImportProgress(keyCount, isMainStore: false, keyCount == 0);
                        while (i < keyCount)
                        {
                            if (!RespReadUtils.TryReadSerializedRecord(out var startAddress, out var length, ref payloadPtr, payloadEndPtr))
                                return;

                            // An error has occurred
                            if (migrateState > 0)
                                continue;

                            var diskLogRecord = new DiskLogRecord(startAddress, length);
                            var slot = HashSlotUtils.HashSlot(diskLogRecord.Key);
                            if (!currentConfig.IsImportingSlot(slot)) // Slot is not in importing state
                            {
                                migrateState = 1;
                                continue;
                            }

                            _ = diskLogRecord.DeserializeValueObject(clusterProvider.storeWrapper.GarnetObjectSerializer);

                            // Set if key replace flag is set or key does not exist
                            var keySlice = PinnedSpanByte.FromPinnedSpan(diskLogRecord.Key);
                            if (replaceOption || !Exists(keySlice))
                                _ = basicGarnetApi.SET(in diskLogRecord, StoreType.Object);
                            i++;
                        }
                    }
                    else
                    {
                        throw new Exception("CLUSTER MIGRATE STORE TYPE ERROR!");
                    }
                }
            }

            var currentConfig = clusterProvider.clusterManager.CurrentConfig;

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