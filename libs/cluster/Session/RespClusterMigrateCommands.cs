// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    using BasicGarnetApi = GarnetApi<BasicContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        BasicContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        BasicContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions,
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

            // Expecting exactly 3 arguments
            if (parseState.Count != 3)
            {
                invalidParameters = true;
                return true;
            }

            var replace = parseState.GetArgSliceByRef(1).ReadOnlySpan;
            var payloadStartPtr = parseState.GetArgSliceByRef(2).ToPointer();
            var lastParam = parseState.GetArgSliceByRef(parseState.Count - 1);
            var payloadEndPtr = lastParam.ToPointer() + lastParam.Length;
            var replaceOption = replace.EqualsUpperCaseSpanIgnoringCase("T"u8);

            var buffer = new Span<byte>(payloadStartPtr, (int)(payloadEndPtr - payloadStartPtr)).ToArray();

            if (clusterProvider.serverOptions.FastMigrate)
                _ = Task.Run(() => Process(basicGarnetApi, buffer, replaceOption));
            else
                Process(basicGarnetApi, buffer, replaceOption);

            void Process(BasicGarnetApi basicGarnetApi, byte[] input, bool replaceOption)
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
                        while (i < keyCount)
                        {
                            if (!RespReadUtils.GetSerializedRecordSpan(out var recordSpan, ref payloadPtr, payloadEndPtr))
                                return;

                            // An error has occurred
                            if (migrateState > 0)
                            {
                                i++;
                                continue;
                            }

                            diskLogRecord = DiskLogRecord.Deserialize(recordSpan, storeWrapper.GarnetObjectSerializer,
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

                            diskLogRecord.Dispose();
                            i++;
                        }
                    }
                    finally
                    {
                        diskLogRecord.Dispose();
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