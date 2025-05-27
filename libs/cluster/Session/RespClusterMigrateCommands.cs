// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;
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

            var replaceSpan = parseState.GetArgSliceByRef(1).ReadOnlySpan;
            var storeTypeSpan = parseState.GetArgSliceByRef(2).ReadOnlySpan;
            var payload = parseState.GetArgSliceByRef(3);
            var payloadPtr = payload.ToPointer();
            var lastParam = parseState.GetArgSliceByRef(parseState.Count - 1);
            var payloadEndPtr = lastParam.ToPointer() + lastParam.Length;

            var replaceOption = replaceSpan.EqualsUpperCaseSpanIgnoringCase("T"u8);

            var currentConfig = clusterProvider.clusterManager.CurrentConfig;
            var migrateError = false;

            if (storeTypeSpan.EqualsUpperCaseSpanIgnoringCase("SSTORE"u8))
            {
                var keyCount = *(int*)payloadPtr;
                payloadPtr += 4;
                TrackImportProgress(keyCount, isMainStore: true, keyCount == 0);

                for (var ii = 0; ii < keyCount; ii++)
                {
                    if (!RespReadUtils.TryReadSerializedRecord(out var startAddress, out var length, ref payloadPtr, payloadEndPtr))
                        return false;

                    // If an error has occurred, continue to drain all records
                    if (migrateError)
                        continue;

                    var diskLogRecord = new DiskLogRecord(startAddress, length);
                    var slot = HashSlotUtils.HashSlot(diskLogRecord.Key);
                    if (!currentConfig.IsImportingSlot(slot)) // Slot is not in importing state
                    {
                        migrateError = true;
                        continue;
                    }

                    // Set if key replace flag is set or key does not exist
                    if (replaceOption || !Exists(PinnedSpanByte.FromPinnedSpan(diskLogRecord.Key)))
                        _ = basicGarnetApi.SET(ref diskLogRecord, StoreType.Main);
                }
            }
            else if (storeTypeSpan.EqualsUpperCaseSpanIgnoringCase("OSTORE"u8))
            {
                var keyCount = *(int*)payloadPtr;
                payloadPtr += 4;
                TrackImportProgress(keyCount, isMainStore: false, keyCount == 0);

                for (var ii = 0; ii < keyCount; ii++)
                {
                    if (!RespReadUtils.TryReadSerializedRecord(out var startAddress, out var length, ref payloadPtr, payloadEndPtr))
                        return false;

                    // If an error has occurred, continue to drain all records
                    if (migrateError)
                        continue;

                    var diskLogRecord = new DiskLogRecord(startAddress, length);
                    var slot = HashSlotUtils.HashSlot(diskLogRecord.Key);
                    if (!currentConfig.IsImportingSlot(slot)) // Slot is not in importing state
                    {
                        migrateError = true;
                        continue;
                    }

                    // Set if key replace flag is set or key does not exist
                    if (replaceOption || !Exists(PinnedSpanByte.FromPinnedSpan(diskLogRecord.Key)))
                    {
                        _ = diskLogRecord.DeserializeValueObject(clusterProvider.storeWrapper.GarnetObjectSerializer);
                        _ = basicGarnetApi.SET(ref diskLogRecord, StoreType.Object);
                    }
                }
            }
            else
            {
                throw new Exception("CLUSTER MIGRATE STORE TYPE ERROR!");
            }

            if (migrateError)
            {
                logger?.LogError("{errorMsg}", Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_NOT_IN_IMPORTING_STATE));
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_NOT_IN_IMPORTING_STATE, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }

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