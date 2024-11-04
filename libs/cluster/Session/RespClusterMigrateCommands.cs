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
            if (completed || lastLog == 0 || duration >= clusterProvider.storeWrapper.loggingFrequncy)
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
            var payload = parseState.GetArgSliceByRef(3).SpanByte;
            var payloadPtr = payload.ToPointer();
            var lastParam = parseState.GetArgSliceByRef(parseState.Count - 1).SpanByte;
            var payloadEndPtr = lastParam.ToPointer() + lastParam.Length;

            var replaceOption = replaceSpan.EqualsUpperCaseSpanIgnoringCase("T"u8);

            var currentConfig = clusterProvider.clusterManager.CurrentConfig;
            byte migrateState = 0;

            if (storeTypeSpan.EqualsUpperCaseSpanIgnoringCase("SSTORE"u8))
            {
                var keyCount = *(int*)payloadPtr;
                payloadPtr += 4;
                var i = 0;

                TrackImportProgress(keyCount, isMainStore: true, keyCount == 0);
                while (i < keyCount)
                {
                    ref var key = ref SpanByte.Reinterpret(payloadPtr);
                    payloadPtr += key.TotalSize;
                    ref var value = ref SpanByte.Reinterpret(payloadPtr);
                    payloadPtr += value.TotalSize;

                    // An error has occurred
                    if (migrateState > 0)
                    {
                        i++;
                        continue;
                    }

                    var slot = HashSlotUtils.HashSlot(ref key);
                    if (!currentConfig.IsImportingSlot(slot)) // Slot is not in importing state
                    {
                        migrateState = 1;
                        i++;
                        continue;
                    }

                    // Set if key replace flag is set or key does not exist
                    var keySlice = new ArgSlice(key.ToPointer(), key.Length);
                    if (replaceOption || !Exists(ref keySlice))
                        _ = garnetApi.SET(ref key, ref value);
                    i++;
                }
            }
            else if (storeTypeSpan.EqualsUpperCaseSpanIgnoringCase("OSTORE"u8))
            {
                var keyCount = *(int*)payloadPtr;
                payloadPtr += 4;
                var i = 0;
                TrackImportProgress(keyCount, isMainStore: true, keyCount == 0);
                while (i < keyCount)
                {
                    if (!RespReadUtils.ReadSerializedData(out var key, out var data, out var expiration, ref payloadPtr, payloadEndPtr))
                        return false;

                    // An error has occurred
                    if (migrateState > 0)
                        continue;

                    var slot = HashSlotUtils.HashSlot(key);
                    if (!currentConfig.IsImportingSlot(slot)) // Slot is not in importing state
                    {
                        migrateState = 1;
                        continue;
                    }

                    var value = clusterProvider.storeWrapper.GarnetObjectSerializer.Deserialize(data);
                    value.Expiration = expiration;

                    // Set if key replace flag is set or key does not exist
                    if (replaceOption || !CheckIfKeyExists(key))
                        _ = garnetApi.SET(key, value);

                    i++;
                }
            }
            else
            {
                throw new Exception("CLUSTER MIGRATE STORE TYPE ERROR!");
            }

            if (migrateState == 1)
            {
                logger?.LogError("{errorMsg}", Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_NOT_IN_IMPORTING_STATE));
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_NOT_IN_IMPORTING_STATE, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
            while (!RespWriteUtils.WriteInteger(mtasks, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}