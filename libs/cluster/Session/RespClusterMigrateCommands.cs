// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        /// <summary>
        /// Implements CLUSTER MIGRATE command (only for internode use)
        /// </summary>
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private bool NetworkClusterMigrate(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 3 arguments
            if (count != 3)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadStringWithLengthHeader(out var sourceNodeId, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadStringWithLengthHeader(out var _replace, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadStringWithLengthHeader(out var storeType, ref ptr, recvBufferPtr + bytesRead))
                return false;

            var replaceOption = _replace.Equals("T");

            // Check if payload size has been received
            if (ptr + 4 > recvBufferPtr + bytesRead)
                return false;

            var headerLength = *(int*)ptr;
            ptr += 4;
            // Check if payload has been received
            if (ptr + headerLength > recvBufferPtr + bytesRead)
                return false;

            var currentConfig = clusterProvider.clusterManager.CurrentConfig;

            if (storeType.Equals("SSTORE"))
            {
                var keyCount = *(int*)ptr;
                ptr += 4;
                var i = 0;

                while (i < keyCount)
                {

                    byte* keyPtr = null, valPtr = null;
                    byte keyMetaDataSize = 0, valMetaDataSize = 0;
                    if (!RespReadUtils.ReadSerializedSpanByte(ref keyPtr, ref keyMetaDataSize, ref valPtr, ref valMetaDataSize, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    ref var key = ref SpanByte.Reinterpret(keyPtr);
                    if (keyMetaDataSize > 0) key.ExtraMetadata = *(long*)(keyPtr + 4);
                    ref var value = ref SpanByte.Reinterpret(valPtr);
                    if (valMetaDataSize > 0) value.ExtraMetadata = *(long*)(valPtr + 4);

                    // An error has occurred
                    if (migrateState > 0)
                    {
                        i++;
                        continue;
                    }

                    var slot = HashSlotUtils.HashSlot(key.ToPointer(), key.LengthWithoutMetadata);
                    if (!currentConfig.IsImportingSlot(slot))//Slot is not in importing state
                    {
                        migrateState = 1;
                        i++;
                        continue;
                    }

                    if (i < migrateSetCount)
                        continue;

                    migrateSetCount++;

                    // Set if key replace flag is set or key does not exist
                    if (replaceOption || !CheckIfKeyExists(new ArgSlice(key.ToPointer(), key.Length)))
                        _ = basicGarnetApi.SET(ref key, ref value);
                    i++;
                }
            }
            else if (storeType.Equals("OSTORE"))
            {
                var keyCount = *(int*)ptr;
                ptr += 4;
                var i = 0;
                while (i < keyCount)
                {
                    if (!RespReadUtils.ReadSerializedData(out var key, out var data, out var expiration, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    // An error has occurred
                    if (migrateState > 0)
                        continue;

                    var slot = HashSlotUtils.HashSlot(key);
                    if (!currentConfig.IsImportingSlot(slot))//Slot is not in importing state
                    {
                        migrateState = 1;
                        continue;
                    }

                    if (i < migrateSetCount)
                        continue;

                    migrateSetCount++;

                    var value = clusterProvider.storeWrapper.GarnetObjectSerializer.Deserialize(data);
                    value.Expiration = expiration;

                    // Set if key replace flag is set or key does not exist
                    if (replaceOption || !CheckIfKeyExists(key))
                        _ = basicGarnetApi.SET(key, value);

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

            migrateSetCount = 0;
            migrateState = 0;
            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Implements CLUSTER MTASKS command
        /// </summary>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterMTasks(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var mtasks = clusterProvider.migrationManager.GetMigrationTaskCount();
            while (!RespWriteUtils.WriteInteger(mtasks, ref dcurr, dend))
                SendAndReset();
            var ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);

            return true;
        }
    }
}