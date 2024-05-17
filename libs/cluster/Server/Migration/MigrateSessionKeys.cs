// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Method used to migrate individual keys from main store to target node.
        /// Used for MIGRATE KEYS option
        /// </summary>
        /// <param name="keys">List of pairs of address to the network receive buffer, key size </param>
        /// <param name="objectStoreKeys">Output keys not found in main store so we can scan the object store next</param>
        /// <returns>True on success, false otherwise</returns>
        private bool MigrateKeysFromMainStore(ref List<ArgSlice> keys, out List<ArgSlice> objectStoreKeys)
        {
            var bufferSize = 1 << 10;
            SectorAlignedMemory buffer = new(bufferSize, 1);
            objectStoreKeys = [];

            try
            {
                // 4 byte length of input
                // 1 byte RespCommand
                // 1 byte RespInputFlags
                var inputSize = sizeof(int) + RespInputHeader.Size;
                var pbCmdInput = stackalloc byte[inputSize];

                ////////////////
                // Build Input//
                ////////////////
                var pcurr = pbCmdInput;
                *(int*)pcurr = inputSize - sizeof(int);
                pcurr += sizeof(int);
                // 1. Header
                ((RespInputHeader*)(pcurr))->SetHeader(RespCommandAccessor.MIGRATE, 0);

                var bufPtr = buffer.GetValidPointer();
                var bufPtrEnd = bufPtr + bufferSize;
                for (var i = 0; i < keys.Count; i++)
                {
                    var key = keys[i].SpanByte;

                    // Read value for key
                    var o = new SpanByteAndMemory(bufPtr, (int)(bufPtrEnd - bufPtr));
                    var status = localServerSession.BasicGarnetApi.Read_MainStore(ref key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref o);

                    // Check if found
                    if (status == GarnetStatus.NOTFOUND) // All keys must exist
                    {
                        // Add to different list to check object store
                        objectStoreKeys.Add(keys[i]);
                        continue;
                    }

                    // Make value SpanByte
                    SpanByte value;
                    MemoryHandle memoryHandle = default;
                    if (!o.IsSpanByte)
                    {
                        memoryHandle = o.Memory.Memory.Pin();
                        value = SpanByte.FromPinnedMemory(o.Memory.Memory);
                    }
                    else
                        value = o.SpanByte;

                    if (!ClusterSession.Expired(ref value) && !WriteOrSendMainStoreKeyValuePair(ref key, ref value))
                        return false;

                    if (!o.IsSpanByte)
                    {
                        memoryHandle.Dispose();
                        o.Memory.Dispose();
                    }
                }

                // Flush data in client buffer
                if (!HandleMigrateTaskResponse(_gcs.SendAndResetMigrate()))
                    return false;
                return true;
            }
            finally
            {
                buffer.Dispose();
            }
        }

        /// <summary>
        /// Method used to migrate individual keys from object store to target node.
        /// Used for MIGRATE KEYS option
        /// </summary>
        /// <param name="objectStoreKeys">List of pairs of address to the network receive buffer, key size that were not found in main store</param>
        /// <returns>True on success, false otherwise</returns>
        private bool MigrateKeysFromObjectStore(ref List<ArgSlice> objectStoreKeys)
        {
            for (var i = 0; i < objectStoreKeys.Count; i++)
            {
                var key = objectStoreKeys[i].ToArray();

                SpanByte input = default;
                GarnetObjectStoreOutput value = default;
                var status = localServerSession.BasicGarnetApi.Read_ObjectStore(ref key, ref input, ref value);
                if (status == GarnetStatus.NOTFOUND)
                    continue;

                if (!ClusterSession.Expired(ref value.garnetObject))
                {
                    var objectData = GarnetObjectSerializer.Serialize(value.garnetObject);

                    if (!WriteOrSendObjectStoreKeyValuePair(key, objectData, value.garnetObject.Expiration))
                        return false;
                }
            }

            // Flush data in client buffer
            if (!HandleMigrateTaskResponse(_gcs.SendAndResetMigrate()))
                return false;
            return true;
        }

        /// <summary>
        /// Method used to migrate keys from main and object stores.
        /// Used for MIGRATE KEYS option
        /// </summary>
        public bool MigrateKeys()
        {
            var keys = _keys;
            try
            {
                if (!CheckConnection())
                    return false;
                _gcs.InitMigrateBuffer();
                if (!MigrateKeysFromMainStore(ref keys, out var objectStoreKeys))
                    return false;

                if (!clusterProvider.serverOptions.DisableObjects && objectStoreKeys.Count > 0)
                {
                    _gcs.InitMigrateBuffer();
                    if (!MigrateKeysFromObjectStore(ref objectStoreKeys))
                        return false;
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred");
            }
            return true;
        }

        /// <summary>
        /// Delete local copy of keys if _copyOption is set to false.
        /// </summary>                
        public void DeleteKeys(List<ArgSlice> keys)
        {
            if (_copyOption)
                return;

            for (var i = 0; i < keys.Count; i++)
            {
                var key = keys[i].SpanByte;
                localServerSession.BasicGarnetApi.DELETE(ref key);
            }
        }
    }
}