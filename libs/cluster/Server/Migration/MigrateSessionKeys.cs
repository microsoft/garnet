// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        /*
         * Brief overview on the migration state machine
         * 1. At the primary slot is set into migrating state before data migration begins
         * 2. Client is responsible for setting migrating state
         * 3. We use epoch protection to ensure that slot transition is visible to all sessions before responding with +OK to client that requested the transition (check RespClusterSlotManagementCommands.cs -> NetworkClusterSetSlot).
         * 4. We can add another level of protection when MIGRATE KEYS is called we should check slot is in MIGRATING state and wait for config transition.
         * 5. To ensure high availability we need to allow access to keys that exist and redirect requests to target node using -ASK otherwise.
         * 6. If slot is in MIGRATING state and key exists we need to ensure that the key will not moved or be prepared for migration until operation completes.
         * 7. The MIGRATE task will add
         */

        private void TransitionKeyState(KeyMigrateState state)
        {
            foreach (var key in _keys.Keys)
            {
                _keys[key] = _keys[key] switch
                {
                    // 1. Transition from PENDING to MIGRATING
                    KeyMigrateState.PENDING when state == KeyMigrateState.MIGRATING => state,
                    // 2. Transition from MIGRATING to MIGRATED
                    KeyMigrateState.MIGRATING when state == KeyMigrateState.MIGRATED => state,
                    // 3. Omit state transition
                    _ => _keys[key],
                };
            }
        }

        /// <summary>
        /// Method used to migrate individual keys from main store to target node.
        /// Used with MIGRATE KEYS option
        /// </summary>
        /// <returns>True on success, false otherwise</returns>
        private bool MigrateKeysFromMainStore()
        {
            var bufferSize = 1 << 10;
            SectorAlignedMemory buffer = new(bufferSize, 1);

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
                ((RespInputHeader*)pcurr)->SetHeader(RespCommandAccessor.MIGRATE, 0);

                var bufPtr = buffer.GetValidPointer();
                var bufPtrEnd = bufPtr + bufferSize;
                foreach (var mKey in _keys)
                {
                    var key = mKey.Key.SpanByte;

                    // Read value for key
                    var o = new SpanByteAndMemory(bufPtr, (int)(bufPtrEnd - bufPtr));
                    var status = localServerSession.BasicGarnetApi.Read_MainStore(ref key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref o);

                    // Check if found
                    if (status == GarnetStatus.NOTFOUND) // All keys must exist
                    {
                        _keys[mKey.Key] = KeyMigrateState.PENDING;
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
                // Transition keys to MIGRATED state
                TransitionKeyState(KeyMigrateState.MIGRATED);
                buffer.Dispose();
            }
        }

        /// <summary>
        /// Method used to migrate individual keys from object store to target node.
        /// Used with MIGRATE KEYS option
        /// </summary>
        /// <returns>True on success, false otherwise</returns>
        private bool MigrateKeysFromObjectStore()
        {
            try
            {
                foreach (var mKey in _keys)
                {
                    if (mKey.Value != KeyMigrateState.MIGRATING)
                        continue;
                    var key = mKey.Key.ToArray();

                    SpanByte input = default;
                    GarnetObjectStoreOutput value = default;
                    var status = localServerSession.BasicGarnetApi.Read_ObjectStore(ref key, ref input, ref value);
                    if (status == GarnetStatus.NOTFOUND)
                    {
                        // Ensure key goes back to PENDING state
                        _keys[mKey.Key] = KeyMigrateState.PENDING;
                        continue;
                    }

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
            }
            finally
            {
                // Transition keys to MIGRATED state
                TransitionKeyState(KeyMigrateState.MIGRATED);
            }
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

                // Transition keys to MIGRATING state
                TransitionKeyState(KeyMigrateState.MIGRATING);

                _gcs.InitMigrateBuffer();
                if (!MigrateKeysFromMainStore())
                    return false;

                if (!clusterProvider.serverOptions.DisableObjects)
                {
                    // Transition all remaining keys (i.e. keys not found in main store) to MIGRATING state
                    TransitionKeyState(KeyMigrateState.MIGRATING);
                    _gcs.InitMigrateBuffer();
                    if (!MigrateKeysFromObjectStore())
                        return false;
                }

                // Delete keys locally if  _copyOption is set to false.
                if (!_copyOption)
                    DeleteKeys();
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
        public void DeleteKeys()
        {
            if (_copyOption)
                return;

            foreach (var mKey in _keys)
            {
                var key = mKey.Key.SpanByte;
                localServerSession.BasicGarnetApi.DELETE(ref key);
            }
        }
    }
}