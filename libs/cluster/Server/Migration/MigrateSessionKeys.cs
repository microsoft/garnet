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
    /// <summary>
    /// This code implements operations associated with the MIGRATE KEYS transfer option.
    /// </summary>
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
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
                var inputHeader = new RawStringInput();

                // Transition keys to MIGRATING status
                TryTransitionState(KeyMigrationStatus.MIGRATING);
                WaitForConfigPropagation();

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
                foreach (var mKey in _keys.GetKeys())
                {
                    // Process only keys in MIGRATING status
                    if (mKey.Value != KeyMigrationStatus.MIGRATING)
                        continue;

                    var key = mKey.Key.SpanByte;

                    // Read value for key
                    var o = new SpanByteAndMemory(bufPtr, (int)(bufPtrEnd - bufPtr));
                    var status = localServerSession.BasicGarnetApi.Read_MainStore(ref key, ref inputHeader, ref o);

                    // Check if found in main store
                    if (status == GarnetStatus.NOTFOUND)
                    {
                        // Transition key status back to QUEUED to unblock any writers
                        _keys.UpdateStatus(mKey.Key, KeyMigrationStatus.QUEUED);
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
                DeleteKeys();
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
                // NOTE: Any keys not found in main store are automatically set to QUEUED before this method is called
                // Transition all QUEUED to MIGRATING state
                TryTransitionState(KeyMigrationStatus.MIGRATING);
                WaitForConfigPropagation();

                foreach (var mKey in _keys.GetKeys())
                {
                    // Process only keys in MIGRATING status
                    if (mKey.Value != KeyMigrationStatus.MIGRATING)
                        continue;
                    var key = mKey.Key.ToArray();

                    ObjectInput input = default;
                    GarnetObjectStoreOutput value = default;
                    var status = localServerSession.BasicGarnetApi.Read_ObjectStore(ref key, ref input, ref value);
                    if (status == GarnetStatus.NOTFOUND)
                    {
                        // Transition key status back to QUEUED to unblock any writers
                        _keys.UpdateStatus(mKey.Key, KeyMigrationStatus.QUEUED);
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
                // Delete keys if COPY option is false or transition KEYS from MIGRATING to MIGRATED status
                DeleteKeys();
            }
            return true;
        }

        /// <summary>
        /// Delete local copy of keys if _copyOption is set to false.
        /// </summary>
        private void DeleteKeys()
        {
            if (_copyOption)
            {
                // Set key as MIGRATED to unblock readers and writers waiting for this key
                TryTransitionState(KeyMigrationStatus.MIGRATED);
                return;
            }

            // Transition to deleting to block read requests
            TryTransitionState(KeyMigrationStatus.DELETING);
            WaitForConfigPropagation();

            foreach (var mKey in _keys.GetKeys())
            {
                // If key is not in deleting state skip
                if (mKey.Value != KeyMigrationStatus.DELETING)
                    continue;

                var key = mKey.Key.SpanByte;
                _ = localServerSession.BasicGarnetApi.DELETE(ref key);

                // Set key as MIGRATED to allow allow all operations
                _keys.UpdateStatus(mKey.Key, KeyMigrationStatus.MIGRATED);
            }
        }

        /// <summary>
        /// Method used to migrate keys from main and object stores.
        /// This method is used to process the MIGRATE KEYS transfer option.
        /// </summary>
        public bool MigrateKeys()
        {
            try
            {
                if (!CheckConnection())
                    return false;

                // Migrate main store keys
                _gcs.InitMigrateBuffer(clusterProvider.storeWrapper.loggingFrequncy);
                if (!MigrateKeysFromMainStore())
                    return false;

                // Migrate object store keys
                if (!clusterProvider.serverOptions.DisableObjects)
                {
                    _gcs.InitMigrateBuffer(clusterProvider.storeWrapper.loggingFrequncy);
                    if (!MigrateKeysFromObjectStore())
                        return false;
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred");
            }
            return true;
        }
    }
}