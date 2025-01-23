﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
            var bufPtr = buffer.GetValidPointer();
            var bufPtrEnd = bufPtr + bufferSize;
            var output = new SpanByteAndMemory(bufPtr, (int)(bufPtrEnd - bufPtr));

            try
            {
                // Transition keys to MIGRATING status
                TryTransitionState(KeyMigrationStatus.MIGRATING);
                WaitForConfigPropagation();

                ////////////////
                // Build Input//
                ////////////////
                var input = new RawStringInput(RespCommandAccessor.MIGRATE);

                foreach (var pair in _keys.GetKeys())
                {
                    // Process only keys in MIGRATING status
                    if (pair.Value != KeyMigrationStatus.MIGRATING)
                        continue;

                    var key = pair.Key.SpanByte;

                    // Read value for key
                    var status = localServerSession.BasicGarnetApi.Read_MainStore(key, ref input, ref output);

                    // Check if found in main store
                    if (status == GarnetStatus.NOTFOUND)
                    {
                        // Transition key status back to QUEUED to unblock any writers
                        _keys.UpdateStatus(pair.Key, KeyMigrationStatus.QUEUED);
                        continue;
                    }

                    // Get SpanByte from stack if any
                    ref var value = ref output.SpanByte;
                    if (!output.IsSpanByte)
                    {
                        // Reinterpret heap memory to SpanByte
                        value = ref SpanByte.ReinterpretWithoutLength(output.Memory.Memory.Span);
                    }

                    // Write key to network buffer. If it had expired, we would have received GarnetStatus.NOTFOUND
                    Debug.Assert(!ClusterSession.Expired(ref value), "Expired record should have received GarnetStatus.NOTFOUND");
                    if (!WriteOrSendMainStoreKeyValuePair(ref key, ref value))
                        return false;

                    // Reset SpanByte for next read if any but don't dispose heap buffer as we might re-use it
                    output.SpanByte = new SpanByte((int)(bufPtrEnd - bufPtr), (IntPtr)bufPtr);
                }

                // Flush data in client buffer
                if (!HandleMigrateTaskResponse(_gcs.SendAndResetMigrate()))
                    return false;

                DeleteKeys();
            }
            finally
            {
                // If allocated memory in heap dispose it here.
                if (output.Memory != default)
                    output.Memory.Dispose();
                buffer.Dispose();
            }
            return true;
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
                    var key = mKey.Key.SpanByte;

                    ObjectInput input = default;
                    GarnetObjectStoreOutput value = default;
                    var status = localServerSession.BasicGarnetApi.Read_ObjectStore(key, ref input, ref value);
                    if (status == GarnetStatus.NOTFOUND)
                    {
                        // Transition key status back to QUEUED to unblock any writers
                        _keys.UpdateStatus(mKey.Key, KeyMigrationStatus.QUEUED);
                        continue;
                    }

                    // Serialize the object.
                    Debug.Assert(!ClusterSession.Expired(ref value.garnetObject), "Expired record should have received GarnetStatus.NOTFOUND");
                    // If it had expired, we would have received GarnetStatus.NOTFOUND.
                    var objectData = GarnetObjectSerializer.Serialize(value.garnetObject);

                    if (!WriteOrSendObjectStoreKeyValuePair(key, objectData, value.garnetObject.Expiration))
                        return false;
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
                _ = localServerSession.BasicGarnetApi.DELETE(key);

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