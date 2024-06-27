﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using Garnet.common;
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
        /*
         * [ Overview of the Key Transfer State Machine ]
         * 
         * - Transition all QUEUED keys to MIGRATING
         * - Wait for status transition to propagate to all running sessions.
         * - Repeat for all keys in MIGRATING status:
         *      1. Lookup key in store and transfer if found
         *      2. Otherwise transition key status back to QUEUED
         * - If COPY option is true transition all MIGRATING keys to MIGRATED
         * - Otherwise transition MIGRATING to DELETING, wait for status propagation and repeat for all keys in DELETING status
         *      1. Delete key from corresponding store
         *      2. Transition status from DELETING to MIGRATED
         * - Repeat above steps for all remaining QUEUED keys (those not found in main store).
         * 
         */

        /// <summary>
        /// Wait for config propagation based on the type of MigrateSession that is currently in progress
        /// </summary>
        /// <exception cref="GarnetException"></exception>
        private void WaitForConfigPropagation()
        {
            if (transferOption == TransferOption.KEYS)
                clusterSession.UnsafeBumpAndWaitForEpochTransition();
            else if (transferOption == TransferOption.SLOTS)
                _ = clusterProvider.BumpAndWaitForEpochTransition();
            else
                throw new GarnetException($"MigrateSession Invalid TransferOption {transferOption}");
        }

        /// <summary>
        /// Try to transition all keys handled by this session to the provided state
        /// Valid state transitions are as follows:
        ///     PREPARE to MIGRATING
        ///     MIGRATING to MIGRATED or DELETING
        /// If state transition is not valid it will result in a no-op and the key state will remain unchanged
        /// </summary>
        /// <param name="state"></param>
        private void TryTransitionState(KeyMigrationStatus state)
        {
            foreach (var key in _keys.Keys)
            {
                _keys[key] = state switch
                {
                    // 1. Transition key to MIGRATING from QUEUED
                    KeyMigrationStatus.MIGRATING when _keys[key] == KeyMigrationStatus.QUEUED => state,
                    // 2. Transition key to MIGRATED from MIGRATING
                    KeyMigrationStatus.MIGRATED when _keys[key] == KeyMigrationStatus.MIGRATING => state,
                    // 3. Transition to DELETING from MIGRATING
                    KeyMigrationStatus.DELETING when _keys[key] == KeyMigrationStatus.MIGRATING => state,
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
                foreach (var mKey in _keys)
                {
                    // Process only keys in MIGRATING status
                    if (mKey.Value != KeyMigrationStatus.MIGRATING)
                        continue;

                    var key = mKey.Key.SpanByte;

                    // Read value for key
                    var o = new SpanByteAndMemory(bufPtr, (int)(bufPtrEnd - bufPtr));
                    var status = localServerSession.BasicGarnetApi.Read_MainStore(ref key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref o);

                    // Check if found in main store
                    if (status == GarnetStatus.NOTFOUND)
                    {
                        // Transition key status back to QUEUED to unblock any writers
                        _keys[mKey.Key] = KeyMigrationStatus.QUEUED;
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

                foreach (var mKey in _keys)
                {
                    // Process only keys in MIGRATING status
                    if (mKey.Value != KeyMigrationStatus.MIGRATING)
                        continue;
                    var key = mKey.Key.ToArray();

                    SpanByte input = default;
                    GarnetObjectStoreOutput value = default;
                    var status = localServerSession.BasicGarnetApi.Read_ObjectStore(ref key, ref input, ref value);
                    if (status == GarnetStatus.NOTFOUND)
                    {
                        // Transition key status back to QUEUED to unblock any writers
                        _keys[mKey.Key] = KeyMigrationStatus.QUEUED;
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

            foreach (var mKey in _keys)
            {
                // If key is not in deleting state skip
                if (mKey.Value != KeyMigrationStatus.DELETING)
                    continue;

                var key = mKey.Key.SpanByte;
                _ = localServerSession.BasicGarnetApi.DELETE(ref key);

                // Set key as MIGRATED to allow allow all operations
                _keys[mKey.Key] = KeyMigrationStatus.MIGRATED;
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
                _gcs.InitMigrateBuffer();
                if (!MigrateKeysFromMainStore())
                    return false;

                // Migrate object store keys
                if (!clusterProvider.serverOptions.DisableObjects)
                {
                    _gcs.InitMigrateBuffer();
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