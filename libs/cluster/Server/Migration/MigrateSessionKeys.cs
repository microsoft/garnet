// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
            var o = new SpanByteAndMemory(bufPtr, (int)(bufPtrEnd - bufPtr));

            try
            {
                // Transition keys to MIGRATING status
                migrateScan[0].sketch.SetStatus(KeyMigrationStatus.MIGRATING);
                WaitForConfigPropagation();

                ////////////////
                // Build Input//
                ////////////////
                var input = new RawStringInput(RespCommandAccessor.MIGRATE);
                var keys = migrateScan[0].sketch.Keys;
                for (var i = 0; i < migrateScan[0].sketch.Keys.Count; i++)
                {
                    if (keys[i].Item2) continue;
                    var spanByte = keys[i].Item1.SpanByte;

                    // Read value for key
                    var status = localServerSessions[0].BasicGarnetApi.Read_MainStore(ref spanByte, ref input, ref o);

                    // Skip if key NOTFOUND
                    if (status == GarnetStatus.NOTFOUND)
                        continue;

                    // Get SpanByte from stack if any
                    ref var value = ref o.SpanByte;
                    if (!o.IsSpanByte)
                    {
                        // Reinterpret heap memory to SpanByte
                        value = ref SpanByte.ReinterpretWithoutLength(o.Memory.Memory.Span);
                    }

                    // Write key to network buffer if it has not expired
                    if (!ClusterSession.Expired(ref value) && !WriteOrSendMainStoreKeyValuePair(ref spanByte, ref value))
                        return false;

                    // Reset SpanByte for next read if any but don't dispose heap buffer as we might re-use it
                    o.SpanByte = new SpanByte((int)(bufPtrEnd - bufPtr), (IntPtr)bufPtr);

                    // Mark for deletion
                    keys[i] = (keys[i].Item1, true);
                }

                // Flush data in client buffer
                if (!HandleMigrateTaskResponse(_gcs.SendAndResetIterationBuffer()))
                    return false;

                DeleteKeys();
            }
            finally
            {
                // If allocated memory in heap dispose it here.
                if (o.Memory != default)
                    o.Memory.Dispose();
                buffer.Dispose();

                migrateScan[0].sketch.SetStatus(KeyMigrationStatus.QUEUED);
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
            // NOTE: Any keys not found in main store are automatically set to QUEUED before this method is called
            // Transition all QUEUED to MIGRATING state
            migrateScan[0].sketch.SetStatus(KeyMigrationStatus.MIGRATING);
            WaitForConfigPropagation();

            var keys = migrateScan[0].sketch.Keys;
            for (var i = 0; i < migrateScan[0].sketch.Keys.Count; i++)
            {
                if (keys[i].Item2) continue;
                var keyByteArray = keys[i].Item1.ToArray();

                ObjectInput input = default;
                GarnetObjectStoreOutput value = default;
                var status = localServerSessions[0].BasicGarnetApi.Read_ObjectStore(ref keyByteArray, ref input, ref value);

                // Skip if key NOTFOUND
                if (status == GarnetStatus.NOTFOUND)
                    continue;

                if (!ClusterSession.Expired(ref value.GarnetObject))
                {
                    var objectData = GarnetObjectSerializer.Serialize(value.GarnetObject);

                    if (!WriteOrSendObjectStoreKeyValuePair(keyByteArray, objectData, value.GarnetObject.Expiration))
                        return false;
                }

                // Mark for deletion
                keys[i] = (keys[i].Item1, true);
            }

            // Flush data in client buffer
            if (!HandleMigrateTaskResponse(_gcs.SendAndResetIterationBuffer()))
                return false;

            // Delete keys if COPY option is false or transition KEYS from MIGRATING to MIGRATED status
            DeleteKeys();
            return true;
        }

        /// <summary>
        /// Delete local copy of keys if _copyOption is set to false.
        /// </summary>
        private void DeleteKeys()
        {
            if (_copyOption)
                goto migrated;

            // Transition to deleting to block read requests
            migrateScan[0].sketch.SetStatus(KeyMigrationStatus.DELETING);
            WaitForConfigPropagation();

            foreach (var pair in migrateScan[0].sketch.Keys)
            {
                if (!pair.Item2) continue;
                var spanByte = pair.Item1.SpanByte;
                _ = localServerSessions[0].BasicGarnetApi.DELETE(ref spanByte);
            }

        migrated:
            // Transition to MIGRATED to release waiting operations
            migrateScan[0].sketch.SetStatus(KeyMigrationStatus.MIGRATED);
            WaitForConfigPropagation();
        }

        /// <summary>
        /// Method used to migrate keys from main and object stores.
        /// This method is used to process the MIGRATE KEYS transfer option.
        /// </summary>
        /// <param name="storeType"></param>
        /// <returns></returns>
        public bool MigrateKeys(StoreType storeType = StoreType.All)
        {
            try
            {
                if (!CheckConnection())
                    return false;

                if (storeType is StoreType.All or StoreType.Main)
                {
                    // Migrate main store keys
                    _gcs.InitializeIterationBuffer(clusterProvider.storeWrapper.loggingFrequency);
                    if (!MigrateKeysFromMainStore())
                        return false;

                }

                // Migrate object store keys
                if (!clusterProvider.serverOptions.DisableObjects && storeType is StoreType.All or StoreType.Object)
                {
                    _gcs.InitializeIterationBuffer(clusterProvider.storeWrapper.loggingFrequency);
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