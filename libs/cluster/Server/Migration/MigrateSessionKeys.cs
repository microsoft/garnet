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
            var input = new RawStringInput(RespCommandAccessor.MIGRATE);
            var output = new SpanByteAndMemory();

            try
            {
                // NOTE: Any keys not found in main store are automatically set to QUEUED before this method is called
                // Transition all QUEUED to MIGRATING state
                TryTransitionState(KeyMigrationStatus.MIGRATING);
                WaitForConfigPropagation();

                // Must initialize this here because we use the network buffer as output.
                if (_gcs.NeedsInitialization)
                    _gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isMainStore: true);

                foreach (var pair in _keys.GetKeys())
                {
                    if (pair.Value != KeyMigrationStatus.MIGRATING)
                        continue;

                    // Read the value for the key. This will populate output with the entire serialized record.
                    output.SpanByte = _gcs.GetAvailableNetworkBufferSpan();
                    var status = localServerSession.BasicGarnetApi.Read_MainStore(pair.Key, ref input, ref output);
                    if (!WriteRecord(pair.Key, ref output, status, isMainStore: true))
                        return false;
                }

                if (!FlushFinalMigrationBuffer(isMainStore: true))
                    return false;

                DeleteKeys();
            }
            finally
            {
                // If allocated memory in heap dispose it here.
                output.Dispose();
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
            var input = new ObjectInput(new RespInputHeader(GarnetObjectType.Migrate));
            var output = new GarnetObjectStoreOutput();

            try
            {
                // Transition keys to MIGRATING status.
                // NOTE: Any keys not found in main store are automatically set to QUEUED.
                TryTransitionState(KeyMigrationStatus.MIGRATING);
                WaitForConfigPropagation();

                // Must initialize this here because we use the network buffer as output.
                if (_gcs.NeedsInitialization)
                    _gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isMainStore: false);

                foreach (var pair in _keys.GetKeys())
                {
                    if (pair.Value != KeyMigrationStatus.MIGRATING)
                        continue;

                    // Read the value for the key. This will populate output with the entire serialized record.
                    output.SpanByteAndMemory.SpanByte = _gcs.GetAvailableNetworkBufferSpan();
                    var status = localServerSession.BasicGarnetApi.Read_ObjectStore(pair.Key, ref input, ref output);
                    if (!WriteRecord(pair.Key, ref output.SpanByteAndMemory, status, isMainStore: false))
                        return false;
                }

                if (!FlushFinalMigrationBuffer(isMainStore: false))
                    return false;
            }
            finally
            {
                // Delete keys if COPY option is false or transition KEYS from MIGRATING to MIGRATED status
                DeleteKeys();

                output.SpanByteAndMemory.Dispose();
            }
            return true;
        }

        bool WriteRecord(PinnedSpanByte key, ref SpanByteAndMemory output, GarnetStatus status, bool isMainStore)
        {
            if (status == GarnetStatus.NOTFOUND)
            {
                // Transition key status back to QUEUED to unblock any writers
                _keys.UpdateStatus(key, KeyMigrationStatus.QUEUED);
                return true;
            }

            // If the SBAM is still SpanByte then there was enough room to write directly to the network buffer, so increment curr and
            // there is nothing more to do for this key. Otherwise, we need to Flush() and copy to the network buffer.
            if (output.IsSpanByte)
            {
                _gcs.IncrementRecordDirect(output.SpanByte.TotalSize);
                return true;
            }
            return WriteOrSendRecordSpan(ref output, isMainStore);
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

                var key = mKey.Key;
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
                _gcs.InitializeIterationBuffer(clusterProvider.storeWrapper.loggingFrequency);
                if (!MigrateKeysFromMainStore())
                    return false;

                // Migrate object store keys
                if (!clusterProvider.serverOptions.DisableObjects)
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