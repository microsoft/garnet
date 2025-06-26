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
            var migrateTask = migrateOperation[0];

            try
            {
                // Transition keys to MIGRATING status
                migrateTask.sketch.SetStatus(SketchStatus.TRANSMITTING);
                WaitForConfigPropagation();

                // Transmit keys from main store
                if (!migrateTask.TransmitKeys(StoreType.Main))
                {
                    logger?.LogError("Failed transmitting keys from main store");
                    return false;
                }

                DeleteKeys();
            }
            finally
            {
                // If allocated memory in heap dispose it here.
                if (o.Memory != default)
                    o.Memory.Dispose();
                buffer.Dispose();

                migrateOperation[0].sketch.SetStatus(SketchStatus.INITIALIZING);
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
            var migrateTask = migrateOperation[0];
            // NOTE: Any keys not found in main store are automatically set to INITIALIZING before this method is called
            // Transition all INITIALIZING to TRANSMITTING state
            migrateTask.sketch.SetStatus(SketchStatus.TRANSMITTING);
            WaitForConfigPropagation();

            // Transmit keys from object store
            if (!migrateTask.TransmitKeys(StoreType.Object))
            {
                logger?.LogError("Failed transmitting keys from object store");
                return false;
            }

            // Delete keys if COPY option is false or transition KEYS from MIGRATING to MIGRATED status
            DeleteKeys();
            return true;
        }

        /// <summary>
        /// Delete local copy of keys if _copyOption is set to false.
        /// </summary>
        private void DeleteKeys()
        {
            var migrateTask = migrateOperation[0];
            // Transition to deleting to block read requests                
            migrateTask.sketch.SetStatus(SketchStatus.DELETING);
            WaitForConfigPropagation();

            // Delete keys
            migrateTask.DeleteKeys();

            // Transition to MIGRATED to release waiting operations
            migrateTask.sketch.SetStatus(SketchStatus.MIGRATED);
            WaitForConfigPropagation();
        }

        /// <summary>
        /// Method used to migrate keys from main and object stores.
        /// This method is used to process the MIGRATE KEYS transfer option.
        /// </summary>
        /// <returns></returns>
        public bool MigrateKeys()
        {
            try
            {
                var migrateTask = migrateOperation[0];
                if (!migrateTask.Initialize())
                    return false;

                // Migrate main store keys
                if (!MigrateKeysFromMainStore())
                    return false;

                // Migrate object store keys
                if (!clusterProvider.serverOptions.DisableObjects)
                {
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