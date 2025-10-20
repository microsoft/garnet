// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// This code implements operations associated with the MIGRATE KEYS transfer option.
    /// </summary>
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Method used to migrate individual keys from store to target node.
        /// Used with MIGRATE KEYS option
        /// </summary>
        /// <returns>True on success, false otherwise</returns>
        private bool MigrateKeysFromStore()
        {
            var migrateTask = migrateOperation[0];

            try
            {
                // Transition keys to MIGRATING status
                migrateTask.sketch.SetStatus(SketchStatus.TRANSMITTING);
                WaitForConfigPropagation();

                // Transmit keys from store
                if (!migrateTask.TransmitKeys())
                {
                    logger?.LogError("Failed transmitting keys from store");
                    return false;
                }

                DeleteKeys();
            }
            finally
            {
                migrateOperation[0].sketch.SetStatus(SketchStatus.INITIALIZING);
            }
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
                if (!MigrateKeysFromStore())
                    return false;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred");
            }
            return true;
        }
    }
}