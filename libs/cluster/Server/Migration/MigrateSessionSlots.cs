// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Main data driver of migration task
        /// </summary>
        /// <returns></returns>
        private bool MigrateSlotsDataDriver()
        {
            try
            {
                var storeTailAddress = clusterProvider.storeWrapper.store.Log.TailAddress;
                MigrationKeyIterationFunctions.MainStoreMigrateSlots mainStoreIterFuncs = new(this, _sslots);

                // Initialize migrate buffers
                _gcs.InitMigrateBuffer();
                // Iterate main store
                if (!localServerSession.BasicGarnetApi.IterateMainStore(ref mainStoreIterFuncs, storeTailAddress))
                    return false;
                // Flush data in client buffer
                if (!HandleMigrateTaskResponse(_gcs.SendAndResetMigrate()))
                    return false;

                // Flush and initialize gcs buffers and offsets
                if (!clusterProvider.serverOptions.DisableObjects)
                {
                    var objectStoreTailAddress = clusterProvider.storeWrapper.objectStore.Log.TailAddress;
                    MigrationKeyIterationFunctions.ObjectStoreMigrateSlots objectStoreIterFuncs = new(this, _sslots);

                    // Initialize migrate buffers
                    _gcs.InitMigrateBuffer();
                    // Iterate object store
                    if (!localServerSession.BasicGarnetApi.IterateObjectStore(ref objectStoreIterFuncs, objectStoreTailAddress))
                        return false;
                    // Flush data in client buffer
                    if (!HandleMigrateTaskResponse(_gcs.SendAndResetMigrate()))
                        return false;
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Exception at MigrateSlotsDataDriver");
            }
            return true;
        }

        /// <summary>
        /// Delete local copy of keys in slot if _copyOption is set to false.
        /// </summary>
        public void DeleteKeysInSlot()
        {
            if (!_copyOption)
                return;

            clusterProvider.clusterManager.DeleteKeysInSlotsFromMainStore(localServerSession.BasicGarnetApi, _sslots);

            if (!clusterProvider.serverOptions.DisableObjects)
                clusterProvider.clusterManager.DeleteKeysInSlotsFromObjectStore(localServerSession.BasicGarnetApi, _sslots);
        }
    }
}