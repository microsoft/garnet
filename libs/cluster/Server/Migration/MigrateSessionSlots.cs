// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Migrate slots main driver
        /// </summary>
        /// <returns></returns>
        public bool MigrateSlotsDriver()
        {
            logger?.LogTrace("Initializing MainStore Iterator");
            var storeTailAddress = clusterProvider.storeWrapper.store.Log.TailAddress;
            var bufferSize = 1 << clusterProvider.serverOptions.PageSizeBits();
            MigrationKeyIterationFunctions.MainStoreGetKeysInSlots mainStoreGetKeysInSlots = new(this, _sslots, bufferSize: bufferSize);

            try
            {
                logger?.LogTrace("Begin MainStore Iteration");
                while (true)
                {
                    // Iterate main store
                    _ = localServerSession.BasicGarnetApi.IterateMainStore(ref mainStoreGetKeysInSlots, storeTailAddress);

                    // If did not acquire any keys stop scanning
                    if (_keys.IsNullOrEmpty())
                        break;

                    // Safely migrate keys to target node
                    if (!MigrateKeys())
                    {
                        logger?.LogError("IOERR Migrate keys failed.");
                        Status = MigrateState.FAIL;
                        return false;
                    }

                    mainStoreGetKeysInSlots.AdvanceIterator();
                    ClearKeys();
                }

                // Signal target transmission completed and log stats for main store after migration completes
                if (!HandleMigrateTaskResponse(_gcs.CompleteMigrate(_sourceNodeId, _replaceOption, isMainStore: true)))
                    return false;
            }
            finally
            {
                mainStoreGetKeysInSlots.Dispose();
            }

            if (!clusterProvider.serverOptions.DisableObjects)
            {
                logger?.LogTrace("Initializing ObjectStore Iterator");
                var objectStoreTailAddress = clusterProvider.storeWrapper.objectStore.Log.TailAddress;
                var objectBufferSize = 1 << clusterProvider.serverOptions.ObjectStorePageSizeBits();
                MigrationKeyIterationFunctions.ObjectStoreGetKeysInSlots objectStoreGetKeysInSlots = new(this, _sslots, bufferSize: objectBufferSize);

                try
                {
                    logger?.LogTrace("Begin ObjectStore Iteration");
                    while (true)
                    {
                        // Iterate object store
                        _ = localServerSession.BasicGarnetApi.IterateObjectStore(ref objectStoreGetKeysInSlots, objectStoreTailAddress);

                        // If did not acquire any keys stop scanning
                        if (_keys.IsNullOrEmpty())
                            break;

                        // Safely migrate keys to target node
                        if (!MigrateKeys())
                        {
                            logger?.LogError("IOERR Migrate keys failed.");
                            Status = MigrateState.FAIL;
                            return false;
                        }

                        objectStoreGetKeysInSlots.AdvanceIterator();
                        ClearKeys();
                    }

                    // Signal target transmission completed and log stats for object store after migration completes
                    if (!HandleMigrateTaskResponse(_gcs.CompleteMigrate(_sourceNodeId, _replaceOption, isMainStore: false)))
                        return false;
                }
                finally
                {
                    objectStoreGetKeysInSlots.Dispose();
                }
            }

            return true;
        }
    }
}