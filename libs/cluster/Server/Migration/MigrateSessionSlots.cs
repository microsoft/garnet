// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;

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
            {
                logger?.LogTrace("Initializing MainStore Iterator");
                var storeTailAddress = clusterProvider.storeWrapper.store.Log.TailAddress;
                MigrationKeyIterationFunctions.MainStoreGetKeysInSlots mainStoreGetKeysInSlots = new(this, _sslots, bufferSize: 1 << clusterProvider.serverOptions.PageSizeBits());

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

                // Log stats for store after migration completes
                _gcs.LogMigrateThrottled(0, 0, isMainStore: true, completed: true);
            }

            if (!clusterProvider.serverOptions.DisableObjects)
            {
                logger?.LogTrace("Initializing ObjectStore Iterator");
                var objectStoreTailAddress = clusterProvider.storeWrapper.objectStore.Log.TailAddress;
                MigrationKeyIterationFunctions.ObjectStoreGetKeysInSlots objectStoreGetKeysInSlots = new(this, _sslots, bufferSize: 1 << clusterProvider.serverOptions.ObjectStorePageSizeBits());

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

                // Log stats for store after migration completes
                _gcs.LogMigrateThrottled(0, 0, isMainStore: true, completed: true);
            }

            return true;
        }
    }
}