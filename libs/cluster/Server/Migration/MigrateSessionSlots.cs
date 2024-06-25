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
                var storeTailAddress = clusterProvider.storeWrapper.store.Log.TailAddress;
                MigrationKeyIterationFunctions.MainStoreGetKeysInSlots mainStoreGetKeysInSlots = new(this, _sslots, bufferSize: 1 << clusterProvider.serverOptions.PageSizeBits());

                while (true)
                {
                    // Iterate main store
                    if (!localServerSession.BasicGarnetApi.IterateMainStore(ref mainStoreGetKeysInSlots, storeTailAddress))
                        return false;

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
            }

            if (!clusterProvider.serverOptions.DisableObjects)
            {
                var objectStoreTailAddress = clusterProvider.storeWrapper.objectStore.Log.TailAddress;
                MigrationKeyIterationFunctions.ObjectStoreGetKeysInSlots objectStoreGetKeysInSlots = new(this, _sslots, bufferSize: 1 << clusterProvider.serverOptions.ObjectStorePageSizeBits());

                while (true)
                {
                    // Iterate object store
                    if (!localServerSession.BasicGarnetApi.IterateObjectStore(ref objectStoreGetKeysInSlots, objectStoreTailAddress))
                        return false;

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
            }

            return true;
        }
    }
}