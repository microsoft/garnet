// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
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
        /// Check if it is safe to operate on the provided key when a slot state is set to MIGRATING
        /// </summary>
        /// <param name="key"></param>
        /// <param name="slot"></param>
        /// <param name="readOnly"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        public bool CanAccessKey(PinnedSpanByte key, int slot, bool readOnly)
        {
            // Skip operation check since this session is not responsible for migrating the associated slot
            if (!_sslots.Contains(slot))
                return true;

            var state = SketchStatus.INITIALIZING;
            foreach (var migrateTask in migrateOperation)
            {
                if (migrateTask.sketch.Probe(key, out state))
                    goto found;
            }

            return true;

        found:
            // NOTE:
            // Caller responsible for spin-wait
            // Check definition of KeyMigrationStatus for more info
            return state switch
            {
                SketchStatus.INITIALIZING or SketchStatus.MIGRATED => true,// Both reads and write commands can access key if it exists
                SketchStatus.TRANSMITTING => readOnly, // If key exists read commands can access key but write commands will be delayed
                SketchStatus.DELETING => false, // Neither read or write commands can access key
                _ => throw new GarnetException($"Invalid KeyMigrationStatus: {state}")
            };
        }
    }
}