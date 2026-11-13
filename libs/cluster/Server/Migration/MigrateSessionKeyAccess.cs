// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
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
        private ValueTask<bool> WaitForConfigPropagationAsync()
        {
            if (transferOption == TransferOption.KEYS)
                return clusterSession.UnsafeBumpAndWaitForEpochTransitionAsync(migrationCancellationToken);
            else if (transferOption == TransferOption.SLOTS)
                return clusterProvider.BumpAndWaitForEpochTransitionAsync(migrationCancellationToken);
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
            if (!ContainsSlot(slot))
                return true;

            var keyHash = HashUtils.MurmurHash2x64A(key.ToPointer(), key.Length);
            foreach (var migrateTask in migrateOperation)
            {
                if (!migrateTask.sketch.Probe(keyHash, out var state))
                    continue;

                // A permissive Bloom-filter match must not mask another worker's protected batch.
                switch (state)
                {
                    case SketchStatus.INITIALIZING:
                    case SketchStatus.MIGRATED:
                        break;
                    case SketchStatus.TRANSMITTING:
                        if (!readOnly)
                            return false;
                        break;
                    case SketchStatus.DELETING:
                        return false;
                    default:
                        throw new GarnetException($"Invalid KeyMigrationStatus: {state}");
                }
            }

            return true;
        }
    }
}