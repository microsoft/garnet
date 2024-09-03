// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Garnet.common;
using Garnet.server;

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
        /// Try to transition all keys handled by this session to the provided state
        /// Valid state transitions are as follows:
        ///     PREPARE to MIGRATING
        ///     MIGRATING to MIGRATED or DELETING
        /// If state transition is not valid it will result in a no-op and the key state will remain unchanged
        /// </summary>
        /// <param name="status"></param>
        private void TryTransitionState(KeyMigrationStatus status)
        {
            foreach (var key in _keys.GetKeys())
            {
                var updateStatus = status switch
                {
                    // 1. Transition key to MIGRATING from QUEUED
                    KeyMigrationStatus.MIGRATING when key.Value == KeyMigrationStatus.QUEUED => status,
                    // 2. Transition key to MIGRATED from MIGRATING
                    KeyMigrationStatus.MIGRATED when key.Value == KeyMigrationStatus.MIGRATING => status,
                    // 3. Transition to DELETING from MIGRATING
                    KeyMigrationStatus.DELETING when key.Value == KeyMigrationStatus.MIGRATING => status,
                    // 3. Omit state transition
                    _ => key.Value,
                };
                _keys.UpdateStatus(key.Key, updateStatus);
            }
        }

        /// <summary>
        /// Check if it is safe to operate on the provided key when a slot state is set to MIGRATING
        /// </summary>
        /// <param name="key"></param>
        /// <param name="slot"></param>
        /// <param name="readOnly"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        public bool CanAccessKey(ref ArgSlice key, int slot, bool readOnly)
        {
            // Skip operation check since this session is not responsible for migrating the associated slot
            if (!_sslots.Contains(slot))
                return true;

            // If key is not queued for migration then
            if (!_keys.TryGetValue(ref key, out var state))
                return true;

            // NOTE:
            // Caller responsible for spin-wait
            // Check definition of KeyMigrationStatus for more info
            return state switch
            {
                KeyMigrationStatus.QUEUED or KeyMigrationStatus.MIGRATED => true,// Both reads and write commands can access key if it exists
                KeyMigrationStatus.MIGRATING => readOnly, // If key exists read commands can access key but write commands will be delayed
                KeyMigrationStatus.DELETING => false, // Neither read or write commands can access key
                _ => throw new GarnetException($"Invalid KeyMigrationStatus: {state}")
            };
        }

        /// <summary>
        /// Add key to the migrate dictionary for tracking progress during migration
        /// </summary>
        /// <param name="key"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool AddKey(ref ArgSlice key)
            => _keys.TryAdd(ref key, KeyMigrationStatus.QUEUED);

        /// <summary>
        /// Clear keys from dictionary
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ClearKeys()
            => _keys.ClearKeys();
    }
}