// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Begin migration task
        /// </summary>
        /// <param name="errorMessage">The ASCII encoded error message if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
        /// <returns></returns>
        public bool TryStartMigrationTask(out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            if (transferOption == TransferOption.KEYS)
            {
                try
                {
                    // This executes synchronously and serves the keys variant of resp command
                    if (!MigrateKeys())
                    {
                        errorMessage = "IOERR Migrate keys failed."u8;
                        Status = MigrateState.FAIL;
                        return false;
                    }

                    Status = MigrateState.SUCCESS;
                }
                finally
                {
                    if (!clusterProvider.migrationManager.TryRemoveMigrationTask(this))
                        logger?.LogError("Could not remove MIGRATE KEYS session");
                }
            }
            else
            {
                // This will execute as a background task for the slots or slotsrange variant
                _ = Task.Run(BeginAsyncMigrationTask);
            }

            return true;
        }

        /// <summary>
        /// Migrate slots session background task
        /// </summary>
        private async Task BeginAsyncMigrationTask()
        {
            var configResumed = true;
            try
            {
                clusterProvider.storeWrapper.store.PauseRevivification();

                // Set target node to import state
                if (!TrySetSlotRanges(GetSourceNodeId, MigrateState.IMPORT))
                {
                    logger?.LogError("Failed to set remote slots {slots} to import state", ClusterManager.GetRange([.. GetSlots]));
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }

                #region transitionLocalSlotToMigratingState
                // Set source node to migrating state and wait for local threads to see changed state.
                if (!TryPrepareLocalForMigration())
                {
                    logger?.LogError("Failed to set local slots {slots} to migrate state", string.Join(',', GetSlots));
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }

                if (!clusterProvider.BumpAndWaitForEpochTransition()) return;
                #endregion

                #region migrateData
                // Migrate actual data
                if (!await MigrateSlotsDriverInline())
                {
                    logger?.LogError("MigrateSlotsDriver failed");
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }
                #endregion

                #region transferSlotOwnnershipToTargetNode
                // Lock config merge to avoid a background epoch bump
                clusterProvider.clusterManager.SuspendConfigMerge();
                configResumed = false;
                await clusterProvider.clusterManager.TryMeetAsync(_targetAddress, _targetPort, acquireLock: false);

                // Change ownership of slots to target node.
                if (!TrySetSlotRanges(GetTargetNodeId, MigrateState.NODE))
                {
                    logger?.LogError("Failed to assign ownership to target node:({tgtNodeId}) ({endpoint})", GetTargetNodeId, GetTargetEndpoint);
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }

                // Clear local migration set.
                if (!RelinquishOwnership())
                {
                    logger?.LogError("Failed to relinquish ownership from source node:({srcNode}) to target node: ({tgtNode})", GetSourceNodeId, GetTargetNodeId);
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }

                // Gossip again to ensure that source and target agree on the slot exchange
                await clusterProvider.clusterManager.TryMeetAsync(_targetAddress, _targetPort, acquireLock: false);
                #endregion

                // Enqueue success log
                Status = MigrateState.SUCCESS;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Exception thrown at BeginAsyncMigrationTask");
            }
            finally
            {
                if (!configResumed) clusterProvider.clusterManager.ResumeConfigMerge();
                clusterProvider.storeWrapper.store.ResumeRevivification();
                _ = clusterProvider.migrationManager.TryRemoveMigrationTask(this);
            }
        }
    }
}