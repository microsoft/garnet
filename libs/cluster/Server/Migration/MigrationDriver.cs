// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
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
        private void BeginAsyncMigrationTask()
        {
            try
            {

                //1. Set target node to import state
                if (!TrySetSlotRanges(GetSourceNodeId, MigrateState.IMPORT))
                {
                    logger?.LogError("Failed to set remote slots {slots} to import state", ClusterManager.GetRange([.. GetSlots]));
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }

                #region transitionLocalSlotToMigratingState
                //2. Set source node to migrating state and wait for local threads to see changed state.
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
                //3. Migrate actual data
                if (!MigrateSlotsDriver())
                {
                    logger?.LogError("MigrateSlotsDriver failed");
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }
                #endregion

                #region transferSlotOwnnershipToTargetNode
                //5. Clear local migration set.
                if (!RelinquishOwnership())
                {
                    logger?.LogError("Failed to relinquish ownership from source node:({srcNode}) to target node: ({tgtNode})", GetSourceNodeId, GetTargetNodeId);
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }

                //6. Change ownership of slots to target node.
                if (!TrySetSlotRanges(GetTargetNodeId, MigrateState.NODE))
                {
                    logger?.LogError("Failed to assign ownership to target node:({tgtNodeId}) ({endpoint})", GetTargetNodeId, GetTargetEndpoint);
                    TryRecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }
                #endregion

                //7. Enqueue success log
                Status = MigrateState.SUCCESS;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Exception thrown at BeginAsyncMigrationTask");
            }
            finally
            {
                clusterProvider.migrationManager.TryRemoveMigrationTask(this);
            }
        }
    }
}