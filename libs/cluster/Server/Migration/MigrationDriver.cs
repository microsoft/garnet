// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Begin migration task
        /// </summary>
        /// <param name="resp"></param>
        /// <returns></returns>
        public bool StartMigrationTask(out ReadOnlySpan<byte> resp)
        {
            resp = CmdStrings.RESP_OK;
            if (_keysWithSize != null)
            {
                try
                {
                    // This executes synchronously and serves the keys variant of resp command
                    if (!MigrateKeys())
                    {
                        resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes("-IOERR Migrate keys failed.\r\n"));
                        Status = MigrateState.FAIL;
                        return false;
                    }

                    // Delete keys locally if option enabled
                    if (_copyOption)
                        DeleteKeys(_keysWithSize);
                    Status = MigrateState.SUCCESS;
                }
                finally
                {
                    clusterProvider.migrationManager.TryRemoveMigrationTask(this);
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
                    logger?.LogError("Failed to set remote slots {slots} to import state", string.Join(',', GetSlots));
                    RecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }

                #region transitionLocalSlotToMigratingState
                //2. Set source node to migrating state and wait for local threads to see changed state.
                if (!TryPrepareLocalForMigration())
                {
                    logger?.LogError("Failed to set local slots {slots} to migrate state", string.Join(',', GetSlots));
                    RecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }

                if (!clusterProvider.WaitForConfigTransition()) return;
                #endregion

                #region migrateData
                //3. Migrate actual data
                if (!MigrateSlotsDataDriver())
                {
                    logger?.LogError($"MigrateSlotsDriver failed");
                    RecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }
                #endregion

                #region transferSlotOwnnershipToTargetNode
                //5. Clear local migration set.
                if (!RelinquishOwnership())
                {
                    logger?.LogError($"Failed to relinquish ownerhsip to target node");
                    RecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }

                //6. Change ownership of slots to target node.
                if (!TrySetSlotRanges(GetTargetNodeId, MigrateState.NODE))
                {
                    logger?.LogError($"Failed to assign ownerhsip to target node");
                    RecoverFromFailure();
                    Status = MigrateState.FAIL;
                    return;
                }
                #endregion

                //7. Delete keys in slot and remove migrate task from set of active migration tasks.            
                DeleteKeysInSlot();

                //8. Enqueue success log
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