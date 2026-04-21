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
        /// Change remote slot state
        /// </summary>
        /// <param name="nodeid"></param>
        /// <param name="state"></param>
        /// <returns></returns>
        public async Task<bool> TrySetSlotRangesAsync(string nodeid, MigrateState state)
        {
            var client = migrateOperation[0].Client;
            try
            {
                if (!await CheckConnectionAsync(client).ConfigureAwait(false))
                {
                    Status = MigrateState.FAIL;
                    return false;
                }

                var stateBytes = state switch
                {
                    MigrateState.IMPORT => IMPORTING,
                    MigrateState.STABLE => STABLE,
                    MigrateState.NODE => NODE,
                    _ => throw new Exception("Invalid SETSLOT Operation"),
                };

                logger?.LogTrace("Sending CLUSTER SETSLOTRANGE {state} {nodeid} {slots}", state, nodeid ?? "null", ClusterManager.GetRange([.. _sslots]));

                var result = await client.SetSlotRange(stateBytes, nodeid, _slotRanges)
                    .WaitAsync(_timeout, _cts.Token).ConfigureAwait(false);

                // Check if setslotsrange executed correctly
                if (!result.Equals("OK", StringComparison.Ordinal))
                {
                    logger?.LogError("SetSlotRange error: {error}", result);
                    Status = MigrateState.FAIL;
                    return false;
                }

                logger?.LogTrace("[Completed] SETSLOT {slots} {state} {nodeid}", ClusterManager.GetRange([.. _sslots]), state, nodeid ?? "");
                return true;
            }
            catch (OperationCanceledException)
            {
                logger?.LogError("SetSlotRange operation timed out or was cancelled after {timeout}ms for slots {slots}", _timeout.TotalMilliseconds, ClusterManager.GetRange([.. _sslots]));
                Status = MigrateState.FAIL;
                return false;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error occurred during SetSlotRange for slots {slots}", ClusterManager.GetRange([.. _sslots]));
                Status = MigrateState.FAIL;
                return false;
            }
        }

        /// <summary>
        /// Try recover to cluster state before migration task.
        /// Used only for MIGRATE SLOTS option.
        /// </summary>
        public async Task<bool> TryRecoverFromFailureAsync()
        {
            // Set slot at target to stable state when migrate slots fails
            // This issues a SETSLOTRANGE STABLE for the slots of the failed migration task
            if (!await TrySetSlotRangesAsync(null, MigrateState.STABLE).ConfigureAwait(false))
            {
                logger?.LogError("MigrateSession.RecoverFromFailure failed to make slots STABLE");
                return false;
            }

            // Set slots at source node to their original state when migrate fails
            // This will execute the equivalent of SETSLOTRANGE STABLE for the slots of the failed migration task
            ResetLocalSlot();

            // TODO: Need to relinquish any migrating Vector Set contexts from target node

            // Log explicit migration failure.
            Status = MigrateState.FAIL;
            return true;
        }

        /// <summary>
        /// Begin migration task
        /// </summary>
        /// <returns></returns>
        public async ValueTask<(bool Success, ReadOnlyMemory<byte> ErrorMessage)> TryStartMigrationTaskAsync()
        {
            ReadOnlyMemory<byte> errorMessage = default;
            if (transferOption == TransferOption.KEYS)
            {
                try
                {
                    // This executes synchronously and serves the keys variant of resp command
                    if (!await MigrateKeysAsync().ConfigureAwait(false))
                    {
                        errorMessage = "IOERR Migrate keys failed."u8.ToArray();
                        Status = MigrateState.FAIL;
                        return (false, errorMessage);
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
                _ = BeginAsyncMigrationTaskAsync();
            }

            return (true, errorMessage);
        }

        /// <summary>
        /// Migrate slots session background task
        /// </summary>
        private async Task BeginAsyncMigrationTaskAsync()
        {
            // Force async
            await Task.Yield();

            var configResumed = true;
            try
            {
                clusterProvider.storeWrapper.store.PauseRevivification(_timeout, _cts.Token);

                // Set target node to import state
                if (!await TrySetSlotRangesAsync(GetSourceNodeId, MigrateState.IMPORT).ConfigureAwait(false))
                {
                    logger?.LogError("Failed to set remote slots {slots} to import state", ClusterManager.GetRange([.. GetSlots]));
                    await TryRecoverFromFailureAsync().ConfigureAwait(false);
                    Status = MigrateState.FAIL;
                    return;
                }

                #region transitionLocalSlotToMigratingState
                // Set source node to migrating state and wait for local threads to see changed state.
                if (!TryPrepareLocalForMigration())
                {
                    logger?.LogError("Failed to set local slots {slots} to migrate state", string.Join(',', GetSlots));
                    await TryRecoverFromFailureAsync().ConfigureAwait(false);
                    Status = MigrateState.FAIL;
                    return;
                }

                if (!await clusterProvider.BumpAndWaitForEpochTransitionAsync().ConfigureAwait(false)) return;
                #endregion

                // Acquire namespaces at this point, after slots have been switch to migration
                _namespaces = clusterProvider.storeWrapper.DefaultDatabase.VectorManager.GetNamespacesForHashSlots(_sslots);

                // If we have any namespaces, that implies Vector Sets, and if we have any of THOSE
                // we need to reserve destination sets on the other side
                if ((_namespaces?.Count ?? 0) > 0 && !await ReserveDestinationVectorSetsAsync().ConfigureAwait(false))
                {
                    logger?.LogError("Failed to reserve destination vector sets, migration failed");
                    await TryRecoverFromFailureAsync().ConfigureAwait(false);
                    Status = MigrateState.FAIL;
                    return;
                }

                #region migrateData
                // Migrate actual data
                if (!await MigrateSlotsDriverInlineAsync().ConfigureAwait(false))
                {
                    logger?.LogError("MigrateSlotsDriver failed");
                    await TryRecoverFromFailureAsync().ConfigureAwait(false);
                    Status = MigrateState.FAIL;
                    return;
                }
                #endregion

                #region transferSlotOwnnershipToTargetNode
                // Lock config merge to avoid a background epoch bump
                clusterProvider.clusterManager.SuspendConfigMerge();
                configResumed = false;
                await clusterProvider.clusterManager.TryMeetAsync(_targetAddress, _targetPort, acquireLock: false).ConfigureAwait(false);

                // Change ownership of slots to target node.
                if (!await TrySetSlotRangesAsync(GetTargetNodeId, MigrateState.NODE).ConfigureAwait(false))
                {
                    logger?.LogError("Failed to assign ownership to target node:({tgtNodeId}) ({endpoint})", GetTargetNodeId, GetTargetEndpoint);
                    await TryRecoverFromFailureAsync().ConfigureAwait(false);
                    Status = MigrateState.FAIL;
                    return;
                }

                // Clear local migration set.
                if (!RelinquishOwnership())
                {
                    logger?.LogError("Failed to relinquish ownership from source node:({srcNode}) to target node: ({tgtNode})", GetSourceNodeId, GetTargetNodeId);
                    await TryRecoverFromFailureAsync().ConfigureAwait(false);
                    Status = MigrateState.FAIL;
                    return;
                }

                // Gossip again to ensure that source and target agree on the slot exchange
                await clusterProvider.clusterManager.TryMeetAsync(_targetAddress, _targetPort, acquireLock: false).ConfigureAwait(false);
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