// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.cluster.Server.Replication;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        /// <summary>
        /// Try to replicate using diskless sync
        /// </summary>
        /// <param name="session">ClusterSession for this connection.</param>
        /// <param name="options">Options for the sync.</param>
        /// <param name="errorMessage">The ASCII encoded error message if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
        /// <returns>A boolean indicating whether replication initiation was successful.</returns>
        public bool TryReplicateDisklessSync(
            ClusterSession session,
            ReplicateSyncOptions options,
            out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;

            try
            {
                logger?.LogTrace("CLUSTER REPLICATE {nodeid}", options.NodeId);
                if (options.TryAddReplica && !clusterProvider.clusterManager.TryAddReplica(options.NodeId, options.Force, options.UpgradeLock, out errorMessage, logger: logger))
                    return false;

                // Create or update timestamp manager for sharded log if needed
                storeWrapper.appendOnlyFile.CreateOrUpdateKeySequenceManager();

                // Wait for threads to agree configuration change of this node
                session?.UnsafeBumpAndWaitForEpochTransition();
                if (options.Background)
                    _ = Task.Run(() => TryBeginReplicaSync(options.UpgradeLock));
                else
                {
                    var result = TryBeginReplicaSync(options.UpgradeLock).Result;
                    if (result != null)
                    {
                        errorMessage = Encoding.ASCII.GetBytes(result);
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"{nameof(TryReplicateDisklessSync)}");
                return false;
            }

            return true;

            async Task<string> TryBeginReplicaSync(bool downgradeLock)
            {
                var disklessSync = clusterProvider.serverOptions.ReplicaDisklessSync;
                GarnetClientSession gcs = null;
                resetHandler ??= new CancellationTokenSource();
                try
                {
                    if (!clusterProvider.serverOptions.EnableFastCommit)
                    {
                        storeWrapper.appendOnlyFile?.Log.Commit();
                        storeWrapper.appendOnlyFile?.Log.WaitForCommit();
                    }

                    // Reset background replay tasks if this node was a replica
                    clusterProvider.replicationManager.ResetReplicaReplayDriverStore();

                    // Remove aofSync tasks if this node was a primary
                    aofSyncDriverStore.Reset();

                    // Reset the database in preparation for connecting to primary
                    // only if we expect to have disk checkpoint to recover from,
                    // otherwise the replica will receive a reset message from primary if needed
                    if (!disklessSync)
                        storeWrapper.Reset();

                    // Suspend background tasks that may interfere with AOF
                    await storeWrapper.SuspendPrimaryOnlyTasks();

                    // Stop advance time task when reconfiguring node to be replica
                    if(storeWrapper.serverOptions.AofPhysicalSublogCount > 1)
                        clusterProvider.storeWrapper.TaskManager.Cancel(TaskType.AdvanceTimeReplicaTask).Wait();

                    // Send request to primary
                    //      Primary will initiate background task and start sending checkpoint data
                    //
                    // Replica waits for retrieval to complete before moving forward to recovery
                    //      Retrieval completion coordinated by remoteCheckpointRetrievalCompleted
                    var current = clusterProvider.clusterManager.CurrentConfig;
                    var (address, port) = current.GetLocalNodePrimaryAddress();
                    CheckpointEntry checkpointEntry = null;

                    if (!disklessSync)
                        checkpointEntry = GetLatestCheckpointEntryFromDisk();

                    if (address == null || port == -1)
                    {
                        var errorMsg = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_NOT_ASSIGNED_PRIMARY_ERROR);
                        logger?.LogError("{msg}", errorMsg);
                        return errorMsg;
                    }

                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ctsRepManager.Token, resetHandler.Token);
                    gcs = new(
                        new IPEndPoint(IPAddress.Parse(address), port),
                        networkBufferSettings: clusterProvider.replicationManager.GetIRSNetworkBufferSettings,
                        networkPool: clusterProvider.replicationManager.GetNetworkPool,
                        tlsOptions: clusterProvider.serverOptions.TlsOptions?.TlsClientOptions,
                        authUsername: clusterProvider.ClusterUsername,
                        authPassword: clusterProvider.ClusterPassword);

                    // Used only for disk-based replication
                    if (!disklessSync)
                        recvCheckpointHandler = new ReceiveCheckpointHandler(clusterProvider, logger);
                    gcs.Connect((int)clusterProvider.serverOptions.ReplicaSyncTimeout.TotalMilliseconds, linkedCts.Token);

                    SyncMetadata syncMetadata = new(
                        fullSync: false,
                        originNodeRole: current.LocalNodeRole,
                        originNodeId: current.LocalNodeId,
                        currentPrimaryReplId: PrimaryReplId,
                        currentStoreVersion: storeWrapper.store.CurrentVersion,
                        currentAofBeginAddress: storeWrapper.appendOnlyFile.Log.BeginAddress,
                        currentAofTailAddress: storeWrapper.appendOnlyFile.Log.TailAddress,
                        currentReplicationOffset: ReplicationOffset,
                        checkpointEntry: checkpointEntry);

                    var resp = await gcs.ExecuteClusterAttachSync(syncMetadata.ToByteArray()).
                        WaitAsync(storeWrapper.serverOptions.ReplicaAttachTimeout, linkedCts.Token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, $"{nameof(TryBeginReplicaSync)}");

                    if (options.AllowReplicaResetOnFailure)
                        clusterProvider.clusterManager.TryResetReplica();

                    return ex.Message;
                }
                finally
                {
                    if (downgradeLock)
                    {
                        EndRecovery(RecoveryStatus.ReadRole, downgradeLock: true);
                    }
                    else
                    {
                        EndRecovery(RecoveryStatus.NoRecovery, downgradeLock: false);
                    }
                    gcs?.Dispose();
                    recvCheckpointHandler?.Dispose();
                    if (!resetHandler.TryReset())
                    {
                        resetHandler.Dispose();
                        resetHandler = new CancellationTokenSource();
                    }
                }
                return null;
            }
        }

        public AofAddress TryReplicaDisklessRecovery(SyncMetadata primarySyncMetadata, out ReadOnlySpan<byte> errorMessage)
        {
            try
            {
                errorMessage = [];
                logger?.LogSyncMetadata(LogLevel.Trace, nameof(TryReplicaDisklessRecovery), primarySyncMetadata);

                var aofBeginAddress = primarySyncMetadata.currentAofBeginAddress;
                var aofTailAddress = aofBeginAddress;
                var _replicationOffset = aofBeginAddress;

                if (!primarySyncMetadata.fullSync)
                {
                    // For diskless replication if we are performing a partial sync need to start streaming from replicationOffset
                    // hence our tail needs to be reset to that point
                    aofTailAddress = _replicationOffset = this.replicationOffset;
                }

                storeWrapper.appendOnlyFile.Log.Initialize(aofBeginAddress, aofTailAddress);

                // Set DB version
                storeWrapper.store.SetVersion(primarySyncMetadata.currentStoreVersion);

                // Update replicationId to mark any subsequent checkpoints as part of this history
                logger?.LogInformation("Updating ReplicationId");
                TryUpdateMyPrimaryReplId(primarySyncMetadata.currentPrimaryReplId);

                this.replicationOffset = _replicationOffset;

                // Mark this txn run as a read-write session if we are replaying as a replica
                // This is necessary to ensure that the stored procedure can perform write operations if needed
                clusterProvider.replicationManager.aofProcessor.SetReadWriteSession();

                // Start advance time signal processing background task
                clusterProvider.replicationManager.StartAdvanceTimeBackgroundTask();

                return this.replicationOffset;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"{nameof(TryReplicaDisklessRecovery)}");
                errorMessage = Encoding.ASCII.GetBytes(ex.Message);
                return AofAddress.Create(clusterProvider.serverOptions.AofPhysicalSublogCount, -1);
            }
            finally
            {
                // Done with recovery at this point
                EndRecovery(RecoveryStatus.CheckpointRecoveredAtReplica, downgradeLock: false);
            }
        }
    }
}