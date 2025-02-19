// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Garnet.client;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        public bool TryReplicateDisklessSync(
            ClusterSession session,
            string nodeId,
            bool background,
            bool force,
            out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            // Ensure two replicate commands do not execute at the same time.
            if (!replicateLock.TryWriteLock())
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_CANNOT_ACQUIRE_REPLICATE_LOCK;
                return false;
            }

            try
            {
                logger?.LogTrace("CLUSTER REPLICATE {nodeid}", nodeId);
                if (!clusterProvider.clusterManager.TryAddReplica(nodeId, force: force, out errorMessage, logger: logger))
                {
                    replicateLock.WriteUnlock();
                    return false;
                }

                // Wait for threads to agree configuration change of this node
                session.UnsafeBumpAndWaitForEpochTransition();
                _ = Task.Run(() => TryBeginReplicaSync());
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"{nameof(TryReplicateDisklessSync)}");
                replicateLock.WriteUnlock();
            }

            async Task TryBeginReplicaSync()
            {
                var disklessSync = clusterProvider.serverOptions.ReplicaDisklessSync;
                var disableObjects = clusterProvider.serverOptions.DisableObjects;
                GarnetClientSession gcs = null;
                try
                {
                    if (!clusterProvider.serverOptions.EnableFastCommit)
                    {
                        storeWrapper.appendOnlyFile?.Commit();
                        storeWrapper.appendOnlyFile?.WaitForCommit();
                    }

                    // Reset background replay iterator
                    ResetReplayIterator();

                    // Reset the database in preparation for connecting to primary
                    // only if we expect to have disk checkpoint to recover from,
                    // otherwise the replica will receive a reset message from primary if needed
                    if (!disklessSync)
                        storeWrapper.Reset();

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
                        return;
                    }

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
                    gcs.Connect();

                    SyncMetadata syncMetadata = new(
                        fullSync: false,
                        originNodeRole: current.LocalNodeRole,
                        originNodeId: current.LocalNodeId,
                        currentPrimaryReplId: PrimaryReplId,
                        currentStoreVersion: storeWrapper.store.CurrentVersion,
                        currentObjectStoreVersion: disableObjects ? -1 : storeWrapper.objectStore.CurrentVersion,
                        currentAofBeginAddress: storeWrapper.appendOnlyFile.BeginAddress,
                        currentAofTailAddress: storeWrapper.appendOnlyFile.TailAddress,
                        currentReplicationOffset: ReplicationOffset,
                        checkpointEntry: checkpointEntry);

                    var resp = await gcs.ExecuteAttachSync(syncMetadata.ToByteArray()).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, $"{nameof(TryBeginReplicaSync)}");
                    clusterProvider.clusterManager.TryResetReplica();
                    SuspendRecovery();
                }
                finally
                {
                    replicateLock.WriteUnlock();
                    gcs?.Dispose();
                    recvCheckpointHandler?.Dispose();
                }
            }

            return true;
        }

        public long ReplicaRecoverDiskless(SyncMetadata primarySyncMetadata)
        {
            try
            {
                logger?.LogSyncMetadata(LogLevel.Trace, nameof(ReplicaRecoverDiskless), primarySyncMetadata);

                var aofBeginAddress = primarySyncMetadata.currentAofBeginAddress;
                var aofTailAddress = aofBeginAddress;
                var replicationOffset = aofBeginAddress;

                if (!primarySyncMetadata.fullSync)
                {
                    // For diskless replication if we are performing a partial sync need to start streaming from replicationOffset
                    // hence our tail needs to be reset to that point
                    aofTailAddress = replicationOffset = ReplicationOffset;
                }

                storeWrapper.appendOnlyFile.Initialize(aofBeginAddress, aofTailAddress);

                // Set DB version
                storeWrapper.store.SetVersion(primarySyncMetadata.currentStoreVersion);
                if (!clusterProvider.serverOptions.DisableObjects)
                    storeWrapper.objectStore.SetVersion(primarySyncMetadata.currentObjectStoreVersion);

                // Update replicationId to mark any subsequent checkpoints as part of this history
                logger?.LogInformation("Updating ReplicationId");
                TryUpdateMyPrimaryReplId(primarySyncMetadata.currentPrimaryReplId);

                ReplicationOffset = replicationOffset;
                return ReplicationOffset;
            }
            finally
            {
                // Done with recovery at this point
                SuspendRecovery();
            }
        }
    }
}