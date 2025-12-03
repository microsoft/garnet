// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.cluster.Server.Replication;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        public ReceiveCheckpointHandler recvCheckpointHandler = null;
        CheckpointEntry cEntry;

        /// <summary>
        /// Try initiate replicate attach
        /// </summary>
        /// <param name="session">ClusterSession for this connection.</param>
        /// <param name="options">Options for the sync.</param>
        /// <param name="errorMessage">The ASCII encoded error message if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
        /// <returns>A boolean indicating whether replication initiation was successful.</returns>
        public bool TryReplicateDiskbasedSync(
            ClusterSession session,
            ReplicateSyncOptions options,
            out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = [];
            try
            {
                logger?.LogTrace("CLUSTER REPLICATE {nodeid}", options.NodeId);
                // Update the configuration to make this node a replica of provided nodeId
                if (options.TryAddReplica && !clusterProvider.clusterManager.TryAddReplica(options.NodeId, options.Force, options.UpgradeLock, out errorMessage, logger: logger))
                    return false;

                // Create or update timestamp manager for sharded log if needed
                storeWrapper.appendOnlyFile.CreateOrUpdateTimestampManager();

                // Wait for threads to agree
                session?.UnsafeBumpAndWaitForEpochTransition();

                // Initiate remote checkpoint retrieval
                if (options.Background)
                {
                    logger?.LogInformation("Initiating background checkpoint retrieval");
                    _ = Task.Run(() => ReplicaSyncAttachTask(options.UpgradeLock));
                }
                else
                {
                    logger?.LogInformation("Initiating foreground checkpoint retrieval");
                    var resp = ReplicaSyncAttachTask(options.UpgradeLock).GetAwaiter().GetResult();
                    if (resp != null)
                    {
                        errorMessage = Encoding.ASCII.GetBytes(resp);
                        return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"{nameof(TryReplicateDiskbasedSync)}");
                return false;
            }

            async Task<string> ReplicaSyncAttachTask(bool downgradeLock)
            {
                Debug.Assert(IsRecovering);
                GarnetClientSession gcs = null;
                resetHandler ??= new CancellationTokenSource();
                try
                {
                    // Immediately try to connect to a primary, so we FAIL
                    // before resetting our local data
                    var current = clusterProvider.clusterManager.CurrentConfig;
                    var (address, port) = current.GetLocalNodePrimaryAddress();

                    if (address == null || port == -1)
                    {
                        var errorMsg = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_NOT_ASSIGNED_PRIMARY_ERROR);
                        logger?.LogError("{msg}", errorMsg);
                        return errorMsg;
                    }

                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ctsRepManager.Token, resetHandler.Token);
                    gcs = new(
                        new IPEndPoint(IPAddress.Parse(address), port),
                        clusterProvider.replicationManager.GetIRSNetworkBufferSettings,
                        clusterProvider.replicationManager.GetNetworkPool,
                        tlsOptions: clusterProvider.serverOptions.TlsOptions?.TlsClientOptions,
                        authUsername: clusterProvider.ClusterUsername,
                        authPassword: clusterProvider.ClusterPassword);
                    gcs.Connect((int)clusterProvider.serverOptions.ReplicaSyncTimeout.TotalMilliseconds, linkedCts.Token);

                    // Wait for Commit of AOF (data received from old primary) if FastCommit is not enabled
                    // If FastCommit is enabled, we commit during AOF stream processing
                    if (!clusterProvider.serverOptions.EnableFastCommit)
                    {
                        storeWrapper.appendOnlyFile?.Log.Commit();
                        storeWrapper.appendOnlyFile?.Log.WaitForCommit();
                    }

                    // Reset background replay iterator if this node was a replica
                    clusterProvider.replicationManager.ResetReplicaReplayGroup();

                    // Remove aofSync tasks if this node was a primary
                    aofSyncDriverStore.RemoveAll();

                    // Reset replication offset
                    replicationOffset.SetValue(0);

                    // Reset the database in preparation for connecting to primary
                    storeWrapper.Reset();

                    // Send request to primary
                    //      Primary will initiate background task and start sending checkpoint data
                    //
                    // Replica waits for retrieval to complete before moving forward to recovery
                    //      Retrieval completion coordinated by remoteCheckpointRetrievalCompleted
                    recvCheckpointHandler = new ReceiveCheckpointHandler(clusterProvider, logger);

                    var nodeId = current.LocalNodeId;
                    cEntry = GetLatestCheckpointEntryFromDisk();
                    logger?.LogCheckpointEntry(LogLevel.Information, nameof(ReplicaSyncAttachTask), cEntry);

                    storeWrapper.RecoverAOF();
                    logger?.LogInformation("InitiateReplicaSync: AOF BeginAddress:{beginAddress} AOF TailAddress:{tailAddress}", storeWrapper.appendOnlyFile.Log.BeginAddress, storeWrapper.appendOnlyFile.Log.TailAddress);

                    // 1. Primary will signal checkpoint send complete
                    // 2. Replica will receive signal and recover checkpoint, initialize AOF
                    // 3. Replica signals recovery complete (here cluster replicate will return to caller)
                    // 4. Replica responds with aofStartAddress sync
                    // 5. Primary will initiate aof sync task
                    // 6. Primary releases checkpoint
                    var resp = await gcs.ExecuteClusterInitiateReplicaSync(
                        nodeId,
                        PrimaryReplId,
                        cEntry.ToByteArray(),
                        storeWrapper.appendOnlyFile.Log.BeginAddress.ToByteArray(),
                        storeWrapper.appendOnlyFile.Log.TailAddress.ToByteArray()).WaitAsync(storeWrapper.serverOptions.ReplicaAttachTimeout, linkedCts.Token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "An error occurred at ReplicationManager.RetrieveStoreCheckpoint");

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
                    recvCheckpointHandler?.Dispose();
                    gcs?.Dispose();
                    if (!resetHandler.TryReset())
                    {
                        resetHandler.Dispose();
                        resetHandler = new CancellationTokenSource();
                    }
                }
                return null;
            }
        }

        /// <summary>
        /// Process checkpoint metadata transmitted from primary during replica synchronization.
        /// </summary>
        /// <param name="fileToken">Checkpoint metadata token.</param>
        /// <param name="fileType">Checkpoint metadata filetype.</param>
        /// <param name="checkpointMetadata">Raw bytes of checkpoint metadata.</param>
        /// <exception cref="Exception">Throws invalid type checkpoint metadata.</exception>
        public void ProcessCheckpointMetadata(Guid fileToken, CheckpointFileType fileType, byte[] checkpointMetadata)
        {
            UpdateLastPrimarySyncTime();
            var ckptManager = fileType switch
            {
                CheckpointFileType.STORE_SNAPSHOT or
                CheckpointFileType.STORE_INDEX => clusterProvider.GetReplicationLogCheckpointManager(),
                _ => throw new Exception($"Invalid checkpoint filetype {fileType}"),
            };

            switch (fileType)
            {
                case CheckpointFileType.STORE_SNAPSHOT:
                    ckptManager.CommitLogCheckpointSendFromPrimary(fileToken, checkpointMetadata);
                    break;
                case CheckpointFileType.STORE_INDEX:
                    ckptManager.CommitIndexCheckpoint(fileToken, checkpointMetadata);
                    break;
                default:
                    throw new Exception($"Invalid checkpoint filetype {fileType}");
            }
        }

        /// <summary>
        /// Check if device needs to be initialized with a specifi segment size depending on the checkpoint file type
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static bool ShouldInitialize(CheckpointFileType type)
        {
            //TODO: verify that the below checkpoint file types require initialization with segment size given as option
            return type switch
            {
                CheckpointFileType.STORE_HLOG or
                CheckpointFileType.STORE_SNAPSHOT
                => true,
                _ => false,
            };
        }

        /// <summary>
        /// Get an IDevice that is also initialized if needed
        /// </summary>
        /// <param name="token"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public IDevice GetInitializedSegmentFileDevice(Guid token, CheckpointFileType type)
        {
            var device = type switch
            {
                CheckpointFileType.STORE_HLOG => GetStoreHLogDevice(isObj: false),
                CheckpointFileType.STORE_HLOG_OBJ => GetStoreHLogDevice(isObj: true),
                _ => clusterProvider.GetReplicationLogCheckpointManager().GetDevice(type, token),
            };

            if (ShouldInitialize(type))
                device.Initialize(segmentSize: 1L << clusterProvider.serverOptions.SegmentSizeBits());
            return device;

            IDevice GetStoreHLogDevice(bool isObj)
            {
                var opts = clusterProvider.serverOptions;
                if (opts.EnableStorageTier)
                {
                    var LogDir = opts.LogDir;
                    if (LogDir is null or "") LogDir = Directory.GetCurrentDirectory();
                    var logFactory = opts.GetInitializedDeviceFactory(LogDir);

                    // These must match GarnetServerOptions.GetSettings, EnableStorageTier
                    return isObj
                        ? logFactory.Get(new FileDescriptor("Store", "hlog_objs"))
                        : logFactory.Get(new FileDescriptor("Store", "hlog"));
                }
                return null;
            }
        }

        /// <summary>
        /// Process request from primary to start recovery process from the retrieved checkpoint.
        /// </summary>
        /// <param name="recoverMainStoreFromToken"></param>
        /// <param name="replayAOFMap"></param>
        /// <param name="primaryReplicationId"></param>
        /// <param name="remoteCheckpoint"></param>
        /// <param name="beginAddress"></param>
        /// <param name="recoveredReplicationOffset"></param>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        public AofAddress TryReplicaDiskbasedRecovery(
            bool recoverMainStoreFromToken,
            ulong replayAOFMap,
            string primaryReplicationId,
            CheckpointEntry remoteCheckpoint,
            in AofAddress beginAddress,
            ref AofAddress recoveredReplicationOffset,
            out ReadOnlySpan<byte> errorMessage)
        {
            try
            {
                errorMessage = [];
                UpdateLastPrimarySyncTime();

                logger?.LogInformation("Replica Recover Store: {storeVersion}>[{sIndexToken} {sHlogToken}]",
                    remoteCheckpoint.metadata.storeVersion,
                    remoteCheckpoint.metadata.storeIndexToken,
                    remoteCheckpoint.metadata.storeHlogToken);

                storeWrapper.RecoverCheckpoint(
                    replicaRecover: true,
                    recoverMainStoreFromToken,
                    remoteCheckpoint.metadata);

                if (replayAOFMap > 0)
                {
                    logger?.LogError("ReplicaRecover: replay local AOF from {beginAddress} until {recoveredReplicationOffset}", beginAddress, recoveredReplicationOffset);
                    var replayUntil = recoveredReplicationOffset;
                    for (var sublogIdx = 0; sublogIdx < recoveredReplicationOffset.Length; sublogIdx++)
                        replayUntil[sublogIdx] = (((1UL) << sublogIdx) > 0) ? recoveredReplicationOffset[sublogIdx] : beginAddress[sublogIdx];
                    recoveredReplicationOffset = storeWrapper.ReplayAOF(replayUntil);
                }

                logger?.LogInformation("Initializing AOF");
                storeWrapper.appendOnlyFile.Initialize(beginAddress, recoveredReplicationOffset);

                // Finally, advertise that we are caught up to the replication offset
                replicationOffset = recoveredReplicationOffset;
                logger?.LogInformation("ReplicaRecover: ReplicaReplicationOffset = {ReplicaReplicationOffset}", replicationOffset);

                // If checkpoint for main store was send add its token here in preparation for purge later on
                if (recoverMainStoreFromToken)
                {
                    cEntry.metadata.storeIndexToken = remoteCheckpoint.metadata.storeIndexToken;
                    cEntry.metadata.storeHlogToken = remoteCheckpoint.metadata.storeHlogToken;
                }

                checkpointStore.PurgeAllCheckpointsExceptEntry(cEntry);

                // Initialize in-memory checkpoint store and delete outdated checkpoint entries
                logger?.LogInformation("Initializing CheckpointStore");
                if (!InitializeCheckpointStore())
                    logger?.LogWarning("Failed acquiring latest memory checkpoint metadata at {method}", nameof(TryReplicaDiskbasedRecovery));

                // Update replicationId to mark any subsequent checkpoints as part of this history
                logger?.LogInformation("Updating ReplicationId");
                TryUpdateMyPrimaryReplId(primaryReplicationId);

                // Mark this txn run as a read-write session if we are replaying as a replica
                // This is necessary to ensure that the stored procedure can perform write operations if needed
                clusterProvider.replicationManager.aofProcessor.SetReadWriteSession();

                return this.replicationOffset;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"{nameof(TryReplicaDiskbasedRecovery)}");
                errorMessage = Encoding.ASCII.GetBytes(ex.Message);
                return AofAddress.Create(clusterProvider.serverOptions.AofSublogCount, -1);
            }
            finally
            {
                // Done with recovery at this point
                EndRecovery(RecoveryStatus.CheckpointRecoveredAtReplica, downgradeLock: false);
            }
        }
    }
}