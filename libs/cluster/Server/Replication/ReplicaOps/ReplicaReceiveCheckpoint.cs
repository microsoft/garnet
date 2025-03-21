// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Garnet.client;
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
        /// <param name="nodeId">Node-id to replicate.</param>
        /// <param name="background">If replication sync will run in the background.</param>
        /// <param name="force">Force adding this node as replica.</param>
        /// <param name="tryAddReplica">Execute try add replica.</param>
        /// <param name="errorMessage">The ASCII encoded error message if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
        /// <returns>A boolean indicating whether replication initiation was successful.</returns>
        public bool TryReplicateDiskbasedSync(
            ClusterSession session,
            string nodeId,
            bool background,
            bool force,
            bool tryAddReplica,
            out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = [];
            try
            {
                logger?.LogTrace("CLUSTER REPLICATE {nodeid}", nodeId);
                // Update the configuration to make this node a replica of provided nodeId
                if (tryAddReplica && !clusterProvider.clusterManager.TryAddReplica(nodeId, force: force, out errorMessage, logger: logger))
                    return false;

                // Wait for threads to agree
                session?.UnsafeBumpAndWaitForEpochTransition();

                // Initiate remote checkpoint retrieval
                if (background)
                {
                    logger?.LogInformation("Initiating background checkpoint retrieval");
                    _ = Task.Run(ReplicaSyncAttachTask);
                }
                else
                {
                    logger?.LogInformation("Initiating foreground checkpoint retrieval");
                    var resp = ReplicaSyncAttachTask().GetAwaiter().GetResult();
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

            async Task<string> ReplicaSyncAttachTask()
            {
                Debug.Assert(IsRecovering);
                GarnetClientSession gcs = null;
                try
                {
                    // Resetting here to decide later when to sync from
                    clusterProvider.replicationManager.ReplicationOffset = 0;

                    // The caller should have stopped accepting AOF records from old primary at this point
                    // (TryREPLICAOF -> TryAddReplica -> UnsafeWaitForConfigTransition)

                    // TODO: ensure we have quiesced reads (no writes on replica)

                    // Wait for Commit of AOF (data received from old primary) if FastCommit is not enabled
                    // If FastCommit is enabled, we commit during AOF stream processing
                    if (!clusterProvider.serverOptions.EnableFastCommit)
                    {
                        storeWrapper.appendOnlyFile?.Commit();
                        storeWrapper.appendOnlyFile?.WaitForCommit();
                    }

                    // Reset background replay iterator
                    ResetReplayIterator();

                    // Reset replication offset
                    ReplicationOffset = 0;

                    // Reset the database in preparation for connecting to primary
                    storeWrapper.Reset();

                    // Send request to primary
                    //      Primary will initiate background task and start sending checkpoint data
                    //
                    // Replica waits for retrieval to complete before moving forward to recovery
                    //      Retrieval completion coordinated by remoteCheckpointRetrievalCompleted
                    var current = clusterProvider.clusterManager.CurrentConfig;
                    var (address, port) = current.GetLocalNodePrimaryAddress();

                    if (address == null || port == -1)
                    {
                        var errorMsg = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_NOT_ASSIGNED_PRIMARY_ERROR);
                        logger?.LogError("{msg}", errorMsg);
                        return errorMsg;
                    }
                    gcs = new(
                        new IPEndPoint(IPAddress.Parse(address), port),
                        clusterProvider.replicationManager.GetIRSNetworkBufferSettings,
                        clusterProvider.replicationManager.GetNetworkPool,
                        tlsOptions: clusterProvider.serverOptions.TlsOptions?.TlsClientOptions,
                        authUsername: clusterProvider.ClusterUsername,
                        authPassword: clusterProvider.ClusterPassword);
                    recvCheckpointHandler = new ReceiveCheckpointHandler(clusterProvider, logger);
                    gcs.Connect();

                    var nodeId = current.LocalNodeId;
                    cEntry = GetLatestCheckpointEntryFromDisk();

                    storeWrapper.RecoverAOF();
                    logger?.LogInformation("InitiateReplicaSync: AOF BeginAddress:{beginAddress} AOF TailAddress:{tailAddress}", storeWrapper.appendOnlyFile.BeginAddress, storeWrapper.appendOnlyFile.TailAddress);

                    // 1. Primary will signal checkpoint send complete
                    // 2. Replica will receive signal and recover checkpoint, initialize AOF
                    // 3. Replica signals recovery complete (here cluster replicate will return to caller)
                    // 4. Replica responds with aofStartAddress sync
                    // 5. Primary will initiate aof sync task
                    // 6. Primary releases checkpoint
                    var resp = await gcs.ExecuteReplicaSync(
                        nodeId,
                        PrimaryReplId,
                        cEntry.ToByteArray(),
                        storeWrapper.appendOnlyFile.BeginAddress,
                        storeWrapper.appendOnlyFile.TailAddress).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "An error occurred at ReplicationManager.RetrieveStoreCheckpoint");
                    clusterProvider.clusterManager.TryResetReplica();
                    return ex.Message;
                }
                finally
                {
                    EndRecovery(RecoveryStatus.NoRecovery);
                    recvCheckpointHandler?.Dispose();
                    gcs?.Dispose();
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
                CheckpointFileType.STORE_INDEX => clusterProvider.GetReplicationLogCheckpointManager(StoreType.Main),
                CheckpointFileType.OBJ_STORE_SNAPSHOT or
                CheckpointFileType.OBJ_STORE_INDEX => clusterProvider.GetReplicationLogCheckpointManager(StoreType.Object),
                _ => throw new Exception($"Invalid checkpoint filetype {fileType}"),
            };

            switch (fileType)
            {
                case CheckpointFileType.STORE_SNAPSHOT:
                case CheckpointFileType.OBJ_STORE_SNAPSHOT:
                    ckptManager.CommiLogCheckpointWithCookie(fileToken, checkpointMetadata);
                    break;
                case CheckpointFileType.STORE_INDEX:
                case CheckpointFileType.OBJ_STORE_INDEX:
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
                CheckpointFileType.STORE_SNAPSHOT or
                CheckpointFileType.OBJ_STORE_HLOG or
                CheckpointFileType.OBJ_STORE_SNAPSHOT
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
                CheckpointFileType.STORE_HLOG => GetStoreHLogDevice(),
                CheckpointFileType.OBJ_STORE_HLOG => GetObjectStoreHLogDevice(false),//TODO: return device for object store hlog
                CheckpointFileType.OBJ_STORE_HLOG_OBJ => GetObjectStoreHLogDevice(true),
                _ => clusterProvider.GetReplicationLogCheckpointManager(type.ToStoreType()).GetDevice(type, token),
            };

            if (ShouldInitialize(type))
                device.Initialize(segmentSize: 1L << clusterProvider.serverOptions.SegmentSizeBits());
            return device;

            IDevice GetStoreHLogDevice()
            {
                var opts = clusterProvider.serverOptions;
                if (opts.EnableStorageTier)
                {
                    var LogDir = opts.LogDir;
                    if (LogDir is null or "") LogDir = Directory.GetCurrentDirectory();
                    var logFactory = opts.GetInitializedDeviceFactory(LogDir);
                    return logFactory.Get(new FileDescriptor("Store", "hlog"));
                }
                return null;
            }

            IDevice GetObjectStoreHLogDevice(bool obj)
            {
                var opts = clusterProvider.serverOptions;
                if (opts.EnableStorageTier)
                {
                    var LogDir = opts.LogDir;
                    if (LogDir is null or "") LogDir = Directory.GetCurrentDirectory();
                    var logFactory = opts.GetInitializedDeviceFactory(LogDir);
                    return obj ? logFactory.Get(new FileDescriptor("ObjectStore", "hlog.obj")) : logFactory.Get(new FileDescriptor("ObjectStore", "hlog"));
                }
                return null;
            }
        }

        /// <summary>
        /// Process request from primary to start recovery process from the retrieved checkpoint.
        /// </summary>
        /// <param name="recoverMainStoreFromToken"></param>
        /// <param name="recoverObjectStoreFromToken"></param>
        /// <param name="replayAOF"></param>
        /// <param name="primaryReplicationId"></param>
        /// <param name="remoteCheckpoint"></param>
        /// <param name="beginAddress"></param>
        /// <param name="recoveredReplicationOffset"></param>
        /// <returns></returns>
        public long BeginReplicaRecover(
            bool recoverMainStoreFromToken,
            bool recoverObjectStoreFromToken,
            bool replayAOF,
            string primaryReplicationId,
            CheckpointEntry remoteCheckpoint,
            long beginAddress,
            long recoveredReplicationOffset,
            out ReadOnlySpan<byte> errorMessage)
        {
            try
            {
                errorMessage = [];
                UpdateLastPrimarySyncTime();

                logger?.LogInformation("Replica Recover MainStore: {storeVersion}>[{sIndexToken} {sHlogToken}]" +
                    "\nObjectStore: {objectStoreVersion}>[{oIndexToken} {oHlogToken}]",
                    remoteCheckpoint.metadata.storeVersion,
                    remoteCheckpoint.metadata.storeIndexToken,
                    remoteCheckpoint.metadata.storeHlogToken,
                    remoteCheckpoint.metadata.objectStoreVersion,
                    remoteCheckpoint.metadata.objectStoreIndexToken,
                    remoteCheckpoint.metadata.objectStoreHlogToken);

                storeWrapper.RecoverCheckpoint(
                    replicaRecover: true,
                    recoverMainStoreFromToken,
                    recoverObjectStoreFromToken,
                    remoteCheckpoint.metadata);

                if (replayAOF)
                {
                    logger?.LogInformation("ReplicaRecover: replay local AOF from {beginAddress} until {recoveredReplicationOffset}", beginAddress, recoveredReplicationOffset);
                    recoveredReplicationOffset = storeWrapper.ReplayAOF(recoveredReplicationOffset);
                }

                logger?.LogInformation("Initializing AOF");
                storeWrapper.appendOnlyFile.Initialize(beginAddress, recoveredReplicationOffset);

                // Finally, advertise that we are caught up to the replication offset
                ReplicationOffset = recoveredReplicationOffset;
                logger?.LogInformation("ReplicaRecover: ReplicaReplicationOffset = {ReplicaReplicationOffset}", ReplicationOffset);

                // If checkpoint for main store was send add its token here in preparation for purge later on
                if (recoverMainStoreFromToken)
                {
                    cEntry.metadata.storeIndexToken = remoteCheckpoint.metadata.storeIndexToken;
                    cEntry.metadata.storeHlogToken = remoteCheckpoint.metadata.storeHlogToken;
                }

                // If checkpoint for object store was send add its token here in preparation for purge later on
                if (recoverObjectStoreFromToken)
                {
                    cEntry.metadata.objectStoreIndexToken = remoteCheckpoint.metadata.objectStoreIndexToken;
                    cEntry.metadata.objectStoreHlogToken = remoteCheckpoint.metadata.objectStoreHlogToken;
                }
                checkpointStore.PurgeAllCheckpointsExceptEntry(cEntry);

                // Initialize in-memory checkpoint store and delete outdated checkpoint entries
                logger?.LogInformation("Initializing CheckpointStore");
                InitializeCheckpointStore();

                // Update replicationId to mark any subsequent checkpoints as part of this history
                logger?.LogInformation("Updating ReplicationId");
                TryUpdateMyPrimaryReplId(primaryReplicationId);

                return ReplicationOffset;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"{nameof(BeginReplicaRecover)}");
                errorMessage = Encoding.ASCII.GetBytes(ex.Message);
                return -1;
            }
            finally
            {
                // Done with recovery at this point
                EndRecovery(RecoveryStatus.CheckpointRecoveredAtReplica);
            }
        }
    }
}