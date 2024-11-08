// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        public ReceiveCheckpointHandler recvCheckpointHandler = null;
        public SingleWriterMultiReaderLock replicateLock;
        CheckpointEntry cEntry;

        /// <summary>
        /// Try to initiate replication while instance is up and running.
        /// NOTE: Caller should be aware of the following
        ///     It is assumed that when this method is called we are under epoch protection
        ///     This method will try to acquire the replicate and recovery locks first.
        ///     This method will try to make this instance a replica of the provided node-id by updating the local config after acquiring all the locks.
        /// </summary>
        /// <param name="session">ClusterSession for this connection.</param>
        /// <param name="nodeid">Node-id to replicate.</param>
        /// <param name="background">If replication sync will run in the background.</param>
        /// <param name="force">Force adding this node as replica.</param>
        /// <param name="errorMessage">The ASCII encoded error message if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
        /// <returns>A boolean indicating whether replication initiation was successful.</returns>
        public bool TryBeginReplicate(ClusterSession session, string nodeid, bool background, bool force, out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            // Ensure two replicate commands do not execute at the same time.
            if (!replicateLock.TryWriteLock())
            {
                errorMessage = "ERR Replicate already in progress"u8;
                return false;
            }

            try
            {
                logger?.LogTrace("CLUSTER REPLICATE {nodeid}", nodeid);

                if (!clusterProvider.clusterManager.TryAddReplica(nodeid, force: force, out errorMessage, logger: logger))
                    return false;

                // Wait for threads to agree
                session.UnsafeBumpAndWaitForEpochTransition();

                // Resetting here to decide later when to sync from
                clusterProvider.replicationManager.ReplicationOffset = 0;
                return clusterProvider.replicationManager.TryReplicateFromPrimary(out errorMessage, background);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"{nameof(TryBeginReplicate)}");
                SuspendRecovery();
                return false;
            }
            finally
            {
                replicateLock.WriteUnlock();
            }
        }

        /// <summary>
        /// Try to initiate the attach to primary sequence to recover checkpoint, replay AOF and start AOF stream.
        /// NOTE:
        ///     This method may be called at runtime or at startup so it does not assume that we are running under epoch protection.
        ///     WARNING: Caller is responsible for acquiring the recoveryLock
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <param name="background"></param>
        /// <returns></returns>
        public bool TryReplicateFromPrimary(out ReadOnlySpan<byte> errorMessage, bool background = false)
        {
            errorMessage = default;
            Debug.Assert(Recovering);

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

            // Reset replication offset
            ReplicationOffset = 0;

            // Reset the database in preparation for connecting to primary
            storeWrapper.Reset();

            // Initiate remote checkpoint retrieval
            if (background)
            {
                logger?.LogInformation("Initiating background checkpoint retrieval");
                _ = Task.Run(InitiateReplicaSync);
            }
            else
            {
                logger?.LogInformation("Initiating foreground checkpoint retrieval");
                var resp = InitiateReplicaSync().GetAwaiter().GetResult();
                if (resp != null)
                {
                    errorMessage = Encoding.ASCII.GetBytes(resp);
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Try to initiate replica
        /// </summary>
        /// <returns>A string representing the error message if error occurred; otherwise <see langword="null"/>.</returns>
        private async Task<string> InitiateReplicaSync()
        {
            // Send request to primary
            //      Primary will initiate background task and start sending checkpoint data
            //
            // Replica waits for retrieval to complete before moving forward to recovery
            //      Retrieval completion coordinated by remoteCheckpointRetrievalCompleted
            var current = clusterProvider.clusterManager.CurrentConfig;
            var (address, port) = current.GetLocalNodePrimaryAddress();
            GarnetClientSession gcs = null;

            if (address == null || port == -1)
            {
                var errorMsg = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_NOT_ASSIGNED_PRIMARY_ERROR);
                logger?.LogError("{msg}", errorMsg);
                return errorMsg;
            }

            try
            {
                gcs = new(
                    address,
                    port,
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
                clusterProvider.replicationManager.SuspendRecovery();
                return ex.Message;
            }
            finally
            {
                recvCheckpointHandler?.Dispose();
                gcs?.Dispose();
            }
            return null;
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

        private IDevice GetStoreHLogDevice()
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

        private IDevice GetObjectStoreHLogDevice(bool obj)
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
            long recoveredReplicationOffset)
        {
            try
            {
                UpdateLastPrimarySyncTime();

                logger?.LogInformation("Initiating Checkpoint Recovery at replica {sIndexToken} {sHlogToken} {oIndexToken} {oHlogToken}", remoteCheckpoint.storeIndexToken, remoteCheckpoint.storeHlogToken, remoteCheckpoint.objectStoreIndexToken, remoteCheckpoint.objectStoreHlogToken);
                storeWrapper.RecoverCheckpoint(recoverMainStoreFromToken, recoverObjectStoreFromToken,
                    remoteCheckpoint.storeIndexToken, remoteCheckpoint.storeHlogToken, remoteCheckpoint.objectStoreIndexToken, remoteCheckpoint.objectStoreHlogToken);

                if (replayAOF)
                {
                    logger?.LogInformation("ReplicaRecover: replay local AOF from {beginAddress} until {recoveredReplicationOffset}", beginAddress, recoveredReplicationOffset);
                    storeWrapper.TryReplayAOF(out long offset);
                    recoveredReplicationOffset = offset;
                }

                logger?.LogInformation("Initializing AOF");
                storeWrapper.appendOnlyFile.Initialize(beginAddress, recoveredReplicationOffset);

                // Finally, advertise that we are caught up to the replication offset
                ReplicationOffset = recoveredReplicationOffset;
                logger?.LogInformation("ReplicaRecover: ReplicaReplicationOffset = {ReplicaReplicationOffset}", ReplicationOffset);

                // If checkpoint for main store was send add its token here in preparation for purge later on
                if (recoverMainStoreFromToken)
                {
                    cEntry.storeIndexToken = remoteCheckpoint.storeIndexToken;
                    cEntry.storeHlogToken = remoteCheckpoint.storeHlogToken;
                }

                // If checkpoint for object store was send add its token here in preparation for purge later on
                if (recoverObjectStoreFromToken)
                {
                    cEntry.objectStoreIndexToken = remoteCheckpoint.objectStoreIndexToken;
                    cEntry.objectStoreHlogToken = remoteCheckpoint.objectStoreHlogToken;
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
            finally
            {
                // Done with recovery at this point
                SuspendRecovery();
            }
        }
    }
}