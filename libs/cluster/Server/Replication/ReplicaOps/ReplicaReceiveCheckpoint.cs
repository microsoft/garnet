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
        /// Initiate replication
        /// </summary>
        /// <param name="session">ClusterSession for this connection.</param>
        /// <param name="nodeid">Node-id to replicate.</param>
        /// <param name="background">If replication sync will run in the background.</param>
        /// <param name="force">Force adding this node as replica.</param>
        /// <returns>Return resp span with success or otherwise any err.</returns>
        public ReadOnlySpan<byte> BeginReplicate(ClusterSession session, string nodeid, bool background, bool force)
        {
            ReadOnlySpan<byte> resp = CmdStrings.RESP_OK;
            if (!replicateLock.TryWriteLock())
                return Encoding.ASCII.GetBytes("-ERR Replicate already in progress\r\n");

            try
            {
                // TODO: ensure two replicate commands do not execute at once
                logger?.LogTrace("CLUSTER REPLICATE {nodeid}", nodeid);

                // TryAddReplica will set the recovering boolean
                if (clusterProvider.clusterManager.TryAddReplica(nodeid, force: force, ref recovering, out resp))
                {
                    // Wait for threads to agree
                    session.UnsafeWaitForConfigTransition();

                    //TODO: We should not be resetting this, need to decide where to start syncing from
                    clusterProvider.replicationManager.ReplicationOffset = 0;
                    resp = clusterProvider.replicationManager.TryReplicateFromPrimary(background);
                }
            }
            finally
            {
                replicateLock.WriteUnlock();
            }
            return resp;
        }

        /// <summary>
        /// Attach to primary for replication, recover checkpoint and initiate task for aof streaming
        /// </summary>
        /// <returns></returns>
        public ReadOnlySpan<byte> TryReplicateFromPrimary(bool background = false)
        {
            Debug.Assert(recovering);

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

            //Initiate remote checkpoint retrieval
            if (background)
            {
                logger?.LogInformation("Initiating background checkpoint retrieval");
                Task.Run(InitiateReplicaSync);
            }
            else
            {
                logger?.LogInformation("Initiating foreground checkpoint retrieval");
                var resp = InitiateReplicaSync().GetAwaiter().GetResult();
                if (resp != null)
                    return Encoding.ASCII.GetBytes($"-{resp}\r\n");
            }

            return CmdStrings.RESP_OK;
        }

        private async Task<string> InitiateReplicaSync()
        {
            //1. Send request to primary
            //      primary will initiate background task and start sending checkpoint data
            //
            //2. replica waits for retrieval to complete before moving forward to recovery
            //      retrieval completion coordinated by remoteCheckpointRetrievalCompleted
            var current = clusterProvider.clusterManager.CurrentConfig;
            var (address, port) = current.GetLocalNodePrimaryAddress();
            GarnetClientSession gcs = null;

            if (address == null || port == -1)
            {
                var errorMsg = $"-ERR don't have primary\r\n";
                logger?.LogError(errorMsg);
                return errorMsg;
            }

            try
            {
                gcs = new(address, port, clusterProvider.serverOptions.TlsOptions?.TlsClientOptions, authUsername: clusterProvider.ClusterUsername, authPassword: clusterProvider.ClusterPassword, bufferSize: 1 << 21);
                recvCheckpointHandler = new ReceiveCheckpointHandler(clusterProvider, logger);
                gcs.Connect();

                var nodeId = current.GetLocalNodeId();
                cEntry = clusterProvider.replicationManager.GetLatestCheckpointEntryFromDisk();

                storeWrapper.RecoverAOF();
                logger?.LogInformation("InitiateReplicaSync: AOF BeginAddress:{beginAddress} AOF TailAddress:{tailAddress}", storeWrapper.appendOnlyFile.BeginAddress, storeWrapper.appendOnlyFile.TailAddress);

                //1. Primary will signal checkpoint send complete
                //2. Replica will receive signal and recover checkpoint, initialize AOF
                //3. Replica signals recovery complete (here cluster replicate will return to caller)
                //4. Replica responds with aofStartAddress sync
                //5. Primary will initiate aof sync task
                //6. Primary releases checkpoint
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
            ReplicationLogCheckpointManager ckptManager = fileType switch
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

        private long GetObjectStoreSnapshotSize(Guid token)
        {
            var device = clusterProvider.GetReplicationLogCheckpointManager(StoreType.Object).GetDevice(CheckpointFileType.OBJ_STORE_SNAPSHOT_OBJ, token);
            long size = 0;
            if (device is not null)
            {
                device.Initialize(-1);
                size = device.GetFileSize(0);
                device.Dispose();
            }
            return size;
        }

        /// <summary>
        /// Check if device needs to be initialized with a specifi segment size depending on the checkpoint file type
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public bool ShouldInitialize(CheckpointFileType type)
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
            IDevice device = type switch
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
        /// <param name="primary_replid"></param>
        /// <param name="remoteCheckpoint"></param>
        /// <param name="beginAddress"></param>
        /// <param name="recoveredReplicationOffset"></param>
        /// <returns></returns>
        public long BeginReplicaRecover(
            bool recoverMainStoreFromToken,
            bool recoverObjectStoreFromToken,
            bool replayAOF,
            string primary_replid,
            CheckpointEntry remoteCheckpoint,
            long beginAddress,
            long recoveredReplicationOffset)
        {
            storeWrapper.RecoverCheckpoint(recoverMainStoreFromToken, recoverObjectStoreFromToken,
                remoteCheckpoint.storeIndexToken, remoteCheckpoint.storeHlogToken, remoteCheckpoint.objectStoreIndexToken, remoteCheckpoint.objectStoreHlogToken);

            if (replayAOF)
            {
                logger?.LogInformation("ReplicaRecover: replay local AOF from {beginAddress} until {recoveredReplicationOffset}", beginAddress, recoveredReplicationOffset);
                recoveredReplicationOffset = storeWrapper.ReplayAOF(recoveredReplicationOffset);
            }

            storeWrapper.appendOnlyFile.Initialize(beginAddress, recoveredReplicationOffset);

            // Done with recovery at this point
            recovering = false;

            // Finally, advertise that we are caught up to the replication offset
            ReplicationOffset = recoveredReplicationOffset;
            logger?.LogInformation("ReplicaRecover: ReplicaReplicationOffset = {ReplicaReplicationOffset}", ReplicationOffset);

            //if checkpoint for main store was send add its token here in preparation for purge later on
            if (recoverMainStoreFromToken)
            {
                cEntry.storeIndexToken = remoteCheckpoint.storeIndexToken;
                cEntry.storeHlogToken = remoteCheckpoint.storeHlogToken;
            }

            //if checkpoint for object store was send add its token here in preparation for purge later on
            if (recoverObjectStoreFromToken)
            {
                cEntry.objectStoreIndexToken = remoteCheckpoint.objectStoreIndexToken;
                cEntry.objectStoreHlogToken = remoteCheckpoint.objectStoreHlogToken;
            }
            checkpointStore.PurgeAllCheckpointsExceptEntry(cEntry);

            //Initialize in-memory checkpoint store and delete outdated checkpoint entries
            InitializeCheckpointStore();

            TryUpdateMyPrimaryReplId(primary_replid);

            return ReplicationOffset;
        }
    }
}