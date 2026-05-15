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
using Garnet.common;
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
        /// <returns>A boolean indicating whether replication initiation was successful, paired with an error message if not.</returns>
        public async Task<(bool Success, ReadOnlyMemory<byte> ErrorMessage)> TryReplicateDiskbasedSyncAsync(
            ClusterSession session,
            ReplicateSyncOptions options)
        {
            ReadOnlyMemory<byte> errorMessage = default;
            try
            {
                logger?.LogTrace("CLUSTER REPLICATE {nodeid}", options.NodeId);
                // Update the configuration to make this node a replica of provided nodeId
                if (options.TryAddReplica)
                {
                    var (success, error) = await clusterProvider.clusterManager.TryAddReplicaAsync(options.NodeId, options.Force, options.UpgradeLock, logger: logger).ConfigureAwait(false); ;
                    if (!success)
                    {
                        return (false, error);
                    }
                }

                // Create or update timestamp manager for sharded log if needed
                storeWrapper.appendOnlyFile.CreateOrUpdateKeySequenceManager();

                // Wait for threads to agree
                if (session != null)
                {
                    await session.UnsafeBumpAndWaitForEpochTransitionAsync().ConfigureAwait(false);
                }

                // Initiate remote checkpoint retrieval
                if (options.Background)
                {
                    logger?.LogInformation("Initiating background checkpoint retrieval");
                    _ = ReplicaSyncAttachTaskAsync(options.UpgradeLock, forceAsync: true);
                }
                else
                {
                    logger?.LogInformation("Initiating foreground checkpoint retrieval");
                    var resp = await ReplicaSyncAttachTaskAsync(options.UpgradeLock, forceAsync: false).ConfigureAwait(false);
                    if (resp != null)
                    {
                        errorMessage = Encoding.ASCII.GetBytes(resp);
                        return (false, errorMessage);
                    }
                }

                return (true, default);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"{nameof(TryReplicateDiskbasedSyncAsync)}");
                return (false, errorMessage);
            }

            async Task<string> ReplicaSyncAttachTaskAsync(bool downgradeLock, bool forceAsync)
            {
                if (forceAsync)
                {
                    await Task.Yield();
                }

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
                        authPassword: clusterProvider.ClusterPassword,
                        clientName: nameof(TryReplicateDiskbasedSyncAsync));
                    await gcs.ConnectAsync((int)clusterProvider.serverOptions.ReplicaSyncTimeout.TotalMilliseconds, linkedCts.Token).ConfigureAwait(false);

                    // Wait for Commit of AOF (data received from old primary) if FastCommit is not enabled
                    // If FastCommit is enabled, we commit during AOF stream processing
                    if (!clusterProvider.serverOptions.EnableFastCommit && storeWrapper.appendOnlyFile != null)
                    {
                        await storeWrapper.appendOnlyFile.Log.CommitAsync().ConfigureAwait(false);
                        await storeWrapper.appendOnlyFile.Log.WaitForCommitAsync().ConfigureAwait(false);
                    }

                    // Reset background replay iterator if this node was a replica
                    clusterProvider.replicationManager.ResetReplicaReplayDriverStore();

                    // Remove aofSync tasks if this node was a primary
                    aofSyncDriverStore.Reset();

                    // Reset replication offset
                    replicationOffset.SetValue(0);

                    // Reset the database in preparation for connecting to primary.
                    // Pause VectorManager's background cleanup task first — Reset's
                    // post-Phase-2 Initialize() rewinds HeadAddress / BeginAddress /
                    // TailPageOffset and reallocates pages. Tsavorite's iterator path is
                    // safe (Initializing flag), but the cleanup task's POST-iterate RMWs
                    // on metadata records (ClearDeleteInProgress / UpdateContextMetadata)
                    // are NOT — they can dereference freed pagePointers and AVE. The pause
                    // serializes the entire cleanup-iteration (iterate + RMWs) with Reset
                    // by holding cleanupGate, restoring Reset's "store is quiesced" contract.
                    //
                    // Pass linkedCts.Token so a slow cleanup-iteration over a large keyspace
                    // doesn't block re-attach indefinitely if the broader replication is
                    // cancelled (ctsRepManager / resetHandler). If PauseCleanupAsync throws
                    // OCE, the try block isn't entered and ResumeCleanup is correctly skipped.
                    var vectorManager = storeWrapper.DefaultDatabase.VectorManager;
                    if (vectorManager != null)
                        await vectorManager.PauseCleanupAsync(linkedCts.Token).ConfigureAwait(false);
                    try
                    {
                        storeWrapper.Reset();
                    }
                    finally
                    {
                        vectorManager?.ResumeCleanup();
                    }

                    // Suspend background tasks that may interfere with AOF
                    await storeWrapper.SuspendPrimaryOnlyTasksAsync().ConfigureAwait(false);

                    // Stop advance time task when reconfiguring node to be replica
                    if (storeWrapper.serverOptions.AofPhysicalSublogCount > 1)
                        await clusterProvider.storeWrapper.TaskManager.CancelAsync(TaskType.AdvanceTimeReplicaTask).ConfigureAwait(false);

                    // Send request to primary
                    //      Primary will initiate background task and start sending checkpoint data
                    //
                    // Replica waits for retrieval to complete before moving forward to recovery
                    //      Retrieval completion coordinated by remoteCheckpointRetrievalCompleted
                    recvCheckpointHandler = new ReceiveCheckpointHandler(clusterProvider, logger);

                    var nodeId = current.LocalNodeId;
                    cEntry = GetLatestCheckpointEntryFromDisk();
                    logger?.LogCheckpointEntry(LogLevel.Information, nameof(ReplicaSyncAttachTaskAsync), cEntry);

                    storeWrapper.RecoverAOF();
                    logger?.LogInformation("InitiateReplicaSync: AOF BeginAddress:{beginAddress} AOF TailAddress:{tailAddress}", storeWrapper.appendOnlyFile.Log.BeginAddress, storeWrapper.appendOnlyFile.Log.TailAddress);

                    var beginAddress = storeWrapper.appendOnlyFile.Log.BeginAddress;
                    var tailAddress = storeWrapper.appendOnlyFile.Log.TailAddress;

                    // 1. Primary will signal checkpoint send complete
                    // 2. Replica will receive signal and recover checkpoint, initialize AOF
                    // 3. Replica signals recovery complete (here cluster replicate will return to caller)
                    // 4. Replica responds with aofStartAddress sync
                    // 5. Primary will initiate aof sync task
                    // 6. Primary releases checkpoint
                    // Exception injection point for testing cluster reset during disk-based replication
                    await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Replication_InProgress_During_DiskBased_Replica_Attach_Sync).WaitAsync(storeWrapper.serverOptions.ReplicaAttachTimeout, linkedCts.Token).ConfigureAwait(false);
                    var resp = await gcs.ExecuteClusterInitiateReplicaSync(
                        nodeId,
                        PrimaryReplId,
                        cEntry.ToByteArray(),
                        beginAddress.Span,
                        tailAddress.Span).WaitAsync(storeWrapper.serverOptions.ReplicaAttachTimeout, linkedCts.Token).ConfigureAwait(false);
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
        /// Check if device needs to be initialized with a specifi segment size depending on the checkpoint file type
        /// </summary>
        /// <param name="type">Checkpoint type</param>
        /// <param name="serverOptions">Server options to acquire segment bit counts</param>
        /// <returns>A tuple indicating whether to initialize and, if so, the segment size bits</returns>
        public static (bool shouldInitialize, int segmentSizeBits) ShouldInitialize(CheckpointFileType type, GarnetServerOptions serverOptions)
        {
            return type switch
            {
                CheckpointFileType.STORE_HLOG or CheckpointFileType.STORE_SNAPSHOT => (true, serverOptions.SegmentSizeBits(isObj: false)),
                CheckpointFileType.STORE_HLOG_OBJ or CheckpointFileType.STORE_SNAPSHOT_OBJ => (true, serverOptions.SegmentSizeBits(isObj: true)),
                _ => (false, 0)
            };
        }

        /// <summary>
        /// Get an IDevice that is also initialized if needed
        /// </summary>
        /// <param name="token"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public IDevice CreateCheckpointDevice(Guid token, CheckpointFileType type)
        {
            var device = type switch
            {
                CheckpointFileType.STORE_HLOG => GetStoreHLogDevice(isObj: false),
                CheckpointFileType.STORE_HLOG_OBJ => GetStoreHLogDevice(isObj: true),
                _ => clusterProvider.ReplicationLogCheckpointManager.GetDevice(type, token),
            };

            var (shouldInitialize, segmentSizeBits) = ShouldInitialize(type, clusterProvider.serverOptions);
            if (shouldInitialize)
                device.Initialize(segmentSize: 1L << segmentSizeBits);
            return device;

            IDevice GetStoreHLogDevice(bool isObj)
            {
                var opts = clusterProvider.serverOptions;
                if (opts.EnableStorageTier)
                {
                    var LogDir = !string.IsNullOrEmpty(opts.LogDir) ? opts.LogDir : Directory.GetCurrentDirectory();
                    var logFactory = opts.GetInitializedDeviceFactory(LogDir);

                    // These must match GarnetServerOptions.GetSettings, EnableStorageTier
                    return logFactory.Get(new FileDescriptor("Store", isObj ? "hlog_objs" : "hlog"));
                }
                return null;
            }
        }

        /// <summary>
        /// Process request from primary to start recovery process from the retrieved checkpoint.
        /// </summary>
        /// <param name="recoverStoreFromToken"></param>
        /// <param name="replayAOFMap"></param>
        /// <param name="primaryReplicationId"></param>
        /// <param name="remoteCheckpoint"></param>
        /// <param name="beginAddress"></param>
        /// <param name="recoveredReplicationOffset"></param>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        public AofAddress TryReplicaDiskbasedRecovery(
            bool recoverStoreFromToken,
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
                    recoverStoreFromToken,
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
                storeWrapper.appendOnlyFile.Log.Initialize(beginAddress, recoveredReplicationOffset);

                // Before we can use the replication offset, we must wait for queued Vector Set ops to complete
                storeWrapper.DefaultDatabase.VectorManager?.WaitForVectorOperationsToComplete();

                // Before we can use the replication offset, we must wait for queued Vector Set ops to complete
                storeWrapper.DefaultDatabase.VectorManager?.WaitForVectorOperationsToComplete();

                // Finally, advertise that we are caught up to the replication offset
                replicationOffset = recoveredReplicationOffset;
                logger?.LogInformation("ReplicaRecover: ReplicaReplicationOffset = {ReplicaReplicationOffset}", replicationOffset);

                // If checkpoint for main store was send add its token here in preparation for purge later on
                if (recoverStoreFromToken)
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

                // Start advance time signal processing background task
                clusterProvider.replicationManager.StartAdvanceTimeBackgroundTask();

                return this.replicationOffset;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"{nameof(TryReplicaDiskbasedRecovery)}");
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