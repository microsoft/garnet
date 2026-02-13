// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.cluster.Server.Replication;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        readonly ClusterProvider clusterProvider;
        readonly StoreWrapper storeWrapper;
        public AofProcessor AofProcessor => aofProcessor;
        readonly AofProcessor aofProcessor;
        readonly CheckpointStore checkpointStore;
        readonly ReplicationSyncManager replicationSyncManager;
        readonly CancellationTokenSource ctsRepManager = new();
        CancellationTokenSource resetHandler = new();

        readonly int pageSizeBits;
        public int PageSizeBits => pageSizeBits;

        readonly ILogger logger;
        bool _disposed;

        private long primary_sync_last_time;

        internal long LastPrimarySyncSeconds => IsRecovering ? (DateTime.UtcNow.Ticks - primary_sync_last_time) / TimeSpan.TicksPerSecond : 0;

        internal void UpdateLastPrimarySyncTime() => this.primary_sync_last_time = DateTime.UtcNow.Ticks;

        private SingleWriterMultiReaderLock recoverLock;
        private SingleWriterMultiReaderLock recoveryStateChangeLock;

        private long lastEnsureReplicationAttempt;

        public bool IsRecovering => currentRecoveryStatus is not (RecoveryStatus.NoRecovery or RecoveryStatus.ReadRole);

        public bool CannotStreamAOF => IsRecovering && currentRecoveryStatus != RecoveryStatus.CheckpointRecoveredAtReplica;

        private AofAddress replicationOffset;

        public AofAddress ReplicationOffset
        {
            get
            {
                if (!storeWrapper.serverOptions.EnableAOF)
                    return replicationOffset;

                // Primary tracks replicationOffset indirectly through AOF tailAddress
                // Replica will adjust replication offset as it receives data from primary (TODO: since AOFs are synced this might obsolete)
                var role = clusterProvider.clusterManager.CurrentConfig.LocalNodeRole;
                if (role == NodeRole.PRIMARY)
                    return storeWrapper.appendOnlyFile.Log.TailAddress;
                return replicationOffset;
            }
        }

        public void SetSublogReplicationOffset(int sublogIdx, long offset)
            => replicationOffset[sublogIdx] = offset;
        public void IncrementSublogReplicationOffset(int sublogIdx, long offset)
            => replicationOffset[sublogIdx] += offset;
        public long GetSublogReplicationOffset(int sublogIdx)
            => replicationOffset[sublogIdx];

        /// <summary>
        /// Replication offset corresponding to the checkpoint start marker. We will truncate only to this point after taking a checkpoint (the checkpoint
        /// is taken only when we encounter a checkpoint end marker).
        /// </summary>
        public AofAddress ReplicationCheckpointStartOffset;

        /// <summary>
        /// Replication offset until which AOF address is valid for old primary if failover has occurred
        /// </summary>
        public AofAddress ReplicationOffset2 => currentReplicationConfig.replicationOffset2;

        public string PrimaryReplId => currentReplicationConfig.PrimaryReplId;
        public string PrimaryReplId2 => currentReplicationConfig.PrimaryReplId2;

        /// <summary>
        /// Recovery status
        /// </summary>
        public RecoveryStatus currentRecoveryStatus;

        public GarnetClusterCheckpointManager CheckpointManager => (GarnetClusterCheckpointManager)storeWrapper.store.CheckpointManager;

        public AofAddress GetRecoveredSafeAofAddress()
        {
            var storeAofAddress = clusterProvider.replicationManager.CheckpointManager.RecoveredSafeAofAddress;
            return storeAofAddress;
        }

        public AofAddress GetCurrentSafeAofAddress()
        {
            var storeAofAddress = clusterProvider.replicationManager.CheckpointManager.CurrentSafeAofAddress;
            return storeAofAddress;
        }

        public ReplicationManager(ClusterProvider clusterProvider, ILogger logger = null)
        {
            var opts = clusterProvider.serverOptions;
            this.logger = logger;
            this.clusterProvider = clusterProvider;
            this.storeWrapper = clusterProvider.storeWrapper;
            this.pageSizeBits = storeWrapper.appendOnlyFile == null ? 0 : storeWrapper.appendOnlyFile.Log.UnsafeGetLogPageSizeBits();

            networkBufferSettings.Log(logger, nameof(ReplicationManager));
            this.networkPool = networkBufferSettings.CreateBufferPool(logger: logger);
            ValidateNetworkBufferSettings();

            aofProcessor = new AofProcessor(storeWrapper, recordToAof: false, clusterProvider: clusterProvider, logger: logger);
            replicaSyncSessionTaskStore = new ReplicaSyncSessionTaskStore(storeWrapper, clusterProvider, logger);
            replicationSyncManager = new ReplicationSyncManager(clusterProvider, logger);

            replicationOffset = AofAddress.Create(clusterProvider.serverOptions.AofPhysicalSublogCount, kFirstValidAofAddress);
            ReplicationCheckpointStartOffset = AofAddress.Create(clusterProvider.serverOptions.AofPhysicalSublogCount, kFirstValidAofAddress);

            // Set the appendOnlyFile field for all stores
            clusterProvider.ReplicationLogCheckpointManager.checkpointVersionShiftStart = CheckpointVersionShiftStart;
            clusterProvider.ReplicationLogCheckpointManager.checkpointVersionShiftEnd = CheckpointVersionShiftEnd;

            // If this node starts as replica, it cannot serve requests until it is connected to primary
            if (clusterProvider.clusterManager.CurrentConfig.LocalNodeRole == NodeRole.REPLICA && clusterProvider.serverOptions.Recover && !BeginRecovery(RecoveryStatus.InitializeRecover, upgradeLock: false))
                throw new Exception(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK));

            checkpointStore = new CheckpointStore(storeWrapper, clusterProvider, true, logger);
            aofSyncDriverStore = new(clusterProvider, 1, logger);

            var clusterFolder = "/cluster";
            var clusterDataPath = opts.CheckpointDir + clusterFolder;
            var deviceFactory = opts.GetInitializedDeviceFactory(clusterDataPath);
            replicationConfigDevice = deviceFactory.Get(new FileDescriptor(directoryName: "", fileName: "replication.conf"));
            replicationConfigDevicePool = new(1, (int)replicationConfigDevice.SectorSize);

            var canRecoverReplicationHistory = replicationConfigDevice.GetFileSize(0) > 0;
            if (clusterProvider.serverOptions.Recover && canRecoverReplicationHistory)
            {
                logger?.LogTrace("Recovering in-memory checkpoint registry");
                // If recover option is enabled and replication history information is available
                // recover replication history and initialize in-memory checkpoint registry.
                RecoverReplicationHistory();
            }
            else
            {
                logger?.LogTrace("Initializing new in-memory checkpoint registry");
                // If recover option is not enabled or replication history is not available
                // initialize new empty replication history.
                InitializeReplicationHistory(storeWrapper.serverOptions.AofPhysicalSublogCount);
            }

            // After initializing replication history propagate replicationId to ReplicationLogCheckpointManager
            SetPrimaryReplicationId();
        }

        /// <summary>
        /// If this node is a Replica, ensures that a session engaged in replication exists from its paired primary.
        /// </summary>
        public void EnsureReplication(ClusterSession activeSession, IEnumerable<IClusterSession> allClusterSessions)
        {
            var pollFrequency = clusterProvider.serverOptions.ClusterReplicationReestablishmentTimeout;

            if (pollFrequency == 0)
            {
                // Disabled
                return;
            }

            var oldLastEnsureReplicationAttempt = Volatile.Read(ref lastEnsureReplicationAttempt);

            var now = Environment.TickCount64;
            var sinceLastCheck = now - oldLastEnsureReplicationAttempt;

            if (TimeSpan.FromMilliseconds(sinceLastCheck) < TimeSpan.FromSeconds(pollFrequency))
            {
                // Last attempt was too recent
                return;
            }

            var primaryId = clusterProvider.clusterManager.CurrentConfig.LocalNodePrimaryId;

            // We only care to take any action if we're a Replica the active session is also our Primary
            if (!clusterProvider.clusterManager.CurrentConfig.IsReplica || activeSession.RemoteNodeId != primaryId)
            {
                return;
            }

            // If any session is actively replicating, we don't need to take any action
            //
            // Note that we can rely on this because, in the event a replication task on a Primary faults, we close
            // the connection it has open to its Replica
            if (allClusterSessions.Any(static x => x.IsReplicating))
            {
                return;
            }

            // Now we're going to attempt to re-establish replication

            // To avoid a TOCTOU issue, we need to prevent role change while we do this
            if (!clusterProvider.PreventRoleChange())
            {
                return;
            }

            var suppressUnlock = false;
            try
            {
                if (!clusterProvider.clusterManager.CurrentConfig.IsReplica || clusterProvider.clusterManager.CurrentConfig.LocalNodePrimaryId != primaryId)
                {
                    // This nodes replication state has changed since we decided to re-sync, bail
                    return;
                }

                var newLastEnsureReplicationAttempt = Environment.TickCount64;
                if (Interlocked.CompareExchange(ref lastEnsureReplicationAttempt, newLastEnsureReplicationAttempt, oldLastEnsureReplicationAttempt) != oldLastEnsureReplicationAttempt)
                {
                    // Another attempt was made, somehow, so bail
                    return;
                }

                logger.LogInformation("Beginning resync to {primaryId} after replication session failed", primaryId);

                // At this point we need to hold the lock until this upcoming task completes
                suppressUnlock = true;
                _ = Task.Run(
                    () =>
                    {
                        try
                        {
                            // Because of lock shenanigans we can't use Background: true here
                            ReplicateSyncOptions syncOpts = new(primaryId, Background: false, Force: true, TryAddReplica: true, AllowReplicaResetOnFailure: false, UpgradeLock: true);
                            ReadOnlySpan<byte> errorMessage;
                            var success =
                                    clusterProvider.serverOptions.ReplicaDisklessSync ?
                                    clusterProvider.replicationManager.TryReplicateDisklessSync(activeSession, syncOpts, out errorMessage) :
                                    clusterProvider.replicationManager.TryReplicateDiskbasedSync(activeSession, syncOpts, out errorMessage);

                            if (success)
                            {
                                logger.LogInformation("Resync to {primaryId} successfully started", primaryId);
                            }
                            else
                            {
                                logger.LogWarning("Failed to resync to {primaryId} after replication session failed: {errorMessage}", primaryId, Encoding.UTF8.GetString(errorMessage));
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Error encountered on replication recovery background task");
                        }
                        finally
                        {
                            clusterProvider.AllowRoleChange();
                        }
                    }
                );
            }
            finally
            {
                if (!suppressUnlock)
                {
                    clusterProvider.AllowRoleChange();
                }
            }
        }

        /// <summary>
        /// Used to free up buffer pool
        /// </summary>
        public void Purge() => networkPool.Purge();

        public string GetBufferPoolStats() => networkPool.GetStats();

        void CheckpointVersionShiftStart(bool isMainStore, long oldVersion, long newVersion, bool isStreaming)
        {
            if (clusterProvider.clusterManager.CurrentConfig.LocalNodeRole == NodeRole.REPLICA)
                return;

            if (isStreaming)
            {
                if (isMainStore)
                    storeWrapper.EnqueueCommit(AofEntryType.MainStoreStreamingCheckpointStartCommit, newVersion);
                else
                    storeWrapper.EnqueueCommit(AofEntryType.ObjectStoreStreamingCheckpointStartCommit, newVersion);
            }
            else
            {
                // We enqueue a single checkpoint start marker, since we have unified checkpointing
                if (isMainStore)
                    storeWrapper.EnqueueCommit(AofEntryType.CheckpointStartCommit, newVersion);
            }
        }

        void CheckpointVersionShiftEnd(bool isMainStore, long oldVersion, long newVersion, bool isStreaming)
        {
            if (clusterProvider.clusterManager.CurrentConfig.LocalNodeRole == NodeRole.REPLICA)
                return;

            if (isStreaming)
            {
                if (isMainStore)
                    storeWrapper.EnqueueCommit(AofEntryType.MainStoreStreamingCheckpointEndCommit, newVersion);
                else
                    storeWrapper.EnqueueCommit(AofEntryType.ObjectStoreStreamingCheckpointEndCommit, newVersion);
            }
            else
            {
                // We enqueue a single checkpoint end marker, since we have unified checkpointing
                if (isMainStore)
                    storeWrapper.EnqueueCommit(AofEntryType.CheckpointEndCommit, newVersion);
            }
        }

        /// <summary>
        /// Acquire recovery and checkpoint locks to prevent checkpoints and parallel recovery tasks
        /// </summary>
        /// <param name="nextRecoveryStatus">Status to transition to on success</param>
        /// <param name="upgradeLock">If true, will attempt to upgrade a read lock to a write lock.  Assumes a prior successful call to <see cref="BeginRecovery(RecoveryStatus, bool)"/> with <see cref="RecoveryStatus.ReadRole"/>.</param>
        public bool BeginRecovery(RecoveryStatus nextRecoveryStatus, bool upgradeLock)
        {
            if (upgradeLock)
            {
                if (!recoverLock.TryUpgradeReadLock())
                {
                    return false;
                }

                currentRecoveryStatus = nextRecoveryStatus;
                logger?.LogTrace("Upgraded recover lock [{recoverStatus}]", nextRecoveryStatus);
                return true;
            }

            if (currentRecoveryStatus != RecoveryStatus.NoRecovery)
            {
                logger?.LogError("Error background recovering task has not completed [{recoverStatus}]", nextRecoveryStatus);
                return false;
            }

            if (!clusterProvider.storeWrapper.TryPauseCheckpoints())
            {
                logger?.LogError("Error could not acquire checkpoint lock [{recoverStatus}]", nextRecoveryStatus);
                return false;
            }

            // For just reading a role, we need a non-exclusive lock
            var lockAcquired =
                nextRecoveryStatus == RecoveryStatus.ReadRole ?
                    recoverLock.TryReadLock() :
                    recoverLock.TryWriteLock();

            if (!lockAcquired)
            {
                logger?.LogError("Error could not acquire recover lock [{recoverStatus}]", nextRecoveryStatus);
                // If failed to acquire recoverLock re-enable checkpoint taking
                clusterProvider.storeWrapper.ResumeCheckpoints();
                return false;
            }

            currentRecoveryStatus = nextRecoveryStatus;
            logger?.LogTrace("Success recover lock [{recoverStatus}]", nextRecoveryStatus);
            return true;
        }

        /// <summary>
        /// Release recovery and checkpoint locks
        /// </summary>
        /// <param name="nextRecoveryStatus">State to transition to.</param>
        /// <param name="downgradeLock">If true, downgrades the held write lock to a read lock instead of just releasing the write lock.</param>
        public void EndRecovery(RecoveryStatus nextRecoveryStatus, bool downgradeLock)
        {
            Debug.Assert(!downgradeLock || nextRecoveryStatus == RecoveryStatus.ReadRole, "Can only downgrade to a read lock for ReadRole status");

            logger?.LogTrace("{method} [{currentRecoveryStatus},{nextRecoveryStatus}]", nameof(EndRecovery), currentRecoveryStatus, nextRecoveryStatus);

            try
            {
                recoveryStateChangeLock.WriteLock();
                switch (currentRecoveryStatus)
                {
                    case RecoveryStatus.NoRecovery:
                        throw new GarnetException($"Invalid state change [{currentRecoveryStatus},{nextRecoveryStatus}]");
                    case RecoveryStatus.InitializeRecover:
                    case RecoveryStatus.ClusterReplicate:
                    case RecoveryStatus.ClusterFailover:
                    case RecoveryStatus.ReplicaOfNoOne:
                        switch (nextRecoveryStatus)
                        {
                            case RecoveryStatus.CheckpointRecoveredAtReplica:
                                Debug.Assert(currentRecoveryStatus is not RecoveryStatus.NoRecovery and not RecoveryStatus.CheckpointRecoveredAtReplica);
                                currentRecoveryStatus = nextRecoveryStatus;
                                break;
                            case RecoveryStatus.NoRecovery:
                                currentRecoveryStatus = nextRecoveryStatus;
                                recoverLock.WriteUnlock();
                                clusterProvider.storeWrapper.ResumeCheckpoints();
                                break;
                            case RecoveryStatus.ReadRole:
                                currentRecoveryStatus = nextRecoveryStatus;
                                recoverLock.DowngradeWriteLock();
                                break;
                            default:
                                throw new GarnetException($"Invalid state change [{currentRecoveryStatus},{nextRecoveryStatus}]");
                        }
                        break;
                    case RecoveryStatus.CheckpointRecoveredAtReplica:
                        switch (nextRecoveryStatus)
                        {
                            case RecoveryStatus.NoRecovery:
                                currentRecoveryStatus = nextRecoveryStatus;
                                recoverLock.WriteUnlock();
                                clusterProvider.storeWrapper.ResumeCheckpoints();
                                break;
                            case RecoveryStatus.ReadRole:
                                currentRecoveryStatus = nextRecoveryStatus;
                                recoverLock.DowngradeWriteLock();
                                break;
                            default:
                                throw new GarnetException($"Invalid state change [{currentRecoveryStatus},{nextRecoveryStatus}]");
                        }
                        break;
                    case RecoveryStatus.ReadRole:
                        if (downgradeLock)
                        {
                            throw new GarnetException($"Cannot downgrade lock FROM a ReadRole [{currentRecoveryStatus}, {nextRecoveryStatus}]");
                        }

                        currentRecoveryStatus = nextRecoveryStatus;
                        recoverLock.ReadUnlock();
                        clusterProvider.storeWrapper.ResumeCheckpoints();
                        break;
                }
            }
            finally
            {
                recoveryStateChangeLock.WriteUnlock();
            }
        }

        public void ResetRecovery()
        {
            switch (currentRecoveryStatus)
            {
                case RecoveryStatus.ClusterReplicate:
                case RecoveryStatus.ClusterFailover:
                case RecoveryStatus.ReplicaOfNoOne:
                case RecoveryStatus.CheckpointRecoveredAtReplica:
                case RecoveryStatus.InitializeRecover:
                    resetHandler.Cancel();
                    break;
            }
        }

        public void Dispose()
        {
            _disposed = true;
            ctsRepManager.Cancel();
            replicationConfigDevice?.Dispose();
            replicationConfigDevicePool?.Free();

            replicationSyncManager?.Dispose();

            checkpointStore.WaitForReplicas();
            replicaSyncSessionTaskStore.Dispose();
            ReplicaReplayDriverStore?.Dispose();
            ctsRepManager.Dispose();
            aofSyncDriverStore.Dispose();
            aofProcessor?.Dispose();
            networkPool?.Dispose();
        }

        /// <summary>
        /// Main recover method for replication
        /// </summary>
        public void Recover()
        {
            var nodeRole = clusterProvider.clusterManager.CurrentConfig.LocalNodeRole;

            switch (nodeRole)
            {
                case NodeRole.PRIMARY:
                    RecoverCheckpointAndAOF();
                    break;
                case NodeRole.REPLICA:
                    // If configured, load from disk - otherwise wait to connect with a Primary
                    if (clusterProvider.serverOptions.ClusterReplicaResumeWithData)
                    {
                        RecoverCheckpointAndAOF();
                    }

                    break;
                default:
                    logger?.LogError("Not valid role for node {nodeRole}", nodeRole);
                    throw new Exception($"Not valid role for node {nodeRole}");
            }
        }

        /// <summary>
        /// Recover whatever is available from <see cref="storeWrapper"/>.
        /// </summary>
        private void RecoverCheckpointAndAOF()
        {
            storeWrapper.RecoverCheckpoint();
            storeWrapper.RecoverAOF();
            if (clusterProvider.serverOptions.EnableAOF)
            {
                // If recovered checkpoint corresponds to an unavailable AOF address, we initialize AOF to that address
                var recoveredSafeAofAddress = GetRecoveredSafeAofAddress();
                storeWrapper.appendOnlyFile.Log.InitializeIf(ref recoveredSafeAofAddress);
                logger?.LogInformation("Recovered AOF: begin address = {beginAddress}, tail address = {tailAddress}", storeWrapper.appendOnlyFile.Log.BeginAddress, storeWrapper.appendOnlyFile.Log.TailAddress);
                var replayedUntil = storeWrapper.ReplayAOF(AofAddress.Create(clusterProvider.serverOptions.AofPhysicalSublogCount, -1));
                replicationOffset.SetValue(ref replayedUntil);
            }

            // First recover and then load latest checkpoint info in-memory
            if (!InitializeCheckpointStore())
                logger?.LogWarning("Failed acquiring latest memory checkpoint metadata at {method}", nameof(RecoverCheckpointAndAOF));
        }

        /// <summary>
        /// Wait for local replication offset to sync with input value
        /// </summary>
        /// <param name="primaryReplicationOffset"></param>
        /// <returns></returns>
        public async Task<AofAddress> WaitForReplicationOffset(AofAddress primaryReplicationOffset)
        {
            while (ReplicationOffset.AnyLesser(primaryReplicationOffset))
            {
                if (ctsRepManager.IsCancellationRequested) return AofAddress.Create(clusterProvider.serverOptions.AofPhysicalSublogCount, -1);
                await Task.Yield();
            }
            return ReplicationOffset;
        }

        /// <summary>
        /// Initiate connection with PRIMARY after restart
        /// </summary>
        public void Start()
        {
            if (clusterProvider.clusterManager == null)
                return;

            var current = clusterProvider.clusterManager.CurrentConfig;

            var localNodeRole = current.LocalNodeRole;
            var replicaOfNodeId = current.LocalNodePrimaryId;
            if (localNodeRole == NodeRole.REPLICA && clusterProvider.serverOptions.Recover && replicaOfNodeId != null)
            {
                ReplicateSyncOptions syncOpts = new(
                    null,
                    Background: false,
                    Force: clusterProvider.serverOptions.ReplicaDisklessSync,
                    TryAddReplica: false,
                    AllowReplicaResetOnFailure: false,
                    UpgradeLock: false
                );
                var success = clusterProvider.serverOptions.ReplicaDisklessSync ?
                    TryReplicateDisklessSync(null, syncOpts, out var errorMessage) :
                    TryReplicateDiskbasedSync(null, syncOpts, out errorMessage);
                // At initialization of ReplicationManager, this node has been put into recovery mode
                if (!success)
                    logger?.LogError($"An error occurred at {nameof(ReplicationManager)}.{nameof(Start)} {{error}}", Encoding.ASCII.GetString(errorMessage));
            }
            else if (localNodeRole == NodeRole.PRIMARY && replicaOfNodeId == null)
            {
                // Restarting as a primary we do nothing.
                // The replica will have to initiate the recovery process.
            }
            else
            {
                logger?.LogWarning("Replication manager starting configuration inconsistent role:{role} replicaOfId:{replicaOfNodeId}", replicaOfNodeId, localNodeRole);
            }
        }

        /// <summary>
        /// Process message from primary related to observing a specific tail address snapshot at a given sequence number (timestamp)
        /// </summary>
        /// <param name="sequenceNumber">Sequence number associated with observing the given tail address.</param>
        /// <param name="tailAddress">Tail address snapshot</param>
        /// <returns></returns>
        public void AdvanceTime(long sequenceNumber, AofAddress tailAddress)
        {
            lock (observationLock)
            {
                observedTailAddress.MonotonicUpdate(ref tailAddress);
                _ = Utility.MonotonicUpdate(ref observationSequenceNumber, sequenceNumber, out _);
            }
        }

        object observationLock = new();
        AofAddress observedTailAddress;
        long observationSequenceNumber;

        /// <summary>
        /// Starts a background task that advances the replica time if multiple AOF physical sublogs are configured.
        /// </summary>
        public void StartAdvanceTimeBackgroundTask()
        {
            if (clusterProvider.serverOptions.AofPhysicalSublogCount > 1)
            {
                if (!clusterProvider.storeWrapper.TaskManager.RegisterAndRun(TaskType.AdvanceTimeReplicaTask, (token) => AdvanceTimeBackground(token)))
                {
                    logger?.LogError("Failed to register AdvanceTime task at the replica");
                    throw new GarnetException("Failed to register AdvanceTime task at the replica");
                }
                observedTailAddress = AofAddress.Create(clusterProvider.serverOptions.AofPhysicalSublogCount, 0);
            }

            async Task AdvanceTimeBackground(CancellationToken token)
            {
                while (!token.IsCancellationRequested)
                {
                    await Task.Delay(clusterProvider.serverOptions.AofTailWitnessFreq, token);
                    lock (observationLock)
                    {
                        for (var i = 0; i < observedTailAddress.Length; i++)
                        {
                            // Move logical time forward for sublog if the replay has progressed at least until the tailAddress
                            if (observedTailAddress[i] <= replicationOffset[i])
                            {
                                storeWrapper.appendOnlyFile.readConsistencyManager.UpdatePhysicalSublogMaxSequenceNumber(i, observationSequenceNumber);
                            }
                        }
                    }
                }
            }
        }
    }
}