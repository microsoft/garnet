// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
        readonly AofProcessor aofProcessor;
        readonly CheckpointStore checkpointStore;

        readonly CancellationTokenSource ctsRepManager = new();

        readonly ILogger logger;
        bool _disposed;

        private long primary_sync_last_time;

        internal long LastPrimarySyncSeconds => Recovering ? (DateTime.UtcNow.Ticks - primary_sync_last_time) / TimeSpan.TicksPerSecond : 0;

        internal void UpdateLastPrimarySyncTime() => this.primary_sync_last_time = DateTime.UtcNow.Ticks;

        private SingleWriterMultiReaderLock recoverLock;
        public bool Recovering => recoverLock.IsWriteLocked;

        private long replicationOffset;
        public long ReplicationOffset
        {
            get
            {
                // Primary tracks replicationOffset indirectly through AOF tailAddress
                // Replica will adjust replication offset as it receives data from primary (TODO: since AOFs are synced this might obsolete)
                var role = clusterProvider.clusterManager.CurrentConfig.LocalNodeRole;
                return role == NodeRole.PRIMARY ?
                    (clusterProvider.serverOptions.EnableAOF && storeWrapper.appendOnlyFile.TailAddress > kFirstValidAofAddress ? storeWrapper.appendOnlyFile.TailAddress : kFirstValidAofAddress) :
                    replicationOffset;
            }

            set { replicationOffset = value; }
        }

        /// <summary>
        /// Replication offset until which AOF address is valid for old primary if failover has occurred
        /// </summary>
        public long ReplicationOffset2
        {
            get { return currentReplicationConfig.replicationOffset2; }
        }

        public string PrimaryReplId => currentReplicationConfig.primary_replid;
        public string PrimaryReplId2 => currentReplicationConfig.primary_replid2;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReplicationLogCheckpointManager GetCkptManager(StoreType storeType)
        {
            return storeType switch
            {
                StoreType.Main => (ReplicationLogCheckpointManager)storeWrapper.store.CheckpointManager,
                StoreType.Object => (ReplicationLogCheckpointManager)storeWrapper.objectStore?.CheckpointManager,
                _ => throw new Exception($"GetCkptManager: unexpected state {storeType}")
            };
        }

        public long GetRecoveredSafeAofAddress()
        {
            var storeAofAddress = clusterProvider.replicationManager.GetCkptManager(StoreType.Main).RecoveredSafeAofAddress;
            var objectStoreAofAddress = clusterProvider.serverOptions.DisableObjects ? long.MaxValue : clusterProvider.replicationManager.GetCkptManager(StoreType.Object).RecoveredSafeAofAddress;
            return Math.Min(storeAofAddress, objectStoreAofAddress);
        }

        public long GetCurrentSafeAofAddress()
        {
            var storeAofAddress = clusterProvider.replicationManager.GetCkptManager(StoreType.Main).CurrentSafeAofAddress;
            var objectStoreAofAddress = clusterProvider.serverOptions.DisableObjects ? long.MaxValue : clusterProvider.replicationManager.GetCkptManager(StoreType.Object).CurrentSafeAofAddress;
            return Math.Min(storeAofAddress, objectStoreAofAddress);
        }

        public ReplicationManager(ClusterProvider clusterProvider, ILogger logger = null)
        {
            var opts = clusterProvider.serverOptions;
            this.logger = logger;
            this.clusterProvider = clusterProvider;
            this.storeWrapper = clusterProvider.storeWrapper;

            this.networkPool = networkBufferSettings.CreateBufferPool(logger: logger);
            ValidateNetworkBufferSettings();

            aofProcessor = new AofProcessor(storeWrapper, recordToAof: false, logger: logger);
            replicaSyncSessionTaskStore = new ReplicaSyncSessionTaskStore(storeWrapper, clusterProvider, logger);

            ReplicationOffset = 0;

            // Set the appendOnlyFile field for all stores
            clusterProvider.GetReplicationLogCheckpointManager(StoreType.Main).checkpointVersionShift = CheckpointVersionShift;
            if (storeWrapper.objectStore != null)
                clusterProvider.GetReplicationLogCheckpointManager(StoreType.Object).checkpointVersionShift = CheckpointVersionShift;

            // If this node starts as replica, it cannot serve requests until it is connected to primary
            if (clusterProvider.clusterManager.CurrentConfig.LocalNodeRole == NodeRole.REPLICA && !StartRecovery())
                throw new Exception(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK));

            checkpointStore = new CheckpointStore(storeWrapper, clusterProvider, true, logger);
            aofTaskStore = new(clusterProvider, 1, logger);

            var clusterFolder = "/cluster";
            var clusterDataPath = opts.CheckpointDir + clusterFolder;
            var deviceFactory = opts.GetInitializedDeviceFactory(clusterDataPath);
            replicationConfigDevice = deviceFactory.Get(new FileDescriptor(directoryName: "", fileName: "replication.conf"));
            pool = new(1, (int)replicationConfigDevice.SectorSize);

            var recoverConfig = replicationConfigDevice.GetFileSize(0) > 0;
            if (!recoverConfig)
            {
                InitializeReplicationHistory();
            }
            else
            {
                RecoverReplicationHistory();
            }

            // After initializing replication history propagate replicationId to ReplicationLogCheckpointManager
            SetPrimaryReplicationId();
        }

        /// <summary>
        /// Used to free up buffer pool
        /// </summary>
        public void Purge() => networkPool.Purge();

        public string GetBufferPoolStats() => networkPool.GetStats();

        void CheckpointVersionShift(bool isMainStore, long oldVersion, long newVersion)
        {
            if (clusterProvider.clusterManager.CurrentConfig.LocalNodeRole == NodeRole.REPLICA)
                return;
            storeWrapper.EnqueueCommit(isMainStore, newVersion);
        }

        /// <summary>
        /// Acquire recovery and checkpoint locks to prevent checkpoints and parallel recovery tasks
        /// </summary>
        public bool StartRecovery()
        {
            if (!clusterProvider.storeWrapper.TryPauseCheckpoints())
                return false;

            if (!recoverLock.TryWriteLock())
            {
                // If failed to acquire recoverLock re-enable checkpoint taking
                clusterProvider.storeWrapper.ResumeCheckpoints();
                return false;
            }

            return true;
        }

        /// <summary>
        /// Release recovery and checkpoint locks
        /// </summary>
        public void SuspendRecovery()
        {
            recoverLock.WriteUnlock();
            clusterProvider.storeWrapper.ResumeCheckpoints();
        }

        public void Dispose()
        {
            _disposed = true;

            replicationConfigDevice.Dispose();
            pool.Free();

            checkpointStore.WaitForReplicas();
            replicaSyncSessionTaskStore.Dispose();
            ctsRepManager.Cancel();
            ctsRepManager.Dispose();
            aofTaskStore.Dispose();
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
                    PrimaryRecover();
                    break;
                case NodeRole.REPLICA:
                    // We will instead recover as part of TryConnectToPrimary instead
                    // ReplicaRecover();
                    break;
                default:
                    logger?.LogError("Not valid role for node {nodeRole}", nodeRole);
                    throw new Exception($"Not valid role for node {nodeRole}");
            }
        }

        /// <summary>
        /// Primary recover
        /// </summary>
        private void PrimaryRecover()
        {
            storeWrapper.RecoverCheckpoint();
            storeWrapper.RecoverAOF();
            if (clusterProvider.serverOptions.EnableAOF)
            {
                // If recovered checkpoint corresponds to an unavailable AOF address, we initialize AOF to that address
                var recoveredSafeAofAddress = GetRecoveredSafeAofAddress();
                if (storeWrapper.appendOnlyFile.TailAddress < recoveredSafeAofAddress)
                    storeWrapper.appendOnlyFile.Initialize(recoveredSafeAofAddress, recoveredSafeAofAddress);
                logger?.LogInformation("Recovered AOF: begin address = {beginAddress}, tail address = {tailAddress}", storeWrapper.appendOnlyFile.BeginAddress, storeWrapper.appendOnlyFile.TailAddress);
                ReplicationOffset = storeWrapper.ReplayAOF();
            }

            // First recover and then load latest checkpoint info in-memory
            InitializeCheckpointStore();
        }

        /// <summary>
        /// Wait for local replication offset to sync with input value
        /// </summary>
        /// <param name="primaryReplicationOffset"></param>
        /// <returns></returns>
        public async Task<long> WaitForReplicationOffset(long primaryReplicationOffset)
        {
            while (ReplicationOffset < primaryReplicationOffset)
            {
                if (ctsRepManager.IsCancellationRequested) return -1;
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
            if (localNodeRole == NodeRole.REPLICA && replicaOfNodeId != null)
            {
                // At initialization of ReplicationManager, this node has been put into recovery mode
                if (!TryReplicateFromPrimary(out var errorMessage))
                    logger?.LogError($"An error occurred at {nameof(ReplicationManager)}.{nameof(Start)} {{error}}", Encoding.ASCII.GetString(errorMessage));
            }
            else if (localNodeRole == NodeRole.PRIMARY && replicaOfNodeId == null)
            {
                var replicaIds = current.GetLocalNodeReplicaIds();
                foreach (var replicaId in replicaIds)
                {
                    // TODO: Initiate AOF sync task correctly when restarting primary
                    if (clusterProvider.replicationManager.TryAddReplicationTask(replicaId, 0, out var aofSyncTaskInfo))
                    {
                        if (!TryConnectToReplica(replicaId, 0, aofSyncTaskInfo, out var errorMessage))
                            logger?.LogError("{errorMessage}", Encoding.ASCII.GetString(errorMessage));
                    }
                }
            }
            else
            {
                logger?.LogWarning("Replication manager starting configuration inconsistent role:{role} replicaOfId:{replicaOfNodeId}", replicaOfNodeId, localNodeRole);
            }
        }
    }
}