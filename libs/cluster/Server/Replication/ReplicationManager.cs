// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
        readonly TimeSpan clusterTimeout;

        readonly ILogger logger;
        bool _disposed;
        public bool recovering;
        private long replicationOffset;

        public long ReplicationOffset
        {
            get
            {
                // Primary tracks replicationOffset indirectly through AOF tailAddress
                // Replica will adjust replication offset as it receives data from primary (TODO: since AOFs are synced this might obsolete)
                var role = clusterProvider.clusterManager.CurrentConfig.GetLocalNodeRole();
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
            var objectStoreAofAddress = clusterProvider.serverOptions.DisableObjects ? clusterProvider.replicationManager.GetCkptManager(StoreType.Main).RecoveredSafeAofAddress : long.MaxValue;
            return Math.Min(storeAofAddress, objectStoreAofAddress);
        }

        public long GetCurrentSafeAofAddress()
        {
            var storeAofAddress = clusterProvider.replicationManager.GetCkptManager(StoreType.Main).CurrentSafeAofAddress;
            var objectStoreAofAddress = clusterProvider.serverOptions.DisableObjects ? clusterProvider.replicationManager.GetCkptManager(StoreType.Main).CurrentSafeAofAddress : long.MaxValue;
            return Math.Min(storeAofAddress, objectStoreAofAddress);
        }

        public ReplicationManager(ClusterProvider clusterProvider, GarnetServerOptions opts, ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.storeWrapper = clusterProvider.storeWrapper;
            aofProcessor = new AofProcessor(storeWrapper, recordToAof: false, logger: logger);
            replicaSyncSessionTaskStore = new ReplicaSyncSessionTaskStore(storeWrapper, clusterProvider, logger);

            ReplicationOffset = 0;

            // Set the appendOnlyFile field for all stores
            clusterProvider.GetReplicationLogCheckpointManager(StoreType.Main).checkpointVersionShift = CheckpointVersionShift;
            if (storeWrapper.objectStore != null)
                clusterProvider.GetReplicationLogCheckpointManager(StoreType.Object).checkpointVersionShift = CheckpointVersionShift;

            // If starts as replica, it cannot serve until it is connected to primary
            if (clusterProvider.clusterManager.CurrentConfig.GetLocalNodeRole() == NodeRole.REPLICA)
                recovering = true;

            clusterTimeout = opts.ClusterTimeout <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(opts.ClusterTimeout);
            this.logger = logger;

            checkpointStore = new CheckpointStore(storeWrapper, clusterProvider, true, logger);
            aofTaskStore = new(clusterProvider, 1, logger);

            var clusterFolder = "/cluster";
            var clusterDataPath = opts.CheckpointDir + clusterFolder;
            var deviceFactory = opts.GetInitializedDeviceFactory(clusterDataPath);
            replicationConfigDevice = deviceFactory.Get(new FileDescriptor(directoryName: "", fileName: "replication.conf"));
            pool = new(1, (int)replicationConfigDevice.SectorSize);

            bool recoverConfig = replicationConfigDevice.GetFileSize(0) > 0;
            if (!recoverConfig)
            {
                InitializeReplicationHistory();
            }
            else
            {
                RecoverReplicationHistory();
            }
        }

        void CheckpointVersionShift(bool isMainStore, long oldVersion, long newVersion)
        {
            if (clusterProvider.clusterManager.CurrentConfig.GetLocalNodeRole() == NodeRole.REPLICA)
                return;
            storeWrapper.EnqueueCommit(isMainStore, newVersion);
        }

        public void Dispose()
        {
            _disposed = true;

            replicationConfigDevice.Dispose();
            pool.Free();

            checkpointStore.WaitForReplicas();
            DisposeReplicaSyncSessionTasks();
            DisposeConnections();
            replicaSyncSessionTaskStore.Dispose();
            ctsRepManager.Dispose();
            aofProcessor?.Dispose();
        }

        public void DisposeConnections()
        {
            ctsRepManager.Cancel();
            aofTaskStore.Dispose();
        }

        /// <summary>
        /// Main recover method for replication
        /// </summary>
        public void Recover()
        {
            var nodeRole = clusterProvider.clusterManager.CurrentConfig.GetLocalNodeRole();

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
                    logger?.LogError($"Not valid role for node {nodeRole}");
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
                long recoveredSafeAofAddress = GetRecoveredSafeAofAddress();
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

            var localNodeRole = current.GetLocalNodeRole();
            var replicaOfNodeId = current.GetLocalNodePrimaryId();
            if (localNodeRole == NodeRole.REPLICA && replicaOfNodeId != null)
            {
                clusterProvider.replicationManager.recovering = true;
                clusterProvider.WaitForConfigTransition();
                var resp = TryReplicateFromPrimary();
                if (!resp.SequenceEqual(CmdStrings.RESP_OK))
                    logger?.LogError("An error occurred at ReplicationManager.Recover {error}", Encoding.ASCII.GetString(resp));
            }
            else if (localNodeRole == NodeRole.PRIMARY && replicaOfNodeId == null)
            {
                var replicaIds = current.GetLocalNodeReplicaIds();
                foreach (var replicaId in replicaIds)
                {
                    // TODO: Initiate AOF sync task correctly when restarting primary
                    if (clusterProvider.replicationManager.TryAddReplicationTask(replicaId, 0, out var aofSyncTaskInfo))
                    {
                        var resp = TryConnectToReplica(replicaId, 0, aofSyncTaskInfo);
                        if (!resp.SequenceEqual(CmdStrings.RESP_OK))
                            logger?.LogError(Encoding.ASCII.GetString(resp));
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