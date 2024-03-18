// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Garnet.common;
using Garnet.networking;
using Garnet.server;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;

    /// <summary>
    /// Cluster provider
    /// </summary>
    public class ClusterProvider : IClusterProvider
    {
        internal readonly ClusterManager clusterManager;
        internal readonly ReplicationManager replicationManager;
        internal readonly FailoverManager failoverManager;
        internal readonly MigrationManager migrationManager;
        internal readonly ILoggerFactory loggerFactory;
        internal readonly StoreWrapper storeWrapper;
        internal readonly GarnetServerOptions serverOptions;
        internal long GarnetCurrentEpoch = 1;
        ClusterAuthContainer authContainer;


        /// <summary>
        /// Get cluster username
        /// </summary>
        public string ClusterUsername => authContainer.ClusterUsername;

        /// <summary>
        /// Get cluster password
        /// </summary>
        public string ClusterPassword => authContainer.ClusterPassword;

        /// <summary>
        /// Create new cluster provider
        /// </summary>
        public ClusterProvider(StoreWrapper storeWrapper)
        {
            this.storeWrapper = storeWrapper;
            this.serverOptions = storeWrapper.serverOptions;
            this.loggerFactory = storeWrapper.loggerFactory;

            authContainer = new ClusterAuthContainer
            {
                ClusterUsername = serverOptions.ClusterUsername,
                ClusterPassword = serverOptions.ClusterPassword
            };

            if (serverOptions.GossipSamplePercent > 100 || serverOptions.GossipSamplePercent < 0)
            {
                throw new Exception("Gossip sample fraction should be in range [0,100]");
            }

            this.clusterManager = NewClusterManagerInstance(serverOptions, loggerFactory);
            this.replicationManager = NewReplicationManagerInstance(serverOptions, this, loggerFactory);
            // Now set replication manager field in cluster manager, to break circular dependency
            if (clusterManager != null) clusterManager.replicationManager = replicationManager;

            this.failoverManager = NewFailoverManagerInstance(serverOptions, this, loggerFactory);
            this.migrationManager = NewMigrationManagerInstance(this, loggerFactory);
        }

        /// <inheritdoc />
        public void Recover()
        {
            replicationManager.Recover();
        }

        /// <inheritdoc />
        public void Start()
        {
            clusterManager?.Start();
            replicationManager?.Start();
        }

        /// <inheritdoc />
        public IClusterSession CreateClusterSession(TransactionManager txnManager, IGarnetAuthenticator authenticator, User user, GarnetSessionMetrics garnetSessionMetrics, BasicGarnetApi basicGarnetApi, INetworkSender networkSender, ILogger logger = null)
            => new ClusterSession(this, txnManager, authenticator, user, garnetSessionMetrics, basicGarnetApi, networkSender, logger);

        /// <inheritdoc />
        public void UpdateClusterAuth(string clusterUsername, string clusterPassword)
        {
            ClusterAuthContainer oldAuthContainer, newAuthContainer;
            do
            {
                oldAuthContainer = authContainer;
                newAuthContainer = new ClusterAuthContainer
                {
                    ClusterUsername = clusterUsername ?? oldAuthContainer.ClusterUsername, // If null username, we will reuse the old username
                    ClusterPassword = clusterPassword
                };
            } while (Interlocked.CompareExchange(ref authContainer, newAuthContainer, oldAuthContainer) != oldAuthContainer);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            clusterManager?.Dispose();
            replicationManager?.Dispose();
            failoverManager?.Dispose();
            migrationManager?.Dispose();
        }

        /// <inheritdoc />
        public bool IsReplica()
            => clusterManager?.CurrentConfig.GetLocalNodeRole() == NodeRole.REPLICA || replicationManager?.recovering == true;

        /// <inheritdoc />
        public void ResetGossipStats()
            => clusterManager?.gossipStats.Reset();

        /// <inheritdoc />
        public void FlushConfig()
            => clusterManager?.FlushConfig();

        /// <inheritdoc />
        public void FlushDB(bool unsafeTruncateLog = false)
        {
            storeWrapper.store.Log.ShiftBeginAddress(storeWrapper.store.Log.TailAddress, truncateLog: unsafeTruncateLog);
            storeWrapper.objectStore?.Log.ShiftBeginAddress(storeWrapper.objectStore.Log.TailAddress, truncateLog: unsafeTruncateLog);
        }

        /// <inheritdoc />
        public void SafeTruncateAOF(StoreType storeType, bool full, long CheckpointCoveredAofAddress, Guid storeCheckpointToken, Guid objectStoreCheckpointToken)
        {
            CheckpointEntry entry = new CheckpointEntry();

            if (storeType == StoreType.Main || storeType == StoreType.All)
            {
                entry.storeVersion = storeWrapper.store.CurrentVersion;
                entry.storeHlogToken = storeCheckpointToken;
                entry.storeIndexToken = storeCheckpointToken;
                entry.storeCheckpointCoveredAofAddress = CheckpointCoveredAofAddress;
                entry.storePrimaryReplId = replicationManager.PrimaryReplId;
            }

            if (storeType == StoreType.Object || storeType == StoreType.All)
            {
                entry.objectStoreVersion = serverOptions.DisableObjects ? -1 : storeWrapper.objectStore.CurrentVersion;
                entry.objectStoreHlogToken = serverOptions.DisableObjects ? default : objectStoreCheckpointToken;
                entry.objectStoreIndexToken = serverOptions.DisableObjects ? default : objectStoreCheckpointToken;
                entry.objectCheckpointCoveredAofAddress = CheckpointCoveredAofAddress;
                entry.objectStorePrimaryReplId = replicationManager.PrimaryReplId;
            }

            // Keep track of checkpoints for replica
            // Used to delete old checkpoints and cleanup and also cleanup during attachment to new primary
            replicationManager.AddCheckpointEntry(entry, storeType, full);

            if (clusterManager.CurrentConfig.GetLocalNodeRole() == NodeRole.PRIMARY)
                replicationManager.SafeTruncateAof(CheckpointCoveredAofAddress);
            else
            {
                if (serverOptions.MainMemoryReplication)
                    storeWrapper.appendOnlyFile?.UnsafeShiftBeginAddress(CheckpointCoveredAofAddress, truncateLog: true, noFlush: true);
                else
                {
                    storeWrapper.appendOnlyFile?.TruncateUntil(CheckpointCoveredAofAddress);
                    if (!serverOptions.EnableFastCommit) storeWrapper.appendOnlyFile?.Commit();
                }
            }
        }

        /// <inheritdoc />
        public void OnCheckpointInitiated(out long CheckpointCoveredAofAddress)
        {
            Debug.Assert(serverOptions.EnableCluster);
            if (serverOptions.EnableAOF && clusterManager.CurrentConfig.GetLocalNodeRole() == NodeRole.REPLICA)
                CheckpointCoveredAofAddress = replicationManager.ReplicationOffset;
            else
                CheckpointCoveredAofAddress = storeWrapper.appendOnlyFile.TailAddress;

            replicationManager?.UpdateCommitSafeAofAddress(CheckpointCoveredAofAddress);
            replicationManager?.SetPrimaryReplicationId();
        }

        /// <inheritdoc />
        public MetricsItem[] GetReplicationInfo()
        {
            bool clusterEnabled = serverOptions.EnableCluster;
            ClusterConfig config = clusterEnabled ? clusterManager.CurrentConfig : null;
            var replicaInfo = clusterEnabled ? replicationManager.GetReplicaInfo() : null;
            int replicaCount = clusterEnabled ? replicaInfo.Count : 0;
            var role = clusterEnabled ? config.GetLocalNodeRole() : NodeRole.PRIMARY;
            int commonInfoCount = 11;
            int replicaInfoCount = 9;
            int replicationInfoCount = commonInfoCount + replicaCount;
            replicationInfoCount += role == NodeRole.REPLICA ? replicaInfoCount : 0;

            var replicationInfo = new MetricsItem[replicationInfoCount];
            replicationInfo[0] = (new("role", NodeRole.PRIMARY == role ? "master" : "slave"));
            replicationInfo[1] = (new("connected_slaves", !clusterEnabled ? "0" : replicationManager.ConnectedReplicasCount.ToString()));
            replicationInfo[2] = (new("master_failover_state", !clusterEnabled ? FailoverUtils.GetFailoverStatus(FailoverStatus.NO_FAILOVER) : failoverManager.GetFailoverStatus()));

            var replication_offset = !clusterEnabled ? "N/A" : replicationManager.ReplicationOffset.ToString();
            replicationInfo[3] = (new("master_replid", clusterEnabled ? replicationManager.PrimaryReplId : Generator.DefaultHexId()));
            replicationInfo[4] = (new("master_replid2", clusterEnabled ? replicationManager.PrimaryReplId2 : Generator.DefaultHexId()));
            replicationInfo[5] = (new("master_repl_offset", replication_offset));
            replicationInfo[6] = (new("second_repl_offset", replication_offset));
            replicationInfo[7] = (new("store_current_safe_aof_address", clusterEnabled ? replicationManager.StoreCurrentSafeAofAddress.ToString() : "N/A"));
            replicationInfo[8] = (new("store_recovered_safe_aof_address", clusterEnabled ? replicationManager.StoreRecoveredSafeAofTailAddress.ToString() : "N/A"));
            replicationInfo[9] = (new("object_store_current_safe_aof_address", clusterEnabled && !serverOptions.DisableObjects ? replicationManager.ObjectStoreCurrentSafeAofAddress.ToString() : "N/A"));
            replicationInfo[10] = (new("object_store_recovered_safe_aof_address", clusterEnabled && !serverOptions.DisableObjects ? replicationManager.ObjectStoreRecoveredSafeAofTailAddress.ToString() : "N/A"));

            if (clusterEnabled)
            {
                if (role == NodeRole.REPLICA)
                {
                    var (address, port) = config.GetLocalNodePrimaryAddress();
                    var primaryLinkStatus = clusterManager.GetPrimaryLinkStatus(config);
                    replicationInfo[commonInfoCount + 0] = new("master_host", address);
                    replicationInfo[commonInfoCount + 1] = new("master_port", port.ToString());
                    replicationInfo[commonInfoCount + 2] = primaryLinkStatus[0];
                    replicationInfo[commonInfoCount + 3] = primaryLinkStatus[1];
                    replicationInfo[commonInfoCount + 4] = new("master_sync_in_progress", replicationManager.recovering.ToString());
                    replicationInfo[commonInfoCount + 5] = new("slave_read_repl_offset", replication_offset);
                    replicationInfo[commonInfoCount + 6] = new("slave_priority", "100");
                    replicationInfo[commonInfoCount + 7] = new("slave_read_only", "1");
                    replicationInfo[commonInfoCount + 8] = new("replica_announced", "1");
                }
                else
                {
                    //replica0: ip=127.0.0.1,port=7001,state=online,offset=56,lag=0
                    int i = commonInfoCount;
                    foreach (var ri in replicaInfo)
                        replicationInfo[i++] = new(ri.Item1, ri.Item2);
                }
            }
            return replicationInfo;
        }

        /// <inheritdoc />
        public void GetGossipInfo(MetricsItem[] statsInfo, int startOffset, bool metricsDisabled)
        {
            var gossipStats = clusterManager.gossipStats;
            statsInfo[startOffset] = (new("meet_requests_recv", metricsDisabled ? "0" : gossipStats.meet_requests_recv.ToString()));
            statsInfo[startOffset + 1] = (new("meet_requests_succeed", metricsDisabled ? "0" : gossipStats.meet_requests_succeed.ToString()));
            statsInfo[startOffset + 2] = (new("meet_requests_failed", metricsDisabled ? "0" : gossipStats.meet_requests_failed.ToString()));
            statsInfo[startOffset + 3] = (new("gossip_success_count", metricsDisabled ? "0" : gossipStats.gossip_success_count.ToString()));
            statsInfo[startOffset + 4] = (new("gossip_failed_count", metricsDisabled ? "0" : gossipStats.gossip_failed_count.ToString()));
            statsInfo[startOffset + 5] = (new("gossip_timeout_count", metricsDisabled ? "0" : gossipStats.gossip_timeout_count.ToString()));
            statsInfo[startOffset + 6] = (new("gossip_full_send", metricsDisabled ? "0" : gossipStats.gossip_full_send.ToString()));
            statsInfo[startOffset + 7] = (new("gossip_empty_send", metricsDisabled ? "0" : gossipStats.gossip_empty_send.ToString()));
            statsInfo[startOffset + 8] = (new("gossip_bytes_send", metricsDisabled ? "0" : gossipStats.gossip_bytes_send.ToString()));
            statsInfo[startOffset + 9] = (new("gossip_bytes_recv", metricsDisabled ? "0" : gossipStats.gossip_bytes_recv.ToString()));
        }

        /// <inheritdoc />
        public DeviceLogCommitCheckpointManager CreateCheckpointManager(INamedDeviceFactory deviceFactory, ICheckpointNamingScheme checkpointNamingScheme, bool isMainStore)
            => new(deviceFactory, checkpointNamingScheme, isMainStore);

        internal ReplicationLogCheckpointManager GetReplicationLogCheckpointManager(StoreType storeType)
        {
            Debug.Assert(serverOptions.EnableCluster);
            return storeType switch
            {
                StoreType.Main => (ReplicationLogCheckpointManager)storeWrapper.store.CheckpointManager,
                StoreType.Object => (ReplicationLogCheckpointManager)storeWrapper.objectStore?.CheckpointManager,
                _ => throw new Exception($"GetCkptManager: unexpected state {storeType}")
            };
        }

        /// <summary>
        /// Bump Garnet epoch
        /// </summary>
        internal void BumpCurrentEpoch() => Interlocked.Increment(ref GarnetCurrentEpoch);

        /// <summary>
        /// Wait for config transition
        /// </summary>
        /// <returns></returns>
        internal bool WaitForConfigTransition()
        {
            var server = storeWrapper.GetServer();
            BumpCurrentEpoch();
            while (true)
            {
            retry:
                var currentEpoch = GarnetCurrentEpoch;
                Thread.Yield();
                var sessions = server.ActiveClusterSessions();
                foreach (var s in sessions)
                {
                    var entryEpoch = s.LocalCurrentEpoch;
                    if (entryEpoch != 0 && entryEpoch >= currentEpoch)
                        goto retry;
                }
                break;
            }
            return true;
        }

        ClusterManager NewClusterManagerInstance(GarnetServerOptions serverOptions, ILoggerFactory loggerFactory)
        {
            if (!serverOptions.EnableCluster)
                return null;
            return new ClusterManager(this, loggerFactory);
        }

        ReplicationManager NewReplicationManagerInstance(GarnetServerOptions serverOptions, ClusterProvider clusterProvider, ILoggerFactory loggerFactory)
        {
            if (!serverOptions.EnableCluster)
                return null;
            return new ReplicationManager(clusterProvider, opts: serverOptions, logger: loggerFactory?.CreateLogger("StoreWrapper"));
        }

        FailoverManager NewFailoverManagerInstance(GarnetServerOptions serverOptions, ClusterProvider clusterProvider, ILoggerFactory loggerFactory)
        {
            if (!serverOptions.EnableCluster)
                return null;
            var clusterTimeout = serverOptions.ClusterTimeout <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(serverOptions.ClusterTimeout);
            return new FailoverManager(clusterProvider, serverOptions, clusterTimeout, loggerFactory);
        }

        MigrationManager NewMigrationManagerInstance(ClusterProvider clusterProvider, ILoggerFactory loggerFactory)
        {
            if (!serverOptions.EnableCluster)
                return null;
            return new MigrationManager(clusterProvider, logger: loggerFactory?.CreateLogger("MigrationManager"));
        }
    }
}