// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

global using BasicGarnetApi = Garnet.server.GarnetApi<
        Tsavorite.core.BasicContext<Garnet.server.StringInput, Tsavorite.core.SpanByteAndMemory, long, Garnet.server.MainSessionFunctions,
            /* MainStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>,
        Tsavorite.core.BasicContext<Garnet.server.ObjectInput, Garnet.server.ObjectOutput, long, Garnet.server.ObjectSessionFunctions,
            /* ObjectStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>,
        Tsavorite.core.BasicContext<Garnet.server.UnifiedInput, Garnet.server.UnifiedOutput, long, Garnet.server.UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>,
            Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByteComparer, Tsavorite.core.DefaultRecordDisposer>>>>;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Garnet.common;
using Garnet.networking;
using Garnet.server;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// Cluster provider
    /// </summary>
    public sealed partial class ClusterProvider : IClusterProvider
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

            if (serverOptions.GossipSamplePercent is > 100 or < 0)
            {
                throw new Exception("Gossip sample fraction should be in range [0,100]");
            }

            this.clusterManager = new ClusterManager(this, logger: loggerFactory?.CreateLogger("ClusterManager"));
            this.replicationManager = new ReplicationManager(this, logger: loggerFactory?.CreateLogger("ReplicationManager"));

            this.failoverManager = new FailoverManager(this, logger: loggerFactory?.CreateLogger("FailoverManager"));
            this.migrationManager = new MigrationManager(this, logger: loggerFactory?.CreateLogger("MigrationManager"));
        }

        /// <inheritdoc />
        public bool AllowDataLoss
            => serverOptions.AllowDataLoss;

        /// <inheritdoc />
        public void Recover()
        {
            replicationManager.Recover();
        }

        /// <inheritdoc />
        public bool PreventRoleChange()
            => replicationManager.BeginRecovery(RecoveryStatus.ReadRole, upgradeLock: false);

        /// <inheritdoc />
        public void AllowRoleChange()
            => replicationManager.EndRecovery(RecoveryStatus.NoRecovery, downgradeLock: false);

        /// <inheritdoc />
        public void Start()
        {
            clusterManager?.Start();
            replicationManager?.Start();
        }

        /// <inheritdoc />
        public IClusterSession CreateClusterSession(TransactionManager txnManager, IGarnetAuthenticator authenticator, UserHandle userHandle, GarnetSessionMetrics garnetSessionMetrics, BasicGarnetApi basicGarnetApi, INetworkSender networkSender, ILogger logger = null)
            => new ClusterSession(this, txnManager, authenticator, userHandle, garnetSessionMetrics, basicGarnetApi, networkSender, logger);

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

        public bool IsPrimary() => clusterManager?.CurrentConfig.LocalNodeRole == NodeRole.PRIMARY;

        /// <inheritdoc />
        public bool IsReplica()
            => clusterManager?.CurrentConfig.LocalNodeRole == NodeRole.REPLICA || replicationManager?.IsRecovering == true;

        /// <inheritdoc />
        public bool IsReplica(string nodeId)
        {
            var config = clusterManager?.CurrentConfig;
            if (config is null)
            {
                return false;
            }

            return config.GetNodeRoleFromNodeId(nodeId) == NodeRole.REPLICA;
        }

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
        }

        /// <inheritdoc />
        public void AddNewCheckpointEntry(bool full, AofAddress CheckpointCoveredAofAddress, Guid storeCheckpointToken, Guid objectStoreCheckpointToken)
        {
            var entry = new CheckpointEntry();

            entry.metadata.storeVersion = storeWrapper.store.CurrentVersion;
            entry.metadata.storeHlogToken = storeCheckpointToken;
            entry.metadata.storeIndexToken = storeCheckpointToken;
            entry.metadata.storeCheckpointCoveredAofAddress = CheckpointCoveredAofAddress;
            entry.metadata.storePrimaryReplId = replicationManager.PrimaryReplId;

            // Keep track of checkpoints for replica
            // Used to delete old checkpoints and cleanup and also cleanup during attachment to new primary
            replicationManager.AddCheckpointEntry(entry, full);

            // Truncate AOF
            SafeTruncateAOF(CheckpointCoveredAofAddress);
        }

        /// <inheritdoc />
        public void SafeTruncateAOF(AofAddress truncateUntil)
        {
            if (clusterManager.CurrentConfig.LocalNodeRole == NodeRole.PRIMARY)
                replicationManager.AofSyncDriverStore.SafeTruncateAof(truncateUntil);
            else
            {
                if (serverOptions.FastAofTruncate)
                    storeWrapper.appendOnlyFile?.Log.UnsafeShiftBeginAddress(truncateUntil, truncateLog: true);
                else
                {
                    storeWrapper.appendOnlyFile?.Log.TruncateUntil(truncateUntil);
                    if (!serverOptions.EnableFastCommit) storeWrapper.appendOnlyFile?.Log.Commit();
                }
            }
        }

        /// <inheritdoc />
        public void OnCheckpointInitiated(ref AofAddress CheckpointCoveredAofAddress)
        {
            Debug.Assert(serverOptions.EnableCluster);
            if (serverOptions.EnableAOF && clusterManager.CurrentConfig.LocalNodeRole == NodeRole.REPLICA)
            {
                // When the replica takes a checkpoint on encountering the checkpoint end marker, it needs to truncate the AOF only
                // until the checkpoint start marker. Otherwise, we will be left with an AOF that starts at the checkpoint end marker.
                // ReplicationCheckpointStartOffset is set by { ReplicaReplayTask.Consume -> AofProcessor.ProcessAofRecordInternal } when
                // it encounters the checkpoint start marker.

                CheckpointCoveredAofAddress = replicationManager.ReplicationCheckpointStartOffset;
            }
            else
                CheckpointCoveredAofAddress = storeWrapper.appendOnlyFile.Log.TailAddress;

            replicationManager?.UpdateCommitSafeAofAddress(ref CheckpointCoveredAofAddress);
        }

        /// <inheritdoc />
        public MetricsItem[] GetReplicationInfo()
        {
            var clusterEnabled = serverOptions.EnableCluster;
            var config = clusterEnabled ? clusterManager.CurrentConfig : null;
            var replicaInfo = clusterEnabled ? replicationManager.GetReplicaInfo() : null;
            var role = clusterEnabled ? config.LocalNodeRole : NodeRole.PRIMARY;
            var replication_offset = !clusterEnabled ? "N/A" : replicationManager.ReplicationOffset.ToString();
            var replication_offset2 = !clusterEnabled ? "N/A" : replicationManager.ReplicationOffset2.ToString();

            var replicationInfo = new List<MetricsItem>()
            {
                new("role", NodeRole.PRIMARY == role ? "master" : "slave"),
                new("connected_slaves", !clusterEnabled ? "0" : replicationManager.ConnectedReplicasCount.ToString()),
                new("master_failover_state", !clusterEnabled ? FailoverUtils.GetFailoverStatus(FailoverStatus.NO_FAILOVER) : failoverManager.GetFailoverStatus()),
                new("master_replid", clusterEnabled ? replicationManager.PrimaryReplId : Generator.DefaultHexId()),
                new("master_replid2", clusterEnabled ? replicationManager.PrimaryReplId2 : Generator.DefaultHexId()),
                new("master_repl_offset", replication_offset),
                new("second_repl_offset", replication_offset2),
                new("store_current_safe_aof_address", clusterEnabled ? replicationManager.StoreCurrentSafeAofAddress.ToString() : "N/A"),
                new("store_recovered_safe_aof_address", clusterEnabled ? replicationManager.StoreRecoveredSafeAofTailAddress.ToString() : "N/A"),
                new("recover_status", replicationManager.currentRecoveryStatus.ToString()),
                new("last_failover_state", !clusterEnabled ? FailoverUtils.GetFailoverStatus(FailoverStatus.NO_FAILOVER) : failoverManager.GetLastFailoverStatus()),
                new("sync_driver_count", !clusterEnabled ? "0" : replicationManager.AofSyncDriverStore.AofSyncDriverCount.ToString())
            };

            if (clusterEnabled)
            {
                if (role == NodeRole.REPLICA)
                {
                    var (address, port) = config.GetLocalNodePrimaryAddress();
                    var primaryLinkStatus = clusterManager.GetPrimaryLinkStatus(config);
                    var replicationOffsetLag = storeWrapper.appendOnlyFile.Log.TailAddress.AggregateDiff(replicationManager.ReplicationOffset);
                    replicationInfo.Add(new("master_host", address));
                    replicationInfo.Add(new("master_port", port.ToString()));
                    replicationInfo.Add(primaryLinkStatus[0]);
                    replicationInfo.Add(primaryLinkStatus[1]);
                    replicationInfo.Add(new("master_sync_in_progress", replicationManager.IsRecovering.ToString()));
                    replicationInfo.Add(new("slave_read_repl_offset", replication_offset));
                    replicationInfo.Add(new("slave_priority", "100"));
                    replicationInfo.Add(new("slave_read_only", "1"));
                    replicationInfo.Add(new("replica_announced", "1"));
                    replicationInfo.Add(new("master_sync_last_io_seconds_ago", replicationManager.LastPrimarySyncSeconds.ToString()));
                    replicationInfo.Add(new("replication_offset_lag", replicationOffsetLag.ToString()));
                    replicationInfo.Add(new("replication_offset_max_lag", storeWrapper.serverOptions.ReplicationOffsetMaxLag.ToString()));
                }
                else
                {
                    // replica0: ip=127.0.0.1,port=7001,state=online,offset=56,lag=0
                    for (var i = 0; i < replicaInfo.Count; i++)
                        replicationInfo.Add(new($"slave{i}", replicaInfo[i].ToString()));
                }
            }
            return [.. replicationInfo];
        }

        /// <inheritdoc />
        public MetricsItem[] GetCheckpointInfo()
            => [new("memory_checkpoint_entry", replicationManager.GetLatestCheckpointFromMemoryInfo()),
                new("disk_checkpoint_entry", replicationManager.GetLatestCheckpointFromDiskInfo())];

        /// <inheritdoc />
        public (AofAddress replication_offset, List<RoleInfo> replicaInfo) GetPrimaryInfo()
        {
            if (!serverOptions.EnableCluster)
            {
                return (replicationManager.ReplicationOffset, default);
            }
            return (replicationManager.ReplicationOffset, replicationManager.GetReplicaInfo());
        }

        /// <inheritdoc />
        public RoleInfo GetReplicaInfo()
        {
            if (!serverOptions.EnableCluster)
            {
                return new RoleInfo()
                {
                };
            }

            var config = clusterManager.CurrentConfig;
            clusterManager.GetConnectionInfo(config.LocalNodePrimaryId, out var connection);

            var (address, port) = config.GetLocalNodePrimaryAddress();
            var info = new RoleInfo()
            {
                address = address,
                port = port,
                replication_offset = replicationManager.ReplicationOffset,
                replication_state = replicationManager.IsRecovering ? "sync" :
                        connection.connected ? "connected" : "connect"
            };

            return info;
        }

        /// <inheritdoc />
        public MetricsItem[] GetGossipStats(bool metricsDisabled)
        {
            var gossipStats = clusterManager.gossipStats;
            return
                [
                    new("meet_requests_recv", metricsDisabled ? "0" : gossipStats.meet_requests_recv.ToString()),
                    new("meet_requests_succeed", metricsDisabled ? "0" : gossipStats.meet_requests_succeed.ToString()),
                    new("meet_requests_failed", metricsDisabled ? "0" : gossipStats.meet_requests_failed.ToString()),
                    new("gossip_success_count", metricsDisabled ? "0" : gossipStats.gossip_success_count.ToString()),
                    new("gossip_failed_count", metricsDisabled ? "0" : gossipStats.gossip_failed_count.ToString()),
                    new("gossip_timeout_count", metricsDisabled ? "0" : gossipStats.gossip_timeout_count.ToString()),
                    new("gossip_full_send", metricsDisabled ? "0" : gossipStats.gossip_full_send.ToString()),
                    new("gossip_empty_send", metricsDisabled ? "0" : gossipStats.gossip_empty_send.ToString()),
                    new("gossip_bytes_send", metricsDisabled ? "0" : gossipStats.gossip_bytes_send.ToString()),
                    new("gossip_bytes_recv", metricsDisabled ? "0" : gossipStats.gossip_bytes_recv.ToString()),
                    new("gossip_open_connections", metricsDisabled ? "0" : this.clusterManager.clusterConnectionStore.Count.ToString())
                ];
        }

        public MetricsItem[] GetBufferPoolStats()
            => [new("migration_manager", migrationManager.GetBufferPoolStats()), new("replication_manager", replicationManager.GetBufferPoolStats())];

        public void PurgeBufferPool(ManagerType managerType)
        {
            if (managerType == ManagerType.MigrationManager)
                migrationManager.Purge();
            else if (managerType == ManagerType.ReplicationManager)
                replicationManager.Purge();
            else
                throw new GarnetException();
        }

        public void ExtractKeySpecs(RespCommandsInfo commandInfo, RespCommand cmd, ref SessionParseState parseState, ref ClusterSlotVerificationInput csvi)
        {
            var specs = commandInfo.KeySpecifications;
            switch (specs.Length)
            {
                case 1:
                    var searchIndex = (BeginSearchIndex)specs[0].BeginSearch;
                    csvi.readOnly = specs[0].Flags.HasFlag(KeySpecificationFlags.RO);
                    switch (specs[0].FindKeys)
                    {
                        case FindKeysRange:
                            var findRange = (FindKeysRange)specs[0].FindKeys;
                            csvi.firstKey = searchIndex.Index - 1;
                            csvi.lastKey = findRange.LastKey < 0 ? findRange.LastKey + parseState.Count + 1 : findRange.LastKey - searchIndex.Index + 1;
                            csvi.step = findRange.KeyStep;
                            csvi.readOnly = !specs[0].Flags.HasFlag(KeySpecificationFlags.RW);
                            break;
                        case FindKeysKeyNum:
                            var findKeysKeyNum = (FindKeysKeyNum)specs[0].FindKeys;
                            csvi.firstKey = searchIndex.Index + findKeysKeyNum.FirstKey - 1;
                            csvi.lastKey = csvi.firstKey + parseState.GetInt(searchIndex.Index + findKeysKeyNum.KeyNumIdx - 1);
                            csvi.step = findKeysKeyNum.KeyStep;
                            break;
                        case FindKeysUnknown:
                        default:
                            throw new GarnetException("FindKeys spec not known");
                    }

                    break;
                case 2:
                    searchIndex = (BeginSearchIndex)specs[0].BeginSearch;
                    switch (specs[0].FindKeys)
                    {
                        case FindKeysRange:
                            csvi.firstKey = RespCommand.BITOP == cmd ? searchIndex.Index - 2 : searchIndex.Index - 1;
                            break;
                        case FindKeysKeyNum:
                        case FindKeysUnknown:
                        default:
                            throw new GarnetException("FindKeys spec not known");
                    }

                    var searchIndex1 = (BeginSearchIndex)specs[1].BeginSearch;
                    switch (specs[1].FindKeys)
                    {
                        case FindKeysRange:
                            var findRange = (FindKeysRange)specs[1].FindKeys;
                            csvi.lastKey = findRange.LastKey < 0 ? findRange.LastKey + parseState.Count + 1 : findRange.LastKey + searchIndex1.Index - searchIndex.Index + 1;
                            csvi.step = findRange.KeyStep;
                            break;
                        case FindKeysKeyNum:
                            var findKeysKeyNum = (FindKeysKeyNum)specs[1].FindKeys;
                            csvi.keyNumOffset = searchIndex1.Index + findKeysKeyNum.KeyNumIdx - 1;
                            csvi.lastKey = searchIndex1.Index + parseState.GetInt(csvi.keyNumOffset);
                            csvi.step = findKeysKeyNum.KeyStep;
                            break;
                        case FindKeysUnknown:
                        default:
                            throw new GarnetException("FindKeys spec not known");
                    }

                    break;
                default:
                    throw new GarnetException("KeySpecification not supported count");
            }
        }

        public void ClusterPublish(RespCommand cmd, ref Span<byte> channel, ref Span<byte> message)
            => clusterManager.TryClusterPublish(cmd, ref channel, ref message);

        internal GarnetClusterCheckpointManager ReplicationLogCheckpointManager
        {
            get
            {
                Debug.Assert(serverOptions.EnableCluster);
                return (GarnetClusterCheckpointManager)storeWrapper.store.CheckpointManager;
            }
        }

        /// <summary>
        /// Bump Garnet epoch
        /// </summary>
        internal void BumpCurrentEpoch() => Interlocked.Increment(ref GarnetCurrentEpoch);

        /// <summary>
        /// Wait for config transition
        /// </summary>
        /// <returns></returns>
        internal bool BumpAndWaitForEpochTransition()
        {
            BumpCurrentEpoch();
            // Acquire latest bumped epoch
            var currentEpoch = GarnetCurrentEpoch;
            foreach (var server in storeWrapper.Servers)
            {
                while (true)
                {
                retry:
                    Thread.Yield();
                    var sessions = ((GarnetServerTcp)server).ActiveClusterSessions();
                    foreach (var s in sessions)
                    {
                        var entryEpoch = s.LocalCurrentEpoch;
                        // Retry if at least one session has not yet caught up to the current epoch.
                        if (entryEpoch != 0 && entryEpoch < currentEpoch)
                            goto retry;
                    }
                    break;
                }
            }
            return true;
        }

        /// <inheritdoc />
        public string GetRunId() => replicationManager.PrimaryReplId;
    }
}