// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Garnet.networking;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Cluster provider
    /// </summary>
    public interface IClusterProvider : IDisposable
    {
        /// <summary>
        /// Create cluster session
        /// </summary>
        IClusterSession CreateClusterSession(TransactionManager txnManager, IGarnetAuthenticator authenticator, UserHandle userHandle, GarnetSessionMetrics garnetSessionMetrics, BasicGarnetApi basicGarnetApi, INetworkSender networkSender, ILogger logger = null);


        /// <summary>
        /// Are we allowed to incur AOF data loss: { using null AOF device } OR { main memory replication AND no on-demand checkpoints }
        /// </summary>
        bool AllowDataLoss { get; }

        /// <summary>
        /// Flush config
        /// </summary>
        void FlushConfig();

        /// <summary>
        /// Get gossip stats
        /// </summary>
        MetricsItem[] GetGossipStats(bool metricsDisabled);

        /// <summary>
        /// Get replication info
        /// </summary>
        MetricsItem[] GetReplicationInfo();

        /// <summary>
        /// Get buffer poolt stats
        /// </summary>
        /// <returns></returns>
        MetricsItem[] GetBufferPoolStats();

        /// <summary>
        /// Get info on primary from replica perspective.
        /// </summary>
        /// <returns></returns>
        (long replication_offset, List<RoleInfo> replicaInfo) GetPrimaryInfo();

        /// <summary>
        /// Get info on replicas from primary perspective.
        /// </summary>
        /// <returns></returns>
        RoleInfo GetReplicaInfo();

        /// <summary>
        /// Purger buffer pool for provided manager
        /// </summary>
        /// <param name="managerType"></param>
        void PurgeBufferPool(ManagerType managerType);

        /// <summary>
        /// Extract key specs
        /// </summary>
        /// <param name="commandInfo"></param>
        /// <param name="cmd"></param>
        /// <param name="csvi"></param>
        void ExtractKeySpecs(RespCommandsInfo commandInfo, RespCommand cmd, ref SessionParseState parseState, ref ClusterSlotVerificationInput csvi);

        /// <summary>
        /// Issue a cluster publish message to remote nodes
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="channel"></param>
        /// <param name="message"></param>
        void ClusterPublish(RespCommand cmd, ref Span<byte> channel, ref Span<byte> message);

        /// <summary>
        /// Is Primary
        /// </summary>
        /// <returns></returns>
        bool IsPrimary();

        /// <summary>
        /// Is replica
        /// </summary>
        /// <returns></returns>
        bool IsReplica();

        /// <summary>
        /// Returns true if the given nodeId is a replica, according to the current cluster configuration.
        /// </summary>
        bool IsReplica(string nodeId);

        /// <summary>
        /// On checkpoint initiated
        /// </summary>
        /// <param name="CheckpointCoveredAofAddress"></param>
        void OnCheckpointInitiated(out long CheckpointCoveredAofAddress);

        /// <summary>
        /// Recover the cluster
        /// </summary>
        void Recover();

        /// <summary>
        /// Reset gossip stats
        /// </summary>
        void ResetGossipStats();

        /// <summary>
        /// Safe truncate AOF
        /// </summary>
        void SafeTruncateAOF(bool full, long CheckpointCoveredAofAddress, Guid storeCheckpointToken, Guid objectStoreCheckpointToken);

        /// <summary>
        /// Safe truncate AOF until address
        /// </summary>
        /// <param name="truncateUntil"></param>
        void SafeTruncateAOF(long truncateUntil);

        /// <summary>
        /// Start cluster operations
        /// </summary>
        void Start();

        /// <summary>
        /// Update cluster auth (atomically)
        /// </summary>
        void UpdateClusterAuth(string clusterUsername, string clusterPassword);

        /// <summary>
        /// Get checkpoint info
        /// </summary>
        MetricsItem[] GetCheckpointInfo();

        /// <summary>
        /// RunID to identify checkpoint history
        /// </summary>
        /// <returns></returns>
        string GetRunId();

        /// <summary>
        /// Call to prevent this node from changing its current role.
        /// 
        /// If this returns true, must be followed by a single call to 
        /// <see cref="AllowRoleChange"/>
        /// </summary>
        bool PreventRoleChange();

        /// <summary>
        /// After a successful call to <see cref="PreventRoleChange"/>, 
        /// allows the node to change roles again.
        /// </summary>
        void AllowRoleChange();
    }
}