// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.networking;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;

    /// <summary>
    /// Cluster provider
    /// </summary>
    public interface IClusterProvider : IDisposable
    {
        /// <summary>
        /// Create cluster session
        /// </summary>
        IClusterSession CreateClusterSession(TransactionManager txnManager, IGarnetAuthenticator authenticator, User user, GarnetSessionMetrics garnetSessionMetrics, BasicGarnetApi basicGarnetApi, INetworkSender networkSender, ILogger logger = null);

        /// <summary>
        /// Flush config
        /// </summary>
        void FlushConfig();

        /// <summary>
        /// Get gossip info
        /// </summary>
        void GetGossipInfo(MetricsItem[] statsInfo, int startOffset, bool metricsDisabled);

        /// <summary>
        /// Get replication info
        /// </summary>
        MetricsItem[] GetReplicationInfo();

        /// <summary>
        /// Is replica
        /// </summary>
        /// <returns></returns>
        bool IsReplica();

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
        void SafeTruncateAOF(StoreType storeType, bool full, long CheckpointCoveredAofAddress, Guid storeCheckpointToken, Guid objectStoreCheckpointToken);

        /// <summary>
        /// Start cluster operations
        /// </summary>
        void Start();

        /// <summary>
        /// Update cluster auth (atomically)
        /// </summary>
        void UpdateClusterAuth(string clusterUsername, string clusterPassword);
    }
}