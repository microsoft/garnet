// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed class GarnetServerNode
    {
        readonly ClusterProvider clusterProvider;
        readonly GarnetClient gc;
        readonly GossipVersionNegotiation gossipNegotiation;

        long gossipSend;
        long gossipRecv;
        CancellationTokenSource cts = new();
        CancellationTokenSource internalCts = new();
        readonly object initializationLock = new();
        Task initializationTask;
        readonly ILogger logger = null;
        SingleWriterMultiReaderLock dispose;

        /// <summary>
        /// Last transmitted configuration
        /// </summary>
        ClusterConfig lastConfig = null;

        /// <summary>
        /// Outstanding gossip task if any
        /// </summary>
        Task gossipTask = null;

        /// <summary>
        /// Timestamp of last gossipSend for this connection
        /// </summary>
        public long GossipSend => gossipSend;

        /// <summary>
        /// GarnetClient connection
        /// </summary>
        public GarnetClient Client => gc;

        /// <summary>
        /// NodeId of remote node
        /// </summary>
        public string NodeId;

        /// <summary>
        /// EndPoint of remote node
        /// </summary>
        public EndPoint EndPoint;

        /// <summary>
        /// Default send page size for GarnetClient
        /// </summary>
        const int defaultSendPageSize = 1 << 17;

        /// <summary>
        /// Default max outstanding tasks for GarnetClient
        /// </summary>
        const int defaultMaxOutstandingTask = 8;

        internal static int GetClientTimeoutMilliseconds(int clusterTimeoutSeconds)
            => clusterTimeoutSeconds <= 0 ? 0 : (int)Math.Min((long)clusterTimeoutSeconds * 1000, int.MaxValue);

        /// <summary>
        /// GarnetServerNode constructor
        /// </summary>
        /// <param name="clusterProvider"></param>
        /// <param name="endpoint">The endpoint of the remote node</param>
        /// <param name="tlsOptions"></param>
        /// <param name="logger"></param>
        public GarnetServerNode(ClusterProvider clusterProvider, EndPoint endpoint, SslClientAuthenticationOptions tlsOptions, LightEpoch epoch, ILogger logger = null)
        {
            var opts = clusterProvider.storeWrapper.serverOptions;
            this.clusterProvider = clusterProvider;
            this.EndPoint = endpoint;
            this.gc = new GarnetClient(
                endpoint,
                tlsOptions,
                sendPageSize: opts.DisablePubSub ? defaultSendPageSize : Math.Max(defaultSendPageSize, (int)opts.PubSubPageSizeBytes()),
                maxOutstandingTasks: defaultMaxOutstandingTask,
                timeoutMilliseconds: GetClientTimeoutMilliseconds(
                    clusterProvider.storeWrapper.runtimeConfig.GetInt(ServerConfigType.CLUSTER_NODE_TIMEOUT)),
                authUsername: clusterProvider.clusterManager.clusterProvider.ClusterUsername,
                authPassword: clusterProvider.clusterManager.clusterProvider.ClusterPassword,
                epoch: epoch,
                clientName: $"Gossip-{clusterProvider.clusterManager.CurrentConfig.LocalNodeEndpoint}",
                logger: logger);
            gossipNegotiation = new(gc, clusterProvider.clusterManager);
            this.logger = logger;
            this.gossipRecv = 0;
            this.gossipSend = 0;
            ResetCts();
        }

        /// <summary>
        /// Initialize connection and cancellation tokens.
        /// Initialization is performed only once
        /// </summary>
        public ValueTask InitializeAsync()
        {
            // Ensure initialize executes only once
            lock (initializationLock)
            {
                initializationTask ??= InitializeConnectionAsync();
                return new(initializationTask);
            }
        }

        private async Task InitializeConnectionAsync()
        {
            cts = CancellationTokenSource.CreateLinkedTokenSource(clusterProvider.clusterManager.ctsGossip.Token, internalCts.Token);
            using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
            timeout.CancelAfter(clusterProvider.clusterManager.clusterTimeout);
            await gc.ReconnectAsync(timeout.Token).WaitAsync(timeout.Token).ConfigureAwait(false);
        }

        public void Dispose()
        {
            // Single write lock acquisition only
            if (!dispose.TryCloseLock())
            {
                logger?.LogTrace("GarnetServerNode.Dispose called multiple times");
                return;
            }

            try
            {
                cts?.Cancel();
                cts?.Dispose();
                internalCts?.Cancel();
                internalCts?.Dispose();
                gc?.Dispose();
            }
            catch { }
        }

        void UpdateGossipSend() => this.gossipSend = DateTimeOffset.UtcNow.Ticks;
        void UpdateGossipRecv() => this.gossipRecv = DateTimeOffset.UtcNow.Ticks;
        void ResetCts()
        {
            var internalCtsDisposed = false;
            internalCts.Cancel();
            if (!internalCts.TryReset())
            {
                internalCts.Dispose();
                internalCts = new();
                internalCtsDisposed = true;
            }

            if (internalCtsDisposed || !cts.TryReset())
            {
                cts.Cancel();
                cts.Dispose();
                cts = CancellationTokenSource.CreateLinkedTokenSource(clusterProvider.clusterManager.ctsGossip.Token, internalCts.Token);
            }
            gossipTask = null;
        }

        /// <summary>
        /// Keep track of updated config per connection. Useful when gossip sampling so as to ensure updates are propagated
        /// </summary>
        /// <returns></returns>
        ClusterConfig GetMostRecentConfig()
        {
            var conf = clusterProvider.clusterManager.CurrentConfig;
            if (conf != lastConfig)
            {
                lastConfig = conf;
                if (clusterProvider.replicationManager != null)
                    // NOTE: We update replication offset for sublog-0 because this info is used in CLUSTER NODES
                    // and we cannot have multiple replication offsets without changing the expected CLUSTER NODES response
                    lastConfig.LazyUpdateLocalReplicationOffset(clusterProvider.replicationManager.GetReplicationOffset(0));
            }
            return conf;
        }

        /// <summary>
        /// Send the latest configuration using the negotiated gossip version.
        /// </summary>
        /// <returns></returns>
        private async Task GossipAsync()
        {
            await InitializeAsync().ConfigureAwait(false);
            using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
            timeout.CancelAfter(gossipNegotiation.IsNegotiated ? clusterProvider.clusterManager.gossipDelay : clusterProvider.clusterManager.clusterTimeout);
            ClusterConfig other = await gossipNegotiation.ExchangeAsync(GetMostRecentConfig(), withMeet: false, NodeId, timeout.Token).ConfigureAwait(false);

            try
            {
                if (other != null)
                {
                    var current = clusterProvider.clusterManager.CurrentConfig;
                    // Check if gossip is from a node that is known and trusted before merging
                    if (current.IsKnown(other.LocalNodeId))
                        clusterProvider.clusterManager.TryMerge(other);
                    else
                        logger?.LogWarning("Received gossip from unknown node: {node-id}", other.LocalNodeId);
                }
            }
            catch (Exception ex)
            {
                logger?.LogCritical(ex, "GOSSIP faulted processing response");
            }
        }

        /// <summary>
        /// Issue gossip meet with meet to force receiving node to trust an untrusted node
        /// </summary>
        /// <param name="config"></param>
        /// <returns></returns>
        public async Task<ClusterConfig> TryMeetAsync(ClusterConfig config)
        {
            UpdateGossipSend();
            await InitializeAsync().ConfigureAwait(false);
            using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
            timeout.CancelAfter(clusterProvider.clusterManager.clusterTimeout);
            return await gossipNegotiation.ExchangeAsync(config, withMeet: true, NodeId, timeout.Token).ConfigureAwait(false);
        }

        /// <summary>
        /// Send gossip message or process response and send again.
        /// </summary>
        /// <returns></returns>
        public bool TryGossip()
        {
            var task = gossipTask;
            if (task == null)
            {
                // Issue first time gossip
                gossipTask = GossipAsync();
                UpdateGossipSend();
                return true;
            }
            else if (task.Status == TaskStatus.RanToCompletion)
            {
                UpdateGossipRecv();

                // Issue new gossip that can be either zero packet size or an updated configuration
                gossipTask = GossipAsync();
                UpdateGossipSend();
                return true;
            }
            if (!task.IsCompleted)
                return true;
            logger?.LogWarning(task.Exception, "GOSSIP round faulted");
            ResetCts();
            gossipTask = null;
            return false;
        }

        /// <summary>
        /// Get connection info
        /// </summary>
        /// <returns></returns>
        public ConnectionInfo GetConnectionInfo()
        {
            var nowTicks = DateTimeOffset.UtcNow.Ticks;
            var last_io_seconds = gossipRecv == 0 ? 0 : (int)TimeSpan.FromTicks(nowTicks - gossipSend).TotalSeconds;

            return new ConnectionInfo()
            {
                ping = gossipSend,
                pong = gossipRecv,
                connected = gc.IsConnected,
                lastIO = last_io_seconds,
            };
        }

        /// <summary>
        /// Send a CLUSTER PUBLISH message to another remote node
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="channel"></param>
        /// <param name="message"></param>
        public void TryClusterPublish(RespCommand cmd, Span<byte> channel, Span<byte> message)
        {
            var locked = false;
            try
            {
                // Try to acquire dispose lock to avoid a dispose during publish forwarding
                if (!dispose.TryReadLock())
                {
                    logger?.LogWarning("Could not acquire readLock for publish forwarding");
                    return;
                }

                locked = true;
                if (!gc.IsConnected)
                {
                    logger?.LogError($"{nameof(TryClusterPublish)}: client not connected; skipping publish forwarding");
                    return;
                }
                gc.ExecuteClusterPublishNoResponse(cmd, channel, message);
            }
            finally
            {
                if (locked) dispose.ReadUnlock();
            }
        }
    }
}