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
        readonly SslClientAuthenticationOptions tlsOptions;
        readonly LightEpoch epoch;
        GarnetClient gc;
        ClusterAuthContainer clientAuth;
        readonly ExponentialBackoff backoff;
        readonly object initializationSync = new();

        long gossipSend;
        long gossipRecv;
        CancellationTokenSource cts = new();
        CancellationTokenSource internalCts = new();
        volatile bool initialized;
        Task<bool> initializationTask;
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
        public GarnetClient Client => Volatile.Read(ref gc);

        /// <summary>
        /// Whether the client connection has been initialized successfully.
        /// </summary>
        public bool IsInitialized => initialized;

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
        const int defaultSendPageSize = 1 << 13;

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
        /// <param name="epoch"></param>
        /// <param name="logger"></param>
        public GarnetServerNode(ClusterProvider clusterProvider, EndPoint endpoint, SslClientAuthenticationOptions tlsOptions, LightEpoch epoch, ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.tlsOptions = tlsOptions;
            this.epoch = epoch;
            this.EndPoint = endpoint;
            this.logger = logger;
            this.clientAuth = clusterProvider.ClusterAuth;
            this.gc = CreateGarnetClient(clientAuth);
            this.backoff = new ExponentialBackoff();
            initialized = false;
            this.gossipRecv = 0;
            this.gossipSend = 0;
            ResetCts();
        }

        GarnetClient CreateGarnetClient(ClusterAuthContainer auth)
            => new(
                EndPoint,
                tlsOptions,
                sendPageSize: defaultSendPageSize,
                bufferSize: defaultSendPageSize,
                maxOutstandingTasks: defaultMaxOutstandingTask,
                timeoutMilliseconds: GetClientTimeoutMilliseconds(
                    clusterProvider.storeWrapper.runtimeConfig.GetInt(ServerConfigType.CLUSTER_NODE_TIMEOUT)),
                authUsername: auth.ClusterUsername,
                authPassword: auth.ClusterPassword,
                epoch: epoch,
                clientName: $"Gossip-{clusterProvider.clusterManager.CurrentConfig.LocalNodeEndpoint}",
                logger: logger,
                useOutOfLineExecution: true);

        /// <summary>
        /// Attempts to initialize the connection when its reconnect backoff permits.
        /// </summary>
        /// <returns>True when the connection is initialized; otherwise false.</returns>
        public ValueTask<bool> TryInitializeAsync()
        {
            lock (initializationSync)
            {
                if (initialized)
                    return new(true);

                if (initializationTask is { IsCompleted: false })
                    return new(initializationTask);

                if (!backoff.CanAttempt())
                    return new(false);

                RefreshClientAuthentication();
                initializationTask = InitializeCoreAsync(gc);
                return new(initializationTask);
            }

            async Task<bool> InitializeCoreAsync(GarnetClient client)
            {
                try
                {
                    cts = CancellationTokenSource.CreateLinkedTokenSource(clusterProvider.clusterManager.ctsGossip.Token, internalCts.Token);
                    await client.ReconnectAsync().WaitAsync(clusterProvider.clusterManager.gossipDelay, cts.Token).ConfigureAwait(false);
                    backoff.Reset();
                    initialized = true;
                    return true;
                }
                catch (Exception ex)
                {
                    initialized = false;
                    ResetCts();
                    var retryDelay = backoff.RecordFailure();
                    logger?.LogWarning(ex, "Could not establish connection to remote node [{nodeId} {endpoint}]; retrying in {retryDelay}",
                        NodeId, EndPoint, retryDelay);
                    return false;
                }
            }

            void RefreshClientAuthentication()
            {
                var currentAuth = clusterProvider.ClusterAuth;
                if (ReferenceEquals(currentAuth, clientAuth))
                    return;

                var oldClient = gc;
                var newClient = CreateGarnetClient(currentAuth);
                clientAuth = currentAuth;
                Volatile.Write(ref gc, newClient);
                oldClient.Dispose();
            }
        }

        /// <summary>
        /// Records a connection failure and returns the delay before reconnection may be attempted.
        /// </summary>
        public TimeSpan RecordConnectionFailure()
        {
            lock (initializationSync)
            {
                initialized = false;
                return backoff.RecordFailure();
            }
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
        byte[] GetMostRecentConfig()
        {
            var conf = clusterProvider.clusterManager.CurrentConfig;
            byte[] byteArray;
            if (conf != lastConfig)
            {
                lastConfig = conf;
                if (clusterProvider.replicationManager != null)
                    // NOTE: We update replication offset for sublog-0 because this info is used in CLUSTER NODES
                    // and we cannot have multiple replication offsets without changing the expected CLUSTER NODES response
                    lastConfig.LazyUpdateLocalReplicationOffset(clusterProvider.replicationManager.GetReplicationOffset(0));
                byteArray = lastConfig.ToByteArray();
            }
            else
            {
                byteArray = [];
            }
            return byteArray;
        }

        /// <summary>
        /// Schedule a Gossip task for provided serialized configuration
        /// </summary>
        /// <param name="configByteArray"></param>
        /// <returns></returns>
        private async Task GossipAsync(byte[] configByteArray)
        {
            try
            {
                using var resp = await gc.GossipAsync(configByteArray, internalCts.Token).WaitAsync(clusterProvider.clusterManager.gossipDelay, cts.Token).ConfigureAwait(false);
                if (resp.Length > 0)
                {
                    clusterProvider.clusterManager.gossipStats.UpdateGossipBytesRecv(resp.Length);
                    var returnedConfigArray = resp.Span.ToArray();

                    // Validate config version before full deserialization
                    if (!ClusterConfig.TryPeekVersion(returnedConfigArray, out var version) || version != ClusterConfig.ClusterConfigVersion)
                    {
                        logger?.LogWarning("Received gossip response with incompatible config version: {version}", version);
                        return;
                    }

                    var other = ClusterConfig.FromByteArray(returnedConfigArray);
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
                throw;
            }
        }

        /// <summary>
        /// Issue gossip meet with meet to force receiving node to trust an untrusted node
        /// </summary>
        /// <param name="configByteArray"></param>
        /// <returns></returns>
        public Task<MemoryResult<byte>> TryMeetAsync(byte[] configByteArray)
        {
            UpdateGossipSend();
            return gc.GossipWithMeetAsync(configByteArray, internalCts.Token).WaitAsync(clusterProvider.clusterManager.clusterTimeout, cts.Token);
        }

        /// <summary>
        /// Send gossip message or process response and send again.
        /// </summary>
        /// <returns></returns>
        public bool TryGossip()
        {
            var task = gossipTask;
            // If first time we are sending gossip make sure to send latest version
            if (task == null)
            {
                // Issue first time gossip
                var configArray = clusterProvider.clusterManager.CurrentConfig.ToByteArray();
                gossipTask = GossipAsync(configArray);
                UpdateGossipSend();
                clusterProvider.clusterManager.gossipStats.gossip_full_send++;
                // Track bytes send
                clusterProvider.clusterManager.gossipStats.UpdateGossipBytesSend(configArray.Length);
                return true;
            }
            else if (task.Status == TaskStatus.RanToCompletion)
            {
                var configByteArray = GetMostRecentConfig();
                UpdateGossipRecv();

                // Issue new gossip that can be either zero packet size or an updated configuration
                gossipTask = GossipAsync(configByteArray);
                UpdateGossipSend();

                // Track number of full vs empty (ping) sends
                if (configByteArray.Length > 0)
                    clusterProvider.clusterManager.gossipStats.gossip_full_send++;
                else
                    clusterProvider.clusterManager.gossipStats.gossip_empty_send++;

                // Track bytes send
                clusterProvider.clusterManager.gossipStats.UpdateGossipBytesSend(configByteArray.Length);
                return true;
            }
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