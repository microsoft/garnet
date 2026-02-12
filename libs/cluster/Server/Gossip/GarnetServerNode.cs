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

        long gossipSend;
        long gossipRecv;
        CancellationTokenSource cts = new();
        CancellationTokenSource internalCts = new();
        volatile int initialized = 0;
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
                endpoint, tlsOptions,
                sendPageSize: opts.DisablePubSub ? defaultSendPageSize : Math.Max(defaultSendPageSize, (int)opts.PubSubPageSizeBytes()),
                maxOutstandingTasks: defaultMaxOutstandingTask,
                timeoutMilliseconds: opts.ClusterTimeout <= 0 ? 0 : TimeSpan.FromSeconds(opts.ClusterTimeout).Milliseconds,
                authUsername: clusterProvider.clusterManager.clusterProvider.ClusterUsername,
                authPassword: clusterProvider.clusterManager.clusterProvider.ClusterPassword,
                epoch: epoch,
                logger: logger);
            this.initialized = 0;
            this.logger = logger;
            this.gossipRecv = 0;
            this.gossipSend = 0;
            ResetCts();
        }

        /// <summary>
        /// Initialize connection and cancellation tokens.
        /// Initialization is performed only once
        /// </summary>
        public Task InitializeAsync()
        {
            // Ensure initialize executes only once
            if (initialized != 0 || Interlocked.CompareExchange(ref initialized, 1, 0) != 0) return Task.CompletedTask;

            cts = CancellationTokenSource.CreateLinkedTokenSource(clusterProvider.clusterManager.ctsGossip.Token, internalCts.Token);
            return gc.ReconnectAsync().WaitAsync(clusterProvider.clusterManager.gossipDelay, cts.Token);
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
            bool internalCtsDisposed = false;
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
                if (clusterProvider.replicationManager != null) lastConfig.LazyUpdateLocalReplicationOffset(clusterProvider.replicationManager.ReplicationOffset);
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
        private Task Gossip(byte[] configByteArray)
        {
            return gc.Gossip(configByteArray).ContinueWith(t =>
            {
                try
                {
                    var resp = t.Result;
                    if (resp.Length > 0)
                    {
                        clusterProvider.clusterManager.gossipStats.UpdateGossipBytesRecv(resp.Length);
                        var returnedConfigArray = resp.Span.ToArray();
                        var other = ClusterConfig.FromByteArray(returnedConfigArray);
                        var current = clusterProvider.clusterManager.CurrentConfig;
                        // Check if gossip is from a node that is known and trusted before merging
                        if (current.IsKnown(other.LocalNodeId))
                            clusterProvider.clusterManager.TryMerge(ClusterConfig.FromByteArray(returnedConfigArray));
                        else
                            logger?.LogWarning("Received gossip from unknown node: {node-id}", other.LocalNodeId);
                    }
                    resp.Dispose();
                }
                catch (Exception ex)
                {
                    logger?.LogCritical(ex, "GOSSIP faulted processing response");
                }
            }, TaskContinuationOptions.OnlyOnRanToCompletion).WaitAsync(clusterProvider.clusterManager.gossipDelay, cts.Token);
        }

        /// <summary>
        /// Issue gossip meet with meet to force receiving node to trust an untrusted node
        /// </summary>
        /// <param name="configByteArray"></param>
        /// <returns></returns>
        public async Task<MemoryResult<byte>> TryMeetAsync(byte[] configByteArray)
        {
            UpdateGossipSend();
            var resp = await gc.GossipWithMeet(configByteArray).WaitAsync(clusterProvider.clusterManager.clusterTimeout, cts.Token);
            return resp;
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
                gossipTask = Gossip(configArray);
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
                gossipTask = Gossip(configByteArray);
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
        public void TryClusterPublish(RespCommand cmd, ref Span<byte> channel, ref Span<byte> message)
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
                gc.ClusterPublishNoResponse(cmd, ref channel, ref message);
            }
            finally
            {
                if (locked) dispose.ReadUnlock();
            }
        }
    }
}