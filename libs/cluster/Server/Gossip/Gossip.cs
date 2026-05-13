// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class ClusterManager : IDisposable
    {
        const int maxRandomNodesToPoll = 3;
        public readonly TimeSpan gossipDelay;
        public readonly TimeSpan clusterTimeout;
        private volatile int numActiveTasks = 0;
        private readonly common.ReaderWriterLock activeMergeLock;
        public readonly GarnetClusterConnectionStore clusterConnectionStore;

        public GossipStats gossipStats;
        readonly int GossipSamplePercent;

        readonly ConcurrentDictionary<string, long> workerBanList = new();
        public readonly CancellationTokenSource ctsGossip = new();

        /// <summary>
        /// Return worker ban list
        /// </summary>
        /// <returns></returns>
        public List<string> GetBanList()
        {
            var banlist = new List<string>();
            foreach (var w in workerBanList)
            {
                var nodeId = w.Key;
                var expiry = w.Value;
                var diff = expiry - DateTimeOffset.UtcNow.Ticks;

                var str = $"{nodeId} : {(int)TimeSpan.FromTicks(diff).TotalSeconds}";
                banlist.Add(str);
            }
            return banlist;
        }

        /// <summary>
        /// Get connection info to populate CLUSTER NODES
        /// </summary>
        /// <param name="nodeId"></param>
        /// <param name="info"></param>
        /// <returns></returns>
        public bool GetConnectionInfo(string nodeId, out ConnectionInfo info)
            => clusterConnectionStore.GetConnectionInfo(nodeId, out info);

        /// <summary>
        /// Get link status info for primary of this node.
        /// </summary>
        /// <param name="config">Snapshot of config to use for retrieving that information.</param>
        /// <returns>MetricsItem array of all the associated info.</returns>
        public MetricsItem[] GetPrimaryLinkStatus(ClusterConfig config)
        {
            ConnectionInfo info = new();
            var primaryId = config.LocalNodePrimaryId;

            if (primaryId != null)
                _ = clusterConnectionStore.GetConnectionInfo(primaryId, out info);

            var primaryLinkStatus = new MetricsItem[2]
            {
                new("master_link_status", info.connected ? "up" : "down"),
                new("master_last_io_seconds_ago", info.lastIO.ToString())
            };
            return primaryLinkStatus;
        }

        /// <summary>
        /// Pause merge config ops by setting numActiveMerge to MinValue.
        /// Called when FORGET op executes and waits until ongoing merge operations complete before executing FORGET
        /// Multiple FORGET ops can execute at the same time.
        /// </summary>
        public void SuspendConfigMerge() => activeMergeLock.WriteLock();

        /// <summary>
        /// Resume config merge
        /// </summary>
        public void ResumeConfigMerge() => activeMergeLock.WriteUnlock();

        /// <summary>
        /// Initiate meet and main gossip tasks
        /// </summary>
        void TryStartGossipTasks()
        {
            // Run one round of meet at start-up
            for (var i = 2; i <= CurrentConfig.NumWorkers; i++)
            {
                var (address, port) = CurrentConfig.GetWorkerAddress((ushort)i);
                RunMeetTask(address, port);
            }

            // Startup gossip background task
            _ = Interlocked.Increment(ref numActiveTasks);
            _ = Task.Run(GossipMainAsync);
        }

        /// <summary>
        /// Merge incoming config to evolve local version
        /// </summary>
        public bool TryMerge(ClusterConfig senderConfig, bool acquireLock = true)
        {
            try
            {
                if (acquireLock) activeMergeLock.ReadLock();
                if (workerBanList.ContainsKey(senderConfig.LocalNodeId))
                {
                    logger?.LogTrace("Cannot merge node <{nodeid}> because still in ban list", senderConfig.LocalNodeId);
                    return false;
                }

                while (true)
                {
                    var current = currentConfig;
                    var currentCopy = current.Copy();
                    var next = currentCopy.Merge(senderConfig, workerBanList, logger).HandleConfigEpochCollision(senderConfig, logger);
                    if (currentCopy == next) return false;
                    if (Interlocked.CompareExchange(ref currentConfig, next, current) == current)
                        break;
                }
                FlushConfig();
                return true;
            }
            finally
            {
                if (acquireLock) activeMergeLock.ReadUnlock();
            }
        }

        /// <summary>
        /// Run meet background task
        /// </summary>
        /// <param name="address"></param>
        /// <param name="port"></param>
        public void RunMeetTask(string address, int port)
            => _ = Task.Run(() => TryMeetAsync(address, port));

        /// <summary>
        /// This task will immediately communicate with the new node and try to merge the retrieve configuration to its own.
        /// If node to meet was previous in the ban list then it will not be added to the cluster.
        /// </summary>
        /// <param name="address">Address of node to issue meet to</param>
        /// <param name="port"> Port of node to issue meet to</param>
        /// <param name="acquireLock">Whether to acquire lock for merging. Default true</param>
        public async Task TryMeetAsync(string address, int port, bool acquireLock = true)
        {
            GarnetServerNode gsn = null;
            var conf = CurrentConfig;
            var nodeId = conf.GetWorkerNodeIdFromAddress(address, port);
            MemoryResult<byte> resp = default;
            var created = false;

            gossipStats.UpdateMeetRequestsRecv();
            try
            {
                if (nodeId != null)
                    clusterConnectionStore.GetConnection(nodeId, out gsn);

                if (gsn == null)
                {
                    var endpoints = await Format.TryCreateEndpointAsync(address, port, tryConnect: true, logger: logger).ConfigureAwait(false);
                    if (endpoints == null)
                    {
                        logger?.LogError("Invalid CLUSTER MEET endpoint!");
                    }
                    gsn = new GarnetServerNode(clusterProvider, endpoints[0], tlsOptions?.TlsClientOptions, clusterConnectionStore.Epoch, logger: logger);
                    created = true;
                }

                // Initialize GarnetServerNode
                // Thread-Safe initialization executes only once
                await gsn.InitializeAsync().ConfigureAwait(false);

                // Send full config in Gossip
                resp = await gsn.TryMeetAsync(conf.ToByteArray()).ConfigureAwait(false);
                if (resp.Length > 0)
                {
                    var respArray = resp.Span.ToArray();

                    // Validate config version before full deserialization
                    if (!ClusterConfig.TryPeekVersion(respArray, out var version) || version != ClusterConfig.ClusterConfigVersion)
                    {
                        logger?.LogWarning("MEET response has incompatible config version: {version}", version);
                        if (created) gsn?.Dispose();
                        gossipStats.UpdateMeetRequestsFailed();
                    }
                    else
                    {
                        var other = ClusterConfig.FromByteArray(respArray);
                        nodeId = other.LocalNodeId;
                        gsn.NodeId = nodeId;

                        logger?.LogInformation("MEET {nodeId} {address} {port}", nodeId, address, port);
                        // Merge without a check because node is trusted as meet was issued by admin
                        _ = TryMerge(other, acquireLock);

                        gossipStats.UpdateMeetRequestsSucceed();

                        // If failed to add newly created connection dispose of it to reclaim resources
                        // Dispose only connections that this meet task has created to avoid conflicts with existing connections from gossip main thread
                        // After connection is added we are no longer the owner. Background gossip task will be owner
                        if (created && !await clusterConnectionStore.AddConnectionAsync(gsn).ConfigureAwait(false))
                            gsn.Dispose();
                    }
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Meet terminated with error");
                if (created) gsn?.Dispose();
                gossipStats.UpdateMeetRequestsFailed();
            }
            finally
            {
                resp.Dispose();
            }
        }

        /// <summary>
        /// Forward message by issuing CLUSTER PUBLISH|SPUBLISH
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="channel"></param>
        /// <param name="message"></param>
        public ValueTask TryClusterPublishAsync(RespCommand cmd, Span<byte> channel, Span<byte> message)
        {
            var conf = CurrentConfig;
            List<(string NodeId, IPEndPoint Endpoint)> nodeEntries = null;
            if (cmd == RespCommand.PUBLISH)
            {
                conf.GetAllNodeIds(out nodeEntries);
            }
            else
            {
                conf.GetNodeIdsForShard(out nodeEntries);
            }

            for (var entryIx = 0; entryIx < nodeEntries.Count; entryIx++)
            {
                var (nodeId, endpoint) = nodeEntries[entryIx];

                var getOrAddTask = clusterConnectionStore.GetOrAddAsync(clusterProvider, endpoint, tlsOptions, nodeId, logger: logger);

                GarnetServerNode gsn;
                if (getOrAddTask.IsCompletedSuccessfully)
                {
                    // Cannot remove .GetResult here, but it's gated by IsCompletedSuccessfully so safe
                    (_, gsn) = AsyncUtils.BlockingWait(getOrAddTask);
                }
                else
                {
                    // Otherwise copy channel & message and go async
                    return new(GoAsyncHelperAsync(getOrAddTask, default, entryIx, null, cmd, channel.ToArray(), message.ToArray()));
                }

                if (gsn == null)
                    continue;

                // Initialize GarnetServerNode
                // Thread-Safe initialization executes only once
                var initTask = gsn.InitializeAsync();
                if (initTask.IsCompletedSuccessfully)
                {
                    // Can stay sync, so proceed
                    gsn.TryClusterPublish(cmd, channel, message);
                }
                else
                {
                    // Copy channel & message and go async
                    return new(GoAsyncHelperAsync(default, initTask, entryIx, gsn, cmd, channel.ToArray(), message.ToArray()));
                }
            }

            // Completed synchronously
            return default;

            async Task GoAsyncHelperAsync(ValueTask<(bool Success, GarnetServerNode Node)> getOrAddTask, ValueTask initTask, int lastEntryIx, GarnetServerNode lastGsn, RespCommand cmd, Memory<byte> channel, Memory<byte> message)
            {
                // Finish the task which caused us to go async
                if (lastGsn == null)
                {
                    (_, lastGsn) = await getOrAddTask.ConfigureAwait(false);

                    if (lastGsn != null)
                    {
                        await lastGsn.InitializeAsync().ConfigureAwait(false);
                    }
                }
                else
                {
                    await initTask.ConfigureAwait(false);
                }

                if (lastGsn != null)
                {
                    lastGsn.TryClusterPublish(cmd, channel.Span, message.Span);
                }

                // Process remainder of entries, staying async
                for (var entryIx = lastEntryIx + 1; entryIx < nodeEntries.Count; entryIx++)
                {
                    var (nodeId, endpoint) = nodeEntries[entryIx];

                    var (_, gsn) = await clusterConnectionStore.GetOrAddAsync(clusterProvider, endpoint, tlsOptions, nodeId, logger: logger).ConfigureAwait(false);

                    if (gsn == null)
                        continue;

                    // Initialize GarnetServerNode
                    // Thread-Safe initialization executes only once
                    await gsn.InitializeAsync().ConfigureAwait(false);

                    // Publish to remote nodes
                    gsn.TryClusterPublish(cmd, channel.Span, message.Span);
                }
            }
        }

        /// <summary>
        /// Main gossip async task
        /// </summary>
        async Task GossipMainAsync()
        {
            // Main gossip loop
            try
            {
                while (true)
                {
                    ctsGossip.Token.ThrowIfCancellationRequested();
                    await InitConnectionsAsync().ConfigureAwait(false);

                    // Choose between full broadcast or sample gossip to few nodes
                    if (GossipSamplePercent == 100)
                        await BroadcastGossipSendAsync().ConfigureAwait(false);
                    else
                        await GossipSampleSendAsync().ConfigureAwait(false);

                    await Task.Delay(gossipDelay, ctsGossip.Token).ConfigureAwait(false);
                }
            }
            catch (TaskCanceledException) when (ctsGossip.Token.IsCancellationRequested)
            {
                // Suppress the exception if the task was cancelled because of store wrapper disposal
            }
            catch (Exception ex)
            {
                logger?.LogWarning("GossipMain terminated with error {msg}", ex.Message);
            }
            finally
            {
                try
                {
                    clusterConnectionStore.Dispose();
                }
                catch (Exception ex)
                {
                    logger?.LogWarning("Error disposing closing gossip connections {msg}", ex.Message);
                }
                _ = Interlocked.Decrement(ref numActiveTasks);
            }

            // Initialize connections for nodes that have either been dispose due to banlist (after expiry) or timeout
            async Task InitConnectionsAsync()
            {
                await DisposeBannedWorkerConnectionsAsync().ConfigureAwait(false);

                var current = currentConfig;
                var addresses = current.GetWorkerInfoForGossip();

                foreach (var a in addresses)
                {
                    ctsGossip.Token.ThrowIfCancellationRequested();
                    var nodeId = a.Item1;
                    var address = a.Item2;
                    var port = a.Item3;

                    // Establish new connection only if it is not in banlist and not in dictionary
                    if (!workerBanList.ContainsKey(nodeId) && !clusterConnectionStore.GetConnection(nodeId, out var _))
                    {
                        try
                        {
                            var (success, gsn) = await clusterConnectionStore.GetOrAddAsync(clusterProvider, new IPEndPoint(IPAddress.Parse(address), port), tlsOptions, nodeId, logger: logger).ConfigureAwait(false);

                            if (gsn == null)
                            {
                                logger?.LogWarning("InitConnections: Could not establish connection to remote node [{nodeId} {address}:{port}] failed", nodeId, address, port);

                                _ = await clusterConnectionStore.TryRemoveConnectionAsync(nodeId).ConfigureAwait(false);

                                continue;
                            }

                            await gsn.InitializeAsync().ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            logger?.LogWarning(ex, "InitConnections: Could not establish connection to remote node [{nodeId} {address}:{port}] failed", nodeId, address, port);
                            _ = await clusterConnectionStore.TryRemoveConnectionAsync(nodeId).ConfigureAwait(false);
                        }
                    }
                }

                async Task DisposeBannedWorkerConnectionsAsync()
                {
                    foreach (var w in workerBanList)
                    {
                        ctsGossip.Token.ThrowIfCancellationRequested();
                        var nodeId = w.Key;
                        var expiry = w.Value;

                        // Check if ban on worker has expired or not
                        if (!Expired(expiry))
                        {
                            // Remove connection for banned worker
                            _ = await clusterConnectionStore.TryRemoveConnectionAsync(nodeId).ConfigureAwait(false);
                        }
                        else // Remove worker from ban list
                            _ = workerBanList.TryRemove(nodeId, out var _);
                    }

                    static bool Expired(long expiry) => expiry < DateTimeOffset.UtcNow.Ticks;
                }
            }

            // Initiate a full broadcast gossip transmission
            async Task BroadcastGossipSendAsync()
            {
                // Issue async gossip tasks to all nodes
                uint offset = 0;
                while (clusterConnectionStore.GetConnectionAtOffset(offset, out var currNode))
                {
                    try
                    {
                        ctsGossip.Token.ThrowIfCancellationRequested();

                        // Issue gossip message to node and truck success metrics
                        if (currNode.TryGossip())
                        {
                            gossipStats.gossip_success_count++;
                            offset++;
                            continue;
                        }

                        gossipStats.gossip_timeout_count++;
                        logger?.LogWarning("GOSSIP to remote node [{nodeId} {endpoint}] timeout!", currNode.NodeId, currNode.EndPoint);
                        _ = await clusterConnectionStore.TryRemoveConnectionAsync(currNode.NodeId).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogWarning(ex, "GOSSIP to remote node [{nodeId} {endpoint}] failed!", currNode.NodeId, currNode.EndPoint);
                        _ = await clusterConnectionStore.TryRemoveConnectionAsync(currNode.NodeId).ConfigureAwait(false);
                        gossipStats.gossip_failed_count++;
                    }
                }
            }

            // Initiate sampling gossip transmission
            async Task GossipSampleSendAsync()
            {
                var nodeCount = clusterConnectionStore.Count;
                var fraction = (int)(Math.Ceiling(nodeCount * (GossipSamplePercent / 100.0f)));
                var count = Math.Max(Math.Min(1, nodeCount), fraction);

                var startTime = DateTimeOffset.UtcNow.Ticks;
                while (count > 0)
                {
                    var minSend = startTime;
                    GarnetServerNode currNode = null;

                    for (var i = 0; i < maxRandomNodesToPoll; i++)
                    {
                        // Pick the node with earliest send timestamp
                        if (clusterConnectionStore.GetRandomConnection(out var c) && c.GossipSend < minSend)
                        {
                            minSend = c.GossipSend;
                            currNode = c;
                        }
                    }

                    if (currNode == null) break;

                    try
                    {
                        ctsGossip.Token.ThrowIfCancellationRequested();

                        // Issue gossip message to node and truck success metrics
                        if (currNode.TryGossip())
                        {
                            gossipStats.gossip_success_count++;
                            continue;
                        }

                        gossipStats.gossip_timeout_count++;
                        logger?.LogWarning("GOSSIP to remote node [{nodeId} {endpoint}] timeout!", currNode.NodeId, currNode.EndPoint);
                        _ = await clusterConnectionStore.TryRemoveConnectionAsync(currNode.NodeId).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "GOSSIP to remote node [{nodeId} {endpoint}] failed!", currNode.NodeId, currNode.EndPoint);
                        _ = await clusterConnectionStore.TryRemoveConnectionAsync(currNode.NodeId).ConfigureAwait(false);
                        gossipStats.gossip_failed_count++;
                    }

                    count--;
                }

            }
        }
    }
}