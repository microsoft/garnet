// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class ClusterManager : IDisposable
    {
        const int maxRandomNodesToPoll = 3;
        public readonly TimeSpan gossipDelay;
        public readonly TimeSpan clusterTimeout;
        private volatile int numActiveTasks = 0;
        private SingleWriterMultiReaderLock activeMergeLock;
        public readonly GarnetClusterConnectionStore clusterConnectionStore;

        public GossipStats gossipStats;
        readonly int GossipSamplePercent;

        public TimeSpan GetClusterTimeout() => clusterTimeout;
        readonly ConcurrentDictionary<string, long> workerBanList = new();
        public readonly CancellationTokenSource ctsGossip = new();

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

        static bool Expired(long expiry) => expiry < DateTimeOffset.UtcNow.Ticks;

        /// <summary>
        /// Pause merge config ops by setting numActiveMerge to MinValue.
        /// Called when FORGET op executes and waits until ongoing merge operations complete before executing FORGET
        /// Multiple FORGET ops can execute at the same time.
        /// </summary>
        void PauseConfigMerge() => activeMergeLock.WriteLock();

        /// <summary>
        /// Resume config merge
        /// </summary>
        void ResumeConfigMerge() => activeMergeLock.WriteUnlock();

        /// <summary>
        /// Initiate meet and main gossip tasks
        /// </summary>
        void TryStartGossipTasks()
        {
            // Start background task for gossip protocol
            for (var i = 2; i <= CurrentConfig.NumWorkers; i++)
            {
                var (address, port) = CurrentConfig.GetWorkerAddress((ushort)i);
                RunMeetTask(address, port);
            }

            _ = Interlocked.Increment(ref numActiveTasks);
            _ = Task.Run(GossipMain);
        }

        /// <summary>
        /// Merge incoming config to evolve local version
        /// </summary>
        public bool TryMerge(ClusterConfig other)
        {
            try
            {
                activeMergeLock.ReadLock();
                if (workerBanList.ContainsKey(other.LocalNodeId))
                {
                    logger?.LogTrace("Cannot merge node <{nodeid}> because still in ban list", other.LocalNodeId);
                    return false;
                }

                while (true)
                {
                    var current = currentConfig;
                    var currentCopy = current.Copy();
                    var next = currentCopy.Merge(other, workerBanList).HandleConfigEpochCollision(other);
                    if (currentCopy == next) return false;
                    if (Interlocked.CompareExchange(ref currentConfig, next, current) == current)
                        break;
                }
                FlushConfig();
                return true;
            }
            finally
            {
                activeMergeLock.ReadUnlock();
            }
        }

        /// <summary>
        /// Run meet background task
        /// </summary>
        /// <param name="address"></param>
        /// <param name="port"></param>
        public void RunMeetTask(string address, int port)
            => Task.Run(() => TryMeet(address, port));

        /// <summary>
        /// This task will immediately communicate with the new node and try to merge the retrieve configuration to its own.
        /// If node to meet was previous in the ban list then it will not be added to the cluster.
        /// </summary>
        /// <param name="address"></param>
        /// <param name="port"></param>
        void TryMeet(string address, int port)
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
                    gsn = new GarnetServerNode(clusterProvider, address, port, tlsOptions?.TlsClientOptions, logger: logger);
                    created = true;
                }

                // Initialize GarnetServerNode
                // Thread-Safe initialization executes only once
                gsn.Initialize();

                // Send full config in Gossip
                resp = gsn.TryMeet(conf.ToByteArray());
                if (resp.Length > 0)
                {
                    var other = ClusterConfig.FromByteArray(resp.Span.ToArray());
                    nodeId = other.LocalNodeId;
                    gsn.NodeId = nodeId;

                    logger?.LogInformation("MEET {nodeId} {address} {port}", nodeId, address, port);
                    // Merge without a check because node is trusted as meet was issued by admin
                    _ = TryMerge(other);

                    gossipStats.UpdateMeetRequestsSucceed();

                    // If failed to add newly created connection dispose of it to reclaim resources
                    // Dispose only connections that this meet task has created to avoid conflicts with existing connections from gossip main thread
                    // After connection is added we are no longer the owner. Background gossip task will be owner
                    if (created && !clusterConnectionStore.AddConnection(gsn))
                        gsn.Dispose();
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
        /// Main gossip async task
        /// </summary>
        async void GossipMain()
        {
            // Main gossip loop
            try
            {
                while (true)
                {
                    if (ctsGossip.Token.IsCancellationRequested) return;
                    InitConnections();

                    // Choose between full broadcast or sample gossip to few nodes
                    if (GossipSamplePercent == 100)
                        BroadcastGossipSend();
                    else
                        GossipSampleSend();

                    await Task.Delay(gossipDelay, ctsGossip.Token);
                }
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
            void InitConnections()
            {
                DisposeBannedWorkerConnections();

                var current = currentConfig;
                var addresses = current.GetWorkerInfoForGossip();

                foreach (var a in addresses)
                {
                    if (ctsGossip.Token.IsCancellationRequested) break;
                    var nodeId = a.Item1;
                    var address = a.Item2;
                    var port = a.Item3;

                    // Establish new connection only if it is not in banlist and not in dictionary
                    if (!workerBanList.ContainsKey(nodeId) && !clusterConnectionStore.GetConnection(nodeId, out var _))
                    {
                        var gsn = new GarnetServerNode(clusterProvider, address, port, tlsOptions?.TlsClientOptions, logger: logger)
                        {
                            NodeId = nodeId
                        };
                        try
                        {
                            gsn.Initialize();
                            if (!clusterConnectionStore.AddConnection(gsn))
                                gsn.Dispose();
                        }
                        catch (Exception ex)
                        {
                            logger?.LogWarning("Connection to remote node [{nodeId} {address}:{port}] failed with message:{msg}", nodeId, address, port, ex.Message);
                            gsn?.Dispose();
                        }
                    }
                }

                void DisposeBannedWorkerConnections()
                {
                    foreach (var w in workerBanList)
                    {
                        if (ctsGossip.Token.IsCancellationRequested) return;
                        var nodeId = w.Key;
                        var expiry = w.Value;

                        // Check if ban on worker has expired or not
                        if (!Expired(expiry))
                        {
                            // Remove connection for banned worker
                            _ = clusterConnectionStore.TryRemove(nodeId);
                        }
                        else // Remove worker from ban list
                            _ = workerBanList.TryRemove(nodeId, out var _);
                    }
                }
            }

            // Initiate a full broadcast gossip transmission
            void BroadcastGossipSend()
            {
                // Issue async gossip tasks to all nodes
                uint offset = 0;
                while (clusterConnectionStore.GetConnectionAtOffset(offset, out var currNode))
                {
                    try
                    {
                        if (ctsGossip.Token.IsCancellationRequested) return;

                        // Issue gossip message to node and truck success metrics
                        if (currNode.TryGossip())
                        {
                            gossipStats.gossip_success_count++;
                            offset++;
                            continue;
                        }

                        gossipStats.gossip_timeout_count++;
                        logger?.LogWarning("GOSSIP to remote node [{nodeId} {address}:{port}] timeout!", currNode.NodeId, currNode.Address, currNode.Port);
                        _ = clusterConnectionStore.TryRemove(currNode.NodeId);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogWarning(ex, "GOSSIP to remote node [{nodeId} {address} {port}] failed!", currNode.NodeId, currNode.Address, currNode.Port);
                        _ = clusterConnectionStore.TryRemove(currNode.NodeId);
                        gossipStats.gossip_failed_count++;
                    }
                }
            }

            // Initiate sampling gossip transmission
            void GossipSampleSend()
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
                        if (ctsGossip.Token.IsCancellationRequested) return;

                        // Issue gossip message to node and truck success metrics
                        if (currNode.TryGossip())
                        {
                            gossipStats.gossip_success_count++;
                            continue;
                        }

                        gossipStats.gossip_timeout_count++;
                        logger?.LogWarning("GOSSIP to remote node [{nodeId} {address}:{port}] timeout!", currNode.NodeId, currNode.Address, currNode.Port);
                        _ = clusterConnectionStore.TryRemove(currNode.NodeId);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "GOSSIP to remote node [{nodeId} {address} {port}] failed!", currNode.NodeId, currNode.Address, currNode.Port);
                        _ = clusterConnectionStore.TryRemove(currNode.NodeId);
                        gossipStats.gossip_failed_count++;
                    }

                    count--;
                }

            }
        }
    }
}