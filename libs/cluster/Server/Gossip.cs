// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal struct GossipStats
    {
        /// <summary>
        /// number of requests for received for processing
        /// </summary>
        public long meet_requests_recv;
        /// <summary>
        /// number of succeeded meet requests
        /// </summary>
        public long meet_requests_succeed;
        /// <summary>
        /// number of failed meet requests
        /// </summary>
        public long meet_requests_failed;

        /// <summary>
        /// number of gossip requests send successfully
        /// </summary>
        public long gossip_success_count;

        /// <summary>
        /// number of gossip requests failed to send
        /// </summary>
        public long gossip_failed_count;

        /// <summary>
        /// number of gossip requests that timed out
        /// </summary>
        public long gossip_timeout_count;

        /// <summary>
        /// number of gossip requests that contained full config array
        /// </summary>
        public long gossip_full_send;

        /// <summary>
        /// number of gossip requests send with empty array (i.e. ping)
        /// </summary>
        public long gossip_empty_send;

        /// <summary>
        /// Aggregate bytes gossip has send
        /// </summary>
        public long gossip_bytes_send;

        /// <summary>
        /// Aggregate bytes gossip has received
        /// </summary>
        public long gossip_bytes_recv;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpdateMeetRequestsRecv()
            => Interlocked.Increment(ref meet_requests_recv);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpdateMeetRequestsSucceed()
            => Interlocked.Increment(ref meet_requests_succeed);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpdateMeetRequestsFailed()
            => Interlocked.Increment(ref meet_requests_failed);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpdateGossipBytesSend(int byteCount)
            => Interlocked.Add(ref gossip_bytes_send, byteCount);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpdateGossipBytesRecv(int byteCount)
            => Interlocked.Add(ref gossip_bytes_recv, byteCount);

        public void Reset()
        {
            meet_requests_recv = 0;
            meet_requests_succeed = 0;
            meet_requests_failed = 0;
            gossip_success_count = 0;
            gossip_failed_count = 0;
            gossip_timeout_count = 0;
            gossip_full_send = 0;
            gossip_empty_send = 0;
            gossip_bytes_send = 0;
            gossip_bytes_recv = 0;
        }
    }

    internal sealed partial class ClusterManager : IDisposable
    {
        public readonly TimeSpan gossipDelay;
        public readonly TimeSpan clusterTimeout;
        private volatile int numActiveTasks = 0;
        private SingleWriterMultiReaderLock activeMergeLock;
        readonly GarnetClusterConnectionStore clusterConnectionStore;

        public GossipStats gossipStats;
        readonly int GossipSamplePercent;

        public TimeSpan GetClusterTimeout() => clusterTimeout;
        readonly ConcurrentDictionary<string, long> workerBanList = new ConcurrentDictionary<string, long>();
        public readonly CancellationTokenSource ctsGossip = new();

        public List<string> GetBanList()
        {
            List<string> banlist = new List<string>();
            foreach (var w in workerBanList)
            {
                var nodeId = w.Key;
                var expiry = w.Value;
                var diff = expiry - DateTimeOffset.UtcNow.Ticks;

                string str = $"{nodeId} : {TimeSpan.FromTicks(diff).Seconds}";
                banlist.Add(str);
            }
            return banlist;
        }

        /// <summary>
        /// Get link status info for primary of this node.
        /// </summary>
        /// <param name="config">Snapshot of config to use for retrieving that information.</param>
        /// <returns>MetricsItem array of all the associated info.</returns>
        public MetricsItem[] GetPrimaryLinkStatus(ClusterConfig config)
        {
            var primaryId = config.GetLocalNodePrimaryId();
            var primaryLinkStatus = new MetricsItem[2];

            primaryLinkStatus[0] = new("master_link_status", "down");
            primaryLinkStatus[1] = new("master_last_io_seconds_ago", "0");
            if (primaryId != null)
                clusterConnectionStore.GetConnectionInfo(primaryId, ref primaryLinkStatus);
            return primaryLinkStatus;
        }

        private bool Expired(long expiry)
            => expiry < DateTimeOffset.UtcNow.Ticks;

        /// <summary>
        /// Pause merge config ops by setting numActiveMerge to MinValue.
        /// Called when FORGET op executes and waits until ongoing merge operations complete before executing FORGET
        /// Multiple FORGET ops can execute at the same time.
        /// </summary>
        private void PauseConfigMerge()
            => activeMergeLock.WriteLock();

        /// <summary>
        /// Unpause config merge
        /// </summary>
        private void UnpauseConfigMerge()
            => activeMergeLock.WriteUnlock();

        /// <summary>
        /// Increment only when numActiveMerge tasks are >= 0 else wait.
        /// numActiveMerge less than 0 when ongoing FORGET op. Ensures that FORGET is atomic and visible to all Merg ops
        /// before returning ack to the caller.
        /// </summary>
        private void IncrementConfigMerge()
            => activeMergeLock.ReadLock();

        private void DecrementConfigMerge()
            => activeMergeLock.ReadUnlock();

        /// <summary>
        /// Initiate meet and main gossip tasks
        /// </summary>
        private void TryStartGossipTasks()
        {
            // Start background task for gossip protocol
            for (int i = 2; i <= CurrentConfig.NumWorkers; i++)
            {
                var (address, port) = CurrentConfig.GetWorkerAddress((ushort)i);
                RunMeetTask(address, port);
            }

            Interlocked.Increment(ref numActiveTasks);
            Task.Run(GossipMain);
        }

        /// <summary>
        /// Merge incoming config to evolve local version
        /// </summary>
        public bool TryMerge(ClusterConfig other)
        {
            try
            {
                IncrementConfigMerge();
                if (workerBanList.ContainsKey(other.GetLocalNodeId()))
                {
                    logger?.LogTrace("Cannot merge node <{nodeid}> because still in ban list", other.GetLocalNodeId());
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
                DecrementConfigMerge();
            }
        }

        /// <summary>
        /// Run meet background task
        /// </summary>
        /// <param name="address"></param>
        /// <param name="port"></param>
        public void RunMeetTask(string address, int port)
            => Task.Run(() => Meet(address, port));

        /// <summary>
        /// Meet will immediately communicate with the new node and try to merge the retrieve configuration to its own.
        /// If node to meet was previous in the ban list then it will not be added to the cluster
        /// </summary>
        /// <param name="address"></param>
        /// <param name="port"></param>
        public void Meet(string address, int port)
        {
            GarnetServerNode gsn = null;
            var conf = CurrentConfig;
            var nodeId = conf.GetWorkerNodeIdFromAddress(address, port);
            MemoryResult<byte> resp = default;
            bool created = false;

            gossipStats.UpdateMeetRequestsRecv();
            try
            {
                if (nodeId != null)
                    clusterConnectionStore.GetConnection(nodeId, out gsn);

                if (gsn == null)
                {
                    gsn = new GarnetServerNode(this, replicationManager, address, port, tlsOptions?.TlsClientOptions, logger: logger);
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
                    nodeId = other.GetLocalNodeId();
                    gsn.nodeid = nodeId;

                    logger?.LogInformation("MEET {nodeId} {address} {port}", nodeId, address, port);
                    // Merge without a check because node is trusted as meet was issued by admin
                    TryMerge(other);

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
        /// Dispose connections for workers in the ban list
        /// </summary>
        private void DisposeBannedWorkerConnections()
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
                    clusterConnectionStore.TryRemove(nodeId);
                }
                else // Remove worker from ban list
                    workerBanList.TryRemove(nodeId, out var _);
            }
        }

        /// <summary>
        /// Initialize connections for nodes that have either been dispose due to banlist (after expiry) or timeout
        /// </summary>
        private void InitConnections()
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
                    var gsn = new GarnetServerNode(this, replicationManager, address, port, tlsOptions?.TlsClientOptions, logger: logger)
                    {
                        nodeid = nodeId
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
        }

        /// <summary>
        /// Initiate a full broadcast gossip transmission
        /// </summary>
        public void BroadcastGossipSend()
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
                    logger?.LogWarning("GOSSIP to remote node [{nodeId} {address}:{port}] timeout!", currNode.nodeid, currNode.address, currNode.port);
                    clusterConnectionStore.TryRemove(currNode.nodeid);
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "GOSSIP to remote node [{nodeId} {address} {port}] failed!", currNode.nodeid, currNode.address, currNode.port);
                    clusterConnectionStore.TryRemove(currNode.nodeid);
                    gossipStats.gossip_failed_count++;
                }
            }
        }

        /// <summary>
        /// Initiate sampling gossip transmission
        /// </summary>
        public void GossipSampleSend()
        {
            var nodeCount = clusterConnectionStore.Count;
            int fraction = (int)(Math.Ceiling(nodeCount * (GossipSamplePercent / 100.0f)));
            int count = Math.Max(Math.Min(1, nodeCount), fraction);

            long startTime = DateTimeOffset.UtcNow.Ticks;
            while (count > 0)
            {
                long minSend = startTime;
                GarnetServerNode currNode = null;

                for (int i = 0; i < 3; i++)
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
                    logger?.LogWarning("GOSSIP to remote node [{nodeId} {address}:{port}] timeout!", currNode.nodeid, currNode.address, currNode.port);
                    clusterConnectionStore.TryRemove(currNode.nodeid);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "GOSSIP to remote node [{nodeId} {address} {port}] failed!", currNode.nodeid, currNode.address, currNode.port);
                    clusterConnectionStore.TryRemove(currNode.nodeid);
                    gossipStats.gossip_failed_count++;
                }

                count--;
            }

        }

        /// <summary>
        /// Main method responsible initiating gossip messages
        /// </summary>
        public void TransmitGossip()
        {
            // Choose between full broadcast or sample gossip to few nodes
            if (GossipSamplePercent == 100)
                BroadcastGossipSend();
            else
                GossipSampleSend();
        }

        /// <summary>
        /// Main gossip async task
        /// </summary>
        private async void GossipMain()
        {
            try
            {
                while (true)
                {
                    if (ctsGossip.Token.IsCancellationRequested) return;
                    InitConnections();
                    TransmitGossip();

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
                Interlocked.Decrement(ref numActiveTasks);
            }
        }
    }
}