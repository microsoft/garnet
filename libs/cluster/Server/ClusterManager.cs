// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Garnet.common;
using Garnet.server;
using Garnet.server.TLS;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal enum ClusterOp : byte
    {
        MIGRATION,
    }

    internal class WorkerComparer : IEqualityComparer<Worker>
    {
        public bool Equals(Worker a, Worker b)
        {
            return a.nodeid.Equals(b.nodeid);
        }

        public int GetHashCode(Worker key)
        {
            return key.nodeid.GetHashCode();
        }
    }

#if NET5_0
    static class MyExtensions
    {
        public static Task<TResult> WaitAsync<TResult>(this Task<TResult> task, int millisecondsTimeout) =>
            WaitAsync(task, TimeSpan.FromMilliseconds(millisecondsTimeout), default);

        public static Task<TResult> WaitAsync<TResult>(this Task<TResult> task, TimeSpan timeout) =>
            WaitAsync(task, timeout, default);

        public static Task<TResult> WaitAsync<TResult>(this Task<TResult> task, CancellationToken cancellationToken) =>
            WaitAsync(task, Timeout.InfiniteTimeSpan, cancellationToken);

        public static async Task<TResult> WaitAsync<TResult>(this Task<TResult> task, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<TResult>();
            using (new Timer(s => ((TaskCompletionSource<TResult>)s).TrySetException(new TimeoutException()), tcs, timeout, Timeout.InfiniteTimeSpan))
            using (cancellationToken.Register(s => ((TaskCompletionSource<TResult>)s).TrySetCanceled(), tcs))
            {
                return await (await Task.WhenAny(task, tcs.Task).ConfigureAwait(false)).ConfigureAwait(false);
            }
        }

        public static Task WaitAsync(this Task task, int millisecondsTimeout) =>
            WaitAsync(task, TimeSpan.FromMilliseconds(millisecondsTimeout), default);

        public static Task WaitAsync(this Task task, TimeSpan timeout) =>
            WaitAsync(task, timeout, default);

        public static Task WaitAsync(this Task task, CancellationToken cancellationToken) =>
            WaitAsync(task, Timeout.InfiniteTimeSpan, cancellationToken);

        public async static Task WaitAsync(this Task task, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            using (new Timer(s => ((TaskCompletionSource<bool>)s).TrySetException(new TimeoutException()), tcs, timeout, Timeout.InfiniteTimeSpan))
            using (cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).TrySetCanceled(), tcs))
            {
                await (await Task.WhenAny(task, tcs.Task).ConfigureAwait(false)).ConfigureAwait(false);
            }
        }
    }
#endif

    /// <summary>
    /// Cluster manager
    /// </summary>
    internal sealed partial class ClusterManager : IDisposable
    {
        ClusterConfig currentConfig;
        readonly IDevice clusterConfigDevice;
        readonly SectorAlignedBufferPool pool;

        /// <summary>
        /// Replication manager - needs to be set after instantiation, hence made public
        /// </summary>
        public ReplicationManager replicationManager;

        readonly ILogger logger;

        /// <summary>
        /// Get current config
        /// </summary>
        public ClusterConfig CurrentConfig => currentConfig;

        /// <summary>
        /// Tls Client options
        /// </summary>
        readonly IGarnetTlsOptions tlsOptions;

        /// <summary>
        /// ClusterProvider
        /// </summary>
        public readonly ClusterProvider clusterProvider;

        /// <summary>
        /// Constructor
        /// </summary>
        public unsafe ClusterManager(
            ClusterProvider clusterProvider,
            ILoggerFactory loggerFactory = null)
        {
            this.clusterProvider = clusterProvider;
            var opts = clusterProvider.serverOptions;
            var clusterFolder = "/cluster";
            var clusterDataPath = opts.CheckpointDir + clusterFolder;
            var deviceFactory = opts.GetInitializedDeviceFactory(clusterDataPath);

            clusterConfigDevice = deviceFactory.Get(new FileDescriptor(directoryName: "", fileName: "nodes.conf"));
            pool = new(1, (int)clusterConfigDevice.SectorSize);

            string address = opts.Address ?? StoreWrapper.GetIp();
            logger = loggerFactory?.CreateLogger($"ClusterManager-{address}:{opts.Port}");
            bool recoverConfig = clusterConfigDevice.GetFileSize(0) > 0 && !opts.CleanClusterConfig;

            tlsOptions = opts.TlsOptions;
            if (!opts.CleanClusterConfig)
                logger?.LogInformation("Attempt to recover cluster config from: {configFilename}", clusterConfigDevice.FileName);
            else
                logger?.LogInformation("Skipping recovery of local config due to CleanClusterConfig flag set");

            if (recoverConfig)
            {
                logger?.LogTrace("Recover cluster config from disk");
                byte[] config = ClusterUtils.ReadDevice(clusterConfigDevice, pool, logger);
                currentConfig = ClusterConfig.FromByteArray(config);
                //Used to update endpoint if it change when running inside a container.
                if (address != currentConfig.GetLocalNodeIp() || opts.Port != currentConfig.GetLocalNodePort())
                {
                    logger?.LogInformation(
                        "Updating local Endpoint: From {currentConfig.GetLocalNodeIp()}:{currentConfig.GetLocalNodePort()} to {address}:{opts.Port}",
                        currentConfig.GetLocalNodeIp(),
                        currentConfig.GetLocalNodePort(),
                        address,
                        opts.Port);
                }
            }
            else
            {
                logger?.LogTrace("Initialize new node instance config");
                currentConfig = new();
            }

            clusterConnectionStore = new GarnetClusterConnectionStore(logger: logger);

            InitLocal(address, opts.Port, recoverConfig);
            logger?.LogInformation("{NodeInfoStartup}", CurrentConfig.GetClusterInfo().TrimEnd('\n'));
            gossipDelay = TimeSpan.FromSeconds(opts.GossipDelay);
            clusterTimeout = opts.ClusterTimeout <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(opts.ClusterTimeout);
            numActiveTasks = 0;
            this.GossipSamplePercent = opts.GossipSamplePercent;
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            DisposeBackgroundTasks();

            clusterConfigDevice.Dispose();
            pool.Free();
        }

        /// <summary>
        /// Dispose background running tasks before disposing cluster manager
        /// </summary>
        public void DisposeBackgroundTasks()
        {
            ctsGossip.Cancel();
            while (numActiveTasks > 0)
                Thread.Yield();
            ctsGossip.Dispose();
        }

        /// <summary>
        /// Startup cluster manager
        /// </summary>
        public void Start()
            => TryStartGossipTasks();

        public void FlushConfig()
        {
            lock (this)
            {
                logger?.LogTrace("Start FlushConfig {path}", clusterConfigDevice.FileName);
                ClusterUtils.WriteInto(clusterConfigDevice, pool, 0, currentConfig.ToByteArray(), logger: logger);
                logger?.LogTrace("End FlushConfig {path}", clusterConfigDevice.FileName);
            }
        }

        /// <summary>
        /// Init local worker info
        /// </summary>
        /// <param name="address"></param>
        /// <param name="port"></param>
        /// <param name="recoverConfig"></param>
        private void InitLocal(string address, int port, bool recoverConfig)
        {
            if (recoverConfig)
            {
                var conf = currentConfig;
                TryInitializeLocalWorker(
                    conf.GetLocalNodeId(),
                    address,
                    port,
                    configEpoch: conf.GetLocalNodeConfigEpoch(),
                    currentConfigEpoch: conf.GetLocalNodeCurrentConfigEpoch(),
                    lastVotedConfigEpoch: conf.GetLocalNodeLastVotedEpoch(),
                    role: conf.GetLocalNodeRole(),
                    replicaOfNodeId: conf.GetLocalNodePrimaryId(),
                    hostname: Format.GetHostName());
            }
            else
            {
                TryInitializeLocalWorker(
                    Generator.CreateHexId(),
                    address,
                    port,
                    configEpoch: 0,
                    currentConfigEpoch: 0,
                    lastVotedConfigEpoch: 0,
                    NodeRole.PRIMARY,
                    null,
                    Format.GetHostName());
            }
        }

        public string GetInfo()
        {
            var current = CurrentConfig;
            string ClusterInfo = $"" +
                $"cluster_state:ok\r\n" +
                $"cluster_slots_assigned:{current.GetSlotCountForState(SlotState.STABLE)}\r\n" +
                $"cluster_slots_ok:{current.GetSlotCountForState(SlotState.STABLE)}\r\n" +
                $"cluster_slots_pfail:{current.GetSlotCountForState(SlotState.FAIL)}\r\n" +
                $"cluster_slots_fail:{current.GetSlotCountForState(SlotState.FAIL)}\r\n" +
                $"cluster_known_nodes:{current.NumWorkers}\r\n" +
                $"cluster_size:{current.GetPrimaryCount()}\r\n" +
                $"cluster_current_epoch:{current.GetMaxConfigEpoch()}\r\n" +
                $"cluster_my_epoch:{current.GetLocalNodeConfigEpoch()}\r\n" +
                $"cluster_stats_messages_sent:0\r\n" +
                $"cluster_stats_messages_received:0\r\n";
            return ClusterInfo;
        }

        private static string GetRange(List<int> slots)
        {
            string range = "> ";
            int start = slots[0];
            int end = slots[0];
            for (int i = 1; i < slots.Count + 1; i++)
            {
                if (i < slots.Count && slots[i] == end + 1)
                    end = slots[i];
                else
                {
                    range += $"{start}-{end} ";
                    if (i < slots.Count)
                    {
                        start = slots[i];
                        end = slots[i];
                    }
                }
            }
            return range;
        }

        /// <summary>
        /// Update config epoch of local worker
        /// </summary>
        /// <param name="configEpoch"></param>
        /// <returns></returns>
        public ReadOnlySpan<byte> TrySetLocalConfigEpoch(long configEpoch)
        {
            while (true)
            {
                var current = currentConfig;
                if (current.NumWorkers == 0)
                    return new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR workers not initialized.\r\n"));

                var newConfig = currentConfig.SetLocalWorkerConfigEpoch(configEpoch);
                if (newConfig == null)
                    return new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Node config epoch was not set due to invalid epoch specified.\r\n"));

                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            logger?.LogTrace("SetConfigEpoch {configEpoch}", configEpoch);
            return CmdStrings.RESP_OK;
        }

        /// <summary>
        /// Bump cluster epoch from client request.
        /// </summary>
        public bool TryBumpClusterEpoch()
        {
            while (true)
            {
                var current = currentConfig;
                var newConfig = currentConfig.BumpLocalNodeConfigEpoch();
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            return true;
        }

        public long TryBumpCurrentClusterEpoch()
        {
            long currentEpoch = 0;
            while (true)
            {
                var current = currentConfig;
                var newConfig = current.BumpLocalNodeCurrentConfigEpoch();
                currentEpoch = newConfig.GetLocalNodeCurrentConfigEpoch();
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            return currentEpoch;
        }

        public void TrySetLocalNodeRole(NodeRole role)
        {
            while (true)
            {
                var current = currentConfig;
                var newConfig = current.SetLocalWorkerRole(role).BumpLocalNodeConfigEpoch();
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
        }

        public void TryResetReplica()
        {
            while (true)
            {
                var current = currentConfig;
                var newConfig = current.MakeReplicaOf(null).SetLocalWorkerRole(NodeRole.PRIMARY).BumpLocalNodeConfigEpoch();
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
        }

        public void TryStopWrites(string replicaId)
        {
            while (true)
            {
                var current = currentConfig;
                var newConfig = current.MakeReplicaOf(replicaId);
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
        }

        public void TryTakeOverForPrimary()
        {
            while (true)
            {
                var current = currentConfig;
                var newConfig = current.TakeOverFromPrimary();
                newConfig = newConfig.BumpLocalNodeConfigEpoch();
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
        }

        private bool slotBitmapGetBit(ref byte[] bitmap, int pos)
        {
            int BYTE = (pos / 8);
            int BIT = pos & 7;
            return (bitmap[BYTE] & (1 << BIT)) != 0;
        }

        /// <summary>
        /// This method is used to process failover requests from replicas of a given primary.
        /// This node will vote in favor of the request when returning true, or against when returning false.
        /// </summary>
        /// <param name="requestingNodeId"></param>
        /// <param name="requestedEpoch"></param>
        /// <param name="claimedSlots"></param>
        /// <returns></returns>
        public bool AuthorizeFailover(string requestingNodeId, long requestedEpoch, byte[] claimedSlots)
        {
            while (true)
            {
                var current = currentConfig;

                //If I am not a primary or I do not have any assigned slots cannot vote
                var role = current.GetLocalNodeRole();
                if (role != NodeRole.PRIMARY || current.HasAssignedSlots(1))
                    return false;

                //if I already voted for this epoch return
                if (current.GetLocalNodeLastVotedEpoch() == requestedEpoch)
                    return false;

                //Requesting node has to be a known replica node
                var requestingNodeWorker = current.GetWorkerFromNodeId(requestingNodeId);
                if (requestingNodeWorker.role == NodeRole.UNASSIGNED)
                    return false;

                //Check if configEpoch for claimed slots is lower than the config of the requested epoch.
                for (int i = 0; i < ClusterConfig.MAX_HASH_SLOT_VALUE; i++)
                {
                    if (slotBitmapGetBit(ref claimedSlots, i)) continue;
                    if (current.GetConfigEpochFromSlot(i) < requestedEpoch) continue;
                    return false;
                }

                //if reached this point try to update last voted epoch with requested epoch
                var newConfig = currentConfig.SetLocalNodeLastVotedConfigEpoch(requestedEpoch);
                //If config has changed in between go back and retry
                //This can happen when another node trying to acquire that epoch succeeded from the perspective of this node
                //If that is the case, when we retry to authorize. If lastVotedEpoch has been captured we return no vote
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();

            return true;
        }
    }
}