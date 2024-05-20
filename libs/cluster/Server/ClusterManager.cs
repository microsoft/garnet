// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;
using Garnet.server;
using Garnet.server.TLS;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Cluster manager
    /// </summary>
    internal sealed partial class ClusterManager : IDisposable
    {
        ClusterConfig currentConfig;
        readonly IDevice clusterConfigDevice;
        readonly SectorAlignedBufferPool pool;
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
        public unsafe ClusterManager(ClusterProvider clusterProvider, ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            var opts = clusterProvider.serverOptions;
            var clusterFolder = "/cluster";
            var clusterDataPath = opts.CheckpointDir + clusterFolder;
            var deviceFactory = opts.GetInitializedDeviceFactory(clusterDataPath);

            clusterConfigDevice = deviceFactory.Get(new FileDescriptor(directoryName: "", fileName: "nodes.conf"));
            pool = new(1, (int)clusterConfigDevice.SectorSize);

            var address = opts.Address ?? StoreWrapper.GetIp();
            this.logger = logger;
            var recoverConfig = clusterConfigDevice.GetFileSize(0) > 0 && !opts.CleanClusterConfig;

            tlsOptions = opts.TlsOptions;
            if (!opts.CleanClusterConfig)
                logger?.LogInformation("Attempt to recover cluster config from: {configFilename}", clusterConfigDevice.FileName);
            else
                logger?.LogInformation("Skipping recovery of local config due to CleanClusterConfig flag set");

            if (recoverConfig)
            {
                logger?.LogTrace("Recover cluster config from disk");
                var config = ClusterUtils.ReadDevice(clusterConfigDevice, pool, logger);
                currentConfig = ClusterConfig.FromByteArray(config);
                // Used to update endpoint if it change when running inside a container.
                if (address != currentConfig.LocalNodeIp || opts.Port != currentConfig.LocalNodePort)
                {
                    logger?.LogInformation(
                        "Updating local Endpoint: From {currentConfig.GetLocalNodeIp()}:{currentConfig.GetLocalNodePort()} to {address}:{opts.Port}",
                        currentConfig.LocalNodeIp,
                        currentConfig.LocalNodePort,
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
            GossipSamplePercent = opts.GossipSamplePercent;
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
                    conf.LocalNodeId,
                    address,
                    port,
                    configEpoch: conf.LocalNodeConfigEpoch,
                    role: conf.LocalNodeRole,
                    replicaOfNodeId: conf.LocalNodePrimaryId,
                    hostname: Format.GetHostName());
            }
            else
            {
                TryInitializeLocalWorker(
                    Generator.CreateHexId(),
                    address,
                    port,
                    configEpoch: 0,
                    NodeRole.PRIMARY,
                    null,
                    Format.GetHostName());
            }
        }

        public string GetInfo()
        {
            var current = CurrentConfig;
            var ClusterInfo = $"" +
                $"cluster_state:ok\r\n" +
                $"cluster_slots_assigned:{current.GetSlotCountForState(SlotState.STABLE)}\r\n" +
                $"cluster_slots_ok:{current.GetSlotCountForState(SlotState.STABLE)}\r\n" +
                $"cluster_slots_pfail:{current.GetSlotCountForState(SlotState.FAIL)}\r\n" +
                $"cluster_slots_fail:{current.GetSlotCountForState(SlotState.FAIL)}\r\n" +
                $"cluster_known_nodes:{current.NumWorkers}\r\n" +
                $"cluster_size:{current.GetPrimaryCount()}\r\n" +
                $"cluster_current_epoch:{current.GetMaxConfigEpoch()}\r\n" +
                $"cluster_my_epoch:{current.LocalNodeConfigEpoch}\r\n" +
                $"cluster_stats_messages_sent:0\r\n" +
                $"cluster_stats_messages_received:0\r\n";
            return ClusterInfo;
        }

        private static string GetRange(int[] slots)
        {
            var range = "> ";
            var start = slots[0];
            var end = slots[0];
            for (var i = 1; i < slots.Length + 1; i++)
            {
                if (i < slots.Length && slots[i] == end + 1)
                    end = slots[i];
                else
                {
                    range += $"{start}-{end} ";
                    if (i < slots.Length)
                    {
                        start = slots[i];
                        end = slots[i];
                    }
                }
            }
            return range;
        }

        /// <summary>
        /// Attempts to update config epoch of local worker
        /// </summary>
        /// <param name="configEpoch"></param>
        /// <param name="errorMessage">The ASCII encoded error message if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
        /// <returns></returns>
        public bool TrySetLocalConfigEpoch(long configEpoch, out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            while (true)
            {
                var current = currentConfig;
                if (current.NumWorkers == 0)
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_WORKERS_NOT_INITIALIZED;
                    return false;
                }

                var newConfig = currentConfig.SetLocalWorkerConfigEpoch(configEpoch);
                if (newConfig == null)
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_CONFIG_EPOCH_NOT_SET;
                    return false;
                }

                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            logger?.LogTrace("SetConfigEpoch {configEpoch}", configEpoch);
            return true;
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

        /// <summary>
        /// Set local node role
        /// </summary>
        /// <param name="role">Role type</param>
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

        /// <summary>
        /// Reset node to primary.
        /// </summary>
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

        /// <summary>
        /// Force this node to be a replica of given node-id
        /// </summary>
        /// <param name="replicaId">Node-id to replicate</param>
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

        /// <summary>
        /// Takeover as new primary but forcefully claim ownership of old primary's slots.
        /// </summary>
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
    }
}