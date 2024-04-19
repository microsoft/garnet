// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class FailoverSession : IDisposable
    {
        readonly ClusterProvider clusterProvider;
        readonly TimeSpan clusterTimeout;
        readonly TimeSpan failoverTimeout;
        readonly CancellationTokenSource cts;
        readonly FailoverOption option;
        readonly ILogger logger;

        readonly GarnetClient[] clients = null;
        readonly long failoverStart;
        readonly long failoverEnd;

        public FailoverStatus status { get; private set; }

        public bool FailoverTimeout => failoverEnd < DateTimeOffset.UtcNow.Ticks;

        readonly ClusterConfig currentConfig;

        /// <summary>
        /// FailoverSession constructor
        /// </summary>
        /// <param name="clusterProvider">ClusterProvider object</param>
        /// <param name="option">Failover options for replica failover session.</param>
        /// <param name="clusterTimeout">Timeout for individual communication between replica.</param>
        /// <param name="failoverTimeout">End to end timeout for failover</param>
        /// <param name="isReplicaSession">Flag indicating if this session is controlled by a replica</param>
        /// <param name="hostAddress"></param>
        /// <param name="hostPort"></param>
        /// <param name="logger"></param>
        public FailoverSession(
            ClusterProvider clusterProvider,
            FailoverOption option,
            TimeSpan clusterTimeout,
            TimeSpan failoverTimeout,
            bool isReplicaSession = true,
            string hostAddress = "",
            int hostPort = -1,
            ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.clusterTimeout = clusterTimeout;
            this.failoverTimeout = failoverTimeout == default ? TimeSpan.FromSeconds(300) : failoverTimeout;
            this.option = option;
            this.cts = new();
            this.logger = logger;

            this.currentConfig = clusterProvider.clusterManager.CurrentConfig;

            // Initialize connections only when failover is initiated by the primary
            if (!isReplicaSession)
            {
                var endpoints = hostPort == -1
                    ? currentConfig.GetLocalNodePrimaryEndpoints(includeMyPrimaryFirst: true)
                    : hostPort == 0 ? currentConfig.GetLocalNodeReplicaEndpoints() : null;
                clients = endpoints != null ? new GarnetClient[endpoints.Count] : new GarnetClient[1];

                if (clients.Length > 1)
                {
                    for (var i = 0; i < endpoints.Count; i++)
                    {
                        clients[i] = new GarnetClient(endpoints[i].Item1, endpoints[i].Item2, clusterProvider.serverOptions.TlsOptions?.TlsClientOptions, authUsername: clusterProvider.ClusterUsername, authPassword: clusterProvider.ClusterPassword, logger: logger);
                    }
                }
                else
                {
                    clients[0] = new GarnetClient(hostAddress, hostPort, clusterProvider.serverOptions.TlsOptions?.TlsClientOptions, authUsername: clusterProvider.ClusterUsername, authPassword: clusterProvider.ClusterPassword, logger: logger);
                }
            }

            // Timeout deadline
            this.failoverStart = DateTimeOffset.UtcNow.Ticks;
            this.failoverEnd = failoverStart + this.failoverTimeout.Ticks;
            this.status = FailoverStatus.BEGIN_FAILOVER;
        }

        public void Dispose()
        {
            cts.Cancel();
            cts.Dispose();
            DisposeConnections();
        }

        private void DisposeConnections()
        {
            if (clients != null)
                foreach (var client in clients)
                    client?.Dispose();
        }
    }
}