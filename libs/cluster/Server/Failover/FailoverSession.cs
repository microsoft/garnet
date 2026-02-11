// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
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
        readonly DateTime failoverDeadline;

        public FailoverStatus status { get; private set; }

        public bool FailoverTimeout => failoverDeadline < DateTime.UtcNow;

        readonly ClusterConfig oldConfig;

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
            LightEpoch epoch,
            bool isReplicaSession = true,
            string hostAddress = "",
            int hostPort = -1,
            ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.clusterTimeout = clusterTimeout;
            this.option = option;
            this.logger = logger;
            oldConfig = clusterProvider.clusterManager.CurrentConfig.Copy();
            cts = new();

            // TODO: move connection initialization at start of async primary failover
            // Initialize connections only when failover is initiated by the primary
            if (!isReplicaSession)
            {
                var endpoints = hostPort == -1
                    ? oldConfig.GetLocalNodePrimaryEndpoints(includeMyPrimaryFirst: true)
                    : hostPort == 0 ? oldConfig.GetLocalNodeReplicaEndpoints() : null;
                clients = endpoints != null ? new GarnetClient[endpoints.Count] : new GarnetClient[1];

                if (clients.Length > 1)
                {
                    for (var i = 0; i < endpoints.Count; i++)
                    {
                        clients[i] = new GarnetClient(endpoints[i], clusterProvider.serverOptions.TlsOptions?.TlsClientOptions, authUsername: clusterProvider.ClusterUsername, authPassword: clusterProvider.ClusterPassword, epoch: epoch, logger: logger);
                    }
                }
                else
                {
                    clients[0] = new GarnetClient(new IPEndPoint(IPAddress.Parse(hostAddress), hostPort), clusterProvider.serverOptions.TlsOptions?.TlsClientOptions, authUsername: clusterProvider.ClusterUsername, authPassword: clusterProvider.ClusterPassword, epoch: epoch, logger: logger);
                }
            }

            // Timeout deadline
            this.failoverTimeout = failoverTimeout == default ? TimeSpan.FromSeconds(600) : failoverTimeout;
            failoverDeadline = DateTime.UtcNow.Add(failoverTimeout);
            status = FailoverStatus.BEGIN_FAILOVER;
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