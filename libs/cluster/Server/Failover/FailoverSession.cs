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
        FailoverStatus status;

        public FailoverStatus GetStatus => status;

        public bool FailoverTimeout => failoverEnd < DateTimeOffset.UtcNow.Ticks;

        /// <summary>
        /// FailoverSession constructor
        /// </summary>
        /// <param name="clusterProvider"></param>
        /// <param name="option"></param>
        /// <param name="clusterTimeout">network request timeout</param>
        /// <param name="failoverTimeout">failover timeout</param>
        /// <param name="hostAddress"></param>
        /// <param name="hostPort"></param>
        /// <param name="logger"></param>
        public FailoverSession(
            ClusterProvider clusterProvider,
            FailoverOption option,
            TimeSpan clusterTimeout,
            TimeSpan failoverTimeout = default,
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

            var endpoints = hostPort == -1 ?
                clusterProvider.clusterManager.CurrentConfig.GetLocalNodePrimaryEndpoints(includeMyPrimaryFirst: true) :
                    hostPort == 0 ? clusterProvider.clusterManager.CurrentConfig.GetLocalNodeReplicaEndpoints() : null;

            clients = endpoints != null ? new GarnetClient[endpoints.Count] : new GarnetClient[1];

            if (clients.Length > 1)
            {
                for (int i = 0; i < endpoints.Count; i++)
                {
                    clients[i] = new GarnetClient(endpoints[i].Item1, endpoints[i].Item2, clusterProvider.serverOptions.TlsOptions?.TlsClientOptions, authUsername: clusterProvider.ClusterUsername, authPassword: clusterProvider.ClusterPassword, logger: logger);
                }
            }
            else
            {
                clients[0] = new GarnetClient(hostAddress, hostPort, clusterProvider.serverOptions.TlsOptions?.TlsClientOptions, authUsername: clusterProvider.ClusterUsername, authPassword: clusterProvider.ClusterPassword, logger: logger);
            }

            //Timeout deadline
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