// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal class FailoverManager : IDisposable
    {
        FailoverSession currentFailoverSession = null;
        readonly ClusterProvider clusterProvider;
        readonly TimeSpan clusterTimeout;
        readonly ILogger logger;
        private SingleWriterMultiReaderLock failoverTaskLock;

        public FailoverManager(ClusterProvider clusterProvider, GarnetServerOptions opts, TimeSpan clusterTimeout, ILoggerFactory loggerFactory)
        {
            this.clusterProvider = clusterProvider;
            this.clusterTimeout = clusterTimeout;

            string address = opts.Address ?? StoreWrapper.GetIp();
            this.logger = loggerFactory?.CreateLogger($"ClusterManager-{address}:{opts.Port}");
        }

        public void Dispose()
        {
            Reset();
        }

        /// <summary>
        /// Abort ongoing failover
        /// </summary>
        public void TryAbortReplicaFailover()
            => Reset();

        private void Reset()
        {
            currentFailoverSession?.Dispose();
            currentFailoverSession = null;
            if (failoverTaskLock.IsWriteLocked) failoverTaskLock.WriteUnlock();
        }

        /// <summary>
        /// Retrieve the status of an ongoing failover
        /// </summary>
        /// <returns></returns>
        public string GetFailoverStatus()
        {
            var status = currentFailoverSession?.GetStatus;
            return status.HasValue ? FailoverUtils.GetFailoverStatus(status) :
                FailoverUtils.GetFailoverStatus(FailoverStatus.NO_FAILOVER);
        }

        /// <summary>
        /// Method used to initiate a background failover from a replica (CLUSTER FAILOVER command)
        /// </summary>
        /// <param name="option">Failover type option.</param>
        /// <param name="failoverTimeout">Timeout per failover operation.</param>
        /// <returns></returns>
        public bool TryStartReplicaFailover(FailoverOption option, TimeSpan failoverTimeout = default)
        {
            if (!failoverTaskLock.TryWriteLock())
                return false;

            var (address, port) = clusterProvider.clusterManager.CurrentConfig.GetLocalNodePrimaryAddress();
            if (address == null)
            {
                failoverTaskLock.WriteUnlock();
                return false;
            }

            currentFailoverSession = new FailoverSession(
                clusterProvider,
                option,
                clusterTimeout: clusterTimeout,
                failoverTimeout: failoverTimeout,
                hostAddress: address,
                hostPort: port,
                logger: logger);
            Task.Run(ReplicaFailoverAsyncTask);
            return true;
        }

        private async void ReplicaFailoverAsyncTask()
        {
            await currentFailoverSession.BeginAsyncReplicaFailover();
            Reset();
        }

        /// <summary>
        /// Method used to initiate a failover from a primary (FAILOVER command).
        /// </summary>
        /// <param name="replicaAddress">IP address of replica.</param>
        /// <param name="replicaPort">Port of replica.</param>
        /// <param name="option">Failover option type.</param>
        /// <param name="timeout">Timeout per failover operation.</param>
        /// <returns></returns>
        public bool TryStartPrimaryFailover(string replicaAddress, int replicaPort, FailoverOption option, TimeSpan timeout)
        {
            if (!failoverTaskLock.TryWriteLock())
                return false;

            currentFailoverSession = new FailoverSession(
                clusterProvider,
                option,
                clusterTimeout,
                timeout,
                hostAddress: replicaAddress,
                hostPort: replicaPort,
                logger: logger);
            Task.Run(PrimaryFailoverAsyncTask);
            return true;
        }

        private async void PrimaryFailoverAsyncTask()
        {
            await currentFailoverSession.BeginAsyncPrimaryFailover();
            Reset();
        }
    }
}