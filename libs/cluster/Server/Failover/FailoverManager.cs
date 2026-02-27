// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed class FailoverManager : IDisposable
    {
        FailoverSession currentFailoverSession = null;
        readonly ClusterProvider clusterProvider;
        readonly TimeSpan clusterTimeout;
        readonly ILogger logger;
        private SingleWriterMultiReaderLock failoverTaskLock;
        public FailoverStatus lastFailoverStatus = FailoverStatus.NO_FAILOVER;

        /// <summary>
        /// Shared epoch instance for failover GarnetClient connections
        /// </summary>
        readonly client.LightEpoch epoch;

        public FailoverManager(ClusterProvider clusterProvider, ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            clusterTimeout = clusterProvider.serverOptions.ClusterTimeout <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(clusterProvider.serverOptions.ClusterTimeout);
            this.logger = logger;
            this.epoch = new client.LightEpoch();
        }

        public void Dispose()
        {
            Reset();
            epoch?.Dispose();
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
            var status = currentFailoverSession?.status;
            return status.HasValue ? FailoverUtils.GetFailoverStatus(status) :
                FailoverUtils.GetFailoverStatus(FailoverStatus.NO_FAILOVER);
        }

        /// <summary>
        /// Retrieve the status of the last failover
        /// </summary>
        /// <returns></returns>
        public string GetLastFailoverStatus() => FailoverUtils.GetFailoverStatus(lastFailoverStatus);

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

            lastFailoverStatus = FailoverStatus.BEGIN_FAILOVER;
            currentFailoverSession = new FailoverSession(
                clusterProvider,
                option,
                clusterTimeout: clusterTimeout,
                failoverTimeout: failoverTimeout,
                epoch: epoch,
                isReplicaSession: true,
                logger: logger);
            _ = Task.Run(async () =>
            {
                var success = await currentFailoverSession.BeginAsyncReplicaFailover();
                lastFailoverStatus = success ? FailoverStatus.FAILOVER_COMPLETED : FailoverStatus.FAILOVER_ABORTED;
                Reset();
            });
            return true;
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
                clusterProvider: clusterProvider,
                option: option,
                clusterTimeout: clusterTimeout,
                failoverTimeout: timeout,
                epoch: epoch,
                isReplicaSession: false,
                hostAddress: replicaAddress,
                hostPort: replicaPort,
                logger: logger);
            _ = Task.Run(async () =>
            {
                _ = await currentFailoverSession.BeginAsyncPrimaryFailover();
                Reset();
            });
            return true;
        }
    }
}