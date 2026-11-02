// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed class FailoverManager : IDisposable
    {
        FailoverSession currentFailoverSession = null;
        readonly ClusterProvider clusterProvider;
        readonly ILogger logger;
        private SingleWriterMultiReaderLock failoverTaskLock;
        public volatile FailoverStatus lastFailoverStatus = FailoverStatus.NO_FAILOVER;

        int writePauseCount;
        readonly object writePauseLock = new();
        TaskCompletionSource writesResumed;

        /// <summary>
        /// Whether local application-client writes are paused for failover.
        /// </summary>
        internal bool IsWritePaused => Volatile.Read(ref writePauseCount) > 0;

        /// <summary>
        /// Acquires or releases a local write pause without changing cluster role or ownership.
        /// </summary>
        /// <param name="paused">True to acquire a pause; false to release a matching pause.</param>
        internal void PauseWrites(bool paused)
        {
            TaskCompletionSource resumed = null;
            lock (writePauseLock)
            {
                var count = paused ? Interlocked.Increment(ref writePauseCount) : Interlocked.Decrement(ref writePauseCount);
                Debug.Assert(count >= 0, "Each write pause release must match an acquisition.");
                if (count == 0)
                {
                    resumed = writesResumed;
                    writesResumed = null;
                }
            }
            resumed?.SetResult();
        }

        /// <summary>
        /// Returns a shared completion task for the current write pause, allocating only when a writer waits.
        /// Callers must recheck the pause after completion because another owner can acquire a new pause.
        /// </summary>
        internal Task WaitForWritesToResumeAsync()
        {
            lock (writePauseLock)
            {
                if (!IsWritePaused)
                    return Task.CompletedTask;
                writesResumed ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
                return writesResumed.Task;
            }
        }

        /// <summary>
        /// Cluster node timeout, read live so that a CONFIG SET is observed by subsequent failover sessions.
        /// </summary>
        TimeSpan clusterTimeout
            => clusterProvider.storeWrapper.runtimeConfig.GetTimeSpan(ServerConfigType.CLUSTER_NODE_TIMEOUT);

        /// <summary>
        /// Shared epoch instance for failover GarnetClient connections
        /// </summary>
        readonly client.LightEpoch epoch;

        public FailoverManager(ClusterProvider clusterProvider, ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.logger = logger;
            this.epoch = new client.LightEpoch();
        }

        public void Dispose()
        {
            TryAbortReplicaFailover();
            AsyncUtils.BlockingWait(clusterProvider.storeWrapper.TaskManager.CancelAsync(TaskType.FailoverTask));
            // A completing worker unregisters before disposing its connections and releasing ownership.
            failoverTaskLock.WriteLock();
            try
            {
                epoch?.Dispose();
            }
            finally
            {
                failoverTaskLock.WriteUnlock();
            }
        }

        /// <summary>
        /// Abort ongoing failover
        /// </summary>
        public void TryAbortReplicaFailover()
            => Volatile.Read(ref currentFailoverSession)?.Cancel();

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
            var session = new FailoverSession(
                clusterProvider,
                option,
                clusterTimeout: clusterTimeout,
                failoverTimeout: failoverTimeout,
                epoch: epoch,
                isReplicaSession: true,
                logger: logger);
            currentFailoverSession = session;
            var registered = clusterProvider.storeWrapper.TaskManager.RegisterAndRun(TaskType.FailoverTask, token => Task.Run(async () =>
            {
                try
                {
                    using var cancellation = token.Register(session.Cancel);
                    var success = await session.BeginAsyncReplicaFailoverAsync().ConfigureAwait(false);
                    lastFailoverStatus = success ? FailoverStatus.FAILOVER_COMPLETED : FailoverStatus.FAILOVER_ABORTED;
                }
                finally
                {
                    clusterProvider.storeWrapper.TaskManager.TryUnregister(TaskType.FailoverTask);
                    Reset();
                }
            }));
            if (!registered)
                Reset();
            return registered;
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

            var session = new FailoverSession(
                clusterProvider: clusterProvider,
                option: option,
                clusterTimeout: clusterTimeout,
                failoverTimeout: timeout,
                epoch: epoch,
                isReplicaSession: false,
                hostAddress: replicaAddress,
                hostPort: replicaPort,
                logger: logger);
            currentFailoverSession = session;
            var registered = clusterProvider.storeWrapper.TaskManager.RegisterAndRun(TaskType.FailoverTask, token => Task.Run(async () =>
            {
                try
                {
                    using var cancellation = token.Register(session.Cancel);
                    _ = await session.BeginAsyncPrimaryFailoverAsync().ConfigureAwait(false);
                }
                finally
                {
                    clusterProvider.storeWrapper.TaskManager.TryUnregister(TaskType.FailoverTask);
                    Reset();
                }
            }));
            if (!registered)
                Reset();
            return registered;
        }
    }
}