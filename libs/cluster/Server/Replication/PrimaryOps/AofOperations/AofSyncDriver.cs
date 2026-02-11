// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class AofSyncDriver : IDisposable
    {
        readonly ClusterProvider clusterProvider;
        readonly AofSyncDriverStore aofSyncDriverStore;
        readonly string localNodeId;
        readonly string remoteNodeId;
        readonly ILogger logger;
        readonly CancellationTokenSource cts;

        AofSyncTask[] aofSyncTasks;

        /// <summary>
        /// Check if client connection is healthy
        /// </summary>
        public bool IsConnected
            => aofSyncTasks.Select(x => x.IsConnected ? 1 : 0).Sum() == aofSyncTasks.Length;

        /// <summary>
        /// Node-id associated with this AofSyncTask
        /// </summary>
        public string RemoteNodeId => remoteNodeId;

        SingleWriterMultiReaderLock activeAofSync = new();

        public bool ResumeAofStreaming()
            => activeAofSync.TryReadLock();

        public void SuspendAofStreaming()
            => activeAofSync.ReadUnlock();

        /// <summary>
        /// Return start address for underlying AofSyncTask
        /// </summary>
        public AofAddress StartAddress
        {
            get
            {
                var startAddress = AofAddress.Create(aofSyncTasks.Length, 0);
                for (var i = 0; i < aofSyncTasks.Length; i++)
                    startAddress[i] = aofSyncTasks[i].StartAddress;
                return startAddress;
            }
        }

        /// <summary>
        /// Return previous address for underlying AofSyncTask
        /// </summary>
        public AofAddress PreviousAddress
        {
            get
            {
                var previousAddress = AofAddress.Create(aofSyncTasks.Length, 0);
                for (var i = 0; i < aofSyncTasks.Length; i++)
                    previousAddress[i] = aofSyncTasks[i].PreviousAddress;
                return previousAddress;
            }
        }

        /// <summary>
        /// Replica endpoint
        /// </summary>
        readonly IPEndPoint endPoint;

        public AofSyncDriver(
            ClusterProvider clusterProvider,
            AofSyncDriverStore aofSyncDriverStore,
            string localNodeId,
            string remoteNodeId,
            IPEndPoint endPoint,
            ref AofAddress startAddress,
            ILogger logger)
        {
            this.clusterProvider = clusterProvider;
            this.aofSyncDriverStore = aofSyncDriverStore;
            this.localNodeId = localNodeId;
            this.remoteNodeId = remoteNodeId;
            this.endPoint = endPoint;
            cts = new();
            this.logger = logger;

            aofSyncTasks = new AofSyncTask[clusterProvider.serverOptions.AofPhysicalSublogCount];
            for (var physicalSublogIdx = 0; physicalSublogIdx < aofSyncTasks.Length; physicalSublogIdx++)
                aofSyncTasks[physicalSublogIdx] = new AofSyncTask(clusterProvider, physicalSublogIdx, endPoint, startAddress[physicalSublogIdx], localNodeId, remoteNodeId, cts, logger);
        }

        /// <summary>
        /// Dispose AofSyncDriver
        /// </summary>
        public void Dispose()
        {
            // Cancel cts
            cts?.Cancel();

            // Dispose sync tasks
            foreach (var aofSyncTask in aofSyncTasks)
                aofSyncTask?.Dispose();

            // Wait for tasks to exit
            activeAofSync.WriteLock();

            // Finally, dispose the cts
            cts?.Dispose();
        }

        /// <summary>
        /// Dispose alls clients associated with this aof sync driver
        /// </summary>
        public void DisposeClient()
        {
            foreach (var aofSyncTask in aofSyncTasks)
                aofSyncTask.garnetClient?.Dispose();
        }

        /// <summary>
        /// Main replica aof sync task.
        /// </summary>
        public async Task Run()
        {
            logger?.LogInformation("Starting ReplicationManager.ReplicaSyncTask for remote node {remoteNodeId} starting from address {address}", remoteNodeId, StartAddress);

            try
            {
                var tasks = new List<Task>();

                // Create advance physical sublog time task when the instance is configured to use more than one physical sublogs.
                if (clusterProvider.serverOptions.AofPhysicalSublogCount > 1)
                    tasks.Add(AdvancePhysicalSublogTime());

                for (var i = 0; i < aofSyncTasks.Length; i++)
                    tasks.Add(aofSyncTasks[i].RunAofSyncTask(this));

                _ = await Task.WhenAny([.. tasks]);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ReplicaSyncTask - terminating");
            }
            finally
            {
                var (address, port) = clusterProvider.clusterManager.CurrentConfig.GetWorkerAddressFromNodeId(remoteNodeId);
                logger?.LogWarning("AofSync task terminated; client disposed {remoteNodeId} {address} {port} {currentAddress}", remoteNodeId, address, port, PreviousAddress);

                if (!aofSyncDriverStore.TryRemove(this))
                    logger?.LogError("Unable to remove {remoteNodeId} from aofTaskStore at end of ReplicaSyncTask", remoteNodeId);
            }
        }

        /// <summary>
        /// Advance physical sublog time background task.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        /// <seealso cref="T:Garnet.cluster.ReplicationManager.AdvanceTime"/>
        async Task AdvancePhysicalSublogTime()
        {
            var acquireReadLock = false;
            var client = new GarnetClientSession(
                        endPoint,
                        clusterProvider.replicationManager.GetAofSyncNetworkBufferSettings,
                        clusterProvider.replicationManager.GetNetworkPool,
                        tlsOptions: this.clusterProvider.serverOptions.TlsOptions?.TlsClientOptions,
                        authUsername: this.clusterProvider.ClusterUsername,
                        authPassword: this.clusterProvider.ClusterPassword,
                        logger: logger);

            try
            {
                acquireReadLock = ResumeAofStreaming();
                if (!acquireReadLock)
                    throw new GarnetException($"Failed to acquire read lock at {nameof(AdvancePhysicalSublogTime)}");

                // Connect to replica
                client.Connect((int)clusterProvider.serverOptions.ReplicaSyncTimeout.TotalMilliseconds, cts.Token);

                var appendOnlyFile = clusterProvider.storeWrapper.appendOnlyFile;
                var previousTailAddress = AofAddress.Create(appendOnlyFile.Log.Size, 0);

                while (!cts.IsCancellationRequested)
                {
                    await Task.Delay(clusterProvider.serverOptions.AofTailWitnessFreq, cts.Token);
                    var currentTailAddress = appendOnlyFile.Log.TailAddress;
                    var newWrites = previousTailAddress.AnyLesser(currentTailAddress);

                    if (newWrites)
                    {
                        var sequenceNumber = appendOnlyFile.GetLargerThanMaximumSequenceNumber();
                        _ = await client.ExecuteClusterAdvanceTime(sequenceNumber, currentTailAddress.Span).
                            WaitAsync(clusterProvider.serverOptions.ReplicaSyncTimeout, cts.Token).
                            ConfigureAwait(false);
                        previousTailAddress.MonotonicUpdate(ref currentTailAddress);
                    }
                }
            }
            finally
            {
                if (acquireReadLock)
                    SuspendAofStreaming();
                client?.Dispose();
            }
        }

        #region DisklesSyncInterface
        public void ConnectClients()
        {
            if (!IsConnected)
                foreach (var aofSyncTask in aofSyncTasks)
                    aofSyncTask.garnetClient.Connect((int)clusterProvider.serverOptions.ReplicaSyncTimeout.TotalMilliseconds, cts.Token);
        }

        public Task<string> IssuesFlushAll()
            => aofSyncTasks[0].garnetClient.ExecuteAsync(["CLUSTER", "FLUSHALL"]);

        public void InitializeIterationBuffer()
            => aofSyncTasks[0].garnetClient.InitializeIterationBuffer(clusterProvider.storeWrapper.loggingFrequency);

        public void InitializeIfNeeded()
        {
            if (aofSyncTasks[0].garnetClient.NeedsInitialization)
                aofSyncTasks[0].garnetClient.SetClusterSyncHeader(clusterProvider.clusterManager.CurrentConfig.LocalNodeId);
        }

        public Task<string> ExecuteAttachSync(SyncMetadata syncMetadata)
            => aofSyncTasks[0].garnetClient.ExecuteClusterAttachSync(syncMetadata.ToByteArray());

        public bool TryWriteRecordSpan(ReadOnlySpan<byte> recordSpan, out Task<string> task)
            => aofSyncTasks[0].garnetClient.TryWriteRecordSpan(recordSpan, out task);

        public Task<string> SendAndResetIterationBuffer()
            => aofSyncTasks[0].garnetClient.SendAndResetIterationBuffer();
        #endregion
    }
}