// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

        readonly AofSyncTask[] aofSyncTasks;

        /// <summary>
        /// Check if client connection is healthy
        /// </summary>
        public bool IsConnected
            => aofSyncTasks.Select(x => x.IsConnected ? 1 : 0).Sum() == aofSyncTasks.Length;

        /// <summary>
        /// Node-id associated with this AofSyncTask
        /// </summary>
        public string RemoteNodeId => remoteNodeId;

        /// <summary>
        /// Active worker monitor for AofSyncDriver tasks
        /// </summary>
        readonly ActiveWorkerMonitor activeWorkerMonitor = new();

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
        /// Return previous address for a specific sublog without copying the full AofAddress struct
        /// </summary>
        /// <param name="physicalSublogIdx">Index of the physical sublog.</param>
        /// <returns>The previous address of the specified sublog's sync task.</returns>
        public long GetPreviousAddress(int physicalSublogIdx) => aofSyncTasks[physicalSublogIdx].PreviousAddress;

        /// <summary>
        /// Return start address for a specific sublog without copying the full AofAddress struct
        /// </summary>
        /// <param name="physicalSublogIdx">Index of the physical sublog.</param>
        /// <returns>The start address of the specified sublog's sync task.</returns>
        public long GetStartAddress(int physicalSublogIdx) => aofSyncTasks[physicalSublogIdx].StartAddress;

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
            activeWorkerMonitor.Dispose();

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
        public async Task RunAsync()
        {
            logger?.LogInformation("Starting ReplicationManager.ReplicaSyncTask for remote node {remoteNodeId} starting from address {address}", remoteNodeId, StartAddress);

            try
            {
                if (clusterProvider.serverOptions.AofPhysicalSublogCount == 1)
                {
                    await aofSyncTasks[0].RunAofSyncTaskAsync(this).ConfigureAwait(false);
                }
                else
                {
                    var tasks = new Task[aofSyncTasks.Length + 1];
                    tasks[0] = AdvancePhysicalSublogTimeAsync();
                    for (var i = 0; i < aofSyncTasks.Length; i++)
                        tasks[i + 1] = aofSyncTasks[i].RunAofSyncTaskAsync(this);

                    _ = await Task.WhenAny(tasks).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ReplicaSyncTask - terminating");
            }
            finally
            {
                var (address, port) = clusterProvider.clusterManager.CurrentConfig.GetWorkerAddressFromNodeId(remoteNodeId);
                logger?.LogWarning("AofSyncDriver terminated; client disposed replicaId:{remoteNodeId} [{address}:{port}] startAddress: {startAddress}, previousAddress:{previousAddress}", remoteNodeId, address, port, StartAddress, PreviousAddress);

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
        async Task AdvancePhysicalSublogTimeAsync()
        {
            var enteredMonitor = false;
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
                enteredMonitor = activeWorkerMonitor.TryEnter();
                if (!enteredMonitor)
                    throw new GarnetException($"Failed to acquire read lock at {nameof(AdvancePhysicalSublogTimeAsync)}");

                // Connect to replica
                await client.ConnectAsync((int)clusterProvider.serverOptions.ReplicaSyncTimeout.TotalMilliseconds, cts.Token).ConfigureAwait(false);

                var appendOnlyFile = clusterProvider.storeWrapper.appendOnlyFile;
                var previousTailAddress = AofAddress.Create(appendOnlyFile.Log.Size, 0);

                while (!cts.IsCancellationRequested)
                {
                    await Task.Delay(clusterProvider.serverOptions.AofTailWitnessFreqMs, cts.Token).ConfigureAwait(false);
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
                if (enteredMonitor)
                    _ = activeWorkerMonitor.Exit();
                client?.Dispose();
            }
        }

        #region DisklesSyncInterface
        public async Task ConnectClientsAsync()
        {
            if (!IsConnected)
                foreach (var aofSyncTask in aofSyncTasks)
                    await aofSyncTask.garnetClient.ConnectAsync((int)clusterProvider.serverOptions.ReplicaSyncTimeout.TotalMilliseconds, cts.Token).ConfigureAwait(false);
        }

        public Task<string> IssuesFlushAllAsync()
            => aofSyncTasks[0].garnetClient.ExecuteAsync(["CLUSTER", "FLUSHALL"]);

        public void InitializeIterationBuffer()
            => aofSyncTasks[0].garnetClient.InitializeIterationBuffer(clusterProvider.storeWrapper.loggingFrequency);

        public void InitializeIfNeeded()
        {
            if (aofSyncTasks[0].garnetClient.NeedsInitialization)
                aofSyncTasks[0].garnetClient.SetClusterSyncHeader(clusterProvider.clusterManager.CurrentConfig.LocalNodeId);
        }

        public Task<string> ExecuteAttachSyncAsync(SyncMetadata syncMetadata)
            => aofSyncTasks[0].garnetClient.ExecuteClusterAttachSync(syncMetadata.ToByteArray());

        public bool TryWriteRecordSpan(ReadOnlySpan<byte> recordSpan, MigrationRecordSpanType type, out Task<string> task)
            => aofSyncTasks[0].garnetClient.TryWriteRecordSpan(recordSpan, type, out task);

        public Task<string> SendAndResetIterationBufferAsync()
            => aofSyncTasks[0].garnetClient.SendAndResetIterationBuffer();
        #endregion
    }
}