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
        public bool IsConnected => aofSyncTasks.Select(x => x.IsConnected ? 1 : 0).Sum() > 0;

        /// <summary>
        /// Node-id associated with this AofSyncTask
        /// </summary>
        public string RemoteNodeId => remoteNodeId;

        public SingleWriterMultiReaderLock dispose = new();

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
            for (var sublogIdx = 0; sublogIdx < aofSyncTasks.Length; sublogIdx++)
                aofSyncTasks[sublogIdx] = new AofSyncTask(clusterProvider, sublogIdx, endPoint, startAddress[sublogIdx], localNodeId, remoteNodeId, cts, logger);
        }

        /// <summary>
        /// Dispose AofSyncDriver
        /// </summary>
        public void Dispose()
        {
            // First cancel the token
            cts?.Cancel();

            // Then, dispose the iterator. This will also signal the iterator so that it can observe the canceled token
            foreach (var aofSyncTask in aofSyncTasks)
                aofSyncTask?.Dispose();

            // Signal dispose
            // NOTE: wait for tasks to complete
            dispose.WriteLock();

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

                // Add refresh sublog task only when using more than 1 physical sublog
                if (clusterProvider.serverOptions.AofPhysicalSublogCount > 1)
                    tasks.Add(RefreshSublogTail());

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

        async Task RefreshSublogTail()
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
                acquireReadLock = dispose.TryReadLock();
                if (!acquireReadLock)
                    throw new GarnetException($"Failed to acquire read lock at {nameof(RefreshSublogTail)}");

                // Connect to replica
                client.Connect((int)clusterProvider.serverOptions.ReplicaSyncTimeout.TotalMilliseconds, cts.Token);

                while (true)
                {
                    await Task.Delay(clusterProvider.serverOptions.AofRefreshSublogTailFrequencyMs, cts.Token);

                    var tailAddress = clusterProvider.storeWrapper.appendOnlyFile.Log.TailAddress;
                    var previousAddress = PreviousAddress;

                    // Acquire key sequence number vector from the replica
                    var resp = await client.ExecuteClusterShardedLogKeySequenceVector().WaitAsync(clusterProvider.serverOptions.ReplicaSyncTimeout, cts.Token);
                    var maxSublogSeqNumber = AofAddress.FromString(resp);
                    var mssn = maxSublogSeqNumber.Max();

                    // At least one sublog has stalled if both of the following conditions hold
                    //  1. the maximum sequence number of the sublog is smaller than MSSN
                    //  2. The sublog does not have any more data to send.
                    // If (1) is false then it is safe to read from that sublog because it will have the highest sequence number
                    // If (2) is false the we still have more data to process hence the sequence number will possible change in the future.
                    for (var i = 0; i < maxSublogSeqNumber.Length; i++)
                    {
                        cts.Token.ThrowIfCancellationRequested();
                        if (maxSublogSeqNumber[i] < mssn && previousAddress[i] == tailAddress[i])
                        {
                            // logger?.LogError("refresh> {i} {mssn}", i, mssn);
                            clusterProvider.storeWrapper.appendOnlyFile.EnqueueRefreshSublogTail(i, mssn);
                        }
                    }
                }
            }
            finally
            {
                if (acquireReadLock)
                    dispose.ReadUnlock();
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