// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class AofSyncDriver : IDisposable
    {
        readonly ClusterProvider clusterProvider;
        readonly AofSyncDriverStore aofTaskStore;
        readonly string localNodeId;
        readonly string remoteNodeId;
        readonly ILogger logger;
        readonly CancellationTokenSource cts;

        AofSyncTask[] aofSyncTasks;

        /// <summary>
        /// Check if client connection is healthy
        /// </summary>
        public bool IsConnected => aofSyncTasks[0].IsConnected;

        /// <summary>
        /// Node-id associated with this AofSyncTask
        /// </summary>
        public string RemoteNodeId => remoteNodeId;

        /// <summary>
        /// Return start address for underlying AofSyncTask
        /// </summary>
        public AofAddress StartAddress => GetAofStartAddress();

        AofAddress startAddress;

        AofAddress GetAofStartAddress()
        {
            for (var i = 0; i < aofSyncTasks.Length; i++)
                startAddress[i] = aofSyncTasks[i].StartAddress;
            return startAddress;
        }

        /// <summary>
        /// Return previous address for underlying AofSyncTask
        /// </summary>
        public ref AofAddress PreviousAddress
        {
            get
            {
                for (var i = 0; i < aofSyncTasks.Length; i++)
                    previousAddress[i] = aofSyncTasks[i].PreviousAddress;
                return ref previousAddress;
            }
        }
        AofAddress previousAddress;

        public ref AofAddress MaxSublogTimestamps
        {
            get
            {
                for (var i = 0; i < aofSyncTasks.Length; i++)
                    maxSublogTimestamps[i] = aofSyncTasks[i].MaxSendTimestamp;
                return ref maxSublogTimestamps;
            }
        }
        AofAddress maxSublogTimestamps;

        public AofSyncDriver(
            ClusterProvider clusterProvider,
            AofSyncDriverStore aofSyncDriver,
            string localNodeId,
            string remoteNodeId,
            IPEndPoint endPoint,
            ref AofAddress startAddress,
            ILogger logger)
        {
            this.clusterProvider = clusterProvider;
            this.aofTaskStore = aofSyncDriver;
            this.localNodeId = localNodeId;
            this.remoteNodeId = remoteNodeId;
            this.startAddress = startAddress;
            this.previousAddress = startAddress;
            this.logger = logger;
            cts = new CancellationTokenSource();

            aofSyncTasks = new AofSyncTask[clusterProvider.serverOptions.AofSublogCount];
            for (var i = 0; i < aofSyncTasks.Length; i++)
                aofSyncTasks[i] = new AofSyncTask(this, i, endPoint, startAddress[i], cts);
        }

        public void Dispose()
        {
            // First cancel the token
            cts?.Cancel();

            // Then, dispose the iterator. This will also signal the iterator so that it can observe the canceled token
            foreach (var aofSyncTask in aofSyncTasks)
                aofSyncTask?.Dispose();

            // Finally, dispose the cts
            cts?.Dispose();
        }

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
                foreach (var aofSyncTask in aofSyncTasks)
                    tasks.Add(aofSyncTask.RunAofSyncTask());
                tasks.Add(WitnessTask());

                await Task.WhenAll([.. tasks]);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ReplicaSyncTask - terminating");
            }
            finally
            {
                foreach (var aofSyncTask in aofSyncTasks)
                    aofSyncTask.Dispose();
                var (address, port) = clusterProvider.clusterManager.CurrentConfig.GetWorkerAddressFromNodeId(remoteNodeId);
                logger?.LogWarning("AofSync task terminated; client disposed {remoteNodeId} {address} {port} {currentAddress}", remoteNodeId, address, port, PreviousAddress);

                if (!aofTaskStore.TryRemove(this))
                {
                    logger?.LogInformation("Did not remove {remoteNodeId} from aofTaskStore at end of ReplicaSyncTask", remoteNodeId);
                }
            }
        }

        async Task WitnessTask()
        {
            var oldPreviousAddress = AofAddress.SetValue(aofSyncTasks.Length, 0L);

            while (true)
            {
                await Task.Delay(100, cts.Token);
                var tailAddress = clusterProvider.storeWrapper.appendOnlyFile.Log.TailAddress;
                var previousAddress = PreviousAddress;
                var masSublogTimestamps = MaxSublogTimestamps;
                // Maximum Send Timestamp (MST)
                var mst = masSublogTimestamps.Max();

                // At least one sublog has stalled if both of the following conditions hold
                //  1. the maximum timestamp of the sublog is smaller than MST
                //  2. The sublog does not have any more data to send.
                // If (1) is false then it is safe to read from that sublog because it will have the highest timestamp
                // If (2) is false the we still have more data to process hence the timestamp will possible change in the future.
                for (var i = 0; i < masSublogTimestamps.Length; i++)
                {
                    if (masSublogTimestamps[i] < mst && previousAddress[i] == tailAddress[i])
                    {
                        //clusterProvider.storeWrapper.appendOnlyFile.Enqueue()
                    }
                }
            }
        }

        #region DisklesSyncInterface
        public void ConnectClient()
        {
            if (!IsConnected)
                foreach (var aofSyncTask in aofSyncTasks)
                    aofSyncTask.garnetClient.Connect();
        }

        public Task<string> IssuesFlushAll()
            => aofSyncTasks[0].garnetClient.ExecuteAsync(["CLUSTER", "FLUSHALL"]);

        public void InitializeIterationBuffer()
            => aofSyncTasks[0].garnetClient.InitializeIterationBuffer(clusterProvider.storeWrapper.loggingFrequency);

        public void InitializeIfNeeded(bool isMainStore)
        {
            if (aofSyncTasks[0].garnetClient.NeedsInitialization)
                aofSyncTasks[0].garnetClient.SetClusterSyncHeader(clusterProvider.clusterManager.CurrentConfig.LocalNodeId, isMainStore: isMainStore);
        }

        public Task<string> ExecuteAttachSync(SyncMetadata syncMetadata)
            => aofSyncTasks[0].garnetClient.ExecuteClusterAttachSync(syncMetadata.ToByteArray());

        public bool TryWriteKeyValueSpanByte(ref SpanByte key, ref SpanByte value, out Task<string> task)
            => aofSyncTasks[0].garnetClient.TryWriteKeyValueSpanByte(ref key, ref value, out task);

        public bool TryWriteKeyValueByteArray(byte[] key, byte[] value, long expiration, out Task<string> task)
            => aofSyncTasks[0].garnetClient.TryWriteKeyValueByteArray(key, value, expiration, out task);

        public Task<string> SendAndResetIterationBuffer()
            => aofSyncTasks[0].garnetClient.SendAndResetIterationBuffer();
        #endregion
    }
}