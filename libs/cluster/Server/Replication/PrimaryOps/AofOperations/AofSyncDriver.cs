// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class AofSyncDriver : IDisposable
    {
        readonly ClusterProvider clusterProvider;
        readonly AofTaskStore aofTaskStore;
        readonly string localNodeId;
        readonly string remoteNodeId;
        readonly ILogger logger;
        readonly CancellationTokenSource cts;

        AofSyncTask aofSyncTask;

        /// <summary>
        /// Check if client connection is healthy
        /// </summary>
        public bool IsConnected => aofSyncTask.IsConnected;

        /// <summary>
        /// Node-id associated with this AofSyncTask
        /// </summary>
        public string RemoteNodeId => remoteNodeId;

        /// <summary>
        /// Return start address for underlying AofSyncTask
        /// </summary>
        public long StartAddress => aofSyncTask.StartAddress;

        /// <summary>
        /// Return previous address for underlying AofSyncTask
        /// </summary>
        public long PreviousAddress => aofSyncTask.PreviousAddress;

        public AofSyncDriver(
            ClusterProvider clusterProvider,
            AofTaskStore aofTaskStore,
            string localNodeId,
            string remoteNodeId,
            IPEndPoint endPoint,
            long startAddress,
            ILogger logger)
        {
            this.clusterProvider = clusterProvider;
            this.aofTaskStore = aofTaskStore;
            this.localNodeId = localNodeId;
            this.remoteNodeId = remoteNodeId;
            this.logger = logger;
            cts = new CancellationTokenSource();

            aofSyncTask = new AofSyncTask(
                this,
                -1,
                endPoint,
                startAddress,
                cts
            );
        }

        public void Dispose()
        {
            // First cancel the token
            cts?.Cancel();

            // Then, dispose the iterator. This will also signal the iterator so that it can observe the canceled token
            aofSyncTask?.Dispose();

            // Finally, dispose the cts
            cts?.Dispose();
        }

        public void DisposeClient() => aofSyncTask.garnetClient?.Dispose();

        /// <summary>
        /// Main replica aof sync task.
        /// </summary>
        public async Task Run()
        {
            logger?.LogInformation("Starting ReplicationManager.ReplicaSyncTask for remote node {remoteNodeId} starting from address {address}", remoteNodeId, StartAddress);
            
            try
            {
                await aofSyncTask.RunAofSyncTask();
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ReplicaSyncTask - terminating");
            }
            finally
            {
                aofSyncTask.Dispose();
                var (address, port) = clusterProvider.clusterManager.CurrentConfig.GetWorkerAddressFromNodeId(remoteNodeId);
                logger?.LogWarning("AofSync task terminated; client disposed {remoteNodeId} {address} {port} {currentAddress}", remoteNodeId, address, port, PreviousAddress);

                if (!aofTaskStore.TryRemove(this))
                {
                    logger?.LogInformation("Did not remove {remoteNodeId} from aofTaskStore at end of ReplicaSyncTask", remoteNodeId);
                }
            }
        }

        #region DisklesSyncInterface
        public void ConnectClient()
        {
            if (!IsConnected)
                aofSyncTask.garnetClient.Connect();
        }

        public Task<string> IssuesFlushAll()
            => aofSyncTask.garnetClient.ExecuteAsync(["CLUSTER", "FLUSHALL"]);

        public void InitializeIterationBuffer()
            => aofSyncTask.garnetClient.InitializeIterationBuffer(clusterProvider.storeWrapper.loggingFrequency);

        public void InitializeIfNeeded(bool isMainStore)
        {
            if (aofSyncTask.garnetClient.NeedsInitialization)
                aofSyncTask.garnetClient.SetClusterSyncHeader(clusterProvider.clusterManager.CurrentConfig.LocalNodeId, isMainStore: isMainStore);
        }

        public Task<string> ExecuteAttachSync(SyncMetadata syncMetadata)
            => aofSyncTask.garnetClient.ExecuteAttachSync(syncMetadata.ToByteArray());

        public bool TryWriteKeyValueSpanByte(ref SpanByte key, ref SpanByte value, out Task<string> task)
            => aofSyncTask.garnetClient.TryWriteKeyValueSpanByte(ref key, ref value, out task);

        public bool TryWriteKeyValueByteArray(byte[] key, byte[] value, long expiration, out Task<string> task)
            => aofSyncTask.garnetClient.TryWriteKeyValueByteArray(key, value, expiration, out task);

        public Task<string> SendAndResetIterationBuffer()
            => aofSyncTask.garnetClient.SendAndResetIterationBuffer();
        #endregion
    }
}