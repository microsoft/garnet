// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal class AofSyncTaskInfo : IBulkLogEntryConsumer, IDisposable
    {
        readonly ClusterProvider clusterProvider;
        readonly AofTaskStore aofTaskStore;
        readonly string localNodeId;
        public readonly string remoteNodeId;
        readonly ILogger logger;
        public readonly GarnetClientSession garnetClient;
        readonly CancellationTokenSource cts;
        TsavoriteLogScanIterator iter;
        readonly long startAddress;
        public long previousAddress;

        public AofSyncTaskInfo(
            ClusterProvider clusterProvider,
            AofTaskStore aofTaskStore,
            string localNodeId,
            string remoteNodeId,
            GarnetClientSession garnetClient,
            CancellationTokenSource cts,
            long startAddress,
            ILogger logger)
        {
            this.clusterProvider = clusterProvider;
            this.aofTaskStore = aofTaskStore;
            this.localNodeId = localNodeId;
            this.remoteNodeId = remoteNodeId;
            this.logger = logger;
            this.garnetClient = garnetClient;
            this.cts = cts;
            this.startAddress = startAddress;
            previousAddress = startAddress;
        }

        public void Dispose()
        {
            iter?.Dispose();
            cts?.Dispose();
        }

        public unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress)
        {
            try
            {
                // logger?.LogInformation("Sending {payloadLength} bytes to {remoteNodeId} at address {currentAddress}-{nextAddress}", payloadLength, remoteNodeId, currentAddress, nextAddress);

                // This is called under epoch protection, so we have to wait for appending to complete
                garnetClient.ExecuteClusterAppendLog(localNodeId, previousAddress, currentAddress, nextAddress, (long)payloadPtr, payloadLength);

                // Set task address to nextAddress, as the iterator is currently at nextAddress
                // (records at currentAddress are already sent above)
                previousAddress = nextAddress;
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.AofSyncTaskInfo.Consume");
                throw;
            }
        }

        public void Throttle()
        {
            garnetClient.Throttle();
        }

        /// <summary>
        /// Main replica aof sync task.
        /// </summary>
        public async Task ReplicaSyncTask()
        {
            logger?.LogInformation("Starting ReplicationManager.ReplicaSyncTask for remote node {remoteNodeId} starting from address {address}", remoteNodeId, startAddress);

            try
            {
                garnetClient.Connect();

                iter = clusterProvider.storeWrapper.appendOnlyFile.Scan(startAddress, long.MaxValue, name: remoteNodeId[..20], scanUncommitted: true, recover: false, logger: logger);

                while (true)
                {
                    if (cts.Token.IsCancellationRequested) break;
                    await iter.BulkConsumeAllAsync(this, clusterProvider.serverOptions.ReplicaSyncDelayMs, maxChunkSize: 1 << 20, cts.Token);
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ReplicaSyncTask - terminating");
            }
            finally
            {
                garnetClient.Dispose();
                var (address, port) = clusterProvider.clusterManager.CurrentConfig.GetWorkerAddressFromNodeId(remoteNodeId);
                logger?.LogWarning("AofSync task terminated; client disposed {remoteNodeId} {address} {port} {currentAddress}", remoteNodeId, address, port, previousAddress);

                if (!aofTaskStore.TryRemove(this))
                {
                    logger?.LogInformation("Did not remove {remoteNodeId} from aofTaskStore at end of ReplicaSyncTask", remoteNodeId);
                }
            }
        }
    }
}