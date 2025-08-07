// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed class AofSyncTaskInfo : IBulkLogEntryConsumer, IDisposable
    {
        readonly ClusterProvider clusterProvider;
        readonly AofTaskStore aofTaskStore;
        readonly string localNodeId;
        public readonly string remoteNodeId;
        readonly ILogger logger;
        public readonly GarnetClientSession garnetClient;
        readonly CancellationTokenSource cts;
        TsavoriteLogScanSingleIterator iter;
        readonly long startAddress;
        public long previousAddress;

        /// <summary>
        /// Check if client connection is healthy
        /// </summary>
        public bool IsConnected => garnetClient != null && garnetClient.IsConnected;

        /// <summary>
        /// Return start address for this AOF iterator
        /// </summary>
        public long StartAddress => startAddress;

        public AofSyncTaskInfo(
            ClusterProvider clusterProvider,
            AofTaskStore aofTaskStore,
            string localNodeId,
            string remoteNodeId,
            GarnetClientSession garnetClient,
            long startAddress,
            ILogger logger)
        {
            this.clusterProvider = clusterProvider;
            this.aofTaskStore = aofTaskStore;
            this.localNodeId = localNodeId;
            this.remoteNodeId = remoteNodeId;
            this.logger = logger;
            this.garnetClient = garnetClient;
            this.startAddress = startAddress;
            previousAddress = startAddress;
            cts = new CancellationTokenSource();
        }

        public void Dispose()
        {
            // First cancel the token
            cts?.Cancel();

            // Then, dispose the iterator. This will also signal the iterator so that it can observe the canceled token
            iter?.Dispose();

            // Finally, dispose the cts
            cts?.Dispose();
        }

        public unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected)
        {
            try
            {
                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Aof_Sync_Task_Consume);

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
            // Trigger flush while we are out of epoch protection
            garnetClient.CompletePending(false);
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
                if (!IsConnected) garnetClient.Connect();

                iter = clusterProvider.storeWrapper.appendOnlyFile.ScanSingle(startAddress, long.MaxValue, scanUncommitted: true, recover: false, logger: logger);

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