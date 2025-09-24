
using System;
using System.Net;
using System.Threading;
using Tsavorite.core;
using Garnet.client;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class AofSyncDriver : IDisposable
    {
        public class AofSyncTask : IBulkLogEntryConsumer, IDisposable
        {
            readonly AofSyncDriver aofSyncDriver;
            readonly int taskId;
            public readonly GarnetClientSession garnetClient;
            readonly CancellationTokenSource cts;
            readonly long startAddress;
            TsavoriteLogScanSingleIterator iter;
            long previousAddress;

            /// <summary>
            /// Return start address for this AofSyncTask
            /// </summary>
            public long StartAddress => startAddress;

            /// <summary>
            /// Return previous address for this AofSyncTask
            /// </summary>
            public long PreviousAddress => previousAddress;

            /// <summary>
            /// Check if client connection is healthy
            /// </summary>
            public bool IsConnected => garnetClient != null && garnetClient.IsConnected;

            public AofSyncTask(
                AofSyncDriver aofSyncDriver,
                int taskId,
                IPEndPoint endPoint,
                long startAddress,
                CancellationTokenSource cts)
            {
                this.aofSyncDriver = aofSyncDriver;
                this.taskId = taskId;
                this.startAddress = startAddress;
                previousAddress = startAddress;
                this.cts = cts;
                garnetClient = new GarnetClientSession(
                            endPoint,
                            aofSyncDriver.clusterProvider.replicationManager.GetAofSyncNetworkBufferSettings,
                            aofSyncDriver.clusterProvider.replicationManager.GetNetworkPool,
                            tlsOptions: aofSyncDriver.clusterProvider.serverOptions.TlsOptions?.TlsClientOptions,
                            authUsername: aofSyncDriver.clusterProvider.ClusterUsername,
                            authPassword: aofSyncDriver.clusterProvider.ClusterPassword,
                            logger: aofSyncDriver.logger);
            }

            public void Dispose()
            {
                // Then, dispose the iterator. This will also signal the iterator so that it can observe the canceled token
                iter?.Dispose();
                iter = null;

                // Dispose GarnetClient
                garnetClient?.Dispose();
            }

            public unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected)
            {
                try
                {
                    ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Aof_Sync_Task_Consume);

                    // logger?.LogInformation("Sending {payloadLength} bytes to {remoteNodeId} at address {currentAddress}-{nextAddress}", payloadLength, remoteNodeId, currentAddress, nextAddress);

                    // This is called under epoch protection, so we have to wait for appending to complete
                    garnetClient.ExecuteClusterAppendLog(
                        aofSyncDriver.localNodeId,
                        previousAddress,
                        currentAddress,
                        nextAddress,
                        (long)payloadPtr,
                        payloadLength);

                    // Set task address to nextAddress, as the iterator is currently at nextAddress
                    // (records at currentAddress are already sent above)
                    previousAddress = nextAddress;
                }
                catch (Exception ex)
                {
                    aofSyncDriver.logger?.LogWarning(
                        ex,
                        "{Consume}[{taskId}]: exception consuming AOF payload to sync {remoteNodeId} ({currenAddress}, {nextAddress})",
                        nameof(AofSyncTask.Consume),
                        taskId,
                        aofSyncDriver.remoteNodeId,
                        currentAddress,
                        nextAddress);
                    throw;
                }
            }

            public void Throttle()
            {
                // Trigger flush while we are out of epoch protection
                garnetClient.CompletePending(false);
                garnetClient.Throttle();
            }

            public async Task RunAofSyncTask()
            {
                aofSyncDriver.logger?.LogInformation(
                    "{RunAofSyncTask}[{taskId}]: syncing {remoteNodeId} starting from address {address}",
                    nameof(AofSyncTask.RunAofSyncTask),
                    taskId,
                    aofSyncDriver.remoteNodeId,
                    startAddress);

                if (!IsConnected) garnetClient.Connect();

                iter = aofSyncDriver.clusterProvider.storeWrapper.appendOnlyFile.ScanSingle(startAddress, long.MaxValue, scanUncommitted: true, recover: false, logger: aofSyncDriver.logger);

                while (true)
                {
                    if (cts.Token.IsCancellationRequested) break;
                    await iter.BulkConsumeAllAsync(this, aofSyncDriver.clusterProvider.serverOptions.ReplicaSyncDelayMs, maxChunkSize: 1 << 20, cts.Token);
                }
            }
        }
    }
}