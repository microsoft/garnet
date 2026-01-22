// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class AofSyncDriver : IDisposable
    {
        public class AofSyncTask : IBulkLogEntryConsumer, IDisposable
        {
            readonly ClusterProvider clusterProvider;
            readonly int physicalSublogIdx;
            public readonly GarnetClientSession garnetClient;
            readonly string localNodeId;
            readonly string remoteNodeId;
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

            /// <summary>
            /// Logger instance
            /// </summary>
            readonly ILogger logger;

            /// <summary>
            /// AofSyncTask constructor
            /// </summary>
            /// <param name="clusterProvider"></param>
            /// <param name="physicalSublogIdx"></param>
            /// <param name="endPoint"></param>
            /// <param name="startAddress"></param>
            /// <param name="localNodeId"></param>
            /// <param name="remoteNodeId"></param>
            /// <param name="cts"></param>
            /// <param name="logger"></param>
            public AofSyncTask(
                ClusterProvider clusterProvider,
                int physicalSublogIdx,
                IPEndPoint endPoint,
                long startAddress,
                string localNodeId,
                string remoteNodeId,
                CancellationTokenSource cts,
                ILogger logger)
            {
                this.clusterProvider = clusterProvider;
                this.physicalSublogIdx = physicalSublogIdx;
                this.startAddress = startAddress;
                previousAddress = startAddress;
                this.localNodeId = localNodeId;
                this.remoteNodeId = remoteNodeId;
                this.cts = cts;
                garnetClient = new GarnetClientSession(
                            endPoint,
                            this.clusterProvider.replicationManager.GetAofSyncNetworkBufferSettings,
                            this.clusterProvider.replicationManager.GetNetworkPool,
                            tlsOptions: this.clusterProvider.serverOptions.TlsOptions?.TlsClientOptions,
                            authUsername: this.clusterProvider.ClusterUsername,
                            authPassword: this.clusterProvider.ClusterPassword,
                            logger: logger);
                this.logger = logger;
            }

            public void Dispose()
            {
                try
                {
                    // Dispose GarnetClient
                    garnetClient?.Dispose();
                }
                catch { }

                try
                {
                    // This forces the background sync task to stop,
                    // unless the cancelled cts already signaled it to stop
                    iter?.Dispose();
                    iter = null;
                }
                catch { }
            }

            /// <summary>
            /// Consume AOF records generated at the primary
            /// </summary>
            /// <param name="payloadPtr"></param>
            /// <param name="payloadLength"></param>
            /// <param name="currentAddress"></param>
            /// <param name="nextAddress"></param>
            /// <param name="isProtected"></param>
            public unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected)
            {
                try
                {
                    ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Aof_Sync_Task_Consume);

                    // logger?.LogInformation("Sending {payloadLength} bytes to {remoteNodeId} at address {currentAddress}-{nextAddress}", payloadLength, remoteNodeId, currentAddress, nextAddress);

                    // This is called under epoch protection, so we have to wait for appending to complete
                    garnetClient.ExecuteClusterAppendLog(
                        localNodeId,
                        physicalSublogIdx,
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
                    logger?.LogWarning(
                        ex,
                        "{Consume}[{taskId}]: exception consuming AOF payload to sync {remoteNodeId} ({currenAddress}, {nextAddress})",
                        nameof(AofSyncTask.Consume),
                        physicalSublogIdx,
                        remoteNodeId,
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

            public async Task RunAofSyncTask(AofSyncDriver aofSyncDriver)
            {
                var acquireReadLock = false;
                try
                {
                    acquireReadLock = aofSyncDriver.ResumeAofStreaming();
                    if (!acquireReadLock)
                        throw new GarnetException($"[{physicalSublogIdx}] Failed to acquire lock at {nameof(RunAofSyncTask)}");

                    logger?.LogInformation(
                        "{RunAofSyncTask}[{taskId}]: syncing {remoteNodeId} starting from address {address}",
                        nameof(AofSyncTask.RunAofSyncTask),
                        physicalSublogIdx,
                        aofSyncDriver.remoteNodeId,
                        startAddress);

                    if (!IsConnected)
                        garnetClient.Connect();

                    iter = clusterProvider.storeWrapper.appendOnlyFile.ScanSingle(physicalSublogIdx, startAddress, long.MaxValue, scanUncommitted: true, recover: false, logger: logger);
                    // Send ping to initialize replication stream
                    garnetClient.ExecuteClusterAppendLog(aofSyncDriver.localNodeId, physicalSublogIdx, -1, -1, -1, -1, 0);
                    garnetClient.CompletePending(false);

                    while (true)
                    {
                        if (cts.Token.IsCancellationRequested) break;
                        await iter.BulkConsumeAllAsync(this, aofSyncDriver.clusterProvider.serverOptions.ReplicaSyncDelayMs, maxChunkSize: 1 << 20, cts.Token);
                    }
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "[{sublogIdx}]({method})", physicalSublogIdx, nameof(RunAofSyncTask));
                }
                finally
                {
                    if (acquireReadLock)
                        aofSyncDriver.SuspendAofStreaming();
                }
            }
        }
    }
}