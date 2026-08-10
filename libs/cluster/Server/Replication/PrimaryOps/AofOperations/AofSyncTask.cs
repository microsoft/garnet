// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
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

            readonly bool timePulseEnabled;
            readonly GarnetAppendOnlyFile appendOnlyFile;
            readonly TsavoriteLog physicalSublog;
            readonly long[] pulseTailSnapshot;
            readonly long[] pulseTailScratch;
            long lastAdvanceTimePulse;

            // Byte-progress gate for refreshing the backpressure shipped watermark: republish only
            // after shipping publishDeltaBytes since the last publish. previousAddress advances only
            // when a chunk ships, so a caught-up (idle) sublog never publishes, and a busy one
            // batches many chunks per publish. With backpressure disabled the publish block is
            // skipped entirely.
            readonly AofSyncDriverStore aofSyncDriverStore;
            readonly AofBackpressure backpressure;
            long lastPublishedShippedAddress;

            // Monotonic high-water of the address this task has shipped/scanned past, published to
            // the backpressure gate. Folds the iterator's scan position (which advances even across
            // null-device skips of evicted, unshippable pages) with previousAddress, so it reaches
            // the tail when the shipper is caught up. Read cross-thread by the driver-store min, so
            // access via Volatile.
            long shippedWatermarkAddress;
            // Shipped high-water observed at the previous Throttle, used to detect an idle
            // (caught-up) shipper so the final watermark is published even when previousAddress
            // stops advancing.
            long lastThrottleShipped;

            /// <summary>
            /// Return start address for this AofSyncTask
            /// </summary>
            public long StartAddress => startAddress;

            /// <summary>
            /// Return previous address for this AofSyncTask
            /// </summary>
            public long PreviousAddress => previousAddress;

            /// <summary>
            /// Monotonic high-water address this task has shipped/scanned past, published to the
            /// backpressure gate as this replica's shipped watermark for the sublog.
            /// </summary>
            public long ShippedWatermarkAddress => Volatile.Read(ref shippedWatermarkAddress);

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
            /// <param name="aofSyncDriverStore"></param>
            /// <param name="physicalSublogIdx"></param>
            /// <param name="endPoint"></param>
            /// <param name="startAddress"></param>
            /// <param name="localNodeId"></param>
            /// <param name="remoteNodeId"></param>
            /// <param name="cts"></param>
            /// <param name="logger"></param>
            public AofSyncTask(
                ClusterProvider clusterProvider,
                AofSyncDriverStore aofSyncDriverStore,
                int physicalSublogIdx,
                IPEndPoint endPoint,
                long startAddress,
                string localNodeId,
                string remoteNodeId,
                CancellationTokenSource cts,
                ILogger logger)
            {
                var currentConfig = clusterProvider.clusterManager.CurrentConfig;
                this.clusterProvider = clusterProvider;
                this.aofSyncDriverStore = aofSyncDriverStore;
                this.physicalSublogIdx = physicalSublogIdx;
                this.startAddress = startAddress;
                previousAddress = startAddress;
                this.localNodeId = localNodeId;
                this.remoteNodeId = remoteNodeId;
                this.cts = cts;
                appendOnlyFile = clusterProvider.storeWrapper.appendOnlyFile;
                timePulseEnabled = clusterProvider.serverOptions.MultiLogEnabled;
                if (timePulseEnabled)
                {
                    physicalSublog = appendOnlyFile.Log.GetSubLog(physicalSublogIdx);
                    pulseTailSnapshot = new long[clusterProvider.serverOptions.AofPhysicalSublogCount];
                    pulseTailScratch = new long[clusterProvider.serverOptions.AofPhysicalSublogCount];
                    Array.Fill(pulseTailSnapshot, -1L);
                }
                backpressure = appendOnlyFile?.backpressure;
                if (backpressure != null)
                {
                    // Seed the shipped-watermark bookkeeping unconditionally when a gate exists, so the
                    // baseline is correct if the gate is enabled at runtime after this task started.
                    lastPublishedShippedAddress = startAddress;
                    shippedWatermarkAddress = startAddress;
                    lastThrottleShipped = startAddress;
                }
                garnetClient = new GarnetClientSession(
                            endPoint,
                            this.clusterProvider.replicationManager.GetAofSyncNetworkBufferSettings,
                            this.clusterProvider.replicationManager.GetNetworkPool,
                            tlsOptions: this.clusterProvider.serverOptions.TlsOptions?.TlsClientOptions,
                            authUsername: this.clusterProvider.ClusterUsername,
                            authPassword: this.clusterProvider.ClusterPassword,
                            clientName: $"AofSyncTask-{physicalSublogIdx}:({currentConfig.LocalNodeEndpoint})",
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
                    lastAdvanceTimePulse = Environment.TickCount64;
                }
                catch (Exception ex)
                {
                    logger?.LogError(
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
                cts.Token.ThrowIfCancellationRequested();

                if (!garnetClient.IsConnected)
                    ExceptionUtils.ThrowException(new GarnetException($"AOF stream client disconnected! [{physicalSublogIdx}]:({startAddress},{previousAddress})"));

                // Trigger flush while we are out of epoch protection
                garnetClient.CompletePending(false);
                garnetClient.Throttle();

                // Refresh the shipped watermark in the backpressure gate outside epoch protection.
                // The shipped high-water folds the iterator's scan position with previousAddress:
                // previousAddress advances only in Consume, but a null-device skip advances the
                // iterator (past evicted, unshippable pages) without a Consume, so previousAddress
                // alone can freeze below the tail while the shipper idle-spins near it. Publish
                // either after shipping publishDeltaBytes (busy path: bound coherence traffic) or
                // once the shipper stops advancing (idle path: land the final watermark). Without
                // the idle publish the watermark ratchets one-way and a caught-up shipper never
                // republishes, deadlocking appenders parked in the gate. A stale watermark is safe
                // because appenders self-check conservatively.
                if (backpressure != null && backpressure.Enabled)
                {
                    var iterNext = iter?.NextAddress ?? previousAddress;
                    var shipped = previousAddress > iterNext ? previousAddress : iterNext;
                    if (shipped > shippedWatermarkAddress)
                        Volatile.Write(ref shippedWatermarkAddress, shipped);

                    var pending = shippedWatermarkAddress - lastPublishedShippedAddress;
                    var idle = shippedWatermarkAddress == lastThrottleShipped;
                    if (pending > 0 && (pending >= backpressure.PublishDeltaBytes || idle))
                    {
                        lastPublishedShippedAddress = shippedWatermarkAddress;
                        aofSyncDriverStore.PublishShippedAddress(physicalSublogIdx);
                    }
                    lastThrottleShipped = shippedWatermarkAddress;
                }

                SendAdvanceTimePulse();
            }

            void SendAdvanceTimePulse()
            {
                if (!timePulseEnabled || iter == null)
                    return;

                var now = Environment.TickCount64;
                if (now - lastAdvanceTimePulse < clusterProvider.serverOptions.AofTailWitnessFreqMs)
                    return;

                var anyTailMoved = false;
                for (var i = 0; i < pulseTailScratch.Length; i++)
                {
                    pulseTailScratch[i] = appendOnlyFile.Log.GetTailAddress(i);
                    anyTailMoved |= pulseTailScratch[i] != pulseTailSnapshot[i];
                }

                if (!anyTailMoved)
                {
                    // No tail moved anywhere. Normally there is nothing to witness, so stay silent.
                    // The exception is an active backpressure stall: appends are frozen (hence the
                    // still tails) while a replica may be parked at a replay-align round waiting for
                    // an idle sublog to arrive. Keep emitting a heartbeat pulse so that idle sublog
                    // arrives, the round completes, replay drains, and the stall lifts -- otherwise
                    // the frozen tail and the parked round deadlock until ReplicaSyncTimeout. The
                    // stall check is a global OR across sublogs (matching the all-sublog tail scan):
                    // an idle sublog that is not itself stalled must still pulse to release a
                    // cross-sublog barrier stalled behind a different sublog.
                    if (!(backpressure?.AnyStalled() ?? false))
                    {
                        lastAdvanceTimePulse = now;
                        return;
                    }
                }

                var sequenceNumber = appendOnlyFile.GetLargerThanMaximumSequenceNumber();
                if (iter.NextAddress < physicalSublog.TailAddress)
                {
                    lastAdvanceTimePulse = now;
                    return;
                }

                garnetClient.ExecuteClusterAdvanceTime(physicalSublogIdx, sequenceNumber);
                garnetClient.CompletePending(false);

                for (var i = 0; i < pulseTailSnapshot.Length; i++)
                    pulseTailSnapshot[i] = pulseTailScratch[i];
                lastAdvanceTimePulse = now;
            }

            /// <summary>
            /// This does a direct copy of the AOF records from the primary to the replica, starting from startAddress. We don't deserialize anything;
            /// it is a vein-to-vein transfusion of records that we do not otherwise operate on.
            /// </summary>
            /// <param name="aofSyncDriver"></param>
            /// <returns></returns>
            public async Task RunAofSyncTaskAsync(AofSyncDriver aofSyncDriver)
            {
                var enteredMonitor = false;
                try
                {
                    enteredMonitor = aofSyncDriver.activeWorkerMonitor.TryEnter();
                    if (!enteredMonitor)
                        ExceptionUtils.ThrowException(new GarnetException($"[{physicalSublogIdx}] Failed to acquire lock at {nameof(RunAofSyncTaskAsync)}"));

                    logger?.LogInformation(
                        "{RunAofSyncTask}[{taskId}]: syncing {remoteNodeId} starting from address {address}",
                        nameof(RunAofSyncTaskAsync),
                        physicalSublogIdx,
                        remoteNodeId,
                        startAddress);

                    if (!IsConnected)
                        await garnetClient.ConnectAsync((int)clusterProvider.serverOptions.ReplicaSyncTimeout.TotalMilliseconds, cts.Token).ConfigureAwait(false);

                    LogRunAofSyncTask(physicalSublogIdx, startAddress, previousAddress, logger);

                    iter = clusterProvider.storeWrapper.appendOnlyFile.Log.ScanSingle(physicalSublogIdx, startAddress, long.MaxValue, scanUncommitted: true, recover: false, logger: logger);

                    // Send ping to initialize replication stream
                    var resp = await garnetClient.ExecuteClusterAppendLogInit(localNodeId, physicalSublogIdx, -1, -1, -1);
                    if (!resp.Equals("OK"))
                        throw new GarnetException("Failed to initialize AofSync stream!");
                    lastAdvanceTimePulse = Environment.TickCount64;

                    await iter.BulkConsumeAllAsync(
                        this,
                        clusterProvider.storeWrapper.runtimeConfig.GetInt(ServerConfigType.REPLICA_SYNC_DELAY),
                        maxChunkSize: 1 << 20,
                        cts.Token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "[{sublogIdx}]({method})", physicalSublogIdx, nameof(RunAofSyncTaskAsync));
                }
                finally
                {
                    if (enteredMonitor)
                        _ = aofSyncDriver.activeWorkerMonitor.Exit();
                    garnetClient?.Dispose();
                }

                [Conditional("DEBUG")]
                static void LogRunAofSyncTask(int physicalSublogIdx, long startAddress, long previousAddress, ILogger logger)
                {
                    var state = new GarnetTestLoggingEvent()
                    {
                        Type = GarnetTestLoggingEventType.LogRunAofSyncTask,
                        Message = $"physicalSublogIdx:{physicalSublogIdx}, startAddress: {startAddress}, previousAddress: {previousAddress}",
                    };

                    logger?.LogTesting(state);
                }
            }
        }
    }
}