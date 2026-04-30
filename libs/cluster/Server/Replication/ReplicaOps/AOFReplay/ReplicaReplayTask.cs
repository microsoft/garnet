// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    [StructLayout(LayoutKind.Sequential, Size = 12)]
    internal unsafe struct ReplayRecord
    {
        public byte* entryPtr;
        public int payloadLength;
    }

    internal sealed class ReplicaReplayTask(
        int replayIdx,
        ReplicaReplayDriver replayDriver,
        ClusterProvider clusterProvider,
        CancellationTokenSource cts,
        ILogger logger = null)
    {
        readonly int replayTaskIdx = replayIdx;
        readonly ReplicaReplayDriver replayDriver = replayDriver;
        readonly ReplicationManager replicationManager = clusterProvider.replicationManager;
        readonly GarnetAppendOnlyFile appendOnlyFile = clusterProvider.storeWrapper.appendOnlyFile;
        readonly ReplayBatchContext replayBatchContext = replayDriver.replayBatchContext;
        readonly CancellationTokenSource cts = cts;
        readonly TsavoriteLog replaySublog = clusterProvider.storeWrapper.appendOnlyFile.Log.GetSubLog(replayDriver.physicalSublogIdx);
        readonly ActiveWorkerMonitor activeWorkerMonitor = new();
        private readonly Channel<ReplayRecord> replayChannel = Channel.CreateUnbounded<ReplayRecord>(
            new() { SingleWriter = true, SingleReader = true, AllowSynchronousContinuations = false });
        readonly ILogger logger = logger;

        /// <summary>
        /// Add record for replay
        /// </summary>
        /// <param name="replayRecord"></param>
        public void AddRecord(ReplayRecord replayRecord)
            => replayChannel.Writer.TryWrite(replayRecord);

        /// <summary>
        /// Asynchronously replays log entries using SemaphoreSlim coordination, processing and applying them for replication
        /// and consistency across sublogs.
        /// </summary>
        /// <returns>A task representing the asynchronous replay operation.</returns>
        internal async Task FullPageBasedBackgroundReplayAsync()
        {
            var physicalSublogIdx = replayDriver.physicalSublogIdx;
            var virtualSublogIdx = appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, replayTaskIdx);

            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    await replayBatchContext.LeaderFollowerBarrier.WaitReadyWorkAsync(cancellationToken: cts.Token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} failed at WaitAsync", nameof(FullPageBasedBackgroundReplayAsync));
                    await cts.CancelAsync().ConfigureAwait(false);
                    break;
                }

                try
                {
                    unsafe
                    {
                        var record = replayBatchContext.Record;
                        var recordLength = replayBatchContext.RecordLength;
                        var currentAddress = replayBatchContext.CurrentAddress;
                        var nextAddress = replayBatchContext.NextAddress;
                        var isProtected = replayBatchContext.IsProtected;
                        var ptr = record;

                        var maxSequenceNumber = 0L;

                        // logger?.LogError("[{sublogIdx},{replayIdx}] = {currentAddress} -> {nextAddress}", sublogIdx, replayIdx, currentAddress, nextAddress);                        
                        while (ptr < record + recordLength)
                        {
                            cts.Token.ThrowIfCancellationRequested();
                            var entryLength = appendOnlyFile.HeaderSize;
                            var payloadLength = replaySublog.UnsafeGetLength(ptr);
                            if (payloadLength > 0)
                            {
                                var entryPtr = ptr + entryLength;
                                if (replicationManager.AofProcessor.CanReplay(entryPtr, replayTaskIdx, out var sequenceNumber))
                                {
                                    replicationManager.AofProcessor.ProcessAofRecordInternal(virtualSublogIdx, entryPtr, payloadLength, true, out var isCheckpointStart);
                                    // Encountered checkpoint start marker, log the ReplicationCheckpointStartOffset so we know the correct AOF truncation
                                    // point when we take a checkpoint at the checkpoint end marker
                                    if (isCheckpointStart)
                                    {
                                        // logger?.LogError("[{sublogIdx}] CheckpointStart {address}", sublogIdx, clusterProvider.replicationManager.GetSublogReplicationOffset(sublogIdx));
                                        replicationManager.ReplicationCheckpointStartOffset[physicalSublogIdx] = replicationManager.GetSublogReplicationOffset(physicalSublogIdx);
                                    }
                                }
                                maxSequenceNumber = Math.Max(sequenceNumber, maxSequenceNumber);
                                entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
                            }
                            else if (payloadLength < 0)
                            {
                                if (!clusterProvider.serverOptions.EnableFastCommit)
                                    throw new GarnetException("Received FastCommit request at replica AOF processor, but FastCommit is not enabled", clientResponse: false);

                                // Only a single thread should commit metadata
                                if (replayTaskIdx == 0)
                                {
                                    TsavoriteLogRecoveryInfo info = new();
                                    info.Initialize(new ReadOnlySpan<byte>(ptr + entryLength, -payloadLength));
                                    replaySublog.UnsafeCommitMetadataOnly(info, isProtected);
                                }
                                entryLength += TsavoriteLog.UnsafeAlign(-payloadLength);
                            }
                            ptr += entryLength;
                        }

                        // Update max sequence number for this virtual sublog which is mapped
                        appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, maxSequenceNumber);
                    }
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} failed at replaying", nameof(FullPageBasedBackgroundReplayAsync));
                    await cts.CancelAsync().ConfigureAwait(false);
                    break;
                }
                finally
                {
                    // Signal work completion after processing
                    replayBatchContext.LeaderFollowerBarrier.SignalCompleted();
                }
            }
        }

        /// <summary>
        /// Asynchronously processes records from the replay channel in the background.
        /// </summary>
        /// <returns>A task that represents the asynchronous replay operation.</returns>
        internal async Task ChannelBasedBackgroundReplayAsync()
        {
            var physicalSublogIdx = replayDriver.physicalSublogIdx;
            var virtualSublogIdx = appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, replayTaskIdx);
            var reader = replayChannel.Reader;

            while (await reader.WaitToReadAsync(cts.Token).ConfigureAwait(false))
            {
                while (reader.TryRead(out var record))
                {
                    unsafe
                    {
                        replicationManager.AofProcessor.ProcessAofRecordInternal(virtualSublogIdx, record.entryPtr, record.payloadLength, true, out var isCheckpointStart);

                        // Encountered checkpoint start marker, log the ReplicationCheckpointStartOffset so we know the correct AOF truncation
                        // point when we take a checkpoint at the checkpoint end marker
                        if (isCheckpointStart)
                        {
                            // logger?.LogError("[{sublogIdx}] CheckpointStart {address}", sublogIdx, clusterProvider.replicationManager.GetSublogReplicationOffset(sublogIdx));
                            replicationManager.ReplicationCheckpointStartOffset[physicalSublogIdx] = replicationManager.GetSublogReplicationOffset(physicalSublogIdx);
                        }
                    }
                }

                // Signal work completion after processing
                replayBatchContext.LeaderFollowerBarrier.SignalCompleted();
            }
        }
    }
}