// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.Intrinsics.X86;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
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
        readonly ILogger logger = logger;
        private readonly Channel<ReplayRecord> replayChannel = Channel.CreateUnbounded<ReplayRecord>(
            new() { SingleWriter = true, SingleReader = true, AllowSynchronousContinuations = false });

        /// <summary>
        /// Asynchronously replays log entries using SemaphoreSlim coordination, processing and applying them for replication
        /// and consistency across sublogs.
        /// </summary>
        /// <returns>A task representing the asynchronous replay operation.</returns>
        internal async Task ContinuousBackgroundReplay()
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
                    logger?.LogError(ex, "{method} failed at WaitAsync", nameof(ContinuousBackgroundReplay));
                    cts.Cancel();
                    break;
                }

                unsafe
                {
                    var record = replayBatchContext.Record;
                    var recordLength = replayBatchContext.RecordLength;
                    var currentAddress = replayBatchContext.CurrentAddress;
                    var nextAddress = replayBatchContext.NextAddress;
                    var isProtected = replayBatchContext.IsProtected;
                    var ptr = record;

                    var maxSequenceNumber = 0L;
                    try
                    {
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
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "{method} failed at replaying", nameof(ContinuousBackgroundReplay));
                        cts.Cancel();
                        break;
                    }
                    finally
                    {
                        // Signal work completion after processing
                        replayBatchContext.LeaderFollowerBarrier.SignalCompleted();
                    }
                }
            }
        }

        public void AddRecord(ReplayRecord replayRecord)
        {
            replayChannel.Writer.TryWrite(replayRecord);
        }

        internal async Task ChannelBackgroundReplay()
        {
            var physicalSublogIdx = replayDriver.physicalSublogIdx;
            var virtualSublogIdx = appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, replayTaskIdx);
            var reader = replayChannel.Reader;

            while (await reader.WaitToReadAsync(cts.Token))
            {
                ProcessRecord(virtualSublogIdx, physicalSublogIdx);
                //ProcessRecordWithPrefetch(virtualSublogIdx, physicalSublogIdx);
            }
        }

        internal unsafe void ProcessRecord(int virtualSublogIdx, int physicalSublogIdx)
        {
            const int PrefetchSize = 4;
            var reader = replayChannel.Reader;
            var prefetchBuffer = stackalloc ReplayRecord[PrefetchSize];

            while (reader.TryRead(out var record))
            {
                try
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
                finally
                {
                    _ = replayDriver.batchWorkerMonitor.Exit();
                }
            }
        }

        internal unsafe void ProcessRecordWithPrefetch(int virtualSublogIdx, int physicalSublogIdx)
        {
            const int PrefetchSize = 4;
            var reader = replayChannel.Reader;
            var prefetchBuffer = stackalloc ReplayRecord[PrefetchSize];

            while (true)
            {
                // Read a batch of records and prefetch their entry pointers
                var count = 0;
                while (count < PrefetchSize && reader.TryRead(out prefetchBuffer[count]))
                {
                    if (Sse.IsSupported)
                        Sse.Prefetch0(prefetchBuffer[count].entryPtr);
                    count++;
                }

                if (count == 0)
                    break;

                // Process all prefetched records
                for (var i = 0; i < count; i++)
                {
                    try
                    {
                        replicationManager.AofProcessor.ProcessAofRecordInternal(virtualSublogIdx, prefetchBuffer[i].entryPtr, prefetchBuffer[i].payloadLength, true, out var isCheckpointStart);

                        // Encountered checkpoint start marker, log the ReplicationCheckpointStartOffset so we know the correct AOF truncation
                        // point when we take a checkpoint at the checkpoint end marker
                        if (isCheckpointStart)
                        {
                            // logger?.LogError("[{sublogIdx}] CheckpointStart {address}", sublogIdx, clusterProvider.replicationManager.GetSublogReplicationOffset(sublogIdx));
                            replicationManager.ReplicationCheckpointStartOffset[physicalSublogIdx] = replicationManager.GetSublogReplicationOffset(physicalSublogIdx);
                        }
                    }
                    finally
                    {
                        _ = replayDriver.batchWorkerMonitor.Exit();
                    }
                }
            }
        }

    }
}