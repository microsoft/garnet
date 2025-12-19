// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal unsafe class ReplayWorkItem(int replayTasks)
    {
        public byte* record;
        public int recordLength;
        public long currentAddress;
        public long nextAddress;
        public bool isProtected;
        public ManualResetEventSlim Completed = new(true);
        public EventBarrier eventBarrier = new(replayTasks);

        public void Reset()
        {
            Completed.Reset();
            eventBarrier.Reset();
        }
    }

    internal sealed class ReplicaReplayTask(int replayIdx, ReplicaReplayDriver replayDriver, ClusterProvider clusterProvider, CancellationTokenSource cts, ILogger logger = null)
    {
        readonly int replayTaskIdx = replayIdx;
        readonly GarnetServerOptions serverOptions = clusterProvider.serverOptions;
        readonly ReplicaReplayDriver replayDriver = replayDriver;
        readonly ReplicationManager replicationManager = clusterProvider.replicationManager;
        readonly GarnetAppendOnlyFile appendOnlyFile = clusterProvider.storeWrapper.appendOnlyFile;
        readonly Channel<ReplayWorkItem> channel = Channel.CreateUnbounded<ReplayWorkItem>(new() { SingleWriter = true, SingleReader = false, AllowSynchronousContinuations = false });
        readonly CancellationTokenSource cts = cts;
        readonly TsavoriteLog replaySublog = clusterProvider.storeWrapper.appendOnlyFile.Log.GetSubLog(replayDriver.sublogIdx);
        readonly ILogger logger = logger;

        internal void Append(ReplayWorkItem item)
        {
            if (!channel.Writer.TryWrite(item))
                throw new GarnetException("Failed to append to channel");
        }

        internal async Task Replay()
        {
            var physicalSublogIdx = replayDriver.sublogIdx;
            var virtualSublogIdx = appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, replayTaskIdx);
            var reader = channel.Reader;
            await foreach (var entry in reader.ReadAllAsync(cts.Token))
            {
                unsafe
                {
                    var record = entry.record;
                    var recordLength = entry.recordLength;
                    var currentAddress = entry.currentAddress;
                    var nextAddress = entry.nextAddress;
                    var isProtected = entry.isProtected;
                    var ptr = record;

                    if (replayTaskIdx == 0)
                        replicationManager.SetSublogReplicationOffset(physicalSublogIdx, currentAddress);

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
                                if (replicationManager.AofProcessor.ShouldReplay(entryPtr, replayTaskIdx))
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
                                entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
                            }
                            else if (payloadLength < 0)
                            {
                                if (!clusterProvider.serverOptions.EnableFastCommit)
                                    throw new GarnetException("Received FastCommit request at replica AOF processor, but FastCommit is not enabled", clientResponse: false);
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
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "{method}", nameof(Replay));
                        cts.Cancel();
                    }

                    var eventBarrier = entry.eventBarrier;
                    try
                    {
                        var isLeader = eventBarrier.TrySignalAndWait(out var signalException, serverOptions.ReplicaSyncTimeout, cts.Token);
                        if (isLeader)
                        {
                            // Update key sequence tracker after everyone replayed their portion
                            // NOTE:
                            //      We need to force an update here to ensure prefix consistency across virtual sublogs                            
                            // PhysicalSublog:
                            //      ReplayTask1 (Virtual sublog): [(A,1) ...  (A,2)]
                            //      ReplayTask2 (Virtual sublog): [ ... (B,3) (C,4)]
                            appendOnlyFile.replicaReadConsistencyStateManager.UpdateSublogMaxSequenceNumber(physicalSublogIdx);
                            // Update replication offset
                            replicationManager.SetSublogReplicationOffset(physicalSublogIdx, nextAddress);
                            eventBarrier.Release();
                        }
                    }
                    finally
                    {
                        // Ensure main thread always gets notified and releated
                        if (replayTaskIdx == 0)
                            entry.Completed.Set();
                    }
                }
            }
        }
    }
}