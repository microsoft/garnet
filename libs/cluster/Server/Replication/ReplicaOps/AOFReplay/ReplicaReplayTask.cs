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
    internal sealed class ReplicaReplayTask(int replayIdx, ReplicaReplayDriver replayDriver, ClusterProvider clusterProvider, CancellationTokenSource cts, ILogger logger = null)
    {
        readonly int replayTaskIdx = replayIdx;
        readonly GarnetServerOptions serverOptions = clusterProvider.serverOptions;
        readonly ReplicaReplayDriver replayDriver = replayDriver;
        readonly ReplicationManager replicationManager = clusterProvider.replicationManager;
        readonly GarnetAppendOnlyFile appendOnlyFile = clusterProvider.storeWrapper.appendOnlyFile;
        readonly Channel<ReplayRecordState> channel = Channel.CreateUnbounded<ReplayRecordState>(new() { SingleWriter = true, SingleReader = false, AllowSynchronousContinuations = false });
        readonly CancellationTokenSource cts = cts;
        readonly ILogger logger = logger;

        internal void Append(ReplayRecordState item)
        {
            if (!channel.Writer.TryWrite(item))
                throw new GarnetException("Failed to append to channel");
        }

        internal async Task Replay()
        {
            try
            {
                var sublogIdx = replayDriver.sublogIdx;
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
                            replicationManager.SetSublogReplicationOffset(sublogIdx, currentAddress);

                        // logger?.LogError("[{sublogIdx},{replayIdx}] = {currentAddress} -> {nextAddress}", sublogIdx, replayIdx, currentAddress, nextAddress);                        
                        while (ptr < record + recordLength)
                        {
                            cts.Token.ThrowIfCancellationRequested();
                            var entryLength = appendOnlyFile.HeaderSize;
                            var payloadLength = appendOnlyFile.Log.GetSubLog(sublogIdx).UnsafeGetLength(ptr);
                            if (payloadLength > 0)
                            {
                                replicationManager.AofProcessor.ProcessAofRecordInternal(sublogIdx, ptr + entryLength, payloadLength, true, out var isCheckpointStart);
                                // Encountered checkpoint start marker, log the ReplicationCheckpointStartOffset so we know the correct AOF truncation
                                // point when we take a checkpoint at the checkpoint end marker
                                if (isCheckpointStart)
                                {
                                    // logger?.LogError("[{sublogIdx}] CheckpointStart {address}", sublogIdx, clusterProvider.replicationManager.GetSublogReplicationOffset(sublogIdx));
                                    replicationManager.ReplicationCheckpointStartOffset[sublogIdx] = replicationManager.GetSublogReplicationOffset(sublogIdx);
                                }
                                entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
                            }
                            else if (payloadLength < 0 && replayTaskIdx == 0)
                            {
                                if (!clusterProvider.serverOptions.EnableFastCommit)
                                    throw new GarnetException("Received FastCommit request at replica AOF processor, but FastCommit is not enabled", clientResponse: false);
                                TsavoriteLogRecoveryInfo info = new();
                                info.Initialize(new ReadOnlySpan<byte>(ptr + entryLength, -payloadLength));
                                appendOnlyFile.Log.GetSubLog(sublogIdx).UnsafeCommitMetadataOnly(info, isProtected);
                                entryLength += TsavoriteLog.UnsafeAlign(-payloadLength);
                            }
                            ptr += entryLength;
                        }

                        var eventBarrier = entry.eventBarrier;
                        var isLeader = eventBarrier.TrySignalAndWait(out var signalException, serverOptions.ReplicaSyncTimeout);
                        if (isLeader)
                        {
                            // Update key sequence tracker after everyone replayed their portion
                            // NOTE:
                            //      We need to force an update here to ensure prefix consistency across virtual sublogs                            
                            // PhysicalSublog:
                            //      ReplayTask1 (Virtual sublog): [(A,1) ...  (A,2)]
                            //      ReplayTask2 (Virtual sublog): [ ... (B,3) (C,4)]
                            appendOnlyFile.replicaReadConsistencyStateManager.UpdateSublogMaxSequenceNumber(sublogIdx);
                            // Update replication offset
                            replicationManager.SetSublogReplicationOffset(sublogIdx, nextAddress);
                            eventBarrier.Release();
                            entry.Completed.Set();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "{method}", nameof(Replay));
                throw;
            }
        }
    }
}