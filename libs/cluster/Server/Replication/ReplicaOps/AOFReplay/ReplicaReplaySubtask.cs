
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed class ReplicaReplaySubtask(
        int sublogIdx,
        int subtaskIdx,
        ReplicaReplayDriver replayDriver,
        ClusterProvider clusterProvider,
        INetworkSender respSessionNetworkSender,
        CancellationTokenSource cts,
        ILogger logger = null) : IBulkLogEntryConsumer, IDisposable
    {
        readonly int sublogIdx = sublogIdx;
        readonly int subtaskIdx = subtaskIdx;
        readonly ReplicaReplayDriver replayDriver = replayDriver;
        readonly ClusterProvider clusterProvider = clusterProvider;
        readonly CancellationTokenSource cts = cts;
        readonly INetworkSender respSessionNetworkSender = respSessionNetworkSender;
        readonly ILogger logger = logger;
        TsavoriteLogScanSingleIterator replayIterator = null;

        public void Dispose()
        {
            respSessionNetworkSender?.Dispose();
            replayIterator?.Dispose();            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ValidateSublogIndex(int sublogIdx)
        {
            if (sublogIdx != this.sublogIdx)
                throw new GarnetException($"SublogIdx mismatch; expected:{this.sublogIdx} - received:{sublogIdx}");
        }

        #region IBulkLogEntryConsumer
        public unsafe void Consume(byte* record, int recordLength, long currentAddress, long nextAddress, bool isProtected)
        {
            ValidateSublogIndex(sublogIdx);
            clusterProvider.replicationManager.SetSublogReplicationOffset(sublogIdx, currentAddress);
            var ptr = record;
            // logger?.LogError("[{sublogIdx}] = {currentAddress} -> {nextAddress}", sublogIdx, currentAddress, nextAddress);
            while (ptr < record + recordLength)
            {
                cts.Token.ThrowIfCancellationRequested();
                var entryLength = clusterProvider.storeWrapper.appendOnlyFile.HeaderSize;
                var payloadLength = clusterProvider.storeWrapper.appendOnlyFile.Log.GetSubLog(sublogIdx).UnsafeGetLength(ptr);
                if (payloadLength > 0)
                {
                    clusterProvider.replicationManager.AofProcessor.ProcessAofRecordInternal(sublogIdx, subtaskIdx, ptr + entryLength, payloadLength, true, out var isCheckpointStart);
                    // Encountered checkpoint start marker, log the ReplicationCheckpointStartOffset so we know the correct AOF truncation
                    // point when we take a checkpoint at the checkpoint end marker
                    if (isCheckpointStart)
                    {
                        // logger?.LogError("[{sublogIdx}] CheckpointStart {address}", sublogIdx, clusterProvider.replicationManager.GetSublogReplicationOffset(sublogIdx));
                        clusterProvider.replicationManager.ReplicationCheckpointStartOffset[sublogIdx] = clusterProvider.replicationManager.GetSublogReplicationOffset(sublogIdx);
                    }
                    entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
                }
                else if (payloadLength < 0)
                {
                    if (!clusterProvider.serverOptions.EnableFastCommit)
                    {
                        throw new GarnetException("Received FastCommit request at replica AOF processor, but FastCommit is not enabled", clientResponse: false);
                    }
                    TsavoriteLogRecoveryInfo info = new();
                    info.Initialize(new ReadOnlySpan<byte>(ptr + entryLength, -payloadLength));
                    clusterProvider.storeWrapper.appendOnlyFile.Log.GetSubLog(sublogIdx).UnsafeCommitMetadataOnly(info, isProtected);
                    entryLength += TsavoriteLog.UnsafeAlign(-payloadLength);
                }
                ptr += entryLength;
                clusterProvider.replicationManager.IncrementSublogReplicationOffset(sublogIdx, entryLength);
            }
            // logger?.LogError("[{sublogIdx}] = {currentAddress} -> {nextAddress}", sublogIdx, currentAddress, nextAddress);

            if (clusterProvider.replicationManager.GetSublogReplicationOffset(sublogIdx) != nextAddress)
            {
                logger?.LogError("ReplicaReplayTask.Consume NextAddress Mismatch sublogIdx: {sublogIdx}; recordLength:{recordLength}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; replicationOffset:{ReplicationOffset}", sublogIdx, recordLength, currentAddress, nextAddress, clusterProvider.replicationManager.ReplicationOffset[sublogIdx]);
                throw new GarnetException("Failed validating integrity of replay", LogLevel.Warning, clientResponse: false);
            }
        }

        public void Throttle() { }
        #endregion

        internal void Run(long startAddress)
        {
            if (replayIterator == null)
            {
                replayIterator = clusterProvider.storeWrapper.appendOnlyFile.ScanSingle(sublogIdx, startAddress, long.MaxValue, scanUncommitted: true, recover: false, logger: logger);
                _ = Task.Run(BackgroundReplayTask);
            }

            async Task BackgroundReplayTask()
            {
                var readLock = replayDriver.ResumeReplay();
                try
                {
                    if (!readLock)
                        throw new GarnetException("Failed to acquire replayLock");
                    while (true)
                    {
                        cts.Token.ThrowIfCancellationRequested();
                        await replayIterator.BulkConsumeAllAsync(
                            this,
                            clusterProvider.serverOptions.ReplicaSyncDelayMs,
                            maxChunkSize: 1 << 20,
                            cts.Token);
                    }
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ReplicaReplayTask - terminating");
                }
                finally
                {
                    if (readLock)
                        replayDriver.SuspendReplay();
                }
            }
        }
    }
}