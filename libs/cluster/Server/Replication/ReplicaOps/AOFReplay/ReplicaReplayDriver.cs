// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Replica main replay driver
    /// </summary>
    internal sealed class ReplicaReplayDriver : IBulkLogEntryConsumer, IDisposable
    {
        internal readonly int sublogIdx;
        readonly GarnetServerOptions serverOptions;
        readonly GarnetAppendOnlyFile appendOnlyFile;
        readonly ReplicationManager replicationManager;
        readonly CancellationTokenSource cts;
        readonly INetworkSender respSessionNetworkSender;
        readonly ILogger logger;
        TsavoriteLogScanSingleIterator replayIterator;
        SingleWriterMultiReaderLock activeReplay;
        const int ReplayBufferSize = 1 << 10;
        internal readonly ReplayRecordState[] replayBuffer;
        long replayBufferOffset = 0;
        readonly ReplicaReplayTask[] replayTasks;

        public ReplicaReplayDriver(int sublogIdx, ClusterProvider clusterProvider, INetworkSender respSessionNetworkSender, CancellationTokenSource cts, ILogger logger = null)
        {
            this.sublogIdx = sublogIdx;
            this.serverOptions = clusterProvider.serverOptions;
            this.appendOnlyFile = clusterProvider.storeWrapper.appendOnlyFile;
            this.replicationManager = clusterProvider.replicationManager;
            this.cts = cts;
            this.respSessionNetworkSender = respSessionNetworkSender;
            this.logger = logger;
            this.replayIterator = null;
            this.activeReplay = new SingleWriterMultiReaderLock();
            this.replayBuffer = [.. Enumerable.Range(0, ReplayBufferSize).Select(_ => new ReplayRecordState(clusterProvider.serverOptions.AofReplaySubtaskCount))];
            this.replayBufferOffset = 0;

            // Initialize background replay tasks for this sublog replay driver
            var replayTaskCount = serverOptions.AofReplaySubtaskCount;
            if (replayTaskCount > 1)
            {
                replayTasks = [.. Enumerable.Range(0, replayTaskCount).Select(i => new ReplicaReplayTask(i, this, clusterProvider, cts, logger))];
                foreach (var replayTask in replayTasks)
                    _ = Task.Run(() => replayTask.Replay());
            }
        }

        public bool ResumeReplay() => activeReplay.TryReadLock();

        public void SuspendReplay() => activeReplay.ReadUnlock();

        public void Dispose()
        {
            activeReplay.WriteLock();
            replayIterator?.Dispose();
            respSessionNetworkSender?.Dispose();
        }

        #region IBulkLogEntryConsumer
        public unsafe void Consume(byte* record, int recordLength, long currentAddress, long nextAddress, bool isProtected)
        {
            if (serverOptions.AofReplaySubtaskCount == 1)
            {
                ConsumeDirect(record, recordLength, currentAddress, nextAddress, isProtected);
            }
            else
            {
                var nextReplaySlot = replayBufferOffset % ReplayBufferSize;
                // Wait for slot to become available
                if (!replayBuffer[nextReplaySlot].Completed.Wait(serverOptions.ReplicaSyncTimeout, cts.Token))
                    throw new GarnetException("Consume background replay timed-out!");

                var replayBufferSlot = replayBuffer[nextReplaySlot];
                replayBufferSlot.record = record;
                replayBufferSlot.recordLength = recordLength;
                replayBufferSlot.currentAddress = currentAddress;
                replayBufferSlot.nextAddress = nextAddress;
                replayBufferSlot.isProtected = isProtected;
                replayBufferSlot.Reset();
                replayBufferOffset++;

                foreach (var replayTask in replayTasks)
                    replayTask.Append(replayBuffer[nextReplaySlot]);

                // Wait for replay to complete
                if (!replayBuffer[nextReplaySlot].Completed.Wait(serverOptions.ReplicaSyncTimeout, cts.Token))
                    throw new GarnetException("Consume background replay timed-out!");
            }
        }

        internal unsafe void ConsumeDirect(byte* record, int recordLength, long currentAddress, long nextAddress, bool isProtected)
        {
            ValidateSublogIndex(sublogIdx);
            replicationManager.SetSublogReplicationOffset(sublogIdx, currentAddress);
            var ptr = record;
            // logger?.LogError("[{sublogIdx}] = {currentAddress} -> {nextAddress}", sublogIdx, currentAddress, nextAddress);
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
                    // FIXME: Do we need to coordinate between sublogs when updating this?
                    if (isCheckpointStart)
                    {
                        // logger?.LogError("[{sublogIdx}] CheckpointStart {address}", sublogIdx, clusterProvider.replicationManager.GetSublogReplicationOffset(sublogIdx));
                        replicationManager.ReplicationCheckpointStartOffset[sublogIdx] = replicationManager.GetSublogReplicationOffset(sublogIdx);
                    }
                    entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
                }
                else if (payloadLength < 0)
                {
                    if (!serverOptions.EnableFastCommit)
                    {
                        throw new GarnetException("Received FastCommit request at replica AOF processor, but FastCommit is not enabled", clientResponse: false);
                    }
                    TsavoriteLogRecoveryInfo info = new();
                    info.Initialize(new ReadOnlySpan<byte>(ptr + entryLength, -payloadLength));
                    appendOnlyFile.Log.GetSubLog(sublogIdx).UnsafeCommitMetadataOnly(info, isProtected);
                    entryLength += TsavoriteLog.UnsafeAlign(-payloadLength);
                }
                ptr += entryLength;
                replicationManager.IncrementSublogReplicationOffset(sublogIdx, entryLength);
            }
            // logger?.LogError("[{sublogIdx}] = {currentAddress} -> {nextAddress}", sublogIdx, currentAddress, nextAddress);

            if (replicationManager.GetSublogReplicationOffset(sublogIdx) != nextAddress)
            {
                logger?.LogError("ReplicaReplayTask.Consume NextAddress Mismatch sublogIdx: {sublogIdx}; recordLength:{recordLength}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; replicationOffset:{ReplicationOffset}", sublogIdx, recordLength, currentAddress, nextAddress, replicationManager.ReplicationOffset[sublogIdx]);
                throw new GarnetException("Failed validating integrity of replay", LogLevel.Warning, clientResponse: false);
            }
        }

        public void Throttle() { }
        #endregion

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ValidateSublogIndex(int sublogIdx)
        {
            if (sublogIdx != this.sublogIdx)
                throw new GarnetException($"SublogIdx mismatch; expected:{this.sublogIdx} - received:{sublogIdx}");
        }

        public void InitialiazeBackgroundReplayTask(long startAddress)
        {
            if (replayIterator == null)
            {
                replayIterator = appendOnlyFile.ScanSingle(sublogIdx, startAddress, long.MaxValue, scanUncommitted: true, recover: false, logger: logger);
                _ = Task.Run(BackgroundReplayTask);
            }

            async Task BackgroundReplayTask()
            {
                var readLock = ResumeReplay();
                try
                {
                    if (!readLock)
                        throw new GarnetException("Failed to acquire replayLock");
                    while (true)
                    {
                        cts.Token.ThrowIfCancellationRequested();
                        await replayIterator.BulkConsumeAllAsync(
                            this,
                            serverOptions.ReplicaSyncDelayMs,
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
                        SuspendReplay();
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ThrottlePrimary()
        {
            while (serverOptions.ReplicationOffsetMaxLag != -1 && replayIterator != null &&
                appendOnlyFile.Log.TailAddress.AggregateDiff(replicationManager.ReplicationOffset) > serverOptions.ReplicationOffsetMaxLag)
            {
                cts.Token.ThrowIfCancellationRequested();
                Thread.Yield();
            }
        }
    }
}