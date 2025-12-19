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
        internal readonly ReplayWorkItem replayWorkItem;
        readonly ReplicaReplayTask[] replayTasks;
        readonly TsavoriteLog sublog;

        public ReplicaReplayDriver(int sublogIdx, ClusterProvider clusterProvider, INetworkSender respSessionNetworkSender, CancellationTokenSource cts, ILogger logger = null)
        {
            this.sublogIdx = sublogIdx;
            this.respSessionNetworkSender = respSessionNetworkSender;
            serverOptions = clusterProvider.serverOptions;
            appendOnlyFile = clusterProvider.storeWrapper.appendOnlyFile;
            replicationManager = clusterProvider.replicationManager;
            replayIterator = null;
            sublog = appendOnlyFile.Log.GetSubLog(sublogIdx);
            activeReplay = new SingleWriterMultiReaderLock();
            this.cts = cts;
            this.logger = logger;

            // Initialize background replay tasks for this sublog replay driver
            var replayTaskCount = serverOptions.AofReplayTaskCount;
            if (replayTaskCount > 1)
            {
                replayWorkItem = new ReplayWorkItem(replayTaskCount);
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
            if (serverOptions.AofReplayTaskCount == 1)
            {
                ConsumeDirect(record, recordLength, currentAddress, nextAddress, isProtected);
            }
            else
            {
                // Wait for slot to become available
                if (!replayWorkItem.Completed.Wait(serverOptions.ReplicaSyncTimeout, cts.Token))
                    throw new GarnetException("Consume background replay timed-out!");

                replayWorkItem.Completed.Reset();
                var replayBufferSlot = replayWorkItem;
                replayBufferSlot.record = record;
                replayBufferSlot.recordLength = recordLength;
                replayBufferSlot.currentAddress = currentAddress;
                replayBufferSlot.nextAddress = nextAddress;
                replayBufferSlot.isProtected = isProtected;
                replayBufferSlot.Completed.Reset();
                replayBufferSlot.Reset();

                foreach (var replayTask in replayTasks)
                    replayTask.Append(replayWorkItem);

                // Wait for replay to complete
                if (!replayWorkItem.Completed.Wait(serverOptions.ReplicaSyncTimeout, cts.Token))
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
                var payloadLength = sublog.UnsafeGetLength(ptr);
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
                    sublog.UnsafeCommitMetadataOnly(info, isProtected);
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