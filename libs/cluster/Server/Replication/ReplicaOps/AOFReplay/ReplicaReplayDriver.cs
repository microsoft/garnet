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
        internal readonly int physicalSublogIdx;
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
        readonly TsavoriteLog physicalSublog;

        public ReplicaReplayDriver(int physicalSublogIdx, ClusterProvider clusterProvider, INetworkSender respSessionNetworkSender, CancellationTokenSource cts, ILogger logger = null)
        {
            this.physicalSublogIdx = physicalSublogIdx;
            this.respSessionNetworkSender = respSessionNetworkSender;
            serverOptions = clusterProvider.serverOptions;
            appendOnlyFile = clusterProvider.storeWrapper.appendOnlyFile;
            replicationManager = clusterProvider.replicationManager;
            replayIterator = null;
            physicalSublog = appendOnlyFile.Log.GetSubLog(physicalSublogIdx);
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
                    _ = Task.Run(async () => await replayTask.ContinuousBackgroundReplay());
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

        /// <summary>
        /// Main bulk consume implementation.
        /// </summary>
        /// <param name="record">Pointer to the log entry record to consume.</param>
        /// <param name="recordLength">Length of the log entry record in bytes.</param>
        /// <param name="currentAddress">Current address of the log entry in the log.</param>
        /// <param name="nextAddress">Next address in the log after the current record.</param>
        /// <param name="isProtected">Indicates whether the log entry is protected.</param>
        /// <exception cref="GarnetException">Thrown if the background replay operation times out.</exception>
        public unsafe void Consume(byte* record, int recordLength, long currentAddress, long nextAddress, bool isProtected)
        {
            if (serverOptions.AofReplayTaskCount == 1)
            {
                ConsumeDirect(record, recordLength, currentAddress, nextAddress, isProtected);
            }
            else
            {
                // Wait for previous replay batch to finish
                if (!replayWorkItem.Completed.Wait(serverOptions.ReplicaSyncTimeout, cts.Token))
                    throw new GarnetException("Consume background replay timed-out!");

                replayWorkItem.Completed.Reset();
                var replayBufferSlot = replayWorkItem;
                replayBufferSlot.Record = record;
                replayBufferSlot.RecordLength = recordLength;
                replayBufferSlot.CurrentAddress = currentAddress;
                replayBufferSlot.NextAddress = nextAddress;
                replayBufferSlot.IsProtected = isProtected;
                replayBufferSlot.Completed.Reset();
                replayBufferSlot.Reset();

                foreach (var replayTask in replayTasks)
                    replayTask.Append(replayWorkItem);

                // Wait for replay to complete
                if (!replayWorkItem.Completed.Wait(serverOptions.ReplicaSyncTimeout, cts.Token))
                    throw new GarnetException("Consume background replay timed-out!");
            }
        }

        /// <summary>
        /// Processes record on a single replay task directly.
        /// </summary>
        /// <param name="record">Pointer to the start of the log record to process.</param>
        /// <param name="recordLength">Length in bytes of the log record.</param>
        /// <param name="currentAddress">Current address in the log for replication.</param>
        /// <param name="nextAddress">Expected next address in the log after processing.</param>
        /// <param name="isProtected">Indicates whether the operation should be performed in protected mode.</param>
        /// <exception cref="GarnetException">Thrown if fast commit is not enabled when a fast commit request is received, or if log integrity validation
        /// fails.</exception>
        internal unsafe void ConsumeDirect(byte* record, int recordLength, long currentAddress, long nextAddress, bool isProtected)
        {
            ValidateSublogIndex(physicalSublogIdx);
            replicationManager.SetSublogReplicationOffset(physicalSublogIdx, currentAddress);
            var ptr = record;
            // logger?.LogError("[{physicalSublogIdx}] = {currentAddress} -> {nextAddress}", physicalSublogIdx, currentAddress, nextAddress);
            while (ptr < record + recordLength)
            {
                cts.Token.ThrowIfCancellationRequested();
                var entryLength = appendOnlyFile.HeaderSize;
                var payloadLength = physicalSublog.UnsafeGetLength(ptr);
                if (payloadLength > 0)
                {
                    replicationManager.AofProcessor.ProcessAofRecordInternal(physicalSublogIdx, ptr + entryLength, payloadLength, true, out var isCheckpointStart);
                    // Encountered checkpoint start marker, log the ReplicationCheckpointStartOffset so we know the correct AOF truncation
                    // point when we take a checkpoint at the checkpoint end marker
                    if (isCheckpointStart)
                    {
                        // This is safe to be updated in parallel given that each sublog replay taks will update its own slot with corresponding address of the checkpoint marker
                        replicationManager.ReplicationCheckpointStartOffset[physicalSublogIdx] = replicationManager.GetSublogReplicationOffset(physicalSublogIdx);
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
                    physicalSublog.UnsafeCommitMetadataOnly(info, isProtected);
                    entryLength += TsavoriteLog.UnsafeAlign(-payloadLength);
                }
                ptr += entryLength;
                replicationManager.IncrementSublogReplicationOffset(physicalSublogIdx, entryLength);
            }
            // logger?.LogError("[{physicalSublogIdx}] = {currentAddress} -> {nextAddress}", physicalSublogIdx, currentAddress, nextAddress);

            if (replicationManager.GetSublogReplicationOffset(physicalSublogIdx) != nextAddress)
            {
                logger?.LogError("ReplicaReplayTask.Consume NextAddress Mismatch sublogIdx: {sublogIdx}; recordLength:{recordLength}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; replicationOffset:{ReplicationOffset}", physicalSublogIdx, recordLength, currentAddress, nextAddress, replicationManager.ReplicationOffset[physicalSublogIdx]);
                throw new GarnetException("Failed validating integrity of replay", LogLevel.Warning, clientResponse: false);
            }
        }

        public void Throttle() { }
        #endregion

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ValidateSublogIndex(int physicalSublogIdx)
        {
            if (physicalSublogIdx != this.physicalSublogIdx)
                throw new GarnetException($"PhysicalSublogIdx mismatch; expected:{this.physicalSublogIdx} - received:{physicalSublogIdx}");
        }

        /// <summary>
        /// Method to create a background replay task that iterates and consume this replicas physical log
        /// </summary>
        /// <param name="startAddress"></param>
        public void InitialiazeBackgroundReplayTask(long startAddress)
        {
            if (replayIterator == null)
            {
                replayIterator = appendOnlyFile.ScanSingle(physicalSublogIdx, startAddress, long.MaxValue, scanUncommitted: true, recover: false, logger: logger);
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