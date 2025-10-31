// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;
using Garnet.server;

namespace Garnet.cluster
{
    /// <summary>
    /// Replica replay task
    /// </summary>
    /// <param name="sublogIdx"></param>
    /// <param name="clusterProvider"></param>
    /// <param name="logger"></param>
    internal sealed class ReplicaReplayTask(int sublogIdx, ClusterProvider clusterProvider, ILogger logger = null) : IBulkLogEntryConsumer, IDisposable
    {
        readonly int sublogIdx = sublogIdx;
        readonly ClusterProvider clusterProvider = clusterProvider;
        readonly CancellationTokenSource cts = new();
        readonly ILogger logger = logger;
        TsavoriteLogScanSingleIterator replayIterator = null;
        SingleWriterMultiReaderLock activeReplay;

        public void Dispose()
        {
            cts.Cancel();
            activeReplay.WriteLock();
            cts.Dispose();
            replayIterator?.Dispose();
        }

        #region IBulkLogEntryConsumer
        public unsafe void Consume(byte* record, int recordLength, long currentAddress, long nextAddress, bool isProtected)
        {
            ValidateSublogIndex(sublogIdx);
            clusterProvider.replicationManager.SetSublogReplicationOffset(sublogIdx, currentAddress);
            var ptr = record;
            while (ptr < record + recordLength)
            {
                cts.Token.ThrowIfCancellationRequested();
                var entryLength = clusterProvider.storeWrapper.appendOnlyFile.HeaderSize;
                var payloadLength = clusterProvider.storeWrapper.appendOnlyFile.Log.GetSubLog(sublogIdx).UnsafeGetLength(ptr);
                if (payloadLength > 0)
                {
                    clusterProvider.replicationManager.AofProcessor.ProcessAofRecordInternal(sublogIdx, ptr + entryLength, payloadLength, true, out var isCheckpointStart);
                    // Encountered checkpoint start marker, log the ReplicationCheckpointStartOffset so we know the correct AOF truncation
                    // point when we take a checkpoint at the checkpoint end marker
                    // FIXME: Do we need to coordinate between sublogs when updating this?
                    if (isCheckpointStart)
                        clusterProvider.replicationManager.ReplicationCheckpointStartOffset[sublogIdx] = clusterProvider.replicationManager.ReplicationOffset[sublogIdx];
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

            if (clusterProvider.replicationManager.GetSublogReplicationOffset(sublogIdx) != nextAddress)
            {
                logger?.LogError("ReplicaReplayTask.Consume NextAddress Mismatch sublogIdx: {sublogIdx}; recordLength:{recordLength}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; replicationOffset:{ReplicationOffset}", sublogIdx, recordLength, currentAddress, nextAddress, clusterProvider.replicationManager.ReplicationOffset[sublogIdx]);
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
                replayIterator = clusterProvider.storeWrapper.appendOnlyFile.ScanSingle(sublogIdx, startAddress, long.MaxValue, scanUncommitted: true, recover: false, logger: logger);
                _ = Task.Run(BackgroundReplayTask);
            }

            async Task BackgroundReplayTask()
            {
                var readLock = activeReplay.TryReadLock();
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
                        activeReplay.ReadUnlock();
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ThrottlePrimary()
        {
            while (clusterProvider.serverOptions.ReplicationOffsetMaxLag != -1 && replayIterator != null &&
                clusterProvider.storeWrapper.appendOnlyFile.Log.TailAddress.AggregateDiff(clusterProvider.replicationManager.ReplicationOffset) > clusterProvider.storeWrapper.serverOptions.ReplicationOffsetMaxLag)
            {
                cts.Token.ThrowIfCancellationRequested();
                Thread.Yield();
            }
        }
    }

    internal sealed partial class ReplicationManager : IDisposable
    {
        /// <summary>
        /// Replay task instances per sublog (used with ShardedLog)
        /// </summary>
        readonly ReplicaReplayTask[] replicaReplayTasks;

        /// <summary>
        /// Dispose replica replay tasks
        /// </summary>
        public void DisposeReplayTasks()
        {
            for (var i = 0; i < replicaReplayTasks.Length; i++)
            {
                replicaReplayTasks[i]?.Dispose();
                replicaReplayTasks[i] = null;
            }
        }
    }

    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        ReplicaReplayTask replicaReplayTask = null;
        TsavoriteLog sublog = null;

        void ResetReplayTask()
        {
            try
            {
                replicaReplayTask?.Dispose();
            }
            finally
            {
                replicaReplayTask = null;
                sublog = null;
            }
        }

        /// <summary>
        /// Apply primary AOF records.
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="record"></param>
        /// <param name="recordLength"></param>
        /// <param name="previousAddress"></param>
        /// <param name="currentAddress"></param>
        /// <param name="nextAddress"></param>
        public unsafe void ProcessPrimaryStream(int sublogIdx, byte* record, int recordLength, long previousAddress, long currentAddress, long nextAddress)
        {
            // logger?.LogInformation("Processing {recordLength} bytes; previousAddress {previousAddress}, currentAddress {currentAddress}, nextAddress {nextAddress}, current AOF tail {tail}", recordLength, previousAddress, currentAddress, nextAddress, storeWrapper.appendOnlyFile.TailAddress);
            var currentConfig = clusterProvider.clusterManager.CurrentConfig;
            try
            {
                if (clusterProvider.replicationManager.CannotStreamAOF)
                {
                    logger?.LogError("Replica is recovering cannot sync AOF");
                    throw new GarnetException("Replica is recovering cannot sync AOF", LogLevel.Warning, clientResponse: false);
                }

                if (currentConfig.LocalNodeRole != NodeRole.REPLICA)
                {
                    logger?.LogWarning("This node {nodeId} is not a replica", currentConfig.LocalNodeId);
                    throw new GarnetException($"This node {currentConfig.LocalNodeId} is not a replica", LogLevel.Warning, clientResponse: false);
                }

                if (clusterProvider.serverOptions.FastAofTruncate)
                {
                    // If the incoming AOF chunk fits in the space between previousAddress and currentAddress (ReplicationOffset),
                    // an enqueue will result in an offset mismatch. So, we have to first reset the AOF to point to currentAddress.
                    if (currentAddress > previousAddress)
                    {
                        if (
                            (currentAddress % (1 << clusterProvider.replicationManager.PageSizeBits) != 0) || // the skip was to a non-page-boundary
                            (currentAddress >= previousAddress + recordLength) // the skip will not be auto-handled by the AOF enqueue
                            )
                        {
                            logger?.LogWarning("MainMemoryReplication: Skipping from {ReplicaReplicationOffset} to {currentAddress}", clusterProvider.replicationManager.GetSublogReplicationOffset(sublogIdx), currentAddress);
                            clusterProvider.storeWrapper.appendOnlyFile.SafeInitialize(sublogIdx, currentAddress, currentAddress);
                            clusterProvider.replicationManager.SetSublogReplicationOffset(sublogIdx, currentAddress);
                        }
                    }
                }

                // Injection for a "something went wrong with THIS Replica's AOF file"
                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Divergent_AOF_Stream);

                var tail = clusterProvider.storeWrapper.appendOnlyFile.Log.TailAddress;
                var nextPageBeginAddress = ((tail[sublogIdx] >> clusterProvider.replicationManager.PageSizeBits) + 1) << clusterProvider.replicationManager.PageSizeBits;
                // Check to ensure:
                // 1. if record fits in current page tailAddress of this local node (replica) should be equal to the incoming currentAddress (address of chunk send from primary node)
                // 2. if record does not fit in current page start address of the next page matches incoming currentAddress (address of chunk send from primary node)
                // otherwise fail and break the connection
                if ((tail[sublogIdx] + recordLength <= nextPageBeginAddress && tail[sublogIdx] != currentAddress) ||
                    (tail[sublogIdx] + recordLength > nextPageBeginAddress && nextPageBeginAddress != currentAddress))
                {
                    logger?.LogError("Divergent AOF Stream recordLength:{recordLength}; previousAddress:{previousAddress}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; tailAddress:{tail}", recordLength, previousAddress, currentAddress, nextAddress, tail);
                    throw new GarnetException($"Divergent AOF Stream recordLength:{recordLength}; previousAddress:{previousAddress}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; tailAddress:{tail}", LogLevel.Warning, clientResponse: false);
                }

                // Address check only if synchronous replication is enabled
                if (clusterProvider.storeWrapper.serverOptions.ReplicationOffsetMaxLag == 0 && clusterProvider.replicationManager.GetSublogReplicationOffset(sublogIdx) != tail[sublogIdx])
                {
                    logger?.LogInformation("Processing {recordLength} bytes; previousAddress {previousAddress}, currentAddress {currentAddress}, nextAddress {nextAddress}, current AOF tail {tail}", recordLength, previousAddress, currentAddress, nextAddress, tail);
                    logger?.LogError("Before ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {ReplicaReplicationOffset}, aof.TailAddress {tailAddress}", clusterProvider.replicationManager.ReplicationOffset, tail);
                    throw new GarnetException($"Before ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {clusterProvider.replicationManager.ReplicationOffset}, aof.TailAddress {tail}", LogLevel.Warning, clientResponse: false);
                }

                // Check that sublogIdx received is one expected
                replicaReplayTask?.ValidateSublogIndex(sublogIdx);

                // Initialize sublog ref if first time
                sublog ??= clusterProvider.storeWrapper.appendOnlyFile.Log.GetSubLog(sublogIdx);

                // Enqueue to AOF
                _ = sublog.UnsafeEnqueueRaw(new Span<byte>(record, recordLength), noCommit: clusterProvider.serverOptions.EnableFastCommit);

                replicaReplayTask ??= new ReplicaReplayTask(sublogIdx, clusterProvider, logger);

                if (clusterProvider.storeWrapper.serverOptions.ReplicationOffsetMaxLag == 0)
                {
                    // Synchronous replay
                    replicaReplayTask.Consume(record, recordLength, currentAddress, nextAddress, isProtected: false);
                }
                else
                {
                    // Initialize iterator and run background task once
                    replicaReplayTask.InitialiazeBackgroundReplayTask(previousAddress);

                    // Throttle to give the opportunity to the background replay task to catch up
                    replicaReplayTask.ThrottlePrimary();
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ProcessPrimaryStream");
                ResetReplayTask();
                throw new GarnetException(ex.Message, ex, LogLevel.Warning, clientResponse: false);
            }
        }

    }
}