// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
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
                clusterProvider.replicationManager.SetSublogReplicationOffset(sublogIdx, currentAddress);
                var ptr = record;
                while (ptr < record + recordLength)
                {
                    cts.Token.ThrowIfCancellationRequested();
                    var entryLength = clusterProvider.storeWrapper.appendOnlyFile.HeaderSize;
                    var payloadLength = clusterProvider.storeWrapper.appendOnlyFile.Log.UnsafeGetLength(sublogIdx, ptr);
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
                        clusterProvider.storeWrapper.appendOnlyFile?.UnsafeCommitMetadataOnly(sublogIdx, info, isProtected);
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
    }
}