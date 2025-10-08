// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        internal sealed class ReplicaAofSyncReplayTask(int sublogIdx, ClusterProvider clusterProvider, ILogger logger = null) : IBulkLogEntryConsumer, IDisposable
        {
            readonly int sublogIdx = sublogIdx;
            readonly ClusterProvider clusterProvider = clusterProvider;
            readonly ILogger logger = logger;

            TsavoriteLogScanSingleIterator replayIterator = null;
            CancellationTokenSource replicaReplayTaskCts = CancellationTokenSource.CreateLinkedTokenSource(clusterProvider.replicationManager.ctsRepManager.Token);
            SingleWriterMultiReaderLock activeReplay;

            public void Dispose()
            {
                replicaReplayTaskCts.Cancel();
                activeReplay.WriteLock();
                replicaReplayTaskCts.Dispose();
            }

            /// <summary>
            /// Reset background replay iterator
            /// </summary>
            public void ResetReplayIterator()
            {
                ResetReplayCts();
                replayIterator?.Dispose();
                replayIterator = null;

                void ResetReplayCts()
                {
                    if (replicaReplayTaskCts == null)
                    {
                        replicaReplayTaskCts = CancellationTokenSource.CreateLinkedTokenSource(clusterProvider.replicationManager.ctsRepManager.Token);
                    }
                    else
                    {
                        replicaReplayTaskCts.Cancel();
                        try
                        {
                            activeReplay.WriteLock();
                            replicaReplayTaskCts.Dispose();
                            replicaReplayTaskCts = CancellationTokenSource.CreateLinkedTokenSource(clusterProvider.replicationManager.ctsRepManager.Token);
                        }
                        finally
                        {
                            activeReplay.WriteUnlock();
                        }
                    }
                }
            }

            public unsafe void Consume(byte* record, int recordLength, long currentAddress, long nextAddress, bool isProtected)
            {
                clusterProvider.replicationManager.ReplicationOffset[sublogIdx] = currentAddress;
                var ptr = record;
                while (ptr < record + recordLength)
                {
                    replicaReplayTaskCts.Token.ThrowIfCancellationRequested();
                    var entryLength = clusterProvider.storeWrapper.appendOnlyFile.HeaderSize;
                    var payloadLength = clusterProvider.storeWrapper.appendOnlyFile.Log.UnsafeGetLength(ptr);
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
                    clusterProvider.replicationManager.ReplicationOffset[sublogIdx] += entryLength;
                }

                if (clusterProvider.replicationManager.ReplicationOffset[sublogIdx] != nextAddress)
                {
                    logger?.LogError("ReplicaReplayTask.Consume NextAddress Mismatch recordLength:{recordLength}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; replicationOffset:{ReplicationOffset}", recordLength, currentAddress, nextAddress, clusterProvider.replicationManager.ReplicationOffset[sublogIdx]);
                    throw new GarnetException($"ReplicaReplayTask.Consume NextAddress Mismatch recordeLength:{recordLength}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; replicationOffset:{clusterProvider.replicationManager.ReplicationOffset[sublogIdx]}", LogLevel.Warning, clientResponse: false);
                }
            }

            public void Throttle() { }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void ThrottlePrimary()
            {
                while (clusterProvider.serverOptions.ReplicationOffsetMaxLag != -1 && replayIterator != null &&
                    clusterProvider.storeWrapper.appendOnlyFile.Log.TailAddress.AggregateDiff(clusterProvider.replicationManager.ReplicationOffset) > clusterProvider.storeWrapper.serverOptions.ReplicationOffsetMaxLag)
                {
                    replicaReplayTaskCts.Token.ThrowIfCancellationRequested();
                    Thread.Yield();
                }
            }

            public async void ReplicaReplayTask()
            {
                try
                {
                    activeReplay.ReadLock();
                    while (true)
                    {
                        replicaReplayTaskCts.Token.ThrowIfCancellationRequested();
                        await replayIterator.BulkConsumeAllAsync(
                            this,
                            clusterProvider.serverOptions.ReplicaSyncDelayMs,
                            maxChunkSize: 1 << 20,
                            replicaReplayTaskCts.Token);
                    }
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ReplicaReplayTask - terminating");
                }
                finally
                {
                    activeReplay.ReadUnlock();
                }
            }
        }
    }
}