// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
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

        /// <summary>
        /// Asynchronously replays log entries using a shared rendezvous barrier for coordination, processing and applying
        /// them for replication and consistency across sublogs.
        /// </summary>
        /// <returns>A task representing the asynchronous replay operation.</returns>
        internal async Task FullPageBasedBackgroundReplayAsync()
        {
            var physicalSublogIdx = replayDriver.physicalSublogIdx;
            var virtualSublogIdx = appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, replayTaskIdx);

            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    // Rendezvous 1 (ready): meet the leader and peers at the barrier before scanning the page.
                    await replayDriver.barrier.SignalWorkReadyWaitAsync(cancellationToken: cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
                {
                    // Suppress the exception if the task was cancelled because of store wrapper disposal
                    break;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} failed at ready barrier", nameof(FullPageBasedBackgroundReplayAsync));
                    await cts.CancelAsync().ConfigureAwait(false);
                    break;
                }

                // Guard: if cancellation happened during the ready barrier, exit cleanly
                // without falling through to the processing block (which would issue a spurious completion)
                if (cts.Token.IsCancellationRequested)
                    break;

                try
                {
                    unsafe
                    {
                        var record = replayBatchContext.Record;
                        var recordLength = replayBatchContext.RecordLength;
                        var currentAddress = replayBatchContext.CurrentAddress;
                        var nextAddress = replayBatchContext.NextAddress;
                        var isProtected = replayBatchContext.IsProtected;
                        var ptr = record;

                        while (ptr < record + recordLength)
                        {
                            cts.Token.ThrowIfCancellationRequested();
                            var entryLength = appendOnlyFile.HeaderSize;
                            var payloadLength = replaySublog.UnsafeGetLength(ptr);
                            if (payloadLength > 0)
                            {
                                var entryPtr = ptr + entryLength;
                                var logAddressSequenceNumber = currentAddress + (ptr - record);
                                if (replicationManager.AofProcessor.CanReplay(entryPtr, replayTaskIdx, logAddressSequenceNumber, out _))
                                {
                                    replicationManager.AofProcessor.ProcessAofRecordInternal(virtualSublogIdx, entryPtr, payloadLength, true, out var isCheckpointStart, logAddressSequenceNumber);
                                    // Encountered checkpoint start marker, log the ReplicationCheckpointStartOffset so we know the correct AOF truncation
                                    // point when we take a checkpoint at the checkpoint end marker
                                    if (isCheckpointStart)
                                    {
                                        replicationManager.ReplicationCheckpointStartOffset[physicalSublogIdx] = replicationManager.GetSublogReplicationOffset(physicalSublogIdx);
                                    }
                                }
                                entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
                            }
                            else if (payloadLength < 0)
                            {
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

                        // Advance frontier to nextAddress (past all entries in this page).
                        // This ensures the read consistency protocol (which waits for frontier > sessionSeq)
                        // can proceed once all writes in the page are complete.
                        appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, nextAddress);
                    }
                }
                catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
                {
                    // Suppress the exception if the task was cancelled because of store wrapper disposal.
                    // Break without signalling completion: the leader must observe cancellation, not a
                    // spurious completion for an unapplied page.
                    break;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} failed at replaying", nameof(FullPageBasedBackgroundReplayAsync));
                    await cts.CancelAsync().ConfigureAwait(false);
                    break;
                }

                try
                {
                    // Rendezvous 2 (completed): signal completion ONLY after the page was fully applied.
                    // On cancellation or fault we break above WITHOUT arriving here, so the leader's
                    // completion barrier observes cancellation / times out and tears down instead of
                    // advancing the replication offset past a page that was not fully applied.
                    await replayDriver.barrier.SignalWorkCompletedWaitAsync(cancellationToken: cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
                {
                    // Suppress the exception if the task was cancelled because of store wrapper disposal
                    break;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} failed at completion barrier", nameof(FullPageBasedBackgroundReplayAsync));
                    await cts.CancelAsync().ConfigureAwait(false);
                    break;
                }
            }
        }
    }
}