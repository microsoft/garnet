// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
#if DEBUG
using Garnet.common;
#endif
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
        /// Starts replaying on a thread of its own. Replaying a record can wait, for instance on the other sublogs or on
        /// a Vector Set operation, and a thread-pool thread that waits can be starved by the very work it waits for.
        /// </summary>
        /// <remarks>
        /// The caller has entered the driver's worker monitor on behalf of the thread, which exits it once it is done,
        /// so that disposing the driver waits for the thread and the cancellation source outlives it.
        /// </remarks>
        internal void Start()
            => new Thread(FullPageBasedBackgroundReplay) { IsBackground = true, Name = $"AofReplay-{replayDriver.physicalSublogIdx}.{replayTaskIdx}" }.Start();

        /// <summary>
        /// Replays log entries using a shared rendezvous barrier for coordination, processing and applying them for
        /// replication and consistency across sublogs.
        /// </summary>
        void FullPageBasedBackgroundReplay()
        {
            try
            {
                ReplayPages();
            }
            finally
            {
                replayDriver.SuspendReplay();
            }
        }

        void ReplayPages()
        {
            var physicalSublogIdx = replayDriver.physicalSublogIdx;
            var virtualSublogIdx = appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, replayTaskIdx);

            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    // Rendezvous 1 (ready): meet the leader and peers at the barrier before scanning the page.
                    replayDriver.barrier.SignalWorkReadyWait(cancellationToken: cts.Token);
                }
                catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
                {
                    // Suppress the exception if the task was cancelled because of store wrapper disposal
                    break;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} failed at ready barrier", nameof(FullPageBasedBackgroundReplay));
                    cts.Cancel();
                    break;
                }

                // Guard: if cancellation happened during the ready barrier, exit cleanly
                // without falling through to the processing block (which would issue a spurious completion)
                if (cts.Token.IsCancellationRequested)
                    break;

#if DEBUG
                // Holds the page on this thread, as replaying a record that waits would
                ExceptionInjectionHelper.WaitOnClear(ExceptionInjectionType.Replication_Hold_Replay_Thread);
#endif

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
                        var pulseSequenceNumber = replayDriver.StagedPulseSequenceNumber;

                        if (pulseSequenceNumber > 0)
                        {
                            appendOnlyFile.readConsistencyManager.AdvanceVirtualSublogTime(virtualSublogIdx, pulseSequenceNumber);
                        }
                        else
                        {
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
                                        if (isCheckpointStart)
                                            replicationManager.ReplicationCheckpointStartOffset[physicalSublogIdx] = replicationManager.GetSublogReplicationOffset(physicalSublogIdx);
                                    }
                                    entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
                                }
                                else if (payloadLength < 0)
                                {
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

                            appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, nextAddress);
                        }
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
                    logger?.LogError(ex, "{method} failed at replaying", nameof(FullPageBasedBackgroundReplay));
                    cts.Cancel();
                    break;
                }

                try
                {
                    // Rendezvous 2 (completed): signal completion ONLY after the page was fully applied.
                    // On cancellation or fault we break above WITHOUT arriving here, so the leader's
                    // completion barrier observes cancellation / times out and tears down instead of
                    // advancing the replication offset past a page that was not fully applied.
                    replayDriver.barrier.SignalWorkCompletedWait(cancellationToken: cts.Token);
                }
                catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
                {
                    // Suppress the exception if the task was cancelled because of store wrapper disposal
                    break;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} failed at completion barrier", nameof(FullPageBasedBackgroundReplay));
                    cts.Cancel();
                    break;
                }
            }
        }
    }
}