// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    internal sealed partial class RecoverLogDriver
    {
        internal async Task RecoverReplayTaskAsync(int replayTaskIdx, TsavoriteLog replaySublog)
        {
            var virtualSublogIdx = appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, replayTaskIdx);
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    // Rendezvous 1 (ready): meet the leader and peers at the barrier before scanning the page.
                    await barrier.SignalWorkReadyWaitAsync(cancellationToken: cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} failed at ready barrier", nameof(RecoverReplayTaskAsync));
                    await cts.CancelAsync().ConfigureAwait(false);
                    break;
                }

                if (cts.Token.IsCancellationRequested)
                    break;

                try
                {
                    unsafe
                    {
                        ReplayPage(
                            replayTaskIdx,
                            virtualSublogIdx,
                            replaySublog,
                            replayBatchContext.Record,
                            replayBatchContext.RecordLength,
                            replayBatchContext.CurrentAddress,
                            replayBatchContext.IsProtected);
                    }
                }
                catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
                {
                    // Cancelled during store disposal / prefix-consistency stop: break without signalling.
                    break;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} failed at replaying", nameof(RecoverReplayTaskAsync));
                    await cts.CancelAsync().ConfigureAwait(false);
                    break;
                }

                try
                {
                    // Rendezvous 2 (completed): signal completion ONLY after the page was fully applied. On
                    // cancellation or fault we break above WITHOUT arriving here, so the leader observes
                    // cancellation instead of a completion for a page that was not fully applied.
                    await barrier.SignalWorkCompletedWaitAsync(cancellationToken: cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} failed at completion barrier", nameof(RecoverReplayTaskAsync));
                    await cts.CancelAsync().ConfigureAwait(false);
                    break;
                }
            }
        }

        /// <summary>
        /// Scans a single consumed page for the given replay task, applying every entry that this task
        /// owns (per <see cref="AofProcessor.CanReplay"/>) exactly once. Invoked by each parallel replay
        /// task (<see cref="RecoverReplayTaskAsync"/>).
        /// </summary>
        private unsafe void ReplayPage(
            int replayTaskIdx,
            int virtualSublogIdx,
            TsavoriteLog replaySublog,
            byte* record,
            int recordLength,
            long currentAddress,
            bool isProtected)
        {
            var ptr = record;
            var maxSequenceNumber = 0L;

            while (ptr < record + recordLength)
            {
                cts.Token.ThrowIfCancellationRequested();
                var entryLength = appendOnlyFile.HeaderSize;
                var payloadLength = replaySublog.UnsafeGetLength(ptr);
                if (payloadLength > 0)
                {
                    var entryPtr = ptr + entryLength;
                    var logAddressSequenceNumber = currentAddress + (ptr - record);
                    Debug.Assert(logAddressSequenceNumber > 0, "Entry log address must be positive");
                    // Check if entry is assigned for processing to this replay task and
                    // the sequence number is below the threshold to ensure prefix consistency
                    if (aofProcessor.CanReplay(entryPtr, replayTaskIdx, logAddressSequenceNumber, out var sequenceNumber))
                    {
                        if (untilSequenceNumber != -1 && sequenceNumber > untilSequenceNumber)
                        {
                            // Sequence numbers are monotonically increasing — stop processing this batch.
                            // Signal the boundary so the leader stops handing subsequent pages once every
                            // task has finished the current one. We only break here (rather than cancelling)
                            // so that other tasks can finish applying their owned sub-threshold entries in
                            // this page before replay is torn down.
                            prefixConsistencyBoundaryReached = true;
                            break;
                        }
                        aofProcessor.ProcessAofRecordInternal(virtualSublogIdx, entryPtr, payloadLength, true, out _, logAddressSequenceNumber);
                        maxSequenceNumber = Math.Max(sequenceNumber, maxSequenceNumber);
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

            // Update max sequence number for this virtual sublog which is mapped
            appendOnlyFile.readConsistencyManager.UpdateVirtualSublogMaxSequenceNumber(virtualSublogIdx, maxSequenceNumber);
        }
    }
}