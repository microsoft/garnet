// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Initializes a new instance of the RecoverLogDriver class for replaying a segment of an append-only file
    /// for recovery.
    /// </summary>
    /// <param name="aofProcessor">Processor responsible for handling append-only file operations.</param>
    /// <param name="appendOnlyFile">The append-only file to be scanned for recovery.</param>
    /// <param name="serverOptions">Configuration options for the server.</param>
    /// <param name="dbId">Identifier of the database we are recovering.</param>
    /// <param name="physicalSublogIdx">Index of the physical sublog to scan.</param>
    /// <param name="startAddress">Start address in the append-only file for recovery.</param>
    /// <param name="untilAddress">End address in the append-only file for recovery.</param>
    /// <param name="untilSequenceNumber">Replay all records with sequence number to ensure prefix consistent recovery.</param>
    /// <param name="logger">Optional logger for diagnostic output.</param>
    internal sealed partial class RecoverLogDriver(
        AofProcessor aofProcessor,
        GarnetAppendOnlyFile appendOnlyFile,
        GarnetServerOptions serverOptions,
        int dbId,
        int physicalSublogIdx,
        long startAddress,
        long untilAddress,
        long untilSequenceNumber,
        ILogger logger = null) : IBulkLogEntryConsumer, IDisposable
    {
        readonly int physicalSublogIdx = physicalSublogIdx;
        readonly AofProcessor aofProcessor = aofProcessor;
        readonly GarnetServerOptions serverOptions = serverOptions;
        readonly GarnetAppendOnlyFile appendOnlyFile = appendOnlyFile;
        readonly TsavoriteLogScanSingleIterator replayIterator = appendOnlyFile.Log.ScanSingle(physicalSublogIdx, startAddress, untilAddress, scanUncommitted: true, recover: false, logger: logger);
        readonly TsavoriteLog physicalSublog = appendOnlyFile.Log.GetSubLog(physicalSublogIdx);
        readonly CancellationTokenSource cts = new();
        readonly ILogger logger = logger;
        readonly long startAddress = startAddress;
        readonly long untilAddress = untilAddress;
        readonly long untilSequenceNumber = untilSequenceNumber;
        readonly int dbId = dbId;
        readonly ReplayBatchContext replayBatchContext = new(serverOptions.AofReplayTaskCount);
        Task[] replayTasks = null;

        /// <summary>
        /// Per-task leader/worker handshakes (one per parallel replay task). The recover leader hands each
        /// task exactly one page at a time and waits for its completion on the matching signal, so no
        /// permit can be stolen across tasks or leak across cycles.
        /// </summary>
        WorkReadyComplete[] signals = null;

        /// <summary>
        /// Set by a replay task when it encounters an entry whose sequence number exceeds
        /// <see cref="untilSequenceNumber"/> (the prefix-consistency boundary). Because sequence numbers are
        /// monotonically increasing, no further entries need to be applied once this is set. The leader
        /// observes it only after every task has finished the current page (via <see cref="WorkReadyComplete.WaitCompleted"/>,
        /// which establishes a happens-before), then cancels to stop handing subsequent pages — avoiding
        /// wasted scans and, importantly, avoiding committing log metadata beyond the intended prefix.
        /// </summary>
        volatile bool prefixConsistencyBoundaryReached = false;

        /// <summary>
        /// Gets the total number of records that have been replayed.
        /// </summary>
        public long ReplayedRecordCount { get; private set; } = 0;

        public void Dispose()
        {
            replayIterator?.Dispose();
            cts?.Dispose();
        }

        /// <summary>
        /// Main consume method for recover driver.
        /// </summary>
        /// <param name="record"></param>
        /// <param name="recordLength"></param>
        /// <param name="currentAddress"></param>
        /// <param name="nextAddress"></param>
        /// <param name="isProtected"></param>
        public unsafe void Consume(byte* record, int recordLength, long currentAddress, long nextAddress, bool isProtected)
        {
            if (serverOptions.AofReplayTaskCount == 1)
            {
                // Recover/Replay on this consume thread
                var ptr = record;
                while (ptr < record + recordLength)
                {
                    var entryLength = appendOnlyFile.HeaderSize;
                    var payloadLength = physicalSublog.UnsafeGetLength(ptr);
                    if (payloadLength > 0)
                    {
                        var entryPtr = ptr + entryLength;
                        var logAddressSequenceNumber = currentAddress + (ptr - record);
                        Debug.Assert(logAddressSequenceNumber > 0, "Entry log address must be positive");
                        if (!aofProcessor.SkipReplay(entryPtr, untilSequenceNumber, logAddressSequenceNumber, out var sequenceNumber))
                        {
                            aofProcessor.ProcessAofRecordInternal(physicalSublogIdx, entryPtr, payloadLength, true, out _, logAddressSequenceNumber);
                        }
                        else
                        {
                            // Sequence numbers are monotonically increasing — all subsequent entries will also exceed the threshold
                            logger?.LogTrace("Skipping entry replay {entrySequenceNumber} > {untilSequenceNumber}, stopping", sequenceNumber, untilSequenceNumber);
                            cts.Cancel();
                            break;
                        }
                        entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
                    }
                    else if (payloadLength < 0)
                    {
                        TsavoriteLogRecoveryInfo info = new();
                        info.Initialize(new ReadOnlySpan<byte>(ptr + entryLength, -payloadLength));
                        physicalSublog.UnsafeCommitMetadataOnly(info, isProtected);
                        entryLength += TsavoriteLog.UnsafeAlign(-payloadLength);
                    }
                    ptr += entryLength;

                    ReplayedRecordCount++;
                    if (ReplayedRecordCount % 100_000 == 0)
                    {
                        logger?.LogTrace("Completed AOF replay of {count} records, until AOF address {nextAofAddress} (DB ID: {id})", ReplayedRecordCount, untilAddress, dbId);
                    }
                }

                // Completed replay
                if (nextAddress == untilAddress)
                    cts.Cancel();
            }
            else
            {
                // Wait for previous batch to complete before overwriting shared batch context
                if (replayTasks != null)
                {
                    // Pass the cancellation token so a worker that cancelled/faulted (and therefore did NOT
                    // signal completion, by design) unwinds this leader wait instead of deadlocking recovery.
                    for (var i = 0; i < signals.Length; i++)
                        _ = signals[i].WaitCompleted(cancellationToken: cts.Token);

                    // Every task has now finished the previous page. If any of them reached the
                    // prefix-consistency boundary, stop here so we neither scan further pages nor commit
                    // log metadata beyond the intended prefix.
                    if (prefixConsistencyBoundaryReached)
                    {
                        logger?.LogTrace("Reached prefix-consistency boundary (until sequence number {untilSequenceNumber}), stopping parallel replay", untilSequenceNumber);
                        cts.Cancel();
                        return;
                    }
                }

                CreateAndRunIntraPageParallelReplayTasks();

                replayBatchContext.Record = record;
                replayBatchContext.RecordLength = recordLength;
                replayBatchContext.CurrentAddress = currentAddress;
                replayBatchContext.NextAddress = nextAddress;
                replayBatchContext.IsProtected = isProtected;

                // Hand this page to every replay task.
                for (var i = 0; i < signals.Length; i++)
                    signals[i].SignalWorkReady();

                // After the last batch, wait for workers and cancel to exit BulkConsumeAllAsync
                if (nextAddress == untilAddress)
                {
                    for (var i = 0; i < signals.Length; i++)
                        _ = signals[i].WaitCompleted(cancellationToken: cts.Token);
                    cts.Cancel();
                }
            }
        }

        private void CreateAndRunIntraPageParallelReplayTasks()
        {
            if (replayTasks != null)
                return;

            signals = [.. Enumerable.Range(0, serverOptions.AofReplayTaskCount).Select(_ => new WorkReadyComplete())];
            replayTasks = [.. Enumerable.Range(0, serverOptions.AofReplayTaskCount).Select(i => Task.Run(() => RecoverReplayTaskAsync(i, physicalSublog)))];
        }

        public void Throttle() { }

        /// <summary>
        /// Starts a background task to replay and recover data until a specified address or when cancellation is requested.
        /// </summary>
        /// <returns>A Task representing the asynchronous recovery operation.</returns>
        public Task RunAsync()
        {
            return Task.Run(async () =>
            {
                try
                {
                    if (startAddress == untilAddress) return;
                    logger?.LogInformation("Recover sublog [{physicalSublogIdx}] for addres range ({startAddress},{untilAddress})", physicalSublogIdx, startAddress, untilAddress);
                    while (!cts.IsCancellationRequested)
                    {
                        await replayIterator.BulkConsumeAllAsync(
                            this,
                            serverOptions.ReplicaSyncDelayMs,
                            maxChunkSize: 1 << 20,
                            cts.Token).ConfigureAwait(false);

                        // Replay completed
                        if (replayIterator.NextAddress == untilAddress)
                            break;
                    }
                }
                catch (TaskCanceledException) when (cts.IsCancellationRequested)
                { }
            });
        }
    }
}