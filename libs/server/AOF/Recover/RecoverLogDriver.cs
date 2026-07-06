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
    internal sealed class RecoverLogDriver(
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
        /// Test-only switch that forces intra-page multi-replay to run serially (single-threaded,
        /// one replay task at a time over each consumed page) instead of spawning parallel tasks
        /// coordinated by the per-task WorkReadyComplete handshakes.
        /// This lets tests deterministically isolate the entry partition/apply logic
        /// (<see cref="AofProcessor.CanReplay"/> + <see cref="AofProcessor.ProcessAofRecordInternal"/>)
        /// from the concurrent barrier handoff — a serial run must still apply every entry exactly once.
        /// Only valid for non-transactional workloads: transaction replay blocks on
        /// ProcessSynchronizedOperation awaiting all participant tasks and would deadlock when serialized.
        /// </summary>
        internal static bool ForceSerialIntraPageReplay;

        /// <summary>
        /// Test-only counter of how many pages were replayed through the serial
        /// (<see cref="ForceSerialIntraPageReplay"/>) branch. Lets tests assert the serial path was
        /// actually exercised, so a passing result cannot be silently attributed to the parallel path.
        /// </summary>
        internal static long SerialIntraPageReplayInvocations;

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
            else if (ForceSerialIntraPageReplay)
            {
                // Test-only deterministic path: run each replay task's page scan sequentially
                // (single-threaded) to isolate the entry partition/apply logic from the parallel
                // per-task WorkReadyComplete handoff. Must apply every owned entry exactly once.
                for (var replayTaskIdx = 0; replayTaskIdx < serverOptions.AofReplayTaskCount; replayTaskIdx++)
                {
                    var virtualSublogIdx = appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, replayTaskIdx);
                    ReplayPage(replayTaskIdx, virtualSublogIdx, physicalSublog, record, recordLength, currentAddress, isProtected);
                }

                _ = Interlocked.Increment(ref SerialIntraPageReplayInvocations);
                ReplayedRecordCount++;

                // Completed replay
                if (nextAddress == untilAddress)
                    cts.Cancel();
            }
            else
            {
                // Wait for previous batch to complete before overwriting shared batch context
                if (replayTasks != null)
                {
                    for (var i = 0; i < signals.Length; i++)
                        signals[i].WaitCompleted();
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
                        signals[i].WaitCompleted();
                    cts.Cancel();
                }
            }
        }

        private void CreateAndRunIntraPageParallelReplayTasks()
        {
            if (replayTasks != null)
                return;

            signals = [.. Enumerable.Range(0, serverOptions.AofReplayTaskCount).Select(_ => new WorkReadyComplete())];
            replayTasks = [.. Enumerable.Range(0, serverOptions.AofReplayTaskCount).Select(i => Task.Run(() => ContinuousBackgroundReplayAsync(i, physicalSublog)))];
        }

        internal async Task ContinuousBackgroundReplayAsync(int replayTaskIdx, TsavoriteLog replaySublog)
        {
            var virtualSublogIdx = appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, replayTaskIdx);
            var signal = signals[replayTaskIdx];
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    await signal.WaitReadyWorkAsync(cancellationToken: cts.Token).ConfigureAwait(false);
                }
                catch (TaskCanceledException) when (cts.IsCancellationRequested)
                { }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} failed at WaitAsync", nameof(ContinuousBackgroundReplayAsync));
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
                    logger?.LogError(ex, "{method} failed at replaying", nameof(ContinuousBackgroundReplayAsync));
                    await cts.CancelAsync().ConfigureAwait(false);
                    break;
                }

                // Signal completion ONLY after the page was fully applied. On cancellation or fault we
                // break above without signalling, so the leader observes cancellation instead of a
                // completion for a page that was not fully applied.
                signal.SignalCompleted();
            }
        }

        /// <summary>
        /// Scans a single consumed page for the given replay task, applying every entry that this task
        /// owns (per <see cref="AofProcessor.CanReplay"/>) exactly once. Shared by the parallel replay
        /// tasks (<see cref="ContinuousBackgroundReplayAsync"/>) and the serial test path in the per-task-signal-free Consume branch.
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
                            // Sequence numbers are monotonically increasing — stop processing this batch
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

        public void Throttle() { }

        /// <summary>
        /// Starts a background task to replay and recover data until a specified address or when cancellation is requested.
        /// </summary>
        /// <returns>A Task representing the asynchronous recovery operation.</returns>
        public Task CreateRecoverTaskAsync()
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