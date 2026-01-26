// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;
using System.Linq;

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
    /// <param name="logger">Optional logger for diagnostic output.</param>
    internal sealed class RecoverLogDriver(
        AofProcessor aofProcessor,
        GarnetAppendOnlyFile appendOnlyFile,
        GarnetServerOptions serverOptions,
        int dbId,
        int physicalSublogIdx,
        long startAddress,
        long untilAddress,
        ILogger logger = null) : IBulkLogEntryConsumer, IDisposable
    {
        readonly int physicalSublogIdx = physicalSublogIdx;
        readonly AofProcessor aofProcessor = aofProcessor;
        readonly GarnetServerOptions serverOptions = serverOptions;
        readonly GarnetAppendOnlyFile appendOnlyFile = appendOnlyFile;
        readonly TsavoriteLogScanSingleIterator replayIterator = appendOnlyFile.ScanSingle(physicalSublogIdx, startAddress, untilAddress, scanUncommitted: true, recover: false, logger: logger);
        readonly TsavoriteLog physicalSublog = appendOnlyFile.Log.GetSubLog(physicalSublogIdx);
        readonly CancellationTokenSource cts = new();
        readonly ILogger logger = logger;
        readonly long startAddress = startAddress;
        readonly long untilAddress = untilAddress;
        readonly int dbId = dbId;
        readonly ReplayBatchContext replayBatchContext = new (serverOptions.AofReplayTaskCount);
        Task[] replayTasks = null;

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
                        aofProcessor.ProcessAofRecordInternal(physicalSublogIdx, ptr + entryLength, payloadLength, true, out var isCheckpointStart);
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
                CreateAndRunIntraPageParallelReplayTasks();

                replayBatchContext.Record = record;
                replayBatchContext.RecordLength = recordLength;
                replayBatchContext.CurrentAddress = currentAddress;
                replayBatchContext.NextAddress = nextAddress;
                replayBatchContext.IsProtected = isProtected;
                replayBatchContext.LeaderFollowerBarrier.SignalWorkReady();
            }
        }

        private void CreateAndRunIntraPageParallelReplayTasks()
            => replayTasks ??= [.. Enumerable.Range(0, serverOptions.AofReplayTaskCount).Select(i => Task.Run(async () => await ContinuousBackgroundReplay(i, physicalSublog)))];

        internal async Task ContinuousBackgroundReplay(int replayTaskIdx, TsavoriteLog replaySublog)
        {
            var virtualSublogIdx = appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, replayTaskIdx);
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    await replayBatchContext.LeaderFollowerBarrier.WaitReadyWorkAsync(cancellationToken: cts.Token);
                }
                catch (TaskCanceledException) when (cts.IsCancellationRequested)
                { }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} failed at WaitAsync", nameof(ContinuousBackgroundReplay));
                    cts.Cancel();
                    break;
                }

                unsafe
                {
                    var record = replayBatchContext.Record;
                    var recordLength = replayBatchContext.RecordLength;
                    var currentAddress = replayBatchContext.CurrentAddress;
                    var nextAddress = replayBatchContext.NextAddress;
                    var isProtected = replayBatchContext.IsProtected;
                    var ptr = record;

                    var maxSequenceNumber = 0L;
                    try
                    {
                        // logger?.LogError("[{sublogIdx},{replayIdx}] = {currentAddress} -> {nextAddress}", sublogIdx, replayIdx, currentAddress, nextAddress);                        
                        while (ptr < record + recordLength)
                        {
                            cts.Token.ThrowIfCancellationRequested();
                            var entryLength = appendOnlyFile.HeaderSize;
                            var payloadLength = replaySublog.UnsafeGetLength(ptr);
                            if (payloadLength > 0)
                            {
                                var entryPtr = ptr + entryLength;
                                if (aofProcessor.ShouldReplay(entryPtr, replayTaskIdx, out var sequenceNumber))
                                {
                                    aofProcessor.ProcessAofRecordInternal(virtualSublogIdx, entryPtr, payloadLength, true, out var isCheckpointStart);
                                }
                                maxSequenceNumber = Math.Max(sequenceNumber, maxSequenceNumber);
                                entryLength += TsavoriteLog.UnsafeAlign(payloadLength);
                            }
                            else if (payloadLength < 0)
                            {
                                if (!serverOptions.EnableFastCommit)
                                    throw new GarnetException("Received FastCommit request at replica AOF processor, but FastCommit is not enabled", clientResponse: false);

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
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "{method} failed at replaying", nameof(ContinuousBackgroundReplay));
                        cts.Cancel();
                        break;
                    }
                    finally
                    {
                        // Signal work completion after processing
                        replayBatchContext.LeaderFollowerBarrier.SignalCompleted();
                    }
                }
            }
        }

        public void Throttle() { }

        /// <summary>
        /// Starts a background task to replay and recover data until a specified address or when cancellation is requested.
        /// </summary>
        /// <returns>A Task representing the asynchronous recovery operation.</returns>
        public Task CreateRecoverTask()
        {
            return Task.Run(async () =>
            {
                try
                {
                    logger?.LogInformation("Recover sublog [{physicalSublogIdx}] for addres range ({startAddress},{untilAddress})", physicalSublogIdx, startAddress, untilAddress);
                    while (!cts.IsCancellationRequested)
                    {
                        await replayIterator.BulkConsumeAllAsync(
                            this,
                            serverOptions.ReplicaSyncDelayMs,
                            maxChunkSize: 1 << 20,
                            cts.Token);

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