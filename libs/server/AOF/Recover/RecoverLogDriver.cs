// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    internal sealed class RecoverLogDriver : IBulkLogEntryConsumer, IDisposable
    {
        readonly int physicalSublogIdx;
        readonly AofProcessor aofProcessor;
        readonly GarnetServerOptions serverOptions;
        readonly GarnetAppendOnlyFile appendOnlyFile;
        readonly TsavoriteLogScanSingleIterator replayIterator;
        readonly TsavoriteLog physicalSublog;
        readonly CancellationTokenSource cts = new();
        readonly ILogger logger = null;
        readonly long startAddress;
        readonly long untilAddress;
        readonly int dbId;
        public long ReplayedRecordCount { get; private set; } = 0;

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
        public RecoverLogDriver(
            AofProcessor aofProcessor,
            GarnetAppendOnlyFile appendOnlyFile,
            GarnetServerOptions serverOptions,
            int dbId,
            int physicalSublogIdx,
            long startAddress,
            long untilAddress,
            ILogger logger = null)
        {
            this.dbId = dbId;
            this.physicalSublogIdx = physicalSublogIdx;
            this.aofProcessor = aofProcessor;
            this.appendOnlyFile = appendOnlyFile;
            this.serverOptions = serverOptions;
            this.startAddress = startAddress;
            this.untilAddress = untilAddress;
            replayIterator = appendOnlyFile.ScanSingle(physicalSublogIdx, startAddress, untilAddress, scanUncommitted: true, recover: false, logger: logger);
            physicalSublog = appendOnlyFile.Log.GetSubLog(physicalSublogIdx);
            this.logger = logger;
        }

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
                // TODO: parallel replay page
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
                {

                }
            });
        }
    }
}