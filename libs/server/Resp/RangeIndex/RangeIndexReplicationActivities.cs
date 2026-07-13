// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Groups activity traces collected during RangeIndex AOF replication.
    /// </summary>
    internal static class RangeIndexReplicationActivities
    {
        /// <summary>
        /// Activity trace for streaming one migrated Range Index's snapshot file into the AOF as chunked
        /// <c>RangeIndexStreamChunk</c> entries. Started when streaming begins and logged (with the key)
        /// when it ends, whether it succeeded or threw.
        /// </summary>
        internal sealed class StreamActivity
        {
            private readonly long timestampStart;
            private readonly int chunkSize;
            private long fileSizeBytes;
            private int chunkCount;
            private long totalBytesEnqueued;
            private string error;

            private StreamActivity(int chunkSize)
            {
                timestampStart = Stopwatch.GetTimestamp();
                this.chunkSize = chunkSize;
            }

            internal static StreamActivity StartActivity(int chunkSize) => new(chunkSize);

            internal void OnFileLength(long fileBytes) => fileSizeBytes = fileBytes;

            internal void OnChunkEnqueued(int bytes)
            {
                chunkCount++;
                totalBytesEnqueued += bytes;
            }

            internal void OnError(string error) => this.error ??= error;

            internal void EndAndLog(ILogger logger, ReadOnlySpan<byte> key)
            {
                if (logger == null)
                    return;

                var totalTicks = Stopwatch.GetElapsedTime(timestampStart).Ticks;
                logger.LogInformation("RangeIndexReplicationStreamActivity: key={key} isError={isError} errorStr={errorStr} chunkSize={chunkSize} fileSizeBytes={fileSizeBytes} chunkCount={chunkCount} totalBytesEnqueued={totalBytesEnqueued} totalTicks={totalTicks}",
                    Encoding.UTF8.GetString(key), error != null, error, chunkSize, fileSizeBytes, chunkCount, totalBytesEnqueued, totalTicks);
            }
        }

        /// <summary>
        /// Activity trace for reassembling one migrated Range Index's AOF stream on the replica / recovery side.
        /// </summary>
        internal sealed class ReassemblyActivity
        {
            private readonly long timestampStart;
            private int chunkCount;
            private long totalBytesReceived;
            private RangeIndexManager.PublishMigratedIndexResult? publishResult;

            private ReassemblyActivity() => timestampStart = Stopwatch.GetTimestamp();

            internal static ReassemblyActivity StartActivity() => new();

            internal void OnChunkReceived(int chunkLength)
            {
                chunkCount++;
                totalBytesReceived += chunkLength;
            }

            internal void OnPublishResult(RangeIndexManager.PublishMigratedIndexResult result) => publishResult = result;

            internal void EndAndLog(ILogger logger, ReadOnlySpan<byte> key, string reason)
            {
                if (logger == null)
                    return;

                var totalTicks = Stopwatch.GetElapsedTime(timestampStart).Ticks;
                logger.LogInformation("RangeIndexReplicationReassemblyActivity: key={key} reason={reason} publishResult={publishResult} chunkCount={chunkCount} totalBytesReceived={totalBytesReceived} totalTicks={totalTicks}",
                    Encoding.UTF8.GetString(key), reason, publishResult?.ToString() ?? "n/a", chunkCount, totalBytesReceived, totalTicks);
            }
        }
    }
}