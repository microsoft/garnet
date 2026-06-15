// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// Groups activity traces collected during RangeIndex migration operations.
    /// </summary>
    internal static class RangeIndexMigrationActivities
    {
        /// <summary>
        /// Activity trace collected during <see cref="MigrateSession.MigrateRangeIndexKeysAsync"/>.
        /// </summary>
        internal struct MigrateActivity
        {
            private long timestampStart;
            private long timestampTransmitting;
            private long timestampDeleting;
            private long timestampEnd;
            private int keyCount;
            private string error;

            internal static MigrateActivity StartActivity(int keyCount) => new() { timestampStart = Stopwatch.GetTimestamp(), keyCount = keyCount };

            internal void OnTransmitting() => timestampTransmitting = Stopwatch.GetTimestamp();

            internal void OnDeleting() => timestampDeleting = Stopwatch.GetTimestamp();

            internal void OnError(string error) => this.error = error;

            internal void End() => timestampEnd = Stopwatch.GetTimestamp();

            internal void LogActivity(ILogger logger)
            {
                var totalTicks = Stopwatch.GetElapsedTime(timestampStart, timestampEnd).Ticks;
                var waitTransmittingTicks = timestampTransmitting > 0 ? Stopwatch.GetElapsedTime(timestampStart, timestampTransmitting).Ticks : -1;
                var transmittingTicks = timestampTransmitting > 0 && timestampDeleting > 0 ? Stopwatch.GetElapsedTime(timestampTransmitting, timestampDeleting).Ticks : -1;
                var deletingTicks = timestampDeleting > 0 ? Stopwatch.GetElapsedTime(timestampDeleting, timestampEnd).Ticks : -1;
                logger?.LogInformation("MigrateRangeIndexKeysAsync: keyCount={keyCount} isError={isError} errorStr={errorStr} totalTicks={totalTicks} waitTransmittingTicks={waitTransmittingTicks} transmittingTicks={transmittingTicks} deletingTicks={deletingTicks}",
                    keyCount, error != null, error, totalTicks, waitTransmittingTicks, transmittingTicks, deletingTicks);
            }

            internal void EndAndLogActivity(ILogger logger)
            {
                End();
                LogActivity(logger);
            }
        }

        /// <summary>
        /// Activity trace collected during <see cref="MigrateSession.TransmitRangeIndexAsync"/>.
        /// </summary>
        internal struct TransmitActivity
        {
            private long timestampStart;
            private long timestampEnd;
            private long totalBytesSent;
            private long fileSizeBytes;
            private long snapshotTicks;
            private string error;

            internal static TransmitActivity StartActivity() => new() { timestampStart = Stopwatch.GetTimestamp() };

            internal void OnSnapshotCompleted(long fileSizeBytes)
            {
                snapshotTicks = Stopwatch.GetElapsedTime(timestampStart).Ticks;
                this.fileSizeBytes = fileSizeBytes;
            }

            internal void OnChunkSent(int bytesSent) => totalBytesSent += bytesSent;

            internal void OnError(string error) => this.error = error;

            internal void End() => timestampEnd = Stopwatch.GetTimestamp();

            internal void LogActivity(ILogger logger, byte[] keyBytes)
            {
                var totalTicks = Stopwatch.GetElapsedTime(timestampStart, timestampEnd).Ticks;
                var transmitTicks = totalTicks - snapshotTicks;
                logger?.LogInformation("TransmitRangeIndexAsync: key={key} isError={isError} errorStr={errorStr} fileSizeBytes={fileSizeBytes} totalBytesSent={totalBytesSent} snapshotTicks={snapshotTicks} transmitTicks={transmitTicks} totalTicks={totalTicks}",
                    Encoding.UTF8.GetString(keyBytes), error != null, error, fileSizeBytes, totalBytesSent, snapshotTicks, transmitTicks, totalTicks);
            }

            internal void EndAndLogActivity(ILogger logger, byte[] keyBytes)
            {
                End();
                LogActivity(logger, keyBytes);
            }
        }

        /// <summary>
        /// Activity trace collected during a single RangeIndex receive operation (one key stream).
        /// </summary>
        internal sealed class ReceiveActivity
        {
            private readonly long timestampFirstChunk;
            private long timestampPublish;
            private long timestampEnd;
            private int chunkCount;
            private long totalBytesReceived;
            private string error;
            private bool sessionDisposed;
            private RangeIndexManager.PublishMigratedIndexResult? publishResult;

            private ReceiveActivity() => timestampFirstChunk = Stopwatch.GetTimestamp();

            internal int ChunkCount => chunkCount;

            internal static ReceiveActivity StartActivity() => new();

            internal void OnChunkReceived(int chunkLength)
            {
                chunkCount++;
                totalBytesReceived += chunkLength;
            }

            internal void OnPublishing() => timestampPublish = Stopwatch.GetTimestamp();

            internal void OnPublishResult(RangeIndexManager.PublishMigratedIndexResult result) => publishResult = result;

            internal void OnError(string error) => this.error = error;

            internal void OnSessionDisposed() => sessionDisposed = true;

            internal void End() => timestampEnd = Stopwatch.GetTimestamp();

            internal void LogActivity(ILogger logger, ReadOnlySpan<byte> keyBytes)
            {
                var totalTicks = Stopwatch.GetElapsedTime(timestampFirstChunk, timestampEnd).Ticks;
                var publishTicks = timestampPublish > 0 ? Stopwatch.GetElapsedTime(timestampPublish, timestampEnd).Ticks : -1;
                logger?.LogInformation("RangeIndexMigrationReceive: key={key} isError={isError} errorStr={errorStr} sessionDisposed={sessionDisposed} publishResult={publishResult} chunkCount={chunkCount} totalBytesReceived={totalBytesReceived} publishTicks={publishTicks} totalTicks={totalTicks}",
                    keyBytes.Length > 0 ? Encoding.UTF8.GetString(keyBytes) : "null", error != null, error, sessionDisposed, publishResult?.ToString() ?? "n/a", chunkCount, totalBytesReceived, publishTicks, totalTicks);
            }

            internal void EndAndLogActivity(ILogger logger, ReadOnlySpan<byte> keyBytes)
            {
                End();
                LogActivity(logger, keyBytes);
            }
        }
    }
}