// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed class ReceiveCheckpointHandler : IDisposable
    {
        readonly ClusterProvider clusterProvider;
        readonly CancellationTokenSource cts;
        readonly ILogger logger;

        // Shared resources across FileDataSink instances
        SectorAlignedBufferPool bufferPool;
        readonly SemaphoreSlim writeSemaphore = new(0);

        // Active sink (one at a time, matching the sequential protocol)
        ISnapshotDataSink activeSink;

        // Received chunks an overlapped file writer may hold, queued or being written; 0 writes each chunk synchronously.
        readonly int writeBuffers;

        // Overlapped sinks still closing after their end-of-stream marker. Their writers return buffers to bufferPool,
        // so Dispose closes them before freeing it. Locked because Dispose can run on another thread when a sync attempt
        // is abandoned.
        readonly List<OverlappedFileDataSink> completingSinks = [];

        public ReceiveCheckpointHandler(ClusterProvider clusterProvider, ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.logger = logger;
            writeBuffers = clusterProvider.serverOptions.ReplicaDiskBasedSyncWriteBuffers;
            cts = new();
        }

        public void Dispose()
        {
            // Cancel first so overlapped writers stop waiting; each sink's Dispose then waits for its writer to finish,
            // so the token source is disposed only after nothing can still be waiting on it.
            cts.Cancel();
            activeSink?.Dispose();
            activeSink = null;
            lock (completingSinks)
            {
                foreach (var sink in completingSinks)
                    sink.Dispose();
            }
            cts.Dispose();
            writeSemaphore?.Dispose();
            bufferPool?.Free();
            bufferPool = null;
        }

        /// <summary>
        /// Process file segments sent from primary.
        /// An empty data span signals end-of-stream for the current file.
        /// </summary>
        /// <param name="token">The checkpoint token.</param>
        /// <param name="type">The checkpoint file type.</param>
        /// <param name="startAddress">The start address for this chunk.</param>
        /// <param name="data">The data to write. Empty signals end-of-stream.</param>
        public void ProcessFileSegment(Guid token, CheckpointFileType type, long startAddress, ReadOnlySpan<byte> data)
        {
            clusterProvider.replicationManager.UpdateLastPrimarySyncTime();

            if (data.Length == 0)
            {
                activeSink?.Complete();
                activeSink = null;
                return;
            }

            if (activeSink == null)
            {
                // On retry, this may reopen an existing file from a previous failed attempt.
                // This is safe because chunks are streamed from the start, overwriting any partial data.
                var device = clusterProvider.replicationManager.CreateCheckpointDevice(token, type);
                bufferPool ??= new SectorAlignedBufferPool(1, (int)device.SectorSize);
                activeSink = new FileDataSink(type, token, device, bufferPool, writeSemaphore, clusterProvider.serverOptions.ReplicaSyncTimeout, cts.Token, logger);
            }

            activeSink.WriteChunk(startAddress, data);

#if DEBUG
            ExceptionInjectionHelper.WaitOnClear(ExceptionInjectionType.Replication_Timeout_On_Receive_Checkpoint);
#endif
        }

        /// <summary>
        /// Process checkpoint metadata transmitted from primary during replica synchronization.
        /// </summary>
        /// <param name="token">Checkpoint metadata token.</param>
        /// <param name="type">Checkpoint metadata filetype.</param>
        /// <param name="checkpointMetadata">Raw bytes of checkpoint metadata.</param>
        public void ProcessMetadata(Guid token, CheckpointFileType type, ReadOnlySpan<byte> checkpointMetadata)
        {
            clusterProvider.replicationManager.UpdateLastPrimarySyncTime();
            using var sink = new MetadataDataSink(type, token, clusterProvider);
            sink.WriteChunk(0, checkpointMetadata);
            sink.Complete();
        }

        /// <summary>
        /// Unified entry point for receiving snapshot data from primary.
        /// Handles file segments and single-message payloads (metadata).
        /// <para>
        /// Convention: A <paramref name="startAddress"/> of -1 indicates a single-message payload
        /// that fits in one message (e.g., checkpoint metadata committed directly).
        /// Any other startAddress indicates a streamed file segment. Empty data signals end-of-stream.
        /// </para>
        /// </summary>
        /// <param name="token">The checkpoint token.</param>
        /// <param name="type">The checkpoint file type.</param>
        /// <param name="startAddress">The start address for this chunk, or -1 for single-message payloads.</param>
        /// <param name="data">The data to write. Empty signals end-of-stream for streamed file segments.</param>
        /// <returns>
        /// A task that completes once the payload is written: for a file chunk, once its write completes; for an
        /// end-of-stream marker, once every write of the file has completed and the file is closed. The caller must not
        /// acknowledge the payload before then. Only overlapped file writes return an incomplete task. <paramref name="data"/>
        /// has been consumed (copied) when this returns, which may block while every write buffer is in use.
        /// </returns>
        public Task ProcessSnapshotDataAsync(Guid token, CheckpointFileType type, long startAddress, ReadOnlySpan<byte> data)
        {
            clusterProvider.replicationManager.UpdateLastPrimarySyncTime();

            // Single-message payload (startAddress == -1)
            // NOTE: Use for single write metadata or to configure initialization parameters for shipping multi-segments files.
            if (startAddress == -1)
            {
                switch (type)
                {
                    case CheckpointFileType.STORE_RANGEINDEX_FLUSH:
                    case CheckpointFileType.STORE_RANGEINDEX_SNAPSHOT:
                        // Create sink immediately from metadata; data chunks will follow
                        if (!clusterProvider.serverOptions.EnableRangeIndexPreview)
                            ExceptionUtils.ThrowException(new GarnetException("RangeIndex not enabled but received RI checkpoint data"));
                        if (activeSink != null)
                            ExceptionUtils.ThrowException(new GarnetException("ActiveSink already initialized!"));
                        activeSink = RangeIndexFileDataSink.FromMetadata(type, token, data, clusterProvider.rangeIndexManager, logger);
                        return Task.CompletedTask;
                    case CheckpointFileType.STORE_INDEX:
                    case CheckpointFileType.STORE_SNAPSHOT:
                        var sink = new MetadataDataSink(type, token, clusterProvider);
                        try
                        {
                            sink.WriteChunk(0, data);
                            sink.Complete();
                            return Task.CompletedTask;
                        }
                        finally
                        {
                            sink.Dispose();
                        }
                    default:
                        ExceptionUtils.ThrowException(new GarnetException($"{nameof(ProcessSnapshotDataAsync)} invalid startAddress for checkpoint type: {type}!"));
                        return Task.CompletedTask;
                }
            }

            // File segment handling: empty data signals end-of-stream
            if (data.Length == 0)
            {
                var completedSink = activeSink;
                activeSink = null;
                if (completedSink is OverlappedFileDataSink overlappedSink)
                {
                    lock (completingSinks)
                        completingSinks.Add(overlappedSink);
                    return overlappedSink.CompleteAsync();
                }
                completedSink?.Complete();
                return Task.CompletedTask;
            }

            // Initialize sink for multi-segment file transmission
            if (activeSink == null)
            {
                switch (type)
                {
                    case CheckpointFileType.STORE_HLOG:
                    case CheckpointFileType.STORE_HLOG_OBJ:
                    case CheckpointFileType.STORE_SNAPSHOT:
                    case CheckpointFileType.STORE_SNAPSHOT_OBJ:
                    case CheckpointFileType.STORE_INDEX:
                        // On retry, this may reopen an existing file from a previous failed attempt.
                        // This is safe because chunks are streamed from the start, overwriting any partial data.
                        var device = clusterProvider.replicationManager.CreateCheckpointDevice(token, type);
                        bufferPool ??= new SectorAlignedBufferPool(1, (int)device.SectorSize);
                        var timeout = clusterProvider.serverOptions.ReplicaSyncTimeout;
                        activeSink = writeBuffers > 0
                            ? new OverlappedFileDataSink(type, token, device, bufferPool, writeBuffers, timeout, cts.Token, logger)
                            : new FileDataSink(type, token, device, bufferPool, writeSemaphore, timeout, cts.Token, logger);
                        break;
                    default:
                        ExceptionUtils.ThrowException(new GarnetException($"{nameof(ProcessSnapshotDataAsync)} invalid startAddress for checkpoint type: {type}!"));
                        return Task.CompletedTask;
                }
            }

            var written = Task.CompletedTask;
            if (activeSink is OverlappedFileDataSink overlapped)
                written = overlapped.WriteChunkAsync(startAddress, data);
            else
                activeSink.WriteChunk(startAddress, data);

#if DEBUG
#pragma warning disable VSTHRD103 // Deliberately holds the receiving thread, as a slow synchronous receive would.
            ExceptionInjectionHelper.WaitOnClear(ExceptionInjectionType.Replication_Timeout_On_Receive_Checkpoint);
#pragma warning restore VSTHRD103
#endif
            return written;
        }
    }
}