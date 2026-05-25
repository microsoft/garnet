// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
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

        public ReceiveCheckpointHandler(ClusterProvider clusterProvider, ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.logger = logger;
            cts = new();
        }

        public void Dispose()
        {
            cts.Cancel();
            cts.Dispose();
            activeSink?.Dispose();
            activeSink = null;
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
        public void ProcessSnapshotData(Guid token, CheckpointFileType type, long startAddress, ReadOnlySpan<byte> data)
        {
            clusterProvider.replicationManager.UpdateLastPrimarySyncTime();

            // Single-message payload (startAddress == -1)
            // NOTE: Use for single write metadata or to configure initialization parameters for shipping multi-segments files.
            if (startAddress == -1)
            {
                switch (type)
                {
                    case CheckpointFileType.STORE_INDEX:
                    case CheckpointFileType.STORE_SNAPSHOT:
                        var sink = new MetadataDataSink(type, token, clusterProvider);
                        try
                        {
                            sink.WriteChunk(0, data);
                            sink.Complete();
                            return;
                        }
                        finally
                        {
                            sink.Dispose();
                        }
                    default:
                        ExceptionUtils.ThrowException(new GarnetException($"{nameof(ProcessSnapshotData)} invalid startAddress for checkpoint type: {type}!"));
                        return;
                }
            }

            // File segment handling: empty data signals end-of-stream
            if (data.Length == 0)
            {
                activeSink?.Complete();
                activeSink = null;
                return;
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
                        activeSink = new FileDataSink(type, token, device, bufferPool, writeSemaphore, clusterProvider.serverOptions.ReplicaSyncTimeout, cts.Token, logger);
                        break;
                    default:
                        ExceptionUtils.ThrowException(new GarnetException($"{nameof(ProcessSnapshotData)} invalid startAddress for checkpoint type: {type}!"));
                        return;
                }
            }

            activeSink.WriteChunk(startAddress, data);

#if DEBUG
            ExceptionInjectionHelper.WaitOnClear(ExceptionInjectionType.Replication_Timeout_On_Receive_Checkpoint);
#endif
        }
    }
}