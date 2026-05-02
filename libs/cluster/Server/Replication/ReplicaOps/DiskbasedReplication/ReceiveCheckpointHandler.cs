// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
#if DEBUG
using Garnet.common;
#endif
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

        // Pending RangeIndex key hash directory name from the header message
        string pendingRangeIndexKeyHashDir;

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
        /// Handles file segments, single-message payloads (metadata), and RangeIndex BfTree snapshots.
        /// <para>
        /// Convention: A <paramref name="startAddress"/> of -1 indicates a single-message payload.
        /// For most types this is checkpoint metadata committed directly.
        /// For <see cref="CheckpointFileType.RINDEX_SNAPSHOT"/>, startAddress -1 carries the key hash
        /// directory name as a header preceding the file data stream.
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
                    case CheckpointFileType.RINDEX_SNAPSHOT:
                        // RangeIndex header: carries key hash directory name for the next file stream
                        pendingRangeIndexKeyHashDir = ValidateAndExtractKeyHashDir(data);
                        return;
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
                    case CheckpointFileType.RINDEX_SNAPSHOT:
                        activeSink = CreateRangeIndexFileSink(token);
                        break;
                    case CheckpointFileType.STORE_HLOG:
                    case CheckpointFileType.STORE_HLOG_OBJ:
                    case CheckpointFileType.STORE_SNAPSHOT:
                    case CheckpointFileType.STORE_SNAPSHOT_OBJ:
                    case CheckpointFileType.STORE_INDEX:
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

        /// <summary>
        /// Creates a <see cref="RangeIndexFileSink"/> for writing a RangeIndex BfTree snapshot file.
        /// Uses the pending key hash directory from the preceding header message.
        /// </summary>
        private RangeIndexFileSink CreateRangeIndexFileSink(Guid token)
        {
            if (string.IsNullOrEmpty(pendingRangeIndexKeyHashDir))
                throw new GarnetException("Received RINDEX_SNAPSHOT file data without a preceding header containing the key hash directory.");

            var dataDir = clusterProvider.storeWrapper.RangeIndexManager?.DataDir;
            if (string.IsNullOrEmpty(dataDir))
                throw new GarnetException("RangeIndex data directory is not configured on this replica.");

            var filePath = System.IO.Path.Combine(dataDir, "rangeindex", pendingRangeIndexKeyHashDir, $"snapshot.{token:N}.bftree");
            pendingRangeIndexKeyHashDir = null;

            return new RangeIndexFileSink(token, filePath, logger);
        }

        /// <summary>
        /// Validates the key hash directory name from the wire.
        /// Must be exactly 32 lowercase hex characters (a Guid formatted with "N").
        /// </summary>
        private static string ValidateAndExtractKeyHashDir(ReadOnlySpan<byte> data)
        {
            var keyHashDir = Encoding.ASCII.GetString(data);

            if (keyHashDir.Length != 32)
                throw new GarnetException($"Invalid RangeIndex key hash directory length: {keyHashDir.Length}, expected 32.");

            foreach (var c in keyHashDir)
            {
                if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
                    throw new GarnetException($"Invalid character '{c}' in RangeIndex key hash directory name.");
            }

            return keyHashDir;
        }
    }
}