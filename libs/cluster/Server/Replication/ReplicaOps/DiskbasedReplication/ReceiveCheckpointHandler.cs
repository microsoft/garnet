// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed class ReceiveCheckpointHandler : IDisposable
    {
        readonly RangeIndexManager rangeIndexManager;
        readonly Action updateLastPrimarySyncTime;
        readonly ILogger logger;
        readonly CheckpointFileReceiveHandler checkpointFileReceiver;
        ISnapshotDataSink activeRangeIndexSink;

        public ReceiveCheckpointHandler(
            ICheckpointFileTransferProvider checkpointFileProvider,
            TimeSpan timeout,
            RangeIndexManager rangeIndexManager = null,
            Action updateLastPrimarySyncTime = null,
            ILogger logger = null)
        {
            this.rangeIndexManager = rangeIndexManager;
            this.updateLastPrimarySyncTime = updateLastPrimarySyncTime;
            this.logger = logger;
            checkpointFileReceiver = new(checkpointFileProvider, timeout, logger);
        }

        public void Dispose()
        {
            checkpointFileReceiver.Dispose();
            activeRangeIndexSink?.Dispose();
            activeRangeIndexSink = null;
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
            updateLastPrimarySyncTime?.Invoke();

            checkpointFileReceiver.ProcessFileSegment(token, type, startAddress, data);

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
            updateLastPrimarySyncTime?.Invoke();
            checkpointFileReceiver.ProcessMetadata(token, type, checkpointMetadata);
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
            updateLastPrimarySyncTime?.Invoke();

            // Single-message payload (startAddress == -1)
            // NOTE: Use for single write metadata or to configure initialization parameters for shipping multi-segments files.
            if (startAddress == -1)
            {
                switch (type)
                {
                    case CheckpointFileType.STORE_RANGEINDEX_FLUSH:
                    case CheckpointFileType.STORE_RANGEINDEX_SNAPSHOT:
                        // Create sink immediately from metadata; data chunks will follow
                        if (rangeIndexManager == null)
                            ExceptionUtils.ThrowException(new GarnetException("RangeIndex not enabled but received RI checkpoint data"));
                        if (activeRangeIndexSink != null)
                            ExceptionUtils.ThrowException(new GarnetException("ActiveSink already initialized!"));
                        activeRangeIndexSink = RangeIndexFileDataSink.FromMetadata(type, token, data, rangeIndexManager, logger);
                        return;
                    case CheckpointFileType.STORE_INDEX:
                    case CheckpointFileType.STORE_SNAPSHOT:
                        checkpointFileReceiver.ProcessMetadata(token, type, data);
                        return;
                    default:
                        ExceptionUtils.ThrowException(new GarnetException($"{nameof(ProcessSnapshotData)} invalid startAddress for checkpoint type: {type}!"));
                        return;
                }
            }

            // File segment handling: empty data signals end-of-stream
            if (data.Length == 0)
            {
                if (type is CheckpointFileType.STORE_RANGEINDEX_FLUSH or CheckpointFileType.STORE_RANGEINDEX_SNAPSHOT)
                {
                    activeRangeIndexSink?.Complete();
                    activeRangeIndexSink = null;
                }
                else
                {
                    checkpointFileReceiver.ProcessFileSegment(token, type, startAddress, data);
                }
                return;
            }

            if (type is CheckpointFileType.STORE_RANGEINDEX_FLUSH or CheckpointFileType.STORE_RANGEINDEX_SNAPSHOT)
            {
                if (activeRangeIndexSink == null)
                    ExceptionUtils.ThrowException(new GarnetException($"RangeIndex sink is not initialized for {type}"));
                activeRangeIndexSink.WriteChunk(startAddress, data);
            }
            else
            {
                checkpointFileReceiver.ProcessFileSegment(token, type, startAddress, data);
            }

#if DEBUG
            ExceptionInjectionHelper.WaitOnClear(ExceptionInjectionType.Replication_Timeout_On_Receive_Checkpoint);
#endif
        }
    }
}