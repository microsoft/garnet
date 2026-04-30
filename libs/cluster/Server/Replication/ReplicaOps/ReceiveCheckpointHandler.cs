// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

        // Active file sink (one at a time, matching the sequential protocol)
        FileDataSink activeFileSink;

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
            activeFileSink?.Dispose();
            activeFileSink = null;
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
                activeFileSink?.Complete();
                activeFileSink = null;
                return;
            }

            if (activeFileSink == null)
            {
                var device = clusterProvider.replicationManager.CreateCheckpointDevice(token, type);
                bufferPool ??= new SectorAlignedBufferPool(1, (int)device.SectorSize);
                activeFileSink = new FileDataSink(type, token, device, bufferPool, writeSemaphore, clusterProvider.serverOptions.ReplicaSyncTimeout, cts.Token, logger);
            }

            activeFileSink.WriteChunk(startAddress, data);

#if DEBUG
            ExceptionInjectionHelper.WaitOnClearAsync(ExceptionInjectionType.Replication_Timeout_On_Receive_Checkpoint).ConfigureAwait(false).GetAwaiter().GetResult();
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
    }
}