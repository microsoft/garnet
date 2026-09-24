// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Receives sequential Tsavorite checkpoint file and metadata frames.
    /// </summary>
    internal sealed class CheckpointFileReceiveHandler : IDisposable
    {
        readonly ICheckpointFileTransferProvider checkpointFileProvider;
        readonly TimeSpan timeout;
        readonly CancellationTokenSource cts = new();
        readonly ILogger logger;
        readonly SemaphoreSlim writeSemaphore = new(0);
        SectorAlignedBufferPool bufferPool;
        ISnapshotDataSink activeSink;

        public CheckpointFileReceiveHandler(
            ICheckpointFileTransferProvider checkpointFileProvider,
            TimeSpan timeout,
            ILogger logger = null)
        {
            this.checkpointFileProvider = checkpointFileProvider;
            this.timeout = timeout;
            this.logger = logger;
        }

        /// <summary>
        /// Writes a checkpoint file segment. An empty payload completes the active file.
        /// </summary>
        public void ProcessFileSegment(Guid token, CheckpointFileType type, long startAddress, ReadOnlySpan<byte> data)
        {
            if (data.Length == 0)
            {
                activeSink?.Complete();
                activeSink = null;
                return;
            }

            if (activeSink == null)
            {
                var device = checkpointFileProvider.CreateCheckpointDevice(type, token);
                bufferPool ??= new SectorAlignedBufferPool(1, (int)device.SectorSize);
                activeSink = new FileDataSink(type, token, device, bufferPool, writeSemaphore, timeout, cts.Token, logger);
            }
            else if (activeSink.Type != type || activeSink.Token != token)
            {
                throw new GarnetException($"Checkpoint frame {type} {token} does not match active file {activeSink.Type} {activeSink.Token}");
            }

            activeSink.WriteChunk(startAddress, data);
        }

        /// <summary>
        /// Commits a complete checkpoint metadata payload.
        /// </summary>
        public void ProcessMetadata(Guid token, CheckpointFileType type, ReadOnlySpan<byte> metadata)
        {
            if (activeSink != null)
                throw new GarnetException("Cannot commit checkpoint metadata while a file transfer is active");

            using var sink = new MetadataDataSink(type, token, checkpointFileProvider);
            sink.WriteChunk(0, metadata);
            sink.Complete();
        }

        public void Dispose()
        {
            cts.Cancel();
            activeSink?.Dispose();
            activeSink = null;
            cts.Dispose();
            writeSemaphore.Dispose();
            bufferPool?.Free();
            bufferPool = null;
        }
    }
}