// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Represents the result of a chunk read from a checkpoint data source.
    /// </summary>
    internal readonly struct ChunkReadResult
    {
        /// <summary>
        /// The buffer containing the read data.
        /// </summary>
        public readonly SectorAlignedMemory Buffer;

        /// <summary>
        /// The number of bytes read (sector-aligned).
        /// </summary>
        public readonly int BytesRead;

        /// <summary>
        /// The start address of this chunk in the source device.
        /// </summary>
        public readonly long ChunkStartAddress;

        public ChunkReadResult(SectorAlignedMemory buffer, int bytesRead, long chunkStartAddress)
        {
            Buffer = buffer;
            BytesRead = bytesRead;
            ChunkStartAddress = chunkStartAddress;
        }
    }

    /// <summary>
    /// Interface for a checkpoint data source that provides chunk-based reading from a device.
    /// </summary>
    internal interface ICheckpointDataSource : IDisposable
    {
        /// <summary>
        /// The type of checkpoint file this data source represents.
        /// </summary>
        CheckpointFileType Type { get; }

        /// <summary>
        /// The token identifying the checkpoint file.
        /// </summary>
        Guid Token { get; }

        /// <summary>
        /// The start offset in the device.
        /// </summary>
        long StartOffset { get; }

        /// <summary>
        /// The current read offset in the device.
        /// </summary>
        long CurrentOffset { get; }

        /// <summary>
        /// The end offset in the device.
        /// </summary>
        long EndOffset { get; }

        /// <summary>
        /// Whether there are remaining chunks to read.
        /// </summary>
        bool HasNextChunk { get; }

        /// <summary>
        /// Reads the next chunk from the underlying device asynchronously.
        /// Advances CurrentOffset by the number of bytes read.
        /// The caller is responsible for returning the buffer via <see cref="ChunkReadResult.Buffer"/>.Return().
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A <see cref="ChunkReadResult"/> containing the buffer, bytes read, and chunk start address.</returns>
        Task<ChunkReadResult> ReadNextChunkAsync(CancellationToken cancellationToken = default);
    }

    /// <summary>
    /// Interface for a checkpoint reader that provides an enumeration of data sources.
    /// </summary>
    internal interface ICheckpointReader
    {
        /// <summary>
        /// Returns an enumeration of checkpoint data sources with initialized devices.
        /// The caller is responsible for disposing each data source after use.
        /// </summary>
        IEnumerable<ICheckpointDataSource> GetDataSources();
    }

    /// <summary>
    /// Base class for checkpoint data sources that reads from an IDevice using sector-aligned async I/O.
    /// Subclasses can override <see cref="ReadIntoAsync"/> to customize read behavior.
    /// </summary>
    internal class CheckpointDataSource : ICheckpointDataSource
    {
        /// <summary>
        /// Default batch size for non-segmented checkpoint files.
        /// </summary>
        internal const int DefaultBatchSize = 1 << 17;

        private readonly int maxBatchSize;
        private readonly TimeSpan timeout;
        private readonly ILogger logger;
        private SectorAlignedBufferPool bufferPool;
        private readonly SemaphoreSlim signalCompletion = new(0);

        public CheckpointFileType Type { get; }
        public Guid Token { get; }
        public IDevice Device { get; }
        public long StartOffset { get; }
        public long CurrentOffset { get; private set; }
        public long EndOffset { get; }

        public bool HasNextChunk => CurrentOffset < EndOffset;

        /// <summary>
        /// Creates a new CheckpointDataSource.
        /// </summary>
        /// <param name="type">The checkpoint file type.</param>
        /// <param name="token">The checkpoint token.</param>
        /// <param name="device">The initialized device to read from.</param>
        /// <param name="startOffset">The start offset.</param>
        /// <param name="endOffset">The end offset.</param>
        /// <param name="maxBatchSize">Maximum bytes to read per chunk (will be further capped by sector alignment).</param>
        /// <param name="timeout">Timeout for async read operations.</param>
        /// <param name="logger">Optional logger.</param>
        public CheckpointDataSource(
            CheckpointFileType type,
            Guid token,
            IDevice device,
            long startOffset,
            long endOffset,
            int maxBatchSize,
            TimeSpan timeout,
            ILogger logger = null)
        {
            Type = type;
            Token = token;
            Device = device;
            StartOffset = startOffset;
            CurrentOffset = startOffset;
            EndOffset = endOffset;
            this.maxBatchSize = maxBatchSize;
            this.timeout = timeout;
            this.logger = logger;
        }

        /// <inheritdoc/>
        public async Task<ChunkReadResult> ReadNextChunkAsync(CancellationToken cancellationToken = default)
        {
            var chunkStartAddress = CurrentOffset;
            var remainingBytes = EndOffset - CurrentOffset;
            var size = (int)Math.Min(remainingBytes, maxBatchSize);

            var (buffer, bytesRead) = await ReadIntoAsync(Device, (ulong)CurrentOffset, size, cancellationToken).ConfigureAwait(false);
            CurrentOffset += bytesRead;

            return new ChunkReadResult(buffer, bytesRead, chunkStartAddress);
        }

        /// <summary>
        /// Reads data from the device into a sector-aligned buffer.
        /// Override this method to customize how reads are performed (e.g., segmented reads).
        /// </summary>
        /// <param name="device">The device to read from.</param>
        /// <param name="address">The address to read from.</param>
        /// <param name="size">The requested number of bytes (will be sector-aligned).</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A tuple of the allocated buffer and the actual (sector-aligned) bytes read.</returns>
        protected virtual async Task<(SectorAlignedMemory buffer, int bytesRead)> ReadIntoAsync(
            IDevice device,
            ulong address,
            int size,
            CancellationToken cancellationToken = default)
        {
            bufferPool ??= new SectorAlignedBufferPool(1, (int)device.SectorSize);

            long numBytesToRead = size;
            numBytesToRead = (numBytesToRead + (device.SectorSize - 1)) & ~(device.SectorSize - 1);

            var buffer = bufferPool.Get((int)numBytesToRead);
            unsafe
            {
                device.ReadAsync(address, (IntPtr)buffer.aligned_pointer, (uint)numBytesToRead, IOCallback, null);
            }
            _ = await signalCompletion.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
            return (buffer, (int)numBytesToRead);
        }

        private void IOCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                var errorMessage = Tsavorite.core.Utility.GetCallbackErrorMessage(errorCode, numBytes, context);
                logger?.LogError("[CheckpointDataSource] ReadAsync error: {errorCode} msg: {errorMessage}", errorCode, errorMessage);
            }
            _ = signalCompletion.Release();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            signalCompletion?.Dispose();
            bufferPool?.Free();
            Device?.Dispose();
        }
    }

    /// <summary>
    /// Drives checkpoint transmission by iterating over checkpoint readers and sending segments.
    /// </summary>
    internal sealed class CheckpointTransmissionDriver : IDisposable
    {
        readonly ICheckpointReader[] checkpointReaders;
        readonly CheckpointReadContext readContext;
        readonly ILogger logger;

        public CheckpointTransmissionDriver(ICheckpointReader[] checkpointReaders, CheckpointReadContext readContext, ILogger logger = null)
        {
            this.checkpointReaders = checkpointReaders;
            this.readContext = readContext;
            this.logger = logger;
        }

        public void Dispose()
        {
            readContext?.Dispose();
        }

        /// <summary>
        /// Sends all checkpoint segments by iterating data sources from each reader.
        /// </summary>
        public async Task SendCheckpointAsync(CancellationToken cancellationToken = default)
        {
            foreach (var checkpointReader in checkpointReaders)
            {
                foreach (var dataSource in checkpointReader.GetDataSources())
                {
                    try
                    {
                        logger?.LogInformation("<Begin sending checkpoint file segments {token} {type} {startAddress} {endAddress}",
                            dataSource.Token, dataSource.Type, dataSource.StartOffset, dataSource.EndOffset);

                        await readContext.SendSegmentsAsync(dataSource, cancellationToken).ConfigureAwait(false);

                        logger?.LogInformation("<Complete sending checkpoint file segments {token} {type} {startAddress} {endAddress}",
                            dataSource.Token, dataSource.Type, dataSource.StartOffset, dataSource.EndOffset);
                    }
                    finally
                    {
                        dataSource.Dispose();
                    }
                }
            }
        }
    }
}
