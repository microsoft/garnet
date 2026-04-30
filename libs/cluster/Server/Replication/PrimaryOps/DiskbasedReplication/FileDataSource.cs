// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Base class for checkpoint data sources that reads from an IDevice using sector-aligned async I/O.
    /// Subclasses can override <see cref="ReadIntoAsync"/> to customize read behavior.
    /// </summary>
    internal class FileDataSource : ISnapshotDataSource
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
        public FileDataSource(
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
        public async Task<DataSourceReadResult> ReadNextChunkAsync(CancellationToken cancellationToken = default)
        {
            var chunkStartAddress = CurrentOffset;
            var remainingBytes = EndOffset - CurrentOffset;
            var size = (int)Math.Min(remainingBytes, maxBatchSize);

            var (buffer, bytesRead) = await ReadIntoAsync(Device, (ulong)CurrentOffset, size, cancellationToken).ConfigureAwait(false);
            CurrentOffset += bytesRead;

            return new DataSourceReadResult(buffer, bytesRead, chunkStartAddress);
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
}