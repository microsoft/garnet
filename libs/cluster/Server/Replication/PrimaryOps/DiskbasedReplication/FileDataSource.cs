// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
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

        /// <summary>
        /// Coordinates buffer ownership between the caller and the async IO callback.
        /// When the caller times out or is cancelled, the callback takes ownership of the
        /// buffer and returns it to the pool once the IO completes, preventing use-after-free.
        /// </summary>
        private sealed class IOCallbackContext
        {
            public const int Pending = 0;
            public const int CallbackFirst = 1;
            public const int CallerAbandoned = 2;

            public SectorAlignedMemory buffer;
            public int state;
        }

        private readonly int maxBatchSize;
        private readonly TimeSpan timeout;
        private readonly ILogger logger;
        private readonly SectorAlignedBufferPool bufferPool;
        private readonly SemaphoreSlim signalCompletion;
        private volatile bool disposed;
        private volatile uint lastIOErrorCode;

        public CheckpointFileType Type { get; }
        public Guid Token { get; }
        public IDevice Device { get; }
        public long StartOffset { get; }
        public long CurrentOffset { get; private set; }
        public long EndOffset { get; }

        public bool HasNextChunk => CurrentOffset < EndOffset;

        /// <summary>
        /// Creates a new FileDataSource.
        /// </summary>
        /// <param name="type">The checkpoint file type.</param>
        /// <param name="token">The checkpoint token.</param>
        /// <param name="device">The initialized device to read from.</param>
        /// <param name="startOffset">The start offset.</param>
        /// <param name="endOffset">The end offset.</param>
        /// <param name="maxBatchSize">Maximum bytes to read per chunk (will be further capped by sector alignment).</param>
        /// <param name="timeout">Timeout for async read operations.</param>
        /// <param name="bufferPool">Shared sector-aligned buffer pool for read operations.</param>
        /// <param name="signalCompletion">Shared semaphore for async I/O completion signaling.</param>
        /// <param name="logger">Optional logger.</param>
        public FileDataSource(
            CheckpointFileType type,
            Guid token,
            IDevice device,
            long startOffset,
            long endOffset,
            int maxBatchSize,
            TimeSpan timeout,
            SectorAlignedBufferPool bufferPool,
            SemaphoreSlim signalCompletion,
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
            this.bufferPool = bufferPool;
            this.signalCompletion = signalCompletion;
            this.logger = logger;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            disposed = true;
            Device?.Dispose();
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
            long numBytesToRead = size;
            numBytesToRead = (numBytesToRead + (device.SectorSize - 1)) & ~(device.SectorSize - 1);

            var buffer = bufferPool.Get((int)numBytesToRead);
            var ioContext = new IOCallbackContext { buffer = buffer };

            unsafe
            {
                device.ReadAsync(address, (IntPtr)buffer.aligned_pointer, (uint)numBytesToRead, IOCallback, ioContext);
            }

            bool completed;
            try
            {
                completed = await signalCompletion.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Cancellation while waiting — coordinate buffer ownership with the IO callback.
                if (Interlocked.CompareExchange(ref ioContext.state, IOCallbackContext.CallerAbandoned, IOCallbackContext.Pending) == IOCallbackContext.Pending)
                {
                    // Callback hasn't fired yet; it will return the buffer when the IO eventually completes.
                    logger?.LogWarning("Cancelled reading {type} checkpoint file at address {address}; buffer ownership transferred to IO callback", Type, address);
                }
                else
                {
                    // Callback already completed and released the semaphore, but cancellation won the race.
                    // We own the buffer — return it. The semaphore has an extra count but the caller
                    // will throw and no further reads will occur on this instance.
                    buffer.Return();
                }
                throw;
            }

            if (completed)
            {
                // IO completed within the timeout — check for IO errors.
                return HandleIOError(buffer, address, numBytesToRead);
            }

            // Timeout — coordinate buffer ownership with the IO callback.
            if (Interlocked.CompareExchange(ref ioContext.state, IOCallbackContext.CallerAbandoned, IOCallbackContext.Pending) == IOCallbackContext.Pending)
            {
                // Callback hasn't fired yet; it will return the buffer when the IO eventually completes.
                logger?.LogWarning("Timed out reading {type} checkpoint file at address {address}; buffer ownership transferred to IO callback", Type, address);
                ExceptionUtils.ThrowException(new GarnetException(
                    $"Timed out reading {Type} checkpoint file at address {address} (requested {numBytesToRead} bytes)"));
                return default; // Unreachable
            }

            // Callback completed between WaitAsync returning false and our CompareExchange.
            // The IO finished — use the result instead of discarding it.
            return HandleIOError(buffer, address, numBytesToRead);
        }

        private (SectorAlignedMemory buffer, int bytesRead) HandleIOError(
            SectorAlignedMemory buffer, ulong address, long numBytesToRead)
        {
            var errorCode = lastIOErrorCode;
            Debug.Assert(errorCode == 0, $"I/O error {errorCode} reading {Type} checkpoint file at address {address}");
            if (errorCode != 0)
            {
                buffer.Return();
                ExceptionUtils.ThrowException(new GarnetException(
                    $"I/O error {errorCode} reading {Type} checkpoint file at address {address} (requested {numBytesToRead} bytes)"));
            }

            return (buffer, (int)numBytesToRead);
        }

        private void IOCallback(uint errorCode, uint numBytes, object context)
        {
            var ioContext = (IOCallbackContext)context;
            lastIOErrorCode = errorCode;
            if (errorCode != 0)
            {
                var errorMessage = Utility.GetCallbackErrorMessage(errorCode, numBytes, context);
                logger?.LogError("[CheckpointDataSource] ReadAsync error: {errorCode} msg: {errorMessage}", errorCode, errorMessage);
            }

            var prevState = Interlocked.CompareExchange(ref ioContext.state, IOCallbackContext.CallbackFirst, IOCallbackContext.Pending);
            if (prevState == IOCallbackContext.Pending)
            {
                // Normal path: callback arrived before caller abandoned. Release semaphore.
                try
                {
                    _ = signalCompletion.Release();
                }
                catch (ObjectDisposedException) { }
            }
            else
            {
                // Caller abandoned (timeout/cancellation). IO is now complete so the buffer
                // is safe to return — no more writes to aligned_pointer.
                // Guard against the buffer pool having been freed after Dispose.
                if (!disposed)
                    ioContext.buffer.Return();
            }
        }
    }
}