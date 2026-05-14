// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Device-backed implementation of <see cref="ISnapshotDataSink"/> that writes checkpoint
    /// file segments to an <see cref="IDevice"/> using sector-aligned buffered I/O.
    /// </summary>
    internal sealed class FileDataSink : ISnapshotDataSink
    {
        private readonly IDevice device;
        private readonly SectorAlignedBufferPool bufferPool;
        private readonly SemaphoreSlim writeSemaphore;
        private readonly TimeSpan timeout;
        private readonly CancellationToken cancellationToken;
        private readonly ILogger logger;
        private readonly IOCallbackContext ioContext = new();
        private volatile uint lastIOErrorCode;

        public CheckpointFileType Type { get; }
        public Guid Token { get; }

        /// <summary>
        /// Creates a new FileDataSink.
        /// </summary>
        /// <param name="type">The checkpoint file type.</param>
        /// <param name="token">The checkpoint token.</param>
        /// <param name="device">The initialized device to write to.</param>
        /// <param name="bufferPool">Shared sector-aligned buffer pool for write operations.</param>
        /// <param name="writeSemaphore">Shared semaphore for async I/O completion signaling.</param>
        /// <param name="timeout">Timeout for async write operations.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <param name="logger">Optional logger.</param>
        public FileDataSink(
            CheckpointFileType type,
            Guid token,
            IDevice device,
            SectorAlignedBufferPool bufferPool,
            SemaphoreSlim writeSemaphore,
            TimeSpan timeout,
            CancellationToken cancellationToken,
            ILogger logger = null)
        {
            Type = type;
            Token = token;
            this.device = device;
            this.bufferPool = bufferPool;
            this.writeSemaphore = writeSemaphore;
            this.timeout = timeout;
            this.cancellationToken = cancellationToken;
            this.logger = logger;
        }

        /// <inheritdoc/>
        public unsafe void WriteChunk(long startAddress, ReadOnlySpan<byte> data)
        {
            if ((startAddress & (device.SectorSize - 1)) != 0)
                throw new ArgumentException($"startAddress {startAddress} is not aligned to device sector size {device.SectorSize}", nameof(startAddress));

            long numBytesToWrite = data.Length;
            numBytesToWrite = (numBytesToWrite + (device.SectorSize - 1)) & ~(device.SectorSize - 1);

            var pbuffer = bufferPool.Get((int)numBytesToWrite);
            ioContext.Buffer = pbuffer;

            fixed (byte* bufferRaw = data)
                Buffer.MemoryCopy(bufferRaw, pbuffer.aligned_pointer, data.Length, data.Length);

            device.WriteAsync((IntPtr)pbuffer.aligned_pointer, (ulong)startAddress, (uint)numBytesToWrite, IOCallback, ioContext);

            // The IOCallbackContext roots the buffer for GC safety while the IO is in-flight.
            // On timeout or cancellation the buffer is intentionally abandoned (not returned to
            // the pool) — the exception aborts the replication session, so the stale semaphore
            // count left by the callback is harmless.
            if (!writeSemaphore.Wait(timeout, cancellationToken))
            {
                ExceptionUtils.ThrowException(new GarnetException(
                    $"Timed out writing {Type} checkpoint file at address {startAddress} (requested {numBytesToWrite} bytes)"));
            }

            var errorCode = lastIOErrorCode;
            Debug.Assert(errorCode == 0, $"I/O error {errorCode} writing {Type} checkpoint file at address {startAddress}");
            if (errorCode != 0)
            {
                ExceptionUtils.ThrowException(new GarnetException(
                    $"I/O error {errorCode} writing {Type} checkpoint file at address {startAddress} (requested {numBytesToWrite} bytes)"));
            }

            // IO completed successfully — return buffer to pool for reuse.
            pbuffer.Return();
        }

        /// <inheritdoc/>
        public void Complete()
        {
            device?.Dispose();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            device?.Dispose();
        }

        private void IOCallback(uint errorCode, uint numBytes, object context)
        {
            lastIOErrorCode = errorCode;
            if (errorCode != 0)
            {
                var errorMessage = Utility.GetCallbackErrorMessage(errorCode, numBytes, context);
                logger?.LogError("[FileDataSink] WriteAsync error: {errorCode} msg: {errorMessage}", errorCode, errorMessage);
            }

            try
            {
                _ = writeSemaphore.Release();
            }
            catch (ObjectDisposedException) { }
        }
    }
}