// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Device-backed <see cref="ISnapshotDataSink"/> that lets the replica keep receiving while it writes.
    /// <para>
    /// The receiving thread copies each chunk into a sector-aligned buffer and queues it. Writes are sequential unless
    /// concurrency is enabled; non-overlapping chunks may then complete out of order. A chunk is accepted once queued; completing the sink
    /// waits for every queued write and reports the first failure.
    /// </para>
    /// <para>
    /// Up to <c>writeBuffers</c> received chunks may be held, queued or being written; beyond that
    /// <see cref="WriteChunk"/> blocks the receiving thread, which is the backpressure point. After the first failed write,
    /// later queued chunks are discarded and no further chunk is accepted.
    /// </para>
    /// </summary>
    internal sealed class OverlappedFileDataSink : ISnapshotDataSink
    {
        private readonly IDevice device;
        private readonly SectorAlignedBufferPool bufferPool;
        private readonly SemaphoreSlim freeBuffers;
        private readonly TimeSpan timeout;
        private readonly CancellationToken cancellationToken;
        private readonly ILogger logger;
        private readonly bool writeBuffersConcurrently;
        // Single reader: the writer task. Not single-writer: Dispose can complete the channel from the thread abandoning a
        // sync attempt while the receiving thread queues a chunk.
        private readonly Channel<PendingWrite> queue =
            Channel.CreateUnbounded<PendingWrite>(new UnboundedChannelOptions { SingleReader = true });
        private readonly Task writer;
        private Exception failure;
        private int disposed;
        // Only the receiving thread advances this; sector-padded ranges cannot overlap outstanding writes.
        private long acceptedUntilAddress;

        private readonly record struct PendingWrite(long Address, SectorAlignedMemory Buffer, uint Length);

        /// <inheritdoc/>
        public CheckpointFileType Type { get; }

        /// <inheritdoc/>
        public Guid Token { get; }

        /// <summary>
        /// Creates a sink that owns <paramref name="device"/> and starts its writer.
        /// </summary>
        /// <param name="type">The checkpoint file type.</param>
        /// <param name="token">The checkpoint token.</param>
        /// <param name="device">The initialized device to write to; disposed when the sink completes or is disposed.</param>
        /// <param name="bufferPool">Shared sector-aligned buffer pool for queued chunks.</param>
        /// <param name="writeBuffers">Received chunks that may be held, queued or being written; at least 1.</param>
        /// <param name="timeout">Timeout for each device write and for waiting on a free buffer.</param>
        /// <param name="cancellationToken">Cancels waiting and in-progress writes when the receive is abandoned.</param>
        /// <param name="logger">Optional logger.</param>
        /// <param name="writeBuffersConcurrently">Whether to issue multiple positional writes concurrently.</param>
        public OverlappedFileDataSink(
            CheckpointFileType type,
            Guid token,
            IDevice device,
            SectorAlignedBufferPool bufferPool,
            int writeBuffers,
            TimeSpan timeout,
            CancellationToken cancellationToken,
            ILogger logger = null,
            bool writeBuffersConcurrently = false)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(writeBuffers, 1);
            Type = type;
            Token = token;
            this.device = device;
            this.bufferPool = bufferPool;
            this.timeout = timeout;
            this.cancellationToken = cancellationToken;
            this.logger = logger;
            this.writeBuffersConcurrently = writeBuffersConcurrently;
            freeBuffers = new SemaphoreSlim(writeBuffers, writeBuffers);
            writer = Task.Run(WriteQueuedChunksAsync);
        }

        /// <summary>
        /// Copies a chunk and queues its write. Completion of the sink confirms that every chunk was written.
        /// Blocks while <c>writeBuffers</c> earlier chunks are still queued or being written.
        /// </summary>
        /// <param name="startAddress">The sector-aligned start address for this chunk.</param>
        /// <param name="data">The chunk; copied before this method returns.</param>
        public void WriteChunk(long startAddress, ReadOnlySpan<byte> data)
        {
            if ((startAddress & (device.SectorSize - 1)) != 0)
                throw new ArgumentException($"startAddress {startAddress} is not aligned to device sector size {device.SectorSize}", nameof(startAddress));

            ThrowIfFailed();
            var length = (uint)((data.Length + (device.SectorSize - 1)) & ~(device.SectorSize - 1));
            if (startAddress < acceptedUntilAddress)
                throw new ArgumentException($"startAddress {startAddress} overlaps an earlier checkpoint chunk ending at {acceptedUntilAddress}", nameof(startAddress));
            var endAddress = checked(startAddress + length);

#pragma warning disable VSTHRD103 // Blocking the receiving thread is the backpressure: it stops reading the primary's chunks.
            if (!freeBuffers.Wait(timeout, cancellationToken))
#pragma warning restore VSTHRD103
                ExceptionUtils.ThrowException(new GarnetException(
                    $"Timed out waiting for a write buffer for {Type} checkpoint file at address {startAddress}"));

            // The writer may have failed while this thread waited; its slot was released so the failure is observed here.
            if (Volatile.Read(ref failure) is { } previous)
            {
                _ = freeBuffers.Release();
                ExceptionDispatchInfo.Throw(previous);
            }

            SectorAlignedMemory buffer = null;
            try
            {
                // The pool zeroes this buffer when it is returned. Skipping that (clearOnReturn: false, plus zeroing the
                // sector padding after a short chunk) saves work, but in a test to compare whether we should zero, it
                // made transfers slower: the replica was not the bottleneck, and with both ends on one machine the
                // sender's loopback copy into this process's receive buffer slowed, most likely because the zeroing had
                // been evicting those cache lines. Between machines the replica may be the bottleneck, where skipping
                // could help, so measure there first.
                buffer = bufferPool.Get((int)length);
                unsafe
                {
                    fixed (byte* source = data)
                        Buffer.MemoryCopy(source, buffer.aligned_pointer, data.Length, data.Length);
                }

                if (!queue.Writer.TryWrite(new PendingWrite(startAddress, buffer, length)))
                    throw new InvalidOperationException($"The {Type} checkpoint sink is already complete.");
                acceptedUntilAddress = endAddress;
            }
            catch
            {
                // Not queued, so this thread still owns the buffer and its slot.
                buffer?.Return();
                _ = freeBuffers.Release();
                throw;
            }
        }

        /// <summary>
        /// Completes after every queued write has completed and the device is closed; faults with the first write failure.
        /// </summary>
        public async Task CompleteAsync()
        {
            _ = queue.Writer.TryComplete();
            try
            {
                await writer.ConfigureAwait(false);
                if (Volatile.Read(ref failure) is { } error)
                    ExceptionDispatchInfo.Throw(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <summary>
        /// Waits for every queued write and closes the device; <see cref="ISnapshotDataSink"/> compatibility.
        /// </summary>
        public void Complete() => AsyncUtils.BlockingWait(CompleteAsync());

        /// <summary>
        /// Stops accepting chunks, waits for the writer (cancel the token first to abandon queued writes), and closes the device.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
                return;

            _ = queue.Writer.TryComplete();
            try
            {
                // Each write is bounded by the timeout, so the writer finishes; the device must not close under it.
                AsyncUtils.BlockingWait(writer);
            }
            catch (Exception ex)
            {
                logger?.LogDebug(ex, "Checkpoint writer for {type} ended with an error", Type);
            }
            device.Dispose();
        }

        private void ThrowIfFailed()
        {
            if (Volatile.Read(ref failure) is { } error)
                ExceptionDispatchInfo.Throw(error);
        }

        // Buffer slots bound queued and in-flight writes together. Retain unfinished or faulted tasks until EOF.
        private async Task WriteQueuedChunksAsync()
        {
            var writes = new List<Task>();
            await foreach (var pending in queue.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                if (writeBuffersConcurrently)
                {
                    _ = writes.RemoveAll(static task => task.IsCompletedSuccessfully);
                    writes.Add(WriteChunkAsync(pending));
                }
                else
                    await WriteChunkAsync(pending).ConfigureAwait(false);
            }
            await Task.WhenAll(writes).ConfigureAwait(false);
        }

        private async Task WriteChunkAsync(PendingWrite pending)
        {
            if (Volatile.Read(ref failure) != null)
            {
                // Never issued, so the buffer can go back to the pool.
                pending.Buffer.Return();
                _ = freeBuffers.Release();
                return;
            }

            var writeStarted = false;
            try
            {
#if DEBUG
                await ExceptionInjectionHelper.WaitOnClearAsync(ExceptionInjectionType.Replication_Hold_Snapshot_Write).ConfigureAwait(false);
#endif
                cancellationToken.ThrowIfCancellationRequested();
                ThrowIfFailed();
                writeStarted = true;
                await WriteAsync(pending).ConfigureAwait(false);
                pending.Buffer.Return();
                _ = freeBuffers.Release();
            }
            catch (Exception ex)
            {
                _ = Interlocked.CompareExchange(ref failure, ex, null);
                logger?.LogError(ex, "Writing {type} checkpoint file at address {address} failed", Type, pending.Address);

                // A timed-out write may still read its buffer, so only a buffer never issued can be returned here.
                // Releasing the slot lets a receiving thread blocked on a buffer observe the failure.
                if (!writeStarted)
                    pending.Buffer.Return();
                _ = freeBuffers.Release();
            }
        }

        private async Task WriteAsync(PendingWrite pending)
        {
            var context = new WriteContext(pending.Buffer);
            IssueWrite(pending, context);

            (uint ErrorCode, Exception IOException) result;
            try
            {
                result = await context.Completion.Task.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                throw new GarnetException(
                    $"Timed out writing {Type} checkpoint file at address {pending.Address} (requested {pending.Length} bytes)");
            }

            if (result.ErrorCode != 0)
            {
                throw new GarnetException(
                    $"I/O error {result.ErrorCode} writing {Type} checkpoint file at address {pending.Address} (requested {pending.Length} bytes): " +
                    Utility.GetCallbackErrorMessage(result.ErrorCode, 0, null), result.IOException);
            }
        }

        private unsafe void IssueWrite(PendingWrite pending, WriteContext context)
            => device.WriteAsync((IntPtr)pending.Buffer.aligned_pointer, (ulong)pending.Address, pending.Length, OnWriteCompleted, context);

        private static void OnWriteCompleted(uint errorCode, uint numBytes, object context, Exception ioException)
            => _ = ((WriteContext)context).Completion.TrySetResult((errorCode, ioException));

        // The device holds this context until the write completes, so it also roots the buffer: a write abandoned on
        // timeout can never read from a buffer that was collected and reused.
        private sealed class WriteContext(SectorAlignedMemory buffer)
        {
            public readonly SectorAlignedMemory Buffer = buffer;
            public readonly TaskCompletionSource<(uint ErrorCode, Exception IOException)> Completion =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }
}