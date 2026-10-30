// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Transmits checkpoint file segments over the network using chunked reads from its owned <see cref="FileDataSource"/>.
    /// Sends each chunk via <see cref="GarnetClientSession.ExecuteClusterSnapshotData"/>
    /// followed by an empty end-of-transmission packet.
    /// </summary>
    internal sealed class FileTransmitSource : ISnapshotTransmitSource
    {
        readonly ILogger logger;
        readonly int pipelineDepth;

        public ISnapshotDataSource DataSource { get; }

        public FileTransmitSource(ISnapshotDataSource dataSource, ILogger logger = null, int pipelineDepth = 1)
        {
            if (pipelineDepth is < 1 or > GarnetServerOptions.ReplicaDiskBasedSyncPipelineDepthMax)
                throw new ArgumentOutOfRangeException(nameof(pipelineDepth));
            DataSource = dataSource;
            this.logger = logger;
            this.pipelineDepth = pipelineDepth;
        }

        /// <inheritdoc/>
        public async Task TransmitAsync(GarnetClientSession gcs, TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            var fileTokenBytes = DataSource.Token.ToByteArray();
            // Link caller cancellation with internal failures/timeouts so either stops outstanding
            // acknowledgements, overlapping reads and blocked sends without cancelling the caller's source.
            using var abort = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            // Close only the socket here; disposing send buffers must wait for the sender to unwind.
            using var closeOnAbort = abort.Token.Register(gcs.CloseConnection);

            // Capture the first reported failure atomically across the sender, response tasks and timeout callbacks.
            // Aborting can surface cancellation/disposal exceptions elsewhere; rethrow the captured cause after cleanup.
            var firstFailure = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);

            // Send chunks serially, allowing a bounded FIFO of outstanding acknowledgements.
            // Only this async flow mutates pending; response continuations cannot change its head across an await.
            // Peek retains the head in the abort-cleanup set until its acknowledgement succeeds.
            var pending = new Queue<Task>(pipelineDepth);

            try
            {
                // Read and send chunks while keeping at most pipelineDepth acknowledgements outstanding.
                while (DataSource.HasNextChunk)
                {
                    if (pending.Count == pipelineDepth)
                    {
                        // Wait for the oldest item to acknowledge completion before dequeueing it and sending another chunk.
                        await pending.Peek().ConfigureAwait(false);
                        _ = pending.Dequeue();
                    }

                    abort.Token.ThrowIfCancellationRequested();
                    var result = await DataSource.ReadNextChunkAsync(abort.Token).ConfigureAwait(false);
                    pending.Enqueue(SendChunkAsync(result.ChunkStartAddress, result.BytesRead, result.Buffer));
                }

                // Wait for all outstanding acknowledgements before sending the completion marker.
                while (pending.Count > 0)
                {
                    await pending.Peek().ConfigureAwait(false);
                    _ = pending.Dequeue();
                }

                abort.Token.ThrowIfCancellationRequested();
                await SendChunkAsync(DataSource.CurrentOffset, 0).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _ = firstFailure.TrySetResult(ex);
                await abort.CancelAsync().ConfigureAwait(false);
                gcs.Dispose();
                try
                {
                    await Task.WhenAll(pending).ConfigureAwait(false);
                }
                catch (Exception cleanupException)
                {
                    logger?.LogDebug(cleanupException, "Aborting outstanding checkpoint acknowledgements for {type}", DataSource.Type);
                }
                var failure = await firstFailure.Task.ConfigureAwait(false);
                ExceptionDispatchInfo.Capture(failure).Throw();
                throw;
            }

            async Task SendChunkAsync(long address, int length, SectorAlignedMemory buffer = null)
            {
                try
                {
                    // Start before submission so socket backpressure also counts against the deadline.
                    using var deadline = new CancellationTokenSource();
                    Task<string> response = null;
                    using var onTimeout = deadline.Token.Register(() =>
                    {
                        if (Volatile.Read(ref response)?.IsCompleted == true)
                            return;
                        _ = firstFailure.TrySetResult(new TimeoutException(
                            $"Checkpoint acknowledgement timed out for {DataSource.Type} [address:{address},length:{length}]"));
                        abort.Cancel();
                    });
                    try
                    {
                        deadline.CancelAfter(timeout);
                        abort.Token.ThrowIfCancellationRequested();
                        Volatile.Write(ref response, gcs.ExecuteClusterSnapshotData(
                            fileTokenBytes, (int)DataSource.Type, address, buffer == null ? [] : buffer.GetSlice(length)));
                    }
                    finally
                    {
                        // Submission copies the span into network-owned buffers before returning.
                        buffer?.Return();
                    }

                    // Cancellation can stop the wait below before the response task faults when the connection closes.
                    // Observe that fault even if no caller is still awaiting the underlying task.
                    _ = response.ContinueWith(task => _ = task.Exception, CancellationToken.None,
                        TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                    var resp = await response.WaitAsync(abort.Token).ConfigureAwait(false);
                    if (!resp.Equals("OK"))
                        ExceptionUtils.ThrowException(new GarnetException(
                            $"Primary error at TransmitAsync {DataSource.Type} {resp} [address:{address},length:{length}]"));
                }
                catch (Exception ex)
                {
                    _ = firstFailure.TrySetResult(ex);
                    await abort.CancelAsync().ConfigureAwait(false);
                    throw;
                }
            }
        }

        public void Dispose()
        {
            DataSource?.Dispose();
        }
    }

    internal static unsafe class SectorAlignedMemoryExtensions
    {
        public static Span<byte> GetSlice(this SectorAlignedMemory pbuffer, int length)
        {
            return new Span<byte>(pbuffer.aligned_pointer, length);
        }
    }
}