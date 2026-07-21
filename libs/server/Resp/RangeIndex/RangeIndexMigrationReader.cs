// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Async wrapper around <see cref="RangeIndexChunkedSerializer"/> that owns a file source
    /// and reads file data asynchronously, then calls the sync serializer to frame it.
    /// </summary>
    public sealed class RangeIndexMigrationReader : IDisposable
    {
        private readonly RangeIndexChunkedSerializer serializer;
        private Stream stream;
        private readonly string tempFilePath;
        private readonly ILogger logger;
        private readonly Memory<byte> readBuffer;
        private bool disposed;

        /// <summary>Whether the serializer has emitted all data.</summary>
        public bool IsComplete => serializer.IsComplete;

        /// <summary>Total snapshot file size in bytes.</summary>
        public long TotalFileBytes => serializer.TotalFileBytes;

        /// <summary>
        /// Default size (bytes) of the internal file-read buffer when the caller does not specify one.
        /// This is independent of the destination (serialization) chunk size passed to
        /// <see cref="ReadNextChunk"/> / <see cref="ReadNextChunkAsync"/>; a larger read buffer reduces
        /// the number of file-system reads.
        /// </summary>
        public const int DefaultFileReadBufferSize = 1 << 20; // 1 MiB

        /// <summary>
        /// Create a migration reader that wraps a serializer and a data stream. On dispose,
        /// the underlying <paramref name="stream"/> is closed and <paramref name="tempFilePath"/>
        /// (when non-null) is deleted (best-effort) so source-side migration snapshots do not accumulate.
        /// </summary>
        /// <param name="serializer">The pure state-machine serializer.</param>
        /// <param name="stream">The stream to read snapshot data from (e.g. a FileStream in production, or a
        /// MemoryStream in tests).</param>
        /// <param name="tempFilePath">The path of the snapshot file owned by this reader; deleted on dispose.
        /// Pass null when the stream is not backed by an owned temp file (nothing is deleted).</param>
        /// <param name="logger">Optional logger for delete failures.</param>
        /// <param name="readBufferSize">Size (bytes) of the internal buffer used to read data from the stream.</param>
        public RangeIndexMigrationReader(RangeIndexChunkedSerializer serializer, Stream stream, string tempFilePath, ILogger logger = null, int readBufferSize = DefaultFileReadBufferSize)
        {
            if (readBufferSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(readBufferSize), readBufferSize, "readBufferSize must be positive.");

            this.serializer = serializer;
            this.stream = stream;
            this.tempFilePath = tempFilePath;
            this.logger = logger;
            readBuffer = new byte[readBufferSize];
        }

        /// <summary>
        /// Read the next chunk: reads file data asynchronously if needed, then calls the
        /// sync serializer to frame it into the destination buffer. Loops to handle
        /// phase transitions (e.g., headers → file data → trailer) within a single call.
        ///
        /// <para><b>Completion protocol:</b> the caller drives the stream by looping while
        /// <see cref="IsComplete"/> is <c>false</c>, sending each returned chunk. Once the
        /// trailer has been framed, <see cref="IsComplete"/> flips to <c>true</c> and the loop
        /// ends — i.e., the call that emits the final bytes is also the one after which
        /// <see cref="IsComplete"/> becomes <c>true</c>. Do not call this method again after
        /// <see cref="IsComplete"/> is <c>true</c> (the underlying serializer throws).</para>
        ///
        /// <para><paramref name="destination"/> must be at least
        /// <see cref="RangeIndexChunkedSerializer.MinChunkSize"/> bytes (the trailer size); smaller
        /// buffers are rejected with <see cref="ArgumentException"/>. With a valid destination the
        /// method always makes progress, so it returns a positive count while
        /// <see cref="IsComplete"/> is <c>false</c> (it returns <c>0</c> only if called when already
        /// complete).</para>
        /// </summary>
        /// <param name="destination">Output buffer. Must be at least <see cref="RangeIndexChunkedSerializer.MinChunkSize"/> bytes.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Number of bytes written to <paramref name="destination"/> (always positive while the stream is incomplete).</returns>
        /// <exception cref="ArgumentException">Thrown if <paramref name="destination"/> is smaller than <see cref="RangeIndexChunkedSerializer.MinChunkSize"/>.</exception>
        public async ValueTask<int> ReadNextChunkAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
        {
            ValidateDestination(destination.Length);

            var initialLength = destination.Length;
            while (!serializer.IsComplete && destination.Length > 0)
            {
                // Refill the file buffer if the serializer needs file data
                if (serializer.NeedsFileData)
                {
                    var bytesRead = await stream.ReadAsync(readBuffer, cancellationToken).ConfigureAwait(false);
                    SupplyFileDataOrThrow(bytesRead);
                }

                var written = serializer.MoveNext(destination.Span);

                // No progress: the remaining destination is too small to fit the next framing
                // element (header/trailer). Stop and let the caller send what we have so far.
                if (written == 0)
                    break;

                destination = destination[written..];
            }

            return initialLength - destination.Length;
        }

        /// <summary>
        /// Synchronous counterpart to <see cref="ReadNextChunkAsync"/> for callers that are not on an async path
        /// </summary>
        /// <param name="destination">Output buffer. Must be at least <see cref="RangeIndexChunkedSerializer.MinChunkSize"/> bytes.</param>
        /// <returns>Number of bytes written to <paramref name="destination"/> (always positive while the stream is incomplete).</returns>
        public int ReadNextChunk(Span<byte> destination)
        {
            ValidateDestination(destination.Length);

            var initialLength = destination.Length;
            while (!serializer.IsComplete && destination.Length > 0)
            {
                if (serializer.NeedsFileData)
                {
                    var bytesRead = stream.Read(readBuffer.Span);
                    SupplyFileDataOrThrow(bytesRead);
                }

                var written = serializer.MoveNext(destination);
                if (written == 0)
                    break;

                destination = destination[written..];
            }

            return initialLength - destination.Length;
        }

        // The destination is the buffer the serializer frames into, so it must be able to hold the
        // largest single-chunk element (the trailer). A destination below this could never frame the
        // trailer and the stream would never complete.
        private static void ValidateDestination(int length)
        {
            if (length < RangeIndexChunkedSerializer.MinChunkSize)
                throw new ArgumentException($"destination must be at least {RangeIndexChunkedSerializer.MinChunkSize} bytes (the trailer size) so the stream can complete.", "destination");
        }

        private void SupplyFileDataOrThrow(int bytesRead)
        {
            if (bytesRead == 0)
                throw new Exception($"RangeIndex file truncated: {serializer.FileDataRemaining} bytes remaining");
            serializer.SupplyFileData(readBuffer[..bytesRead]);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (disposed) return;
            disposed = true;

            try
            {
                stream?.Dispose();
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "RangeIndexMigrationReader: failed to dispose stream for {Path} (ignored)", tempFilePath);
            }
            finally
            {
                stream = null;
            }

            if (tempFilePath != null)
            {
                try
                {
                    if (File.Exists(tempFilePath))
                        File.Delete(tempFilePath);
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "RangeIndexMigrationReader: failed to delete migration snapshot {Path}", tempFilePath);
                }
            }
        }
    }
}