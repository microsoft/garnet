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
        private FileStream fileStream;
        private readonly string tempFilePath;
        private readonly ILogger logger;
        private readonly Memory<byte> readBuffer;
        private bool disposed;

        /// <summary>
        /// Minimum supported chunk size. A chunk must be able to hold the largest single-chunk
        /// framing element — the trailer (<c>[8-byte hash][4-byte stubLen][stub]</c>) — otherwise
        /// the stream can never complete (the serializer would never be able to emit the trailer).
        /// </summary>
        public const int MinChunkSize = sizeof(ulong) + sizeof(int) + RangeIndexManager.IndexSizeBytes;

        /// <summary>Whether the serializer has emitted all data.</summary>
        public bool IsComplete => serializer.IsComplete;

        /// <summary>Total snapshot file size in bytes.</summary>
        public long TotalFileBytes => serializer.TotalFileBytes;

        /// <summary>
        /// Create a migration reader that wraps a serializer and file stream. On dispose,
        /// the underlying <paramref name="fileStream"/> is closed and <paramref name="tempFilePath"/>
        /// is deleted (best-effort) so source-side migration snapshots do not accumulate.
        /// </summary>
        /// <param name="serializer">The pure state-machine serializer.</param>
        /// <param name="fileStream">The file stream to read snapshot data from.</param>
        /// <param name="tempFilePath">The path of the snapshot file owned by this reader; deleted on dispose.</param>
        /// <param name="chunkSize">Buffer size for file reads. Must be at least <see cref="MinChunkSize"/>.</param>
        /// <param name="logger">Optional logger for delete failures.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="chunkSize"/> is below <see cref="MinChunkSize"/>.</exception>
        public RangeIndexMigrationReader(RangeIndexChunkedSerializer serializer, FileStream fileStream, string tempFilePath, int chunkSize, ILogger logger = null)
        {
            if (chunkSize < MinChunkSize)
                throw new ArgumentOutOfRangeException(nameof(chunkSize), chunkSize, $"chunkSize must be at least {MinChunkSize} bytes (the trailer size) so the stream can complete.");

            this.serializer = serializer;
            this.fileStream = fileStream;
            this.tempFilePath = tempFilePath;
            this.logger = logger;
            readBuffer = new byte[chunkSize];
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
        /// <para>A well-sized <paramref name="destination"/> (at least the trailer size) always
        /// makes progress, so a return value of <c>0</c> while <see cref="IsComplete"/> is still
        /// <c>false</c> indicates the buffer is too small for the next framing element and the
        /// caller should treat it as an error rather than loop indefinitely.</para>
        /// </summary>
        /// <param name="destination">Output buffer. Must be at least the trailer size to guarantee progress.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Number of bytes written to <paramref name="destination"/>; <c>0</c> only if the
        /// buffer was too small to advance.</returns>
        public async ValueTask<int> ReadNextChunkAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
        {
            var initialLength = destination.Length;
            while (!serializer.IsComplete && destination.Length > 0)
            {
                // Refill the file buffer if the serializer needs file data
                if (serializer.NeedsFileData)
                {
                    var bytesRead = await fileStream.ReadAsync(readBuffer, cancellationToken).ConfigureAwait(false);
                    if (bytesRead == 0)
                        throw new Exception($"RangeIndex file truncated: {serializer.FileDataRemaining} bytes remaining");

                    serializer.SupplyFileData(readBuffer[..bytesRead]);
                }

                var written = serializer.MoveNext(destination.Span);

                if (written == 0)
                    break;

                destination = destination[written..];
            }

            return initialLength - destination.Length;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (disposed) return;
            disposed = true;

            try
            {
                fileStream?.Dispose();
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "RangeIndexMigrationReader: failed to dispose file stream for {Path} (ignored)", tempFilePath);
            }
            finally
            {
                fileStream = null;
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