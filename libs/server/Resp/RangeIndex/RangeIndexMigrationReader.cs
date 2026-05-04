// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

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
        private readonly byte[] fileBuffer;
        private int fileBufferOffset;
        private int fileBufferAvailable;
        private bool disposed;

        /// <summary>Whether the serializer has emitted all data.</summary>
        public bool IsComplete => serializer.IsComplete;

        /// <summary>
        /// Create a migration reader that wraps a serializer and file stream.
        /// </summary>
        /// <param name="serializer">The pure state-machine serializer.</param>
        /// <param name="fileStream">The file stream to read snapshot data from.</param>
        /// <param name="chunkSize">Buffer size for file reads.</param>
        public RangeIndexMigrationReader(RangeIndexChunkedSerializer serializer, FileStream fileStream, int chunkSize)
        {
            this.serializer = serializer;
            this.fileStream = fileStream;
            fileBuffer = new byte[chunkSize];
        }

        /// <summary>
        /// Read the next chunk: reads file data asynchronously if needed, then calls the
        /// sync serializer to frame it into the destination buffer. Loops to handle
        /// phase transitions (e.g., headers → file data → trailer) within a single call.
        /// </summary>
        /// <param name="destination">Output buffer.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Number of bytes written to <paramref name="destination"/>.</returns>
        public async ValueTask<int> ReadNextChunkAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
        {
            var totalWritten = 0;

            while (!serializer.IsComplete && destination.Length > 0)
            {
                // Refill the file buffer if the serializer needs file data and we've consumed everything
                if (serializer.NeedsFileData && fileBufferAvailable == 0)
                {
                    var maxRead = (int)Math.Min(fileBuffer.Length, serializer.FileDataRemaining);
                    var bytesRead = await fileStream.ReadAsync(fileBuffer.AsMemory(0, maxRead), cancellationToken).ConfigureAwait(false);
                    if (bytesRead == 0)
                        throw new EndOfStreamException($"RangeIndex file truncated: {serializer.FileDataRemaining} bytes remaining");

                    fileBufferOffset = 0;
                    fileBufferAvailable = bytesRead;
                }

                var fileData = fileBufferAvailable > 0
                    ? fileBuffer.AsSpan(fileBufferOffset, fileBufferAvailable)
                    : ReadOnlySpan<byte>.Empty;

                var written = serializer.MoveNext(destination.Span, fileData, out var consumed);
                fileBufferOffset += consumed;
                fileBufferAvailable -= consumed;

                if (written == 0)
                    break;

                destination = destination[written..];
                totalWritten += written;
            }

            return totalWritten;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (disposed) return;
            disposed = true;

            fileStream?.Dispose();
            fileStream = null;
        }
    }
}
