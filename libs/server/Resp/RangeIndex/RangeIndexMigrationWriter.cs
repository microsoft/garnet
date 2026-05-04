// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.server
{
    /// <summary>
    /// Async wrapper around <see cref="RangeIndexChunkedDeserializer"/> that owns a file sink
    /// and writes file data asynchronously after the sync deserializer identifies file bytes.
    /// </summary>
    public sealed class RangeIndexMigrationWriter : IDisposable
    {
        private readonly RangeIndexChunkedDeserializer deserializer;
        private readonly string tempPath;
        private FileStream stream;
        private bool disposed;

        /// <summary>Whether the stream completed successfully.</summary>
        public bool IsComplete => deserializer.IsComplete;

        /// <summary>Whether the stream encountered an irrecoverable error.</summary>
        public bool HasError => deserializer.HasError;

        /// <summary>The key bytes extracted from the header. Valid only after <see cref="IsComplete"/>.</summary>
        public ReadOnlySpan<byte> Key => deserializer.Key;

        /// <summary>The temporary file path where file data is being written.</summary>
        public string TempPath => tempPath;

        /// <summary>
        /// Create a migration writer that wraps a deserializer and manages file output.
        /// </summary>
        /// <param name="deserializer">The pure state-machine deserializer.</param>
        /// <param name="tempPath">The temporary file path to write file data to.</param>
        public RangeIndexMigrationWriter(RangeIndexChunkedDeserializer deserializer, string tempPath)
        {
            this.deserializer = deserializer;
            this.tempPath = tempPath;
        }

        /// <summary>
        /// Process an incoming chunk: calls the sync deserializer to parse framing,
        /// then writes any file bytes asynchronously.
        /// </summary>
        /// <param name="data">The incoming chunk data.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns><c>true</c> if valid; <c>false</c> if corruption or invalid data detected.</returns>
        public async ValueTask<bool> ProcessChunkAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            var result = deserializer.ProcessChunk(data.Span, out var fileDataOffset, out var fileDataLength);

            if (fileDataLength > 0)
            {
                stream ??= new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None);
                await stream.WriteAsync(data.Slice(fileDataOffset, fileDataLength), cancellationToken).ConfigureAwait(false);
            }

            if (deserializer.IsComplete || deserializer.HasError)
                CloseStream();

            return result;
        }

        /// <summary>
        /// Publish the completed migration: move temp file to working path, recover BfTree, insert stub.
        /// </summary>
        public bool Publish(ref StringBasicContext ctx) => deserializer.Publish(ref ctx, tempPath);

        private void CloseStream()
        {
            if (stream == null) return;
            stream.Flush();
            stream.Dispose();
            stream = null;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (disposed) return;
            disposed = true;

            CloseStream();

            try
            {
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
            }
            catch { }
        }
    }
}
