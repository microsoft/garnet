// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.cluster
{
    /// <summary>
    /// A checkpoint data source that reads a RangeIndex .bftree file using FileStream.
    /// Unlike <see cref="FileDataSource"/> which uses Tsavorite's IDevice for sector-aligned I/O,
    /// this source reads plain files directly since .bftree files are not managed by the device layer.
    /// </summary>
    internal sealed class RangeIndexFileDataSource : ISnapshotDataSource
    {
        /// <summary>
        /// Default chunk size for streaming .bftree files (128 KB).
        /// </summary>
        internal const int DefaultChunkSize = 1 << 17;

        private readonly string filePath;
        private readonly int chunkSize;
        private FileStream stream;

        /// <inheritdoc/>
        public CheckpointFileType Type { get; }

        /// <inheritdoc/>
        public Guid Token { get; }

        /// <summary>
        /// The 32-character key hash prefix identifying the RangeIndex tree.
        /// </summary>
        public string KeyHash { get; }

        /// <summary>
        /// The logical hlog address embedded in the flush filename.
        /// Only meaningful for <see cref="CheckpointFileType.STORE_RANGEINDEX_FLUSH"/>.
        /// </summary>
        public long Address { get; }

        /// <inheritdoc/>
        public long StartOffset => 0;

        /// <inheritdoc/>
        public long CurrentOffset { get; private set; }

        /// <inheritdoc/>
        public long EndOffset { get; }

        /// <inheritdoc/>
        public bool HasNextChunk => CurrentOffset < EndOffset;

        /// <inheritdoc/>
        public byte[] GetMetadata()
        {
            var keyHashBytes = System.Text.Encoding.ASCII.GetBytes(KeyHash);

            if (Type == CheckpointFileType.STORE_RANGEINDEX_FLUSH)
            {
                var metadata = new byte[32 + 8];
                Buffer.BlockCopy(keyHashBytes, 0, metadata, 0, 32);
                BitConverter.TryWriteBytes(metadata.AsSpan(32), Address);
                return metadata;
            }

            // Snapshot: keyHash only
            return keyHashBytes;
        }

        /// <summary>
        /// Creates a new RangeIndexFileDataSource.
        /// </summary>
        /// <param name="type">The checkpoint file type (STORE_RANGEINDEX_FLUSH or STORE_RANGEINDEX_SNAPSHOT).</param>
        /// <param name="token">The checkpoint token.</param>
        /// <param name="filePath">Full path to the .bftree file on disk.</param>
        /// <param name="keyHash">The 32-character key hash prefix.</param>
        /// <param name="address">The hlog logical address (flush files only).</param>
        /// <param name="chunkSize">Maximum bytes to read per chunk.</param>
        public RangeIndexFileDataSource(CheckpointFileType type, Guid token, string filePath, string keyHash, long address, int chunkSize = DefaultChunkSize)
        {
            Type = type;
            Token = token;
            KeyHash = keyHash;
            Address = address;
            this.filePath = filePath;
            this.chunkSize = chunkSize;

            var fileInfo = new FileInfo(filePath);
            if (!fileInfo.Exists)
                throw new FileNotFoundException($"RangeIndex file not found: {filePath}");

            EndOffset = fileInfo.Length;
        }

        /// <inheritdoc/>
        public async Task<DataSourceReadResult> ReadNextChunkAsync(CancellationToken cancellationToken = default)
        {
            stream ??= new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: chunkSize, useAsync: true);

            var remaining = EndOffset - CurrentOffset;
            var bytesToRead = (int)Math.Min(remaining, chunkSize);
            var buffer = new byte[bytesToRead];

            var bytesRead = await stream.ReadAsync(buffer, 0, bytesToRead, cancellationToken).ConfigureAwait(false);
            var chunkStart = CurrentOffset;
            CurrentOffset += bytesRead;

            return new DataSourceReadResult(buffer, chunkStartAddress: chunkStart);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            stream?.Dispose();
            stream = null;
        }
    }
}