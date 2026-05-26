// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

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
        /// Default chunk size for streaming .bftree files (64 KB).
        /// </summary>
        internal const int DefaultChunkSize = 1 << 16;

        /// <summary>
        /// Length in bytes of the ASCII-encoded key hash in the metadata payload.
        /// </summary>
        internal const int KeyHashLength = 32;

        /// <summary>
        /// Length in bytes of the little-endian encoded logical address in the metadata payload.
        /// </summary>
        internal const int AddressLength = sizeof(long);

        /// <summary>
        /// Total metadata length for flush files (key hash + address).
        /// </summary>
        internal const int FlushMetadataLength = KeyHashLength + AddressLength;

        private readonly string filePath;
        private readonly int chunkSize;
        private readonly ILogger logger;
        private FileStream stream;
        private byte[] buffer;

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
                var metadata = new byte[FlushMetadataLength];
                Buffer.BlockCopy(keyHashBytes, 0, metadata, 0, KeyHashLength);
                BinaryPrimitives.WriteInt64LittleEndian(metadata.AsSpan(KeyHashLength), Address);
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
        /// <param name="logger">Optional logger.</param>
        public RangeIndexFileDataSource(CheckpointFileType type, Guid token, string filePath, string keyHash, long address, int chunkSize = DefaultChunkSize, ILogger logger = null)
        {
            Type = type;
            Token = token;
            KeyHash = keyHash;
            Address = address;
            this.filePath = filePath;
            this.chunkSize = chunkSize;
            this.logger = logger;

            var fileInfo = new FileInfo(filePath);
            if (!fileInfo.Exists)
                throw new FileNotFoundException($"RangeIndex file not found: {filePath}");

            EndOffset = fileInfo.Length;
        }

        /// <inheritdoc/>
        public async Task<DataSourceReadResult> ReadNextChunkAsync(CancellationToken cancellationToken = default)
        {
            stream ??= new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: chunkSize, useAsync: true);
            buffer ??= new byte[chunkSize];

            var remaining = EndOffset - CurrentOffset;
            var bytesToRead = (int)Math.Min(remaining, chunkSize);

            var bytesRead = await stream.ReadAsync(buffer, 0, bytesToRead, cancellationToken).ConfigureAwait(false);

            if (bytesRead == 0)
            {
                logger?.LogWarning("RangeIndexFileDataSource: unexpected EOF at offset {currentOffset}, expected {endOffset} for {filePath}", CurrentOffset, EndOffset, filePath);
                CurrentOffset = EndOffset;
                return new DataSourceReadResult([], chunkStartAddress: CurrentOffset);
            }

            var chunkStart = CurrentOffset;
            CurrentOffset += bytesRead;

            return new DataSourceReadResult(buffer, bytesRead, chunkStartAddress: chunkStart);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            stream?.Dispose();
            stream = null;
        }
    }
}