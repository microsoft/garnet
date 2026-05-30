// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.cluster
{
    /// <summary>
    /// A checkpoint data source backed by in-memory byte data (e.g., checkpoint metadata).
    /// Returns the byte array as a single chunk via <see cref="ReadNextChunkAsync"/>.
    /// </summary>
    internal sealed class TsavoriteMetadataSource : ISnapshotDataSource
    {
        private readonly Func<byte[]> dataFactory;
        private bool consumed;

        /// <inheritdoc/>
        public CheckpointFileType Type { get; }

        /// <inheritdoc/>
        public Guid Token { get; }

        /// <inheritdoc/>
        public long StartOffset => 0;

        /// <inheritdoc/>
        public long CurrentOffset => consumed ? EndOffset : 0;

        /// <inheritdoc/>
        public long EndOffset { get; private set; }

        /// <inheritdoc/>
        public bool HasNextChunk => !consumed;

        /// <summary>
        /// Creates a metadata data source with a factory that lazily produces the byte array.
        /// </summary>
        /// <param name="type">The checkpoint file type (e.g., STORE_INDEX or STORE_SNAPSHOT).</param>
        /// <param name="token">The checkpoint token.</param>
        /// <param name="dataFactory">A factory that returns the metadata bytes. Called on first read.</param>
        public TsavoriteMetadataSource(CheckpointFileType type, Guid token, Func<byte[]> dataFactory)
        {
            Type = type;
            Token = token;
            this.dataFactory = dataFactory;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
        }

        /// <inheritdoc/>
        public Task<DataSourceReadResult> ReadNextChunkAsync(CancellationToken cancellationToken = default)
        {
            if (consumed)
                throw new InvalidOperationException("TsavoriteMetadataSource has already been consumed.");

            var data = dataFactory() ?? [];
            EndOffset = data.Length;
            consumed = true;

            return Task.FromResult(new DataSourceReadResult(data, chunkStartAddress: 0));
        }
    }
}