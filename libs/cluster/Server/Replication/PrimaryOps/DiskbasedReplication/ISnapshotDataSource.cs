// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.cluster
{
    /// <summary>
    /// Interface for a checkpoint data source that provides chunk-based reading.
    /// </summary>
    internal interface ISnapshotDataSource : IDisposable
    {
        /// <summary>
        /// The type of checkpoint file this data source represents.
        /// </summary>
        CheckpointFileType Type { get; }

        /// <summary>
        /// The token identifying the checkpoint file.
        /// </summary>
        Guid Token { get; }

        /// <summary>
        /// The start offset in the source.
        /// </summary>
        long StartOffset { get; }

        /// <summary>
        /// The current read offset in the source.
        /// </summary>
        long CurrentOffset { get; }

        /// <summary>
        /// The end offset in the source.
        /// </summary>
        long EndOffset { get; }

        /// <summary>
        /// Whether there are remaining chunks to read.
        /// </summary>
        bool HasNextChunk { get; }

        /// <summary>
        /// Reads the next chunk from the underlying source asynchronously.
        /// Advances CurrentOffset by the number of bytes read.
        /// For device-backed sources, the caller is responsible for returning the buffer via <see cref="DataSourceReadResult.Buffer"/>.Return().
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A <see cref="DataSourceReadResult"/> containing the data, bytes read, and chunk start address.</returns>
        Task<DataSourceReadResult> ReadNextChunkAsync(CancellationToken cancellationToken = default);
    }
}