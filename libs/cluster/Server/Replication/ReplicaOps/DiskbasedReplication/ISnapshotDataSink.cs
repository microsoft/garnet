// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.cluster
{
    /// <summary>
    /// Interface for a checkpoint data sink that receives chunk-based writes.
    /// This is the write-side counterpart to <see cref="ISnapshotDataSource"/>.
    /// </summary>
    internal interface ISnapshotDataSink : IDisposable
    {
        /// <summary>
        /// The type of checkpoint file this sink represents.
        /// </summary>
        CheckpointFileType Type { get; }

        /// <summary>
        /// The token identifying the checkpoint.
        /// </summary>
        Guid Token { get; }

        /// <summary>
        /// Writes a chunk of data to the sink.
        /// </summary>
        /// <param name="startAddress">The start address for this chunk.</param>
        /// <param name="data">The data to write.</param>
        void WriteChunk(long startAddress, ReadOnlySpan<byte> data);

        /// <summary>
        /// Signals that all data has been written and the sink should finalize.
        /// </summary>
        void Complete();
    }
}