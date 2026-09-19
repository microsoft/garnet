// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.cluster
{
    /// <summary>
    /// In-memory implementation of <see cref="ISnapshotDataSink"/> that commits checkpoint
    /// metadata bytes to the checkpoint manager.
    /// </summary>
    internal sealed class MetadataDataSink : ISnapshotDataSink
    {
        private readonly ICheckpointFileTransferProvider checkpointFileProvider;

        public CheckpointFileType Type { get; }
        public Guid Token { get; }

        /// <summary>
        /// Creates a new MetadataDataSink.
        /// </summary>
        /// <param name="type">The checkpoint file type (STORE_INDEX or STORE_SNAPSHOT).</param>
        /// <param name="token">The checkpoint token.</param>
        /// <param name="checkpointFileProvider">The checkpoint storage provider.</param>
        public MetadataDataSink(CheckpointFileType type, Guid token, ICheckpointFileTransferProvider checkpointFileProvider)
        {
            Type = type;
            Token = token;
            this.checkpointFileProvider = checkpointFileProvider;
        }

        /// <inheritdoc/>
        public void WriteChunk(long startAddress, ReadOnlySpan<byte> data)
        {
            switch (Type)
            {
                case CheckpointFileType.STORE_INDEX:
                case CheckpointFileType.STORE_SNAPSHOT:
                    checkpointFileProvider.CommitCheckpointMetadata(Type, Token, data);
                    break;
                default:
                    throw new Exception($"Invalid checkpoint file type {Type}");
            }
        }

        /// <inheritdoc/>
        public void Complete()
        {
            // No finalization needed for metadata commits
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            // Nothing to dispose
        }
    }
}