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
        private readonly ClusterProvider clusterProvider;

        public CheckpointFileType Type { get; }
        public Guid Token { get; }

        /// <summary>
        /// Creates a new MetadataDataSink.
        /// </summary>
        /// <param name="type">The checkpoint file type (STORE_INDEX or STORE_SNAPSHOT).</param>
        /// <param name="token">The checkpoint token.</param>
        /// <param name="clusterProvider">The cluster provider for accessing checkpoint managers.</param>
        public MetadataDataSink(CheckpointFileType type, Guid token, ClusterProvider clusterProvider)
        {
            Type = type;
            Token = token;
            this.clusterProvider = clusterProvider;
        }

        /// <inheritdoc/>
        public void WriteChunk(long startAddress, ReadOnlySpan<byte> data)
        {
            var checkpointMetadata = data.ToArray();
            var ckptManager = Type switch
            {
                CheckpointFileType.STORE_SNAPSHOT or
                CheckpointFileType.STORE_INDEX => clusterProvider.ReplicationLogCheckpointManager,
                _ => throw new Exception($"Invalid checkpoint filetype {Type}"),
            };

            switch (Type)
            {
                case CheckpointFileType.STORE_SNAPSHOT:
                    ckptManager.CommitLogCheckpointSendFromPrimary(Token, checkpointMetadata);
                    break;
                case CheckpointFileType.STORE_INDEX:
                    ckptManager.CommitIndexCheckpoint(Token, checkpointMetadata);
                    break;
                default:
                    throw new Exception($"Invalid checkpoint filetype {Type}");
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