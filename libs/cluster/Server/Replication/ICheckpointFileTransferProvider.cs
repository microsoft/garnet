// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Provides checkpoint storage operations required by snapshot transmission and receipt.
    /// </summary>
    internal interface ICheckpointFileTransferProvider
    {
        /// <summary>
        /// Whether checkpoint data can include the persisted hybrid log.
        /// </summary>
        bool EnableStorageTier { get; }

        /// <summary>
        /// Gets the maximum transfer batch size for a checkpoint file type.
        /// </summary>
        int GetMaxBatchSize(CheckpointFileType type);

        /// <summary>
        /// Creates and initializes the device for a checkpoint file.
        /// </summary>
        IDevice CreateCheckpointDevice(CheckpointFileType type, Guid token);

        /// <summary>
        /// Gets index checkpoint metadata.
        /// </summary>
        byte[] GetIndexCheckpointMetadata(Guid token);

        /// <summary>
        /// Gets log checkpoint metadata.
        /// </summary>
        byte[] GetLogCheckpointMetadata(Guid token);

        /// <summary>
        /// Commits checkpoint metadata received from a primary.
        /// </summary>
        void CommitCheckpointMetadata(CheckpointFileType type, Guid token, ReadOnlySpan<byte> metadata);
    }
}