// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Interface to provide paths and names for all checkpoint-related files
    /// </summary>
    public interface ICheckpointNamingScheme
    {
        /// <summary>
        /// Base (or container) name for all checkpoint files
        /// </summary>
        public string BaseName { get; }

        /// <summary>
        /// Hash table (including overflow buckets)
        /// </summary>
        FileDescriptor HashTable(Guid token);

        /// <summary>
        /// Index checkpoint base location (directory)
        /// </summary>
        FileDescriptor IndexCheckpointBase(Guid token);

        /// <summary>
        /// Index checkpoint metadata
        /// </summary>
        FileDescriptor IndexCheckpointMetadata(Guid token);

        /// <summary>
        /// Hybrid log checkpoint base location (directory)
        /// </summary>
        FileDescriptor LogCheckpointBase(Guid token);

        /// <summary>
        /// Hybrid log checkpoint metadata
        /// </summary>
        FileDescriptor LogCheckpointMetadata(Guid token);

        /// <summary>
        /// Hybrid log snapshot
        /// </summary>
        FileDescriptor LogSnapshot(Guid token);

        /// <summary>
        /// Object log snapshot
        /// </summary>
        FileDescriptor ObjectLogSnapshot(Guid token);

        /// <summary>
        /// Delta log
        /// </summary>
        FileDescriptor DeltaLog(Guid token);

        /// <summary>
        /// TsavoriteLog commit metadata
        /// </summary>
        FileDescriptor TsavoriteLogCommitMetadata(long commitNumber);

        /// <summary>
        /// Token associated with given file descriptor
        /// </summary>
        Guid Token(FileDescriptor fileDescriptor);

        /// <summary>
        /// Commit number associated with given file descriptor
        /// </summary>
        long CommitNumber(FileDescriptor fileDescriptor);

        /// <summary>
        /// Get base path holding index checkpoints
        /// </summary>
        string IndexCheckpointBasePath { get; }

        /// <summary>
        /// Get base path holding log checkpoints
        /// </summary>
        string LogCheckpointBasePath { get; }

        /// <summary>
        /// Get base path holding TsavoriteLog commits
        /// </summary>
        string TsavoriteLogCommitBasePath { get; }
    }
}