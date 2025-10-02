// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;

namespace Tsavorite.core
{
    /// <summary>
    /// Default checkpoint naming scheme used by Tsavorite
    /// </summary>
    public class DefaultCheckpointNamingScheme : ICheckpointNamingScheme
    {
        /// <inheritdoc />
        public string BaseName { get; }

        int SublogIdx { get; }

        /// <summary>
        /// Create instance of default naming scheme
        /// </summary>
        /// <param name="baseName">Overall location specifier (e.g., local path or cloud container name)</param>
        public DefaultCheckpointNamingScheme(string baseName = "", int sublogIdx = -1)
        {
            BaseName = baseName;
            SublogIdx = sublogIdx;
        }

        /// <inheritdoc />
        public FileDescriptor LogCheckpointBase(Guid token) => new(Path.Join(LogCheckpointBasePath, token.ToString()), null);
        /// <inheritdoc />
        public FileDescriptor LogCheckpointMetadata(Guid token) => new(Path.Join(LogCheckpointBasePath, token.ToString()), "info.dat");
        /// <inheritdoc />
        public FileDescriptor LogSnapshot(Guid token) => new(Path.Join(LogCheckpointBasePath, token.ToString()), "snapshot.dat");
        /// <inheritdoc />
        public FileDescriptor ObjectLogSnapshot(Guid token) => new(Path.Join(LogCheckpointBasePath, token.ToString()), "snapshot.obj.dat");
        /// <inheritdoc />
        public FileDescriptor DeltaLog(Guid token) => new(Path.Join(LogCheckpointBasePath, token.ToString()), "delta.dat");

        /// <inheritdoc />
        public FileDescriptor IndexCheckpointBase(Guid token) => new(Path.Join(IndexCheckpointBasePath, token.ToString()), null);
        /// <inheritdoc />
        public FileDescriptor IndexCheckpointMetadata(Guid token) => new(Path.Join(IndexCheckpointBasePath, token.ToString()), "info.dat");
        /// <inheritdoc />
        public FileDescriptor HashTable(Guid token) => new(Path.Join(IndexCheckpointBasePath, token.ToString()), "ht.dat");
        /// <inheritdoc />
        public FileDescriptor TsavoriteLogCommitMetadata(long commitNumber) => new(TsavoriteLogCommitBasePath, SublogIdx == -1 ? $"commit.{commitNumber}" : $"commit.{commitNumber}.{SublogIdx}");

        /// <inheritdoc />
        public Guid Token(FileDescriptor fileDescriptor) => Guid.Parse(new DirectoryInfo(fileDescriptor.directoryName).Name);
        /// <inheritdoc />
        public long CommitNumber(FileDescriptor fileDescriptor) => long.Parse(fileDescriptor.fileName.Split('.')[^2]);

        /// <inheritdoc />
        public string IndexCheckpointBasePath => "index-checkpoints";
        /// <inheritdoc />
        public string LogCheckpointBasePath => "cpr-checkpoints";
        /// <inheritdoc />
        public string TsavoriteLogCommitBasePath => "log-commits";
    }
}