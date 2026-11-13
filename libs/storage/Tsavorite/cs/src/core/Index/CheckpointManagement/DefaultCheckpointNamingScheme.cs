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
        /// <remarks>
        /// Commit n's metadata is segment 0 of the device that <see cref="TsavoriteLogCommitMetadata"/> names, so its file is
        /// commit.n.0, or commit.n.s.0 for sublog s. The sublogs of a sharded log share the directory, so a sublog's files are
        /// those that carry its index.
        /// </remarks>
        public long CommitNumber(FileDescriptor fileDescriptor)
        {
            var parts = fileDescriptor.fileName.Split('.');
            if (SublogIdx == -1)
                return long.Parse(parts[^2]);

            return parts.Length >= 4 && long.TryParse(parts[^2], out var sublogIdx) && sublogIdx == SublogIdx && long.TryParse(parts[^3], out var commitNumber)
                ? commitNumber
                : -1;
        }

        /// <inheritdoc />
        public string IndexCheckpointBasePath => "index-checkpoints";
        /// <inheritdoc />
        public string LogCheckpointBasePath => "cpr-checkpoints";
        /// <inheritdoc />
        public string TsavoriteLogCommitBasePath => "log-commits";
    }
}