// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;

namespace Tsavorite.core
{
    /// <summary>
    /// Default checkpoint naming scheme used by Tsavorite
    /// </summary>
    public class DefaultCheckpointNamingScheme : ICheckpointNamingScheme
    {
        readonly string baseName;

        /// <summary>
        /// Create instance of default naming scheme
        /// </summary>
        /// <param name="baseName">Overall location specifier (e.g., local path or cloud container name)</param>
        public DefaultCheckpointNamingScheme(string baseName = "")
        {
            this.baseName = baseName;
        }

        /// <inheritdoc />
        public string BaseName() => baseName;

        /// <inheritdoc />
        public FileDescriptor LogCheckpointBase(Guid token) => new FileDescriptor($"{LogCheckpointBasePath()}/{token}", null);

        /// <inheritdoc />
        public FileDescriptor LogCheckpointMetadata(Guid token) => new FileDescriptor($"{LogCheckpointBasePath()}/{token}", "info.dat");

        /// <inheritdoc />
        public FileDescriptor LogSnapshot(Guid token) => new FileDescriptor($"{LogCheckpointBasePath()}/{token}", "snapshot.dat");
        /// <inheritdoc />
        public FileDescriptor ObjectLogSnapshot(Guid token) => new FileDescriptor($"{LogCheckpointBasePath()}/{token}", "snapshot.obj.dat");
        /// <inheritdoc />
        public FileDescriptor DeltaLog(Guid token) => new FileDescriptor($"{LogCheckpointBasePath()}/{token}", "delta.dat");


        /// <inheritdoc />
        public FileDescriptor IndexCheckpointBase(Guid token) => new FileDescriptor($"{IndexCheckpointBasePath()}/{token}", null);

        /// <inheritdoc />
        public FileDescriptor IndexCheckpointMetadata(Guid token) => new FileDescriptor($"{IndexCheckpointBasePath()}/{token}", "info.dat");
        /// <inheritdoc />
        public FileDescriptor HashTable(Guid token) => new FileDescriptor($"{IndexCheckpointBasePath()}/{token}", "ht.dat");
        /// <inheritdoc />
        public FileDescriptor TsavoriteLogCommitMetadata(long commitNumber) => new FileDescriptor($"{TsavoriteLogCommitBasePath()}", $"commit.{commitNumber}");

        /// <inheritdoc />
        public Guid Token(FileDescriptor fileDescriptor) => Guid.Parse(new DirectoryInfo(fileDescriptor.directoryName).Name);
        /// <inheritdoc />
        public long CommitNumber(FileDescriptor fileDescriptor) => long.Parse(fileDescriptor.fileName.Split('.').Reverse().Take(2).Last());

        /// <inheritdoc />
        public string IndexCheckpointBasePath() => "index-checkpoints";
        /// <inheritdoc />
        public string LogCheckpointBasePath() => "cpr-checkpoints";
        /// <inheritdoc />
        public string TsavoriteLogCommitBasePath() => "log-commits";
    }
}