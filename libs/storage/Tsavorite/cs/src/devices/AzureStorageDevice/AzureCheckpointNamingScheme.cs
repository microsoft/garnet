// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Tsavorite.core;

namespace Tsavorite.devices
{
    /// <summary>
    /// Represents a checkpoint naming scheme used for Azure Blob Storage backed <see cref="DeviceLogCommitCheckpointManager"/> instances.
    /// </summary>
    public class AzureCheckpointNamingScheme : ICheckpointNamingScheme
    {
        /// <inheritdoc />
        public string BaseName { get; }

        /// <summary>
        /// Create instance of default naming scheme
        /// </summary>
        /// <param name="baseName">Overall location specifier (e.g., local path or cloud container name)</param>
        public AzureCheckpointNamingScheme(string baseName = "")
        {
            BaseName = baseName;
        }

        /// <inheritdoc />
        public FileDescriptor LogCheckpointBase(Guid token) => new(string.Join('/', LogCheckpointBasePath, token.ToString()), null);
        /// <inheritdoc />
        public FileDescriptor LogCheckpointMetadata(Guid token) => new(string.Join('/', LogCheckpointBasePath, token.ToString()), "info.dat");
        /// <inheritdoc />
        public FileDescriptor LogSnapshot(Guid token) => new(string.Join('/', LogCheckpointBasePath, token.ToString()), "snapshot.dat");
        /// <inheritdoc />
        public FileDescriptor ObjectLogSnapshot(Guid token) => new(string.Join('/', LogCheckpointBasePath, token.ToString()), "snapshot.obj.dat");
        /// <inheritdoc />
        public FileDescriptor DeltaLog(Guid token) => new(string.Join('/', LogCheckpointBasePath, token.ToString()), "delta.dat");

        /// <inheritdoc />
        public FileDescriptor IndexCheckpointBase(Guid token) => new(string.Join('/', IndexCheckpointBasePath, token.ToString()), null);
        /// <inheritdoc />
        public FileDescriptor IndexCheckpointMetadata(Guid token) => new(string.Join('/', IndexCheckpointBasePath, token.ToString()), "info.dat");
        /// <inheritdoc />
        public FileDescriptor HashTable(Guid token) => new(string.Join('/', IndexCheckpointBasePath, token.ToString()), "ht.dat");
        /// <inheritdoc />
        public FileDescriptor TsavoriteLogCommitMetadata(long commitNumber) => new(TsavoriteLogCommitBasePath, $"commit.{commitNumber}");

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