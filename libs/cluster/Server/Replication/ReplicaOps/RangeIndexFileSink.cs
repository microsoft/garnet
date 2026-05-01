// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// File-backed implementation of <see cref="ISnapshotDataSink"/> that writes RangeIndex
    /// BfTree snapshot data to a plain file using <see cref="FileStream"/>.
    /// Unlike <see cref="FileDataSink"/> which writes to an <see cref="Tsavorite.core.IDevice"/>,
    /// this writes directly to a file path since BfTree snapshots are not device-backed.
    /// </summary>
    internal sealed class RangeIndexFileSink : ISnapshotDataSink
    {
        private readonly string filePath;
        private readonly ILogger logger;
        private FileStream fileStream;

        public CheckpointFileType Type => CheckpointFileType.RINDEX_SNAPSHOT;
        public Guid Token { get; }

        /// <summary>
        /// Creates a new RangeIndexFileSink.
        /// </summary>
        /// <param name="token">The checkpoint token.</param>
        /// <param name="filePath">Full path to the target .bftree snapshot file.</param>
        /// <param name="logger">Optional logger.</param>
        public RangeIndexFileSink(Guid token, string filePath, ILogger logger = null)
        {
            Token = token;
            this.filePath = filePath;
            this.logger = logger;

            // Ensure the directory exists
            var dir = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);

            fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096);
        }

        /// <inheritdoc/>
        public void WriteChunk(long startAddress, ReadOnlySpan<byte> data)
        {
            if (fileStream == null)
                throw new ObjectDisposedException(nameof(RangeIndexFileSink));

            fileStream.Seek(startAddress, SeekOrigin.Begin);
            fileStream.Write(data);
        }

        /// <inheritdoc/>
        public void Complete()
        {
            fileStream?.Flush();
            fileStream?.Dispose();
            fileStream = null;
            logger?.LogInformation("Completed writing RangeIndex snapshot file: {filePath}", filePath);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            fileStream?.Dispose();
            fileStream = null;
        }
    }
}