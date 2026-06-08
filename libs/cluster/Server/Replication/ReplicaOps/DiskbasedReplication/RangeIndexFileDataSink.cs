// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// FileStream-backed implementation of <see cref="ISnapshotDataSink"/> that writes
    /// RangeIndex .bftree file segments received from the primary during replication.
    /// </summary>
    internal sealed class RangeIndexFileDataSink : ISnapshotDataSink
    {
        private readonly string filePath;
        private readonly ILogger logger;
        private FileStream stream;

        public CheckpointFileType Type { get; }
        public Guid Token { get; }

        /// <summary>
        /// Creates a new RangeIndexFileDataSink.
        /// </summary>
        /// <param name="type">The checkpoint file type.</param>
        /// <param name="token">The checkpoint token.</param>
        /// <param name="filePath">Full path to write the received file.</param>
        /// <param name="logger">Optional logger.</param>
        private RangeIndexFileDataSink(CheckpointFileType type, Guid token, string filePath, ILogger logger = null)
        {
            Type = type;
            Token = token;
            this.filePath = filePath;
            this.logger = logger;

            // Ensure the target directory exists
            var dir = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);

            // Open for writing (overwrite any partial file from a prior failed attempt)
            stream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 1 << 17, useAsync: false);
        }

        /// <summary>
        /// Deserializes the metadata payload and creates a sink targeting the correct file path.
        /// <para>Metadata layout (sizes defined in <see cref="RangeIndexFileDataSource"/>):</para>
        /// <list type="bullet">
        /// <item><b>STORE_RANGEINDEX_FLUSH</b>: keyHash (KeyHashLength bytes ASCII) + address (AddressLength bytes LE) = FlushMetadataLength bytes</item>
        /// <item><b>STORE_RANGEINDEX_SNAPSHOT</b>: keyHash (KeyHashLength bytes ASCII)</item>
        /// </list>
        /// </summary>
        /// <param name="type">The checkpoint file type.</param>
        /// <param name="token">The checkpoint token.</param>
        /// <param name="metadata">The raw metadata bytes from the header message.</param>
        /// <param name="riManager">The RangeIndex manager for path derivation.</param>
        /// <param name="logger">Optional logger.</param>
        public static RangeIndexFileDataSink FromMetadata(CheckpointFileType type, Guid token, ReadOnlySpan<byte> metadata, RangeIndexManager riManager, ILogger logger = null)
        {
            if (metadata.Length < RangeIndexFileDataSource.KeyHashLength)
                ExceptionUtils.ThrowException(new GarnetException($"RangeIndex metadata too short ({metadata.Length} bytes) for type {type}"));

            var keyHash = Encoding.ASCII.GetString(metadata[..RangeIndexFileDataSource.KeyHashLength]);
            string filePath;

            if (type == CheckpointFileType.STORE_RANGEINDEX_FLUSH)
            {
                if (metadata.Length < RangeIndexFileDataSource.FlushMetadataLength)
                    ExceptionUtils.ThrowException(new GarnetException($"RangeIndex flush metadata too short ({metadata.Length} bytes), expected {RangeIndexFileDataSource.FlushMetadataLength}"));
                var address = BinaryPrimitives.ReadInt64LittleEndian(metadata[RangeIndexFileDataSource.KeyHashLength..RangeIndexFileDataSource.FlushMetadataLength]);
                filePath = riManager.LogFlushPath(keyHash, address);
            }
            else
            {
                filePath = riManager.CheckpointSnapshotPath(keyHash, token);
            }

            return new RangeIndexFileDataSink(type, token, filePath, logger);
        }

        /// <inheritdoc/>
        public void WriteChunk(long startAddress, ReadOnlySpan<byte> data)
        {
            if (stream.Position != startAddress)
                ExceptionUtils.ThrowException(new GarnetException($"RangeIndexFileDataSink: expected stream position {startAddress} but found {stream.Position} for {filePath}"));
            stream.Write(data);
        }

        /// <inheritdoc/>
        public void Complete()
        {
            stream?.Flush();
            stream?.Dispose();
            stream = null;
            logger?.LogInformation("RangeIndexFileDataSink: completed writing {type} to {path}", Type, filePath);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            stream?.Dispose();
            stream = null;
        }
    }
}