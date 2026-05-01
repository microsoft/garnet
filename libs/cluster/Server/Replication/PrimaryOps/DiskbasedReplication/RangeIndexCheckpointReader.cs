// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Checkpoint reader that enumerates RangeIndex BfTree snapshot files for a given
    /// checkpoint token and yields transmit sources for each file.
    /// Uses <see cref="ManagedLocalStorageDevice"/> and <see cref="SectorAlignedBufferPool"/>
    /// for sector-aligned, zero-copy reads matching the <see cref="TsavoriteCheckpointReader"/> pattern.
    /// </summary>
    internal sealed class RangeIndexCheckpointReader : ISnapshotReader
    {
        readonly RangeIndexManager rangeIndexManager;
        readonly Guid checkpointToken;
        readonly TimeSpan timeout;
        readonly ILogger logger;

        readonly SemaphoreSlim signalCompletion = new(0);
        SectorAlignedBufferPool bufferPool;

        /// <summary>
        /// Creates a new RangeIndexCheckpointReader.
        /// </summary>
        /// <param name="rangeIndexManager">The RangeIndex manager to enumerate snapshot files from.</param>
        /// <param name="checkpointToken">The checkpoint token identifying the snapshot files.</param>
        /// <param name="timeout">Timeout for async I/O operations.</param>
        /// <param name="logger">Optional logger.</param>
        public RangeIndexCheckpointReader(RangeIndexManager rangeIndexManager, Guid checkpointToken, TimeSpan timeout, ILogger logger = null)
        {
            this.rangeIndexManager = rangeIndexManager;
            this.checkpointToken = checkpointToken;
            this.timeout = timeout;
            this.logger = logger;
        }

        /// <inheritdoc/>
        public IEnumerable<ISnapshotTransmitSource> GetTransmitSources()
        {
            foreach (var (keyHashDir, filePath, fileSize) in rangeIndexManager.EnumerateCheckpointSnapshotFiles(checkpointToken))
            {
                if (fileSize <= 0)
                    continue;

                logger?.LogInformation("Enumerating RangeIndex snapshot file {keyHashDir} size={fileSize}", keyHashDir, fileSize);

                var device = new ManagedLocalStorageDevice(filePath, readOnly: true);
                bufferPool ??= new SectorAlignedBufferPool(1, (int)device.SectorSize);

                var dataSource = new FileDataSource(
                    CheckpointFileType.RINDEX_SNAPSHOT,
                    checkpointToken,
                    device,
                    startOffset: 0,
                    endOffset: fileSize,
                    FileDataSource.DefaultBatchSize,
                    timeout,
                    bufferPool,
                    signalCompletion,
                    logger);

                yield return new RangeIndexFileTransmitSource(dataSource, keyHashDir, logger);
            }
        }

        /// <summary>
        /// Disposes shared resources (buffer pool and semaphore).
        /// Individual device instances are disposed by their owning <see cref="FileDataSource"/>.
        /// </summary>
        public void Dispose()
        {
            signalCompletion?.Dispose();
            bufferPool?.Free();
        }
    }
}