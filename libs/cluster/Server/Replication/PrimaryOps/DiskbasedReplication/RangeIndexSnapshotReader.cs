// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// Snapshot reader for RangeIndex files. Enumerates all flush.bftree and checkpoint
    /// snapshot.bftree files that must be shipped for a given checkpoint, and yields
    /// transmit sources for each.
    ///
    /// <para>Each <see cref="RangeIndexFileTransmitSource"/> sends a filename header
    /// (with <c>startAddress = -1</c>) followed by chunked file content, so the replica
    /// knows where to write each file.</para>
    /// </summary>
    internal sealed class RangeIndexSnapshotReader : ISnapshotReader
    {
        private readonly List<RangeIndexFileDataSource> dataSources = [];
        private readonly ILogger logger;

        /// <summary>
        /// Creates a new RangeIndexSnapshotReader.
        /// </summary>
        /// <param name="rangeIndexManager">The RangeIndexManager instance for file enumeration.</param>
        /// <param name="checkpointToken">The checkpoint token (storeHlogToken).</param>
        /// <param name="hlogStartAddress">hybridLogFileStartAddress from LogFileInfo.</param>
        /// <param name="hlogEndAddress">hybridLogFileEndAddress from LogFileInfo.</param>
        /// <param name="logger">Optional logger.</param>
        public RangeIndexSnapshotReader(
            RangeIndexManager rangeIndexManager,
            Guid checkpointToken,
            long hlogStartAddress,
            long hlogEndAddress,
            ILogger logger = null)
        {
            this.logger = logger;

            foreach (var entry in rangeIndexManager.EnumerateFilesForReplication(checkpointToken, hlogStartAddress, hlogEndAddress))
            {
                var type = entry.IsFlushFile
                    ? CheckpointFileType.STORE_RANGEINDEX_FLUSH
                    : CheckpointFileType.STORE_RANGEINDEX_SNAPSHOT;

                dataSources.Add(new RangeIndexFileDataSource(type, checkpointToken, entry.Path, entry.KeyHash, entry.Address));
                logger?.LogInformation("RangeIndexSnapshotReader: queued {type} keyHash={keyHash} addr={addr}", type, entry.KeyHash, entry.Address);
            }
        }

        /// <inheritdoc/>
        public IEnumerable<ISnapshotTransmitSource> GetTransmitSources()
        {
            foreach (var dataSource in dataSources)
            {
                yield return new RangeIndexFileTransmitSource(dataSource, logger);
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            foreach (var ds in dataSources)
            {
                try { ds.Dispose(); }
                catch (Exception ex) { logger?.LogError(ex, "Error disposing RI data source {type} {keyHash}", ds.Type, ds.KeyHash); }
            }
            dataSources.Clear();
        }
    }
}
