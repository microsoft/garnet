// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed class TsavoriteSnapshotReader : ISnapshotReader
    {
        readonly ILogger logger;
        readonly TsavoriteCheckpointDataSourceReader dataSourceReader;

        public TsavoriteSnapshotReader(
            ICheckpointFileTransferProvider checkpointFileProvider,
            CheckpointEntry checkpointEntry,
            LogFileInfo logFileInfo,
            long indexSize,
            TimeSpan timeout,
            ILogger logger = null)
        {
            this.logger = logger;
            dataSourceReader = new TsavoriteCheckpointDataSourceReader(
                checkpointFileProvider,
                checkpointEntry.metadata,
                logFileInfo,
                indexSize,
                timeout,
                logger);
        }

        /// <inheritdoc/>
        public IEnumerable<ISnapshotTransmitSource> GetTransmitSources()
        {
            foreach (var dataSource in dataSourceReader.GetDataSources())
            {
                yield return dataSource is TsavoriteMetadataSource
                    ? new TsavoriteMetadataTransmitSource(dataSource, logger)
                    : new FileTransmitSource(dataSource, logger);
            }
        }

        public void Dispose()
        {
            dataSourceReader.Dispose();
        }
    }
}