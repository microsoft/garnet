// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Enumerates the files and metadata required to transfer a Tsavorite checkpoint.
    /// </summary>
    internal sealed class TsavoriteCheckpointDataSourceReader : IDisposable
    {
        readonly ICheckpointFileTransferProvider checkpointFileProvider;
        readonly TimeSpan timeout;
        readonly ILogger logger;
        readonly List<ISnapshotDataSource> dataSources = [];
        SectorAlignedBufferPool bufferPool;
        readonly System.Threading.SemaphoreSlim signalCompletion = new(0);

        public TsavoriteCheckpointDataSourceReader(
            ICheckpointFileTransferProvider checkpointFileProvider,
            CheckpointMetadata checkpointMetadata,
            LogFileInfo logFileInfo,
            long indexSize,
            TimeSpan timeout,
            ILogger logger = null)
        {
            this.checkpointFileProvider = checkpointFileProvider;
            this.timeout = timeout;
            this.logger = logger;

            if (checkpointFileProvider.EnableStorageTier && logFileInfo.hybridLogFileEndAddress > PageHeader.Size)
            {
                dataSources.Add(CreateFileDataSource(
                    CheckpointFileType.STORE_HLOG,
                    checkpointMetadata.storeHlogToken,
                    logFileInfo.hybridLogFileStartAddress,
                    logFileInfo.hybridLogFileEndAddress));

                if (logFileInfo.hasSnapshotObjects)
                    dataSources.Add(CreateFileDataSource(
                        CheckpointFileType.STORE_HLOG_OBJ,
                        checkpointMetadata.storeHlogToken,
                        logFileInfo.hybridLogObjectFileStartAddress,
                        logFileInfo.hybridLogObjectFileEndAddress));
            }

            dataSources.Add(CreateFileDataSource(
                CheckpointFileType.STORE_INDEX,
                checkpointMetadata.storeIndexToken,
                0,
                indexSize));

            if (logFileInfo.snapshotFileEndAddress > PageHeader.Size)
            {
                dataSources.Add(CreateFileDataSource(
                    CheckpointFileType.STORE_SNAPSHOT,
                    checkpointMetadata.storeHlogToken,
                    0,
                    logFileInfo.snapshotFileEndAddress));

                if (logFileInfo.hasSnapshotObjects)
                    dataSources.Add(CreateFileDataSource(
                        CheckpointFileType.STORE_SNAPSHOT_OBJ,
                        checkpointMetadata.storeHlogToken,
                        0,
                        logFileInfo.snapshotObjectFileEndAddress));
            }

            dataSources.Add(new TsavoriteMetadataSource(
                CheckpointFileType.STORE_INDEX,
                checkpointMetadata.storeIndexToken,
                () => checkpointMetadata.storeIndexToken != default
                    ? checkpointFileProvider.GetIndexCheckpointMetadata(checkpointMetadata.storeIndexToken)
                    : []));

            dataSources.Add(new TsavoriteMetadataSource(
                CheckpointFileType.STORE_SNAPSHOT,
                checkpointMetadata.storeHlogToken,
                () => checkpointMetadata.storeHlogToken != default
                    ? checkpointFileProvider.GetLogCheckpointMetadata(checkpointMetadata.storeHlogToken)
                    : []));
        }

        /// <summary>
        /// Gets the checkpoint data sources in recovery order.
        /// </summary>
        public IEnumerable<ISnapshotDataSource> GetDataSources() => dataSources;

        FileDataSource CreateFileDataSource(CheckpointFileType type, Guid token, long startOffset, long endOffset)
        {
            var device = checkpointFileProvider.CreateCheckpointDevice(type, token);
            bufferPool ??= new SectorAlignedBufferPool(1, (int)device.SectorSize);
            var maxBatchSize = Math.Min(FileDataSource.DefaultBatchSize, checkpointFileProvider.GetMaxBatchSize(type));

            return new FileDataSource(
                type,
                token,
                device,
                startOffset,
                endOffset,
                maxBatchSize,
                timeout,
                bufferPool,
                signalCompletion,
                logger);
        }

        public void Dispose()
        {
            foreach (var dataSource in dataSources)
            {
                try { dataSource.Dispose(); }
                catch (Exception ex) { logger?.LogError(ex, "Error disposing checkpoint data source {type} {token}", dataSource.Type, dataSource.Token); }
            }
            dataSources.Clear();

            signalCompletion.Dispose();
            bufferPool?.Free();
        }
    }
}