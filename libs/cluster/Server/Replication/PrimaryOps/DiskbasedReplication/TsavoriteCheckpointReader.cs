// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed class TsavoriteSnapshotReader : ISnapshotReader
    {
        readonly ICheckpointFileTransferProvider checkpointFileProvider;
        readonly TimeSpan timeout;
        readonly ILogger logger;
        readonly List<ISnapshotDataSource> fileDataSources = [];
        readonly List<ISnapshotDataSource> metadataDataSources = [];
        SectorAlignedBufferPool bufferPool;
        readonly SemaphoreSlim signalCompletion = new(0);

        public TsavoriteSnapshotReader(
            ICheckpointFileTransferProvider checkpointFileProvider,
            CheckpointEntry checkpointEntry,
            LogFileInfo logFileInfo,
            long indexSize,
            TimeSpan timeout,
            ILogger logger = null)
        {
            this.checkpointFileProvider = checkpointFileProvider;
            this.timeout = timeout;
            this.logger = logger;

            // 1. send hlog file segments
            if (checkpointFileProvider.EnableStorageTier && logFileInfo.hybridLogFileEndAddress > PageHeader.Size)
            {
                fileDataSources.Add(CreateFileDataSource(
                    CheckpointFileType.STORE_HLOG,
                    checkpointEntry.metadata.storeHlogToken,
                    logFileInfo.hybridLogFileStartAddress,
                    logFileInfo.hybridLogFileEndAddress));

                if (logFileInfo.hasSnapshotObjects)
                    fileDataSources.Add(CreateFileDataSource(
                        CheckpointFileType.STORE_HLOG_OBJ,
                        checkpointEntry.metadata.storeHlogToken,
                        logFileInfo.hybridLogObjectFileStartAddress,
                        logFileInfo.hybridLogObjectFileEndAddress));
            }

            // 2. Send index file segments
            fileDataSources.Add(CreateFileDataSource(
                CheckpointFileType.STORE_INDEX,
                checkpointEntry.metadata.storeIndexToken,
                0,
                indexSize));

            // 3. Send snapshot files
            if (logFileInfo.snapshotFileEndAddress > PageHeader.Size)
            {
                fileDataSources.Add(CreateFileDataSource(
                    CheckpointFileType.STORE_SNAPSHOT,
                    checkpointEntry.metadata.storeHlogToken,
                    0,
                    logFileInfo.snapshotFileEndAddress));

                if (logFileInfo.hasSnapshotObjects)
                    fileDataSources.Add(CreateFileDataSource(
                        CheckpointFileType.STORE_SNAPSHOT_OBJ,
                        checkpointEntry.metadata.storeHlogToken,
                        0,
                        logFileInfo.snapshotObjectFileEndAddress));
            }

            // 4. Metadata sources
            metadataDataSources.Add(new TsavoriteMetadataSource(
                CheckpointFileType.STORE_INDEX,
                checkpointEntry.metadata.storeIndexToken,
                () => checkpointEntry.metadata.storeIndexToken != default
                    ? checkpointFileProvider.GetIndexCheckpointMetadata(checkpointEntry.metadata.storeIndexToken)
                    : []));

            metadataDataSources.Add(new TsavoriteMetadataSource(
                CheckpointFileType.STORE_SNAPSHOT,
                checkpointEntry.metadata.storeHlogToken,
                () => checkpointEntry.metadata.storeHlogToken != default
                    ? checkpointFileProvider.GetLogCheckpointMetadata(checkpointEntry.metadata.storeHlogToken)
                    : []));
        }

        /// <inheritdoc/>
        public IEnumerable<ISnapshotTransmitSource> GetTransmitSources()
        {
            foreach (var dataSource in fileDataSources)
            {
                yield return new FileTransmitSource(dataSource, logger);
            }

            foreach (var dataSource in metadataDataSources)
            {
                yield return new TsavoriteMetadataTransmitSource(dataSource, logger);
            }
        }

        private FileDataSource CreateFileDataSource(CheckpointFileType type, Guid token, long startOffset, long endOffset)
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
            foreach (var ds in fileDataSources)
            {
                try { ds.Dispose(); }
                catch (Exception ex) { logger?.LogError(ex, "Error disposing file data source {type} {token}", ds.Type, ds.Token); }
            }
            fileDataSources.Clear();

            foreach (var ds in metadataDataSources)
            {
                try { ds.Dispose(); }
                catch (Exception ex) { logger?.LogError(ex, "Error disposing metadata data source {type} {token}", ds.Type, ds.Token); }
            }
            metadataDataSources.Clear();

            signalCompletion?.Dispose();
            bufferPool?.Free();
        }
    }
}