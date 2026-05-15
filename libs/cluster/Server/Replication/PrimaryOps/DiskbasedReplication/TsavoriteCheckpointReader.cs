// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed class TsavoriteSnapshotReader : ISnapshotReader
    {
        readonly ClusterProvider clusterProvider;
        readonly TimeSpan timeout;
        readonly ILogger logger;
        readonly List<ISnapshotDataSource> fileDataSources = [];
        readonly List<ISnapshotDataSource> metadataDataSources = [];
        SectorAlignedBufferPool bufferPool;
        readonly SemaphoreSlim signalCompletion = new(0);

        /// <summary>
        /// Computes the maximum batch size for a given checkpoint file type.
        /// For segmented types (HLOG, SNAPSHOT), returns the segment size.
        /// For other types, returns the default batch size.
        /// The actual read batch is capped at min(DefaultBatchSize, GetMaxBatchSize).
        /// </summary>
        public static int GetMaxBatchSize(CheckpointFileType type, GarnetServerOptions serverOptions)
        {
            return type switch
            {
                CheckpointFileType.STORE_HLOG or CheckpointFileType.STORE_SNAPSHOT => 1 << serverOptions.SegmentSizeBits(isObj: false),
                CheckpointFileType.STORE_HLOG_OBJ or CheckpointFileType.STORE_SNAPSHOT_OBJ => 1 << serverOptions.SegmentSizeBits(isObj: true),
                _ => FileDataSource.DefaultBatchSize
            };
        }

        public TsavoriteSnapshotReader(
            ClusterProvider clusterProvider,
            CheckpointEntry checkpointEntry,
            LogFileInfo logFileInfo,
            long indexSize,
            TimeSpan timeout,
            ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.timeout = timeout;
            this.logger = logger;

            // 1. send hlog file segments
            if (clusterProvider.serverOptions.EnableStorageTier && logFileInfo.hybridLogFileEndAddress > PageHeader.Size)
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
            var storeCkptManager = clusterProvider.ReplicationLogCheckpointManager;

            metadataDataSources.Add(new TsavoriteMetadataSource(
                CheckpointFileType.STORE_INDEX,
                checkpointEntry.metadata.storeIndexToken,
                () => checkpointEntry.metadata.storeIndexToken != default
                    ? storeCkptManager.GetIndexCheckpointMetadata(checkpointEntry.metadata.storeIndexToken)
                    : []));

            metadataDataSources.Add(new TsavoriteMetadataSource(
                CheckpointFileType.STORE_SNAPSHOT,
                checkpointEntry.metadata.storeHlogToken,
                () => checkpointEntry.metadata.storeHlogToken != default
                    ? storeCkptManager.GetLogCheckpointMetadata(checkpointEntry.metadata.storeHlogToken)
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
            var device = CreateCheckpointDevice(type, token);
            bufferPool ??= new SectorAlignedBufferPool(1, (int)device.SectorSize);
            var maxBatchSize = Math.Min(FileDataSource.DefaultBatchSize, GetMaxBatchSize(type, clusterProvider.serverOptions));

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

        private IDevice CreateCheckpointDevice(CheckpointFileType type, Guid token)
        {
            var device = type switch
            {
                CheckpointFileType.STORE_HLOG => GetStoreHLogDevice(isObj: false),
                CheckpointFileType.STORE_HLOG_OBJ => GetStoreHLogDevice(isObj: true),
                _ => clusterProvider.ReplicationLogCheckpointManager.GetDevice(type, token),
            };

            var segmentSize = GetMaxBatchSize(type, clusterProvider.serverOptions);
            switch (type)
            {
                case CheckpointFileType.STORE_HLOG:
                case CheckpointFileType.STORE_SNAPSHOT:
                case CheckpointFileType.STORE_HLOG_OBJ:
                case CheckpointFileType.STORE_SNAPSHOT_OBJ:
                    device.Initialize(segmentSize: segmentSize);
                    break;
            }

            return device;
        }

        private IDevice GetStoreHLogDevice(bool isObj)
        {
            var opts = clusterProvider.serverOptions;
            if (opts.EnableStorageTier)
            {
                var LogDir = !string.IsNullOrEmpty(opts.LogDir) ? opts.LogDir : Directory.GetCurrentDirectory();
                var logFactory = opts.GetInitializedDeviceFactory(LogDir);

                // These must match GarnetServerOptions.GetSettings, EnableStorageTier
                return logFactory.Get(new FileDescriptor("Store", isObj ? "hlog_objs" : "hlog"));
            }
            return null;
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