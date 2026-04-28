// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Describes a checkpoint data source entry before device creation.
    /// </summary>
    internal readonly struct CheckpointDataSourceEntry
    {
        public readonly CheckpointFileType Type;
        public readonly Guid Token;
        public readonly long StartOffset;
        public readonly long EndOffset;

        public CheckpointDataSourceEntry(CheckpointFileType type, Guid token, long startOffset, long endOffset)
        {
            Type = type;
            Token = token;
            StartOffset = startOffset;
            EndOffset = endOffset;
        }
    }

    internal sealed class TsavoriteCheckpointReader : ICheckpointReader
    {
        readonly ClusterProvider clusterProvider;
        readonly TimeSpan timeout;
        readonly ILogger logger;
        readonly List<CheckpointDataSourceEntry> entries = [];

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
                _ => CheckpointDataSource.DefaultBatchSize
            };
        }

        public TsavoriteCheckpointReader(
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
                entries.Add(new CheckpointDataSourceEntry(
                    CheckpointFileType.STORE_HLOG,
                    checkpointEntry.metadata.storeHlogToken,
                    logFileInfo.hybridLogFileStartAddress,
                    logFileInfo.hybridLogFileEndAddress));

                if (logFileInfo.hasSnapshotObjects)
                    entries.Add(new CheckpointDataSourceEntry(
                        CheckpointFileType.STORE_HLOG_OBJ,
                        checkpointEntry.metadata.storeHlogToken,
                        logFileInfo.hybridLogObjectFileStartAddress,
                        logFileInfo.hybridLogObjectFileEndAddress));
            }

            // 2. Send index file segments
            entries.Add(new CheckpointDataSourceEntry(
                CheckpointFileType.STORE_INDEX,
                checkpointEntry.metadata.storeIndexToken,
                0,
                indexSize));

            // 3. Send snapshot files
            if (logFileInfo.snapshotFileEndAddress > PageHeader.Size)
            {
                entries.Add(new CheckpointDataSourceEntry(
                    CheckpointFileType.STORE_SNAPSHOT,
                    checkpointEntry.metadata.storeHlogToken,
                    0,
                    logFileInfo.snapshotFileEndAddress));

                if (logFileInfo.hasSnapshotObjects)
                    entries.Add(new CheckpointDataSourceEntry(
                        CheckpointFileType.STORE_SNAPSHOT_OBJ,
                        checkpointEntry.metadata.storeHlogToken,
                        0,
                        logFileInfo.snapshotObjectFileEndAddress));
            }

            // 4. Send delta log segments
            entries.Add(new CheckpointDataSourceEntry(
                CheckpointFileType.STORE_DLOG,
                checkpointEntry.metadata.storeHlogToken,
                0,
                logFileInfo.deltaLogTailAddress));
        }

        /// <inheritdoc/>
        public IEnumerable<ICheckpointDataSource> GetDataSources()
        {
            foreach (var entry in entries)
            {
                var device = CreateCheckpointDevice(entry.Type, entry.Token);
                var maxBatchSize = Math.Min(CheckpointDataSource.DefaultBatchSize, GetMaxBatchSize(entry.Type, clusterProvider.serverOptions));

                yield return new CheckpointDataSource(
                    entry.Type,
                    entry.Token,
                    device,
                    entry.StartOffset,
                    entry.EndOffset,
                    maxBatchSize,
                    timeout,
                    logger);
            }
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
    }
}