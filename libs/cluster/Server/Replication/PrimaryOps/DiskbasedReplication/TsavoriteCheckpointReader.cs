// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.IO;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed class TsavoriteCheckpointReader : ICheckpointReader
    {
        const int BatchSize = 1 << 17;
        readonly ClusterProvider clusterProvider;
        readonly List<CheckpointDataSource> dataSources = [];

        public static int GetBatchSize(CheckpointFileType type, GarnetServerOptions serverOptions)
        {
            return type switch
            {
                CheckpointFileType.STORE_HLOG or CheckpointFileType.STORE_SNAPSHOT => 1 << serverOptions.SegmentSizeBits(isObj: false),
                CheckpointFileType.STORE_HLOG_OBJ or CheckpointFileType.STORE_SNAPSHOT_OBJ => 1 << serverOptions.SegmentSizeBits(isObj: true),
                _ => BatchSize
            };
        }

        public TsavoriteCheckpointReader(
            ClusterProvider clusterProvider,
            CheckpointEntry checkpointEntry,
            LogFileInfo logFileInfo,
            long indexSize)
        {
            this.clusterProvider = clusterProvider;

            // 1. send hlog file segments
            if (clusterProvider.serverOptions.EnableStorageTier && logFileInfo.hybridLogFileEndAddress > PageHeader.Size)
            {
                dataSources.Add(new CheckpointDataSource
                {
                    type = CheckpointFileType.STORE_HLOG,
                    token = checkpointEntry.metadata.storeHlogToken,
                    startOffset = logFileInfo.hybridLogFileStartAddress,
                    currOffset = logFileInfo.hybridLogFileStartAddress,
                    endOffset = logFileInfo.hybridLogFileEndAddress
                });

                if (logFileInfo.hasSnapshotObjects)
                    dataSources.Add(new CheckpointDataSource
                    {
                        type = CheckpointFileType.STORE_HLOG_OBJ,
                        token = checkpointEntry.metadata.storeHlogToken,
                        startOffset = logFileInfo.hybridLogObjectFileStartAddress,
                        currOffset = logFileInfo.hybridLogObjectFileStartAddress,
                        endOffset = logFileInfo.hybridLogObjectFileEndAddress
                    });
            }

            // 2.Send index file segments
            dataSources.Add(new CheckpointDataSource
            {
                type = CheckpointFileType.STORE_INDEX,
                token = checkpointEntry.metadata.storeIndexToken,
                startOffset = 0,
                currOffset = 0,
                endOffset = indexSize
            });

            // 3. Send snapshot files
            if (logFileInfo.snapshotFileEndAddress > PageHeader.Size)
            {
                dataSources.Add(new CheckpointDataSource
                {
                    type = CheckpointFileType.STORE_SNAPSHOT,
                    token = checkpointEntry.metadata.storeHlogToken,
                    startOffset = 0,
                    currOffset = 0,
                    endOffset = logFileInfo.snapshotFileEndAddress
                });
                if (logFileInfo.hasSnapshotObjects)
                    dataSources.Add(new CheckpointDataSource
                    {
                        type = CheckpointFileType.STORE_SNAPSHOT_OBJ,
                        token = checkpointEntry.metadata.storeHlogToken,
                        startOffset = 0,
                        currOffset = 0,
                        endOffset = logFileInfo.snapshotObjectFileEndAddress
                    });
            }

            // 4. Send delta log segments
            dataSources.Add(new CheckpointDataSource
            {
                type = CheckpointFileType.STORE_DLOG,
                token = checkpointEntry.metadata.storeHlogToken,
                startOffset = 0,
                currOffset = 0,
                endOffset = logFileInfo.deltaLogTailAddress
            });
        }

        public IEnumerable<CheckpointDataSource> GetNextDataSource()
        {
            foreach (var dataSource in dataSources)
            {
                var ds = dataSource;
                CreateCheckpointDevice(ref ds);
                yield return ds;
            }
        }

        private void CreateCheckpointDevice(ref CheckpointDataSource cds)
        {
            cds.device = cds.type switch
            {
                CheckpointFileType.STORE_HLOG => GetStoreHLogDevice(isObj: false),
                CheckpointFileType.STORE_HLOG_OBJ => GetStoreHLogDevice(isObj: true),
                _ => clusterProvider.ReplicationLogCheckpointManager.GetDevice(cds.type, cds.token),
            };

            var segmentSize = GetBatchSize(cds.type, clusterProvider.serverOptions);
            switch (cds.type)
            {
                case CheckpointFileType.STORE_HLOG:
                case CheckpointFileType.STORE_SNAPSHOT:
                case CheckpointFileType.STORE_HLOG_OBJ:
                case CheckpointFileType.STORE_SNAPSHOT_OBJ:
                    cds.device.Initialize(segmentSize: segmentSize);
                    break;
                default:
                    break;
            }

            IDevice GetStoreHLogDevice(bool isObj)
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
}