// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Cluster checkpoint storage adapter used by snapshot transfer components.
    /// </summary>
    internal sealed class ClusterCheckpointFileTransferProvider : ICheckpointFileTransferProvider
    {
        readonly GarnetServerOptions serverOptions;
        readonly GarnetClusterCheckpointManager checkpointManager;

        public bool EnableStorageTier => serverOptions.EnableStorageTier;

        public ClusterCheckpointFileTransferProvider(
            GarnetServerOptions serverOptions,
            GarnetClusterCheckpointManager checkpointManager)
        {
            this.serverOptions = serverOptions;
            this.checkpointManager = checkpointManager;
        }

        public int GetMaxBatchSize(CheckpointFileType type)
            => type switch
            {
                CheckpointFileType.STORE_HLOG or CheckpointFileType.STORE_SNAPSHOT => 1 << serverOptions.SegmentSizeBits(isObj: false),
                CheckpointFileType.STORE_HLOG_OBJ or CheckpointFileType.STORE_SNAPSHOT_OBJ => 1 << serverOptions.SegmentSizeBits(isObj: true),
                _ => FileDataSource.DefaultBatchSize
            };

        public IDevice CreateCheckpointDevice(CheckpointFileType type, Guid token)
        {
            var device = type switch
            {
                CheckpointFileType.STORE_HLOG => GetStoreHLogDevice(isObj: false),
                CheckpointFileType.STORE_HLOG_OBJ => GetStoreHLogDevice(isObj: true),
                _ => checkpointManager.GetDevice(type, token),
            };

            switch (type)
            {
                case CheckpointFileType.STORE_HLOG:
                case CheckpointFileType.STORE_SNAPSHOT:
                case CheckpointFileType.STORE_HLOG_OBJ:
                case CheckpointFileType.STORE_SNAPSHOT_OBJ:
                    device.Initialize(segmentSize: GetMaxBatchSize(type));
                    break;
            }

            return device;
        }

        public byte[] GetIndexCheckpointMetadata(Guid token)
            => checkpointManager.GetIndexCheckpointMetadata(token);

        public byte[] GetLogCheckpointMetadata(Guid token)
            => checkpointManager.GetLogCheckpointMetadata(token);

        public void CommitCheckpointMetadata(CheckpointFileType type, Guid token, ReadOnlySpan<byte> metadata)
        {
            var checkpointMetadata = metadata.ToArray();
            switch (type)
            {
                case CheckpointFileType.STORE_SNAPSHOT:
                    checkpointManager.CommitLogCheckpointSendFromPrimary(token, checkpointMetadata);
                    break;
                case CheckpointFileType.STORE_INDEX:
                    checkpointManager.CommitIndexCheckpoint(token, checkpointMetadata);
                    break;
                default:
                    throw new GarnetException($"Invalid checkpoint file type {type}");
            }
        }

        private IDevice GetStoreHLogDevice(bool isObj)
        {
            if (!serverOptions.EnableStorageTier)
                return null;

            var logDir = !string.IsNullOrEmpty(serverOptions.LogDir) ? serverOptions.LogDir : Directory.GetCurrentDirectory();
            var logFactory = serverOptions.GetInitializedDeviceFactory(logDir);

            // These must match GarnetServerOptions.GetSettings when storage tiering is enabled.
            return logFactory.Get(new FileDescriptor("Store", isObj ? "hlog_objs" : "hlog"));
        }
    }
}