// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Checkpoint storage adapter used by snapshot transfer components.
    /// </summary>
    internal sealed class CheckpointFileTransferProvider : ICheckpointFileTransferProvider
    {
        const int DefaultBatchSize = 1 << 17;

        readonly GarnetServerOptions serverOptions;
        readonly GarnetCheckpointManager checkpointManager;

        public bool EnableStorageTier => serverOptions.EnableStorageTier;

        public CheckpointFileTransferProvider(
            GarnetServerOptions serverOptions,
            GarnetCheckpointManager checkpointManager)
        {
            this.serverOptions = serverOptions;
            this.checkpointManager = checkpointManager;
        }

        public int GetMaxBatchSize(CheckpointFileType type)
            => type switch
            {
                CheckpointFileType.STORE_HLOG or CheckpointFileType.STORE_SNAPSHOT => 1 << serverOptions.SegmentSizeBits(isObj: false),
                CheckpointFileType.STORE_HLOG_OBJ or CheckpointFileType.STORE_SNAPSHOT_OBJ => 1 << serverOptions.SegmentSizeBits(isObj: true),
                _ => DefaultBatchSize
            };

        public IDevice CreateCheckpointDevice(CheckpointFileType type, Guid token)
        {
            var device = type switch
            {
                CheckpointFileType.STORE_HLOG => GetStoreHLogDevice(isObj: false),
                CheckpointFileType.STORE_HLOG_OBJ => GetStoreHLogDevice(isObj: true),
                CheckpointFileType.STORE_INDEX => checkpointManager.GetIndexDevice(token),
                CheckpointFileType.STORE_SNAPSHOT => checkpointManager.GetSnapshotLogDevice(token),
                CheckpointFileType.STORE_SNAPSHOT_OBJ => checkpointManager.GetSnapshotObjectLogDevice(token),
                _ => throw new GarnetException($"Invalid checkpoint file type {type}")
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
            switch (type)
            {
                case CheckpointFileType.STORE_SNAPSHOT:
                    checkpointManager.CommitLogCheckpointFromTransfer(token, metadata);
                    break;
                case CheckpointFileType.STORE_INDEX:
                    checkpointManager.CommitIndexCheckpoint(token, metadata.ToArray());
                    break;
                default:
                    throw new GarnetException($"Invalid checkpoint file type {type}");
            }
        }

        IDevice GetStoreHLogDevice(bool isObj)
        {
            if (!serverOptions.EnableStorageTier)
                return null;

            var logDir = !string.IsNullOrEmpty(serverOptions.LogDir) ? serverOptions.LogDir : Directory.GetCurrentDirectory();
            var logFactory = serverOptions.GetInitializedDeviceFactory(logDir);

            return logFactory.Get(new FileDescriptor("Store", isObj ? "hlog_objs" : "hlog"));
        }
    }
}