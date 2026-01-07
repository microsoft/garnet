// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Checkpoint manager for Garnet cluster, inherits from GarnetCheckpointManager.
    /// </summary>
    internal sealed class GarnetClusterCheckpointManager : GarnetCheckpointManager, IDisposable
    {
        readonly bool isMainStore;
        public Action<bool, long, long, bool> checkpointVersionShiftStart;
        public Action<bool, long, long, bool> checkpointVersionShiftEnd;

        readonly bool safelyRemoveOutdated;

        readonly ILogger logger;

        public GarnetClusterCheckpointManager(
            int aofPhysicalSublogCount,
            INamedDeviceFactoryCreator deviceFactoryCreator,
            ICheckpointNamingScheme checkpointNamingScheme,
            bool isMainStore,
            bool safelyRemoveOutdated = false,
            int fastCommitThrottleFreq = 0,
            ILogger logger = null)
            : base(aofPhysicalSublogCount, deviceFactoryCreator, checkpointNamingScheme, removeOutdated: false, fastCommitThrottleFreq, logger)
        {
            this.isMainStore = isMainStore;
            this.safelyRemoveOutdated = safelyRemoveOutdated;
            this.logger = logger;
        }

        public override void CheckpointVersionShiftStart(long oldVersion, long newVersion, bool isStreaming)
            => checkpointVersionShiftStart?.Invoke(isMainStore, oldVersion, newVersion, isStreaming);

        public override void CheckpointVersionShiftEnd(long oldVersion, long newVersion, bool isStreaming)
            => checkpointVersionShiftEnd?.Invoke(isMainStore, oldVersion, newVersion, isStreaming);

        public void DeleteLogCheckpoint(Guid logToken)
            => deviceFactory.Delete(checkpointNamingScheme.LogCheckpointBase(logToken));

        public void DeleteIndexCheckpoint(Guid indexToken)
            => deviceFactory.Delete(checkpointNamingScheme.IndexCheckpointBase(indexToken));

        public IDevice GetDevice(CheckpointFileType retStateType, Guid fileToken)
        {
            var device = retStateType switch
            {
                CheckpointFileType.STORE_DLOG => GetDeltaLogDevice(fileToken),
                CheckpointFileType.STORE_INDEX => GetIndexDevice(fileToken),
                CheckpointFileType.STORE_SNAPSHOT => GetSnapshotLogDevice(fileToken),
                CheckpointFileType.STORE_SNAPSHOT_OBJ => GetSnapshotObjectLogDevice(fileToken),
                _ => throw new Exception($"RetrieveCheckpointFile: unexpected state{retStateType}")
            };
            return device;
        }

        #region ICheckpointManager

        private HybridLogRecoveryInfo ConvertMetadata(byte[] checkpointMetadata)
        {
            HybridLogRecoveryInfo recoveryInfo = new();

            // Try to parse new format where cookie is embedded inside the HybridLogRecoveryInfo
            try
            {
                using (StreamReader s = new(new MemoryStream(checkpointMetadata)))
                {
                    recoveryInfo.Initialize(s);
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Best effort read of checkpoint metadata failed");
                throw ex.InnerException;
            }

            return recoveryInfo;
        }

        /// <summary>
        /// Commit log checkpoint metadata with included cookie in byte array
        /// </summary>
        /// <param name="logToken"></param>
        /// <param name="checkpointMetadata"></param>
        public void CommitLogCheckpointSendFromPrimary(Guid logToken, byte[] checkpointMetadata)
        {
            var recoveryInfo = ConvertMetadata(checkpointMetadata);
            CommitLogCheckpointMetadata(logToken, recoveryInfo.ToByteArray());
        }

        /// <summary>
        /// Retrieve RecoveredSafeAofAddress and RecoveredReplicationId for checkpoint
        /// </summary>
        /// <param name="logToken"></param>
        /// <param name="deltaLog"></param>
        /// <param name="scanDelta"></param>
        /// <param name="recoverTo"></param>
        /// <param name="recoveredSafeAofAddress"></param>
        /// <param name="recoveredReplicationId"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public unsafe void GetCheckpointCookieMetadata(Guid logToken, DeltaLog deltaLog, bool scanDelta, long recoverTo, ref AofAddress recoveredSafeAofAddress, out string recoveredReplicationId)
        {
            var metadata = GetLogCheckpointMetadata(logToken, deltaLog, scanDelta, recoverTo);
            var hlri = ConvertMetadata(metadata);
            GetCookieData(hlri, ref recoveredSafeAofAddress, out recoveredReplicationId);

            static unsafe void GetCookieData(HybridLogRecoveryInfo hlri, ref AofAddress checkpointCoveredAddress, out string primaryReplId)
            {
                using var ms = new MemoryStream(hlri.cookie);
                using var reader = new BinaryReader(ms, Encoding.ASCII);
                primaryReplId = reader.ReadInt32() > 0 ? reader.ReadString() : null;
                checkpointCoveredAddress.DeserializeInPlace(reader);
                reader.Dispose();
                ms.Dispose();
            }
        }

        public override byte[] GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog, bool scanDelta, long recoverTo)
        {
            byte[] metadata = null;
            HybridLogRecoveryInfo hlri;
            if (deltaLog != null && scanDelta)
            {
                // Try to get latest valid metadata from delta-log
                deltaLog.Reset();
                while (deltaLog.GetNext(out long physicalAddress, out int entryLength, out var type))
                {
                    switch (type)
                    {
                        case DeltaLogEntryType.DELTA:
                            // consider only metadata records
                            continue;
                        case DeltaLogEntryType.CHECKPOINT_METADATA:
                            metadata = new byte[entryLength];
                            unsafe
                            {
                                fixed (byte* m = metadata)
                                    Buffer.MemoryCopy((void*)physicalAddress, m, entryLength, entryLength);
                            }
                            hlri = ConvertMetadata(metadata);
                            if (hlri.version == recoverTo || hlri.version < recoverTo && hlri.nextVersion > recoverTo)
                                goto LoopEnd;
                            continue;
                        default:
                            throw new GarnetException("Unexpected entry type");
                    }
                LoopEnd:
                    break;
                }
                if (metadata != null) return metadata;
            }

            var device = deviceFactory.Get(checkpointNamingScheme.LogCheckpointMetadata(logToken));

            ReadInto(device, 0, out byte[] writePad, sizeof(int));
            var size = BitConverter.ToInt32(writePad, 0);

            byte[] body;
            if (writePad.Length >= size + sizeof(int))
                body = writePad;
            else
                ReadInto(device, 0, out body, size + sizeof(int));
            device.Dispose();

            body = body.AsSpan().Slice(sizeof(int), size).ToArray();
            hlri = ConvertMetadata(body);
            return hlri.ToByteArray();
        }

        #endregion
    }
}