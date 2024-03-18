// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal class ReplicationLogCheckpointManager : DeviceLogCommitCheckpointManager, IDisposable
    {
        public long CurrentSafeAofAddress;
        public long RecoveredSafeAofAddress;

        public string PrimaryReplicationId;

        readonly bool isMainStore;
        public Action<bool, long, long> checkpointVersionShift;

        readonly bool safelyRemoveOutdated;

        public ReplicationLogCheckpointManager(
            INamedDeviceFactory deviceFactory,
            ICheckpointNamingScheme checkpointNamingScheme,
            bool isMainStore,
            bool removeOutdated = false,
            int fastCommitThrottleFreq = 0,
            ILogger logger = null)
            : base(deviceFactory, checkpointNamingScheme, removeOutdated: false, fastCommitThrottleFreq, logger)
        {
            this.isMainStore = isMainStore;
            this.safelyRemoveOutdated = removeOutdated;
            CurrentSafeAofAddress = 0;
            RecoveredSafeAofAddress = 0;
            PrimaryReplicationId = String.Empty;
        }

        public override void CheckpointVersionShift(long oldVersion, long newVersion)
        {
            checkpointVersionShift?.Invoke(isMainStore, oldVersion, newVersion);
        }

        public void DeleteLogCheckpoint(Guid logToken)
            => deviceFactory.Delete(checkpointNamingScheme.LogCheckpointBase(logToken));

        public void DeleteIndexCheckpoint(Guid indexToken)
            => deviceFactory.Delete(checkpointNamingScheme.IndexCheckpointBase(indexToken));

        public IDevice GetDevice(CheckpointFileType retStateType, Guid fileToken)
        {
            IDevice device = retStateType switch
            {
                CheckpointFileType.STORE_DLOG => GetDeltaLogDevice(fileToken),
                CheckpointFileType.STORE_INDEX => GetIndexDevice(fileToken),
                CheckpointFileType.STORE_SNAPSHOT => GetSnapshotLogDevice(fileToken),
                CheckpointFileType.OBJ_STORE_DLOG => GetDeltaLogDevice(fileToken),
                CheckpointFileType.OBJ_STORE_INDEX => GetIndexDevice(fileToken),
                CheckpointFileType.OBJ_STORE_SNAPSHOT => GetSnapshotLogDevice(fileToken),
                CheckpointFileType.OBJ_STORE_SNAPSHOT_OBJ => GetSnapshotObjectLogDevice(fileToken),
                _ => throw new Exception($"RetrieveCheckpointFile: unexpected state{retStateType}")
            };
            return device;
        }


        #region ICheckpointManager

        /// <summary>
        /// Pre-append cookie in commitMetadata.
        /// cookieMetadata 52 bytes
        /// 1. 4 bytes to track size of cookie
        /// 2. 8 bytes for checkpointCoveredAddress
        /// 3. 40 bytes for primaryReplicationId
        /// </summary>        
        /// <param name="commitMetadata"></param>
        /// <returns></returns>
        private unsafe byte[] AddCookie(byte[] commitMetadata)
        {
            int cookieSize = sizeof(long) + this.PrimaryReplicationId.Length;
            byte[] commitMetadataWithCookie = new byte[sizeof(int) + cookieSize + commitMetadata.Length];
            byte[] primaryReplIdBytes = Encoding.ASCII.GetBytes(PrimaryReplicationId);
            fixed (byte* ptr = commitMetadataWithCookie)
            fixed (byte* pridPtr = primaryReplIdBytes)
            fixed (byte* cmPtr = commitMetadata)
            {
                *(int*)ptr = cookieSize;
                *(long*)(ptr + 4) = CurrentSafeAofAddress;
                Buffer.MemoryCopy(pridPtr, ptr + 12, primaryReplIdBytes.Length, primaryReplIdBytes.Length);
                Buffer.MemoryCopy(cmPtr, ptr + 12 + primaryReplIdBytes.Length, commitMetadata.Length, commitMetadata.Length);
            }
            return commitMetadataWithCookie;
        }

        private byte[] ExtractCookie(byte[] commitMetadataWithCookie)
        {
            int cookieTotalSize = GetCookieData(commitMetadataWithCookie, out RecoveredSafeAofAddress, out PrimaryReplicationId);
            int payloadSize = commitMetadataWithCookie.Length - cookieTotalSize;

            byte[] commitMetadata = new byte[payloadSize];
            Array.Copy(commitMetadataWithCookie, cookieTotalSize, commitMetadata, 0, payloadSize);
            return commitMetadata;
        }

        private unsafe int GetCookieData(byte[] commitMetadataWithCookie, out long checkpointCoveredAddress, out string primaryReplId)
        {
            checkpointCoveredAddress = -1;
            primaryReplId = null;
            int size = sizeof(int);
            fixed (byte* ptr = commitMetadataWithCookie)
            {
                if (commitMetadataWithCookie.Length < 4) throw new Exception($"invalid metadata length: {commitMetadataWithCookie.Length} < 4");
                int cookieSize = *(int*)ptr;
                size += cookieSize;

                if (commitMetadataWithCookie.Length < 12) throw new Exception($"invalid metadata length: {commitMetadataWithCookie.Length} < 12");
                checkpointCoveredAddress = *(long*)(ptr + 4);

                if (commitMetadataWithCookie.Length < 52) throw new Exception($"invalid metadata length: {commitMetadataWithCookie.Length} < 52");
                primaryReplId = Encoding.ASCII.GetString(ptr + 12, 40);
            }
            return size;
        }

        public unsafe (long, string) GetCheckpointCookieMetadata(Guid logToken, DeltaLog deltaLog, bool scanDelta, long recoverTo)
        {
            byte[] metadata = GetLogCheckpointMetadata(logToken, deltaLog, scanDelta, recoverTo, withoutCookie: false);
            GetCookieData(metadata, out var checkpointCoveredAddress, out var primaryReplId);
            return (checkpointCoveredAddress, primaryReplId);
        }

        public override byte[] GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog, bool scanDelta, long recoverTo)
            => GetLogCheckpointMetadata(logToken, deltaLog, scanDelta, recoverTo);

        /// <summary>
        /// Commit log checkpoint metadata and append cookie
        /// </summary>
        /// <param name="logToken"></param>
        /// <param name="commitMetadata"></param>
        public override unsafe void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            byte[] commitMetadataWithCookie = AddCookie(commitMetadata);
            base.CommitLogCheckpoint(logToken, commitMetadataWithCookie);
        }

        /// <summary>
        /// Commit log checkpoint metadata with included cookie in byte array
        /// </summary>
        /// <param name="logToken"></param>
        /// <param name="commitMetadataWithCookie"></param>
        public void CommiLogCheckpointWithCookie(Guid logToken, byte[] commitMetadataWithCookie)
            => base.CommitLogCheckpoint(logToken, commitMetadataWithCookie);

        public override unsafe void CommitLogIncrementalCheckpoint(Guid logToken, long version, byte[] commitMetadata, DeltaLog deltaLog)
        {
            byte[] commitMetadataWithCookie = AddCookie(commitMetadata);
            base.CommitLogIncrementalCheckpoint(logToken, version, commitMetadataWithCookie, deltaLog);
        }

        public byte[] GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog, bool scanDelta, long recoverTo, bool withoutCookie = true)
        {
            byte[] metadata = null;
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
                            var metadataWithoutCookie = ExtractCookie(metadata);
                            if (withoutCookie) metadata = metadataWithoutCookie;
                            HybridLogRecoveryInfo recoveryInfo = new();
                            using (StreamReader s = new(new MemoryStream(metadataWithoutCookie)))
                            {
                                recoveryInfo.Initialize(s);
                                // Finish recovery if only specific versions are requested
                                if (recoveryInfo.version == recoverTo || recoveryInfo.version < recoverTo && recoveryInfo.nextVersion > recoverTo) goto LoopEnd;
                            }
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
            int size = BitConverter.ToInt32(writePad, 0);

            byte[] body;
            if (writePad.Length >= size + sizeof(int))
                body = writePad;
            else
                ReadInto(device, 0, out body, size + sizeof(int));
            device.Dispose();

            body = body.AsSpan().Slice(sizeof(int), size).ToArray();
            if (withoutCookie) body = ExtractCookie(body);
            return body;
        }

        #endregion
    }
}