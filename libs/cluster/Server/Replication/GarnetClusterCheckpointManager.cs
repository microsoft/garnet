// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
            INamedDeviceFactoryCreator deviceFactoryCreator,
            ICheckpointNamingScheme checkpointNamingScheme,
            bool isMainStore,
            bool safelyRemoveOutdated = false,
            int fastCommitThrottleFreq = 0,
            ILogger logger = null)
            : base(deviceFactoryCreator, checkpointNamingScheme, removeOutdated: false, fastCommitThrottleFreq, logger)
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
                CheckpointFileType.OBJ_STORE_DLOG => GetDeltaLogDevice(fileToken),
                CheckpointFileType.OBJ_STORE_INDEX => GetIndexDevice(fileToken),
                CheckpointFileType.OBJ_STORE_SNAPSHOT => GetSnapshotLogDevice(fileToken),
                CheckpointFileType.OBJ_STORE_SNAPSHOT_OBJ => GetSnapshotObjectLogDevice(fileToken),
                _ => throw new Exception($"RetrieveCheckpointFile: unexpected state{retStateType}")
            };
            return device;
        }

        #region ICheckpointManager

        private HybridLogRecoveryInfo ConvertMetadata(byte[] checkpointMetadata)
        {
            // NOTE: this conversion should be simplified after suspending support for the old format which assumed the cookie is stored in the prefix.
            var success = true;
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
                success = false;
            }

            if (!success)
            {
                // If failed to parse above cookie is at prefix
                // so extract it and convert it to new format
                // NOTE: this needs to be deprecated at some point after 1.0.61 because conversion will not be necessary.
                var metadataWithoutCookie = ExtractCookie(checkpointMetadata);
                try
                {
                    using (StreamReader s = new(new MemoryStream(metadataWithoutCookie)))
                    {
                        recoveryInfo.Initialize(s);
                    }

                    var cookieSize = checkpointMetadata.Length - metadataWithoutCookie.Length;
                    var cookie = new byte[cookieSize];
                    Array.Copy(checkpointMetadata, cookie, cookieSize);
                    recoveryInfo.cookie = cookie;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Old format checkpoint metadata failed");
                    throw ex.InnerException;
                }

                byte[] ExtractCookie(byte[] commitMetadataWithCookie)
                {
                    var cookieTotalSize = GetCookieData(commitMetadataWithCookie, out var recoveredSafeAofAddress, out var recoveredReplicationId);
                    RecoveredSafeAofAddress = recoveredSafeAofAddress;
                    RecoveredHistoryId = recoveredReplicationId;
                    var payloadSize = commitMetadataWithCookie.Length - cookieTotalSize;

                    var commitMetadata = new byte[payloadSize];
                    Array.Copy(commitMetadataWithCookie, cookieTotalSize, commitMetadata, 0, payloadSize);
                    return commitMetadata;

                    unsafe int GetCookieData(byte[] commitMetadataWithCookie, out long checkpointCoveredAddress, out string primaryReplId)
                    {
                        checkpointCoveredAddress = -1;
                        primaryReplId = null;
                        var size = sizeof(int);
                        fixed (byte* ptr = commitMetadataWithCookie)
                        {
                            if (commitMetadataWithCookie.Length < 4) throw new Exception($"invalid metadata length: {commitMetadataWithCookie.Length} < 4");
                            var cookieSize = *(int*)ptr;
                            size += cookieSize;

                            if (commitMetadataWithCookie.Length < 12) throw new Exception($"invalid metadata length: {commitMetadataWithCookie.Length} < 12");
                            checkpointCoveredAddress = *(long*)(ptr + 4);

                            if (commitMetadataWithCookie.Length < 52) throw new Exception($"invalid metadata length: {commitMetadataWithCookie.Length} < 52");
                            primaryReplId = Encoding.ASCII.GetString(ptr + 12, 40);
                        }
                        return size;
                    }
                }
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
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public unsafe (long RecoveredSafeAofAddress, string RecoveredReplicationId) GetCheckpointCookieMetadata(Guid logToken, DeltaLog deltaLog, bool scanDelta, long recoverTo)
        {
            var metadata = GetLogCheckpointMetadata(logToken, deltaLog, scanDelta, recoverTo);
            var hlri = ConvertMetadata(metadata);
            var bytesRead = GetCookieData(hlri, out var RecoveredSafeAofAddress, out var RecoveredReplicationId);
            Debug.Assert(bytesRead == 52);
            return (RecoveredSafeAofAddress, RecoveredReplicationId);

            static unsafe int GetCookieData(HybridLogRecoveryInfo hlri, out long checkpointCoveredAddress, out string primaryReplId)
            {
                checkpointCoveredAddress = -1;
                primaryReplId = null;

                var bytesRead = sizeof(int);
                fixed (byte* ptr = hlri.cookie)
                {
                    if (hlri.cookie.Length < 4) throw new Exception($"invalid metadata length: {hlri.cookie.Length} < 4");
                    var cookieSize = *(int*)ptr;
                    bytesRead += cookieSize;

                    if (hlri.cookie.Length < 12) throw new Exception($"invalid metadata length: {hlri.cookie.Length} < 12");
                    checkpointCoveredAddress = *(long*)(ptr + 4);

                    if (hlri.cookie.Length < 52) throw new Exception($"invalid metadata length: {hlri.cookie.Length} < 52");
                    primaryReplId = Encoding.ASCII.GetString(ptr + 12, 40);
                }
                return bytesRead;
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
                            if (hlri.version == recoverTo || hlri.version < recoverTo && hlri.nextVersion > recoverTo) goto LoopEnd;
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