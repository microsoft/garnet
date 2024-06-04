﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Recovery info for hybrid log
    /// </summary>
    public struct HybridLogRecoveryInfo
    {
        const int CheckpointVersion = 5;

        /// <summary>
        /// Guid
        /// </summary>
        public Guid guid;
        /// <summary>
        /// Use snapshot file
        /// </summary>
        public int useSnapshotFile;
        /// <summary>
        /// Version
        /// </summary>
        public long version;
        /// <summary>
        /// Next Version
        /// </summary>
        public long nextVersion;
        /// <summary>
        /// Flushed logical address; indicates the latest immutable address on the main Tsavorite log at checkpoint commit time.
        /// </summary>
        public long flushedLogicalAddress;
        /// <summary>
        /// Flushed logical address at snapshot start; indicates device offset for snapshot file
        /// </summary>
        public long snapshotStartFlushedLogicalAddress;
        /// <summary>
        /// Start logical address
        /// </summary>
        public long startLogicalAddress;
        /// <summary>
        /// Final logical address
        /// </summary>
        public long finalLogicalAddress;
        /// <summary>
        /// Snapshot end logical address: snaphot is [startLogicalAddress, snapshotFinalLogicalAddress)
        /// Note that finalLogicalAddress may be higher due to delta records
        /// </summary>
        public long snapshotFinalLogicalAddress;
        /// <summary>
        /// Head address
        /// </summary>
        public long headAddress;
        /// <summary>
        /// Begin address
        /// </summary>
        public long beginAddress;

        /// <summary>
        /// If true, there was at least one ITsavoriteContext implementation active that did manual locking at some point during the checkpoint;
        /// these pages must be scanned for lock cleanup.
        /// </summary>
        public bool manualLockingActive;

        /// <summary>
        /// Object log segment offsets
        /// </summary>
        public long[] objectLogSegmentOffsets;

        /// <summary>
        /// Tail address of delta file: -1 indicates this is not a delta checkpoint metadata
        /// At recovery, this value denotes the delta tail address excluding the metadata record for the checkpoint
        /// because we create the metadata before writing to the delta file.
        /// </summary>
        public long deltaTailAddress;

        /// <summary>
        /// Initialize
        /// </summary>
        /// <param name="token"></param>
        /// <param name="_version"></param>
        public void Initialize(Guid token, long _version)
        {
            guid = token;
            useSnapshotFile = 0;
            version = _version;
            flushedLogicalAddress = 0;
            snapshotStartFlushedLogicalAddress = 0;
            startLogicalAddress = 0;
            finalLogicalAddress = 0;
            snapshotFinalLogicalAddress = 0;
            deltaTailAddress = -1; // indicates this is not a delta checkpoint metadata
            headAddress = 0;

            objectLogSegmentOffsets = null;
        }

        const int checkpointTokenCount = 0;  // Temporary to keep compatibility with previous checkpoint versions

        /// <summary>
        /// Initialize from stream
        /// </summary>
        /// <param name="reader"></param>
        public void Initialize(StreamReader reader)
        {
            string value = reader.ReadLine();
            var cversion = int.Parse(value);

            if (cversion != CheckpointVersion)
                throw new TsavoriteException($"Invalid checkpoint version {cversion} encountered, current version is {CheckpointVersion}, cannot recover with this checkpoint");

            value = reader.ReadLine();
            var checksum = long.Parse(value);

            value = reader.ReadLine();
            guid = Guid.Parse(value);

            value = reader.ReadLine();
            useSnapshotFile = int.Parse(value);

            value = reader.ReadLine();
            version = long.Parse(value);

            value = reader.ReadLine();
            nextVersion = long.Parse(value);

            value = reader.ReadLine();
            flushedLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            snapshotStartFlushedLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            startLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            finalLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            snapshotFinalLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            headAddress = long.Parse(value);

            value = reader.ReadLine();
            beginAddress = long.Parse(value);

            value = reader.ReadLine();
            deltaTailAddress = long.Parse(value);

            value = reader.ReadLine();
            manualLockingActive = bool.Parse(value);

            value = reader.ReadLine();
            var numSessions = int.Parse(value);

            // Temporary for backward compatibility
            for (int i = 0; i < numSessions; i++)
            {
                _ /*var sessionID*/ = int.Parse(reader.ReadLine());
                _ /*var sessionName*/ = reader.ReadLine();
                _ /*var serialno*/ = long.Parse(reader.ReadLine());

                var exclusionCount = int.Parse(reader.ReadLine());
                for (int j = 0; j < exclusionCount; j++)
                    _ = reader.ReadLine();
            }

            // Read object log segment offsets
            value = reader.ReadLine();
            var numSegments = int.Parse(value);
            if (numSegments > 0)
            {
                objectLogSegmentOffsets = new long[numSegments];
                for (int i = 0; i < numSegments; i++)
                {
                    value = reader.ReadLine();
                    objectLogSegmentOffsets[i] = long.Parse(value);
                }
            }

            if (checksum != Checksum(numSessions))
                throw new TsavoriteException("Invalid checksum for checkpoint");
        }

        /// <summary>
        ///  Recover info from token
        /// </summary>
        /// <param name="token"></param>
        /// <param name="checkpointManager"></param>
        /// <param name="deltaLog"></param>
        /// <param name = "scanDelta">
        /// whether to scan the delta log to obtain the latest info contained in an incremental snapshot checkpoint.
        /// If false, this will recover the base snapshot info but avoid potentially expensive scans.
        /// </param>
        /// <param name="recoverTo"> specific version to recover to, if using delta log</param>
        internal void Recover(Guid token, ICheckpointManager checkpointManager, DeltaLog deltaLog = null, bool scanDelta = false, long recoverTo = -1)
        {
            var metadata = checkpointManager.GetLogCheckpointMetadata(token, deltaLog, scanDelta, recoverTo);
            if (metadata == null)
                throw new TsavoriteException("Invalid log commit metadata for ID " + token.ToString());
            using StreamReader s = new(new MemoryStream(metadata));
            Initialize(s);
        }

        /// <summary>
        ///  Recover info from token
        /// </summary>
        /// <param name="token"></param>
        /// <param name="checkpointManager"></param>
        /// <param name="deltaLog"></param>
        /// <param name="commitCookie"> Any user-specified commit cookie written as part of the checkpoint </param>
        /// <param name = "scanDelta">
        /// whether to scan the delta log to obtain the latest info contained in an incremental snapshot checkpoint.
        /// If false, this will recover the base snapshot info but avoid potentially expensive scans.
        /// </param>
        /// <param name="recoverTo"> specific version to recover to, if using delta log</param>

        internal void Recover(Guid token, ICheckpointManager checkpointManager, out byte[] commitCookie, DeltaLog deltaLog = null, bool scanDelta = false, long recoverTo = -1)
        {
            var metadata = checkpointManager.GetLogCheckpointMetadata(token, deltaLog, scanDelta, recoverTo);
            if (metadata == null)
                throw new TsavoriteException("Invalid log commit metadata for ID " + token.ToString());
            using StreamReader s = new(new MemoryStream(metadata));
            Initialize(s);
            if (scanDelta && deltaLog != null && deltaTailAddress >= 0)
            {
                // Adjust delta tail address to include the metadata record
                deltaTailAddress = deltaLog.NextAddress;
            }
            var cookie = s.ReadToEnd();
            commitCookie = cookie.Length == 0 ? null : Convert.FromBase64String(cookie);
        }

        /// <summary>
        /// Write info to byte array
        /// </summary>
        public byte[] ToByteArray()
        {
            using (MemoryStream ms = new())
            {
                using (StreamWriter writer = new(ms))
                {
                    writer.WriteLine(CheckpointVersion); // checkpoint version

                    writer.WriteLine(Checksum(checkpointTokenCount)); // checksum

                    writer.WriteLine(guid);
                    writer.WriteLine(useSnapshotFile);
                    writer.WriteLine(version);
                    writer.WriteLine(nextVersion);
                    writer.WriteLine(flushedLogicalAddress);
                    writer.WriteLine(snapshotStartFlushedLogicalAddress);
                    writer.WriteLine(startLogicalAddress);
                    writer.WriteLine(finalLogicalAddress);
                    writer.WriteLine(snapshotFinalLogicalAddress);
                    writer.WriteLine(headAddress);
                    writer.WriteLine(beginAddress);
                    writer.WriteLine(deltaTailAddress);
                    writer.WriteLine(manualLockingActive);

                    writer.WriteLine(checkpointTokenCount);

                    // Write object log segment offsets
                    writer.WriteLine(objectLogSegmentOffsets == null ? 0 : objectLogSegmentOffsets.Length);
                    if (objectLogSegmentOffsets != null)
                    {
                        for (int i = 0; i < objectLogSegmentOffsets.Length; i++)
                        {
                            writer.WriteLine(objectLogSegmentOffsets[i]);
                        }
                    }
                }
                return ms.ToArray();
            }
        }

        private readonly long Checksum(int checkpointTokensCount)
        {
            var bytes = guid.ToByteArray();
            var long1 = BitConverter.ToInt64(bytes, 0);
            var long2 = BitConverter.ToInt64(bytes, 8);
            return long1 ^ long2 ^ version ^ flushedLogicalAddress ^ snapshotStartFlushedLogicalAddress ^ startLogicalAddress ^ finalLogicalAddress ^ snapshotFinalLogicalAddress ^ headAddress ^ beginAddress
                ^ checkpointTokensCount ^ (objectLogSegmentOffsets == null ? 0 : objectLogSegmentOffsets.Length);
        }

        /// <summary>
        /// Print checkpoint info for debugging purposes
        /// </summary>
        public readonly void DebugPrint(ILogger logger)
        {
            logger?.LogInformation("******** HybridLog Checkpoint Info for {guid} ********", guid);
            logger?.LogInformation("Version: {version}", version);
            logger?.LogInformation("Next Version: {nextVersion}", nextVersion);
            logger?.LogInformation("Is Snapshot?: {useSnapshotFile}", useSnapshotFile == 1);
            logger?.LogInformation("Flushed LogicalAddress: {flushedLogicalAddress}", flushedLogicalAddress);
            logger?.LogInformation("SnapshotStart Flushed LogicalAddress: {snapshotStartFlushedLogicalAddress}", snapshotStartFlushedLogicalAddress);
            logger?.LogInformation("Start Logical Address: {startLogicalAddress}", startLogicalAddress);
            logger?.LogInformation("Final Logical Address: {finalLogicalAddress}", finalLogicalAddress);
            logger?.LogInformation("Snapshot Final Logical Address: {snapshotFinalLogicalAddress}", snapshotFinalLogicalAddress);
            logger?.LogInformation("Head Address: {headAddress}", headAddress);
            logger?.LogInformation("Begin Address: {beginAddress}", beginAddress);
            logger?.LogInformation("Delta Tail Address: {deltaTailAddress}", deltaTailAddress);
            logger?.LogInformation("Manual Locking Active: {manualLockingActive}", manualLockingActive);
        }
    }

    internal struct HybridLogCheckpointInfo : IDisposable
    {
        public HybridLogRecoveryInfo info;
        public IDevice snapshotFileDevice;
        public IDevice snapshotFileObjectLogDevice;
        public IDevice deltaFileDevice;
        public DeltaLog deltaLog;
        public SemaphoreSlim flushedSemaphore;
        public long prevVersion;

        public void Initialize(Guid token, long _version, ICheckpointManager checkpointManager)
        {
            info.Initialize(token, _version);
            checkpointManager.InitializeLogCheckpoint(token);
        }

        public void Dispose()
        {
            snapshotFileDevice?.Dispose();
            snapshotFileObjectLogDevice?.Dispose();
            deltaLog?.Dispose();
            deltaFileDevice?.Dispose();
            this = default;
        }

        public HybridLogCheckpointInfo Transfer()
        {
            // Ownership transfer of handles across struct copies
            var dest = this;
            dest.snapshotFileDevice = default;
            dest.snapshotFileObjectLogDevice = default;
            deltaLog = default;
            deltaFileDevice = default;
            return dest;
        }

        public void Recover(Guid token, ICheckpointManager checkpointManager, int deltaLogPageSizeBits,
            bool scanDelta = false, long recoverTo = -1)
        {
            deltaFileDevice = checkpointManager.GetDeltaLogDevice(token);
            if (deltaFileDevice is not null)
            {
                deltaFileDevice.Initialize(-1);
                if (deltaFileDevice.GetFileSize(0) > 0)
                {
                    deltaLog = new DeltaLog(deltaFileDevice, deltaLogPageSizeBits, -1);
                    deltaLog.InitializeForReads();
                    info.Recover(token, checkpointManager, deltaLog, scanDelta, recoverTo);
                    return;
                }
            }
            info.Recover(token, checkpointManager, null);
        }

        public void Recover(Guid token, ICheckpointManager checkpointManager, int deltaLogPageSizeBits,
            out byte[] commitCookie, bool scanDelta = false, long recoverTo = -1)
        {
            deltaFileDevice = checkpointManager.GetDeltaLogDevice(token);
            if (deltaFileDevice is not null)
            {
                deltaFileDevice.Initialize(-1);
                if (deltaFileDevice.GetFileSize(0) > 0)
                {
                    deltaLog = new DeltaLog(deltaFileDevice, deltaLogPageSizeBits, -1);
                    deltaLog.InitializeForReads();
                    info.Recover(token, checkpointManager, out commitCookie, deltaLog, scanDelta, recoverTo);
                    return;
                }
            }
            info.Recover(token, checkpointManager, out commitCookie);
        }

        public bool IsDefault()
        {
            return info.guid == default;
        }
    }

    internal struct IndexRecoveryInfo
    {
        const int CheckpointVersion = 1;
        public Guid token;
        public long table_size;
        public ulong num_ht_bytes;
        public ulong num_ofb_bytes;
        public int num_buckets;
        public long startLogicalAddress;
        public long finalLogicalAddress;

        public void Initialize(Guid token, long _size)
        {
            this.token = token;
            table_size = _size;
            num_ht_bytes = 0;
            num_ofb_bytes = 0;
            startLogicalAddress = 0;
            finalLogicalAddress = 0;
            num_buckets = 0;
        }

        public void Initialize(StreamReader reader)
        {
            string value = reader.ReadLine();
            var cversion = int.Parse(value);

            value = reader.ReadLine();
            var checksum = long.Parse(value);

            value = reader.ReadLine();
            token = Guid.Parse(value);

            value = reader.ReadLine();
            table_size = long.Parse(value);

            value = reader.ReadLine();
            num_ht_bytes = ulong.Parse(value);

            value = reader.ReadLine();
            num_ofb_bytes = ulong.Parse(value);

            value = reader.ReadLine();
            num_buckets = int.Parse(value);

            value = reader.ReadLine();
            startLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            finalLogicalAddress = long.Parse(value);

            if (cversion != CheckpointVersion)
                throw new TsavoriteException("Invalid version");

            if (checksum != Checksum())
                throw new TsavoriteException("Invalid checksum for checkpoint");
        }

        public void Recover(Guid guid, ICheckpointManager checkpointManager)
        {
            token = guid;
            var metadata = checkpointManager.GetIndexCheckpointMetadata(guid);
            if (metadata == null)
                throw new TsavoriteException("Invalid index commit metadata for ID " + guid.ToString());
            using (StreamReader s = new(new MemoryStream(metadata)))
                Initialize(s);
        }

        public readonly byte[] ToByteArray()
        {
            using (MemoryStream ms = new())
            {
                using (StreamWriter writer = new(ms))
                {
                    writer.WriteLine(CheckpointVersion); // checkpoint version
                    writer.WriteLine(Checksum()); // checksum

                    writer.WriteLine(token);
                    writer.WriteLine(table_size);
                    writer.WriteLine(num_ht_bytes);
                    writer.WriteLine(num_ofb_bytes);
                    writer.WriteLine(num_buckets);
                    writer.WriteLine(startLogicalAddress);
                    writer.WriteLine(finalLogicalAddress);
                }
                return ms.ToArray();
            }
        }

        private readonly long Checksum()
        {
            var bytes = token.ToByteArray();
            var long1 = BitConverter.ToInt64(bytes, 0);
            var long2 = BitConverter.ToInt64(bytes, 8);
            return long1 ^ long2 ^ table_size ^ (long)num_ht_bytes ^ (long)num_ofb_bytes
                        ^ num_buckets ^ startLogicalAddress ^ finalLogicalAddress;
        }

        public readonly void DebugPrint(ILogger logger)
        {
            logger?.LogInformation("******** Index Checkpoint Info for {token} ********", token);
            logger?.LogInformation("Table Size: {table_size}", table_size);
            logger?.LogInformation("Main Table Size (in GB): {num_ht_bytes}", ((double)num_ht_bytes) / 1000.0 / 1000.0 / 1000.0);
            logger?.LogInformation("Overflow Table Size (in GB): {num_ofb_bytes}", ((double)num_ofb_bytes) / 1000.0 / 1000.0 / 1000.0);
            logger?.LogInformation("Num Buckets: {num_buckets}", num_buckets);
            logger?.LogInformation("Start Logical Address: {startLogicalAddress}", startLogicalAddress);
            logger?.LogInformation("Final Logical Address: {finalLogicalAddress}", finalLogicalAddress);
        }

        public void Reset()
        {
            token = default;
            table_size = 0;
            num_ht_bytes = 0;
            num_ofb_bytes = 0;
            num_buckets = 0;
            startLogicalAddress = 0;
            finalLogicalAddress = 0;
        }
    }

    internal struct IndexCheckpointInfo
    {
        public IndexRecoveryInfo info;
        public IDevice main_ht_device;

        public void Initialize(Guid token, long _size, ICheckpointManager checkpointManager)
        {
            info.Initialize(token, _size);
            checkpointManager.InitializeIndexCheckpoint(token);
            main_ht_device = checkpointManager.GetIndexDevice(token);
        }

        public void Recover(Guid token, ICheckpointManager checkpointManager)
        {
            info.Recover(token, checkpointManager);
        }

        public void Reset()
        {
            info = default;
            main_ht_device?.Dispose();
            main_ht_device = null;
        }

        public bool IsDefault()
        {
            return info.token == default;
        }
    }
}