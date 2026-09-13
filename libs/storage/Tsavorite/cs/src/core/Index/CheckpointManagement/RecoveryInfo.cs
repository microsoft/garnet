// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Recovery info for hybrid log
    /// </summary>
    public struct HybridLogRecoveryInfo
    {
        /// <summary>Current checkpoint version written by this build. v9 carries the hybrid-log <see cref="pageSize"/> and
        /// <see cref="segmentSize"/> in metadata; v8 is the object-log chunk-framing format ("v2.2") whose fifth address slot
        /// held a duplicate of <see cref="recoveredTailAddress"/>; v7 is the
        /// downlevel split/objectId-slot object-log encoding ("v2.1", read via <see cref="LogRecord.GetObjectLogRecordStartPositionAndLengths_v21"/>).</summary>
        public const int CheckpointVersion = 9;

        /// <summary>First version whose metadata carries <see cref="pageSize"/>/<see cref="segmentSize"/> instead of the
        /// legacy duplicated snapshot-end address.</summary>
        internal const int LogGeometryCheckpointVersion = 9;

        /// <summary>Oldest checkpoint version this build can recover. Version 7 checkpoints remain readable.</summary>
        public const int MinRecoverableCheckpointVersion = 7;

        /// <summary>
        /// HybridLogRecoveryVersion 
        /// </summary>
        public int hybridLogRecoveryVersion;
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
        /// The next version of the database when the checkpoint flush was started
        /// </summary>
        public long nextVersion;
        /// <summary>
        /// Exclusive end of the main-log recovery range. For Snapshot recovery, the Snapshot overlay begins here.
        /// </summary>
        public long mainLogRecoveryEndAddress;
        /// <summary>
        /// Logical base used to map snapshot-file pages to hybrid-log addresses.
        /// </summary>
        public long snapshotFileLogicalStartAddress;
        /// <summary>
        /// Start of the version-fuzzy region used to reject or undo next-version records during recovery.
        /// </summary>
        public long fuzzyRegionStartAddress;
        /// <summary>
        /// TailAddress established after recovery; also the exclusive logical end represented by a Snapshot file.
        /// </summary>
        public long recoveredTailAddress;
        /// <summary>
        /// Hybrid-log page size, in bytes, of the store that wrote this checkpoint. Zero for downlevel (pre-v9) checkpoints,
        /// whose metadata carried a duplicate of <see cref="recoveredTailAddress"/> in this slot instead.
        /// </summary>
        public long pageSize;
        /// <summary>
        /// hlog HeadAddress at the start of the WAIT_FLUSH phase. This is the initial address to start scanning from; the lowest address at which we will bring pages
        /// into the circular buffer (may be in the middle of a page)
        /// </summary>
        public long headAddress;
        /// <summary>
        /// hlog BeginAddress at the start of the PREPARE phase
        /// </summary>
        public long beginAddress;

        /// <summary>
        /// The objectLog segment for hlog's BeginAddress (<see cref="ObjectAllocatorImpl{TStoreFunctions}.lowestObjectLogSegmentInUse"/>) at PREPARE;
        /// corresponds to <see cref="beginAddress"/>. Will be zero unless the log has been truncated.
        /// </summary>
        internal int beginAddressObjectLogSegment;

        /// <summary>
        /// The <see cref="ObjectAllocatorImpl{TStoreFunctions>.objectLogTail"/> taken at PERSISTENCE_CALLBACK (matching <see cref="mainLogRecoveryEndAddress"/>).
        /// This is incremented for any flushes due to ReadOnlyAddress growth during the snapshot.
        /// </summary>
        internal ObjectLogFilePositionInfo hlogEndObjectLogTail;

        /// <summary>
        /// The <see cref="ObjectAllocatorImpl{TStoreFunctions>.objectLogTail"/> at the start of the checkpoint (start of WAIT_FLUSH).
        /// </summary>
        internal ObjectLogFilePositionInfo snapshotStartObjectLogTail;

        /// <summary>
        /// The <see cref="ObjectAllocatorImpl{TStoreFunctions>.objectLogTail"/> at the end of the checkpoint (at PERSISTENCE_CALLBACK).
        /// </summary>
        internal ObjectLogFilePositionInfo snapshotEndObjectLogTail;

        /// <summary>
        /// User cookie
        /// </summary>
        public byte[] cookie;

        /// <summary>
        /// Hybrid-log segment size, in bytes, of the store that wrote this checkpoint. Appended in v9; zero for downlevel
        /// (pre-v9) checkpoints, whose metadata did not record it.
        /// </summary>
        public long segmentSize;

        /// <summary>
        /// If struct deserialized succesfully
        /// </summary>
        public bool Deserialized { get; private set; }

        /// <summary>
        /// Initialize
        /// </summary>
        /// <param name="token"></param>
        /// <param name="_version"></param>
        public void Initialize(Guid token, long _version)
        {
            Deserialized = false;
            guid = token;
            useSnapshotFile = 0;
            version = _version;
            mainLogRecoveryEndAddress = 0;
            snapshotFileLogicalStartAddress = 0;
            fuzzyRegionStartAddress = 0;
            recoveredTailAddress = 0;
            pageSize = 0;
            segmentSize = 0;
            headAddress = 0;

            hlogEndObjectLogTail = new();       // Marks as "unset"
            snapshotStartObjectLogTail = new();
            snapshotEndObjectLogTail = new();
        }

        /// <summary>
        /// Initialize from stream
        /// </summary>
        /// <param name="reader"></param>
        public void Initialize(StreamReader reader)
        {
            var value = reader.ReadLine();
            var cversion = int.Parse(value);

            if (cversion < MinRecoverableCheckpointVersion || cversion > CheckpointVersion)
                throw new TsavoriteException($"Invalid checkpoint version {cversion} encountered, this build recovers versions {MinRecoverableCheckpointVersion}..{CheckpointVersion}, cannot recover with this checkpoint");

            hybridLogRecoveryVersion = cversion;

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
            mainLogRecoveryEndAddress = long.Parse(value);

            value = reader.ReadLine();
            snapshotFileLogicalStartAddress = long.Parse(value);

            value = reader.ReadLine();
            fuzzyRegionStartAddress = long.Parse(value);

            value = reader.ReadLine();
            recoveredTailAddress = long.Parse(value);

            // Fifth address slot. Before v9 this held a duplicate of recoveredTailAddress; v9 repurposed it as pageSize.
            // Retain the raw value so a downlevel checksum validates against exactly the bytes that were written.
            value = reader.ReadLine();
            var addressSlotValue = long.Parse(value);
            pageSize = cversion >= LogGeometryCheckpointVersion ? addressSlotValue : 0;

            value = reader.ReadLine();
            headAddress = long.Parse(value);

            value = reader.ReadLine();
            beginAddress = long.Parse(value);

            value = reader.ReadLine();
            beginAddressObjectLogSegment = int.Parse(value);

            hlogEndObjectLogTail.Deserialize(reader);
            snapshotStartObjectLogTail.Deserialize(reader);
            snapshotEndObjectLogTail.Deserialize(reader);

            // Appended in v9; downlevel metadata ends the scalar fields at the object-log tails.
            segmentSize = 0;
            if (cversion >= LogGeometryCheckpointVersion)
            {
                value = reader.ReadLine();
                segmentSize = long.Parse(value);
            }

            // Read user cookie
            value = reader.ReadLine();
            var cookieSize = int.Parse(value);
            if (cookieSize > 0)
            {
                cookie = new byte[cookieSize];
                for (var i = 0; i < cookieSize; i++)
                {
                    value = reader.ReadLine();
                    cookie[i] = byte.Parse(value);
                }
            }

            if (checksum != ChecksumCore(addressSlotValue, segmentSize))
                throw new TsavoriteException("Invalid checksum for checkpoint");

            Deserialized = true;
        }

        /// <summary>
        ///  Recover info from token
        /// </summary>
        /// <param name="token"></param>
        /// <param name="checkpointManager"></param>
        internal void Recover(Guid token, ICheckpointManager checkpointManager)
        {
            var metadata = checkpointManager.GetLogCheckpointMetadata(token)
                ?? throw new TsavoriteException("Invalid log commit metadata for ID " + token.ToString());
            using StreamReader s = new(new MemoryStream(metadata));
            Initialize(s);
        }

        /// <summary>
        ///  Recover info from token
        /// </summary>
        /// <param name="token"></param>
        /// <param name="checkpointManager"></param>
        /// <param name="commitCookie"> Any user-specified commit cookie written as part of the checkpoint </param>
        internal void Recover(Guid token, ICheckpointManager checkpointManager, out byte[] commitCookie)
        {
            var metadata = checkpointManager.GetLogCheckpointMetadata(token)
                ?? throw new TsavoriteException("Invalid log commit metadata for ID " + token.ToString());
            using StreamReader s = new(new MemoryStream(metadata));
            Initialize(s);
            commitCookie = cookie;
        }

        /// <summary>
        /// Write info to byte array in the current checkpoint format.
        /// </summary>
        public readonly byte[] ToByteArray() => ToByteArray(CheckpointVersion);

        /// <summary>
        /// Write info to byte array in the layout of <paramref name="targetVersion"/>. Only the current version is written by
        /// production code; downlevel targets exist so compatibility tests can generate real historical metadata through this
        /// serializer rather than relabeling current bytes with an older version number.
        /// </summary>
        internal readonly byte[] ToByteArray(int targetVersion)
        {
            if (targetVersion < MinRecoverableCheckpointVersion || targetVersion > CheckpointVersion)
                throw new TsavoriteException($"Cannot serialize checkpoint metadata as version {targetVersion}; this build writes versions {MinRecoverableCheckpointVersion}..{CheckpointVersion}");

            var writesLogGeometry = targetVersion >= LogGeometryCheckpointVersion;

            // Pre-v9 metadata duplicated recoveredTailAddress into the fifth address slot and had no trailing value.
            var addressSlotValue = writesLogGeometry ? pageSize : recoveredTailAddress;
            var trailingValue = writesLogGeometry ? segmentSize : 0;

            using (MemoryStream ms = new())
            {
                using (StreamWriter writer = new(ms))
                {
                    writer.WriteLine(targetVersion); // checkpoint version
                    writer.WriteLine(ChecksumCore(addressSlotValue, trailingValue));

                    writer.WriteLine(guid);
                    writer.WriteLine(useSnapshotFile);
                    writer.WriteLine(version);
                    writer.WriteLine(nextVersion);
                    writer.WriteLine(mainLogRecoveryEndAddress);
                    writer.WriteLine(snapshotFileLogicalStartAddress);
                    writer.WriteLine(fuzzyRegionStartAddress);
                    writer.WriteLine(recoveredTailAddress);
                    writer.WriteLine(addressSlotValue);
                    writer.WriteLine(headAddress);
                    writer.WriteLine(beginAddress);

                    writer.WriteLine(beginAddressObjectLogSegment);

                    hlogEndObjectLogTail.Serialize(writer);
                    snapshotStartObjectLogTail.Serialize(writer);
                    snapshotEndObjectLogTail.Serialize(writer);

                    if (writesLogGeometry)
                        writer.WriteLine(segmentSize);

                    // Write user cookie
                    var cookieSize = cookie == null ? 0 : cookie.Length;
                    writer.WriteLine(cookieSize);
                    if (cookieSize > 0)
                    {
                        for (var i = 0; i < cookieSize; i++)
                            writer.WriteLine(cookie[i]);
                    }
                }
                return ms.ToArray();
            }
        }

        /// <summary>
        /// Checksum over the fixed scalar fields. The fifth address slot and the appended trailing value are passed in so a
        /// downlevel checkpoint validates against the raw values it actually serialized: pre-v9 metadata wrote a duplicate
        /// of <see cref="recoveredTailAddress"/> in the slot and had no trailing value, which reduces this to the v8 formula.
        /// </summary>
        private readonly long ChecksumCore(long addressSlotValue, long trailingValue)
        {
            var bytes = guid.ToByteArray();
            var long1 = BitConverter.ToInt64(bytes, 0);
            var long2 = BitConverter.ToInt64(bytes, 8);
            return long1 ^ long2 ^ version ^ mainLogRecoveryEndAddress ^ snapshotFileLogicalStartAddress ^ fuzzyRegionStartAddress ^ recoveredTailAddress ^ addressSlotValue
                ^ headAddress ^ beginAddress ^ beginAddressObjectLogSegment ^ (long)hlogEndObjectLogTail.word ^ (long)snapshotStartObjectLogTail.word ^ (long)snapshotEndObjectLogTail.word
                ^ trailingValue;
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
            logger?.LogInformation("Main-log recovery end address: {mainLogRecoveryEndAddress}", mainLogRecoveryEndAddress);
            logger?.LogInformation("Snapshot-file logical start address: {snapshotFileLogicalStartAddress}", snapshotFileLogicalStartAddress);
            logger?.LogInformation("Fuzzy-region start address: {fuzzyRegionStartAddress}", fuzzyRegionStartAddress);
            logger?.LogInformation("Recovered tail address: {recoveredTailAddress}", recoveredTailAddress);
            logger?.LogInformation("Page Size: {pageSize}", pageSize);
            logger?.LogInformation("Segment Size: {segmentSize}", segmentSize);
            logger?.LogInformation("Head Address: {headAddress}", headAddress);
            logger?.LogInformation("Begin Address: {beginAddress}", beginAddress);
            logger?.LogInformation("Begin object log segment: {beginObjLogSegment}", beginAddressObjectLogSegment);
            logger?.LogInformation("Hybrid Log End Object Tail Position: {hlogEndObjLogTail}", hlogEndObjectLogTail);
            logger?.LogInformation("Snapshot Begin Object Log Tail Position: {snapshotStartObjLogTail}", snapshotStartObjectLogTail);
            logger?.LogInformation("Snapshot End Object Log Tail Position: {snapshotEndObjLogTail}", snapshotEndObjectLogTail);
        }
    }

    internal struct HybridLogCheckpointInfo : IDisposable
    {
        public HybridLogRecoveryInfo info;
        public IDevice snapshotFileDevice;
        public IDevice snapshotFileObjectLogDevice;
        public Task flushedTask;
        internal CircularDiskWriteBuffer objectLogFlushBuffers;
        internal SnapshotFlushCoordination snapshotFlushCoordination;

        public void Initialize(Guid token, long _version, ICheckpointManager checkpointManager)
        {
            info.Initialize(token, _version);
            checkpointManager.InitializeLogCheckpoint(token);
        }

        public void Dispose()
        {
            snapshotFileDevice?.Dispose();
            snapshotFileObjectLogDevice?.Dispose();
            objectLogFlushBuffers?.Dispose();
            snapshotFlushCoordination?.Dispose();
            this = default;
        }

        public void Recover(Guid token, ICheckpointManager checkpointManager)
        {
            info.Recover(token, checkpointManager);
        }

        public void Recover(Guid token, ICheckpointManager checkpointManager, out byte[] commitCookie)
        {
            info.Recover(token, checkpointManager, out commitCookie);
        }

        public readonly bool IsDefault => info.guid == default;
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
            var value = reader.ReadLine();
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
            var metadata = checkpointManager.GetIndexCheckpointMetadata(guid) ?? throw new TsavoriteException("Invalid index commit metadata for ID " + guid.ToString());
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
            logger?.LogInformation("Main Table Size (in GB): {num_ht_bytes}", num_ht_bytes / 1000.0 / 1000.0 / 1000.0);
            logger?.LogInformation("Overflow Table Size (in GB): {num_ofb_bytes}", num_ofb_bytes / 1000.0 / 1000.0 / 1000.0);
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

        public readonly bool IsDefault => info.token == default;
    }
}