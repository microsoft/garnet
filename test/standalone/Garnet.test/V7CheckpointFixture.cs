// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Garnet.server;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Rewrites a Garnet store's current-format checkpoint on disk so that it reads as downlevel (v7), letting tests exercise the
    /// <c>--upgrade</c> up-conversion against a checkpoint this build can no longer write.
    /// </summary>
    /// <remarks>
    /// Shared by the standalone and cluster test projects, which drive the same conversion through different server topologies.
    /// Every method takes the directory that contains the store's <c>Store</c> folder, because a cluster node's store lives under a
    /// per-port subdirectory rather than the test directory itself.
    /// </remarks>
    internal static class V7CheckpointFixture
    {
        /// <summary>The store's single main-log segment file.</summary>
        static string MainLogPath(string storeRoot) => Path.Combine(storeRoot, "Store", "hlog.0");

        static DeviceLogCommitCheckpointManager OpenCheckpointManager(string storeRoot)
        {
            var storeCheckpointDir = Path.Combine(storeRoot, "Store", GarnetServerOptions.GetCheckpointDirectoryName(0));
            return new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactoryCreator(),
                new DefaultCheckpointNamingScheme(storeCheckpointDir), removeOutdated: false);
        }

        /// <summary>
        /// Rewrite the store's current-format checkpoint on disk so it reads as downlevel (v7): re-stamp each out-of-line record into
        /// the v7 split-length encoding and re-serialize the metadata at version 7. The object-log bytes and record positions are left
        /// untouched, which is only valid while every object is headerless in the current format. Handles both checkpoint types: a
        /// FoldOver checkpoint's records are all on the main log, while a Snapshot checkpoint splits them between the main log (below
        /// mainLogRecoveryEndAddress) and the snapshot file above it.
        /// </summary>
        /// <param name="storeRoot">The directory containing the store's <c>Store</c> folder.</param>
        /// <returns>The number of records converted.</returns>
        public static unsafe int TransformCheckpointToV7(string storeRoot)
        {
            using var checkpointManager = OpenCheckpointManager(storeRoot);

            var token = checkpointManager.GetLogCheckpointTokens().Single();
            var info = new HybridLogRecoveryInfo();
            info.Recover(token, checkpointManager);
            ClassicAssert.AreEqual(HybridLogRecoveryInfo.CheckpointVersion, info.hybridLogRecoveryVersion, "expected a current-format checkpoint before transform");
            ClassicAssert.Greater(info.pageSize, 0, "current-format metadata should record the page size");
            ClassicAssert.LessOrEqual(info.recoveredTailAddress, info.segmentSize, "v7 fixture expects a single main-log segment");

            // The main log carries the whole recovered range for FoldOver, and only the region below the snapshot boundary otherwise.
            var mainLogUntil = info.useSnapshotFile == 0 ? info.recoveredTailAddress : info.mainLogRecoveryEndAddress;
            var converted = StampRegionAsV7(MainLogPath(storeRoot), addressBias: 0, info.beginAddress, mainLogUntil, info.pageSize);

            if (info.useSnapshotFile != 0)
            {
                // Snapshot-file offset 0 is the start of the page containing snapshotFileLogicalStartAddress, so bias the image by that
                // page's logical start to index it with logical addresses.
                var storeCheckpointDir = Path.Combine(storeRoot, "Store", GarnetServerOptions.GetCheckpointDirectoryName(0));
                var snapshotDir = Path.Combine(storeCheckpointDir, "cpr-checkpoints", token.ToString());
                var snapshotSegment = Directory.GetFiles(snapshotDir, "snapshot.dat.*").OrderBy(f => f, StringComparer.Ordinal).FirstOrDefault();
                ClassicAssert.IsNotNull(snapshotSegment, $"no snapshot segment in {snapshotDir}");
                var snapshotStartPageAddress = info.snapshotFileLogicalStartAddress & ~(info.pageSize - 1);
                converted += StampRegionAsV7(snapshotSegment, addressBias: snapshotStartPageAddress, info.mainLogRecoveryEndAddress, info.recoveredTailAddress, info.pageSize);
            }

            checkpointManager.CommitLogCheckpointMetadata(token, info.ToByteArray(targetVersion: 7));
            return converted;
        }

        /// <summary>Recover the metadata of the newest log checkpoint. Token enumeration is directory order, so an upgrade run that
        /// leaves the recovered checkpoint in place alongside the one it took would otherwise be read from arbitrarily.</summary>
        static HybridLogRecoveryInfo RecoverNewestCheckpoint(DeviceLogCommitCheckpointManager checkpointManager)
        {
            var newest = new HybridLogRecoveryInfo();
            var found = false;
            foreach (var token in checkpointManager.GetLogCheckpointTokens())
            {
                var info = new HybridLogRecoveryInfo();
                info.Recover(token, checkpointManager);
                if (!found || info.version > newest.version)
                {
                    newest = info;
                    found = true;
                }
            }
            ClassicAssert.IsTrue(found, "no log checkpoint to inspect");
            return newest;
        }

        /// <summary>Count out-of-line main-log records still carrying the downlevel ReuseObjectIdForSize flag (bit 63).</summary>
        /// <param name="storeRoot">The directory containing the store's <c>Store</c> folder.</param>
        public static unsafe int CountDownlevelRecordsOnMainLog(string storeRoot)
        {
            using var checkpointManager = OpenCheckpointManager(storeRoot);

            var info = RecoverNewestCheckpoint(checkpointManager);

            var pageBytes = File.ReadAllBytes(MainLogPath(storeRoot));

            // A Snapshot checkpoint keeps the region above mainLogRecoveryEndAddress in the snapshot file, not on the main log, so
            // scanning to recoveredTailAddress would walk past the end of the image.
            var mainLogUntil = info.useSnapshotFile == 0 ? info.recoveredTailAddress : info.mainLogRecoveryEndAddress;
            ClassicAssert.LessOrEqual(mainLogUntil, pageBytes.Length, $"checkpoint describes main-log data beyond the end of {MainLogPath(storeRoot)}");

            var downlevel = 0;
            fixed (byte* pagePtr = pageBytes)
            {
                var pageBase = (long)pagePtr;
                foreach (var offset in GetOnDiskRecordOffsets(pageBase, info.beginAddress, mainLogUntil, info.pageSize))
                {
                    var logRecord = new LogRecord(pageBase + offset);
                    if (!logRecord.Info.Valid || logRecord.DataHeader.RecordIsInline)
                        continue;
                    var word = *(ulong*)logRecord.GetObjectLogPositionAddress(logRecord.GetOptionalStartAddress());
                    if ((word & ObjectLogFilePositionInfo.kReuseObjectIdForSizeMask) != 0)
                        ++downlevel;
                }
            }
            return downlevel;
        }

        /// <summary>Re-stamp every out-of-line record of one on-disk log image in [fromAddress, untilAddress) into the v7 encoding.</summary>
        /// <param name="path">The log segment file.</param>
        /// <param name="addressBias">The logical address that maps to offset 0 of this file.</param>
        /// <param name="fromAddress">The lowest logical address to convert.</param>
        /// <param name="untilAddress">The exclusive highest logical address to convert.</param>
        /// <param name="pageSize">The log's page size.</param>
        static unsafe int StampRegionAsV7(string path, long addressBias, long fromAddress, long untilAddress, long pageSize)
        {
            if (fromAddress >= untilAddress)
                return 0;
            ClassicAssert.IsTrue(File.Exists(path), $"no log segment at {path}");

            var imageBytes = File.ReadAllBytes(path);
            ClassicAssert.LessOrEqual(untilAddress - addressBias, imageBytes.Length, $"checkpoint describes data beyond the end of {path}");
            var converted = 0;
            fixed (byte* imagePtr = imageBytes)
            {
                var pageBase = (long)imagePtr - addressBias;
                foreach (var offset in GetOnDiskRecordOffsets(pageBase, fromAddress, untilAddress, pageSize))
                {
                    var logRecord = new LogRecord(pageBase + offset);
                    if (!logRecord.Info.Valid || logRecord.DataHeader.RecordIsInline)
                        continue;
                    StampRecordAsV7(pageBase + offset);
                    ++converted;
                }
            }
            File.WriteAllBytes(path, imageBytes);
            return converted;
        }

        /// <summary>
        /// Convert one record from the current objectId-hint encoding to the v7 split-length encoding. The object-log bytes and the
        /// record's position are left alone, so this is only valid when the current encoding is headerless.
        /// </summary>
        static unsafe void StampRecordAsV7(long physicalAddress)
        {
            var logRecord = new LogRecord(physicalAddress);
            var dataHeader = logRecord.DataHeader;

            // Field addresses use the physical (objectId-slot) field lengths, which are independent of the raw length bits, so they are
            // stable across the stamp. Read the current exact lengths and the position word BEFORE mutating the length fields.
            var (_, keyAddress) = dataHeader.GetKeyFieldInfo(physicalAddress);
            var (_, valueAddress) = dataHeader.GetValueFieldInfo(physicalAddress);
            var objectLogPositionPtr = (ulong*)logRecord.GetObjectLogPositionAddress(logRecord.GetOptionalStartAddress());
            var positionWord = *objectLogPositionPtr;
            _ = logRecord.GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength, HybridLogRecoveryInfo.CheckpointVersion);

            // A component the current format frames with a leading ChunkHeader is NOT byte-identical to v7 dense, so reusing its bytes
            // would silently misframe the fixture rather than exercise the up-conversion. The exact-size flag is precisely the
            // "headerless, and the extent is the true length" condition this reuse depends on.
            ClassicAssert.AreNotEqual(0UL, positionWord & ObjectLogFilePositionInfo.kValueIsExactSizeMask,
                "fixture value is not stored exact-size/headerless, so its current bytes are not v7 dense");
            ClassicAssert.LessOrEqual((long)valueLength, (long)RecordDataHeader.kOutOfLineExactSizeCutoff, "fixture value is too large to reuse as v7 dense bytes");

            if (dataHeader.KeyIsOverflow)
            {
                *(int*)keyAddress = (int)((uint)keyLength >> RecordDataHeader.kKeyLengthBits);
                dataHeader.KeyLength = keyLength & (int)RecordDataHeader.kKeyLengthLowBitsMask;
            }
            if (!dataHeader.ValueIsInline)
            {
                *(int*)valueAddress = (int)(valueLength >> RecordDataHeader.kValueLengthBits);
                dataHeader.ValueLength = (int)(valueLength & RecordDataHeader.kValueLengthLowBitsMask);
            }
            logRecord.SetDataHeader(dataHeader);

            // Keep the segment+offset; set the ReuseObjectIdForSize flag (bit 63); clear the current size-hint flags.
            *objectLogPositionPtr = (positionWord & ObjectLogFilePositionInfo.SegmentAndOffsetMask) | ObjectLogFilePositionInfo.kReuseObjectIdForSizeMask;
        }

        /// <summary>
        /// Offsets of the on-disk main-log records in a pinned segment-0 image, skipping each page's PageHeader and the end-of-page
        /// filler. Logical address equals file offset on segment 0.
        /// </summary>
        static unsafe List<long> GetOnDiskRecordOffsets(long pageBase, long beginAddress, long tailAddress, long pageSize)
        {
            List<long> offsets = [];
            for (var pageStart = beginAddress & ~(pageSize - 1); pageStart < tailAddress; pageStart += pageSize)
            {
                var offset = Math.Max(beginAddress, pageStart + PageHeader.Size);
                var pageEnd = Math.Min(tailAddress, pageStart + pageSize);
                while (offset < pageEnd)
                {
                    var logRecord = new LogRecord(pageBase + offset);
                    if (logRecord.Info.IsNull)
                    {
                        offset += RecordInfo.Size;
                        continue;
                    }
                    offsets.Add(offset);

                    // A zero size cannot advance the scan, so report where the image stopped parsing instead of looping forever.
                    var allocatedSize = logRecord.AllocatedSize;
                    ClassicAssert.Greater(allocatedSize, 0, $"log image does not parse as records at address 0x{offset:x} (page 0x{pageStart:x})");
                    offset += allocatedSize;
                }
            }
            return offsets;
        }
    }
}