// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.recovery
{
    /// <summary>
    /// Checkpoint-metadata format compatibility.
    ///
    /// v8 repurposes the fifth address slot -- which in v7 held a duplicate of <c>recoveredTailAddress</c> -- as the
    /// hybrid-log <c>pageSize</c>, records the object log as three flat addresses where v7 recorded a segment and three
    /// segment-split positions, and appends <c>segmentSize</c>, <c>objectLogSegmentSize</c>, and <c>objectLogPageSize</c> after
    /// them. These tests round-trip real metadata produced by the same serializer at each supported version, so a downlevel
    /// checkpoint stays readable and its checksum keeps validating against exactly the bytes that were written.
    /// </summary>
    [TestFixture]
    public class CheckpointMetadataVersionTests
    {
        // Index (0-based) of the fifth address slot among the text lines: version, checksum, guid, useSnapshotFile,
        // version, nextVersion, mainLogRecoveryEnd, snapshotFileLogicalStart, fuzzyRegionStart, recoveredTail, <slot>.
        const int AddressSlotLineIndex = 10;

        private static HybridLogRecoveryInfo MakeInfo()
        {
            var info = default(HybridLogRecoveryInfo);
            info.Initialize(Guid.NewGuid(), _version: 3);
            info.useSnapshotFile = 1;
            info.nextVersion = 4;
            info.mainLogRecoveryEndAddress = 0x1_0000;
            info.snapshotFileLogicalStartAddress = 0x8000;
            info.fuzzyRegionStartAddress = 0xC000;
            info.recoveredTailAddress = 0x2_4680;
            info.headAddress = 0x4000;
            info.beginAddress = 0x1000;
            info.pageSize = 1 << 22;
            info.segmentSize = 1L << 30;
            info.objectLogSegmentSize = 1L << 26;
            info.objectLogPageSize = 1L << 23;
            info.objectLogBeginAddress = 3L << 26;
            info.hlogEndObjectLogTail = (3L << 26) + 0x1_2340;
            info.snapshotEndObjectLogTail = 0x9_8760;
            info.legacyObjectLogSegmentSizeBits = 26;
            info.legacySnapshotStartObjectLogTail = (3L << 26) + 0x1000;
            return info;
        }

        private static HybridLogRecoveryInfo RoundTrip(byte[] bytes)
        {
            var read = default(HybridLogRecoveryInfo);
            using var reader = new StreamReader(new MemoryStream(bytes));
            read.Initialize(reader);
            return read;
        }

        private static string[] Lines(byte[] bytes)
            // The metadata body is plain text, one value per line.
            => Encoding.UTF8.GetString(bytes).Split('\n').Select(l => l.TrimEnd('\r')).ToArray();

        [Test]
        [Category("CheckpointRestore")]
        public void CurrentVersionRoundTripsLogGeometry()
        {
            var info = MakeInfo();
            var read = RoundTrip(info.ToByteArray());

            Assert.Multiple(() =>
            {
                Assert.That(read.hybridLogRecoveryVersion, Is.EqualTo(HybridLogRecoveryInfo.CheckpointVersion));
                Assert.That(read.pageSize, Is.EqualTo(info.pageSize));
                Assert.That(read.segmentSize, Is.EqualTo(info.segmentSize));
                Assert.That(read.objectLogSegmentSize, Is.EqualTo(info.objectLogSegmentSize));
                Assert.That(read.objectLogPageSize, Is.EqualTo(info.objectLogPageSize));
                Assert.That(read.objectLogBeginAddress, Is.EqualTo(info.objectLogBeginAddress));
                Assert.That(read.hlogEndObjectLogTail, Is.EqualTo(info.hlogEndObjectLogTail));
                Assert.That(read.snapshotEndObjectLogTail, Is.EqualTo(info.snapshotEndObjectLogTail));
                Assert.That(read.recoveredTailAddress, Is.EqualTo(info.recoveredTailAddress));
                Assert.That(read.mainLogRecoveryEndAddress, Is.EqualTo(info.mainLogRecoveryEndAddress));
                Assert.That(read.snapshotFileLogicalStartAddress, Is.EqualTo(info.snapshotFileLogicalStartAddress));
                Assert.That(read.beginAddress, Is.EqualTo(info.beginAddress));
            });
        }

        [Test]
        [Category("CheckpointRestore")]
        public void DownlevelVersionConvertsObjectLogPositionsToAddresses()
        {
            // v7 records the object log's lowest segment in use and three segment-split position words; reading one yields flat addresses.
            var info = MakeInfo();
            var read = RoundTrip(info.ToByteArray(7));

            Assert.Multiple(() =>
            {
                Assert.That(read.legacyObjectLogSegmentSizeBits, Is.EqualTo(info.legacyObjectLogSegmentSizeBits));
                Assert.That(read.objectLogBeginAddress, Is.EqualTo(info.objectLogBeginAddress));
                Assert.That(read.hlogEndObjectLogTail, Is.EqualTo(info.hlogEndObjectLogTail));
                Assert.That(read.legacySnapshotStartObjectLogTail, Is.EqualTo(info.legacySnapshotStartObjectLogTail));
                Assert.That(read.snapshotEndObjectLogTail, Is.EqualTo(info.snapshotEndObjectLogTail));
                Assert.That(read.objectLogPageSize, Is.Zero);
            });
        }

        [Test]
        [Category("CheckpointRestore")]
        public void CurrentVersionWritesPageSizeInAddressSlot()
        {
            var info = MakeInfo();
            var lines = Lines(info.ToByteArray());

            Assert.Multiple(() =>
            {
                Assert.That(lines[0].Trim(), Is.EqualTo(HybridLogRecoveryInfo.CheckpointVersion.ToString()));
                Assert.That(lines[AddressSlotLineIndex - 1].Trim(), Is.EqualTo(info.recoveredTailAddress.ToString()));
                Assert.That(lines[AddressSlotLineIndex].Trim(), Is.EqualTo(info.pageSize.ToString()),
                    "v8 must write pageSize into the slot that v7 used to duplicate recoveredTailAddress");
            });
        }

        [Test]
        [Category("CheckpointRestore")]
        public void DownlevelVersionDuplicatesTailAddressInAddressSlot(
            [Values(7)] int targetVersion)
        {
            var info = MakeInfo();
            var lines = Lines(info.ToByteArray(targetVersion));

            Assert.Multiple(() =>
            {
                Assert.That(lines[0].Trim(), Is.EqualTo(targetVersion.ToString()));
                Assert.That(lines[AddressSlotLineIndex].Trim(), Is.EqualTo(info.recoveredTailAddress.ToString()),
                    "v7 metadata must duplicate recoveredTailAddress in the address slot, not write pageSize");
            });
        }

        [Test]
        [Category("CheckpointRestore")]
        public void DownlevelVersionRecoversWithoutLogGeometry(
            [Values(7)] int targetVersion)
        {
            var info = MakeInfo();
            var read = RoundTrip(info.ToByteArray(targetVersion));

            Assert.Multiple(() =>
            {
                Assert.That(read.hybridLogRecoveryVersion, Is.EqualTo(targetVersion));

                // The geometry is simply unknown for a downlevel checkpoint; it must normalize to zero rather than
                // inheriting the legacy address that occupied the slot.
                Assert.That(read.pageSize, Is.Zero);
                Assert.That(read.segmentSize, Is.Zero);
                Assert.That(read.objectLogSegmentSize, Is.Zero);

                Assert.That(read.recoveredTailAddress, Is.EqualTo(info.recoveredTailAddress));
                Assert.That(read.mainLogRecoveryEndAddress, Is.EqualTo(info.mainLogRecoveryEndAddress));
                Assert.That(read.snapshotFileLogicalStartAddress, Is.EqualTo(info.snapshotFileLogicalStartAddress));
                Assert.That(read.fuzzyRegionStartAddress, Is.EqualTo(info.fuzzyRegionStartAddress));
                Assert.That(read.beginAddress, Is.EqualTo(info.beginAddress));
                Assert.That(read.headAddress, Is.EqualTo(info.headAddress));
            });
        }

        [Test]
        [Category("CheckpointRestore")]
        public void DownlevelMetadataOmitsAppendedLogGeometry()
        {
            var info = MakeInfo();
            Assert.That(Lines(info.ToByteArray(7)), Has.Length.EqualTo(Lines(info.ToByteArray(8)).Length - 1),
                "v8 records the object log in three lines where v7 used seven (a segment and three two-line positions), and appends five:"
                + " segmentSize, objectLogSegmentSize, and objectLogPageSize from the store, then the host-supplied databaseMapping length"
                + " (zero here, so no entries follow) and swapEpoch");
        }

        [Test]
        [Category("CheckpointRestore")]
        public void TamperedAddressSlotFailsChecksum([Values(7, 8)] int targetVersion)
        {
            var info = MakeInfo();
            var lines = Lines(info.ToByteArray(targetVersion));
            lines[AddressSlotLineIndex] = (long.Parse(lines[AddressSlotLineIndex].Trim()) + 4096).ToString();

            var tampered = Encoding.UTF8.GetBytes(string.Join("\r\n", lines));
            _ = Assert.Throws<TsavoriteException>(() => _ = RoundTrip(tampered));
        }

        [Test]
        [Category("CheckpointRestore")]
        public void TamperedSegmentSizeFailsChecksum()
        {
            var info = MakeInfo();
            var lines = Lines(info.ToByteArray());

            // segmentSize is followed by objectLogSegmentSize, then the cookie-size line.
            var segmentSizeIndex = Array.FindLastIndex(lines, l => l.Trim() == info.segmentSize.ToString());
            Assert.That(segmentSizeIndex, Is.GreaterThan(AddressSlotLineIndex));
            lines[segmentSizeIndex] = (info.segmentSize * 2).ToString();

            var tampered = Encoding.UTF8.GetBytes(string.Join("\r\n", lines));
            _ = Assert.Throws<TsavoriteException>(() => _ = RoundTrip(tampered));
        }

        [Test]
        [Category("CheckpointRestore")]
        public void TamperedObjectLogSegmentSizeFailsChecksum()
        {
            var info = MakeInfo();
            var lines = Lines(info.ToByteArray());

            var index = Array.FindLastIndex(lines, l => l.Trim() == info.objectLogSegmentSize.ToString());
            Assert.That(index, Is.GreaterThan(AddressSlotLineIndex));
            lines[index] = (info.objectLogSegmentSize * 2).ToString();

            var tampered = Encoding.UTF8.GetBytes(string.Join("\r\n", lines));
            _ = Assert.Throws<TsavoriteException>(() => _ = RoundTrip(tampered));
        }

        [Test]
        [Category("CheckpointRestore")]
        public void TamperedObjectLogPageSizeFailsChecksum()
        {
            var info = MakeInfo();
            var lines = Lines(info.ToByteArray());

            var index = Array.FindLastIndex(lines, l => l.Trim() == info.objectLogPageSize.ToString());
            Assert.That(index, Is.GreaterThan(AddressSlotLineIndex));
            lines[index] = (info.objectLogPageSize * 2).ToString();

            var tampered = Encoding.UTF8.GetBytes(string.Join("\r\n", lines));
            _ = Assert.Throws<TsavoriteException>(() => _ = RoundTrip(tampered));
        }

        [Test]
        [Category("CheckpointRestore")]
        public void TamperedObjectLogTailsFailChecksum()
        {
            // The two tails are folded into the checksum, so changing both by the same amount, which an xor would cancel, is still caught.
            var info = MakeInfo();
            info.snapshotEndObjectLogTail = info.hlogEndObjectLogTail;
            var lines = Lines(info.ToByteArray());

            var tailText = info.hlogEndObjectLogTail.ToString();
            var tailIndices = Enumerable.Range(0, lines.Length).Where(i => lines[i].Trim() == tailText).ToArray();
            Assert.That(tailIndices, Has.Length.EqualTo(2));
            foreach (var i in tailIndices)
                lines[i] = (info.hlogEndObjectLogTail + 4096).ToString();

            var tampered = Encoding.UTF8.GetBytes(string.Join("\r\n", lines));
            _ = Assert.Throws<TsavoriteException>(() => _ = RoundTrip(tampered));
        }

        [Test]
        [Category("CheckpointRestore")]
        public void AppendedLogGeometryIsOrderedSegmentSizeThenObjectLogSegmentSizeThenObjectLogPageSize()
        {
            var info = MakeInfo();
            var lines = Lines(info.ToByteArray());

            var segmentSizeIndex = Array.FindLastIndex(lines, l => l.Trim() == info.segmentSize.ToString());
            var objectLogSegmentSizeIndex = Array.FindLastIndex(lines, l => l.Trim() == info.objectLogSegmentSize.ToString());
            var objectLogPageSizeIndex = Array.FindLastIndex(lines, l => l.Trim() == info.objectLogPageSize.ToString());
            Assert.Multiple(() =>
            {
                Assert.That(objectLogSegmentSizeIndex, Is.EqualTo(segmentSizeIndex + 1),
                    "objectLogSegmentSize must be serialized immediately after segmentSize");
                Assert.That(objectLogPageSizeIndex, Is.EqualTo(objectLogSegmentSizeIndex + 1),
                    "objectLogPageSize must be serialized immediately after objectLogSegmentSize");
            });
        }

        [Test]
        [Category("CheckpointRestore")]
        public void UnsupportedVersionsAreRejected()
        {
            var info = MakeInfo();
            Assert.Multiple(() =>
            {
                _ = Assert.Throws<TsavoriteException>(() => _ = info.ToByteArray(HybridLogRecoveryInfo.MinRecoverableCheckpointVersion - 1));
                _ = Assert.Throws<TsavoriteException>(() => _ = info.ToByteArray(HybridLogRecoveryInfo.CheckpointVersion + 1));
            });
        }

        [Test]
        [Category("CheckpointRestore")]
        public void CookieSurvivesEveryVersion([Values(7, 8)] int targetVersion)
        {
            var info = MakeInfo();
            info.cookie = [1, 2, 3, 250];

            var read = RoundTrip(info.ToByteArray(targetVersion));
            Assert.That(read.cookie, Is.EqualTo(info.cookie));
        }
    }
}