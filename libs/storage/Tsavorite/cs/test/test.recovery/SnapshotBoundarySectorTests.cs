// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.objects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// Covers the sector containing a Snapshot checkpoint's end boundary, when that boundary falls mid-sector.
    ///
    /// A no-copy flush writes whole sectors straight out of the live allocator page, so the bytes between the checkpoint
    /// boundary and the end of its sector reach disk verbatim rather than zeroed. These tests pin the invariants that make
    /// that safe, so a regression surfaces here instead of as a silent misread:
    ///
    /// <list type="bullet">
    ///   <item><see cref="SnapshotRecoveryIgnoresBytesAboveCheckpointBoundary"/> overwrites the post-boundary bytes in the
    ///     snapshot file with a non-zero pattern and asserts recovery is unaffected. Recovery must bound its record walk by
    ///     the UNROUNDED boundary (<c>PageAsyncReadResult.maxAddressOffsetOnPage</c>), so it never parses those bytes as a
    ///     record nor interprets their object-log position word.</item>
    ///   <item><see cref="SnapshotWritesExactlyThroughBoundarySector"/> asserts the flush writes through the boundary's
    ///     sector and no further, so the file extent stays in step with the length recovery rounds its read up to.</item>
    /// </list>
    /// </summary>
    [TestFixture]
    public class SnapshotBoundarySectorTests : TestBase
    {
        const int NumRecords = 2000;

        // A byte pattern that is emphatically not zero, so a walk that runs past the boundary reads a bogus
        // RecordInfo/object-log position instead of an "unmistakably unset" one.
        const byte JunkPattern = 0xCC;

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task SnapshotRecoveryIgnoresBytesAboveCheckpointBoundary()
        {
            var (token, _, boundaryOffsetInSector, sectorSize) = await WriteSnapshotWithMidSectorBoundary().ConfigureAwait(false);

            var junkLength = (int)(sectorSize - boundaryOffsetInSector);
            var snapshotFile = FindSnapshotFile();
            var originalLength = new FileInfo(snapshotFile).Length;

            // Stamp the post-boundary remainder of the boundary sector. This is exactly the region the flush used to zero.
            using (var stream = new FileStream(snapshotFile, FileMode.Open, FileAccess.Write, FileShare.None))
            {
                stream.Seek(-junkLength, SeekOrigin.End);
                stream.Write(Enumerable.Repeat(JunkPattern, junkLength).ToArray(), 0, junkLength);
            }

            Assert.That(new FileInfo(snapshotFile).Length, Is.EqualTo(originalLength), "corrupting the tail must not resize the snapshot file");

            Prepare(out var log, out var objlog, out var store);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                var bContext = session.BasicContext;
                for (var i = 0; i < NumRecords; i++)
                {
                    var found = TryReadValue(bContext, i, out var value);
                    ClassicAssert.IsTrue(found, $"recovered key {i} not found");
                    ClassicAssert.AreEqual(i, value, $"recovered key {i} has wrong value");
                }
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task SnapshotWritesExactlyThroughBoundarySector()
        {
            var (_, info, boundaryOffsetInSector, sectorSize) = await WriteSnapshotWithMidSectorBoundary().ConfigureAwait(false);

            // PageSize is a power of two and at least the sector size, so AlignedPageSizeBytes == PageSize.
            const long pageSize = MinKvLogPageSize;
            var startPage = info.snapshotFileLogicalStartAddress / pageSize;
            var lastPage = info.recoveredTailAddress / pageSize;
            var endOffset = info.recoveredTailAddress % pageSize;
            var expectedLength = ((lastPage - startPage) * pageSize) + RoundUpTo(endOffset, sectorSize);

            var snapshotLength = new FileInfo(FindSnapshotFile()).Length;
            Assert.Multiple(() =>
            {
                // The flush must carry the boundary's sector in full and stop there -- neither truncating at the boundary
                // (which would leave recovery's sector-rounded read past EOF) nor padding out to a whole page.
                Assert.That(snapshotLength, Is.EqualTo(expectedLength));
                Assert.That(snapshotLength % sectorSize, Is.Zero, "the snapshot file must end on a sector boundary");

                // The junk region written by SnapshotRecoveryIgnoresBytesAboveCheckpointBoundary is the file's last
                // (sectorSize - boundaryOffsetInSector) bytes; confirm that region lies entirely above the boundary.
                Assert.That(snapshotLength - (sectorSize - boundaryOffsetInSector),
                    Is.EqualTo(((lastPage - startPage) * pageSize) + endOffset),
                    "the corrupted tail region must begin exactly at the checkpoint boundary");
            });
        }

        private static long RoundUpTo(long value, long alignment) => (value + alignment - 1) & ~(alignment - 1);

        /// <summary>
        /// Fills a store, takes a quiesced Snapshot checkpoint whose end boundary lands mid-sector, and returns the token,
        /// its recovery info, the boundary's offset within its sector, and the device sector size.
        /// </summary>
        private static async Task<(Guid token, HybridLogRecoveryInfo info, long boundaryOffsetInSector, long sectorSize)> WriteSnapshotWithMidSectorBoundary()
        {
            Guid token;
            long sectorSize;

            Prepare(out var log, out var objlog, out var store);
            try
            {
                sectorSize = log.SectorSize;
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var i = 0; i < NumRecords; i++)
                        _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });

                    // Nudge the tail off a sector boundary so the checkpoint's end address is genuinely mid-sector. No
                    // concurrent writer runs, so recoveredTailAddress is this TailAddress.
                    var extraKey = NumRecords;
                    while (store.Log.TailAddress % sectorSize == 0)
                        _ = bContext.Upsert(new TestObjectKey { key = extraKey++ }, new TestObjectValue { value = extraKey });

                    ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot),
                        "failed to initiate Snapshot checkpoint");
                }

                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            var checkpointInfo = default(HybridLogRecoveryInfo);
            checkpointInfo.Recover(token,
                new DeviceLogCommitCheckpointManager(
                    new LocalStorageNamedDeviceFactoryCreator(),
                    new DefaultCheckpointNamingScheme(new DirectoryInfo(Path.Combine(MethodTestDir, "check-points")).FullName)));

            var boundaryOffsetInSector = checkpointInfo.recoveredTailAddress % sectorSize;
            Assert.That(boundaryOffsetInSector, Is.Not.Zero,
                "the checkpoint boundary must fall mid-sector for this test to exercise the boundary sector");

            return (token, checkpointInfo, boundaryOffsetInSector, sectorSize);
        }

        private static string FindSnapshotFile()
        {
            var files = Directory.GetFiles(Path.Combine(MethodTestDir, "check-points"), "snapshot.dat.0", SearchOption.AllDirectories);
            Assert.That(files, Has.Length.EqualTo(1), "expected exactly one snapshot main-log file");
            return files[0];
        }

        private static bool TryReadValue(BasicContext<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions, ClassStoreFunctions, ClassAllocator> bContext,
            int key, out int value)
        {
            TestObjectInput input = default;
            TestObjectOutput output = new();
            var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output);
            if (status.IsPending)
            {
                Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                (status, output) = GetSinglePendingResult(completedOutputs);
            }

            value = output.value?.value ?? 0;
            return status.Found;
        }

        private static void Prepare(out IDevice log, out IDevice objlog, out TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "boundarysector.log"));
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "boundarysector.obj.log"));
            store = new(new()
            {
                IndexSize = 1L << 22,
                LogDevice = log,
                ObjectLogDevice = objlog,
                SegmentSize = 1L << 20,
                LogMemorySize = 64L * MinKvLogPageSize,
                PageSize = MinKvLogPageSize,
                CheckpointDir = Path.Combine(MethodTestDir, "check-points")
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        private static void Destroy(IDevice log, IDevice objlog, TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            store.Dispose();
            log.Dispose();
            objlog.Dispose();
        }
    }
}