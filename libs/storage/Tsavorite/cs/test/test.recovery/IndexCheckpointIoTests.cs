// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.recovery
{
    /// <summary>
    /// Covers the index checkpoint and recovery IO paths against a device that completes a request successfully but
    /// short, which is what Linux does for any single read or write larger than MAX_RW_COUNT.
    /// </summary>
    [TestFixture]
    public class IndexCheckpointIoTests : TestBase
    {
        /// <summary>Linux MAX_RW_COUNT with 4KiB pages: INT_MAX rounded down to a page boundary. A single request
        /// larger than this transfers only this many bytes and reports success.</summary>
        private const long LinuxMaxRwCount = 0x7ffff000;

        private static readonly int HashBucketSize = Unsafe.SizeOf<HashBucket>();

        private const int TableSizeInBuckets = 1 << 16;
        private const int NumAdds = 1 << 14;
        private const int Seed = 123;

        /// <summary>One sector: smaller than the single overflow-bucket level a lightly loaded table produces, so a
        /// device capped at this length truncates that level's transfer.</summary>
        private const uint OneSector = 512;

        private TsavoriteBase table;
        private TruncatingIoDevice htDevice, ofbDevice;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            _ = Directory.CreateDirectory(TestUtils.MethodTestDir);

            htDevice = new TruncatingIoDevice(Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "IndexCheckpointIoHt.dat"), deleteOnClose: true));
            ofbDevice = new TruncatingIoDevice(Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "IndexCheckpointIoOfb.dat"), deleteOnClose: true));
        }

        [TearDown]
        public void TearDown()
        {
            table?.Free();
            table = null;
            htDevice?.Dispose();
            ofbDevice?.Dispose();
            htDevice = ofbDevice = null;
            TestUtils.OnTearDown();
        }

        /// <summary>Populate a hash table with pseudo-random tags and entries derived from <paramref name="seed"/>.</summary>
        private static unsafe TsavoriteBase CreatePopulatedTable(int seed, int sizeInBuckets, int numAdds)
        {
            var newTable = new TsavoriteBase();
            newTable.Initialize(sizeInBuckets, 512);

            var keyGenerator = new Random(seed);
            var valueGenerator = new Random(seed + 1);
            for (var ii = 0; ii < numAdds; ii++)
            {
                long key = keyGenerator.Next();
                HashEntryInfo hei = new(Utility.GetHashCode(key));
                newTable.FindOrCreateTag(ref hei, 0);
                newTable.UpdateSlot(hei.bucket, hei.slot, hei.entry.word, valueGenerator.Next(), out _);
            }
            return newTable;
        }

        /// <summary>Verify that every tag is present in both tables with the same entry.</summary>
        private static void AssertTablesMatch(int seed, int numAdds, TsavoriteBase expected, TsavoriteBase actual)
        {
            var keyGenerator = new Random(seed);
            for (var ii = 0; ii < 2 * numAdds; ii++)
            {
                long key = keyGenerator.Next();
                HashEntryInfo expectedHei = new(Utility.GetHashCode(key));
                HashEntryInfo actualHei = new(expectedHei.hash);

                var existsInExpected = expected.FindTag(ref expectedHei);
                var existsInActual = actual.FindTag(ref actualHei);

                ClassicAssert.AreEqual(existsInExpected, existsInActual);
                if (existsInExpected)
                    ClassicAssert.AreEqual(expectedHei.entry.word, actualHei.entry.word);
            }
        }

        /// <summary>The read-cache variant of the checkpoint rewrites each bucket before writing it; this test does
        /// not exercise read-cache eviction, so the rewrite is a no-op.</summary>
        private static unsafe SkipReadCache NoOpSkipReadCache() => static bucket => { };

        /// <summary>Assert that <paramref name="requests"/> cover [0, <paramref name="totalBytes"/>) exactly once, in
        /// order, with no request larger than <paramref name="maxBytesPerRequest"/>.</summary>
        private static void AssertChunksTile((ulong offset, uint length)[] requests, ulong totalBytes, long maxBytesPerRequest)
        {
            ClassicAssert.Greater(requests.Length, 1, "Expected the table to be split into more than one chunk");
            ulong expectedOffset = 0;
            foreach (var (offset, length) in requests)
            {
                ClassicAssert.AreEqual(expectedOffset, offset, "Chunks must be issued back to back");
                ClassicAssert.LessOrEqual(length, maxBytesPerRequest, "A chunk must not exceed the per-request limit");
                expectedOffset += length;
            }
            ClassicAssert.AreEqual(totalBytes, expectedOffset, "Chunks must cover the whole table");
        }

        /// <summary>The table is split into chunks small enough that a single request is never truncated by the OS.
        /// 2GiB is the index size reported in the bug: it was written as one request, which Linux completed 4096
        /// bytes short while reporting success.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        [TestCase(1L << 20)]
        [TestCase(1L << 26)]
        [TestCase(1L << 30)]
        [TestCase(1L << 31)]
        [TestCase(1L << 33)]
        [TestCase(1L << 36)]
        [TestCase(1L << 42)]
        public void IndexIoChunksStayWithinOsRequestLimit(long totalSize)
        {
            ClassicAssert.LessOrEqual(Constants.kMaxIoBytesPerRequest, LinuxMaxRwCount, "A single request must not exceed the OS transfer limit");
            ClassicAssert.LessOrEqual(Constants.kMaxIoBytesPerRequest, int.MaxValue, "A single request must fit devices that transfer through Memory<byte>");

            var numChunks = Utility.GetNumIoChunks(totalSize, Constants.kMaxIoBytesPerRequest);
            ClassicAssert.IsTrue(Utility.IsPowerOfTwo(numChunks), $"Chunk count {numChunks} is not a power of two");

            var chunkSize = totalSize / numChunks;
            ClassicAssert.AreEqual(totalSize, chunkSize * numChunks, "Chunks must tile the table exactly");
            ClassicAssert.LessOrEqual(chunkSize, Constants.kMaxIoBytesPerRequest);
            ClassicAssert.Less(chunkSize, LinuxMaxRwCount);

            // The split must not be finer than required, or a large index pays for needless requests.
            if (numChunks > 1)
                ClassicAssert.Greater(chunkSize * 2, Constants.kMaxIoBytesPerRequest, "Chunks are smaller than necessary");
        }

        /// <summary>A checkpoint whose write completes successfully but short must fail rather than record an index
        /// image that is missing its tail.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void MainIndexCheckpointFailsOnShortWrite()
        {
            table = CreatePopulatedTable(Seed, TableSizeInBuckets, NumAdds);

            // Drop the last sector of the table's single write, as Linux does to a request above MAX_RW_COUNT.
            var tableBytes = (long)TableSizeInBuckets * HashBucketSize;
            htDevice.MaxBytesPerRequest = (uint)(tableBytes - 4096);

            table.TakeIndexFuzzyCheckpoint(0, htDevice, out _, ofbDevice, out _, out _);

            var ex = Assert.ThrowsAsync<TsavoriteException>(async () => await table.IsIndexFuzzyCheckpointCompletedAsync());
            StringAssert.Contains("Main index checkpoint flush failed", ex.Message);
            ClassicAssert.AreEqual(1, htDevice.TruncatedRequestCount, "Expected exactly one truncated write");
        }

        /// <summary>A checkpoint of the overflow buckets whose write completes successfully but short must fail.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void OverflowBucketCheckpointFailsOnShortWrite()
        {
            table = CreatePopulatedTable(Seed, TableSizeInBuckets, NumAdds);

            ofbDevice.MaxBytesPerRequest = OneSector;

            table.TakeIndexFuzzyCheckpoint(0, htDevice, out _, ofbDevice, out _, out _);

            var ex = Assert.ThrowsAsync<TsavoriteException>(async () => await table.IsIndexFuzzyCheckpointCompletedAsync());
            StringAssert.Contains("Overflow-bucket checkpoint flush failed", ex.Message);
            ClassicAssert.GreaterOrEqual(ofbDevice.TruncatedRequestCount, 1, "Expected at least one truncated write");
        }

        /// <summary>Recovery whose read completes successfully but short must fail rather than bring up a hash table
        /// whose tail buckets were never read.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void MainIndexRecoveryFailsOnShortRead()
        {
            table = CreatePopulatedTable(Seed, TableSizeInBuckets, NumAdds);
            table.TakeIndexFuzzyCheckpoint(0, htDevice, out var htBytesWritten, ofbDevice, out var ofbBytesWritten, out var numOfbBuckets);
            table.IsIndexFuzzyCheckpointCompletedAsync().AsTask().Wait();

            // Drop the last sector of the table's single read, as Linux does to a request above MAX_RW_COUNT.
            var tableBytes = (long)TableSizeInBuckets * HashBucketSize;
            htDevice.MaxBytesPerRequest = (uint)(tableBytes - 4096);

            var recovered = new TsavoriteBase();
            try
            {
                recovered.Initialize(TableSizeInBuckets, 512);
                var ex = Assert.ThrowsAsync<TsavoriteException>(async ()
                    => await recovered.RecoverFuzzyIndexAsync(0, htDevice, htBytesWritten, ofbDevice, numOfbBuckets, ofbBytesWritten, default));
                StringAssert.Contains("Main index recovery failed", ex.Message);
                ClassicAssert.AreEqual(1, htDevice.TruncatedRequestCount, "Expected exactly one truncated read");
            }
            finally
            {
                recovered.Free();
            }
        }

        /// <summary>Recovery of the overflow buckets whose read completes successfully but short must fail.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void OverflowBucketRecoveryFailsOnShortRead()
        {
            table = CreatePopulatedTable(Seed, TableSizeInBuckets, NumAdds);
            table.TakeIndexFuzzyCheckpoint(0, htDevice, out var htBytesWritten, ofbDevice, out var ofbBytesWritten, out var numOfbBuckets);
            table.IsIndexFuzzyCheckpointCompletedAsync().AsTask().Wait();

            ofbDevice.MaxBytesPerRequest = OneSector;

            var recovered = new TsavoriteBase();
            try
            {
                recovered.Initialize(TableSizeInBuckets, 512);
                var ex = Assert.ThrowsAsync<TsavoriteException>(async ()
                    => await recovered.RecoverFuzzyIndexAsync(0, htDevice, htBytesWritten, ofbDevice, numOfbBuckets, ofbBytesWritten, default));
                StringAssert.Contains("Overflow-bucket recovery failed", ex.Message);
                ClassicAssert.GreaterOrEqual(ofbDevice.TruncatedRequestCount, 1, "Expected at least one truncated read");
            }
            finally
            {
                recovered.Free();
            }
        }

        /// <summary>Recovery must not read past the end of the checkpoint region: the overflow buckets follow the
        /// index in the same file, and the final overflow level is shorter than a full page.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async Task OverflowBucketRecoveryReadsOnlyWhatWasWritten()
        {
            table = CreatePopulatedTable(Seed, TableSizeInBuckets, NumAdds);
            table.TakeIndexFuzzyCheckpoint(0, htDevice, out var htBytesWritten, ofbDevice, out var ofbBytesWritten, out var numOfbBuckets);
            await table.IsIndexFuzzyCheckpointCompletedAsync();

            var recovered = new TsavoriteBase();
            try
            {
                recovered.Initialize(TableSizeInBuckets, 512);
                await recovered.RecoverFuzzyIndexAsync(0, htDevice, htBytesWritten, ofbDevice, numOfbBuckets, ofbBytesWritten, CancellationToken.None);

                var ofbWriteBytes = ofbDevice.Writes.Sum(write => (long)write.length);
                var ofbReadBytes = ofbDevice.Reads.Sum(read => (long)read.length);
                ClassicAssert.AreEqual((long)ofbBytesWritten, ofbWriteBytes, "Overflow-bucket writes must cover exactly the reported size");
                ClassicAssert.AreEqual(ofbWriteBytes, ofbReadBytes, "Recovery must not request more overflow-bucket bytes than were written");

                AssertTablesMatch(Seed, NumAdds, table, recovered);
            }
            finally
            {
                recovered.Free();
            }
        }

        /// <summary>An index written as several chunks is read back intact: the chunks tile the checkpoint region with
        /// no gaps or overlaps, and no chunk exceeds the per-request cap. Chunking is driven here by the smaller
        /// read-cache chunk size so the table stays small enough for a test.</summary>
        [Test]
        [Category("CheckpointRestore")]
        public async Task MultiChunkIndexCheckpointRoundTrips()
        {
            const int multiChunkTableSize = 1 << 20;    // 64MiB of buckets: more than one 32MiB read-cache chunk

            table = CreatePopulatedTable(Seed, multiChunkTableSize, NumAdds);

            table.BeginMainIndexCheckpoint(0, htDevice, out var htBytesWritten, useReadCache: true, skipReadCache: NoOpSkipReadCache());
            var sectorSize = htDevice.SectorSize;
            var alignedIndexSize = (htBytesWritten + (sectorSize - 1)) & ~((ulong)sectorSize - 1);
            table.overflowBucketsAllocator.BeginCheckpoint(ofbDevice, alignedIndexSize, out var ofbBytesWritten);
            var numOfbBuckets = table.overflowBucketsAllocator.GetMaxValidAddress();
            await table.IsIndexFuzzyCheckpointCompletedAsync();

            ClassicAssert.AreEqual((ulong)multiChunkTableSize * (ulong)HashBucketSize, htBytesWritten);
            AssertChunksTile(htDevice.Writes.ToArray(), htBytesWritten, Constants.kMaxIoBytesPerRequest);
            ClassicAssert.AreEqual(0, htDevice.TruncatedRequestCount);

            var recovered = new TsavoriteBase();
            try
            {
                recovered.Initialize(multiChunkTableSize, 512);
                await recovered.RecoverFuzzyIndexAsync(0, htDevice, htBytesWritten, ofbDevice, numOfbBuckets, ofbBytesWritten, CancellationToken.None);
                AssertTablesMatch(Seed, NumAdds, table, recovered);
            }
            finally
            {
                recovered.Free();
            }
        }

        /// <summary>A table larger than the per-request cap is written and read back as several requests, none of
        /// which the device truncates, and the recovered table matches the original. The cap is lowered here to a size
        /// a test can allocate; at the production cap this is the 2GiB index from the bug report, which was previously
        /// issued as a single request that the OS silently completed short.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async Task IndexLargerThanRequestCapRoundTripsWithoutTruncation()
        {
            const long maxIoBytesPerRequest = 1L << 20;     // The 4MiB table below needs four requests at this cap

            table = CreatePopulatedTable(Seed, TableSizeInBuckets, NumAdds);

            // The device truncates anything longer, as Linux does above MAX_RW_COUNT, so an oversized request loses
            // data rather than merely being inefficient.
            htDevice.MaxBytesPerRequest = (uint)maxIoBytesPerRequest;
            ofbDevice.MaxBytesPerRequest = (uint)maxIoBytesPerRequest;

            table.BeginMainIndexCheckpoint(0, htDevice, out var htBytesWritten, maxIoBytesPerRequest: maxIoBytesPerRequest);
            var sectorSize = htDevice.SectorSize;
            var alignedIndexSize = (htBytesWritten + (sectorSize - 1)) & ~((ulong)sectorSize - 1);
            table.overflowBucketsAllocator.BeginCheckpoint(ofbDevice, alignedIndexSize, out var ofbBytesWritten);
            var numOfbBuckets = table.overflowBucketsAllocator.GetMaxValidAddress();
            await table.IsIndexFuzzyCheckpointCompletedAsync();

            AssertChunksTile(htDevice.Writes.ToArray(), htBytesWritten, maxIoBytesPerRequest);
            ClassicAssert.AreEqual(0, htDevice.TruncatedRequestCount, "No write may exceed the device's per-request limit");

            var recovered = new TsavoriteBase();
            try
            {
                recovered.Initialize(TableSizeInBuckets, 512);
                await recovered.RecoverFuzzyIndexAsync(0, htDevice, htBytesWritten, ofbDevice, numOfbBuckets, ofbBytesWritten, CancellationToken.None, maxIoBytesPerRequest);

                AssertChunksTile(htDevice.Reads.ToArray(), htBytesWritten, maxIoBytesPerRequest);
                ClassicAssert.AreEqual(0, htDevice.TruncatedRequestCount, "No read may exceed the device's per-request limit");
                AssertTablesMatch(Seed, NumAdds, table, recovered);
            }
            finally
            {
                recovered.Free();
            }
        }
    }
}