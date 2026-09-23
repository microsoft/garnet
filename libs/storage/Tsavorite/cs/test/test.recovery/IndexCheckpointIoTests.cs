// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
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

            var ex = Assert.ThrowsAsync<TsavoriteIOException>(async () => await table.IsIndexFuzzyCheckpointCompletedAsync());
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

            var ex = Assert.ThrowsAsync<TsavoriteIOException>(async () => await table.IsIndexFuzzyCheckpointCompletedAsync());
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
                var ex = Assert.ThrowsAsync<TsavoriteIOException>(async ()
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
                var ex = Assert.ThrowsAsync<TsavoriteIOException>(async ()
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

        /// <summary>A device is permitted to complete successfully without reporting a transferred count, which
        /// <see cref="DeviceIOCompletionCallback"/> represents as 0. Short-transfer detection must treat that as
        /// "not reported" rather than as a truncated transfer, or every checkpoint and recovery on such a device
        /// would fail.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async Task ZeroReportedByteCountIsNotTreatedAsShortTransfer()
        {
            var htZeroCount = new ZeroCountReportingDevice(htDevice);
            var ofbZeroCount = new ZeroCountReportingDevice(ofbDevice);

            table = CreatePopulatedTable(Seed, TableSizeInBuckets, NumAdds);
            table.TakeIndexFuzzyCheckpoint(0, htZeroCount, out var htBytesWritten, ofbZeroCount, out var ofbBytesWritten, out var numOfbBuckets);
            await table.IsIndexFuzzyCheckpointCompletedAsync();

            ClassicAssert.AreEqual(0, htDevice.TruncatedRequestCount, "The device reports 0 bytes but transfers everything");

            var recovered = new TsavoriteBase();
            try
            {
                recovered.Initialize(TableSizeInBuckets, 512);
                await recovered.RecoverFuzzyIndexAsync(0, htZeroCount, htBytesWritten, ofbZeroCount, numOfbBuckets, ofbBytesWritten, CancellationToken.None);
                AssertTablesMatch(Seed, NumAdds, table, recovered);
            }
            finally
            {
                recovered.Free();
            }
        }

        /// <summary>A device that throws while submitting one of several chunk reads leaves the earlier reads
        /// outstanding. Recovery must still surface the failure instead of waiting forever for the chunks that were
        /// never issued, so that a caller can drain and close the device.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void RecoveryFailsPromptlyWhenAChunkCannotBeIssued()
        {
            const long maxIoBytesPerRequest = 1L << 20;     // The 4MiB table below needs four requests at this cap

            table = CreatePopulatedTable(Seed, TableSizeInBuckets, NumAdds);
            table.TakeIndexFuzzyCheckpoint(0, htDevice, out var htBytesWritten, ofbDevice, out var ofbBytesWritten, out var numOfbBuckets);
            table.IsIndexFuzzyCheckpointCompletedAsync().AsTask().Wait();

            // Let the first chunk through, then fail submission with the rest of the table still unread.
            var failing = new ThrowOnNthReadDevice(htDevice) { ThrowReadsAfter = 1 };

            var recovered = new TsavoriteBase();
            try
            {
                recovered.Initialize(TableSizeInBuckets, 512);

                var recovery = Task.Run(async ()
                    => await recovered.RecoverFuzzyIndexAsync(0, failing, htBytesWritten, ofbDevice, numOfbBuckets, ofbBytesWritten, CancellationToken.None, maxIoBytesPerRequest));

                // Wait through a continuation so a faulted recovery is observed as completion, not rethrown here.
                var settled = recovery.ContinueWith(_ => { }, TaskScheduler.Default).Wait(TimeSpan.FromSeconds(30));
                ClassicAssert.IsTrue(settled, "Recovery must not hang on chunks that were never issued");
                ClassicAssert.IsTrue(recovery.IsFaulted, "Recovery must report the submission failure");
                ClassicAssert.AreEqual(1, failing.ForwardedReadCount);

                // The chunks that were never issued must have been retired, so that a caller waiting for outstanding
                // reads to finish before closing the device is not left waiting forever.
                var drain = recovered.DrainMainIndexRecoveryAsync().AsTask();
                ClassicAssert.IsTrue(drain.Wait(TimeSpan.FromSeconds(30)), "Draining must complete once the issued read has called back");
            }
            finally
            {
                recovered.Free();
            }
        }

        /// <summary>When the device reports a write failure through the completion callback, the exception it supplied
        /// must survive to the caller as the inner exception rather than being flattened into a message, so the
        /// original error and its stack trace remain available for diagnosis.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void CheckpointPreservesTheDeviceExceptionAsInnerException()
        {
            table = CreatePopulatedTable(Seed, TableSizeInBuckets, NumAdds);

            var failing = new CallbackExceptionDevice(htDevice) { FailWrites = true };

            table.TakeIndexFuzzyCheckpoint(0, failing, out _, ofbDevice, out _, out _);

            var ex = Assert.ThrowsAsync<TsavoriteIOException>(async () => await table.IsIndexFuzzyCheckpointCompletedAsync());
            StringAssert.Contains("Main index checkpoint flush failed", ex.Message);
            ClassicAssert.AreSame(failing.Injected, ex.InnerException, "The device's exception must be preserved as the inner exception");
        }

        /// <summary>The same for recovery: a read failure reported with an exception must reach the caller with that
        /// exception attached.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void RecoveryPreservesTheDeviceExceptionAsInnerException()
        {
            table = CreatePopulatedTable(Seed, TableSizeInBuckets, NumAdds);
            table.TakeIndexFuzzyCheckpoint(0, htDevice, out var htBytesWritten, ofbDevice, out var ofbBytesWritten, out var numOfbBuckets);
            table.IsIndexFuzzyCheckpointCompletedAsync().AsTask().Wait();

            var failing = new CallbackExceptionDevice(htDevice) { FailReads = true };

            var recovered = new TsavoriteBase();
            try
            {
                recovered.Initialize(TableSizeInBuckets, 512);
                var ex = Assert.ThrowsAsync<TsavoriteIOException>(async ()
                    => await recovered.RecoverFuzzyIndexAsync(0, failing, htBytesWritten, ofbDevice, numOfbBuckets, ofbBytesWritten, default));
                StringAssert.Contains("Main index recovery failed", ex.Message);
                ClassicAssert.AreSame(failing.Injected, ex.InnerException, "The device's exception must be preserved as the inner exception");
            }
            finally
            {
                recovered.Free();
            }
        }

        /// <summary>A recovery that fails or is cancelled closes the index checkpoint file, but the reads it issued
        /// are still reading from that file and writing into the in-memory table. Closing it first is a use-after-free,
        /// so the failure path must wait for every issued read to call back. This exercises the production entry point,
        /// which owns both the device and the failure path.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void CancelledRecoveryClosesTheIndexFileOnlyAfterItsReadsHaveCompleted()
        {
            table = CreatePopulatedTable(Seed, TableSizeInBuckets, NumAdds);
            table.TakeIndexFuzzyCheckpoint(0, htDevice, out var htBytesWritten, ofbDevice, out var ofbBytesWritten, out var numOfbBuckets);
            table.IsIndexFuzzyCheckpointCompletedAsync().AsTask().Wait();

            var deferring = new DeferredCompletionDevice(htDevice);
            var recovered = new TsavoriteBase { checkpointManager = new SingleDeviceCheckpointManager(deferring) };
            try
            {
                recovered.Initialize(TableSizeInBuckets, 512);

                var info = new IndexCheckpointInfo();
                info.info.token = Guid.NewGuid();
                info.info.table_size = TableSizeInBuckets;
                info.info.num_ht_bytes = htBytesWritten;
                info.info.num_buckets = numOfbBuckets;
                info.info.num_ofb_bytes = ofbBytesWritten;

                using var cts = new CancellationTokenSource();
                var recovery = Task.Run(async () => await recovered.RecoverFuzzyIndexAsync(info, cts.Token));

                // The read is held, so recovery is waiting on it. Cancelling abandons that wait but not the read.
                while (deferring.DeferredCount == 0)
                    Thread.Sleep(10);
                cts.Cancel();

                var settled = recovery.ContinueWith(_ => { }, TaskScheduler.Default);
                ClassicAssert.IsFalse(settled.Wait(TimeSpan.FromMilliseconds(250)), "Recovery must not return while its read is outstanding");
                ClassicAssert.IsFalse(deferring.Disposed, "The index file must stay open while a read is reading from it");

                deferring.CompleteDeferred();
                ClassicAssert.IsTrue(settled.Wait(TimeSpan.FromSeconds(30)), "Recovery must return once the outstanding read has called back");
                ClassicAssert.IsTrue(recovery.IsCanceled || recovery.IsFaulted, "Recovery must report the cancellation");
                ClassicAssert.IsTrue(deferring.Disposed, "The index file must be closed on the failure path");
                ClassicAssert.IsFalse(deferring.DisposedWithIoOutstanding, "The index file was closed while a read was still using it");
            }
            finally
            {
                recovered.Free();
            }
        }

        /// <summary>A device may complete a request synchronously and then throw out of the submit. The completed
        /// request must be retired from the countdown exactly once: retiring it again in the submission-failure path
        /// would drop the countdown to zero while earlier reads are still writing into the table, and the caller would
        /// then close the device out from under them.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void SynchronousCompletionThenThrowRetiresAChunkOnlyOnce()
        {
            const long maxIoBytesPerRequest = 1L << 20;     // The 4MiB table below needs four requests at this cap

            table = CreatePopulatedTable(Seed, TableSizeInBuckets, NumAdds);
            table.TakeIndexFuzzyCheckpoint(0, htDevice, out var htBytesWritten, ofbDevice, out var ofbBytesWritten, out var numOfbBuckets);
            table.IsIndexFuzzyCheckpointCompletedAsync().AsTask().Wait();

            // Chunk 0 is left outstanding; chunk 1 completes synchronously and then fails to submit.
            var failing = new CallbackThenThrowDevice(htDevice) { DeferReadsBefore = 1 };

            var recovered = new TsavoriteBase();
            try
            {
                recovered.Initialize(TableSizeInBuckets, 512);

                var recovery = Task.Run(async ()
                    => await recovered.RecoverFuzzyIndexAsync(0, failing, htBytesWritten, ofbDevice, numOfbBuckets, ofbBytesWritten, CancellationToken.None, maxIoBytesPerRequest));
                ClassicAssert.IsTrue(recovery.ContinueWith(_ => { }, TaskScheduler.Default).Wait(TimeSpan.FromSeconds(30)));
                ClassicAssert.IsTrue(recovery.IsFaulted, "Recovery must report the submission failure");
                ClassicAssert.AreEqual(1, failing.DeferredCount, "Chunk 0 must still be outstanding");

                // Chunk 0 has not called back, so the countdown must not have reached zero. If chunk 1 were retired
                // twice the drain would complete here, telling the caller it is safe to close the device.
                var drain = recovered.DrainMainIndexRecoveryAsync().AsTask();
                ClassicAssert.IsFalse(drain.Wait(TimeSpan.FromMilliseconds(250)), "Draining must not complete while a read is outstanding");

                failing.CompleteDeferred();
                ClassicAssert.IsTrue(drain.Wait(TimeSpan.FromSeconds(30)), "Draining must complete once the outstanding read has called back");
            }
            finally
            {
                recovered.Free();
            }
        }

        /// <summary>A checkpoint whose level cannot be submitted must still complete its checkpoint task. The levels
        /// already issued call back and retire themselves, but a level that was never issued has no callback, so
        /// without retiring it the outstanding count never reaches zero and every waiter on the checkpoint blocks
        /// forever.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void OverflowBucketCheckpointFailsRatherThanHangingWhenALevelCannotBeIssued()
        {
            // Three levels, so that the first is issued, the second's submission fails, and the third is never issued
            // and so has no callback of its own to retire it.
            const int numRecords = (2 << 16) + 1;

            var allocator = new MallocFixedPageSize<HashBucket>();
            for (var ii = 0; ii < numRecords; ii++)
                _ = allocator.Allocate();

            // Not disposed here: it wraps ofbDevice, which the fixture teardown owns.
            var failing = new ThrowOnNthWriteDevice(ofbDevice) { ThrowWritesAfter = 1 };
            try
            {
                _ = Assert.Throws<IOException>(() => allocator.BeginCheckpoint(failing, 0, out _, useReadCache: false, skipReadCache: null, epoch: null));

                var completion = allocator.IsCheckpointCompletedAsync().AsTask();
                ClassicAssert.IsTrue(completion.ContinueWith(_ => { }, TaskScheduler.Default).Wait(TimeSpan.FromSeconds(30)),
                    "The checkpoint task must complete rather than hang");
                ClassicAssert.IsTrue(completion.IsFaulted, "The checkpoint must report the submission failure");
            }
            finally
            {
                allocator.Dispose();
            }
        }

        /// <summary>An index checkpoint whose chunk cannot be submitted must fail rather than hang - the chunks after
        /// the failure were counted but were never issued, so nothing else will retire them - and must not report
        /// completion while the chunks it did issue are still writing. Completing early releases the waiters that go
        /// on to dispose the checkpoint device, which fails those writes; their completions then land on whichever
        /// checkpoint owns the shared flush state by that point, carrying this checkpoint's error into the next one.
        /// </summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void IndexCheckpointDoesNotCompleteWhileAnIssuedChunkIsStillWriting()
        {
            const long maxIoBytesPerRequest = 1L << 20;     // The 4MiB table below needs four requests at this cap

            table = CreatePopulatedTable(Seed, TableSizeInBuckets, NumAdds);

            // Chunk 0 is forwarded but its completion is held, chunk 1's submission fails, and chunks 2 and 3 are
            // never issued and so have no callback of their own to retire them.
            // Not disposed here: it wraps htDevice, which the fixture teardown owns.
            var failing = new ThrowOnNthWriteDevice(htDevice) { ThrowWritesAfter = 1, DeferWriteCompletions = true };

            table.BeginMainIndexCheckpoint(0, failing, out _, maxIoBytesPerRequest: maxIoBytesPerRequest);

            // The held completion only reaches the queue once the underlying write finishes, so wait for it before
            // concluding anything from the checkpoint task still being incomplete.
            var heldByDeadline = SpinWait.SpinUntil(() => failing.DeferredCount > 0, TimeSpan.FromSeconds(30));
            ClassicAssert.IsTrue(heldByDeadline, "The issued chunk's completion must be the one being held");

            var completion = table.GetMainIndexCheckpointTask();
            var settled = completion.ContinueWith(_ => { }, TaskScheduler.Default);
            ClassicAssert.IsFalse(settled.Wait(TimeSpan.FromMilliseconds(250)),
                "The checkpoint must not report completion while the chunk it issued is still writing");

            failing.CompleteDeferred();

            ClassicAssert.IsTrue(settled.Wait(TimeSpan.FromSeconds(30)),
                "The checkpoint must complete rather than hang on chunks that were never issued");
            ClassicAssert.IsTrue(completion.IsFaulted, "The checkpoint must report the submission failure");
        }

        /// <summary>A 12-byte record: deliberately not a divisor of any sector size, so the checkpoint's sector
        /// rounding leaves padding that is not a whole number of records.</summary>
        [StructLayout(LayoutKind.Sequential, Size = 12)]
        private struct TwelveByteRecord
        {
            internal int a, b, c;
        }

        /// <summary>The overflow-bucket checkpoint rounds its final level up to a sector, so the persisted byte count
        /// is not generally a whole number of records. Recovery must read back exactly those bytes: deriving the
        /// length from a record count instead truncates, and issues a read that is both short and unaligned, which the
        /// O_DIRECT device paths reject outright.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void OverflowBucketRecoveryReadsSectorAlignedFinalLevel()
        {
            // 17 records of 12 bytes is 204 bytes, which the checkpoint rounds up to one 512-byte sector. 512 is not a
            // multiple of 12, so a record count round-trip loses the remainder.
            const int numRecords = 17;

            var allocator = new MallocFixedPageSize<TwelveByteRecord>();
            for (var ii = 0; ii < numRecords; ii++)
                _ = allocator.Allocate();

            allocator.BeginCheckpoint(ofbDevice, 0, out var numBytesWritten, useReadCache: false, skipReadCache: null, epoch: null);
            allocator.IsCheckpointCompletedAsync().AsTask().Wait();
            allocator.Dispose();

            ClassicAssert.AreEqual(0UL, numBytesWritten % OneSector, "The checkpoint always sector-aligns its final level");
            ClassicAssert.AreNotEqual(0UL, numBytesWritten % 12, "This test is only meaningful when the persisted size is not a whole number of records");

            ofbDevice.Reads.Clear();
            var recovered = new MallocFixedPageSize<TwelveByteRecord>();
            try
            {
                var numBytesRead = recovered.RecoverAsync(ofbDevice, 0, numRecords, numBytesWritten, CancellationToken.None).AsTask().GetAwaiter().GetResult();
                ClassicAssert.AreEqual(numBytesWritten, numBytesRead, "Recovery must read back exactly what was written");

                // Every read must be sector aligned, or a device opened with O_DIRECT rejects it outright.
                foreach (var (_, length) in ofbDevice.Reads)
                    ClassicAssert.AreEqual(0, length % OneSector, $"Read of {length} bytes is not sector aligned");
            }
            finally
            {
                recovered.Dispose();
            }
        }

        /// <summary>A device that splits one logical write across shards must report what the whole write transferred.
        /// Reporting only the last shard's count makes a complete checkpoint look short, which would fail a checkpoint
        /// that actually succeeded.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void ShardedCheckpointIsNotReportedAsShort()
        {
            var shard0 = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "Shard0.dat"), deleteOnClose: true);
            var shard1 = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "Shard1.dat"), deleteOnClose: true);
            using var sharded = new ShardedStorageDevice(new UniformPartitionScheme(IDevice.MinDeviceSectorSize, shard0, shard1));
            sharded.Initialize(segmentSize: 1L << 30, epoch: null);

            table = CreatePopulatedTable(Seed, TableSizeInBuckets, NumAdds);

            table.TakeIndexFuzzyCheckpoint(0, sharded, out _, ofbDevice, out _, out _);

            // Each shard transfers only its slice, so an aggregate that reports one shard's count looks short here.
            Assert.DoesNotThrowAsync(async () => await table.IsIndexFuzzyCheckpointCompletedAsync());
        }

        /// <summary>Cancelling a wait for outstanding IO must abandon only the wait. The IO itself is not cancellable,
        /// so the countdown has to stay usable: a failure path still needs to learn when the last callback has run
        /// before it releases the buffers and devices that IO is using.</summary>
        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void CancellingAWaitLeavesTheCountdownDrainable()
        {
            var countdown = new CountdownWrapper(2, isAsync: true);
            using var cts = new CancellationTokenSource();

            var wait = Task.Run(async () => await countdown.WaitAsync(cts.Token));
            cts.Cancel();
            _ = Assert.ThrowsAsync<TaskCanceledException>(async () => await wait);

            // The "outstanding IO" now completes, as it would have regardless of the cancellation.
            countdown.Decrement();
            var drain = countdown.DrainAsync().AsTask();
            ClassicAssert.IsFalse(drain.Wait(TimeSpan.FromMilliseconds(250)), "Draining must not complete while IO is outstanding");

            countdown.Decrement();
            ClassicAssert.IsTrue(drain.Wait(TimeSpan.FromSeconds(30)), "Draining must complete once the last callback has run");
            ClassicAssert.IsTrue(countdown.IsCompleted);
        }
    }
}