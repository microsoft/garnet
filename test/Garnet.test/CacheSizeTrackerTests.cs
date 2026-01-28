// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture]
    public class CacheSizeTrackerTests
    {
        GarnetServer server;
        TsavoriteKV<StoreFunctions, StoreAllocator> store;
        CacheSizeTracker cacheSizeTracker;

        // The HLOG will always have at least two pages allocated.
        const int MinLogAllocatedPageCount = 2;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // memorySizeStr is 2k for inline pages (hence pageCount: 4) plus 3k for heap allocations
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, memorySize: "5k", pageSize: "512", pageCount: "4", lowMemory: true, indexSize: "1k");
            server.Start();
            store = server.Provider.StoreWrapper.store;
            cacheSizeTracker = server.Provider.StoreWrapper.sizeTracker;
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void HeapSizeValidationTest()
        {
            ClassicAssert.AreEqual(0, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Faster")]);
            string r = db.HashGet("user:user1", "Title");
            ClassicAssert.AreEqual("Faster", r);

            // This will count only the value object; there is no key overflow.
            ClassicAssert.AreEqual(208, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);
        }

        [Test, CancelAfter(40 * 1000)]
        public void IncreaseEmptyPageCountTest()
        {
            var epcEvent = new ManualResetEventSlim(false);
            var emptyPageCountIncrements = 0;
            cacheSizeTracker.mainLogTracker.PostMemoryTrim = (allocatedPageCount, headAddress) => { emptyPageCountIncrements++; if (emptyPageCountIncrements == 3) epcEvent.Set(); };

            ClassicAssert.AreEqual(0, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);
            ClassicAssert.AreEqual(MinLogAllocatedPageCount, cacheSizeTracker.mainLogTracker.logAccessor.AllocatedPageCount);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Faster")]);
            string r = db.HashGet("user:user1", "Title");
            ClassicAssert.AreEqual("Faster", r);

            // This will count only the value object; there is no key overflow.
            const int MemorySizePerEntry = 208;

            ClassicAssert.AreEqual(MemorySizePerEntry, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);
            ClassicAssert.AreEqual(MinLogAllocatedPageCount, cacheSizeTracker.mainLogTracker.logAccessor.AllocatedPageCount); // Ensure APC hasn't changed as memory is still within the min & max limits

            // K/V lengths fit into a single byte each, so the record size is: RecordInfo, MinLengthMetadataBytes, keyLength, valueLength; the total rounded up to record alignment.
            // ValueLength is 4 for the ObjectId, so this becomes 8 + 3 + (11) + 4 totalling 26, rounding up to 32 which is a even divisor for the page size.
            // First valid address is 64, so a memory size of 1k and page size of 512b allow 28 total records. Create enough records to cross a page boundary.
            const int NumRecords = 20;
            for (var i = 2; i <= NumRecords; i++)
                db.HashSet($"user:user{i:00}", [new HashEntry("Title", "Faster")]);
            ClassicAssert.AreEqual(NumRecords * MemorySizePerEntry, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);

            // Wait for up to 3x resize task delay for the resizing to happen
            if (!epcEvent.Wait(TimeSpan.FromSeconds(3 * LogSizeTracker<StoreFunctions, StoreAllocator>.ResizeTaskDelaySeconds)))
                Assert.Fail("Timeout occurred. Resizing did not happen within the specified time.");
        }

        [Test]
        public void ReadCacheIncreaseEmptyPageCountTest()
        {
            server?.Dispose();

            // Create with a main-log heapMemorySize we won't hit, just to instantiate the tracker to ensure record heapMemory size.
            // memorySizeStr is 1GB for that, while readCache has only 1k for its limit; both have 1k for inline pages (hence pageCounts are 2)
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, memorySize: "1GB", pageSize: "512", pageCount: "2", lowMemory: true, indexSize: "1k",
                    readCacheMemorySize: "2k", readCachePageSize: "1k", readCachePageCount: "2", enableReadCache: true);

            server.Start();
            store = server.Provider.StoreWrapper.store;
            cacheSizeTracker = server.Provider.StoreWrapper.sizeTracker;

            var readCacheEmptyPageCountIncrements = 0;
            var readCacheEpcEvent = new ManualResetEventSlim(false);

            cacheSizeTracker.readCacheTracker.PostMemoryTrim = (allocatedPageDCount, headAddress) => { readCacheEmptyPageCountIncrements++; readCacheEpcEvent.Set(); };

            ClassicAssert.AreEqual(0, cacheSizeTracker.readCacheTracker.LogHeapSizeBytes);
            ClassicAssert.AreEqual(MinLogAllocatedPageCount, cacheSizeTracker.readCacheTracker.logAccessor.AllocatedPageCount);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            // This will count only the value object; there is no key overflow.
            const int MemorySizePerEntry = 208;

            // K/V lengths fit into a single byte each, so the record size is: RecordInfo, MinLengthMetadataSize, keyLength, valueLength; the total rounded up to record alignment.
            //   RecordInfo.Size + MinLengthMetadataSize (5) + keyLength (12) + valueLength (4) + ObjectLogPosition (8) rounded up to record alignment (8) = 40
            // With PageHeader.Size (64) and 1024-byte memory with 512-byte pages, we can fit 11 records per page: (512 - 64 = 448) / 40 = 11 (with 8 bytes left over)
            const int InlineRecordSize = 40;

            // Insert one record to verify MemorySizePerEntry
            db.HashSet($"user:user{0:000}", [new HashEntry("Title", "Faster")]);
            ClassicAssert.AreEqual(MemorySizePerEntry, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);

            // Insert the rest of the records, enough to spill over to disk.
            for (var i = 1; i < 100; i++)
                db.HashSet($"user:user{i:000}", [new HashEntry("Title", "Faster")]);

            var info = TestUtils.GetStoreAddressInfo(redis.GetServer(TestUtils.EndPoint), includeReadCache: true);
            ClassicAssert.AreEqual(PageHeader.Size, info.ReadCacheTailAddress);

            // Now read back the earlier records, which were evicted to disk and will come back into the readcache. With 20 we will have one full and one partial page.
            const int NumReadCacheRecords = 20;
            for (var i = 0; i < NumReadCacheRecords; i++)
            {
                var value = db.HashGet($"user:user{i:000}", "Title");
                ClassicAssert.AreEqual("Faster", (string)value, i.ToString());
            }
            ClassicAssert.AreEqual(NumReadCacheRecords * MemorySizePerEntry, cacheSizeTracker.readCacheTracker.LogHeapSizeBytes);

            // We have two pages in the read cache now: one full (11 records) and one partial (9 records). So we will have one 8-byte leftover at the end of the first page.
            info = TestUtils.GetStoreAddressInfo(redis.GetServer(TestUtils.EndPoint), includeReadCache: true);
            ClassicAssert.AreEqual(PageHeader.Size * 2 + InlineRecordSize * NumReadCacheRecords + 8, info.ReadCacheTailAddress);

            if (!readCacheEpcEvent.Wait(TimeSpan.FromSeconds(3 * 3 * LogSizeTracker<StoreFunctions, StoreAllocator>.ResizeTaskDelaySeconds)))
                Assert.Fail("Timeout occurred. Resizing did not happen within the specified time.");

            ClassicAssert.AreEqual(1, readCacheEmptyPageCountIncrements);
            // The first page of the read cache has been evicted => 11 records removed, 9 remain.
            ClassicAssert.AreEqual(9 * MemorySizePerEntry, cacheSizeTracker.readCacheTracker.LogHeapSizeBytes);
        }
    }
}