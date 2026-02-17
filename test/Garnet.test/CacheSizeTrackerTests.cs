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
        const int PageSize = 512;
        const int TargetSize = 9000;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // memorySizeStr is 2k for inline pages (hence pageCount: 4) plus 7k for heap allocations so we end up in the middle of the third page (see individual test notes).
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, memorySize: $"{TargetSize}", pageSize: $"{PageSize}", pageCount: 4, lowMemory: true, indexSize: "1k");
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
        public void RecordHeapSizeValidationTest()
        {
            ClassicAssert.AreEqual(0, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("usr01", [new HashEntry("Title", "Faster")]);
            string r = db.HashGet("usr01", "Title");
            ClassicAssert.AreEqual("Faster", r);

            // This will count only the value object; there is no key overflow.
            ClassicAssert.AreEqual(208, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);
        }

        [Test]
        public void SmallMainLogFineGrainedEvictionTest()
        {
            using var epcEvent = new ManualResetEventSlim(false);
            bool evicted = false;
            cacheSizeTracker.mainLogTracker.PostMemoryTrim = (allocatedPageCount, headAddress) => { evicted = true; epcEvent.Set(); };

            ClassicAssert.AreEqual(0, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);
            ClassicAssert.AreEqual(MinLogAllocatedPageCount, cacheSizeTracker.mainLogTracker.logAccessor.AllocatedPageCount);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // This will count only the value object; there is no key overflow. This is for an object with one entry; the dictionary
            // overhead for the entire object is 80, and the per-entry overhead is 128. So for each record we have 80 + 128 = 208 heap bytes.
            const int MemorySizePerObject = 208;
            const int RecordSize = 32;

            // Add one record to verify expected memory size.
            db.HashSet("u00", [new HashEntry("Title", "Faster")]);
            string r = db.HashGet("u00", "Title");
            ClassicAssert.AreEqual("Faster", r);

            ClassicAssert.AreEqual(MemorySizePerObject, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);
            ClassicAssert.AreEqual(MinLogAllocatedPageCount, cacheSizeTracker.mainLogTracker.logAccessor.AllocatedPageCount); // Ensure APC hasn't changed as memory is still within the min & max limits

            // Inline size:
            //   K/V lengths fit into a single byte each, so the record size is: RecordInfo, MinLengthMetadataBytes, valueLength, keyLength, objectLogPosition; the total rounded up to record alignment.
            //   ValueLength is 4 for the ObjectId, so this becomes 8 + 5 + 3(key) + 4(value) totalling 20, plus 8 for objectLogPosition totaling 32 which is already rounded to record alignment
            //   and is a even divisor for the page size. First valid address is 64, so a 512b page allows 14 records evenly; a memory size of 2k allows 56 total records. 
            Assert.That(store.Log.TailAddress, Is.EqualTo(PageHeader.Size + RecordSize));

            // Heap size:
            //   MemorySizePerEntry is 208 heap bytes per record, which x 14 records per page is 2912 heap bytes per page.
            // Total size:
            //   Per-page, heap size plus the 512 bytes of the page itself is 3424 tatal bytes per page.
            // We've limited ourselves to 9k memory size; so initially we'll have two fully allocated pages (6848 bytes) and then the third page will be partially allocated with the remaining memory
            // before we need to start HeadAddress moving up (by 32 bytes each record), and then we'll evict pages as we pass their boundaries. For this test, the heap size per page being larger than
            // the inline page size makes it easy to track (a page's inline size is less than 3 records' worth of heap data), so we'll stay at 3 pages and advance HeadAddress.

            // Allocate the first two pages (remember we added one record above); we should not evict.
            int numRecords = 28;
            for (var ii = 1; ii < numRecords; ii++)
                db.HashSet($"u{ii:00}", [new HashEntry("Title", "Faster")]);
            Assert.That(evicted, Is.False, "Eviction should not have occurred yet");
            Assert.That(cacheSizeTracker.mainLogTracker.LogHeapSizeBytes, Is.EqualTo(numRecords * MemorySizePerObject));
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo(numRecords * MemorySizePerObject + 3 * PageSize));  // We are at the end of the second page and have the "allocate one page ahead" allocated
            Assert.That(store.Log.HeadAddress, Is.EqualTo(PageHeader.Size));
            Assert.That(store.Log.TailAddress, Is.EqualTo(PageSize * 2));

            var (highTarget, lowTarget) = cacheSizeTracker.mainLogTracker.TargetDeltaRange;
            Assert.That(highTarget, Is.EqualTo(9900));
            Assert.That(lowTarget, Is.EqualTo(8820));
            var remaining = highTarget - cacheSizeTracker.mainLogTracker.TotalSize;
            Assert.That(remaining, Is.EqualTo(2540));

            // Our next allocation will add a page and thus subtract 512 from our budget, leaving 2028. This is enough for 9 more records (1872 bytes), which will put us in the middle of the third page
            // with 156 bytes remaining in our budget. We should still not have evicted yet.
            int batchSize = 9;
            for (var ii = 0; ii < batchSize; ii++)
                db.HashSet($"u{ii + numRecords:00}", [new HashEntry("Title", "Faster")]);
            numRecords += batchSize;
            Assert.That(numRecords, Is.EqualTo(37));
            Assert.That(evicted, Is.False, "Eviction should not have occurred yet");
            Assert.That(cacheSizeTracker.mainLogTracker.LogHeapSizeBytes, Is.EqualTo(numRecords * MemorySizePerObject));
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo(numRecords * MemorySizePerObject + 4 * PageSize));  // We are now on the third page and have the "allocate one page ahead" allocated
            Assert.That(store.Log.HeadAddress, Is.EqualTo(PageHeader.Size));
            Assert.That(store.Log.TailAddress, Is.EqualTo(PageSize * 2 + PageHeader.Size + RecordSize * batchSize));

            // Now we start evicting. Add one record, then wait for the eviction of one record. This will signal the completion event.
            // Eviction will proceed until it goes at or below lowTarget. We will go 208-156=52 bytes over size, then evict until we have gained
            // 9900-8820+52 (highTarget - lowTarget plus overage) = 1132 bytes, which is more than 5 records so we'll evict 6, leaving us with
            // a total size of 6*208-1132=116 bytes under the low target.
            db.HashSet($"u{numRecords++:00}", [new HashEntry("Title", "Faster")]);
            Assert.That(numRecords, Is.EqualTo(38));
            Assert.That(epcEvent.Wait(TimeSpan.FromSeconds(2 * LogSizeTracker.ResizeTaskDelaySeconds)), Is.True, "Timeout occurred. Resizing did not happen within the specified time, pt 1");
            Assert.That(evicted, Is.True, "Eviction should have occurred");
            var evictedRecords = 6;

            // HeadAddress will have advanced those 6 records, but we've added only one record so Tail will only grow by one.
            batchSize = 6;  // reuse this for the "batch" of evicted records
            Assert.That(store.Log.HeadAddress, Is.EqualTo(PageHeader.Size + RecordSize * batchSize));
            Assert.That(store.Log.TailAddress, Is.EqualTo(PageSize * 2 + PageHeader.Size + RecordSize * 10));  // was 9 records in, now 10
            Assert.That(cacheSizeTracker.mainLogTracker.LogHeapSizeBytes, Is.EqualTo((numRecords - evictedRecords) * MemorySizePerObject));
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo((numRecords - batchSize) * MemorySizePerObject + 4 * PageSize));  // We are now on the third page and have the "allocate one page ahead" allocated
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo(lowTarget - 116));                // alternate verification of TotalSize, calculated from expected eviction amount

            // We have space for 4 records on the current page, and we are 116 bytes below lowTarget, and highTarget-lowTarget = 1080,
            // so we have 1196 / 208 = 5 records worth of budget with 156 bytes left over. So add 6 records, which will go over budget by
            // 208-156=52 again, so again we will evict 6 records, with HeadAddress staying on the same page.
            evicted = false;
            epcEvent.Reset();
            batchSize = 6;
            for (var ii = 0; ii < batchSize; ii++)
                db.HashSet($"u{ii + numRecords:00}", [new HashEntry("Title", "Faster")]);
            numRecords += batchSize;
            Assert.That(numRecords, Is.EqualTo(44));
            evictedRecords += 6;
            Assert.That(evictedRecords, Is.EqualTo(12));

            Assert.That(epcEvent.Wait(TimeSpan.FromSeconds(2 * LogSizeTracker.ResizeTaskDelaySeconds)), Is.True, "Timeout occurred. Resizing did not happen within the specified time, pt 2");
            Assert.That(evicted, Is.True, "Eviction should have occurred");

            // HeadAddress was at 6 records into its page, so it has 8 records to go on the page; we evicted 6 of them, so are now 12 in.
            Assert.That(store.Log.HeadAddress, Is.EqualTo(PageHeader.Size + RecordSize * 12));
            Assert.That(store.Log.TailAddress, Is.EqualTo(PageSize * 3 + PageHeader.Size + RecordSize * 2));    // We are two records into the fourth page
            Assert.That(cacheSizeTracker.mainLogTracker.LogHeapSizeBytes, Is.EqualTo((numRecords - evictedRecords) * MemorySizePerObject));
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo((numRecords - evictedRecords) * MemorySizePerObject + 4 * PageSize));  // We are now on the third page and have the "allocate one page ahead" allocated
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo(lowTarget - 116));                // alternate verification of TotalSize, calculated from expected eviction amount

            // As before, evicting 6 left us with a total size of 6*208-1132=116 bytes under the low target, so have 1196 / 208 = 5 records worth of budget with 156 bytes left over.
            // So add 6 records, which will go over budget by 208-156=52 again. This time, however, we have only 2 records for HeadAddress to advance before it can evict its page,
            // which is more than twice a record size. That means we can evict to clear up the 1132 bytes by evicting 2 records for 416 bytes, then the page for 512 bytes, which
            // totals 928 bytes leaving us still 204 bytes over budget, which means HeadAddress will advance one record in to the next page (rather than 4 in, which it would be if
            // we could not reclaim the page space).
            evicted = false;
            epcEvent.Reset();
            batchSize = 6;
            for (var ii = 0; ii < batchSize; ii++)
                db.HashSet($"u{ii + numRecords:00}", [new HashEntry("Title", "Faster")]);
            numRecords += batchSize;
            evictedRecords += 3;
            Assert.That(evictedRecords, Is.EqualTo(15));
            Assert.That(numRecords, Is.EqualTo(50));

            Assert.That(epcEvent.Wait(TimeSpan.FromSeconds(2 * LogSizeTracker.ResizeTaskDelaySeconds)), Is.True, "Timeout occurred. Resizing did not happen within the specified time, pt 3");
            Assert.That(evicted, Is.True, "Eviction should have occurred");

            // HeadAddress should be one record in to its new page.
            Assert.That(store.Log.HeadAddress, Is.EqualTo(PageSize + PageHeader.Size + RecordSize));
            Assert.That(store.Log.TailAddress, Is.EqualTo(PageSize * 3 + PageHeader.Size + RecordSize * 8));    // We were two records into the fourth page, now we're 8 records in
            Assert.That(cacheSizeTracker.mainLogTracker.LogHeapSizeBytes, Is.EqualTo((numRecords - evictedRecords) * MemorySizePerObject));
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo((numRecords - evictedRecords) * MemorySizePerObject + 3 * PageSize));  // We trimmed an allocated page
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo(lowTarget - 4));                  // alternate verification of TotalSize, calculated from expected eviction amount
        }

        [Test]
        [Explicit("Revivification for readcache: update to be like SmallMainLogFineGrainedEvictionTest for readcache")]
        public void ReadCacheIncreaseEmptyPageCountTest()
        {
            server?.Dispose();

            // Create with a main-log heapMemorySize we won't hit, just to instantiate the tracker to ensure record heapMemory size.
            // memorySizeStr is 1GB for that, while readCache has only 1k for its limit; both have 1k for inline pages (hence pageCounts are 2)
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, memorySize: "1GB", pageSize: "512", pageCount: 2, lowMemory: true, indexSize: "1k",
                    readCacheMemorySize: "2k", readCachePageSize: "1k", readCachePageCount: 2, enableReadCache: true);

            server.Start();
            store = server.Provider.StoreWrapper.store;
            cacheSizeTracker = server.Provider.StoreWrapper.sizeTracker;

            var readCacheEmptyPageCountIncrements = 0;
            using var readCacheEpcEvent = new ManualResetEventSlim(false);

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
            db.HashSet($"usr{0:000}", [new HashEntry("Title", "Faster")]);
            ClassicAssert.AreEqual(MemorySizePerEntry, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);

            // Insert the rest of the records, enough to spill over to disk.
            for (var i = 1; i < 100; i++)
                db.HashSet($"usr{i:000}", [new HashEntry("Title", "Faster")]);

            var info = TestUtils.GetStoreAddressInfo(redis.GetServer(TestUtils.EndPoint), includeReadCache: true);
            ClassicAssert.AreEqual(PageHeader.Size, info.ReadCacheTailAddress);

            // Now read back the earlier records, which were evicted to disk and will come back into the readcache. With 20 we will have one full and one partial page.
            const int NumReadCacheRecords = 20;
            for (var i = 0; i < NumReadCacheRecords; i++)
            {
                var value = db.HashGet($"usr{i:000}", "Title");
                ClassicAssert.AreEqual("Faster", (string)value, i.ToString());
            }
            ClassicAssert.AreEqual(NumReadCacheRecords * MemorySizePerEntry, cacheSizeTracker.readCacheTracker.LogHeapSizeBytes);

            // We have two pages in the read cache now: one full (11 records) and one partial (9 records). So we will have one 8-byte leftover at the end of the first page.
            info = TestUtils.GetStoreAddressInfo(redis.GetServer(TestUtils.EndPoint), includeReadCache: true);
            ClassicAssert.AreEqual(PageHeader.Size * 2 + InlineRecordSize * NumReadCacheRecords + 8, info.ReadCacheTailAddress);

            if (!readCacheEpcEvent.Wait(TimeSpan.FromSeconds(3 * 3 * LogSizeTracker.ResizeTaskDelaySeconds)))
                Assert.Fail("Timeout occurred. Resizing did not happen within the specified time.");

            ClassicAssert.AreEqual(1, readCacheEmptyPageCountIncrements);
            // The first page of the read cache has been evicted => 11 records removed, 9 remain.
            ClassicAssert.AreEqual(9 * MemorySizePerEntry, cacheSizeTracker.readCacheTracker.LogHeapSizeBytes);
        }
    }
}