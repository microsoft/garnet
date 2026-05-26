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
    public class CacheSizeTrackerTests : TestBase
    {
        GarnetServer server;
        TsavoriteKV<StoreFunctions, StoreAllocator> store;
        CacheSizeTracker cacheSizeTracker;

        // The HLOG will always have at least two pages allocated.
        const int MinLogAllocatedPageCount = 2;
        const int PageSize = IDevice.MinDeviceSectorSize;
        const int PageCount = LogSizeTracker.MinTargetPageCount * 2;
        const int TargetSize = 70_000;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // memorySizeStr is 2k for inline pages (hence pageCount: 4) plus 7k for heap allocations so we end up in the middle of the third page (see individual test notes).
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, memorySize: $"{TargetSize}", pageSize: $"{PageSize}", pageCount: PageCount, lowMemory: true, indexSize: "1k");
            server.Start();
            store = server.Provider.StoreWrapper.store;
            cacheSizeTracker = server.Provider.StoreWrapper.sizeTracker;
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
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
            db.HashSet("u000", [new HashEntry("Title", "Faster")]);
            string r = db.HashGet("u000", "Title");
            ClassicAssert.AreEqual("Faster", r);

            ClassicAssert.AreEqual(MemorySizePerObject, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);
            ClassicAssert.AreEqual(MinLogAllocatedPageCount, cacheSizeTracker.mainLogTracker.logAccessor.AllocatedPageCount); // Ensure APC hasn't changed as memory is still within the min & max limits

            // Inline size:
            //   K/V lengths fit into a single byte each, so the record size is: RecordInfo, MinLengthMetadataBytes, valueLength, keyLength, objectLogPosition; the total rounded up to record alignment.
            //   ValueLength is 4 for the ObjectId, so this becomes 8 + 5 + 3(key) + 4(value) totalling 20, plus 8 for objectLogPosition totaling 32 which is already rounded to record alignment
            //   and is a even divisor for the page size. First valid address is 64, so a 4kb page allows 126 records evenly.
            Assert.That(store.Log.TailAddress, Is.EqualTo(PageHeader.Size + RecordSize));

            // Heap size:
            //   MemorySizePerObject is 208 heap bytes per record, which x 126 records per page is 26208 heap bytes per page.
            // Total size:
            //   Per-page, heap size plus the 4kb of the page itself is 30304 tatal bytes per page.
            // We've limited ourselves to 100_000 memory size; so initially we'll have 3 fully allocated pages including object heap cost (90912 total bytes) and then the third page will be partially
            // allocated with the remaining memory before we need to start HeadAddress moving up (by 32 bytes each record), and then we'll evict pages as we pass their boundaries. For this test, the heap
            // size per page being larger than the inline page size makes it easy to track (a page's inline size is less than 3 records' worth of heap data), so we'll stay at 3 pages and advance HeadAddress.

            // Allocate the first two pages (remember we added one record above); we should not evict.
            int numRecords = 252;
            for (var ii = 1; ii < numRecords; ii++)
                db.HashSet($"u{ii:000}", [new HashEntry("Title", "Faster")]);
            Assert.That(evicted, Is.False, "Eviction should not have occurred yet");
            Assert.That(cacheSizeTracker.mainLogTracker.LogHeapSizeBytes, Is.EqualTo(numRecords * MemorySizePerObject));
            Assert.That(cacheSizeTracker.mainLogTracker.logAccessor.AllocatedPageCount, Is.EqualTo(3));                         // We are at the end of the second page and have the "allocate one page ahead" allocated
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo(numRecords * MemorySizePerObject + 3 * PageSize));
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo(64704)); // To help follow below
            Assert.That(store.Log.HeadAddress, Is.EqualTo(PageHeader.Size));
            Assert.That(store.Log.TailAddress, Is.EqualTo(PageHeader.Size * 2 + RecordSize * numRecords));

            var (highTarget, lowTarget) = cacheSizeTracker.mainLogTracker.TargetDeltaRange;
            Assert.That(highTarget, Is.EqualTo(77000));
            Assert.That(lowTarget, Is.EqualTo(68600));
            var remaining = highTarget - cacheSizeTracker.mainLogTracker.TotalSize;
            Assert.That(cacheSizeTracker.mainLogTracker.LogHeapSizeBytes, Is.EqualTo(numRecords * MemorySizePerObject));
            Assert.That(remaining, Is.EqualTo(12296));  // High Target - totalSize

            // Our next allocation will add a page and thus subtract 4kb from our budget, leaving 8200. This is enough for 39 more records (8112 bytes), which will put us in the middle of the third page
            // with 88 bytes remaining in our budget. We should still not have evicted yet.
            int batchSize = 39;
            for (var ii = 0; ii < batchSize; ii++)
                db.HashSet($"u{ii + numRecords:000}", [new HashEntry("Title", "Faster")]);
            numRecords += batchSize;
            Assert.That(numRecords, Is.EqualTo(291));
            Assert.That(evicted, Is.False, "Eviction should not have occurred yet");
            Assert.That(cacheSizeTracker.mainLogTracker.LogHeapSizeBytes, Is.EqualTo(numRecords * MemorySizePerObject));
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo(numRecords * MemorySizePerObject + 4 * PageSize));  // We are now on the third page and have the "allocate one page ahead" allocated
            Assert.That(store.Log.HeadAddress, Is.EqualTo(PageHeader.Size));
            Assert.That(store.Log.TailAddress, Is.EqualTo(PageSize * 2 + PageHeader.Size + RecordSize * batchSize));    // We're on the third page

            // Now we start evicting. Add one record, then wait for the eviction of one record. This will signal the completion event.
            // Eviction will proceed until it goes at or below lowTarget. We will go 208-88=120 bytes over budget, then evict until we have gained
            // 77000-68600+120 (highTarget - lowTarget plus overage) = 8520 bytes, which is more than 40 records so we'll evict 41, leaving us with
            // a total size of 41*208-8520=8 bytes under the low target.
            remaining = highTarget - cacheSizeTracker.mainLogTracker.TotalSize;
            Assert.That(remaining, Is.EqualTo(88));   // Verify our expected math
            db.HashSet($"u{numRecords++:000}", [new HashEntry("Title", "Faster")]);
            Assert.That(numRecords, Is.EqualTo(292));   // Verify our expected count after the addition that triggers eviction
            Assert.That(epcEvent.Wait(TimeSpan.FromSeconds(2 * LogSizeTracker.ResizeTaskDelaySeconds)), Is.True, "Timeout occurred. Resizing did not happen within the specified time, pt 1");
            Assert.That(evicted, Is.True, "Eviction should have occurred");
            var evictedRecords = 41;

            // HeadAddress will have advanced by #evictedRecords, but we've added only one record so Tail will only grow by one.
            batchSize = 41;     // reuse this for the "batch" of evicted records
            Assert.That(store.Log.HeadAddress, Is.EqualTo(PageHeader.Size + RecordSize * batchSize));
            Assert.That(store.Log.TailAddress, Is.EqualTo(PageSize * 2 + PageHeader.Size + RecordSize * 40));   // was 39 records into the page, now 40
            Assert.That(cacheSizeTracker.mainLogTracker.LogHeapSizeBytes, Is.EqualTo((numRecords - evictedRecords) * MemorySizePerObject));
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo((numRecords - batchSize) * MemorySizePerObject + 4 * PageSize));  // We are now on the third page and have the "allocate one page ahead" allocated
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo(lowTarget - 8));                  // alternate verification of TotalSize, calculated from expected eviction amount

            remaining = highTarget - cacheSizeTracker.mainLogTracker.TotalSize;
            Assert.That(remaining, Is.EqualTo(highTarget - lowTarget + 8));                                     // Another alternative verification of our expected math

            // We are 40 records into the current page and thus have space for 86 more, and we are again 8 bytes below lowTarget, so we will
            // add 41 records, which will go over budget by 120 bytes, and again we will evict 41 records leaving us 8 bytes under lowTarget,
            // with HeadAddress staying on the same page.
            evicted = false;
            epcEvent.Reset();
            batchSize = 41;
            for (var ii = 0; ii < batchSize; ii++)
                db.HashSet($"u{ii + numRecords:000}", [new HashEntry("Title", "Faster")]);
            numRecords += batchSize;
            Assert.That(numRecords, Is.EqualTo(333));
            evictedRecords += 41;
            Assert.That(evictedRecords, Is.EqualTo(82));

            Assert.That(epcEvent.Wait(TimeSpan.FromSeconds(2 * LogSizeTracker.ResizeTaskDelaySeconds)), Is.True, "Timeout occurred. Resizing did not happen within the specified time, pt 2");
            Assert.That(evicted, Is.True, "Eviction should have occurred");

            // HeadAddress has now advanced farther on the page.
            Assert.That(store.Log.HeadAddress, Is.EqualTo(PageHeader.Size + RecordSize * evictedRecords));
            Assert.That(store.Log.TailAddress, Is.EqualTo(PageSize * 2 + PageHeader.Size + RecordSize * 81));    // We are 81 records into the third page
            Assert.That(cacheSizeTracker.mainLogTracker.LogHeapSizeBytes, Is.EqualTo((numRecords - evictedRecords) * MemorySizePerObject));
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo((numRecords - evictedRecords) * MemorySizePerObject + 4 * PageSize));  // We are now on the third page and have the "allocate one page ahead" allocated
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo(lowTarget - 8));          // alternate verification of TotalSize, calculated from expected eviction amount

            // As before, evicting 41 left us 8 bytes under the low target, so we have (8400 + 8) / 208 = 40 records worth of budget with 88 bytes left over.
            // This will still leave HeadAddress on the same page, and by this point we are done with everything except verifying head address moving to the
            // next page will evict the page it was on and subtract its size from the TotalSize. So just do another quick 41-record batch.
            evicted = false;
            epcEvent.Reset();
            batchSize = 41;
            for (var ii = 0; ii < batchSize; ii++)
                db.HashSet($"u{ii + numRecords:000}", [new HashEntry("Title", "Faster")]);
            numRecords += batchSize;
            Assert.That(numRecords, Is.EqualTo(374));
            evictedRecords += 41;
            Assert.That(evictedRecords, Is.EqualTo(123));

            Assert.That(epcEvent.Wait(TimeSpan.FromSeconds(2 * LogSizeTracker.ResizeTaskDelaySeconds)), Is.True, "Timeout occurred. Resizing did not happen within the specified time, pt 2");
            Assert.That(evicted, Is.True, "Eviction should have occurred");

            // HeadAddress has now advanced farther on the page.
            Assert.That(store.Log.HeadAddress, Is.EqualTo(PageHeader.Size + RecordSize * evictedRecords));
            Assert.That(store.Log.TailAddress, Is.EqualTo(PageSize * 2 + PageHeader.Size + RecordSize * 122));  // We are 122 records into the third page
            Assert.That(cacheSizeTracker.mainLogTracker.LogHeapSizeBytes, Is.EqualTo((numRecords - evictedRecords) * MemorySizePerObject));
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo((numRecords - evictedRecords) * MemorySizePerObject + 4 * PageSize));  // We are still on the third page and have the "allocate one page ahead" allocated
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo(lowTarget - 8));          // alternate verification of TotalSize, calculated from expected eviction amount

            // Again, we have budget to add 40 records with 88 bytes left over. This time, however, we have only 3 records for HeadAddress to advance before it can evict its page.
            // And we have only 4 records remaining on tail page. So the sequence will be:
            // - Add 4 records, advancing tailAddress to the next page. This adds 4k to the budget, so we will take up 832 heap bytes for the 4 records' heap allocations plus
            //   the 4k page, totalling 4928. 8408-4928 = 3480 budget remaining, which is enough for 16 more records with 152 bytes left over, so we'll add 19 on the second page,
            //   totaling 21 records to add this loop and leaving us 56 bytes over budget.
            // - As HeadAddress moves to the second page it will evict the first page. That means we will evict not only those 3 records with their 624 heap bytes, but also the 4k page,
            //   so 4720 bytes evicted by the time HeadAddress moves to the first valid record of the second page. We started off with an eviction range of (hi-lo+56) for 8456, less 4720 = 3736,
            //   which divided by 208 = just under 18 so we'll evict 18, again leaving us 8 under lowTarget and moving HeadAddress to be at the 19th record on the second page.
            evicted = false;
            epcEvent.Reset();
            batchSize = 21;
            for (var ii = 0; ii < batchSize; ii++)
                db.HashSet($"u{ii + numRecords:000}", [new HashEntry("Title", "Faster")]);
            numRecords += batchSize;
            evictedRecords += 21;   // 3 + 18
            Assert.That(evictedRecords, Is.EqualTo(144));
            Assert.That(numRecords, Is.EqualTo(395));

            Assert.That(epcEvent.Wait(TimeSpan.FromSeconds(2 * LogSizeTracker.ResizeTaskDelaySeconds)), Is.True, "Timeout occurred. Resizing did not happen within the specified time, pt 3");
            Assert.That(evicted, Is.True, "Eviction should have occurred");

            // HeadAddress should be 19 records in to its new page.
            Assert.That(store.Log.HeadAddress, Is.EqualTo(PageSize + PageHeader.Size + RecordSize * 18));       // Should be 18 records into the second page
            Assert.That(store.Log.TailAddress, Is.EqualTo(PageSize * 3 + PageHeader.Size + RecordSize * 17));   // We are 17 records into the fifth page
            Assert.That(cacheSizeTracker.mainLogTracker.LogHeapSizeBytes, Is.EqualTo((numRecords - evictedRecords) * MemorySizePerObject));
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo((numRecords - evictedRecords) * MemorySizePerObject + 4 * PageSize));  // We trimmed an allocated page but added a new page, so no change to page count
            Assert.That(cacheSizeTracker.mainLogTracker.TotalSize, Is.EqualTo(lowTarget - 8));                  // alternate verification of TotalSize, calculated from expected eviction amount
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

        /// <summary>
        /// Verifies that removing the last element from an object collection (triggering HasRemoveKey →
        /// ExpireAndStop) correctly returns the heap tracker to zero. This exercises the IPU path where
        /// Operate mutates the object in-place, the sizeChange delta is applied before the ExpireAndStop
        /// early return, and OnDispose(Deleted) subtracts the remaining empty-collection overhead.
        /// </summary>
        [Test]
        public void RemoveLastElementReturnsTrackerToZero_IPU()
        {
            ClassicAssert.AreEqual(0, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add a single element to a set — creates the object record with heap tracking.
            db.SetAdd("myset", "value1");
            var heapAfterAdd = cacheSizeTracker.mainLogTracker.LogHeapSizeBytes;
            ClassicAssert.Greater(heapAfterAdd, 0, "Heap should be positive after SADD");

            // Remove the only element — triggers HasRemoveKey → ExpireAndStop → tombstone.
            var removed = db.SetRemove("myset", "value1");
            ClassicAssert.IsTrue(removed, "SREM should return true");

            // The tracker should return to zero: the sizeChange delta from SREM (negative) plus
            // OnDispose(Deleted) subtracting the empty-collection overhead should exactly cancel
            // the original SADD increment.
            ClassicAssert.AreEqual(0, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes,
                "Heap tracker must return to zero after removing the last element (IPU path)");
        }

        /// <summary>
        /// Same scenario but with the record in the readonly region, forcing the remove-last-element
        /// through CopyUpdate → PostCopyUpdater → HasRemoveKey → ExpireAndStop. The PCU path's
        /// tombstoned new record must not leak value heap because +value is only added on pcuSuccess=true.
        /// </summary>
        [Test]
        public void RemoveLastElementReturnsTrackerToZero_PCU()
        {
            ClassicAssert.AreEqual(0, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add a single element to a list.
            db.ListRightPush("mylist", "value1");
            var heapAfterAdd = cacheSizeTracker.mainLogTracker.LogHeapSizeBytes;
            ClassicAssert.Greater(heapAfterAdd, 0, "Heap should be positive after RPUSH");

            // Shift the record to the readonly region so the next mutation goes through CopyUpdate.
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            // Remove the only element — forces CopyUpdate → PCU → HasRemoveKey → ExpireAndStop.
            var popped = db.ListLeftPop("mylist");
            ClassicAssert.AreEqual("value1", (string)popped, "LPOP should return the value");

            // Force eviction to flush any remaining sealed source records.
            store.Log.FlushAndEvict(wait: true);

            ClassicAssert.AreEqual(0, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes,
                "Heap tracker must return to zero after removing the last element (PCU path)");
        }
    }
}