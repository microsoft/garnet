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
    using ObjectStoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using ObjectStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    [TestFixture]
    public class CacheSizeTrackerTests
    {
        GarnetServer server;
        TsavoriteKV<ObjectStoreFunctions, ObjectStoreAllocator> objStore;
        CacheSizeTracker cacheSizeTracker;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, memorySize: "2k", pageSize: "512", lowMemory: true, objectStoreIndexSize: "1k", objectStoreHeapMemorySize: "3k");
            server.Start();
            objStore = server.Provider.StoreWrapper.objectStore;
            cacheSizeTracker = server.Provider.StoreWrapper.objectStoreSizeTracker;
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
            int emptyPageCountIncrements = 0;
            cacheSizeTracker.mainLogTracker.PostEmptyPageCountIncrease = (int count) => { emptyPageCountIncrements++; if (emptyPageCountIncrements == 3) epcEvent.Set(); };

            ClassicAssert.AreEqual(0, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);
            ClassicAssert.AreEqual(0, cacheSizeTracker.mainLogTracker.logAccessor.EmptyPageCount);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Faster")]);
            string r = db.HashGet("user:user1", "Title");
            ClassicAssert.AreEqual("Faster", r);

            // This will count only the value object; there is no key overflow.
            const int MemorySizePerEntry = 208;
            const int NumRecords = 20;

            ClassicAssert.AreEqual(MemorySizePerEntry, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);
            ClassicAssert.AreEqual(0, cacheSizeTracker.mainLogTracker.logAccessor.EmptyPageCount); // Ensure empty page count hasn't changed as EPC is still within the min & max limits

            // K/V lengths fit into a single byte each, so the record size is: RecordInfo, MinLengthMetadataSize, keyLength, valueLength; the total rounded up to record alignment.
            // ValueLength is 4 for the ObjectId, so this becomes 8 + 3 + (10 or 11) + 4 totalling 25 or 26, both rounding up to 32 which is a even divisor for the page size.
            // First valid address is 64, and there are 25 total records. Have enough records to cross a page boundary (512)
            for (int i = 2; i <= NumRecords; i++)
                db.HashSet($"user:user{i}", [new HashEntry("Title", "Faster")]);
            ClassicAssert.AreEqual(NumRecords * MemorySizePerEntry, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);

            // Wait for up to 3x resize task delay for the resizing to happen
            if (!epcEvent.Wait(TimeSpan.FromSeconds(3 * LogSizeTracker<ObjectStoreFunctions, ObjectStoreAllocator, CacheSizeTracker.LogSizeCalculator>.ResizeTaskDelaySeconds)))
                Assert.Fail("Timeout occurred. Resizing did not happen within the specified time.");
        }

        [Test]
        public void ReadCacheIncreaseEmptyPageCountTest()
        {
            server?.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, memorySize: "1k", pageSize: "512", lowMemory: true, objectStoreIndexSize: "1k", objectStoreReadCacheHeapMemorySize: "1k", enableObjectStoreReadCache: true);
            server.Start();
            objStore = server.Provider.StoreWrapper.objectStore;
            cacheSizeTracker = server.Provider.StoreWrapper.objectStoreSizeTracker;

            var readCacheEmptyPageCountIncrements = 0;
            var readCacheEpcEvent = new ManualResetEventSlim(false);

            cacheSizeTracker.readCacheTracker.PostEmptyPageCountIncrease = (int count) => { readCacheEmptyPageCountIncrements++; readCacheEpcEvent.Set(); };

            ClassicAssert.AreEqual(0, cacheSizeTracker.readCacheTracker.LogHeapSizeBytes);
            ClassicAssert.AreEqual(0, cacheSizeTracker.readCacheTracker.logAccessor.EmptyPageCount);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            // Have enough records (24 bytes each) to spill over to disk
            for (var i = 0; i < 100; i++)
                db.HashSet($"user:user{i}", [new HashEntry("Title", "Faster")]);

            // This will count only the value object; there is no key overflow.
            const int MemorySizePerEntry = 208;
            const int NumRecords = 20;

            for (var i = 0; i < NumRecords; i++)
            {
                var value = db.HashGet($"user:user{i}", "Title");
                ClassicAssert.AreEqual("Faster", (string)value, i.ToString());
            }
            ClassicAssert.AreEqual(25 * MemorySizePerEntry, cacheSizeTracker.readCacheTracker.LogHeapSizeBytes);

            // K/V lengths fit into a single byte each, so the record size is: RecordInfo, MinLengthMetadataSize, keyLength, valueLength; the total rounded up to record alignment.
            // ValueLength is 4 for the ObjectId, so this becomes 8 + 3 + (10 or 11) + 4 totalling 25 or 26, both rounding up to 32 which is a even divisor for the page size.
            // First valid address is 64, and there are 25 total records.
            var info = TestUtils.GetStoreAddressInfo(redis.GetServer(TestUtils.EndPoint), includeReadCache: true, isObjectStore: true);
            ClassicAssert.AreEqual(64 + 32 * NumRecords, info.ReadCacheTailAddress);

            if (!readCacheEpcEvent.Wait(TimeSpan.FromSeconds(3 * 3 * LogSizeTracker<ObjectStoreFunctions, ObjectStoreAllocator, CacheSizeTracker.LogSizeCalculator>.ResizeTaskDelaySeconds)))
                ClassicAssert.Fail("Timeout occurred. Resizing did not happen within the specified time.");

            ClassicAssert.AreEqual(1, readCacheEmptyPageCountIncrements);
            // 1 page of the read cache has been evicted => 20 records removed (512 pg size - 24 initial - 8 pg boundary = 480. 480/24 = 20 records)
            // Leaves 5 records in the read cache. 5 * 248 = 1240
            ClassicAssert.AreEqual(1240, cacheSizeTracker.readCacheTracker.LogHeapSizeBytes);
        }
    }
}