// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// The read cache page count sizes the read cache, with or without a read cache memory size, and never the main log.
    /// </summary>
    [TestFixture]
    public class ReadCacheConfigurationTests : TestBase
    {
        const string PageSize = "4k";

        // A main log sized by memory holds its memory size divided by its page size.
        const string MainLogMemorySize = "64k";
        const int MainLogPageCount = 16;

        GarnetServer server;

        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// A read cache sized only by its page count starts, holds that count rounded up to a power of 2, and leaves a main log
        /// sized by memory at its memory size divided by its page size.
        /// </summary>
        [Test]
        public void PageCountOnlyReadCacheStartsAndSizesOnlyTheReadCache([Values(3, 4)] int readCachePageCount)
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, memorySize: MainLogMemorySize, pageSize: PageSize,
                enableReadCache: true, readCacheMemorySize: "0", readCachePageSize: PageSize, readCachePageCount: readCachePageCount);
            server.Start();

            AssertPageCaps(mainLog: MainLogPageCount, readCache: 4);
            AssertReadCacheStaysWithinItsPageCap(4);
        }

        /// <summary>A read cache page count below what the read cache memory size allows caps the read cache at that count.</summary>
        [Test]
        public void PageCountBelowMemorySizeCapsTheReadCache()
        {
            // The read cache memory size alone allows 16 pages.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, memorySize: MainLogMemorySize, pageSize: PageSize,
                enableReadCache: true, readCacheMemorySize: "64k", readCachePageSize: PageSize, readCachePageCount: 4);
            server.Start();

            AssertPageCaps(mainLog: MainLogPageCount, readCache: 4);
            AssertReadCacheStaysWithinItsPageCap(4);
        }

        /// <summary>A larger read cache page count does not raise the page count of a main log sized by page count.</summary>
        [Test]
        public void ReadCachePageCountDoesNotRaiseTheMainLogPageCount()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, pageSize: PageSize, pageCount: 4,
                enableReadCache: true, readCacheMemorySize: "0", readCachePageSize: PageSize, readCachePageCount: 16);
            server.Start();

            AssertPageCaps(mainLog: 4, readCache: 16);
        }

        private void AssertPageCaps(int mainLog, int readCache)
        {
            var store = server.Provider.StoreWrapper.store;
            ClassicAssert.AreEqual(mainLog, store.Log.MaxAllocatedPageCount, "main log page cap");
            ClassicAssert.AreEqual(readCache, store.ReadCache.MaxAllocatedPageCount, "read cache page cap");
        }

        /// <summary>Read back from disk many more records than the read cache's page cap holds, and verify the read cache stays within that cap.</summary>
        private void AssertReadCacheStaysWithinItsPageCap(int pageCap)
        {
            // Enough records to spill the main log to disk, and enough reads of them to fill the read cache several times over.
            const int NumKeys = 4000;
            const int NumReads = 1500;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            for (var i = 0; i < NumKeys; i++)
                ClassicAssert.IsTrue(db.StringSet($"key{i:00000}", $"val{i:00000}"));

            var store = server.Provider.StoreWrapper.store;
            ClassicAssert.Greater(store.Log.HeadAddress, store.Log.BeginAddress, "records must have spilled to disk");

            var initialHeadAddress = store.ReadCache.HeadAddress;
            var capBytes = pageCap * store.ReadCache.PageSizeBytes;
            for (var i = 0; i < NumReads; i++)
            {
                ClassicAssert.AreEqual($"val{i:00000}", (string)db.StringGet($"key{i:00000}"));
                ClassicAssert.LessOrEqual(store.ReadCache.TailAddress - store.ReadCache.HeadAddress, capBytes,
                    $"The read cache exceeded its {pageCap}-page cap after reading key {i}");
            }
            ClassicAssert.Greater(store.ReadCache.HeadAddress, initialHeadAddress, "The read cache must have evicted pages to stay within its cap");
        }
    }
}