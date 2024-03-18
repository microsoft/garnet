// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture]
    public class CacheSizeTrackerTests
    {
        GarnetServer server;
        TsavoriteKV<byte[], IGarnetObject> objStore;
        CacheSizeTracker cacheSizeTracker;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, MemorySize: "2k", PageSize: "512", lowMemory: true, objectStoreIndexSize: "1k", objectStoreTotalMemorySize: "8k");
            server.Start();
            objStore = server.Provider.StoreWrapper.objectStore;
            cacheSizeTracker = server.Provider.StoreWrapper.objectStoreSizeTracker;
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void HeapSizeValidationTest()
        {
            Assert.AreEqual(0, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Title", "Faster") });
            string r = db.HashGet("user:user1", "Title");
            Assert.AreEqual("Faster", r);

            Assert.AreEqual(248, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);
        }

        [Test, Timeout(40 * 1000)]
        public void IncreaseEmptyPageCountTest()
        {
            ManualResetEventSlim epcEvent = new ManualResetEventSlim(false);
            int emptyPageCountIncrements = 0;
            cacheSizeTracker.mainLogTracker.PostEmptyPageCountIncrease = (int count) => { emptyPageCountIncrements++; if (emptyPageCountIncrements == 3) epcEvent.Set(); };

            Assert.AreEqual(0, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);
            Assert.AreEqual(0, cacheSizeTracker.mainLogTracker.logAccessor.EmptyPageCount);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Title", "Faster") });
            string r = db.HashGet("user:user1", "Title");
            Assert.AreEqual("Faster", r);

            Assert.AreEqual(248, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes);
            Assert.AreEqual(0, cacheSizeTracker.mainLogTracker.logAccessor.EmptyPageCount); // Ensure empty page count hasn't changed as EPC is still within the min & max limits

            // Have enough records (24 bytes each) to cross a page boundary (512)
            for (int i = 2; i <= 24; i++)
            {
                db.HashSet($"user:user{i}", new HashEntry[] { new HashEntry("Title", "Faster") });
            }

            Assert.AreEqual(5952, cacheSizeTracker.mainLogTracker.LogHeapSizeBytes); // 24 * 248 for each hashset object

            // Wait for the resizing to happen
            bool eventSignaled = epcEvent.Wait(
                TimeSpan.FromSeconds(3 * LogSizeTracker<byte[], IGarnetObject, CacheSizeTracker.LogSizeCalculator>.resizeTaskDelaySeconds)); // Wait for 3x resize task delay

            if (!eventSignaled)
            {
                Assert.Fail("Timeout occurred. Resizing did not happen within the specified time.");
            }
        }
    }
}