// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class ReadCacheTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableReadCache: true, lowMemory: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void MainStoreReadCacheTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.Address, TestUtils.Port);
            var info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true);

            // Start at tail address of 64
            ClassicAssert.AreEqual(64, info.ReadCacheBeginAddress);
            ClassicAssert.AreEqual(64, info.ReadCacheTailAddress);

            // Do enough writes to overflow memory to push records to disk
            var targetHeadAddress = info.MemorySize;
            var i = 0;
            while (info.HeadAddress < targetHeadAddress)
            {
                var key = $"key{i:00000}";
                var value = $"val{i++:00000}";
                _ = db.StringSet(key, value);
                info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true);
            }

            // Read cache should not have been updated as there are no reads yet
            ClassicAssert.AreEqual(64, info.ReadCacheTailAddress);

            // Issue read of initial key to populate read cache
            var key0 = $"key00000";
            var value0 = db.StringGet(key0);
            ClassicAssert.AreEqual("val00000", (string)value0);
            info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true);
            ClassicAssert.AreEqual(64 + 40, info.ReadCacheTailAddress); // 40 bytes for one record

            Assert.Pass();
        }
    }
}