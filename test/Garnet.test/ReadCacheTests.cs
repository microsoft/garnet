// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class ReadCacheTests : AllureTestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableReadCache: true, enableObjectStoreReadCache: true, lowMemory: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        /// <summary>
        /// Verifies that the read cache is updated correctly when reading keys. 
        /// Also verifies that the read cache is evicted when it is full.
        /// </summary>
        [Test]
        public void MainStoreReadCacheTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.EndPoint);
            var info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true);

            // Start at tail address of 64
            ClassicAssert.AreEqual(64, info.ReadCacheBeginAddress);
            ClassicAssert.AreEqual(64, info.ReadCacheTailAddress);

            // Do enough writes to overflow memory to push records to disk
            for (var i = 0; i < 100; i++)
            {
                var key = $"key{i:00000}";
                var value = $"val{i:00000}";
                _ = db.StringSet(key, value);
            }

            info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true);
            // Ensure data has spilled to disk
            ClassicAssert.Greater(info.HeadAddress, info.BeginAddress);

            // Read cache should not have been updated as there are no reads yet
            ClassicAssert.AreEqual(64, info.ReadCacheTailAddress);

            // Issue read of initial key to populate read cache
            var key0 = $"key00000";
            var value0 = db.StringGet(key0);
            ClassicAssert.AreEqual("val00000", (string)value0);
            info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true);
            ClassicAssert.AreEqual(64 + 40, info.ReadCacheTailAddress); // 40 bytes for one record

            // Issue read again to ensure read cache is not updated
            value0 = db.StringGet(key0);
            ClassicAssert.AreEqual("val00000", (string)value0);
            info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true);
            ClassicAssert.AreEqual(64 + 40, info.ReadCacheTailAddress);

            // Read more keys to update read cache
            for (var j = 1; j < 20; j++)
            {
                var key = $"key{j:00000}";
                var value = db.StringGet(key);
                ClassicAssert.AreEqual($"val{j:00000}", (string)value);
            }
            info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true);
            ClassicAssert.AreEqual(64 + 40 * 20 + 8, info.ReadCacheTailAddress); // 40 bytes for 20 records + 8 bytes for page boundary alignment
            ClassicAssert.AreEqual(64, info.ReadCacheBeginAddress); // Read cache should not have been evicted yet

            // Issue more reads to start evicting read cache entries
            for (var j = 20; j < 40; j++)
            {
                var key = $"key{j:00000}";
                var value = db.StringGet(key);
                ClassicAssert.AreEqual($"val{j:00000}", (string)value);
            }
            info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true);
            ClassicAssert.Greater(info.ReadCacheBeginAddress, 64); // Read cache entries should have been evicted
        }

        [Test]
        public void ObjectStoreReadCacheTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.EndPoint);
            var info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true, isObjectStore: true);

            // Start at tail address of 24
            ClassicAssert.AreEqual(24, info.ReadCacheBeginAddress);
            ClassicAssert.AreEqual(24, info.ReadCacheTailAddress);

            // Do enough list pushes to overflow memory to push records to disk
            for (var i = 0; i < 100; i++)
            {
                var key = $"objKey{i:00000}";
                var value = $"objVal{i:00000}";
                _ = db.ListRightPush(key, value);
            }

            info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true, isObjectStore: true);
            // Ensure data has spilled to disk
            ClassicAssert.Greater(info.HeadAddress, info.BeginAddress);

            // Read cache should not have been updated as there are no reads yet
            ClassicAssert.AreEqual(24, info.ReadCacheTailAddress);

            // Issue read of initial key to populate read cache
            var key0 = $"objKey00000";
            var value0 = db.ListGetByIndex(key0, 0);
            ClassicAssert.AreEqual("objVal00000", (string)value0);
            info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true, isObjectStore: true);
            ClassicAssert.AreEqual(24 + 24, info.ReadCacheTailAddress); // 24 bytes for one record

            // Issue read again to ensure read cache is not updated
            value0 = db.ListGetByIndex(key0, 0);
            ClassicAssert.AreEqual("objVal00000", (string)value0);
            info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true, isObjectStore: true);
            ClassicAssert.AreEqual(24 + 24, info.ReadCacheTailAddress);

            // Read more keys to update read cache
            for (var j = 1; j < 40; j++)
            {
                var key = $"objKey{j:00000}";
                var value = db.ListGetByIndex(key, 0);
                ClassicAssert.AreEqual($"objVal{j:00000}", (string)value);
            }
            info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true, isObjectStore: true);
            ClassicAssert.AreEqual(24 + 24 * 40 + 8, info.ReadCacheTailAddress); // 24 bytes for 20 records + 8 bytes for page boundary alignment
            ClassicAssert.AreEqual(24, info.ReadCacheBeginAddress); // Read cache should not have been evicted yet

            // Issue more reads to start evicting read cache entries
            for (var j = 40; j < 80; j++)
            {
                var key = $"objKey{j:00000}";
                var value = db.ListGetByIndex(key, 0);
                ClassicAssert.AreEqual($"objVal{j:00000}", (string)value);
            }
            info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true, isObjectStore: true);
            ClassicAssert.Greater(info.ReadCacheBeginAddress, 24); // Read cache entries should have been evicted
        }
    }
}