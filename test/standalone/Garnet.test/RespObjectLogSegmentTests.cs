// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespObjectLogSegmentTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            var options = TestUtils.GetGarnetServerOptions(TestUtils.MethodTestDir, TestUtils.MethodTestDir, TestUtils.EndPoint, enableCluster: false);
            options.LogMemorySize = "64m";
            options.PageSize = "64k";
            options.ObjectLogSegmentSize = "4m";
            server = new GarnetServer(options);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        [TestCase(4 * 1024 * 1024)]
        [TestCase(8 * 1024 * 1024)]
        public void LargeValuesScanAndGetAcrossObjectLogSegments(int valueLength)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            RedisKey[] keys = ["large:0", "large:1", "large:2"];
            var expected = new byte[keys.Length][];
            for (var record = 0; record < keys.Length; record++)
            {
                expected[record] = new byte[valueLength];
                for (var offset = 0; offset < valueLength; offset++)
                    expected[record][offset] = (byte)((offset * 31 + 7) ^ (record * 131 + 17));
                Assert.That(db.StringSet(keys[record], expected[record]), Is.True);
            }

            var log = server.Provider.StoreWrapper.store.Log;
            var tail = log.TailAddress;
            log.FlushAndEvict(wait: true);
            Assert.That(log.HeadAddress, Is.GreaterThanOrEqualTo(tail), "All records must be evicted so SCAN and GET read from disk");

            var scannedKeys = new List<RedisKey>();
            foreach (var key in redis.GetServer(TestUtils.EndPoint).Keys(pageSize: 1))
                scannedKeys.Add(key);
            Assert.That(scannedKeys, Is.EquivalentTo(keys));

            for (var record = 0; record < keys.Length; record++)
            {
                var actual = (byte[])db.StringGet(keys[record]);
                Assert.That(actual, Is.Not.Null, $"Missing value for {keys[record]}");
                Assert.That(actual.Length, Is.EqualTo(valueLength), $"Wrong length for {keys[record]}");
                Assert.That(expected[record].AsSpan().SequenceEqual(actual), Is.True, $"Wrong bytes for {keys[record]}");
            }
        }
    }
}