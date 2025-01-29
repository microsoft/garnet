// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespSlowLogTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: true, latencyMonitor: false, DisableObjects: true, slowLogThreshold: 5_000_000);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void TestSlowLogHelp()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var result = (string[])db.Execute("SLOWLOG", "HELP");
            ClassicAssert.IsNotNull(result);
            ClassicAssert.AreEqual(12, result.Length);
        }

        [Test]
        public void TestSlowLogGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var result = db.Execute("SLOWLOG", "GET");
            ClassicAssert.AreEqual(0, ((string[])result).Length);
        }

        [Test]
        public void TestSlowLogGetCount()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var result = db.Execute("SLOWLOG", "GET", -1);
            ClassicAssert.AreEqual(0, ((string[])result).Length);
        }

        [Test]
        public void TestSlowLogLen()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var result = db.Execute("SLOWLOG", "LEN");
            ClassicAssert.AreEqual(0, (int)result);
        }

        [Test]
        public void TestSlowLogReset()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var result = db.Execute("SLOWLOG", "RESET");
            ClassicAssert.AreEqual("OK", (string)result);
        }
    }
}