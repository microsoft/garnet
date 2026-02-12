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
    public class RespSlowLogTests : AllureTestBase
    {
        GarnetServer server;
        int slowLogThreshold = 3_000_000;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: true, latencyMonitor: false, disableObjects: false, slowLogThreshold: slowLogThreshold);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
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
        public void TestSlowLogGetWithEntry()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var result = db.Execute("SLOWLOG", "GET", -1);
            ClassicAssert.AreEqual(0, ((string[])result).Length);

            // Run a command that will exceed the slow log threshold
            string timeout = $"{(float)(0.1 + slowLogThreshold / 1_000_000)}";
            db.Execute("BLPOP", "foo", timeout);

            var newResult = (RedisResult[])db.Execute("SLOWLOG", "GET", -1);
            ClassicAssert.AreEqual(1, newResult.Length);

            var entry = (RedisResult[])newResult[0];
            ClassicAssert.AreEqual(6, entry.Length); // Number of fields in each entry
            ClassicAssert.AreEqual(0, (int)entry[0]); // Id
            ClassicAssert.Less(slowLogThreshold, (int)entry[2]); // Execution time

            var args = (string[])entry[3]; // Command and arguments
            ClassicAssert.AreEqual(3, args.Length);
            ClassicAssert.AreEqual("BLPOP", args[0]);
            ClassicAssert.AreEqual("foo", args[1]);
            ClassicAssert.AreEqual(timeout, args[2]);

            db.Execute("SLOWLOG", "RESET");

            var resetResult = db.Execute("SLOWLOG", "GET", -1);
            ClassicAssert.AreEqual(0, ((string[])resetResult).Length);
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