// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespConfigTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void ConfigSetMemoryTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var metrics = server.Metrics.GetInfoMetrics(InfoMetricsType.STORE);
            var epc = metrics.FirstOrDefault(mi => mi.Name == "Log.EmptyPageCount");
            ClassicAssert.IsNotNull(epc);
            ClassicAssert.IsTrue(long.TryParse(epc.Value, out var emptyPageCount));
            ClassicAssert.AreEqual(0, emptyPageCount);

            var result = db.Execute("CONFIG", "SET", "memory", "16g");
            ClassicAssert.AreEqual("OK", result.ToString());

            Assert.Throws<RedisServerException>(() => db.Execute("CONFIG", "SET", "memory", "32g"),
                "ERR Cannot set dynamic memory size greater than configured circular buffer size.");

            metrics = server.Metrics.GetInfoMetrics(InfoMetricsType.STORE);
            epc = metrics.FirstOrDefault(mi => mi.Name == "Log.EmptyPageCount");
            ClassicAssert.IsNotNull(epc);
            ClassicAssert.IsTrue(long.TryParse(epc.Value, out emptyPageCount));
            ClassicAssert.AreEqual(0, emptyPageCount);

            try
            {
                db.Execute("CONFIG", "SET", "index", "129m");
            }
            catch (RedisServerException e)
            {
                ClassicAssert.AreEqual("ERR index size must be a power of 2.", e.Message);
            }

            Assert.Throws<RedisServerException>(() => db.Execute("CONFIG", "SET", "index", "129m"),
                "ERR index size must be a power of 2.");

            //result = db.Execute("CONFIG", "SET", "index", "512g");
            //ClassicAssert.AreEqual("OK", result.ToString());
        }
    }
}
