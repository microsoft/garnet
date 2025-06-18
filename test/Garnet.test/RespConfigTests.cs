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
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableReadCache: true, enableObjectStoreReadCache: true, lowMemory: true);
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

            var result = db.Execute("CONFIG", "SET", "memory", "32g");

            metrics = server.Metrics.GetInfoMetrics(InfoMetricsType.STORE);
            epc = metrics.FirstOrDefault(mi => mi.Name == "Log.EmptyPageCount");
            ClassicAssert.IsNotNull(epc);
            ClassicAssert.IsTrue(long.TryParse(epc.Value, out emptyPageCount));
            ClassicAssert.AreEqual(0, emptyPageCount);
        }
    }
}
