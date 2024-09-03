// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespInfoTests
    {
        GarnetServer server;
        Random r;

        [SetUp]
        public void Setup()
        {
            r = new Random(674386);
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: true, latencyMonitor: true, metricsSamplingFreq: 1, DisableObjects: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void ResetStatsTest()
        {
            TimeSpan metricsUpdateDelay = TimeSpan.FromSeconds(1.1);
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var infoResult = db.Execute("INFO").ToString();
            var infoResultArr = infoResult.Split("\r\n");
            var totalFound = infoResultArr.First(x => x.StartsWith("total_found"));
            ClassicAssert.AreEqual("total_found:0", totalFound, "Expected total_found to be 0 after starting the server.");

            var key = "myKey";
            var val = "myKeyValue";
            db.StringSet(key, val);
            ClassicAssert.AreEqual(val, db.StringGet(key).ToString());
            Thread.Sleep(metricsUpdateDelay);

            infoResult = db.Execute("INFO").ToString();
            infoResultArr = infoResult.Split("\r\n");
            totalFound = infoResultArr.First(x => x.StartsWith("total_found"));
            ClassicAssert.AreEqual("total_found:1", totalFound, "Expected total_foudn to be incremented to 1 after a successful request.");

            var result = db.Execute("INFO", "RESET");
            ClassicAssert.IsNotNull(result);
            Thread.Sleep(metricsUpdateDelay);

            infoResult = db.Execute("INFO").ToString();
            infoResultArr = infoResult.Split("\r\n");
            totalFound = infoResultArr.First(x => x.StartsWith("total_found"));
            ClassicAssert.AreEqual("total_found:0", totalFound, "Expected total_found to be reset to 0 after INFO RESET command");

            ClassicAssert.AreEqual(val, db.StringGet(key).ToString(), "Expected the value to match what was set earlier.");
            Thread.Sleep(metricsUpdateDelay);

            infoResult = db.Execute("INFO").ToString();
            infoResultArr = infoResult.Split("\r\n");
            totalFound = infoResultArr.First(x => x.StartsWith("total_found"));
            ClassicAssert.AreEqual("total_found:1", totalFound, "Expected total_found to be one after sending one successful request");
        }
    }
}