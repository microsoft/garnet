// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
using Allure.NUnit;
using Garnet.common;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class RespMetricsTest : AllureTestBase
    {
        GarnetServer server;
        ILoggerFactory loggerFactory;
        Random r;

        private void StartServer(int metricsSamplingFreq = -1, bool latencyMonitor = false)
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, metricsSamplingFreq: metricsSamplingFreq, latencyMonitor: latencyMonitor);
            server.Start();
        }

        [SetUp]
        public void Setup()
        {
            server = null;
            r = new Random(674386);
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            loggerFactory = TestUtils.CreateLoggerFactoryInstance(TestContext.Progress, LogLevel.Error);
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown(waitForDelete: true);
        }

        [Test]
        public void MetricsDisabledTest()
        {
            StartServer();
            var logger = loggerFactory.CreateLogger(TestContext.CurrentContext.Test.Name);
            var infoMetrics = server.Metrics.GetInfoMetrics().ToArray();

            ClassicAssert.AreNotEqual(null, infoMetrics);
            foreach (var section in infoMetrics)
            {
                logger.LogDebug("<{sectionName}>", section.Item1);
                foreach (var prop in section.Item2)
                {
                    if (section.Item1 == InfoMetricsType.STATS)
                    {
                        if (prop.Name.Equals("garnet_hit_rate"))
                            ClassicAssert.AreEqual("0.00", prop.Value);
                        else
                            ClassicAssert.AreEqual("0", prop.Value);
                    }
                    logger.LogDebug("\t {propName} : {propValue}", prop.Name, prop.Value);
                }
                logger.LogDebug("</{sectionName}>", section.Item1);
            }

            var latencyMetrics = server.Metrics.GetLatencyMetrics().ToArray();
            ClassicAssert.AreEqual(Array.Empty<(LatencyMetricsType, MetricsItem[])>(), latencyMetrics);
        }

        [Test]
        public void MetricsEnabledTest()
        {
            StartServer(1, true);
            var logger = loggerFactory.CreateLogger(TestContext.CurrentContext.Test.Name);
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int opCount = 1000;
            for (int i = 0; i < opCount; i++)
            {
                ClassicAssert.IsTrue(db.StringSet(i.ToString(), i.ToString()));
                var result = (string)db.StringGet(i.ToString());
                ClassicAssert.AreEqual(i.ToString(), result);
            }

            bool first = true;
        retry:
            Thread.Sleep(2000);
            var infoMetrics = server.Metrics.GetInfoMetrics().ToArray();
            ClassicAssert.AreNotEqual(null, infoMetrics);
            foreach (var section in infoMetrics)
            {
                logger.LogDebug("<{sectionName}>", section.Item1);
                foreach (var prop in section.Item2)
                {
                    if (section.Item1 == InfoMetricsType.STATS)
                    {
                        if (prop.Name.Equals("total_commands_processed"))
                        {
                            var total_commands_processed = Int32.Parse(prop.Value);
                            if (first && total_commands_processed < opCount)
                            {
                                first = false;
                                goto retry;
                            }
                            ClassicAssert.GreaterOrEqual(total_commands_processed, opCount);
                        }
                    }
                    logger.LogDebug("\t {propName} : {propValue}", prop.Name, prop.Value);
                }
                logger.LogDebug("</{sectionName}>", section.Item1);
            }

            var latencyMetrics = server.Metrics.GetLatencyMetrics(LatencyMetricsType.NET_RS_LAT).ToArray();
            while (latencyMetrics.Length == 0)
            {
                Thread.Yield();
                latencyMetrics = [.. server.Metrics.GetLatencyMetrics(LatencyMetricsType.NET_RS_LAT)];
            }
            ClassicAssert.AreNotEqual(Array.Empty<(LatencyMetricsType, MetricsItem[])>(), latencyMetrics);
            ClassicAssert.AreEqual(8, latencyMetrics.Length);

            ClassicAssert.AreEqual("calls", latencyMetrics[0].Name);
            ClassicAssert.AreEqual("min", latencyMetrics[1].Name);
            ClassicAssert.AreEqual("5th", latencyMetrics[2].Name);
            ClassicAssert.AreEqual("50th", latencyMetrics[3].Name);
            ClassicAssert.AreEqual("mean", latencyMetrics[4].Name);
            ClassicAssert.AreEqual("95th", latencyMetrics[5].Name);
            ClassicAssert.AreEqual("99th", latencyMetrics[6].Name);
            ClassicAssert.AreEqual("99.9th", latencyMetrics[7].Name);

        }
    }
}