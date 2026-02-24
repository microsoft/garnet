// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Allure.NUnit;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class OpenTelemetryTests : AllureTestBase
    {
        private GarnetServer server;

        [SetUp]
        public void Setup()
        {
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown(waitForDelete: true);
        }

        [Test]
        public void OpenTelemetryMetricsDisabled_WhenNoOpenTelemetryEndpointConfigured()
        {
            // Arrange:
            var meterListener = new MeterListener();
            var publishedInstruments = new HashSet<string>();
            meterListener.InstrumentPublished = (instrument, listener) => publishedInstruments.Add(instrument.Name);
            meterListener.Start();
            server = CreateAndStartServer(0, false, false);

            // Act:
            SetAndGetRandomData();

            // Assert:
            Assert.That(server, Is.Not.Null);
            Assert.That(server.Provider, Is.Not.Null);
            Assert.That(server.Provider.StoreWrapper, Is.Not.Null);
            Assert.That(server.Provider.StoreWrapper.openTelemetryServerMonitor, Is.Null);
            Assert.That(publishedInstruments, Has.None.StartsWith("garnet.server."));
        }

        [Test]
        public void StartThrowsException_WhenOpenTelemetryConfigurationInvalid()
        {
            // Arrange:

            // Act & Assert:
            var exception = Assert.Throws<Exception>(() => CreateAndStartServer(0, false, true));
            Assert.That(exception.Message, Is.EqualTo("OpenTelemetry requires MetricsSamplingFrequency to be set"));
        }

        [TestCase(false)]
        [TestCase(true)]
        public void OpenTelemetryMetricsRecorded_WhenOpenTelemetryEndpointConfigured(bool enableLatencyMonitor)
        {
            // Arrange:
            var meterListener = new MeterListener();
            var publishedInstruments = new HashSet<string>();
            meterListener.InstrumentPublished = (instrument, listener) => publishedInstruments.Add(instrument.Name);
            meterListener.Start();
            server = CreateAndStartServer(1, enableLatencyMonitor, true);

            // Act:
            SetAndGetRandomData();

            // Assert:
            Assert.That(publishedInstruments, Has.One.EqualTo("garnet.server.connections.active"));
            Assert.That(publishedInstruments, Has.Exactly(enableLatencyMonitor ? 1 : 0).EqualTo("garnet.server.command.latency"));
        }

        private void SetAndGetRandomData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int opCount = 100;
            for (int i = 0; i < opCount; i++)
            {
                Assert.That(db.StringSet(i.ToString(), i.ToString()), Is.True);
                var result = (string)db.StringGet(i.ToString());
                Assert.That(result, Is.EqualTo(i.ToString()));
            }
        }

        private GarnetServer CreateAndStartServer(int metricsSamplingFrequency, bool enableLatencyMonitor, bool enableOpenTelemetry)
        {
            var server = TestUtils.CreateGarnetServer(
                TestUtils.MethodTestDir,
                metricsSamplingFreq: metricsSamplingFrequency,
                latencyMonitor: enableLatencyMonitor,
                enableOpenTelemetry: enableOpenTelemetry);

            server.Start();

            return server;
        }
    }
}
