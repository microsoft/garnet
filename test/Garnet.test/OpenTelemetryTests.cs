// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
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
            using var meterListener = new MeterListener();
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
            using var meterListener = new MeterListener();
            var publishedInstruments = new HashSet<string>();
            var recordedMeasurements = new ConcurrentDictionary<string, double>();

            meterListener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Name.StartsWith("garnet.server."))
                {
                    publishedInstruments.Add(instrument.Name);
                    listener.EnableMeasurementEvents(instrument);
                }
            };

            meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
                recordedMeasurements.AddOrUpdate(instrument.Name, measurement, (_, existing) => existing + measurement));
            meterListener.SetMeasurementEventCallback<int>((instrument, measurement, tags, state) =>
                recordedMeasurements.AddOrUpdate(instrument.Name, measurement, (_, existing) => existing + measurement));
            meterListener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
                recordedMeasurements.AddOrUpdate(instrument.Name, measurement, (_, existing) => existing + measurement));

            meterListener.Start();
            server = CreateAndStartServer(1, enableLatencyMonitor, true);

            // Act:
            SetAndGetRandomData();

            // Wait for at least one monitor sampling cycle (configured at 1 second) to
            // aggregate session metrics from active sessions into globalSessionMetrics.
            Thread.Sleep(2000);

            // Trigger collection of observable instruments
            meterListener.RecordObservableInstruments();

            // Assert: Verify all expected server metrics are published
            Assert.That(publishedInstruments, Has.Member("garnet.server.connections.active"));
            Assert.That(publishedInstruments, Has.Member("garnet.server.connections.received"));
            Assert.That(publishedInstruments, Has.Member("garnet.server.connections.disposed"));

            // Assert: Verify all expected session metrics are published
            Assert.That(publishedInstruments, Has.Member("garnet.server.commands.processed"));
            Assert.That(publishedInstruments, Has.Member("garnet.server.transaction.commands.received"));
            Assert.That(publishedInstruments, Has.Member("garnet.server.transaction.commands.failed"));
            Assert.That(publishedInstruments, Has.Member("garnet.server.write.commands.processed"));
            Assert.That(publishedInstruments, Has.Member("garnet.server.read.commands.processed"));
            Assert.That(publishedInstruments, Has.Member("garnet.server.cluster.commands.processed"));
            Assert.That(publishedInstruments, Has.Member("garnet.server.network.bytes.received"));
            Assert.That(publishedInstruments, Has.Member("garnet.server.network.bytes.sent"));
            Assert.That(publishedInstruments, Has.Member("garnet.server.cache.lookups"));
            Assert.That(publishedInstruments, Has.Member("garnet.server.cache.lookups.missed"));
            Assert.That(publishedInstruments, Has.Member("garnet.server.operations.pending"));
            Assert.That(publishedInstruments, Has.Member("garnet.server.resp.session.exceptions"));

            // Assert: Verify latency metrics are published only when latency monitoring is enabled
            if (enableLatencyMonitor)
            {
                Assert.That(publishedInstruments, Has.Member("garnet.server.command.latency"));
                Assert.That(publishedInstruments, Has.Member("garnet.server.bytes.processed"));
                Assert.That(publishedInstruments, Has.Member("garnet.server.operations.processed"));
            }
            else
            {
                Assert.That(publishedInstruments, Has.No.Member("garnet.server.command.latency"));
                Assert.That(publishedInstruments, Has.No.Member("garnet.server.bytes.processed"));
                Assert.That(publishedInstruments, Has.No.Member("garnet.server.operations.processed"));
            }

            // Assert: Verify session metrics have non-zero values after operations.
            Assert.That(recordedMeasurements.GetValueOrDefault("garnet.server.commands.processed"), Is.GreaterThan(0),
                "Expected commands to have been processed.");
            Assert.That(recordedMeasurements.GetValueOrDefault("garnet.server.write.commands.processed"), Is.GreaterThan(0),
                "Expected write commands to have been processed.");
            Assert.That(recordedMeasurements.GetValueOrDefault("garnet.server.read.commands.processed"), Is.GreaterThan(0),
                "Expected read commands to have been processed.");
            Assert.That(recordedMeasurements.GetValueOrDefault("garnet.server.network.bytes.received"), Is.GreaterThan(0),
                "Expected bytes to have been received from the network.");
            Assert.That(recordedMeasurements.GetValueOrDefault("garnet.server.network.bytes.sent"), Is.GreaterThan(0),
                "Expected bytes to have been sent to the network.");
            Assert.That(recordedMeasurements.GetValueOrDefault("garnet.server.cache.lookups"), Is.GreaterThan(0),
                "Expected cache lookups to have occurred.");

            if (enableLatencyMonitor)
            {
                Assert.That(recordedMeasurements.GetValueOrDefault("garnet.server.command.latency"), Is.GreaterThan(0),
                    "Expected command latency to have been recorded.");
                Assert.That(recordedMeasurements.GetValueOrDefault("garnet.server.bytes.processed"), Is.GreaterThan(0),
                    "Expected bytes processed per call to have been recorded.");
                Assert.That(recordedMeasurements.GetValueOrDefault("garnet.server.operations.processed"), Is.GreaterThan(0),
                    "Expected operations processed per call to have been recorded.");
            }
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
