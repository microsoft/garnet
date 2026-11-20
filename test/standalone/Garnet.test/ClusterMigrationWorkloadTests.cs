// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

extern alias RespBenchmark;

using System;
using System.IO;
using System.Text;
using System.Text.Json;
using NUnit.Framework;
using Workload = RespBenchmark::Resp.benchmark.ClusterMigrationWorkload;

namespace Garnet.test
{
    [TestFixture]
    public class ClusterMigrationWorkloadTests : TestBase
    {
        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            TestUtils.OnTearDown();
        }

        [TestCase(0, "XXXXXXX0")]
        [TestCase(4194303, "X4194303")]
        [TestCase(52232191, "52232191")]
        public void MatchesExistingPreloadKeys(int index, string expected)
        {
            var bytes = new byte[8];
            Workload.FillKey(bytes, index);
            Assert.That(Encoding.ASCII.GetString(bytes), Is.EqualTo(expected));
        }

        [Test]
        public void DetectsLostVersionsAndWrongKeys()
        {
            var value = new byte[2048];
            Workload.EncodeValue(value, 27, 8);
            Assert.DoesNotThrow(() => Workload.ValidateValue(value, 27, 7, false));
            Assert.Throws<InvalidDataException>(() => Workload.ValidateValue(value, 27, 9, false));
            Assert.Throws<InvalidDataException>(() => Workload.ValidateValue(value, 27, 7, true));
            Assert.Throws<InvalidDataException>(() => Workload.ValidateValue(value, 28, 8, true));
            Assert.DoesNotThrow(() => Workload.ValidateValue(value, 27, 8, true));
        }

        [Test]
        public void IssueWindowRetainsLateCompletionsAndFailedOperations()
        {
            Directory.CreateDirectory(TestUtils.MethodTestDir);
            var path = Path.Combine(TestUtils.MethodTestDir, "requests.bin");
            var start = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            using (var stream = File.Create(path))
            {
                Workload.WriteTrace(stream, start.AddMilliseconds(-1).Ticks, 2000, false, false, 1);
                Workload.WriteTrace(stream, start.AddMilliseconds(10).Ticks, 2_000_000, false, false, 2);
                Workload.WriteTrace(stream, start.AddMilliseconds(20).Ticks, 5000, true, true, 2);
                Workload.WriteTrace(stream, start.AddSeconds(1).Ticks, 100, false, false, 3);
            }
            using var json = JsonDocument.Parse(JsonSerializer.Serialize(Workload.AnalyzeTraces([path], start, start.AddSeconds(1))));
            var root = json.RootElement;
            Assert.That(root.GetProperty("CarryIn").GetInt64(), Is.EqualTo(1));
            Assert.That(root.GetProperty("CarryOut").GetInt64(), Is.EqualTo(1));
            var measurements = root.GetProperty("Measurements");
            Assert.That(measurements.GetProperty("StartedUtc").GetDateTime(), Is.EqualTo(start.AddMilliseconds(10)));
            Assert.That(measurements.GetProperty("CompletedUtc").GetDateTime(), Is.EqualTo(start.AddMilliseconds(2010)));
            Assert.That(measurements.GetProperty("GET").GetProperty("Count").GetInt64(), Is.EqualTo(1));
            Assert.That(measurements.GetProperty("GET").GetProperty("P99Microseconds").GetInt64(), Is.GreaterThanOrEqualTo(2_000_000));
            Assert.That(measurements.GetProperty("SET").GetProperty("Successes").GetInt64(), Is.EqualTo(0));
            Assert.That(measurements.GetProperty("SET").GetProperty("Errors").GetInt64(), Is.EqualTo(1));
        }

        [Test]
        public void RejectsIncompleteTraceInsteadOfLosingSamples()
        {
            Directory.CreateDirectory(TestUtils.MethodTestDir);
            var path = Path.Combine(TestUtils.MethodTestDir, "requests.bin");
            File.WriteAllBytes(path, new byte[17]);
            Assert.Throws<InvalidDataException>(() => Workload.AnalyzeTraces([path], DateTime.UtcNow, DateTime.UtcNow.AddSeconds(1)));
        }
    }
}