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
    public class RespCommandStatsTests : TestBase
    {
        GarnetServer server;

        private void StartServer(bool commandStatsMonitor = true, int metricsSamplingFreq = 0)
        {
            server = TestUtils.CreateGarnetServer(
                TestUtils.MethodTestDir,
                metricsSamplingFreq: metricsSamplingFreq,
                commandStatsMonitor: commandStatsMonitor);
            server.Start();
        }

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
        public void CommandStatsDisabledByDefaultTest()
        {
            // When CommandStatsMonitor is off, INFO COMMANDSTATS should return a disabled message
            StartServer(commandStatsMonitor: false);

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            db.StringSet("key1", "value1");

            string infoResult = db.Execute("INFO", "COMMANDSTATS").ToString();
            ClassicAssert.IsTrue(infoResult.Contains("Commandstats"), "Expected Commandstats section header");
            ClassicAssert.IsFalse(infoResult.Contains("cmdstat_set"), "Expected no cmdstat entries when disabled");
        }

        [Test]
        public void CommandStatsCallsTrackingTest()
        {
            StartServer();

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            int setCount = 10;
            int getCount = 5;

            for (int i = 0; i < setCount; i++)
                db.StringSet($"key{i}", $"value{i}");

            for (int i = 0; i < getCount; i++)
                db.StringGet($"key{i}");

            string infoResult = db.Execute("INFO", "COMMANDSTATS").ToString();
            string[] lines = infoResult.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);

            // Find SET command stats
            string setLine = lines.FirstOrDefault(l => l.StartsWith("cmdstat_set:"));
            ClassicAssert.IsNotNull(setLine, "Expected cmdstat_set entry");
            ulong setCalls = ParseCommandStatField(setLine, "calls");
            ClassicAssert.GreaterOrEqual(setCalls, (ulong)setCount, $"Expected at least {setCount} SET calls");

            // Find GET command stats
            string getLine = lines.FirstOrDefault(l => l.StartsWith("cmdstat_get:"));
            ClassicAssert.IsNotNull(getLine, "Expected cmdstat_get entry");
            ulong getCalls = ParseCommandStatField(getLine, "calls");
            ClassicAssert.GreaterOrEqual(getCalls, (ulong)getCount, $"Expected at least {getCount} GET calls");
        }

        [Test]
        public void CommandStatsFailedCallsTest()
        {
            StartServer();

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            // SETRANGE with a non-integer offset triggers AbortWithErrorMessage,
            // which sets commandErrorWritten and is tracked as a failed call.
            try
            {
                db.Execute("SETRANGE", "mykey", "not_a_number", "value");
            }
            catch (RedisServerException)
            {
                // Expected error: value is not an integer
            }

            string infoResult = db.Execute("INFO", "COMMANDSTATS").ToString();
            string[] lines = infoResult.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);

            // The SETRANGE command should show a failed call
            string setrangeLine = lines.FirstOrDefault(l => l.StartsWith("cmdstat_setrange:"));
            ClassicAssert.IsNotNull(setrangeLine, "Expected cmdstat_setrange entry");
            ulong failedCalls = ParseCommandStatField(setrangeLine, "failed_calls");
            ClassicAssert.GreaterOrEqual(failedCalls, (ulong)1, "Expected at least 1 failed SETRANGE call");
        }

        [Test]
        public void CommandStatsUsecFieldsZeroTest()
        {
            StartServer();

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            for (int i = 0; i < 100; i++)
                db.StringSet($"key{i}", $"value{i}");

            string infoResult = db.Execute("INFO", "COMMANDSTATS").ToString();
            string[] lines = infoResult.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);

            string setLine = lines.FirstOrDefault(l => l.StartsWith("cmdstat_set:"));
            ClassicAssert.IsNotNull(setLine, "Expected cmdstat_set entry");

            // usec fields are present for Redis format compatibility but report 0
            // (per-command latency tracking is not implemented to avoid Stopwatch overhead)
            ulong usec = ParseCommandStatField(setLine, "usec");
            ClassicAssert.AreEqual((ulong)0, usec, "Expected usec=0 (latency tracking not implemented)");

            string usecPerCallStr = ParseCommandStatFieldString(setLine, "usec_per_call");
            ClassicAssert.AreEqual("0.00", usecPerCallStr, "Expected usec_per_call=0.00");
        }

        [Test]
        public void CommandStatsMultipleCommandsTest()
        {
            StartServer();

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            // Execute various commands
            db.StringSet("k1", "v1");
            db.StringGet("k1");
            db.Execute("DEL", "k1");
            db.Execute("PING");

            string infoResult = db.Execute("INFO", "COMMANDSTATS").ToString();
            string[] lines = infoResult.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);

            // Verify multiple commands appear independently
            ClassicAssert.IsTrue(lines.Any(l => l.StartsWith("cmdstat_set:")), "Expected cmdstat_set");
            ClassicAssert.IsTrue(lines.Any(l => l.StartsWith("cmdstat_get:")), "Expected cmdstat_get");
            ClassicAssert.IsTrue(lines.Any(l => l.StartsWith("cmdstat_del:")), "Expected cmdstat_del");
            ClassicAssert.IsTrue(lines.Any(l => l.StartsWith("cmdstat_ping:")), "Expected cmdstat_ping");
        }

        [Test]
        public void CommandStatsSuccessRateTest()
        {
            // Use periodic sampling to cover the MetricsSamplingFrequency > 0 aggregation path
            StartServer(metricsSamplingFreq: 1);

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            // Execute 5 successful SETs
            for (int i = 0; i < 5; i++)
                db.StringSet($"key{i}", $"value{i}");

            Thread.Sleep(TimeSpan.FromSeconds(1.5));

            string infoResult = db.Execute("INFO", "COMMANDSTATS").ToString();
            string[] lines = infoResult.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);

            string setLine = lines.FirstOrDefault(l => l.StartsWith("cmdstat_set:"));
            ClassicAssert.IsNotNull(setLine, "Expected cmdstat_set entry");

            ulong calls = ParseCommandStatField(setLine, "calls");
            ulong failedCalls = ParseCommandStatField(setLine, "failed_calls");
            ulong rejectedCalls = ParseCommandStatField(setLine, "rejected_calls");

            // Success rate should be 100% — no failures or rejections for normal SET
            double successRate = calls > 0 ? (double)(calls - failedCalls - rejectedCalls) / calls : 0;
            ClassicAssert.AreEqual(1.0, successRate, 0.001, "Expected 100% success rate for SET commands");
        }

        [Test]
        public void CommandStatsInfoServerShowsMonitorStatus()
        {
            StartServer();

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            string infoResult = db.Execute("INFO", "SERVER").ToString();
            ClassicAssert.IsTrue(infoResult.Contains("commandstats_monitor:enabled"),
                "Expected commandstats_monitor:enabled in INFO SERVER");
        }

        [Test]
        public void CommandStatsFormatMatchesRedisConvention()
        {
            StartServer();

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            IDatabase db = redis.GetDatabase(0);

            db.StringSet("k", "v");

            string infoResult = db.Execute("INFO", "COMMANDSTATS").ToString();
            string[] lines = infoResult.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);

            string setLine = lines.FirstOrDefault(l => l.StartsWith("cmdstat_set:"));
            ClassicAssert.IsNotNull(setLine, "Expected cmdstat_set entry");

            // Verify Redis-compatible format: cmdstat_<name>:calls=X,usec=Y,usec_per_call=Z,rejected_calls=R,failed_calls=F
            ClassicAssert.IsTrue(setLine.Contains("calls="), "Expected calls= field");
            ClassicAssert.IsTrue(setLine.Contains("usec="), "Expected usec= field");
            ClassicAssert.IsTrue(setLine.Contains("usec_per_call="), "Expected usec_per_call= field");
            ClassicAssert.IsTrue(setLine.Contains("rejected_calls="), "Expected rejected_calls= field");
            ClassicAssert.IsTrue(setLine.Contains("failed_calls="), "Expected failed_calls= field");
        }

        /// <summary>
        /// Parse a numeric field value from a Redis commandstats line.
        /// Format: cmdstat_set:calls=100,usec=500,usec_per_call=5.00,rejected_calls=0,failed_calls=2
        /// </summary>
        private static ulong ParseCommandStatField(string line, string fieldName)
        {
            string value = ParseCommandStatFieldString(line, fieldName);
            // Handle floating point values by parsing as double first
            if (value.Contains('.'))
                return (ulong)double.Parse(value, System.Globalization.CultureInfo.InvariantCulture);
            return ulong.Parse(value);
        }

        /// <summary>
        /// Parse a string field value from a Redis commandstats line.
        /// </summary>
        private static string ParseCommandStatFieldString(string line, string fieldName)
        {
            string searchKey = fieldName + "=";
            int startIdx = line.IndexOf(searchKey, StringComparison.Ordinal);
            ClassicAssert.IsTrue(startIdx >= 0, $"Field '{fieldName}' not found in: {line}");
            startIdx += searchKey.Length;

            int endIdx = line.IndexOf(',', startIdx);
            if (endIdx < 0) endIdx = line.Length;

            return line.Substring(startIdx, endIdx - startIdx);
        }
    }
}