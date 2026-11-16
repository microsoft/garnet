// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
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

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
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

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
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

        /// <summary>
        /// Commands a script runs execute on the script processor's session, whose statistics reach
        /// <c>INFO COMMANDSTATS</c> when the connection that ran the script ends. GET, SET, HGET, HSET and
        /// EXPIRE are served without RESP dispatch, so each must be counted like the commands that are dispatched.
        /// </summary>
        [Test]
        public void CommandStatsCountCommandsRunByScriptsTest([Values(0, 1)] int metricsSamplingFreq)
        {
            server = TestUtils.CreateGarnetServer(
                TestUtils.MethodTestDir,
                enableLua: true,
                metricsSamplingFreq: metricsSamplingFreq,
                commandStatsMonitor: true);
            server.Start();

            // The client's connection tie-breaker is a GET, which would be counted along with the script's
            var config = TestUtils.GetConfig(allowAdmin: true);
            config.TieBreaker = "";

            const int Calls = 10;
            const string Script = @"
                redis.call('SET', KEYS[1], 'v')
                redis.call('GET', KEYS[1])
                redis.call('EXPIRE', KEYS[1], 100)
                redis.call('HSET', KEYS[2], 'f', 'v')
                redis.call('HGET', KEYS[2], 'f')
                redis.call('INCR', KEYS[3])
                return 1";

            using var redis = ConnectionMultiplexer.Connect(config);
            var db = redis.GetDatabase(0);
            var baseline = InfoStats(db);

            using (var scriptRedis = ConnectionMultiplexer.Connect(config))
            {
                var scriptDb = scriptRedis.GetDatabase(0);
                for (var i = 0; i < Calls; i++)
                    _ = scriptDb.ScriptEvaluate(Script, [(RedisKey)"stats-string", (RedisKey)"stats-hash", (RedisKey)"stats-counter"]);
            }

            foreach (var cmd in new[] { "set", "get", "expire", "hset", "hget", "incr" })
            {
                var calls = WaitForCommandStat(db, cmd, "calls", Calls);
                ClassicAssert.AreEqual((ulong)Calls, calls, $"{cmd} called from scripts was not counted");
            }

            if (metricsSamplingFreq > 0)
            {
                // SET, EXPIRE, HSET and INCR write; GET and HGET read
                var delta = WaitForInfoStats(db, baseline, 4 * Calls);
                ClassicAssert.AreEqual((ulong)(4 * Calls), delta.Write, "Writes run by scripts were not counted in the session counters");
                ClassicAssert.AreEqual((ulong)(2 * Calls), delta.Read, "Reads run by scripts were not counted in the session counters");
                ClassicAssert.GreaterOrEqual(delta.Total, (ulong)(6 * Calls), "Commands run by scripts were not counted in the session counters");
            }
        }

        /// <summary>
        /// A command the ACL refuses is counted as rejected, also when the script runs it through a path that
        /// does not go through RESP dispatch.
        /// </summary>
        [Test]
        public void CommandStatsCountScriptCommandsRejectedByAclTest()
        {
            var aclFile = Path.GetTempFileName();
            try
            {
                File.WriteAllLines(aclFile,
                [
                    "user default on nopass +@all",
                    "user limited on nopass +@all -get -set -hget -hset -expire",
                ]);

                server = TestUtils.CreateGarnetServer(
                    TestUtils.MethodTestDir,
                    enableLua: true,
                    metricsSamplingFreq: 1,
                    commandStatsMonitor: true,
                    useAcl: true,
                    aclFile: aclFile);
                server.Start();

                var config = TestUtils.GetConfig(allowAdmin: true);
                config.TieBreaker = "";

                const int Calls = 10;
                const string Script = @"
                    redis.pcall('SET', KEYS[1], 'v')
                    redis.pcall('GET', KEYS[1])
                    redis.pcall('EXPIRE', KEYS[1], 100)
                    redis.pcall('HSET', KEYS[2], 'f', 'v')
                    redis.pcall('HGET', KEYS[2], 'f')
                    return 1";

                using var redis = ConnectionMultiplexer.Connect(config);
                var db = redis.GetDatabase(0);
                var baseline = InfoStats(db);

                var limitedConfig = TestUtils.GetConfig(authUsername: "limited");
                limitedConfig.TieBreaker = "";
                using (var limitedRedis = ConnectionMultiplexer.Connect(limitedConfig))
                {
                    var limitedDb = limitedRedis.GetDatabase(0);
                    for (var i = 0; i < Calls; i++)
                        _ = limitedDb.ScriptEvaluate(Script, [(RedisKey)"stats-string", (RedisKey)"stats-hash"]);
                }

                foreach (var cmd in new[] { "set", "get", "expire", "hset", "hget" })
                {
                    var rejected = WaitForCommandStat(db, cmd, "rejected_calls", Calls);
                    ClassicAssert.AreEqual((ulong)Calls, rejected, $"{cmd} refused to a script was not counted as rejected");
                }

                // SET, EXPIRE and HSET write; GET and HGET read
                var delta = WaitForInfoStats(db, baseline, 3 * Calls);
                ClassicAssert.AreEqual((ulong)(3 * Calls), delta.Write, "Writes refused to scripts were not counted in the session counters");
                ClassicAssert.AreEqual((ulong)(2 * Calls), delta.Read, "Reads refused to scripts were not counted in the session counters");
                ClassicAssert.GreaterOrEqual(delta.Total, (ulong)(5 * Calls), "Commands refused to scripts were not counted in the session counters");
            }
            finally
            {
                File.Delete(aclFile);
            }
        }

        private static (ulong Total, ulong Write, ulong Read) InfoStats(IDatabase db)
        {
            var lines = db.Execute("INFO", "STATS").ToString().Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
            ulong Field(string name) => ulong.Parse(lines.Single(l => l.StartsWith($"{name}:"))[(name.Length + 1)..]);
            return (Field("total_commands_processed"), Field("total_write_commands_processed"), Field("total_read_commands_processed"));
        }

        /// <summary>
        /// Waits for the session counters, which a script's commands reach when the connection that ran the
        /// script ends, to account for at least the expected writes.
        /// </summary>
        private static (ulong Total, ulong Write, ulong Read) WaitForInfoStats(IDatabase db, (ulong Total, ulong Write, ulong Read) baseline, ulong expectedWrites)
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            (ulong Total, ulong Write, ulong Read) stats;
            while ((stats = InfoStats(db)).Write - baseline.Write < expectedWrites && DateTime.UtcNow < deadline)
                Thread.Sleep(100);

            return (stats.Total - baseline.Total, stats.Write - baseline.Write, stats.Read - baseline.Read);
        }

        private static ulong WaitForCommandStat(IDatabase db, string cmd, string field, int expected)
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            ulong value;
            while ((value = CommandStat(db, cmd, field)) < (ulong)expected && DateTime.UtcNow < deadline)
                Thread.Sleep(100);

            return value;
        }

        private static ulong CommandStat(IDatabase db, string cmd, string field)
        {
            var lines = db.Execute("INFO", "COMMANDSTATS").ToString().Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
            var line = lines.FirstOrDefault(l => l.StartsWith($"cmdstat_{cmd}:"));
            return line == null ? 0 : ParseCommandStatField(line, field);
        }

        [Test]
        public void CommandStatsFailedCallsTest()
        {
            StartServer();

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
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

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
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

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
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

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
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

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            IDatabase db = redis.GetDatabase(0);

            string infoResult = db.Execute("INFO", "SERVER").ToString();
            ClassicAssert.IsTrue(infoResult.Contains("commandstats_monitor:enabled"),
                "Expected commandstats_monitor:enabled in INFO SERVER");
        }

        [Test]
        public void CommandStatsFormatMatchesRedisConvention()
        {
            StartServer();

            using ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
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