// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Streams are an ordinary per-database key type, so the same key in different databases must be
    /// fully isolated and FLUSHDB must be scoped to the current database. These tests use on-disk
    /// streams (streamLogDir set) so the per-database log-directory isolation is exercised, not just
    /// the per-database store record.
    /// </summary>
    [TestFixture]
    public class RespStreamMultiDbTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            // On-disk streams so per-database directory namespacing is exercised; default MaxDatabases=16.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                streamLogDir: Path.Combine(TestUtils.MethodTestDir, "streams"));
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public async Task StreamsAreIsolatedPerDatabase()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            await c.ExecuteAsync("SELECT", "0");
            await c.ExecuteAsync("XADD", "s", "*", "db", "zero");
            await c.ExecuteAsync("SELECT", "1");
            await c.ExecuteAsync("XADD", "s", "*", "db", "one");

            // DB 1 sees only its own entry.
            ClassicAssert.AreEqual("1", await c.ExecuteAsync("XLEN", "s"));
            var range1 = await c.ExecuteForArrayAsync("XRANGE", "s", "-", "+");
            ClassicAssert.IsTrue(string.Join(",", range1).Contains("one"));
            ClassicAssert.IsFalse(string.Join(",", range1).Contains("zero"));

            // DB 0 sees only its own entry — same key "s", fully independent.
            await c.ExecuteAsync("SELECT", "0");
            ClassicAssert.AreEqual("1", await c.ExecuteAsync("XLEN", "s"));
            var range0 = await c.ExecuteForArrayAsync("XRANGE", "s", "-", "+");
            ClassicAssert.IsTrue(string.Join(",", range0).Contains("zero"));
            ClassicAssert.IsFalse(string.Join(",", range0).Contains("one"));
        }

        [Test]
        public async Task FlushDbScopedToCurrentDatabase()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            await c.ExecuteAsync("XADD", "s", "*", "f", "v");   // DB 0
            await c.ExecuteAsync("SELECT", "1");
            await c.ExecuteAsync("XADD", "s", "*", "f", "v");   // DB 1
            await c.ExecuteAsync("FLUSHDB");                    // flush DB 1 only

            ClassicAssert.AreEqual("0", await c.ExecuteAsync("XLEN", "s")); // DB 1 wiped
            await c.ExecuteAsync("SELECT", "0");
            ClassicAssert.AreEqual("1", await c.ExecuteAsync("XLEN", "s")); // DB 0 survives
        }

        [Test]
        public async Task FlushAllClearsStreamsInAllDatabases()
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            await c.ExecuteAsync("XADD", "s", "*", "f", "v");   // DB 0
            await c.ExecuteAsync("SELECT", "1");
            await c.ExecuteAsync("XADD", "s", "*", "f", "v");   // DB 1
            await c.ExecuteAsync("FLUSHALL");

            ClassicAssert.AreEqual("0", await c.ExecuteAsync("XLEN", "s")); // DB 1 wiped
            await c.ExecuteAsync("SELECT", "0");
            ClassicAssert.AreEqual("0", await c.ExecuteAsync("XLEN", "s")); // DB 0 wiped
        }

        [Test]
        [Category("Persistence")]
        public void StreamRecoversUnderItsOwnDatabase()
        {
            // Control the server lifecycle for SAVE + restart; tear down the auto-setup server first.
            var saveDir = Path.Combine(TestUtils.MethodTestDir, "streams");
            server.Dispose();
            server = null; // owns its own server lifecycle below; avoid the TearDown double-dispose
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);

            const string key = "persist";

            using (var first = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, streamLogDir: saveDir))
            {
                first.Start();
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                // Same key in DB 0 and DB 1 with different contents.
                redis.GetDatabase(0).StreamAdd(key, "f", "db0", "1-0");
                var db1 = redis.GetDatabase(1);
                for (var i = 0; i < 5; i++)
                    db1.StreamAdd(key, $"f{i}", $"v{i}", $"{i + 1}-0");

                redis.GetServers()[0].Execute("SAVE"); // checkpoints all active databases
                System.Threading.Thread.Sleep(500);    // let the fire-and-forget stream commit drain
            }

            using (var second = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, streamLogDir: saveDir, tryRecover: true))
            {
                second.Start();
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                ClassicAssert.AreEqual(5, redis.GetDatabase(1).StreamLength(key),
                    "DB 1 stream must recover under DB 1");
                ClassicAssert.AreEqual(1, redis.GetDatabase(0).StreamLength(key),
                    "DB 0 stream (same key) must recover independently");
            }
        }

        [Test]
        [Category("Persistence")]
        public void DiskStreamsSurviveEvictionChurn()
        {
            // Many disk-backed streams under low memory: the object store evicts cold StreamObjects,
            // and OnEvict closes each evicted stream's per-stream log/device handle. Re-reading every
            // stream must transparently re-open it with all entries intact — proving the
            // evict -> dispose -> re-deserialize -> re-open cycle is lossless and has no use-after-dispose.
            var saveDir = Path.Combine(TestUtils.MethodTestDir, "streams");
            server.Dispose();
            server = null; // owns its own server lifecycle below
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);

            const int streamCount = 400;       // < LightEpoch.MaxInstances (1024) so the cap is never the cause
            const int entriesPerStream = 3;

            using var evictServer = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true,
                streamLogDir: saveDir);
            evictServer.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            for (var i = 0; i < streamCount; i++)
                for (var j = 0; j < entriesPerStream; j++)
                    db.StreamAdd($"s{i}", $"f{j}", $"v{j}", $"{j + 1}-0");

            // If OnEvict did not dispose evicted streams, all streamCount per-stream LightEpoch
            // instances would still be live. Under low memory the object store evicts cold streams,
            // so the live epoch count must be well below the total — proof that OnEvict fired and
            // released handles/epochs (also keeps us clear of LightEpoch.MaxInstances).
            var liveEpochsAfterCreate = Tsavorite.core.LightEpoch.ActiveInstanceCount();
            ClassicAssert.Less(liveEpochsAfterCreate, streamCount,
                "Eviction should have disposed cold streams (freeing their LightEpoch)");

            for (var i = 0; i < streamCount; i++)
            {
                ClassicAssert.AreEqual(entriesPerStream, db.StreamLength($"s{i}"),
                    $"stream s{i} length must survive eviction churn");
                var range = db.StreamRange($"s{i}", "-", "+");
                ClassicAssert.AreEqual(entriesPerStream, range.Length, $"stream s{i} entries must be intact");
            }
        }
    }
}