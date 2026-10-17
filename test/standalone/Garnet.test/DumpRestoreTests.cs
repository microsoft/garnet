// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class DumpRestoreTests : TestBase
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
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            TestUtils.OnTearDown();
        }

        public static IEnumerable<TestCaseData> RedisFixtures()
        {
            using var stream = typeof(DumpRestoreTests).Assembly.GetManifestResourceStream("Garnet.test.DumpRestoreRedis72.json");
            using var document = JsonDocument.Parse(stream);
            foreach (var fixture in document.RootElement.GetProperty("fixtures").EnumerateArray())
            {
                var contents = fixture.GetProperty("contents").EnumerateArray()
                    .Select(x => Convert.FromHexString(x.GetString())).ToArray();
                yield return new TestCaseData(fixture.GetProperty("type").GetString(),
                    Convert.FromHexString(fixture.GetProperty("dump").GetString()), contents)
                    .SetName("RestoreRedis72_" + fixture.GetProperty("name").GetString());
            }
        }

        [TestCaseSource(nameof(RedisFixtures))]
        public void RestoreRedisFixtureAndRoundTrip(string type, byte[] dump, byte[][] expected)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            db.KeyRestore("source", dump);
            ClassicAssert.AreEqual(type, db.Execute("TYPE", "source").ToString());
            AssertContents(db, "source", type, expected);

            var exported = db.KeyDump("source");
            ClassicAssert.AreEqual(type switch { "string" => 0, "list" => 1, "set" => 2, "hash" => 4, _ => 5 }, exported[0]);
            db.KeyRestore("destination", exported);
            AssertContents(db, "destination", type, expected);
        }

        static void AssertContents(IDatabase db, string key, string type, byte[][] expected)
        {
            byte[][] actual = type switch
            {
                "string" => [(byte[])db.StringGet(key)],
                "list" => db.ListRange(key).Select(x => (byte[])x).ToArray(),
                "set" => db.SetMembers(key).Select(x => (byte[])x).ToArray(),
                "hash" => db.HashGetAll(key).SelectMany(x => new[] { (byte[])x.Name, (byte[])x.Value }).ToArray(),
                _ => db.SortedSetRangeByRankWithScores(key).SelectMany(x => new[]
                {
                    (byte[])x.Element, System.Text.Encoding.ASCII.GetBytes(x.Score.ToString("G", System.Globalization.CultureInfo.InvariantCulture))
                }).ToArray()
            };
            if (type == "set")
            {
                CollectionAssert.AreEquivalent(expected, actual);
                return;
            }
            if (type == "hash")
            {
                CollectionAssert.AreEquivalent(expected.Chunk(2).Select(x => Convert.ToHexString(x[0]) + ":" + Convert.ToHexString(x[1])),
                    actual.Chunk(2).Select(x => Convert.ToHexString(x[0]) + ":" + Convert.ToHexString(x[1])));
                return;
            }
            if (type == "zset")
            {
                var values = db.SortedSetRangeByRankWithScores(key);
                ClassicAssert.AreEqual(expected.Length / 2, values.Length);
                for (var i = 0; i < values.Length; i++)
                {
                    CollectionAssert.AreEqual(expected[2 * i], (byte[])values[i].Element);
                    var text = System.Text.Encoding.ASCII.GetString(expected[2 * i + 1]);
                    var score = text == "inf" ? double.PositiveInfinity : text == "-inf" ? double.NegativeInfinity :
                        double.Parse(text, System.Globalization.CultureInfo.InvariantCulture);
                    ClassicAssert.AreEqual(score, values[i].Score);
                }
                return;
            }
            CollectionAssert.AreEqual(expected, actual);
        }

        [TestCase("string")]
        [TestCase("list")]
        [TestCase("set")]
        [TestCase("hash")]
        [TestCase("zset")]
        public void RestorePreservesMillisecondsAndRejectsEveryOccupiedType(string type)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            CreateValue(db, "source", type);
            var dump = db.KeyDump("source");
            db.KeyRestore("timed", dump, TimeSpan.FromMilliseconds(1500));
            var ttl = (long)db.Execute("PTTL", "timed");
            ClassicAssert.That(ttl, Is.InRange(1, 1500));
            db.Execute("RESTORE", "long-ttl", 3_000_000_000L, dump);
            ClassicAssert.That((long)db.Execute("PTTL", "long-ttl"), Is.InRange(2_999_990_000L, 3_000_000_000L));

            foreach (var destinationType in new[] { "string", "list", "set", "hash", "zset" })
            {
                db.KeyDelete("occupied");
                CreateValue(db, "occupied", destinationType);
                db.KeyExpire("occupied", TimeSpan.FromMinutes(1));
                var original = db.KeyDump("occupied");
                var ex = Assert.Throws<RedisServerException>(() => db.KeyRestore("occupied", dump));
                ClassicAssert.That(ex.Message, Does.StartWith("BUSYKEY"));
                CollectionAssert.AreEqual(original, db.KeyDump("occupied"));
                ClassicAssert.That((long)db.Execute("PTTL", "occupied"), Is.InRange(1, 60_000));
            }
            ClassicAssert.AreEqual("PONG", db.Execute("PING").ToString());
        }

        static void CreateValue(IDatabase db, string key, string type)
        {
            switch (type)
            {
                case "string": db.StringSet(key, "value"); break;
                case "list": db.ListRightPush(key, ["one", "two", "one"]); break;
                case "set": db.SetAdd(key, ["one", "two"]); break;
                case "hash": db.HashSet(key, [new HashEntry("field", "value")]); break;
                case "zset": db.SortedSetAdd(key, [new SortedSetEntry("one", -0.5), new SortedSetEntry("two", 3)]); break;
            }
        }

        static byte[] Envelope(params byte[] body)
        {
            byte[] checkedBytes = [.. body, 11, 0];
            return [.. checkedBytes, .. Crc64.Hash(checkedBytes)];
        }

        public static IEnumerable<TestCaseData> InvalidPayloads()
        {
            for (var i = 0; i < 12; i++)
                yield return new TestCaseData(new byte[i]).SetName("RestoreShortPayload_" + i);
            byte[][] bodies =
            [
                [0, 4, 1], [0, 0, 99], [0, 0x80, 0, 0], [0, 0x82, 0, 0, 0, 0],
                [0, 0x81, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
                [0, 0xc3, 1, 10, 0], [0, 0xc3, 2, 5, 0xe0, 0],
                [1, 0], [2, 2, 1, 97, 1, 97], [4, 2, 1, 97, 0, 1, 97, 0],
                [5, 1, 0, 0, 0, 0, 0, 0, 0, 0xf8, 0x7f],
                [18, 1, 3, 0], [18, 1, 2, 7, 7, 0, 0, 0, 0, 0, 255],
                [20, 11, 11, 0, 0, 0, 2, 0, 1, 2, 2, 1, 255],
                [11, 10, 2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 99],
                [11, 12, 2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 1, 0],
                [99, 0], [9, 0], [14, 0], [3, 0]
            ];
            for (var i = 0; i < bodies.Length; i++)
                yield return new TestCaseData(Envelope(bodies[i])).SetName("RestoreMalformedBody_" + i);
            var corrupted = Envelope(0, 1, 97);
            corrupted[^1] ^= 1;
            yield return new TestCaseData(corrupted).SetName("RestoreBadChecksum");
            var newVersion = Envelope(0, 1, 97);
            newVersion[^10] = 12;
            yield return new TestCaseData(newVersion).SetName("RestoreNewVersion");
        }

        [TestCaseSource(nameof(InvalidPayloads))]
        public void MalformedRestoreDoesNotMutateAndConnectionRemainsUsable(byte[] payload)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            db.StringSet("occupied", "original", TimeSpan.FromMinutes(1));
            Assert.Throws<RedisServerException>(() => db.KeyRestore("absent", payload));
            ClassicAssert.IsFalse(db.KeyExists("absent"));
            Assert.Throws<RedisServerException>(() => db.KeyRestore("occupied", payload));
            ClassicAssert.AreEqual("original", db.StringGet("occupied").ToString());
            ClassicAssert.That((long)db.Execute("PTTL", "occupied"), Is.InRange(1, 60_000));
            ClassicAssert.AreEqual("PONG", db.Execute("PING").ToString());
        }

        [TestCase(-1L)]
        [TestCase(long.MaxValue)]
        public void InvalidTtlDoesNotCreateKey(long ttl)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            Assert.Throws<RedisServerException>(() => db.Execute("RESTORE", "key", ttl, Envelope(0, 0)));
            ClassicAssert.IsFalse(db.KeyExists("key"));
        }

        [Test]
        public void RejectsDecodedSizeLimitBeforeDecompression()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            var payload = Envelope(0, 0xc3, 2, 0x80, 0x20, 0, 0, 1, 0, 97);
            var error = Assert.Throws<RedisServerException>(() => db.KeyRestore("key", payload));
            ClassicAssert.That(error.Message, Does.Contain("size limit"));
            ClassicAssert.IsFalse(db.KeyExists("key"));
        }

        [Test]
        public void RestoreAndDumpWithinScript()
        {
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableLua: true);
            server.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            var payload = Envelope(2, 1, 1, 97);
            var result = (byte[])db.ScriptEvaluate(
                "redis.call('RESTORE', KEYS[1], 0, ARGV[1]); return redis.call('DUMP', KEYS[1])",
                ["destination"], [payload]);
            CollectionAssert.AreEqual(payload, result);
        }

        [Test]
        public void Restore64BitLengthAndQuicklistPlainNodes()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            db.KeyRestore("wide", Envelope(0, 0x81, 0, 0, 0, 0, 0, 0, 0, 1, 97));
            ClassicAssert.AreEqual("a", db.StringGet("wide").ToString());
            db.KeyRestore("list", Envelope(18, 2, 1, 1, 97, 1, 1, 98));
            CollectionAssert.AreEqual(new RedisValue[] { "a", "b" }, db.ListRange("list"));
        }

        [TestCase(0)]
        [TestCase(63)]
        [TestCase(64)]
        [TestCase(16_383)]
        [TestCase(16_384)]
        [TestCase(16_777_216)]
        public void BinaryStringLengthBoundaries(int length)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            var value = new byte[length];
            new Random(1343).NextBytes(value);
            db.StringSet("source", value);
            db.KeyRestore("destination", db.KeyDump("source"));
            CollectionAssert.AreEqual(value, (byte[])db.StringGet("destination"));
        }

        [TestCase(64)]
        [TestCase(16_384)]
        public void CollectionCountLengthBoundaries(int count)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            db.SetAdd("source", Enumerable.Range(0, count).Select(x => (RedisValue)x).ToArray());
            db.KeyRestore("destination", db.KeyDump("source"));
            CollectionAssert.AreEquivalent(db.SetMembers("source"), db.SetMembers("destination"));
        }

        [TestCase("hash")]
        [TestCase("zset")]
        public async Task DumpRejectsLiveItemExpirationAndOmitsExpiredItems(string type)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            var command = type == "hash" ? "HPEXPIRE" : "ZPEXPIRE";
            var itemArgument = type == "hash" ? "FIELDS" : "MEMBERS";
            if (type == "hash")
                db.HashSet("source", [new HashEntry("expiring", "value"), new HashEntry("persistent", "other")]);
            else
                db.SortedSetAdd("source", [new SortedSetEntry("expiring", 1), new SortedSetEntry("persistent", 2)]);

            db.Execute(command, "source", 60_000, itemArgument, 1, "expiring");
            var error = Assert.Throws<RedisServerException>(() => db.KeyDump("source"));
            ClassicAssert.That(error.Message, Does.Contain("live item expiration"));
            db.Execute(command, "source", 1, itemArgument, 1, "expiring");
            await Task.Delay(50);
            db.KeyRestore("destination", db.KeyDump("source"));
            if (type == "hash")
                CollectionAssert.AreEqual(new[] { new HashEntry("persistent", "other") }, db.HashGetAll("destination"));
            else
                CollectionAssert.AreEqual(new[] { new SortedSetEntry("persistent", 2) }, db.SortedSetRangeByRankWithScores("destination"));

            db.Execute(command, "source", 1, itemArgument, 1, "persistent");
            await Task.Delay(50);
            ClassicAssert.IsNull(db.KeyDump("source"));
        }

        [Test]
        public void DumpAndRejectedRestoreDoNotInvalidateWatchButSuccessfulRestoreDoes()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            db.SetAdd("source", "member");
            using var watcher = TestUtils.CreateRequest();
            watcher.SendCommand("WATCH source");
            var dump = db.KeyDump("source");
            Assert.Throws<RedisServerException>(() => db.KeyRestore("source", dump));
            watcher.SendCommand("MULTI");
            watcher.SendCommand("SET witness value");
            TestUtils.AssertEqualUpToExpectedLength("*1\r\n+OK\r\n", watcher.SendCommand("EXEC", 2));

            watcher.SendCommand("WATCH destination");
            db.KeyRestore("destination", dump);
            watcher.SendCommand("MULTI");
            watcher.SendCommand("SET witness changed");
            TestUtils.AssertEqualUpToExpectedLength("*-1\r\n", watcher.SendCommand("EXEC"));
            ClassicAssert.AreEqual("value", db.StringGet("witness").ToString());
        }

        [Test]
        public async Task DumpCapturesCoherentSnapshotDuringWrites()
        {
            using var reader = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var writer = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = reader.GetDatabase();
            db.ListRightPush("source", ["a", "b", "c"]);
            using var stop = new CancellationTokenSource();
            var writes = Task.Run(async () =>
            {
                while (!stop.IsCancellationRequested)
                    await writer.GetDatabase().ListRightPopLeftPushAsync("source", "source");
            });
            try
            {
                for (var i = 0; i < 100; i++)
                {
                    var dump = await db.KeyDumpAsync("source");
                    await db.KeyRestoreAsync("snapshot", dump);
                    var contents = string.Concat((await db.ListRangeAsync("snapshot")).Select(x => x.ToString()));
                    ClassicAssert.That(contents, Is.EqualTo("abc").Or.EqualTo("cab").Or.EqualTo("bca"));
                    await db.KeyDeleteAsync("snapshot");
                }
            }
            finally
            {
                stop.Cancel();
                await writes;
            }
        }

        [Test]
        public async Task ConcurrentRestoresOnlyPublishOneValue()
        {
            using var first = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var second = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var payload = Envelope(2, 1, 1, 97);
            var attempts = new[] { first, second }.Select(async connection =>
            {
                try
                {
                    await connection.GetDatabase().KeyRestoreAsync("destination", payload);
                    return true;
                }
                catch (RedisServerException ex) when (ex.Message.StartsWith("BUSYKEY", StringComparison.Ordinal))
                {
                    return false;
                }
            });
            ClassicAssert.AreEqual(1, (await Task.WhenAll(attempts)).Count(x => x));
            ClassicAssert.AreEqual(1, first.GetDatabase().SetLength("destination"));
        }

        [Test]
        public async Task RestoreAndDumpInsideTransaction()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            var transaction = db.CreateTransaction();
            var restore = transaction.KeyRestoreAsync("destination", Envelope(2, 1, 1, 97));
            var dump = transaction.KeyDumpAsync("destination");
            ClassicAssert.IsTrue(await transaction.ExecuteAsync());
            await restore;
            CollectionAssert.AreEqual(Envelope(2, 1, 1, 97), await dump);
        }

        [Test]
        public void ObjectDisabledStillRestoresStrings()
        {
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: true);
            server.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            db.KeyRestore("string", Envelope(0, 1, 97));
            ClassicAssert.AreEqual("a", db.StringGet("string").ToString());
            var error = Assert.Throws<RedisServerException>(() => db.KeyRestore("object", Envelope(2, 1, 1, 97)));
            ClassicAssert.That(error.Message, Does.Contain("Object store is disabled"));
            ClassicAssert.IsFalse(db.KeyExists("object"));
        }

        [TestCase(1, false)]
        [TestCase(2, false)]
        [TestCase(1, true)]
        [TestCase(2, true)]
        public void RestoredValuesAndAbsoluteExpirationSurviveAofRecovery(int sublogs, bool checkpoint)
        {
            server.Dispose();
            server = CreateAofServer(sublogs, false);
            server.Start();
            var deadlines = new Dictionary<string, long>();
            var dumps = new Dictionary<string, byte[]>();
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase();
                foreach (var type in new[] { "string", "list", "set", "hash", "zset" })
                {
                    CreateValue(db, "source", type);
                    dumps[type] = db.KeyDump("source");
                    db.KeyDelete("source");
                    db.KeyRestore(type, dumps[type], TimeSpan.FromMinutes(1));
                    deadlines[type] = (long)db.Execute("PEXPIRETIME", type);
                    db.KeyRestore(type + "-persistent", dumps[type]);
                }
                if (checkpoint)
                    db.Execute("SAVE");
                db.StringSet("after-checkpoint", "tail");
                db.Execute("COMMITAOF");
            }
            server.Dispose(false);
            server = CreateAofServer(sublogs, true);
            server.Start();
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase();
                foreach (var type in dumps.Keys)
                {
                    CollectionAssert.AreEqual(dumps[type], db.KeyDump(type));
                    ClassicAssert.AreEqual(deadlines[type], (long)db.Execute("PEXPIRETIME", type));
                    CollectionAssert.AreEqual(dumps[type], db.KeyDump(type + "-persistent"));
                    ClassicAssert.AreEqual(-1, (long)db.Execute("PTTL", type + "-persistent"));
                }
                ClassicAssert.AreEqual("tail", db.StringGet("after-checkpoint").ToString());
            }
        }

        [Test]
        public async Task LargeObjectRestoreSurvivesChunkedAofAndDelayedExpiration()
        {
            server.Dispose();
            server = CreateAofServer(2, false);
            server.Start();
            var largeValue = new byte[2 * 1024 * 1024];
            new Random(1343).NextBytes(largeValue);
            long deadline;
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase();
                db.HashSet("source", "field", largeValue);
                var dump = db.KeyDump("source");
                db.KeyRestore("large", dump, TimeSpan.FromMinutes(1));
                deadline = (long)db.Execute("PEXPIRETIME", "large");
                db.KeyRestore("expired", dump, TimeSpan.FromMilliseconds(1));
                db.KeyDelete("source");
                db.Execute("COMMITAOF");
            }
            server.Dispose(false);
            await Task.Delay(50);
            server = CreateAofServer(2, true);
            server.Start();
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase();
                CollectionAssert.AreEqual(largeValue, (byte[])db.HashGet("large", "field"));
                ClassicAssert.AreEqual(deadline, (long)db.Execute("PEXPIRETIME", "large"));
                ClassicAssert.IsFalse(db.KeyExists("expired"));
            }
        }

        [Test]
        public void DumpAndRestoreHandleEvictedRecordsAndExpiredDestinations()
        {
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true,
                memorySize: "64m", pageSize: "512k");
            server.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();
            foreach (var type in new[] { "string", "list", "set", "hash", "zset" })
                CreateValue(db, type, type);
            var log = server.Provider.StoreWrapper.store.Log;
            log.FlushAndEvict(wait: true);
            foreach (var type in new[] { "string", "list", "set", "hash", "zset" })
            {
                var dump = db.KeyDump(type);
                db.KeyRestore(type + "-restored", dump);
                CollectionAssert.AreEqual(dump, db.KeyDump(type + "-restored"));
            }

            db.StringSet("expired-destination", "old");
            db.KeyExpire("expired-destination", DateTime.UtcNow.AddSeconds(-1));
            log.FlushAndEvict(wait: true);
            db.KeyRestore("expired-destination", db.KeyDump("list"));
            ClassicAssert.AreEqual(RedisType.List, db.KeyType("expired-destination"));
            ClassicAssert.AreEqual(-1, (long)db.Execute("PTTL", "expired-destination"));

            var stringDump = db.KeyDump("string");
            foreach (var type in new[] { "list", "set", "hash", "zset" })
            {
                CreateValue(db, type + "-expired", type);
                db.KeyExpire(type + "-expired", TimeSpan.FromMilliseconds(50));
            }
            Thread.Sleep(100);
            log.FlushAndEvict(wait: true);
            foreach (var type in new[] { "list", "set", "hash", "zset" })
            {
                db.KeyRestore(type + "-expired", stringDump);
                CollectionAssert.AreEqual(stringDump, db.KeyDump(type + "-expired"));
            }
        }

        GarnetServer CreateAofServer(int sublogs, bool recover)
        {
            var options = TestUtils.GetGarnetServerOptions(checkpointDir: TestUtils.MethodTestDir,
                logDir: TestUtils.MethodTestDir, endpoint: TestUtils.EndPoint, enableCluster: false, enableAOF: true,
                tryRecover: recover, sublogCount: sublogs, aofPageSize: "1m", replayTaskCount: 2,
                lowMemory: true, pageSize: "512k", memorySize: "128m");
            options.FailOnRecoveryError = true;
            return new GarnetServer(options);
        }
    }
}