// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// AOF round-trip tests for chunked records. A small AOF page size is used so that multi-MB keys/values/objects are written
    /// as multiple chunk records and must be reconstructed on recovery.
    /// </summary>
    [TestFixture]
    public class RespAofChunkTests : TestBase
    {
        GarnetServer server;

        // Per-test AOF topology / sizing (overridden by variants before they recreate the server).
        int replayTaskCount = 1;
        int aofPhysicalSublogCount = 1;
        string pageSize = "512k";
        string aofPageSize = "1m";
        string memorySize = "256m";
        string aofMemorySize = "64m";

        GarnetServer CreateServer(bool tryRecover)
            => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, tryRecover: tryRecover,
                memorySize: memorySize, pageSize: pageSize, aofPageSize: aofPageSize, aofMemorySize: aofMemorySize,
                replayTaskCount: replayTaskCount, aofPhysicalSublogCount: aofPhysicalSublogCount);

        [SetUp]
        public void Setup()
        {
            // Reset AOF topology/sizing to the default so a prior test's override does not leak into this one (NUnit reuses the
            // fixture instance across tests).
            replayTaskCount = 1;
            aofPhysicalSublogCount = 1;
            pageSize = "512k";
            aofPageSize = "1m";
            memorySize = "256m";
            aofMemorySize = "64m";
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = CreateServer(tryRecover: false);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        void RestartForRecovery()
        {
            server.Dispose(false);
            server = CreateServer(tryRecover: true);
            server.Start();
        }

        // Recreate the (empty) server with a larger page size (so the partialSlots split path can trigger); SetUp wrote nothing.
        void UsePageSizes(string page, string aofPage)
        {
            server.Dispose(false);
            pageSize = page;
            aofPageSize = aofPage;
            server = CreateServer(tryRecover: false);
            server.Start();
        }

        // Recreate the (empty) server with a different AOF topology; the SetUp server has not written anything yet.
        void UseTopology(int replayTasks, int sublogs)
        {
            server.Dispose(false);
            replayTaskCount = replayTasks;
            aofPhysicalSublogCount = sublogs;
            server = CreateServer(tryRecover: false);
            server.Start();
        }

        static string MakeValue(int size)
        {
            var sb = new StringBuilder(size);
            for (var i = 0; i < size; i++)
                sb.Append((char)('a' + (i % 26)));
            return sb.ToString();
        }

        [Test]
        public async Task AofLargeStringValueSpanChunkRecoverTest()
        {
            const string key = "bigstr";
            // > MinPartialAllocSize (1 MB) and > the 1 MB AOF page → written as multiple chunk records via the span path.
            var value = MakeValue(3 * 1024 * 1024);

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.StringSet(key, value);
                ClassicAssert.AreEqual(value, (string)db.StringGet(key));
            }

            _ = await server.Store.CommitAOFAsync(default);
            RestartForRecovery();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(value, (string)db.StringGet(key));
            }
        }

        [Test]
        public async Task AofLargeStringValueOverwriteSpanChunkRecoverTest()
        {
            const string key = "bigstr";
            var first = MakeValue(2 * 1024 * 1024);
            var second = MakeValue(4 * 1024 * 1024);

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.StringSet(key, first);
                db.StringSet(key, second); // overwrite with a different large value; only the latest should survive
                ClassicAssert.AreEqual(second, (string)db.StringGet(key));
            }

            _ = await server.Store.CommitAOFAsync(default);
            RestartForRecovery();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(second, (string)db.StringGet(key));
            }
        }

        [Test]
        public async Task AofLargeObjectChunkRecoverTest()
        {
            const string key1 = "bighash1";
            const string key2 = "bighash2";
            // ~2 MB hash across many large fields. A rename re-upserts the whole object (ObjectStoreUpsert) which, with the small
            // AOF page, is written as many chunk records (multiple value chunks) and reconstructed on recovery.
            var entries = Enumerable.Range(0, 128)
                .Select(i => new HashEntry("field-" + i, MakeValue(16 * 1024)))
                .ToArray();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                foreach (var e in entries)
                    db.HashSet(key1, e.Name, e.Value);
                ClassicAssert.IsTrue(db.KeyRename(key1, key2));
                ClassicAssert.AreEqual(entries.Length, db.HashLength(key2));
            }

            _ = await server.Store.CommitAOFAsync(default);
            RestartForRecovery();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                var recovered = db.HashGetAll(key2);
                ClassicAssert.AreEqual(entries.Length, recovered.Length);
                var map = recovered.ToDictionary(e => (string)e.Name, e => (string)e.Value);
                foreach (var e in entries)
                    ClassicAssert.AreEqual((string)e.Value, map[(string)e.Name]);
            }
        }

        // Deterministic field value so we can verify a huge object without holding every value in memory.
        static byte[] MakeFieldBytes(int fieldIndex, int size)
        {
            var v = new byte[size];
            var seed = (byte)(fieldIndex * 131 + 7);
            for (var j = 0; j < size; j++)
                v[j] = (byte)(seed + j);
            return v;
        }

        [Test]
        [Explicit("Heavy: builds up to a >6 GB object (needs ~20 GB RAM); run on demand. Exercises AOF object chunking across and past the 2 GB (int-overflow) boundary at several sizes.")]
        public async Task AofHugeObjectChunkRecoverTest(
            [Values(512L * 1024 * 1024, 1024L * 1024 * 1024, 2048L * 1024 * 1024, 6L * 1024 * 1024 * 1024)] long targetBytes)
        {
            // A hash of ~targetBytes exercises the AOF object-chunking length arithmetic at/around/beyond 2 GB. A rename
            // re-upserts the whole object (ObjectStoreUpsert) as one huge entry, chunked across pages and reconstructed on recovery.
            const int fieldSize = 16 * 1024 * 1024;              // 16 MB per field
            var fieldCount = (int)(targetBytes / fieldSize);
            const string key1 = "hugehash1";
            const string key2 = "hugehash2";

            // Generous main-store memory so the object stays resident; the main-store page and AOF in-memory region scale
            // with the object (pageSize = targetBytes/128, aofMemorySize = targetBytes/4) so a proportional number of pages
            // is exercised. Sizes are raw byte counts; the allocator rounds each down to the previous power of 2.
            server.Dispose(false);
            memorySize = "16g";
            pageSize = (targetBytes / 128).ToString();
            aofPageSize = "64m";
            aofMemorySize = (targetBytes / 4).ToString();
            server = CreateServer(tryRecover: false);
            server.Start();

            var config = TestUtils.GetConfig();
            config.SyncTimeout = (int)TimeSpan.FromMinutes(10).TotalMilliseconds;

            using (var redis = ConnectionMultiplexer.Connect(config))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < fieldCount; i++)
                    db.HashSet(key1, "f" + i, MakeFieldBytes(i, fieldSize));
                ClassicAssert.IsTrue(db.KeyRename(key1, key2));
                ClassicAssert.AreEqual(fieldCount, db.HashLength(key2));
            }

            _ = await server.Store.CommitAOFAsync(default);
            RestartForRecovery();

            using (var redis = ConnectionMultiplexer.Connect(config))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(fieldCount, db.HashLength(key2));
                for (var i = 0; i < fieldCount; i++)
                {
                    var got = (byte[])db.HashGet(key2, "f" + i);
                    ClassicAssert.AreEqual(MakeFieldBytes(i, fieldSize), got, $"field f{i}");
                }
            }
        }

        [Test]
        public async Task AofLargeSortedSetChunkRecoverTest()
        {
            const string key1 = "bigzset1";
            const string key2 = "bigzset2";
            var members = Enumerable.Range(0, 200)
                .Select(i => new SortedSetEntry(MakeValue(8 * 1024) + i, i))
                .ToArray();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                foreach (var m in members)
                    db.SortedSetAdd(key1, m.Element, m.Score);
                ClassicAssert.IsTrue(db.KeyRename(key1, key2));
            }

            _ = await server.Store.CommitAOFAsync(default);
            RestartForRecovery();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(members.Length, db.SortedSetLength(key2));
                var recovered = db.SortedSetRangeByRankWithScores(key2);
                var map = recovered.ToDictionary(e => (string)e.Element, e => e.Score);
                foreach (var m in members)
                    ClassicAssert.AreEqual(m.Score, map[(string)m.Element]);
            }
        }

        [Test]
        public async Task AofLargeKeyBrokenAcrossPagesRecoverTest()
        {
            // A ~2 MB key with a tiny value: the KEY alone exceeds the AOF page, so it is broken into multiple key chunks
            // spanning pages, then reassembled into a single OverflowByteArray-equivalent on recovery. This also exercises the
            // invariant that MinPartialAllocSize comfortably holds any Aof*ChunkHeader plus the key's length prefix.
            var key = "K:" + MakeValue(2 * 1024 * 1024);
            const string value = "small-value";

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.StringSet(key, value);
                ClassicAssert.AreEqual(value, (string)db.StringGet(key));
            }

            _ = await server.Store.CommitAOFAsync(default);
            RestartForRecovery();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(value, (string)db.StringGet(key));
            }
        }

        [Test]
        public async Task AofLargeInputRmwBrokenAcrossPagesRecoverTest()
        {
            // APPEND logs as StoreRMW, whose (large) payload rides entirely in the input currentComponent. A ~3 MB APPEND therefore
            // forces the INPUT to be split across multiple chunk records / pages, and the RMW is replayed from the reassembled input.
            const string key = "appendkey";
            var chunk = MakeValue(3 * 1024 * 1024);

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                _ = db.StringAppend(key, chunk);
                ClassicAssert.AreEqual(chunk, (string)db.StringGet(key));
            }

            _ = await server.Store.CommitAOFAsync(default);
            RestartForRecovery();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(chunk, (string)db.StringGet(key));
            }
        }

        [Test]
        public async Task AofLargeKeySmallInputInlineRmwRecoverTest()
        {
            // A ~2 MB key with a SMALL APPEND payload: the large key makes the record chunkable and span pages, while the small
            // input fits a single record and is written inline (no materialized input buffer). Recovery replays the RMW from the
            // reassembled key and the inline input. Complements AofLargeInputRmwBrokenAcrossPagesRecoverTest (which splits the input).
            var key = "IK:" + MakeValue(2 * 1024 * 1024);
            const string value = "small-appended-value";

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                _ = db.StringAppend(key, value);
                ClassicAssert.AreEqual(value, (string)db.StringGet(key));
            }

            _ = await server.Store.CommitAOFAsync(default);
            RestartForRecovery();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(value, (string)db.StringGet(key));
            }
        }

        [Test]
        public async Task AofInterleavedChunksFromMultipleRecordsRecoverTest()
        {
            // Multiple large SETs issued concurrently: their chunk records interleave in the shared AOF (each record's chunks are
            // allocated independently, so another writer can slot records in between). The reader groups chunks by objectId and
            // must reconstruct each record correctly despite interleaving.
            const int writers = 6;
            var expected = new string[writers];
            for (var i = 0; i < writers; i++)
                expected[i] = MakeValue(2 * 1024 * 1024) + i;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                var tasks = Enumerable.Range(0, writers)
                    .Select(i => db.StringSetAsync("ilkey" + i, expected[i]))
                    .ToArray();
                await Task.WhenAll(tasks);
            }

            _ = await server.Store.CommitAOFAsync(default);
            RestartForRecovery();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < writers; i++)
                    ClassicAssert.AreEqual(expected[i], (string)db.StringGet("ilkey" + i), $"key ilkey{i}");
            }
        }

        [Test]
        public async Task AofChunkedAndNonChunkedInterleavedRecoverTest()
        {
            // Interleave large (chunked) records with many small (single-record, non-chunked) records, concurrently. On recovery the
            // non-chunked records must replay as they arrive while the chunked records replay when their final chunk lands.
            const int bigWriters = 3;
            const int smallKeys = 50;
            var big = new string[bigWriters];
            for (var i = 0; i < bigWriters; i++)
                big[i] = MakeValue(2 * 1024 * 1024) + i;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                var tasks = new System.Collections.Generic.List<Task>();
                for (var i = 0; i < bigWriters; i++)
                    tasks.Add(db.StringSetAsync("big" + i, big[i]));
                for (var i = 0; i < smallKeys; i++)
                    tasks.Add(db.StringSetAsync("small" + i, "v" + i));
                await Task.WhenAll(tasks);
            }

            _ = await server.Store.CommitAOFAsync(default);
            RestartForRecovery();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < bigWriters; i++)
                    ClassicAssert.AreEqual(big[i], (string)db.StringGet("big" + i), $"big{i}");
                for (var i = 0; i < smallKeys; i++)
                    ClassicAssert.AreEqual("v" + i, (string)db.StringGet("small" + i), $"small{i}");
            }
        }

        [Test]
        public async Task AofChunkMultiReplayRecoverTest()
        {
            // Single physical log with multiple replay tasks: chunk records use the Basic chunk header but are replayed through the
            // multi-replay read path. Mix a large chunked value/object with small non-chunked records.
            UseTopology(replayTasks: 3, sublogs: 1);

            var big = MakeValue(3 * 1024 * 1024);
            var hash = Enumerable.Range(0, 64).Select(i => new HashEntry("f" + i, MakeValue(16 * 1024))).ToArray();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.StringSet("mrbig", big);
                db.StringSet("mrsmall", "v");
                foreach (var e in hash)
                    db.HashSet("mrhash1", e.Name, e.Value);
                ClassicAssert.IsTrue(db.KeyRename("mrhash1", "mrhash2"));
            }

            _ = await server.Store.CommitAOFAsync(default);
            RestartForRecovery();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(big, (string)db.StringGet("mrbig"));
                ClassicAssert.AreEqual("v", (string)db.StringGet("mrsmall"));
                ClassicAssert.AreEqual(hash.Length, db.HashLength("mrhash2"));
            }
        }

        [Test]
        public async Task AofChunkShardedRecoverTest()
        {
            // Multiple physical sublogs: chunk records use the Sharded chunk header (AofShardedChunkHeader), and different keys hash
            // to different sublogs. Several large string records are written concurrently and recovered across sublogs, exercising
            // the sharded chunk write header + the keyHash-based replay routing.
            // NOTE: standalone sharded OBJECT recovery is a pre-existing unsupported path (even small non-chunked objects fail to
            // recover in AofPhysicalSublogCount>1 standalone mode), so the object/sharded chunk path is left to the cluster harness.
            UseTopology(replayTasks: 1, sublogs: 2);

            const int writers = 6;
            var expected = new string[writers];
            for (var i = 0; i < writers; i++)
                expected[i] = MakeValue(2 * 1024 * 1024) + i;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                var tasks = Enumerable.Range(0, writers)
                    .Select(i => Task.Run(() => db.StringSet("shkey" + i, expected[i])))
                    .ToArray();
                await Task.WhenAll(tasks);
            }

            _ = await server.Store.CommitAOFAsync(default);
            RestartForRecovery();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < writers; i++)
                    ClassicAssert.AreEqual(expected[i], (string)db.StringGet("shkey" + i), $"shkey{i}");
            }
        }

        [Test]
        public async Task AofChunkShardedObjectRecoverTest()
        {
            // Sharded object chunking: a large hash re-upserted via rename (ObjectStoreUpsert) is written as AofShardedChunkHeader
            // value chunks into a sublog, then recovered. Exercises the sharded object-chunk path end-to-end.
            UseTopology(replayTasks: 1, sublogs: 2);

            var hash = Enumerable.Range(0, 64).Select(i => new HashEntry("f" + i, MakeValue(16 * 1024))).ToArray();
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                foreach (var e in hash)
                    db.HashSet("shhash1", e.Name, e.Value);
                ClassicAssert.IsTrue(db.KeyRename("shhash1", "shhash2"));
            }

            _ = await server.Store.CommitAOFAsync(default);
            RestartForRecovery();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(hash.Length, db.HashLength("shhash2"));
            }
        }

        [Test]
        public async Task AofChunkPartialSlotsPageSplitRecoverTest()
        {
            // Large AOF page (4 MB) so a chunk record can be split across a page boundary by partialSlots (MinPartialAllocSize =
            // 1 MB): after a ~2 MB value fills part of page 0, a ~3.5 MB value's first chunk record overflows the ~2 MB page tail
            // with a ~1.5 MB remainder — both sides >= 1 MB, so the allocator returns a partial (page-tail) allocation and the
            // record continues on page 1. Verifies the packed writer's partial-allocation handling round-trips.
            UsePageSizes(page: "2m", aofPage: "4m");

            var a = MakeValue(2 * 1024 * 1024);
            var b = MakeValue(7 * 1024 * 1024 / 2); // 3.5 MB

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.StringSet("splitA", a);
                db.StringSet("splitB", b);
                ClassicAssert.AreEqual(a, (string)db.StringGet("splitA"));
                ClassicAssert.AreEqual(b, (string)db.StringGet("splitB"));
            }

            _ = await server.Store.CommitAOFAsync(default);
            RestartForRecovery();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(a, (string)db.StringGet("splitA"));
                ClassicAssert.AreEqual(b, (string)db.StringGet("splitB"));
            }
        }
    }
}