// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture]
    public class RespLowMemoryTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        void MakeReadOnly(long untilAddress, IServer server, IDatabase db)
        {
            var i = 1000;
            var info = TestUtils.GetStoreAddressInfo(server);

            // Add keys so that the first record enters the read-only region
            // Each record is 40 bytes here, because they do not have expirations
            while (info.ReadOnlyAddress < untilAddress)
            {
                var key = $"key{i++:00000}";
                _ = db.StringSet(key, key);
                info = TestUtils.GetStoreAddressInfo(server);
            }
        }

        [Test]
        public void PersistCopyUpdateTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.EndPoint);
            var info = TestUtils.GetStoreAddressInfo(server);

            // Start at tail address of PageHeader.Size (64)
            ClassicAssert.AreEqual(PageHeader.Size, info.TailAddress);

            var expire = 100;
            var key0 = $"key{0:00000}";
            _ = db.StringSet(key0, key0, TimeSpan.FromSeconds(expire));

            // Record size for key0 is RecordInfo.Size (8) + MinLengthMetadataBytes (5) + 2 * 8 bytes (key and value) + 8 bytes expiry = 13 + 24 = 37 bytes rounded up to 40
            // so the new tail address should be 64 + 40 = 104 (that is, the record for key0 is located at [64, 104)).
            info = TestUtils.GetStoreAddressInfo(server);
            ClassicAssert.AreEqual(104, info.TailAddress);

            // Make the record read-only by adding more records
            MakeReadOnly(info.TailAddress, server, db);

            info = TestUtils.GetStoreAddressInfo(server);
            var previousTail = info.TailAddress;

            // The first record inserted (key0) is now read-only
            ClassicAssert.IsTrue(info.ReadOnlyAddress >= 104);

            // Persist the key, which should cause RMW to CopyUpdate to tail
            var response = db.KeyPersist(key0);
            ClassicAssert.IsTrue(response);

            // Now key0 is only 32 bytes, as we are removing the expiration
            // That is, key0 is now moved to [previousTail, previousTail + 32)
            info = TestUtils.GetStoreAddressInfo(server);
            ClassicAssert.AreEqual(previousTail + 32, info.TailAddress);

            // Verify that key0 exists with correct value
            ClassicAssert.AreEqual(key0, (string)db.StringGet(key0));
        }

        [Test]
        public void StringUpdateOfExpiredReadOnlyObjectKeyCreatesNewKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.EndPoint);

            var key0 = "key00000";
            ClassicAssert.IsTrue(db.HashSet(key0, "field", "value"));
            ClassicAssert.IsTrue(db.KeyExpire(key0, TimeSpan.FromMilliseconds(200)));

            MakeReadOnly(TestUtils.GetStoreAddressInfo(server).TailAddress, server, db);
            while (db.KeyExists(key0))
                Thread.Sleep(10);

            // The expired hash is read-only, so INCR goes through copy-update, which must treat it as absent rather than as a hash.
            ClassicAssert.AreEqual(1, db.StringIncrement(key0));
            ClassicAssert.AreEqual("1", (string)db.StringGet(key0));
        }

        /// <summary>
        /// Writes a record of <paramref name="kind"/> at <paramref name="key"/>: text, a number, a number with a leading zero,
        /// text that expires in an hour, text with an ETag, a hash, or text or a hash that has already expired.
        /// </summary>
        static void WriteRecord(IDatabase db, string kind, string key)
        {
            if (kind == "gonehash")
            {
                WriteRecord(db, "hash", key);
                ClassicAssert.AreEqual(1, (int)db.Execute("PEXPIRE", key, "1"));
                return;
            }

            if (kind == "ttlhash")
            {
                WriteRecord(db, "hash", key);
                ClassicAssert.AreEqual(1, (int)db.Execute("EXPIRE", key, "3600"));
                return;
            }

            var reply = kind switch
            {
                "text" => db.Execute("SET", key, "garnet-string-value"),
                "number" => db.Execute("SET", key, "10"),
                "leadingzero" => db.Execute("SET", key, "01.5"),
                "ttl" => db.Execute("SET", key, "garnet-string-value", "EX", "3600"),
                "etag" => db.Execute("SETWITHETAG", key, "garnet-string-value"),
                "hash" => db.Execute("HSET", key, "f", "v"),
                "gone" => db.Execute("SET", key, "garnet-string-value", "PX", "1"),
                _ => throw new ArgumentOutOfRangeException(nameof(kind)),
            };
            ClassicAssert.IsFalse(reply.IsNull, $"Writing a {kind} record failed");
        }

        /// <summary>Renders a reply, or the error it raised, so that replies can be compared.</summary>
        static string Render(Func<RedisResult> execute)
        {
            try
            {
                return Render(execute());
            }
            catch (RedisServerException e)
            {
                return "-" + e.Message;
            }
        }

        static string Render(RedisResult result)
            => result.IsNull ? "(nil)" : result.Resp2Type == ResultType.Array ? "[" + string.Join(",", Array.ConvertAll((RedisResult[])result, Render)) + "]" : result.ToString();

        /// <summary>
        /// A string, object or key command answers, and leaves its record, the same whether the record is in the mutable
        /// region, where it is updated in place, or in the read-only region or on disk, where it is copied to the tail.
        /// </summary>
        /// <remarks>
        /// The reference is the same command on an identical record in the mutable region, or, for a record that has
        /// expired, on a key that was never written. DEL of a live record is omitted because its count is approximate
        /// outside the mutable region, as are non-ETag updates of an ETag record, whose ETag is undefined.
        /// </remarks>
        [Test]
        public void UpdatesAnswerTheSameWhereverTheRecordIs()
        {
            RestartWithALargeReadOnlyRegion();

            var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.EndPoint);
            var payload = (byte[])db.Execute("DUMP", (RedisKey)WriteDumpSource(db));

            (string kind, object[] command)[] cases =
            [
                ("text", ["GETEX"]),
                ("text", ["GETEX", "EX", "100"]),
                ("ttl", ["GETEX", "PERSIST"]),
                ("text", ["GETDEL"]),
                ("text", ["SETNX", "x"]),
                ("text", ["SET", "x", "NX"]),
                ("text", ["SET", "x", "XX"]),
                ("text", ["SET", "x", "GET"]),
                ("text", ["SET", "x", "XX", "GET"]),
                ("text", ["SET", "x", "EX", "100", "XX"]),
                ("ttl", ["SET", "x", "KEEPTTL"]),
                ("ttl", ["SET", "x", "XX", "KEEPTTL"]),
                ("text", ["GETSET", "x"]),
                ("text", ["APPEND", "-more"]),
                ("text", ["SETRANGE", "2", "xyz"]),
                ("text", ["SETRANGE", "30", "x"]),
                ("text", ["SETBIT", "7", "1"]),
                ("text", ["INCR"]),
                ("text", ["DECR"]),
                ("text", ["INCRBY", "5"]),
                ("text", ["INCRBYFLOAT", "1.5"]),
                ("text", ["RESTORE", "0", payload]),
                ("text", ["SETWITHETAG", "x"]),
                ("text", ["DELIFGREATER", "5"]),
                ("text", ["EXPIRE", "100"]),
                ("text", ["PERSIST"]),
                ("text", ["HSET", "f", "v"]),
                ("text", ["LPUSH", "x"]),
                ("text", ["SADD", "x"]),
                ("text", ["ZADD", "1", "x"]),
                ("ttl", ["PERSIST"]),
                ("ttl", ["EXPIRE", "100"]),

                ("number", ["INCR"]),
                ("number", ["DECR"]),
                ("number", ["INCRBY", "5"]),
                ("number", ["DECRBY", "5"]),
                ("number", ["INCRBYFLOAT", "1.5"]),
                ("number", ["INCRBYFLOAT", "-0.25"]),
                ("leadingzero", ["INCRBYFLOAT", "1.5"]),

                ("etag", ["SETIFMATCH", "x", "1"]),
                ("etag", ["SETIFMATCH", "x", "7"]),
                ("etag", ["SETIFGREATER", "x", "5"]),
                ("etag", ["SETIFGREATER", "x", "0"]),
                ("etag", ["SETWITHETAG", "x"]),
                ("etag", ["DELIFGREATER", "5"]),
                ("etag", ["DELIFGREATER", "0"]),

                ("hash", ["GETEX"]),
                ("hash", ["GETDEL"]),
                ("hash", ["SETNX", "x"]),
                ("hash", ["SET", "x", "NX"]),
                ("hash", ["SET", "x", "XX"]),
                ("hash", ["SET", "x", "GET"]),
                ("hash", ["GETSET", "x"]),
                ("hash", ["INCR"]),
                ("hash", ["INCRBYFLOAT", "1.5"]),
                ("hash", ["APPEND", "x"]),
                ("hash", ["SETRANGE", "0", "x"]),
                ("hash", ["SETBIT", "7", "1"]),
                ("hash", ["RESTORE", "0", payload]),
                ("hash", ["SETWITHETAG", "x"]),
                ("hash", ["SETIFMATCH", "x", "1"]),
                ("hash", ["DELIFGREATER", "5"]),
                ("hash", ["HSET", "g", "w"]),
                ("hash", ["EXPIRE", "100"]),
                ("hash", ["EXPIRE", "100", "NX"]),
                ("hash", ["EXPIRE", "100", "XX"]),
                ("hash", ["PERSIST"]),

                ("ttlhash", ["HSET", "g", "w"]),
                ("ttlhash", ["HDEL", "f"]),
                ("ttlhash", ["LPUSH", "x"]),
                ("ttlhash", ["SET", "x", "NX"]),
                ("ttlhash", ["PERSIST"]),
                ("ttlhash", ["EXPIRE", "100"]),
                ("ttlhash", ["EXPIRE", "100", "NX"]),
                ("ttlhash", ["EXPIRE", "100", "XX"]),
                ("ttlhash", ["EXPIRE", "100", "GT"]),
                ("ttlhash", ["EXPIRE", "100", "LT"]),

                ("gone", ["GETEX"]),
                ("gone", ["GETDEL"]),
                ("gone", ["SETNX", "x"]),
                ("gone", ["SET", "x", "NX"]),
                ("gone", ["SET", "x", "XX"]),
                ("gone", ["SET", "x", "EX", "100", "NX"]),
                ("gone", ["SET", "x", "GET"]),
                ("gone", ["SET", "x", "KEEPTTL"]),
                ("gone", ["GETSET", "x"]),
                ("gone", ["INCR"]),
                ("gone", ["INCRBYFLOAT", "1.5"]),
                ("gone", ["APPEND", "x"]),
                ("gone", ["SETRANGE", "3", "x"]),
                ("gone", ["SETBIT", "100", "1"]),
                ("gone", ["RESTORE", "0", payload]),
                ("gone", ["SETWITHETAG", "x"]),
                ("gone", ["SETIFMATCH", "x", "1"]),
                ("gone", ["SETIFGREATER", "x", "1"]),
                ("gone", ["DELIFGREATER", "5"]),
                ("gone", ["EXPIRE", "100"]),
                ("gone", ["PERSIST"]),
                ("gone", ["DEL"]),
                ("gone", ["HSET", "f", "v"]),

                ("gonehash", ["HDEL", "f"]),
                ("gonehash", ["HSET", "g", "w"]),
                ("gonehash", ["LPOP"]),
                ("gonehash", ["INCR"]),
                ("gonehash", ["GETDEL"]),
                ("gonehash", ["EXPIRE", "100"]),
                ("gonehash", ["PERSIST"]),
                ("gonehash", ["DEL"]),
            ];

            var readOnlyBegin = PlaceOnDiskAndReadOnly(server, db, region =>
            {
                for (var i = 0; i < cases.Length; i++)
                    WriteRecord(db, cases[i].kind, $"{region}:{i}");
            });

            string StateOf(string key)
            {
                var ttl = Render(() => db.Execute("TTL", key));
                return Render(() => db.Execute("GETWITHETAG", key)) + "|" + Render(() => db.Execute("TYPE", key)) + "|" + (ttl is "-1" or "-2" ? ttl : "expires");
            }

            (string reply, string state) Apply(string key, object[] command)
            {
                try
                {
                    return (Render(() => db.Execute((string)command[0], [key, .. command[1..]])), StateOf(key));
                }
                catch (RedisConnectionException)
                {
                    redis.Dispose();
                    redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
                    db = redis.GetDatabase(0);
                    return ("<connection closed>", StateOf(key));
                }
            }

            var mismatches = new System.Collections.Generic.List<string>();
            void Compare((string reply, string state) expected, (string reply, string state) actual, string description)
            {
                if (expected != actual)
                    mismatches.Add($"{description}: expected {expected}, got {actual}");
            }

            for (var i = 0; i < cases.Length; i++)
            {
                var (kind, command) = cases[i];
                var description = $"{string.Join(" ", Array.ConvertAll(command, a => a as string ?? "<payload>"))} on a {kind} record";

                var mutableKey = $"mutable:{i}";
                WriteRecord(db, kind, mutableKey);
                var expired = kind.StartsWith("gone", StringComparison.Ordinal);
                if (expired)
                    Thread.Sleep(10);

                var expected = expired ? Apply($"absent:{i}", command) : Apply(mutableKey, command);
                if (expired)
                    Compare(expected, Apply(mutableKey, command), $"{description} in the mutable region");
                Compare(expected, Apply($"readonly:{i}", command), $"{description} in the read-only region");
                Compare(expected, Apply($"disk:{i}", command), $"{description} on disk");
            }

            server = redis.GetServer(TestUtils.EndPoint);
            ClassicAssert.LessOrEqual(TestUtils.GetStoreAddressInfo(server).HeadAddress, readOnlyBegin, "A read-only record left memory before it was updated.");
            redis.Dispose();
            ClassicAssert.IsEmpty(mismatches, string.Join("\n", mismatches));
        }

        /// <summary>
        /// SET NX leaves a key of another type intact wherever its record is, and SET NX GET answers WRONGTYPE for it.
        /// </summary>
        [Test]
        public void SetNxLeavesAKeyOfAnotherTypeWhereverItIs()
        {
            RestartWithALargeReadOnlyRegion();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.EndPoint);

            var readOnlyBegin = PlaceOnDiskAndReadOnly(server, db, region => WriteRecord(db, "hash", region));
            WriteRecord(db, "hash", "mutable");

            foreach (var key in Regions)
            {
                ClassicAssert.IsTrue(db.Execute("SET", key, "x", "NX").IsNull, $"SET NX in the {key} region");
                ClassicAssert.IsTrue(db.Execute("SET", key, "x", "EX", "100", "NX").IsNull, $"SET EX NX in the {key} region");
                ClassicAssert.AreEqual(0, (int)db.Execute("SETNX", key, "x"), $"SETNX in the {key} region");
                var error = Assert.Throws<RedisServerException>(() => db.Execute("SET", key, "x", "NX", "GET"), $"SET NX GET in the {key} region");
                StringAssert.StartsWith("WRONGTYPE", error.Message);

                ClassicAssert.AreEqual("hash", db.Execute("TYPE", key).ToString(), $"Type in the {key} region");
                ClassicAssert.AreEqual("v", (string)db.HashGet(key, "f"), $"Field in the {key} region");
                ClassicAssert.AreEqual(-1, (int)db.Execute("TTL", key), $"TTL in the {key} region");
            }

            ClassicAssert.LessOrEqual(TestUtils.GetStoreAddressInfo(server).HeadAddress, readOnlyBegin, "A read-only record left memory before it was updated.");
        }

        /// <summary>
        /// BLMOVE onto an expired list wakes a client blocked on that list wherever the expired record is.
        /// </summary>
        [Test]
        public async Task BlockingMoveOntoAnExpiredListWakesClientsBlockedOnIt()
        {
            RestartWithALargeReadOnlyRegion();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.EndPoint);

            void WriteExpiredList(string key)
            {
                ClassicAssert.AreEqual(1, (long)db.Execute("RPUSH", key, "old"));
                ClassicAssert.AreEqual(1, (int)db.Execute("PEXPIRE", key, "1"));
            }

            var readOnlyBegin = PlaceOnDiskAndReadOnly(server, db, WriteExpiredList);
            WriteExpiredList("mutable");
            Thread.Sleep(10);

            // A client per region blocks on the expired list, which BLPOP sees as absent
            var blocked = new (ConnectionMultiplexer connection, Task<RedisResult> pop)[Regions.Length];
            for (var i = 0; i < Regions.Length; i++)
            {
                var connection = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                blocked[i] = (connection, connection.GetDatabase(0).ExecuteAsync("BLPOP", Regions[i], "5"));
            }

            try
            {
                // Wait for the clients to block
                await Task.Delay(1000).ConfigureAwait(false);
                foreach (var (_, pop) in blocked)
                    ClassicAssert.IsFalse(pop.IsCompleted, "BLPOP answered before anything was pushed");

                for (var i = 0; i < Regions.Length; i++)
                {
                    var source = $"source:{Regions[i]}";
                    ClassicAssert.AreEqual(1, (long)db.Execute("RPUSH", source, "moved"));
                    ClassicAssert.AreEqual("moved", (string)db.Execute("BLMOVE", source, Regions[i], "LEFT", "RIGHT", "5"), $"BLMOVE onto the {Regions[i]} region");
                }

                for (var i = 0; i < Regions.Length; i++)
                {
                    var popped = (RedisResult[])await blocked[i].pop.ConfigureAwait(false);
                    ClassicAssert.IsNotNull(popped, $"The client blocked on the list in the {Regions[i]} region was not woken");
                    ClassicAssert.AreEqual("moved", (string)popped[1], $"BLPOP from the {Regions[i]} region");
                }
            }
            finally
            {
                foreach (var (connection, _) in blocked)
                    connection.Dispose();
            }

            ClassicAssert.LessOrEqual(TestUtils.GetStoreAddressInfo(server).HeadAddress, readOnlyBegin, "A read-only record left memory before it was updated.");
        }

        /// <summary>
        /// The storage API reports the elements a ZADD added to an expired sorted set wherever the expired record is.
        /// </summary>
        [Test]
        public void SortedSetAddToAnExpiredSortedSetCountsTheMember()
        {
            RestartWithALargeReadOnlyRegion();
            this.server.Register.NewProcedure("ZADDCOUNT", () => new SortedSetAddCount());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.EndPoint);

            void WriteExpiredSortedSet(string key)
            {
                ClassicAssert.AreEqual(1, (int)db.Execute("ZADD", key, "1", "old"));
                ClassicAssert.AreEqual(1, (int)db.Execute("PEXPIRE", key, "1"));
            }

            var readOnlyBegin = PlaceOnDiskAndReadOnly(server, db, WriteExpiredSortedSet);
            WriteExpiredSortedSet("mutable");
            Thread.Sleep(10);

            foreach (var key in Regions)
            {
                ClassicAssert.AreEqual("1", db.Execute("ZADDCOUNT", key, "2", "new").ToString(), $"ZADD to the {key} region");
                ClassicAssert.AreEqual(new[] { "new" }, (string[])db.Execute("ZRANGE", key, "0", "-1"), $"Members in the {key} region");
            }

            ClassicAssert.LessOrEqual(TestUtils.GetStoreAddressInfo(server).HeadAddress, readOnlyBegin, "A read-only record left memory before it was updated.");
        }

        /// <summary>Adds a member to a sorted set through the storage API, and answers with the count it reports.</summary>
        sealed class SortedSetAddCount : CustomProcedure
        {
            public override bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
            {
                var offset = 0;
                var key = GetNextArg(ref procInput, ref offset);
                var score = GetNextArg(ref procInput, ref offset);
                var member = GetNextArg(ref procInput, ref offset);
                _ = garnetApi.SortedSetAdd(key, score, member, out var count);
                WriteSimpleString(ref output, count.ToString());
                return true;
            }
        }

        /// <summary>The keys <see cref="PlaceOnDiskAndReadOnly"/> and the tests place their records at, each named for its region.</summary>
        static readonly string[] Regions = ["mutable", "readonly", "disk"];

        /// <summary>
        /// Restarts the server with a read-only region far larger than the mutable one, which keeps records in memory long
        /// after they become read-only.
        /// </summary>
        void RestartWithALargeReadOnlyRegion()
        {
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, memorySize: "256k", pageSize: "4k", mutablePercent: 10);
            server.Start();
        }

        /// <summary>
        /// Calls <paramref name="write"/> with "disk", and then writes enough other records that its records leave memory;
        /// then calls it with "readonly", and writes enough other records that its records become read-only but stay in memory.
        /// </summary>
        /// <returns>The address the read-only records begin at, which must still be in memory once they have been used.</returns>
        static long PlaceOnDiskAndReadOnly(IServer server, IDatabase db, Action<string> write)
        {
            var filler = 0;
            void Fill(Func<StoreAddressInfo, bool> until)
            {
                while (!until(TestUtils.GetStoreAddressInfo(server)))
                {
                    for (var i = 0; i < 50; i++)
                        _ = db.StringSet($"filler{filler++:D6}", "filler");
                }
            }

            write("disk");
            var diskTail = TestUtils.GetStoreAddressInfo(server).TailAddress;
            Fill(info => info.HeadAddress >= diskTail);

            var readOnlyBegin = TestUtils.GetStoreAddressInfo(server).TailAddress;
            write("readonly");
            var readOnlyTail = TestUtils.GetStoreAddressInfo(server).TailAddress;
            Fill(info => info.ReadOnlyAddress >= readOnlyTail);
            Thread.Sleep(10);
            return readOnlyBegin;
        }

        static string WriteDumpSource(IDatabase db)
        {
            const string key = "dumpsource";
            WriteRecord(db, "text", key);
            return key;
        }
    }
}