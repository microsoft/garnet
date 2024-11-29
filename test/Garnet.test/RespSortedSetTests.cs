// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Embedded.perftest;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;
using SetOperation = StackExchange.Redis.SetOperation;

namespace Garnet.test
{
    using TestBasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
        BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>>;

    [TestFixture]
    public class RespSortedSetTests
    {
        protected GarnetServer server;

        static readonly SortedSetEntry[] entries =
              [
                new SortedSetEntry("a", 1),
                  new SortedSetEntry("b", 2),
                  new SortedSetEntry("c", 3),
                  new SortedSetEntry("d", 4),
                  new SortedSetEntry("e", 5),
                  new SortedSetEntry("f", 6),
                  new SortedSetEntry("g", 7),
                  new SortedSetEntry("h", 8),
                  new SortedSetEntry("i", 9),
                  new SortedSetEntry("j", 10)
              ];

        static readonly SortedSetEntry[] leaderBoard =
             [
                new SortedSetEntry("Dave", 340),
                 new SortedSetEntry("Kendra", 400),
                 new SortedSetEntry("Tom", 560),
                 new SortedSetEntry("Barbara", 650),
                 new SortedSetEntry("Jennifer", 690),
                 new SortedSetEntry("Peter", 690),
                 new SortedSetEntry("Frank", 740),
                 new SortedSetEntry("Lester", 790),
                 new SortedSetEntry("Alice", 850),
                 new SortedSetEntry("Mary", 980)
             ];

        static readonly SortedSetEntry[] powOfTwo =
            [
                new SortedSetEntry("a", 1),
                new SortedSetEntry("b", 2),
                new SortedSetEntry("c", 4),
                new SortedSetEntry("d", 8),
                new SortedSetEntry("e", 16),
                new SortedSetEntry("f", 32),
                new SortedSetEntry("g", 64),
                new SortedSetEntry("h", 128),
                new SortedSetEntry("i", 256),
                new SortedSetEntry("j", 512)
            ];


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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        #region SETests
        [Test]
        public unsafe void SortedSetPopTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);

            var session = new RespServerSession(0, new DummyNetworkSender(), server.Provider.StoreWrapper, null, null, false);
            var api = new TestBasicGarnetApi(session.storageSession, session.storageSession.basicContext, session.storageSession.objectStoreBasicContext);
            var key = Encoding.ASCII.GetBytes("key1");
            fixed (byte* keyPtr = key)
            {
                var result = api.SortedSetPop(new ArgSlice(keyPtr, key.Length), out var items);
                ClassicAssert.AreEqual(1, items.Length);
                ClassicAssert.AreEqual("a", Encoding.ASCII.GetString(items[0].member.ReadOnlySpan));
                ClassicAssert.AreEqual("1", Encoding.ASCII.GetString(items[0].score.ReadOnlySpan));

                result = api.SortedSetPop(new ArgSlice(keyPtr, key.Length), out items);
                ClassicAssert.AreEqual(1, items.Length);
                ClassicAssert.AreEqual("b", Encoding.ASCII.GetString(items[0].member.ReadOnlySpan));
                ClassicAssert.AreEqual("2", Encoding.ASCII.GetString(items[0].score.ReadOnlySpan));
            }
        }

        [Test]
        public void AddAndLength()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "SortedSet_Add";

            // 10 entries are added
            var added = db.SortedSetAdd(key, entries);
            ClassicAssert.AreEqual(entries.Length, added);

            var card = db.SortedSetLength(key);
            ClassicAssert.AreEqual(entries.Length, card);

            var response = db.Execute("MEMORY", "USAGE", key);
            var actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            var expectedResponse = 1792;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var entries2 = new SortedSetEntry[entries.Length + 1];
            entries.CopyTo(entries2, 0);
            entries2[entries2.Length - 1] = new SortedSetEntry("k", 11);

            // only 1 new entry should get added
            added = db.SortedSetAdd(key, entries2);
            ClassicAssert.AreEqual(1, added);

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 1952;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // no new entries get added
            added = db.SortedSetAdd(key, entries2);
            ClassicAssert.AreEqual(0, added);

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 1952;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            card = db.SortedSetLength(key);
            ClassicAssert.AreEqual(entries2.Length, card);

            added = db.SortedSetAdd(key, [new SortedSetEntry("a", 12)]);
            ClassicAssert.AreEqual(0, added);

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 1952;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var deleted = db.KeyDelete(key);
            ClassicAssert.IsTrue(deleted);

            added = db.SortedSetAdd(key, []);
            ClassicAssert.AreEqual(0, added);

            var exists = db.KeyExists(key);
            ClassicAssert.IsFalse(exists);
        }

        [Test]
        public void AddWithOptions()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "SortedSet_Add";

            var added = db.SortedSetAdd(key, entries);
            ClassicAssert.AreEqual(entries.Length, added);

            // XX - Only update elements that already exist. Don't add new elements.
            var testEntries = new[]
            {
                new SortedSetEntry("a", 3),
                new SortedSetEntry("b", 4),
                new SortedSetEntry("k", 11),
                new SortedSetEntry("l", 12),
            };

            added = db.SortedSetAdd(key, testEntries, SortedSetWhen.Exists);
            ClassicAssert.AreEqual(0, added);
            var scores = db.SortedSetScores(key, [new RedisValue("a"), new RedisValue("b")]);
            CollectionAssert.AreEqual(new double[] { 3, 4 }, scores);
            var count = db.SortedSetLength(key);
            ClassicAssert.AreEqual(10, count);

            // NX - Only add new elements. Don't update already existing elements.
            testEntries =
            [
                new SortedSetEntry("a", 4),
                new SortedSetEntry("b", 5),
                new SortedSetEntry("k", 11),
                new SortedSetEntry("l", 12),
            ];

            added = db.SortedSetAdd(key, testEntries, SortedSetWhen.NotExists);
            ClassicAssert.AreEqual(2, added);
            scores = db.SortedSetScores(key, [new RedisValue("a"), new RedisValue("b"), new RedisValue("k"), new RedisValue("l")]);
            CollectionAssert.AreEqual(new double[] { 3, 4, 11, 12 }, scores);
            count = db.SortedSetLength(key);
            ClassicAssert.AreEqual(12, count);

            // LT - Only update existing elements if the new score is less than the current score.
            testEntries =
            [
                new SortedSetEntry("a", 4),
                new SortedSetEntry("b", 3),
                new SortedSetEntry("m", 13),
            ];

            added = db.SortedSetAdd(key, testEntries, SortedSetWhen.LessThan);
            ClassicAssert.AreEqual(1, added);
            scores = db.SortedSetScores(key, [new RedisValue("a"), new RedisValue("b"), new RedisValue("m")]);
            CollectionAssert.AreEqual(new double[] { 3, 3, 13 }, scores);
            count = db.SortedSetLength(key);
            ClassicAssert.AreEqual(13, count);

            // GT - Only update existing elements if the new score is greater than the current score.
            testEntries =
            [
                new SortedSetEntry("a", 4),
                new SortedSetEntry("b", 2),
                new SortedSetEntry("n", 14),
            ];

            added = db.SortedSetAdd(key, testEntries, SortedSetWhen.GreaterThan);
            ClassicAssert.AreEqual(1, added);
            scores = db.SortedSetScores(key, [new RedisValue("a"), new RedisValue("b"), new RedisValue("n")]);
            CollectionAssert.AreEqual(new double[] { 4, 3, 14 }, scores);
            count = db.SortedSetLength(key);
            ClassicAssert.AreEqual(14, count);

            // CH - Modify the return value from the number of new elements added, to the total number of elements changed
            var testArgs = new object[]
            {
                key, "CH",
                "1", "a",
                "2", "b",
                "3", "c",
                "15", "o"
            };

            var resp = db.Execute("ZADD", testArgs);
            ClassicAssert.IsTrue(int.TryParse(resp.ToString(), out var changed));
            ClassicAssert.AreEqual(3, changed);

            // INCR - When this option is specified ZADD acts like ZINCRBY
            testArgs = [key, "INCR", "3.5", "a"];

            resp = db.Execute("ZADD", testArgs);
            ClassicAssert.IsTrue(double.TryParse(resp.ToString(), out var newVal));
            ClassicAssert.AreEqual(4.5, newVal);
        }

        [Test]
        public void AddWithOptionsErrorConditions()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "SortedSet_Add";
            var sampleEntries = new[] { "1", "m1", "2", "m2" };

            // XX & NX options are mutually exclusive
            var args = new[] { key, "XX", "NX" }.Union(sampleEntries).ToArray<object>();
            var ex = Assert.Throws<RedisServerException>(() => db.Execute("ZADD", args));
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_XX_NX_NOT_COMPATIBLE), ex.Message);

            // GT, LT & NX options are mutually exclusive
            var argCombinations = new[]
            {
                new[] { key, "GT", "LT" },
                [key, "GT", "NX"],
                [key, "LT", "NX"],
            };

            foreach (var argCombination in argCombinations)
            {
                args = argCombination.Union(sampleEntries).ToArray<object>();
                ex = Assert.Throws<RedisServerException>(() => db.Execute("ZADD", args));
                ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GT_LT_NX_NOT_COMPATIBLE), ex.Message);
            }

            // INCR option supports only one score-element pair
            args = new[] { key, "INCR" }.Union(sampleEntries).ToArray<object>();
            ex = Assert.Throws<RedisServerException>(() => db.Execute("ZADD", args));
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR), ex.Message);

            // No member-score pairs
            args = [key, "XX", "CH"];
            ex = Assert.Throws<RedisServerException>(() => db.Execute("ZADD", args));
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_SYNTAX_ERROR), ex.Message);
        }

        [Test]
        public void CanCreateLeaderBoard()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "LeaderBoard";

            // 10 entries are added
            var added = db.SortedSetAdd(key, leaderBoard);
            ClassicAssert.AreEqual(leaderBoard.Length, added);

            var response = db.Execute("MEMORY", "USAGE", key);
            var actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            var expectedResponse = 1792;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var card = db.SortedSetLength(key);
            ClassicAssert.AreEqual(leaderBoard.Length, card);
        }

        [Test]
        public void CanGetScoresZCount()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "LeaderBoard";

            // 10 entries are added
            var added = db.SortedSetAdd(key, leaderBoard);
            ClassicAssert.AreEqual(leaderBoard.Length, added);

            var card = db.SortedSetLength(new RedisKey(key), min: 500, max: 700);
            ClassicAssert.IsTrue(4 == card);

            //using infinity
            card = db.SortedSetLength(new RedisKey(key), min: -1);
            ClassicAssert.IsTrue(10 == card);
        }

        [Test]
        public void AddRemove()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "SortedSet_AddRemove";

            // 10 entries are added
            var added = db.SortedSetAdd(key, entries);
            ClassicAssert.AreEqual(entries.Length, added);

            var card = db.SortedSetLength(key);
            ClassicAssert.AreEqual(entries.Length, card);

            var response = db.Execute("MEMORY", "USAGE", key);
            var actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            var expectedResponse = 1800;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // remove all entries
            var removed = db.SortedSetRemove(key, entries.Select(e => e.Element).ToArray());
            ClassicAssert.AreEqual(entries.Length, removed);

            // length should be 0
            card = db.SortedSetLength(key);
            ClassicAssert.AreEqual(0, card);

            var keyExists = db.KeyExists(key);
            ClassicAssert.IsFalse(keyExists);

            response = db.Execute("MEMORY", "USAGE", key);
            ClassicAssert.IsTrue(response.IsNull);

            // 1 entry added
            added = db.SortedSetAdd(key, [entries[0]]);
            ClassicAssert.AreEqual(1, added);

            card = db.SortedSetLength(key);
            ClassicAssert.AreEqual(1, card);

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 360;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // remove the single entry
            removed = db.SortedSetRemove(key, entries.Take(1).Select(e => e.Element).ToArray());
            ClassicAssert.AreEqual(1, removed);

            // length should be 0
            card = db.SortedSetLength(key);
            ClassicAssert.AreEqual(0, card);

            var response_keys = db.SortedSetRangeByRankWithScores(key, 0, 100);
            ClassicAssert.IsEmpty(response_keys);

            keyExists = db.KeyExists(key);
            ClassicAssert.IsFalse(keyExists);

            response = db.Execute("MEMORY", "USAGE", key);
            ClassicAssert.IsTrue(response.IsNull);

            // 10 entries are added
            added = db.SortedSetAdd(key, entries);
            ClassicAssert.AreEqual(entries.Length, added);

            card = db.SortedSetLength(key);
            ClassicAssert.AreEqual(entries.Length, card);

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 1800;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // 1 entry removed
            bool isRemoved = db.SortedSetRemove(key, entries[0].Element);
            ClassicAssert.IsTrue(isRemoved);

            // length should be 1 less
            card = db.SortedSetLength(key);
            ClassicAssert.AreEqual(entries.Length - 1, card);

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 1640;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // remaining entries removed
            removed = db.SortedSetRemove(key, entries.Select(e => e.Element).ToArray());
            ClassicAssert.AreEqual(entries.Length - 1, removed);

            keyExists = db.KeyExists(key);
            ClassicAssert.IsFalse(keyExists);

            response = db.Execute("MEMORY", "USAGE", key);
            ClassicAssert.IsTrue(response.IsNull);
        }

        [Test]
        public void AddRemoveBy()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "SortedSet_AddRemoveBy";

            // 10 entries are added
            var added = db.SortedSetAdd(key, entries);
            ClassicAssert.AreEqual(entries.Length, added);

            var result = db.SortedSetRemoveRangeByValue(key, new RedisValue("e"), new RedisValue("g"));
            ClassicAssert.AreEqual(3, result);

            result = db.SortedSetRemoveRangeByScore(key, 9, 10);
            ClassicAssert.AreEqual(2, result);

            result = db.SortedSetRemoveRangeByRank(key, 0, 1);
            ClassicAssert.AreEqual(2, result);

            var members = db.SortedSetRangeByRank(key);
            ClassicAssert.AreEqual(new[] { new RedisValue("c"), new RedisValue("d"), new RedisValue("h") }, members);

            result = db.SortedSetRemoveRangeByRank(key, 0, 2);
            ClassicAssert.AreEqual(3, result);

            var exists = db.KeyExists(key);
            ClassicAssert.IsFalse(exists);

            added = db.SortedSetAdd(key, entries);
            ClassicAssert.AreEqual(entries.Length, added);

            result = db.SortedSetRemoveRangeByScore(key, 0, 10);
            ClassicAssert.AreEqual(10, result);

            exists = db.KeyExists(key);
            ClassicAssert.IsFalse(exists);

            added = db.SortedSetAdd(key, entries);
            ClassicAssert.AreEqual(entries.Length, added);

            result = db.SortedSetRemoveRangeByValue(key, "a", "j");
            ClassicAssert.AreEqual(10, result);

            exists = db.KeyExists(key);
            ClassicAssert.IsFalse(exists);
        }

        [Test]
        public void AddPopDesc()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "SortedSet_AddPop";

            var added = db.SortedSetAdd(key, entries);

            var response = db.Execute("MEMORY", "USAGE", key);
            var actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            var expectedResponse = 1792;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var last = db.SortedSetPop(key, Order.Descending);
            ClassicAssert.True(last.HasValue);
            ClassicAssert.AreEqual(entries[9], last.Value);
            ClassicAssert.AreEqual(9, db.SortedSetLength(key));

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 1632;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var last2 = db.SortedSetPop(key, 2, Order.Descending);
            ClassicAssert.AreEqual(2, last2.Length);
            ClassicAssert.AreEqual(entries[8], last2[0]);
            ClassicAssert.AreEqual(entries[7], last2[1]);
            ClassicAssert.AreEqual(7, db.SortedSetLength(key));

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 1312;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var last3 = db.SortedSetPop(key, 999, Order.Descending);
            ClassicAssert.AreEqual(7, last3.Length);
            for (int i = 0; i < 7; i++)
                ClassicAssert.AreEqual(entries[6 - i], last3[i]);
            ClassicAssert.AreEqual(0, db.SortedSetLength(key));

            var keyExists = db.KeyExists(key);
            ClassicAssert.IsFalse(keyExists);

            response = db.Execute("MEMORY", "USAGE", key);
            ClassicAssert.IsTrue(response.IsNull);
        }

        [Test]
        public void AddScore()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "SortedSet_AddScore";

            var added = db.SortedSetAdd(key, entries);

            var score = db.SortedSetScore(key, "a");
            ClassicAssert.True(score.HasValue);
            ClassicAssert.AreEqual(1, score.Value);

            score = db.SortedSetScore(key, "x");
            ClassicAssert.False(score.HasValue);

            var response = db.Execute("MEMORY", "USAGE", key);
            var actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            var expectedResponse = 1800;
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoZMScore()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "SortedSet_GetMemberScores";
            var added = db.SortedSetAdd(key, entries);
            var scores = db.SortedSetScores(key, ["a", "b", "z", "i"]);
            ClassicAssert.AreEqual(scores, new List<double?>() { 1, 2, null, 9 });


            scores = db.SortedSetScores("nokey", ["a", "b", "c"]);
            ClassicAssert.AreEqual(scores, new List<double?>() { null, null, null });

            Assert.Throws<RedisServerException>(
                () => db.SortedSetScores("nokey", []),
                "ERR wrong number of arguments for ZMSCORE command.");

            var memResponse = db.Execute("MEMORY", "USAGE", key);
            var memActualValue = ResultType.Integer == memResponse.Resp2Type ? Int32.Parse(memResponse.ToString()) : -1;
            var memExpectedResponse = 1808;
            ClassicAssert.AreEqual(memExpectedResponse, memActualValue);
        }

        [Test]
        public void CanDoZMScoreLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            lightClientRequest.SendCommands("ZADD zmscore 0 a 1 b", "PING");

            var response = lightClientRequest.SendCommands("ZMSCORE zmscore", "PING");
            var expectedResponse = $"{FormatWrongNumOfArgsError("ZMSCORE")}+PONG\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommands("ZMSCORE nokey a b c", "PING", 4, 1);
            expectedResponse = "*3\r\n$-1\r\n$-1\r\n$-1\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommands("ZMSCORE zmscore a z b", "PING", 4, 1);
            expectedResponse = "*3\r\n$1\r\n0\r\n$-1\r\n$1\r\n1\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CandDoZIncrby()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "LeaderBoard";

            // 10 entries should be added
            var added = db.SortedSetAdd(key, leaderBoard);
            ClassicAssert.AreEqual(leaderBoard.Length, added);

            var incr = db.SortedSetIncrement(key, new RedisValue("Tom"), 90);
            ClassicAssert.IsTrue(incr == 650);

            var response = db.Execute("MEMORY", "USAGE", key);
            var actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            var expectedResponse = 1792;
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void CanManageNotExistingKeySE()
        {

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            //ZPOPMAX
            var actualResult = db.SortedSetPop(new RedisKey("nokey"), Order.Descending);
            ClassicAssert.AreEqual(null, actualResult);

            //ZCOUNT
            var doneZCount = db.SortedSetLength(new RedisKey("nokey"), 1, 3, Exclude.None, CommandFlags.None);
            ClassicAssert.AreEqual(0, doneZCount);

            //ZLEXCOUNT
            var doneZLEXCount = db.SortedSetLengthByValue(new RedisKey("nokey"), Double.NegativeInfinity, Double.PositiveInfinity);
            ClassicAssert.AreEqual(0, doneZLEXCount);

            //ZCARD
            var doneZCard = db.SortedSetLength(new RedisKey("nokey"));
            ClassicAssert.AreEqual(0, doneZCard);

            //ZPOPMIN
            actualResult = db.SortedSetPop(new RedisKey("nokey"));
            ClassicAssert.AreEqual(null, actualResult);

            //ZREM
            var doneRemove = db.SortedSetRemove(new RedisKey("nokey"), new RedisValue("a"));
            ClassicAssert.AreEqual(false, doneRemove);

            //ZREMRANGEBYLEX
            var doneRemByLex = db.SortedSetRemoveRangeByValue(new RedisKey("nokey"), new RedisValue("a"), new RedisValue("b"));
            ClassicAssert.AreEqual(0, doneRemByLex);

            //ZREMRANGEBYRANK
            var doneRemRangeByRank = db.SortedSetRemoveRangeByRank(new RedisKey("nokey"), 0, 1);
            ClassicAssert.AreEqual(0, doneRemRangeByRank);

            //ZREMRANGEBYSCORE
            var doneRemRangeByScore = db.SortedSetRemoveRangeByScore(new RedisKey("nokey"), 0, 1);
            ClassicAssert.AreEqual(0, doneRemRangeByScore);

            var response = db.Execute("MEMORY", "USAGE", "nokey");
            var actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            var expectedResponse = -1;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //ZINCR, with this command the sorted set gets created
            var doneZIncr = db.SortedSetIncrement(new RedisKey("nokey"), new RedisValue("1"), 1);
            ClassicAssert.AreEqual(1, doneZIncr);

            response = db.Execute("MEMORY", "USAGE", "nokey");
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 344;
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanUseZScanNoParameters()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // ZSCAN without key
            try
            {
                db.Execute("ZSCAN");
                Assert.Fail();
            }
            catch (RedisServerException e)
            {
                var expectedErrorMessage = string.Format(CmdStrings.GenericErrWrongNumArgs, nameof(SortedSetOperation.ZSCAN));
                ClassicAssert.AreEqual(expectedErrorMessage, e.Message);
            }

            // Use sortedsetscan on non existing key
            var items = db.SortedSetScan(new RedisKey("foo"), new RedisValue("*"), pageSize: 10);
            ClassicAssert.IsEmpty(items, "Failed to use SortedSetScan on non existing key");

            // Add some items
            var added = db.SortedSetAdd("myss", entries);
            ClassicAssert.AreEqual(entries.Length, added);

            var members = db.SortedSetScan(new RedisKey("myss"), new RedisValue("*"));
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsTrue(members.Count() == 10);

            int i = 0;
            foreach (var item in members)
            {
                ClassicAssert.IsTrue(entries[i++].Element.Equals(item.Element));
            }
        }

        [Test]
        public void CanUseZScanWithMatch()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add some items
            var added = db.SortedSetAdd("myss", entries);
            ClassicAssert.AreEqual(entries.Length, added);

            var members = db.SortedSetScan(new RedisKey("myss"), new RedisValue("j*"));
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsTrue(members.Count() == 1);
            ClassicAssert.IsTrue(entries[9].Element.Equals(members.ElementAt(0).Element));
        }

        [Test]
        public void CanUseZScanWithCollection()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add some items
            var r = new Random();

            // Fill a new SortedSetEntry with 1000 random entries
            int n = 1000;
            var entries = new SortedSetEntry[n];

            for (int i = 0; i < n; i++)
            {
                var memberId = r.Next(0, 10000000);
                entries[i] = new SortedSetEntry($"key:{memberId}", memberId);
            }

            var ssLen = db.SortedSetAdd("myss", entries);
            var members = db.SortedSetScan(new RedisKey("myss"), new RedisValue("key:*"), (Int32)ssLen);
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsTrue(members.Count() == ssLen);

            entries = new SortedSetEntry[n];
            for (int i = 0; i < n; i++)
            {
                var memberId = r.NextDouble();
                entries[i] = new SortedSetEntry($"key:{memberId}", memberId);
            }

            ssLen = db.SortedSetAdd("myssDoubles", entries);
            members = db.SortedSetScan(new RedisKey("myssDoubles"), new RedisValue("key:*"), (Int32)ssLen);
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsTrue(members.Count() == ssLen);
        }


        [Test]
        public void CanUseZScanWithDoubleDifferentFormats()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            double[] numbers;

            numbers = new double[7];

            // initialize numbers array with different doubles values including scientific notation and exponential notation
            numbers[0] = 1.11;
            numbers[1] = 42e73;
            numbers[2] = 4.94065645841247E-324;
            numbers[3] = 1.7976931348623157E+308;
            numbers[4] = -1.7976931348623157E+308;
            numbers[5] = 9223372036854775807;
            numbers[6] = -9223372036854775808;

            var key = "ssScores";
            var entries = new SortedSetEntry[numbers.Length];

            for (int i = 0; i < numbers.Length; i++)
            {
                entries[i] = new SortedSetEntry($"{key}:{i}", numbers[i]);
            }

            var ssLen = db.SortedSetAdd(key, entries);
            ClassicAssert.IsTrue(numbers.Length == ssLen);

            var members = db.SortedSetScan(key, new RedisValue("*Scores:*"), (Int32)ssLen);
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsTrue(members.Count() == ssLen);

            int k = 0;
            foreach (var item in members)
            {
                ClassicAssert.AreEqual(item.Score, numbers[k++]);
            }

            // Test for no matching members
            members = db.SortedSetScan(key, new RedisValue("key*"), (Int32)ssLen);
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsEmpty(members);
        }

        [Test]
        public void CanDoZScanWithCursor()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Fill a new SortedSetEntry with 1000 random entries
            int n = 1000;
            var entries = new SortedSetEntry[n];
            var keySS = new RedisKey("keySS");

            // Add some items
            for (int i = 0; i < n; i++)
            {
                entries[i] = new SortedSetEntry($"key:{i}", i);
            }

            db.SortedSetAdd(keySS, entries);

            int pageSize = 45;
            var response = db.SortedSetScan(keySS, "*", pageSize: pageSize, cursor: 0);
            var cursor = ((IScanningCursor)response);
            var j = 0;
            long pageNumber = 0;
            long pageOffset = 0;

            // Consume the enumeration
            foreach (var i in response)
            {
                // Represents the *active* page of results (not the pending/next page of results as returned by HSCAN/ZSCAN/SSCAN)
                pageNumber = cursor.Cursor;

                // The offset into the current page.
                pageOffset = cursor.PageOffset;
                j++;
            }

            // Assert the end of the enumeration was reached
            ClassicAssert.AreEqual(entries.Length, j);

            // Assert the cursor is at the end of the enumeration
            ClassicAssert.AreEqual(pageNumber + pageOffset, entries.Length - 1);

            var l = response.LastOrDefault();
            ClassicAssert.AreEqual($"key:{entries.Length - 1}", l.Element.ToString());
        }

        [Test]
        public async Task CanUseZRangeByScoreWithSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "SortedSet_Pof2";

            // 10 entries are added
            await db.SortedSetAddAsync(key, powOfTwo, CommandFlags.FireAndForget);

            var res = await db.SortedSetRangeByScoreAsync(key, start: double.PositiveInfinity, double.NegativeInfinity, order: Order.Ascending);
            ClassicAssert.AreEqual(powOfTwo.Length, res.Length);

            var range = await db.SortedSetRangeByRankWithScoresAsync(key);
            ClassicAssert.AreEqual(powOfTwo.Length, range.Length);
        }

        [Test]
        public async Task CanManageZRangeByScoreWhenStartHigherThanExistingMaxScoreSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "SortedSet_OnlyZeroScore";

            await db.SortedSetAddAsync(key, "A", 0, CommandFlags.FireAndForget);

            var res = await db.SortedSetRangeByScoreAsync(key, start: 1);
            ClassicAssert.AreEqual(0, res.Length);

            var range = await db.SortedSetRangeByRankWithScoresAsync(key, start: 1);
            ClassicAssert.AreEqual(0, range.Length);
        }

        [Test]
        public void CheckEmptySortedSetKeyRemoved()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var key = new RedisKey("user1:sortedset");
            var db = redis.GetDatabase(0);

            var added = db.SortedSetAdd(key, entries);
            ClassicAssert.AreEqual(entries.Length, added);

            var actualMembers = db.SortedSetPop(key, entries.Length);
            ClassicAssert.AreEqual(entries.Length, actualMembers.Length);

            var keyExists = db.KeyExists(key);
            ClassicAssert.IsFalse(keyExists);
        }

        [Test]
        public void CheckSortedSetOperationsOnWrongTypeObjectSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var keys = new[] { new RedisKey("user1:obj1"), new RedisKey("user1:obj2") };
            var destinationKey = new RedisKey("user1:objA");
            var key1Values = new[] { new RedisValue("Hello"), new RedisValue("World") };
            var key2Values = new[] { new RedisValue("Hola"), new RedisValue("Mundo") };
            var values = new[] { key1Values, key2Values };
            double[][] scores = [[1.1, 1.2], [2.1, 2.2]];
            var sortedSetEntries = values.Select((h, idx) => h
                .Zip(scores[idx], (n, v) => new SortedSetEntry(n, v)).ToArray()).ToArray();


            // Set up different type objects
            RespTestsUtils.SetUpTestObjects(db, GarnetObjectType.Set, keys, values);

            // ZADD
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetAdd(keys[0], sortedSetEntries[0]));
            // ZCARD
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetLength(keys[0]));
            // ZPOPMAX
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetPop(keys[0]));
            // ZSCORE
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetScore(keys[0], values[0][0]));
            // ZREM
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetRemove(keys[0], values[0]));
            // ZCOUNT
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetLength(keys[1], 1, 2));
            // ZINCRBY
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetIncrement(keys[1], values[1][0], 2.2));
            // ZRANK
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetRank(keys[1], values[1][0]));
            // ZRANGE
            //RespTests.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetRangeByValueAsync(keys[1]).Wait());
            // ZRANGEBYSCORE
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetRangeByScore(keys[1]));
            // ZREVRANGE
            //RespTests.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetRangeByScore(keys[1], 1, 2, Exclude.None, Order.Descending));
            // ZREVRANK
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetRangeByRank(keys[1], 1, 2, Order.Descending));
            // ZREMRANGEBYLEX
            //RespTests.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetRemoveRangeByValue(keys[1], values[1][0], values[1][1]));
            // ZREMRANGEBYRANK
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetRemoveRangeByRank(keys[1], 0, 1));
            // ZREMRANGEBYSCORE
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetRemoveRangeByScore(keys[1], 1, 2));
            // ZLEXCOUNT
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetLengthByValue(keys[1], values[1][0], values[1][1]));
            // ZPOPMIN
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetPop(keys[1], Order.Descending));
            // ZRANDMEMBER
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetRandomMember(keys[1]));
            // ZDIFF
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetCombine(SetOperation.Difference, keys));
            // ZDIFFSTORE
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetCombineAndStore(SetOperation.Difference, destinationKey, keys));
            // ZSCAN
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetScan(keys[1], new RedisValue("*")).FirstOrDefault());
            //ZMSCORE
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SortedSetScores(keys[1], values[1]));
        }

        [Test]
        public void CanDoZDiff()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key1 = new RedisKey("key1");
            var key2 = new RedisKey("key2");
            var key1Values = new[] { new SortedSetEntry("Hello", 1), new SortedSetEntry("World", 2) };
            var key2Values = new[] { new SortedSetEntry("Hello", 5), new SortedSetEntry("Mundo", 7) };
            var expectedValue = new SortedSetEntry("World", 2);

            db.SortedSetAdd(key1, key1Values);
            db.SortedSetAdd(key2, key2Values);

            var diff = db.SortedSetCombine(SetOperation.Difference, [key1, key2]);
            ClassicAssert.AreEqual(1, diff.Length);
            ClassicAssert.AreEqual(expectedValue.Element.ToString(), diff[0].ToString());

            var diffWithScore = db.SortedSetCombineWithScores(SetOperation.Difference, [key1, key2]);
            ClassicAssert.AreEqual(1, diffWithScore.Length);
            ClassicAssert.AreEqual(expectedValue.Element.ToString(), diffWithScore[0].Element.ToString());
            ClassicAssert.AreEqual(expectedValue.Score, diffWithScore[0].Score);

            // With only one key, it should return the same elements
            diffWithScore = db.SortedSetCombineWithScores(SetOperation.Difference, [key1]);
            ClassicAssert.AreEqual(2, diffWithScore.Length);
            ClassicAssert.AreEqual(key1Values[0].Element.ToString(), diffWithScore[0].Element.ToString());
            ClassicAssert.AreEqual(key1Values[0].Score, diffWithScore[0].Score);
            ClassicAssert.AreEqual(key1Values[1].Element.ToString(), diffWithScore[1].Element.ToString());
            ClassicAssert.AreEqual(key1Values[1].Score, diffWithScore[1].Score);

            // With no value key, it should return an empty array
            diffWithScore = db.SortedSetCombineWithScores(SetOperation.Difference, [new RedisKey("key3")]);
            ClassicAssert.AreEqual(0, diffWithScore.Length);
        }

        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public void CheckSortedSetDifferenceStoreSE(bool isDestinationKeyExisting)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var keys = new[] { new RedisKey("user1:obj1"), new RedisKey("user1:obj2") };
            var destinationKey = new RedisKey("user1:objA");
            var key1Values = new[] { new SortedSetEntry("Hello", 1), new SortedSetEntry("World", 2) };
            var key2Values = new[] { new SortedSetEntry("Hello", 5), new SortedSetEntry("Mundo", 7) };
            var expectedValue = new SortedSetEntry("World", 2);

            // Set up sorted sets
            db.SortedSetAdd(keys[0], key1Values);
            db.SortedSetAdd(keys[1], key2Values);

            if (isDestinationKeyExisting)
            {
                db.SortedSetAdd(destinationKey, key1Values); // Set up destination key to overwrite if exists
            }

            var actualCount = db.SortedSetCombineAndStore(SetOperation.Difference, destinationKey, keys);
            ClassicAssert.AreEqual(1, actualCount);

            var actualMembers = db.SortedSetRangeByScoreWithScores(destinationKey);
            ClassicAssert.AreEqual(1, actualMembers.Length);
            ClassicAssert.AreEqual(expectedValue.Element.ToString(), actualMembers[0].Element.ToString());
            ClassicAssert.AreEqual(expectedValue.Score, actualMembers[0].Score);
        }

        [Test]
        public void CheckSortedSetDifferenceStoreWithNoMatchSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var keys = new[] { new RedisKey("user1:obj1"), new RedisKey("user1:obj2") };
            var destinationKey = new RedisKey("user1:objA");

            // Set up sorted sets
            db.SortedSetAdd(destinationKey, "Dummy", 10); // Set up destination key to overwrite if exists

            var actualCount = db.SortedSetCombineAndStore(SetOperation.Difference, destinationKey, keys);
            ClassicAssert.AreEqual(0, actualCount);

            var actualMembers = db.SortedSetRangeByScoreWithScores(destinationKey);
            ClassicAssert.AreEqual(0, actualMembers.Length);
        }

        [Test]
        [TestCase("board1", 1, Description = "Pop from single key")]
        [TestCase("board2", 3, Description = "Pop multiple elements")]
        public void SortedSetMultiPopTest(string key, int count)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.SortedSetAdd(key, entries);

            var result = db.Execute("ZMPOP", 1, key, "MIN", "COUNT", count);
            ClassicAssert.IsNotNull(result);
            var popResult = (RedisResult[])result;
            ClassicAssert.AreEqual(key, (string)popResult[0]);

            var poppedItems = (RedisResult[])popResult[1];
            ClassicAssert.AreEqual(Math.Min(count, entries.Length), poppedItems.Length / 2);

            if (count == 1)
            {
                ClassicAssert.AreEqual("a", (string)poppedItems[0]);
                ClassicAssert.AreEqual("1", (string)poppedItems[1]);
            }
        }

        [Test]
        [TestCase(new string[] { "board1" }, "MAX", 1, new string[] { "j" }, new double[] { 10.0 }, Description = "Pop maximum element from single key with count")]
        [TestCase(new string[] { "board1" }, "MIN", 1, new string[] { "a" }, new double[] { 1.0 }, Description = "Pop minimum element from single key with count")]
        [TestCase(new string[] { "board1" }, "MAX", 3, new string[] { "j", "i", "h" }, new double[] { 10.0, 9.0, 8.0 }, Description = "Pop multiple maximum elements from single key with count")]
        [TestCase(new string[] { "board1" }, "MIN", 3, new string[] { "a", "b", "c" }, new double[] { 1.0, 2.0, 3.0 }, Description = "Pop multiple minimum elements from single key with count")]
        [TestCase(new string[] { "board1", "nokey1" }, "MAX", 1, new string[] { "j" }, new double[] { 10.0 }, Description = "Pop maximum element from mixed existing and missing keys with count")]
        [TestCase(new string[] { "board1", "nokey1" }, "MIN", 1, new string[] { "a" }, new double[] { 1.0 }, Description = "Pop minimum element from mixed existing and missing keys with count")]
        [TestCase(new string[] { "nokey1", "nokey2" }, "MAX", 1, new string[] { }, new double[] { }, Description = "Pop maximum element from all missing keys with count")]
        [TestCase(new string[] { "nokey1", "nokey2" }, "MIN", 1, new string[] { }, new double[] { }, Description = "Pop minimum element from all missing keys with count")]
        [TestCase(new string[] { "board1", "nokey1" }, "MAX", null, new string[] { "j" }, new double[] { 10.0 }, Description = "Pop maximum element from mixed existing and missing keys without count")]
        [TestCase(new string[] { "board1", "nokey1" }, "MIN", null, new string[] { "a" }, new double[] { 1.0 }, Description = "Pop minimum element from mixed existing and missing keys without count")]
        public void SortedSetMultiPopWithOptionsTest(string[] keys, string direction, int? count, string[] expectedValues, double[] expectedScores)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            if (keys[0] == "board1")
            {
                db.SortedSetAdd(keys[0], entries);
            }

            List<object> commandArgs = [keys.Length, .. keys, direction];
            if (count.HasValue)
            {
                commandArgs.AddRange(["COUNT", count.Value]);
            }

            var result = db.Execute("ZMPOP", commandArgs);

            if (keys[0] == "board1")
            {
                ClassicAssert.IsNotNull(result);
                var popResult = (RedisResult[])result;
                ClassicAssert.AreEqual(keys[0], (string)popResult[0]);

                var valuesAndScores = (RedisResult[])popResult[1];
                for (int i = 0; i < expectedValues.Length; i++)
                {
                    ClassicAssert.AreEqual(expectedValues[i], (string)valuesAndScores[i * 2]);
                    ClassicAssert.AreEqual(expectedScores[i], (double)valuesAndScores[i * 2 + 1]);
                }
            }
            else
            {
                ClassicAssert.IsTrue(result.IsNull);
            }
        }

        [Test]
        public void SortedSetMultiPopWithFirstKeyEmptyOnSecondPopTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string[] keys = ["board1", "board2"];
            db.SortedSetAdd("board1", entries);
            db.SortedSetAdd("board2", leaderBoard);

            // First pop
            var result1 = db.Execute("ZMPOP", [keys.Length, keys[0], keys[1], "MAX", "COUNT", entries.Length]);
            ClassicAssert.IsNotNull(result1);
            var popResult1 = (RedisResult[])result1;
            ClassicAssert.AreEqual("board1", (string)popResult1[0]);

            // Second pop
            var result2 = db.Execute("ZMPOP", [keys.Length, keys[0], keys[1], "MIN"]);
            ClassicAssert.IsNotNull(result2);
            var popResult2 = (RedisResult[])result2;
            ClassicAssert.AreEqual("board2", (string)popResult2[0]);
        }

        #endregion

        #region LightClientTests

        /// <summary>
        /// This test exercises the SortedSet Comparer used in the Tsavorite resp commands
        /// </summary>
        [Test]
        public void CanHaveEqualScores()
        {
            SortedSet<(double, byte[])> sortedSet = new(new SortedSetComparer())
            {
                (340, Encoding.ASCII.GetBytes("Dave")),
                (400, Encoding.ASCII.GetBytes("Kendra")),
                (560, Encoding.ASCII.GetBytes("Tom")),
                (650, Encoding.ASCII.GetBytes("Barbara")),
                (690, Encoding.ASCII.GetBytes("Jennifer")),
                (690, Encoding.ASCII.GetBytes("Peter")),
                (740, Encoding.ASCII.GetBytes("Frank"))
            };
            var c = sortedSet.Count;
            ClassicAssert.AreEqual(7, c);

            //This simulates the ZCOUNT min max
            var r = sortedSet.Where(t => t.Item1 >= 500 && t.Item1 <= 700).Count();
            ClassicAssert.AreEqual(4, r);
        }

        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanDoZCountLC(int bytesPerSend)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommandChunks("ZADD board 340 Dave 400 Kendra 560 Tom 650 Barbara 690 Jennifer 690 Peter", bytesPerSend);
            var expectedResponse = ":6\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommands("ZCOUNT board 500 700", "PING");
            expectedResponse = ":4\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZCOUNT board 500 700", bytesPerSend);
            expectedResponse = ":4\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanDoZRangeByIndexLC(int bytesSent)
        {
            //ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 1 one");
            lightClientRequest.SendCommand("ZADD board 2 two");
            lightClientRequest.SendCommand("ZADD board 3 three");

            response = lightClientRequest.SendCommandChunks("ZRANGE board 0 -1", bytesSent, 4);
            var expectedResponse = "*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // get a range by index with scores
            response = lightClientRequest.SendCommandChunks("ZRANGE board 0 -1 WITHSCORES", bytesSent, 7);
            expectedResponse = "*6\r\n$3\r\none\r\n$1\r\n1\r\n$3\r\ntwo\r\n$1\r\n2\r\n$5\r\nthree\r\n$1\r\n3\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZRANGE board 2 3", 2);
            expectedResponse = "*1\r\n$5\r\nthree\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZRANGE board -2 -1", 3);
            expectedResponse = "*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZRANGE board -2 -1 WITHSCORES", 5);
            expectedResponse = "*4\r\n$3\r\ntwo\r\n$1\r\n2\r\n$5\r\nthree\r\n$1\r\n3\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZRANGE board -50 -1 WITHSCORES", 7);
            expectedResponse = "*6\r\n$3\r\none\r\n$1\r\n1\r\n$3\r\ntwo\r\n$1\r\n2\r\n$5\r\nthree\r\n$1\r\n3\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZRANGE board -50 -10 WITHSCORES", 1);
            expectedResponse = "*0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZRANGE board 2 1 WITHSCORES", 1);
            expectedResponse = "*0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZRANGE board -1 -2 WITHSCORES", 1);
            expectedResponse = "*0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZRANGE board 50 60 WITHSCORES", 1);
            expectedResponse = "*0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZRANGE board (1 +inf BYSCORE LIMIT 1 1", 2);
            expectedResponse = "*1\r\n$5\r\nthree\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZRANGE board (1 +inf BYSCORE LIMIT 1 1", bytesSent, 2);
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanDoZRangeByScoreLC(int bytesSent)
        {
            //ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 1 one");
            lightClientRequest.SendCommand("ZADD board 2 two");
            lightClientRequest.SendCommand("ZADD board 3 three");

            // 1 < score <= 5
            response = lightClientRequest.SendCommandChunks("ZRANGEBYSCORE board (1 5", bytesSent, 3);
            var expectedResponse = "*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // 1 < score <= 5
            response = lightClientRequest.SendCommands("ZRANGEBYSCORE board (1 5", "PING", 3, 1);
            expectedResponse = "*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanDoZRangeByScoreReverseLC(int bytesSent)
        {
            //ZRANGEBYSCORE key min max REV [WITHSCORES] [LIMIT offset count]
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 1 one");
            lightClientRequest.SendCommand("ZADD board 2 two");
            lightClientRequest.SendCommand("ZADD board 3 three");

            // 5 < score <= 1
            response = lightClientRequest.SendCommandChunks("ZRANGE board 2 5 BYSCORE REV", bytesSent, 1);
            var expectedResponse = "*0\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // 1 < score <= 5
            response = lightClientRequest.SendCommandChunks("ZRANGE board 5 2 BYSCORE REV", bytesSent, 3);
            expectedResponse = "*2\r\n$5\r\nthree\r\n$3\r\ntwo\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // 1 < score <= 5
            response = lightClientRequest.SendCommands("ZRANGE board 5 2 BYSCORE REV", "PING", 3, 1);
            expectedResponse = "*2\r\n$5\r\nthree\r\n$3\r\ntwo\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase(2)]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanDoZRangeByScoreWithLimitLC(int bytesSent)
        {
            // ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Bytes);

            var expectedResponse = ":3\r\n";
            var response = lightClientRequest.Execute("ZADD mysales 1556 Samsung 2000 Nokia 1800 Micromax", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = ":3\r\n";
            response = lightClientRequest.Execute("ZADD mysales 2200 Sunsui 1800 MicroSoft 2500 LG", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "*4\r\n$5\r\nNokia\r\n$4\r\n2000\r\n$6\r\nSunsui\r\n$4\r\n2200\r\n";
            response = lightClientRequest.Execute("ZRANGEBYSCORE mysales (1800 2200 WITHSCORES", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            // LIMIT
            expectedResponse = "*6\r\n$7\r\nSamsung\r\n$4\r\n1556\r\n$9\r\nMicroSoft\r\n$4\r\n1800\r\n$8\r\nMicromax\r\n$4\r\n1800\r\n";
            response = lightClientRequest.Execute("ZRANGEBYSCORE mysales -inf +inf WITHSCORES LIMIT 0 3", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "*4\r\n$6\r\nSunsui\r\n$4\r\n2200\r\n$2\r\nLG\r\n$4\r\n2500\r\n";
            response = lightClientRequest.Execute("ZRANGEBYSCORE mysales -inf +inf WITHSCORES LIMIT 4 10", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);
        }


        [Test]
        public void CanDoZRangeByLex()
        {
            //ZRANGE key min max BYLEX [WITHSCORES]
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 0 a");
            lightClientRequest.SendCommand("ZADD board 0 b");
            lightClientRequest.SendCommand("ZADD board 0 c");
            lightClientRequest.SendCommand("ZADD board 0 d");
            lightClientRequest.SendCommand("ZADD board 0 e");
            lightClientRequest.SendCommand("ZADD board 0 f");
            lightClientRequest.SendCommand("ZADD board 0 g");

            // get a range by lex order
            response = lightClientRequest.SendCommand("ZRANGE board (a (d BYLEX", 3);
            var expectedResponse = "*2\r\n$1\r\nb\r\n$1\r\nc\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //by lex with different range
            response = lightClientRequest.SendCommand("ZRANGE board [aaa (g BYLEX", 6);
            expectedResponse = "*5\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\nf\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //by lex with different range
            response = lightClientRequest.SendCommand("ZRANGE board - [c BYLEX", 4);
            expectedResponse = "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoZRangeByLexReverse()
        {
            //ZRANGE key min max BYLEX REV [WITHSCORES]
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 0 a");
            lightClientRequest.SendCommand("ZADD board 0 b");
            lightClientRequest.SendCommand("ZADD board 0 c");
            lightClientRequest.SendCommand("ZADD board 0 d");
            lightClientRequest.SendCommand("ZADD board 0 e");
            lightClientRequest.SendCommand("ZADD board 0 f");
            lightClientRequest.SendCommand("ZADD board 0 g");

            // get a range by lex order
            response = lightClientRequest.SendCommand("ZRANGE board (a (d BYLEX REV", 1);
            var expectedResponse = "*0\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // get a range by lex order
            response = lightClientRequest.SendCommand("ZRANGE board (d (a BYLEX REV", 3);
            expectedResponse = "*2\r\n$1\r\nc\r\n$1\r\nb\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //by lex with different range
            response = lightClientRequest.SendCommand("ZRANGE board [g (aaa BYLEX REV", 6);
            expectedResponse = "*5\r\n$1\r\nf\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //by lex with different range
            response = lightClientRequest.SendCommand("ZRANGE board [c - BYLEX REV", 4);
            expectedResponse = "*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoZRangeByLexWithLimit()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD mycity 0 Delhi 0 London 0 Paris 0 Tokyo 0 NewYork 0 Seoul");
            response = lightClientRequest.SendCommand("ZRANGE mycity (London + BYLEX", 5);
            var expectedResponse = "*4\r\n$7\r\nNewYork\r\n$5\r\nParis\r\n$5\r\nSeoul\r\n$5\r\nTokyo\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZRANGE mycity - + BYLEX LIMIT 2 3", 4);
            expectedResponse = "*3\r\n$7\r\nNewYork\r\n$5\r\nParis\r\n$5\r\nSeoul\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanDoZRangeByIndexReverse(int bytesSent)
        {
            //ZRANGE key min max REV
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 0 a");
            lightClientRequest.SendCommand("ZADD board 0 b");
            lightClientRequest.SendCommand("ZADD board 0 c");
            lightClientRequest.SendCommand("ZADD board 0 d");
            lightClientRequest.SendCommand("ZADD board 0 e");
            lightClientRequest.SendCommand("ZADD board 0 f");

            // get a range by lex order
            response = lightClientRequest.SendCommandChunks("ZRANGE board 0 -1 REV", bytesSent, 7);

            var expectedResponse = "*6\r\n$1\r\nf\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZRANGE board 0 -1 REV", 7);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanUseZRevRangeCitiesCommandInChunksLC(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommandChunks("ZADD cities 100000 Delhi 850000 Mumbai 700000 Hyderabad 800000 Kolkata", bytesSent);
            var expectedResponse = ":4\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZREVRANGE cities -2 -1 WITHSCORES", bytesSent, 5);
            expectedResponse = "*4\r\n$9\r\nHyderabad\r\n$6\r\n700000\r\n$5\r\nDelhi\r\n$6\r\n100000\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase(10, "MIN", 1, "*2\r\n$5\r\nboard\r\n*2\r\n$3\r\none\r\n$1\r\n1\r\n", Description = "Pop minimum with small chunk size")]
        [TestCase(100, "MAX", 3, "*2\r\n$5\r\nboard\r\n*6\r\n$4\r\nfive\r\n$1\r\n5\r\n$4\r\nfour\r\n$1\r\n4\r\n$5\r\nthree\r\n$1\r\n3\r\n", Description = "Pop maximum with large chunk size")]
        public void CanDoZMPopLC(int bytesSent, string direction, int count, string expectedResponse)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("ZADD board 1 one 2 two 3 three 4 four 5 five");

            var response = lightClientRequest.SendCommandChunks($"ZMPOP 1 board {direction} COUNT {count}", bytesSent);
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase("COUNT", Description = "Missing count value")]
        [TestCase("INVALID", Description = "Invalid direction")]
        public void CanManageZMPopErrorsLC(string invalidArg)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("ZADD board 1 one 2 two 3 three");

            var response = lightClientRequest.SendCommand($"ZMPOP 1 board MIN {invalidArg}");
            var expectedResponse = "-ERR syntax error\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoZMPopWithMultipleKeysLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("ZADD board1 1 one 2 two");
            lightClientRequest.SendCommand("ZADD board2 3 three 4 four");

            var response = lightClientRequest.SendCommand("ZMPOP 2 board1 board2 MIN");
            var expectedResponse = "*2\r\n$6\r\nboard1\r\n*2\r\n$3\r\none\r\n$1\r\n1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        #endregion

        #region NegativeTestsLC

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanValidateInvalidParamentersZCountLC(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Bytes);

            var expectedResponse = ":1\r\n";
            var response = lightClientRequest.Execute("ZADD board 400 Kendra", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = ":1\r\n";
            response = lightClientRequest.Execute("ZADD board 560 Tom", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT)}\r\n";
            response = lightClientRequest.Execute("ZCOUNT board 5 b", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);
        }


        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanManageErrorsInZCountLC(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("ZCOUNT nokey 12 232 4343 5454", "PING");
            var expectedResponse = $"{FormatWrongNumOfArgsError("ZCOUNT")}+PONG\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZCOUNT nokey 12 232 4343 5454", bytesSent);
            expectedResponse = FormatWrongNumOfArgsError("ZCOUNT");
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // no arguments
            response = lightClientRequest.SendCommandChunks("ZCOUNT nokey", bytesSent);
            expectedResponse = FormatWrongNumOfArgsError("ZCOUNT");
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // not found key
            response = lightClientRequest.SendCommandChunks("ZCOUNT nokey", bytesSent);
            expectedResponse = FormatWrongNumOfArgsError("ZCOUNT");
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZCOUNT nokey 1 2", bytesSent);
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommands("ZCOUNT nokey 1 2", "PING");
            expectedResponse = ":0\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanCreateNewSortedSetWithIncrbyLC(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommandChunks("ZINCRBY newboard 200 Tom", bytesSent);
            var expectedResponse = "$3\r\n200\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZCARD newboard", bytesSent);
            expectedResponse = ":1\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CreateLeaderBoardWithZADDWithStatusPending()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "LeaderBoard";

            var added = db.SortedSetAdd(key, leaderBoard);
            ClassicAssert.AreEqual(leaderBoard.Length, added);

            // 100 keys should be added
            for (int i = 0; i < 100; i++)
                db.SortedSetAdd(key + i, leaderBoard);

            added = db.SortedSetAdd(key, leaderBoard);
            ClassicAssert.AreEqual(0, added);

            var card = db.SortedSetLength(key);
            ClassicAssert.AreEqual(leaderBoard.Length, card);
        }


        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanAddDuplicateScoreLC(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommandChunks("ZADD board 340 Dave 400 Kendra 560 Tom 650 Barbara 690 Jennifer 690 Peter", bytesSent);
            var expectedResponse = ":6\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //get the number of elements in the Sorted Set
            response = lightClientRequest.SendCommand("ZCARD board");
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZCARD board", bytesSent);
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanDoIncrByLC(int bytesSent)
        {
            //ZINCRBY key increment member
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 340 Dave");
            lightClientRequest.SendCommand("ZADD board 400 Kendra");
            lightClientRequest.SendCommand("ZADD board 560 Tom");

            response = lightClientRequest.SendCommandChunks("ZINCRBY board 10 Tom", bytesSent);
            var expectedResponse = "$3\r\n570\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZINCRBY board -590 Tom");
            expectedResponse = "$3\r\n-20\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZINCRBY board -590", bytesSent);
            expectedResponse = FormatWrongNumOfArgsError("ZINCRBY");
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //using key exists but non existing member
            response = lightClientRequest.SendCommandChunks("ZINCRBY board 10 Julia", bytesSent);
            expectedResponse = "$2\r\n10\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZCARD board", bytesSent);
            expectedResponse = ":4\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

        }

        [Test]
        public void CanManageNoParametersInZIncrbyLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("ZINCRBY nokey", "PING");
            var expectedResponse = $"{FormatWrongNumOfArgsError("ZINCRBY")}+PONG\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanManageExistingKeyButOtherTypeLC(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            //create a hash myboard
            lightClientRequest.SendCommand("HSET myboard field1 myvalue");

            //do zincrby
            var response = lightClientRequest.SendCommandChunks("ZINCRBY myboard 1 field1", bytesSent);
            var expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE)}\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanUseZRevRange()
        {
            //ZREVRANGE key start stop [WITHSCORES]
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 10 a");
            lightClientRequest.SendCommand("ZADD board 20 b");
            lightClientRequest.SendCommand("ZADD board 30 c");
            lightClientRequest.SendCommand("ZADD board 40 d");
            lightClientRequest.SendCommand("ZADD board 50 e");
            lightClientRequest.SendCommand("ZADD board 60 f");

            // get a range by lex order
            response = lightClientRequest.SendCommand("ZREVRANGE board 0 -1", 7);
            var expectedResponse = "*6\r\n$1\r\nf\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);


            // including scores
            response = lightClientRequest.SendCommand("ZREVRANGE board 0 -1 WITHSCORES", 13);
            expectedResponse = "*12\r\n$1\r\nf\r\n$2\r\n60\r\n$1\r\ne\r\n$2\r\n50\r\n$1\r\nd\r\n$2\r\n40\r\n$1\r\nc\r\n$2\r\n30\r\n$1\r\nb\r\n$2\r\n20\r\n$1\r\na\r\n$2\r\n10\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZREVRANGE board 0 1", 3);
            expectedResponse = "*2\r\n$1\r\nf\r\n$1\r\ne\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanUseZRevRangeByScore()
        {
            //ZREVRANGESCORE key start stop [WITHSCORES] [LIMIT offset count]
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 10 a");
            lightClientRequest.SendCommand("ZADD board 20 b");
            lightClientRequest.SendCommand("ZADD board 30 c");
            lightClientRequest.SendCommand("ZADD board 40 d");
            lightClientRequest.SendCommand("ZADD board 50 e");
            lightClientRequest.SendCommand("ZADD board 60 f");

            // get a reverse range by score order
            response = lightClientRequest.SendCommand("ZREVRANGEBYSCORE board 70 0", 7);
            var expectedResponse = "*6\r\n$1\r\nf\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // including scores
            response = lightClientRequest.SendCommand("ZREVRANGEBYSCORE board 60 10 WITHSCORES", 13);
            expectedResponse = "*12\r\n$1\r\nf\r\n$2\r\n60\r\n$1\r\ne\r\n$2\r\n50\r\n$1\r\nd\r\n$2\r\n40\r\n$1\r\nc\r\n$2\r\n30\r\n$1\r\nb\r\n$2\r\n20\r\n$1\r\na\r\n$2\r\n10\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZREVRANGEBYSCORE board +inf 45", 3);
            expectedResponse = "*2\r\n$1\r\nf\r\n$1\r\ne\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZREVRANGEBYSCORE board 70 45 LIMIT 0 1", 2);
            expectedResponse = "*1\r\n$1\r\nf\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanDoZRankLC(int bytesSent)
        {
            //ZRANK key member
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 340 Dave");
            lightClientRequest.SendCommand("ZADD board 400 Kendra");
            lightClientRequest.SendCommand("ZADD board 560 Tom");

            response = lightClientRequest.SendCommandChunks("ZRANK board Jon", bytesSent);
            var expectedResponse = "$-1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZRANK board Tom INVALIDOPTION", bytesSent);
            expectedResponse = "-ERR syntax error\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZRANK board Tom", bytesSent);
            expectedResponse = ":2\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZRANK board Tom withscore", bytesSent);
            expectedResponse = "*2\r\n:2\r\n$3\r\n560\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanDoZRevRankLC(int bytesSent)
        {
            //ZREVRANK key member
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 340 Dave");
            lightClientRequest.SendCommand("ZADD board 400 Kendra");
            lightClientRequest.SendCommand("ZADD board 560 Tom");

            response = lightClientRequest.SendCommandChunks("ZREVRANK board Jon", bytesSent);
            var expectedResponse = "$-1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZREVRANK board Tom INVALIDOPTION", bytesSent);
            expectedResponse = "-ERR syntax error\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZREVRANK board Tom", bytesSent);
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZREVRANK board Kendra", bytesSent);
            expectedResponse = ":1\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZREVRANK board Dave", bytesSent);
            expectedResponse = ":2\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZREVRANK board Dave WITHSCORE", bytesSent);
            expectedResponse = "*2\r\n:2\r\n$3\r\n340\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanDoZRemRangeByLexLC(int bytesSent)
        {
            //ZREMRANGEBYLEX key member
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD myzset 0 aaaa 0 b 0 c 0 d 0 e");
            lightClientRequest.SendCommand("ZADD myzset 0 foo 0 zap 0 zip 0 ALPHA 0 alpha");
            response = lightClientRequest.SendCommand("ZRANGE myzset 0 -1", 11);
            var expectedResponse = "*10\r\n$5\r\nALPHA\r\n$4\r\naaaa\r\n$5\r\nalpha\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$3\r\nfoo\r\n$3\r\nzap\r\n$3\r\nzip\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZREMRANGEBYLEX myzset [alpha [omega", bytesSent);
            expectedResponse = ":6\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZRANGE myzset 0 -1", bytesSent, 5);
            expectedResponse = "*4\r\n$5\r\nALPHA\r\n$4\r\naaaa\r\n$3\r\nzap\r\n$3\r\nzip\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanDoZRemRangeByRank(int bytesSent)
        {
            //ZREMRANGEBYRANK key start stop
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 340 Dave");
            lightClientRequest.SendCommand("ZADD board 400 Kendra");
            lightClientRequest.SendCommand("ZADD board 560 Tom");

            response = lightClientRequest.SendCommand("ZADD myzset 0 aaaa 0 b 0 c 0 d 0 e");
            lightClientRequest.SendCommand("ZADD myzset 0 foo 0 zap 0 zip 0 ALPHA 0 alpha");
            response = lightClientRequest.SendCommand("ZRANGE myzset 0 -1", 11);
            var expectedResponse = "*10\r\n$5\r\nALPHA\r\n$4\r\naaaa\r\n$5\r\nalpha\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$3\r\nfoo\r\n$3\r\nzap\r\n$3\r\nzip\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZREMRANGEBYLEX myzset [alpha [omega", bytesSent);
            expectedResponse = ":6\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZREMRANGEBYLEX myzset =a .", bytesSent);
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING)}\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZRANGE myzset 0 -1", bytesSent, 5);
            expectedResponse = "*4\r\n$5\r\nALPHA\r\n$4\r\naaaa\r\n$3\r\nzap\r\n$3\r\nzip\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZREMRANGEBYRANK board a b", bytesSent);
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER)}\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZREMRANGEBYRANK board 0 1", bytesSent);
            expectedResponse = ":2\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommands("ZREMRANGEBYRANK board 0 1", "PING");
            expectedResponse = ":1\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanDoZRemRangeByScore(int bytesSent)
        {
            //ZREMRANGEBYSCORE key min max
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 340 Dave");
            lightClientRequest.SendCommand("ZADD board 400 Kendra");
            lightClientRequest.SendCommand("ZADD board 560 Tom");

            response = lightClientRequest.SendCommandChunks("ZREMRANGEBYSCORE board -inf (500", bytesSent);
            var expectedResponse = ":2\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZCARD board", bytesSent);
            expectedResponse = ":1\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZREMRANGEBYSCORE board a b", bytesSent);
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT)}\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanSendErrorZRangeWithLimit(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 0 a");
            lightClientRequest.SendCommand("ZADD board 0 b");
            lightClientRequest.SendCommand("ZADD board 0 c");

            response = lightClientRequest.SendCommandChunks("ZRANGE board 0 -1 LIMIT 1 1", bytesSent);
            var expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_LIMIT_NOT_SUPPORTED)}\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanUseZLexCount(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 0 a 0 b 0 c 0 d 0 e 0 f 0 g");

            response = lightClientRequest.SendCommandChunks("ZLEXCOUNT board - +", bytesSent);
            var expectedResponse = ":7\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZLEXCOUNT board [b [f", bytesSent);
            expectedResponse = ":5\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZLEXCOUNT board *d 8", bytesSent);
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING)}\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanUseZPopMin(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 1 one 2 two 3 three");

            response = lightClientRequest.SendCommandChunks("ZPOPMIN board", bytesSent);
            var expectedResponse = "*2\r\n$3\r\none\r\n$1\r\n1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZPOPMIN board 3", bytesSent, 5);
            expectedResponse = "*4\r\n$3\r\ntwo\r\n$1\r\n2\r\n$5\r\nthree\r\n$1\r\n3\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanUseZRandMember(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            _ = lightClientRequest.SendCommand("ZADD dadi 1 uno 2 due 3 tre 4 quattro 5 cinque 6 six 7 sept 8 huit 9 nough 10 dis");

            // ZRANDMEMBER
            var s = Encoding.ASCII.GetString(lightClientRequest.SendCommandChunks("ZRANDMEMBER dadi", bytesSent));
            int startIndexField = s.IndexOf('\n') + 1;
            int endIndexField = s.IndexOf('\n', startIndexField) - 1;
            string memberValue = s.Substring(startIndexField, endIndexField - startIndexField);
            var foundInSet = ("uno due tre quattro cinque six sept huit nough dis").IndexOf(memberValue, StringComparison.InvariantCultureIgnoreCase);
            ClassicAssert.IsTrue(foundInSet >= 0);

            // ZRANDMEMBER count
            var response = lightClientRequest.SendCommandChunks("ZRANDMEMBER dadi 5", bytesSent, 6);
            var expectedResponse = "*5\r\n"; // 5 values
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // ZRANDMEMBER [count [WITHSCORES]]
            response = lightClientRequest.SendCommandChunks("ZRANDMEMBER dadi 3 WITHSCORES", bytesSent, 7);
            expectedResponse = "*6\r\n"; // 3 keyvalue pairs
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZRANDMEMBER dadi 1 WITHSCORES", bytesSent, 3);
            expectedResponse = "*2\r\n"; // 2 elements
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("ZRANDMEMBER dadi 0 WITHSCORES", bytesSent);
            expectedResponse = "*0\r\n"; // Empty List
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public async Task CanUseZRandMemberWithSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "SortedSet_Add";

            // no-existing key case
            var key0 = "SortedSet_Foo";

            db.KeyDelete(key, CommandFlags.FireAndForget);
            db.SortedSetAdd(key, powOfTwo, CommandFlags.FireAndForget);

            var randMember = await db.SortedSetRandomMemberAsync(key);
            ClassicAssert.True(Array.Exists(powOfTwo, element => element.Element.Equals(randMember)));

            // Check ZRANDMEMBER with wrong number of arguments
            var ex = Assert.Throws<RedisServerException>(() => db.Execute("ZRANDMEMBER", key, 3, "WITHSCORES", "bla"));
            var expectedMessage = string.Format(CmdStrings.GenericErrWrongNumArgs, nameof(RespCommand.ZRANDMEMBER));
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedMessage, ex.Message);

            // Check ZRANDMEMBER with non-numeric count
            ex = Assert.Throws<RedisServerException>(() => db.Execute("ZRANDMEMBER", key, "bla"));
            expectedMessage = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedMessage, ex.Message);

            // Check ZRANDMEMBER with syntax error
            ex = Assert.Throws<RedisServerException>(() => db.Execute("ZRANDMEMBER", key, 3, "withscore"));
            expectedMessage = Encoding.ASCII.GetString(CmdStrings.RESP_SYNTAX_ERROR);
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedMessage, ex.Message);

            //ZRANDMEMBER count
            var randMemberArray = await db.SortedSetRandomMembersAsync(key, 5);
            ClassicAssert.AreEqual(5, randMemberArray.Length);
            ClassicAssert.AreEqual(5, randMemberArray.Distinct().Count());
            foreach (var member in randMemberArray)
            {
                var match = powOfTwo.FirstOrDefault(pt => pt.Element == member);
                ClassicAssert.IsNotNull(match);
            }

            randMemberArray = await db.SortedSetRandomMembersAsync(key, 15);
            ClassicAssert.AreEqual(10, randMemberArray.Length);
            ClassicAssert.AreEqual(10, randMemberArray.Distinct().Count());
            foreach (var member in randMemberArray)
            {
                var match = powOfTwo.FirstOrDefault(pt => pt.Element == member);
                ClassicAssert.IsNotNull(match);
            }

            randMemberArray = await db.SortedSetRandomMembersAsync(key, -5);
            ClassicAssert.AreEqual(5, randMemberArray.Length);

            randMemberArray = await db.SortedSetRandomMembersAsync(key, -15);
            ClassicAssert.AreEqual(15, randMemberArray.Length);
            ClassicAssert.GreaterOrEqual(10, randMemberArray.Distinct().Count());
            foreach (var member in randMemberArray)
            {
                var match = powOfTwo.FirstOrDefault(pt => pt.Element == member);
                ClassicAssert.IsNotNull(match);
            }

            //ZRANDMEMBER [count [WITHSCORES]]
            var randMemberArray2 = await db.SortedSetRandomMembersWithScoresAsync(key, 2);
            ClassicAssert.AreEqual(2, randMemberArray2.Length);
            foreach (var member in randMemberArray2)
            {
                ClassicAssert.Contains(member, powOfTwo);
            }

            // No-existing key case
            randMember = await db.SortedSetRandomMemberAsync(key0);
            ClassicAssert.True(randMember.IsNull);
            randMemberArray = await db.SortedSetRandomMembersAsync(key0, 2);
            ClassicAssert.True(randMemberArray.Length == 0);
            randMemberArray2 = await db.SortedSetRandomMembersWithScoresAsync(key0, 2);
            ClassicAssert.True(randMemberArray2.Length == 0);
        }


        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanUseZDiff(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("ZADD dadi 1 uno 2 due 3 tre 4 quattro 5 cinque 6 sei");
            lightClientRequest.SendCommand("ZADD seconddadi 1 uno 2 due 3 tre 4 quattro");

            //zdiff withscores
            var zdiffResult = lightClientRequest.SendCommandChunks("ZDIFF 2 dadi seconddadi WITHSCORES", bytesSent, 5);
            var expectedResponse = "*4\r\n$6\r\ncinque\r\n$1\r\n5\r\n$3\r\nsei\r\n$1\r\n6\r\n";
            var actualValue = Encoding.ASCII.GetString(zdiffResult).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            zdiffResult = lightClientRequest.SendCommandChunks("ZDIFF 2 dadi seconddadi", bytesSent, 3);
            expectedResponse = "*2\r\n$6\r\ncinque\r\n$3\r\nsei\r\n";
            actualValue = Encoding.ASCII.GetString(zdiffResult).Substring(0, expectedResponse.Length);
        }

        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanUseZDiffSTORE(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            //zdiff withscores
            var zdiffResult = lightClientRequest.SendCommandChunks("ZDIFFSTORE desKey 2 dadi seconddadi", bytesSent);
            var expectedResponse = ":0\r\n";
            var actualValue = Encoding.ASCII.GetString(zdiffResult).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            lightClientRequest.SendCommand("ZADD dadi 1 uno 2 due 3 tre 4 quattro 5 cinque 6 sei");
            lightClientRequest.SendCommand("ZADD seconddadi 1 uno 2 due 3 tre 4 quattro");

            zdiffResult = lightClientRequest.SendCommandChunks("ZDIFFSTORE desKey 2 dadi seconddadi", bytesSent);
            expectedResponse = "1\r\n";
            actualValue = Encoding.ASCII.GetString(zdiffResult).Substring(0, expectedResponse.Length);
        }

        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanUseZDiffMultipleKeys(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("ZADD zset1 1 uno 2 due 3 tre 4 quattro 5 cinque 6 sei");
            lightClientRequest.SendCommand("ZADD zset2 1 uno 2 due 3 tre 4 quattro");
            lightClientRequest.SendCommand("ZADD zset3 300 Jean 500 Leia 350 Grant 700 Rue");

            var zdiffResult = lightClientRequest.SendCommandChunks("ZDIFF 3 zset1 zset2 zset3", bytesSent, 3);
            var expectedResponse = "*2\r\n$6\r\ncinque\r\n$3\r\nsei\r\n";
            var actualValue = Encoding.ASCII.GetString(zdiffResult).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // Zdiff withscores
            zdiffResult = lightClientRequest.SendCommandChunks("ZDIFF 3 zset1 zset2 zset3 WITHSCORES", bytesSent, 5);
            expectedResponse = "*4\r\n$6\r\ncinque\r\n$1\r\n5\r\n$3\r\nsei\r\n$1\r\n6\r\n";
            actualValue = Encoding.ASCII.GetString(zdiffResult).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanUseZDiffWithNull()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("ZADD zset1 1 uno 2 due 3 tre 4 quattro 5 cinque 6 sei");
            var zdiffResult = lightClientRequest.SendCommand("ZDIFF 2 zsetnotfound zset1");
            var expectedResponse = "*0\r\n";
            var actualValue = Encoding.ASCII.GetString(zdiffResult).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            zdiffResult = lightClientRequest.SendCommand("ZDIFF 2 zsetnotfound zset1notfound");
            expectedResponse = "*0\r\n";
            actualValue = Encoding.ASCII.GetString(zdiffResult).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanManageNotExistingKeySortedSetCommandsReadOps()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            var result = lightClientRequest.SendCommand("ZCARD nokey");
            var expectedResponse = ":0\r\n";
            var actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZSCORE nokey a");
            expectedResponse = "$-1\r\n"; // NULL
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZRANK noboard a");
            expectedResponse = "$-1\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZRANGE unekey 0 1");
            expectedResponse = "*0\r\n"; //empty array
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZRANGEBYSCORE nonekey 0 1");
            expectedResponse = "*0\r\n"; //empty array
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZREVRANGE nonekey 0 1");
            expectedResponse = "*0\r\n"; //empty array
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZREVRANK nonekey 0");
            expectedResponse = "$-1\r\n"; // NULL
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZREVRANGE nonekey 0 1");
            expectedResponse = "*0\r\n"; //empty array
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZLEXCOUNT nonekey [a [f");
            expectedResponse = ":0\r\n"; //integer reply
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //without the additional count argument, the command returns nil when key does not exist.
            result = lightClientRequest.SendCommand("ZRANDMEMBER nonekey");
            expectedResponse = "$-1\r\n"; //nil when key does not exist.
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //when the additional count argument is passed, the command returns an array of elements,
            //or an empty array when key does not exist.
            result = lightClientRequest.SendCommand("ZRANDMEMBER nonekey 1");
            expectedResponse = "*0\r\n"; //empty array
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZDIFF 2 i i");
            expectedResponse = "*0\r\n"; //empty array
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZDIFF 1 nonekey");
            expectedResponse = "*0\r\n"; //empty array
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanManageNotExistingKeySortedSetCommandsRMWOps()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            var result = lightClientRequest.SendCommand("ZPOPMAX nokey");
            var expectedResponse = "*0\r\n";
            var actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZREM nokey a");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //for this case the boundaries arguments are wrong, in redis this validation occurs
            //before the validation of a non existing key, but we are not executing the backend until
            //the key is validated first.
            result = lightClientRequest.SendCommand("ZREMRANGEBYLEX nokey 0 1");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //testing out only a nonexisting key
            result = lightClientRequest.SendCommand("ZREMRANGEBYLEX nokey [a [b");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZREMRANGEBYLEX iii [a [b");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZREM nokey a");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZREMRANGEBYRANK nokey 0 1");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZREMRANGEBYSCORE nokey 0 1");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            lightClientRequest.SendCommand("ZADD zset1 1 uno 2 due 3 tre 4 quattro 5 cinque 6 sei");

            result = lightClientRequest.SendCommand("ZREMRANGEBYLEX zset1 0 1");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING)}\r\n"; ;
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

        }


        /// <summary>
        /// This test executes an RMW followed by a Read method which none
        /// of them should create a new key
        /// </summary>
        [Test]
        public void CanManageRMWAndReadInCommands()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            // Executing an RMW method first
            var result = lightClientRequest.SendCommand("ZPOPMAX nokey");
            var expectedResponse = "*0\r\n";
            var actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // When the additional count argument is passed, the command returns an array of elements,
            // Or an empty array when key does not exist.
            result = lightClientRequest.SendCommand("ZRANDMEMBER nokey 1");
            expectedResponse = "*0\r\n"; //empty array
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            result = lightClientRequest.SendCommand("ZCARD nokey");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void CanManageAddAndDelete()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommandChunks("ZADD board 340 Dave 400 Kendra 560 Tom 650 Barbara 690 Jennifer 690 Peter", 100);
            var expectedResponse = ":6\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZPOPMAX board", 3);
            expectedResponse = "*2\r\n$5\r\nPeter\r\n$3\r\n690\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZPOPMAX board 2", 5);
            expectedResponse = "*4\r\n$8\r\nJennifer\r\n$3\r\n690\r\n$7\r\nBarbara\r\n$3\r\n650\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommands("DEL board", "PING");
            expectedResponse = ":1\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // Check the key is null or empty
            response = lightClientRequest.SendCommand("ZPOPMAX board");
            expectedResponse = "*0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "LeaderBoard";

            var added = db.SortedSetAdd(key, leaderBoard);
            ClassicAssert.AreEqual(leaderBoard.Length, added);
            var removed = db.KeyDelete(key);

            // ZPOPMAX
            var actualResult = db.SortedSetPop(new RedisKey(key), Order.Descending);
            ClassicAssert.AreEqual(null, actualResult);
        }

        [Test]
        public void CanManageKeyAbscentInCommands()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            SendCommandWithoutKey("ZCARD", lightClientRequest);
            SendCommandWithoutKey("ZPOPMAX", lightClientRequest);
            SendCommandWithoutKey("ZSCORE", lightClientRequest);
            SendCommandWithoutKey("ZREM", lightClientRequest);
            SendCommandWithoutKey("ZINCRBY", lightClientRequest);
            SendCommandWithoutKey("ZCARD", lightClientRequest);
            SendCommandWithoutKey("ZCOUNT", lightClientRequest);
        }

        [Test]
        public async Task CanFailWhenUseMultiWatchTest()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            string key = "MySSKey";
            byte[] res;

            res = lightClientRequest.SendCommand($"ZADD {key} 1 a 2 b 3 c");
            string expectedResponse = ":3\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand($"WATCH {key}");
            expectedResponse = "+OK\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("MULTI");
            expectedResponse = "+OK\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand($"ZREM {key} a");
            expectedResponse = "+QUEUED\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            await Task.Run(() => UpdateSortedSetKey(key));

            res = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // This sequence should work
            res = lightClientRequest.SendCommand("MULTI");
            expectedResponse = "+OK\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand($"ZADD {key} 7 g");
            expectedResponse = "+QUEUED\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // This should commit
            res = lightClientRequest.SendCommand("EXEC", 2);
            expectedResponse = "*1\r\n:1\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
        }

        [Test]
        public void CanUseMultiTest()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            byte[] res;

            res = lightClientRequest.SendCommand("ZADD MySSKey 1 a 2 b 3 c");
            string expectedResponse = ":3\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("MULTI");
            expectedResponse = "+OK\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("ZREM MySSKey a");
            expectedResponse = "+QUEUED\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("ZADD MySSKey 7 g");
            expectedResponse = "+QUEUED\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("EXEC", 3);
            expectedResponse = "*2\r\n:1\r\n:1\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
        }

        [Test]
        public void CanFastForwardExtraArguments()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZSCORE foo bar foo bar foo");
            var expectedResponse = FormatWrongNumOfArgsError("ZSCORE");
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // Add a large number
            response = lightClientRequest.SendCommand("ZADD zset1 -9007199254740992 uno");
            expectedResponse = ":1\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("ZSCORE zset1 uno");
            expectedResponse = $"$17\r\n-9007199254740992\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void CanDoZaddWithInvalidInput()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var key = "zset1";
            var response = lightClientRequest.SendCommand($"ZADD {key} 1 uno 2 due 3 tre 4 quattro 5 cinque foo bar");
            var expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_NOT_VALID_FLOAT)}\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            key = "zset2";
            response = lightClientRequest.SendCommand($"ZADD {key} 1 uno 2 due 3 tre foo bar 4 quattro 5 cinque");
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            key = "zset3";
            response = lightClientRequest.SendCommand($"ZADD {key} foo bar 1 uno 2 due 3 tre 4 quattro 5 cinque");
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CheckSortedSetOperationsOnWrongTypeObjectLC()
        {
            // This is to test remaining commands that were not covered in CheckSortedSetOperationsOnWrongTypeObjectLC
            // since they are not supported in SE.Redis
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            using var lightClientRequest = TestUtils.CreateRequest();

            var keys = new[] { new RedisKey("user1:obj1"), new RedisKey("user1:obj2") };
            var key1Values = new[] { new RedisValue("Hello"), new RedisValue("World") };
            var key2Values = new[] { new RedisValue("Hola"), new RedisValue("Mundo") };
            var values = new[] { key1Values, key2Values };

            // Set up different type objects
            RespTestsUtils.SetUpTestObjects(db, GarnetObjectType.Set, keys, values);

            // ZRANGE
            var response = lightClientRequest.SendCommand($"ZRANGE {keys[0]} 0 -1");
            var expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE)}\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);


            // ZREVRANGE
            response = lightClientRequest.SendCommand($"ZREVRANGE {keys[0]} 0 -1");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE)}\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // ZREMRANGEBYLEX
            response = lightClientRequest.SendCommand($"ZREMRANGEBYLEX {keys[0]} {values[1][0]} {values[1][1]}");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE)}\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // ZREVRANGEBYSCORE
            response = lightClientRequest.SendCommand($"ZREVRANGEBYSCORE {keys[0]} 0 -1");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE)}\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        #endregion

        private static void SendCommandWithoutKey(string command, LightClientRequest lightClientRequest)
        {
            var result = lightClientRequest.SendCommand(command);
            var expectedResponse = FormatWrongNumOfArgsError(command);
            var actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        private static void UpdateSortedSetKey(string keyName)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            byte[] res = lightClientRequest.SendCommand($"ZADD {keyName} 4 d");
            string expectedResponse = ":1\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
        }

        private static string FormatWrongNumOfArgsError(string commandName) => $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, commandName)}\r\n";
    }

    public class SortedSetComparer : IComparer<(double, byte[])>
    {
        public int Compare((double, byte[]) x, (double, byte[]) y)
        {
            var ret = x.Item1.CompareTo(y.Item1);
            if (ret == 0)
                return new ReadOnlySpan<byte>(x.Item2).SequenceCompareTo(y.Item2);
            return ret;
        }
    }
}