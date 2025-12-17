// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Embedded.server;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;
using SetOperation = StackExchange.Redis.SetOperation;

namespace Garnet.test
{
    using TestBasicGarnetApi = GarnetApi<BasicContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        BasicContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        BasicContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>>;

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
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableReadCache: true, enableAOF: true, lowMemory: true);
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

            var session = new RespServerSession(0, new EmbeddedNetworkSender(), server.Provider.StoreWrapper, null, null, false);
            var api = new TestBasicGarnetApi(session.storageSession, session.storageSession.stringBasicContext,
                session.storageSession.objectBasicContext, session.storageSession.unifiedBasicContext);
            var key = Encoding.ASCII.GetBytes("key1");
            fixed (byte* keyPtr = key)
            {
                var result = api.SortedSetPop(PinnedSpanByte.FromPinnedPointer(keyPtr, key.Length), out var items);
                ClassicAssert.AreEqual(1, items.Length);
                ClassicAssert.AreEqual("a", Encoding.ASCII.GetString(items[0].member.ReadOnlySpan));
                ClassicAssert.AreEqual("1", Encoding.ASCII.GetString(items[0].score.ReadOnlySpan));

                result = api.SortedSetPop(PinnedSpanByte.FromPinnedPointer(keyPtr, key.Length), out items);
                ClassicAssert.AreEqual(1, items.Length);
                ClassicAssert.AreEqual("b", Encoding.ASCII.GetString(items[0].member.ReadOnlySpan));
                ClassicAssert.AreEqual("2", Encoding.ASCII.GetString(items[0].score.ReadOnlySpan));
            }
        }

        [Test]
        public unsafe void SortedSetPopWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);

            // Set expiration for the minimum and maximum items
            db.Execute("ZPEXPIRE", "key1", "100", "MEMBERS", "2", "a", "c");

            // Wait for expiration
            Thread.Sleep(200);

            var session = new RespServerSession(0, new EmbeddedNetworkSender(), server.Provider.StoreWrapper, null, null, false);
            var api = new TestBasicGarnetApi(session.storageSession, session.storageSession.stringBasicContext,
                session.storageSession.objectBasicContext, session.storageSession.unifiedBasicContext);
            var key = Encoding.ASCII.GetBytes("key1");
            fixed (byte* keyPtr = key)
            {
                var result = api.SortedSetPop(PinnedSpanByte.FromPinnedPointer(keyPtr, key.Length), out var items);
                ClassicAssert.AreEqual(1, items.Length);
                ClassicAssert.AreEqual("b", Encoding.ASCII.GetString(items[0].member.ReadOnlySpan));
                ClassicAssert.AreEqual("2", Encoding.ASCII.GetString(items[0].score.ReadOnlySpan));

                var count = (int)db.SortedSetLength("key1");
                ClassicAssert.AreEqual(0, count);
            }
        }

        [Test]
        public async Task SortedSetAddWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);

            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "1", "c");

            await Task.Delay(300);

            var ttl = db.Execute("ZPTTL", "key1", "MEMBERS", "1", "c");
            ClassicAssert.AreEqual(-2, (long)ttl);

            db.SortedSetAdd("key1", "c", 3);
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "b", "c");

            var ttls = db.Execute("ZPTTL", "key1", "MEMBERS", "2", "b", "c");
            ClassicAssert.LessOrEqual((long)ttls[0], 200);
            ClassicAssert.Greater((long)ttls[0], 0);
            ClassicAssert.LessOrEqual((long)ttls[1], 200);
            ClassicAssert.Greater((long)ttls[1], 0);

            // Add the expiring item "c" again, which should remove the expiration. Score is not changed
            db.SortedSetAdd("key1", "c", 3);
            // Add the expiring item "b" again, which should remove the expiration. Score is changed
            db.SortedSetAdd("key1", "b", 1);

            ttls = db.Execute("ZPTTL", "key1", "MEMBERS", "2", "b", "c");
            ClassicAssert.AreEqual(-1, (long)ttls[0]);
            ClassicAssert.AreEqual(-1, (long)ttls[1]);

            var items = db.SortedSetRangeByRankWithScores("key1");
            ClassicAssert.AreEqual(2, items.Length);
            ClassicAssert.AreEqual("b", items[0].Element.ToString());
            ClassicAssert.AreEqual(1, items[0].Score);
            ClassicAssert.AreEqual("c", items[1].Element.ToString());
            ClassicAssert.AreEqual(3, items[1].Score);
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
            var expectedResponse = 1832;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var entries2 = new SortedSetEntry[entries.Length + 1];
            entries.CopyTo(entries2, 0);
            entries2[entries2.Length - 1] = new SortedSetEntry("k", 11);

            // only 1 new entry should get added
            added = db.SortedSetAdd(key, entries2);
            ClassicAssert.AreEqual(1, added);

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 1992;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // no new entries get added
            added = db.SortedSetAdd(key, entries2);
            ClassicAssert.AreEqual(0, added);

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            card = db.SortedSetLength(key);
            ClassicAssert.AreEqual(entries2.Length, card);

            added = db.SortedSetAdd(key, [new SortedSetEntry("a", 12)]);
            ClassicAssert.AreEqual(0, added);

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
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

            var lex = db.SortedSetRangeByValue(key, default, "c");
            CollectionAssert.AreEqual(new RedisValue[] { "a", "b", "c" }, lex);

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
            lex = db.SortedSetRangeByValue(key, default, "c");
            CollectionAssert.AreEqual(new RedisValue[] { "a", "c", "b" }, lex);
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
            lex = db.SortedSetRangeByValue(key, default, "c");
            CollectionAssert.AreEqual(new RedisValue[] { "a", "c", "b" }, lex);
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
            lex = db.SortedSetRangeByValue(key, default, "c");
            CollectionAssert.AreEqual(new RedisValue[] { "a", "b", "c" }, lex);
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
            lex = db.SortedSetRangeByValue(key, default, "c");
            CollectionAssert.AreEqual(new RedisValue[] { "b", "c", "a" }, lex);
            scores = db.SortedSetScores(key, [new RedisValue("a"), new RedisValue("b"), new RedisValue("n")]);
            CollectionAssert.AreEqual(new double[] { 4, 3, 14 }, scores);
            count = db.SortedSetLength(key);
            ClassicAssert.AreEqual(14, count);

            // CH - Modify the return value from the number of new elements added, to the total number of elements changed
            var testArgs = new string[]
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
            testArgs = ["INCR", "3.5", "a"];

            // INCR as ZINCRBY
            resp = db.Execute("ZADD", [key, .. testArgs]);
            ClassicAssert.AreEqual(4.5, double.Parse(resp.ToString(), CultureInfo.InvariantCulture));

            // Test NX option.
            resp = db.Execute("ZADD", [key, "NX", .. testArgs]);
            ClassicAssert.IsTrue(resp.IsNull);

            resp = db.Execute("ZADD", [key + '2', "NX", .. testArgs]);
            ClassicAssert.AreEqual(3.5, double.Parse(resp.ToString(), CultureInfo.InvariantCulture));

            // INCR + LT/GT combination should prevent update when condition fails and key exists.
            resp = db.Execute("ZADD", [key, "LT", .. testArgs]);
            ClassicAssert.IsTrue(resp.IsNull);

            // Condition does not prevent creation of new value though.
            resp = db.Execute("ZADD", [key + '3', "LT", .. testArgs]);
            ClassicAssert.AreEqual(3.5, double.Parse(resp.ToString(), CultureInfo.InvariantCulture));

            // Test GT option.
            resp = db.Execute("ZADD", [key, "GT", .. testArgs]);
            ClassicAssert.AreEqual(8, double.Parse(resp.ToString(), CultureInfo.InvariantCulture));

            // Test negative values.
            testArgs = ["INCR", "-4", "a"];
            resp = db.Execute("ZADD", [key, "GT", .. testArgs]);
            ClassicAssert.IsTrue(resp.IsNull);

            resp = db.Execute("ZADD", [key, "LT", .. testArgs]);
            ClassicAssert.AreEqual(4, double.Parse(resp.ToString(), CultureInfo.InvariantCulture));
        }

        [Test]
        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void AddInfinites(RedisProtocol protocol)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
            var db = redis.GetDatabase(0);

            var key = "SortedSet_Add";
            var key2 = "SortedSet_Inter";

            // Test accepting variations of inf, -inf, +inf.
            var response = db.Execute("ZADD", key, "inf", "a", "-inf", "b", "+inf", "c", "iNF", "d", "+Inf", "e");
            ClassicAssert.AreEqual(5, int.Parse(response.ToString()));

            //Test addition
            response = db.Execute("ZADD", key, "INCR", "inf", "a");
            ClassicAssert.AreEqual("inf", response.ToString());

            ClassicAssert.AreEqual(double.PositiveInfinity, db.SortedSetIncrement(key, "a", double.PositiveInfinity));
            ClassicAssert.AreEqual(double.NegativeInfinity, db.SortedSetIncrement(key, "b", double.NegativeInfinity));

            response = db.Execute("ZINCRBY", key, "+1", "b");
            ClassicAssert.AreEqual("-inf", response.ToString());

            response = db.Execute("ZADD", key, "INCR", "-1", "c");
            ClassicAssert.AreEqual("inf", response.ToString());

            ClassicAssert.AreEqual(3, db.SortedSetAdd(key2,
            [
                new SortedSetEntry("a", double.PositiveInfinity),
                new SortedSetEntry("b", 0),
                new SortedSetEntry("c", double.NegativeInfinity)
            ]));

            var responses = db.SortedSetCombineWithScores(SetOperation.Intersect, [key, key2]);
            ClassicAssert.AreEqual(3, responses.Length);
            ClassicAssert.AreEqual("b", responses[0].Element.ToString());
            ClassicAssert.AreEqual(double.NegativeInfinity, responses[0].Score);
            ClassicAssert.AreEqual("c", responses[1].Element.ToString());
            ClassicAssert.AreEqual(0, responses[1].Score);
            ClassicAssert.AreEqual("a", responses[2].Element.ToString());
            ClassicAssert.AreEqual(double.PositiveInfinity, responses[2].Score);

            //These should blow up
            var expectedErrorMessage = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_SCORE_NAN);
            try
            {
                db.Execute("ZADD", key, "INCR", "-inf", "a");
            }
            catch (RedisServerException e)
            {
                ClassicAssert.AreEqual(expectedErrorMessage, e.Message);
            }

            try
            {
                db.Execute("ZINCRBY", key, "+inf", "b");
            }
            catch (RedisServerException e)
            {
                ClassicAssert.AreEqual(expectedErrorMessage, e.Message);
            }
        }

        [Test]
        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void AddWithOptionsErrorConditions(RedisProtocol protocol)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
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
                args = [.. argCombination.Union(sampleEntries)];
                ex = Assert.Throws<RedisServerException>(() => db.Execute("ZADD", args));
                ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GT_LT_NX_NOT_COMPATIBLE), ex.Message);
            }

            // INCR option supports only one score-element pair
            args = [.. new[] { key, "INCR" }.Union(sampleEntries)];
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
            var expectedResponse = 1832;
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
        public void CanGetScoresZCountWithMinHigherThanMaxScore()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "LeaderBoard";

            var added = db.SortedSetAdd(key, "a", 1);

            var card = db.SortedSetLength(new RedisKey(key), min: 2, max: 3);
            ClassicAssert.IsTrue(0 == card);
        }

        [Test]
        public async Task ZCountAndZCardWithExpiredAndExpiringItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for the minimum, maximum and middle items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "3", "a", "e", "c");

            await Task.Delay(300);

            // ZCARD
            var count = db.SortedSetLength("key1");
            ClassicAssert.AreEqual(2, count); // Only "b" and "d" should remain

            // Check the count of items within a score range
            // ZCOUNT
            var rangeCount = db.SortedSetLength("key1", 3, 5);
            ClassicAssert.AreEqual(1, rangeCount); // Only "d" should remain within the range
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
            var expectedResponse = 1848;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // remove all entries
            var removed = db.SortedSetRemove(key, [.. entries.Select(e => e.Element)]);
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
            expectedResponse = 408;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // remove the single entry
            removed = db.SortedSetRemove(key, [.. entries.Take(1).Select(e => e.Element)]);
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
            expectedResponse = 1848;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // 1 entry removed
            bool isRemoved = db.SortedSetRemove(key, entries[0].Element);
            ClassicAssert.IsTrue(isRemoved);

            // length should be 1 less
            card = db.SortedSetLength(key);
            ClassicAssert.AreEqual(entries.Length - 1, card);

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 1688;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // remaining entries removed
            removed = db.SortedSetRemove(key, [.. entries.Select(e => e.Element)]);
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
            var expectedResponse = 1840;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var last = db.SortedSetPop(key, Order.Descending);
            ClassicAssert.True(last.HasValue);
            ClassicAssert.AreEqual(entries[9], last.Value);
            ClassicAssert.AreEqual(9, db.SortedSetLength(key));

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 1680;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var last2 = db.SortedSetPop(key, 2, Order.Descending);
            ClassicAssert.AreEqual(2, last2.Length);
            ClassicAssert.AreEqual(entries[8], last2[0]);
            ClassicAssert.AreEqual(entries[7], last2[1]);
            ClassicAssert.AreEqual(7, db.SortedSetLength(key));

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 1360;
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
            var expectedResponse = 1848;
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
            var memExpectedResponse = 1864;
            ClassicAssert.AreEqual(memExpectedResponse, memActualValue);
        }

        [Test]
        public void CanDoZMScoreLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            lightClientRequest.SendCommands("ZADD zmscore 0 a 1 b", "PING");

            var response = lightClientRequest.SendCommands("ZMSCORE zmscore", "PING");
            var expectedResponse = $"{FormatWrongNumOfArgsError("ZMSCORE")}+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommands("ZMSCORE nokey a b c", "PING", 4, 1);
            expectedResponse = "*3\r\n$-1\r\n$-1\r\n$-1\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommands("ZMSCORE zmscore a z b", "PING", 4, 1);
            expectedResponse = "*3\r\n$1\r\n0\r\n$-1\r\n$1\r\n1\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            var expectedResponse = 1832;
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
            expectedResponse = 376;
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanUseZScanNoParameters()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // ZSCAN without key
            var e = Assert.Throws<RedisServerException>(() => db.Execute("ZSCAN"));
            var expectedErrorMessage = string.Format(CmdStrings.GenericErrWrongNumArgs, nameof(SortedSetOperation.ZSCAN));
            ClassicAssert.AreEqual(expectedErrorMessage, e.Message);

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
            var nxKey = new RedisKey("nx");
            var key1Values = new[] { new SortedSetEntry("A", 2), new SortedSetEntry("B", 3), new SortedSetEntry("C", 3), new SortedSetEntry("D", 5), new SortedSetEntry("!", 8) };
            var key2Values = new[] { new SortedSetEntry("B", 5), new SortedSetEntry("D", 1), new SortedSetEntry("M", 7) };
            var expectedValue = new[] { new SortedSetEntry("A", 2), new SortedSetEntry("C", 3), new SortedSetEntry("!", 8) };

            db.SortedSetAdd(key1, key1Values);
            db.SortedSetAdd(key2, key2Values);

            var diff = db.SortedSetCombine(SetOperation.Difference, [key1, key2]);
            ClassicAssert.AreEqual(3, diff.Length);
            ClassicAssert.AreEqual(expectedValue[0].Element.ToString(), diff[0].ToString());
            ClassicAssert.AreEqual(expectedValue[1].Element.ToString(), diff[1].ToString());
            ClassicAssert.AreEqual(expectedValue[2].Element.ToString(), diff[2].ToString());

            var diffWithScore = db.SortedSetCombineWithScores(SetOperation.Difference, [key1, key2]);
            ClassicAssert.AreEqual(3, diffWithScore.Length);
            ClassicAssert.AreEqual(expectedValue[0].Element.ToString(), diff[0].ToString());
            ClassicAssert.AreEqual(expectedValue[1].Element.ToString(), diff[1].ToString());
            ClassicAssert.AreEqual(expectedValue[2].Element.ToString(), diff[2].ToString());
            ClassicAssert.AreEqual(expectedValue[0].Score, diffWithScore[0].Score);
            ClassicAssert.AreEqual(expectedValue[1].Score, diffWithScore[1].Score);
            ClassicAssert.AreEqual(expectedValue[2].Score, diffWithScore[2].Score);

            // With only one key, it should return the same elements
            diff = db.SortedSetCombine(SetOperation.Difference, [key1]);
            ClassicAssert.AreEqual(5, diff.Length);
            ClassicAssert.AreEqual(key1Values[0].Element.ToString(), diff[0].ToString());
            ClassicAssert.AreEqual(key1Values[1].Element.ToString(), diff[1].ToString());
            ClassicAssert.AreEqual(key1Values[2].Element.ToString(), diff[2].ToString());
            ClassicAssert.AreEqual(key1Values[3].Element.ToString(), diff[3].ToString());
            ClassicAssert.AreEqual(key1Values[4].Element.ToString(), diff[4].ToString());

            diffWithScore = db.SortedSetCombineWithScores(SetOperation.Difference, [key1]);
            ClassicAssert.AreEqual(5, diffWithScore.Length);
            ClassicAssert.AreEqual(key1Values[0].Element.ToString(), diffWithScore[0].Element.ToString());
            ClassicAssert.AreEqual(key1Values[0].Score, diffWithScore[0].Score);
            ClassicAssert.AreEqual(key1Values[1].Element.ToString(), diffWithScore[1].Element.ToString());
            ClassicAssert.AreEqual(key1Values[1].Score, diffWithScore[1].Score);
            ClassicAssert.AreEqual(key1Values[2].Element.ToString(), diffWithScore[2].Element.ToString());
            ClassicAssert.AreEqual(key1Values[2].Score, diffWithScore[2].Score);
            ClassicAssert.AreEqual(key1Values[3].Element.ToString(), diffWithScore[3].Element.ToString());
            ClassicAssert.AreEqual(key1Values[3].Score, diffWithScore[3].Score);
            ClassicAssert.AreEqual(key1Values[4].Element.ToString(), diffWithScore[4].Element.ToString());
            ClassicAssert.AreEqual(key1Values[4].Score, diffWithScore[4].Score);

            // With one key and nonexisting key, it should return the same elements
            diffWithScore = db.SortedSetCombineWithScores(SetOperation.Difference, [key1, nxKey]);
            ClassicAssert.AreEqual(5, diffWithScore.Length);
            ClassicAssert.AreEqual(key1Values[0].Element.ToString(), diffWithScore[0].Element.ToString());
            ClassicAssert.AreEqual(key1Values[0].Score, diffWithScore[0].Score);
            ClassicAssert.AreEqual(key1Values[1].Element.ToString(), diffWithScore[1].Element.ToString());
            ClassicAssert.AreEqual(key1Values[1].Score, diffWithScore[1].Score);
            ClassicAssert.AreEqual(key1Values[2].Element.ToString(), diffWithScore[2].Element.ToString());
            ClassicAssert.AreEqual(key1Values[2].Score, diffWithScore[2].Score);
            ClassicAssert.AreEqual(key1Values[3].Element.ToString(), diffWithScore[3].Element.ToString());
            ClassicAssert.AreEqual(key1Values[3].Score, diffWithScore[3].Score);
            ClassicAssert.AreEqual(key1Values[4].Element.ToString(), diffWithScore[4].Element.ToString());
            ClassicAssert.AreEqual(key1Values[4].Score, diffWithScore[4].Score);

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
        [TestCase("(a", "(a", new string[] { })]
        public void CanDoZRevRangeByLex(string max, string min, string[] expected, int offset = 0, int count = -1)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myzset";
            db.SortedSetAdd(key, "a", 0);
            db.SortedSetAdd(key, "b", 0);
            db.SortedSetAdd(key, "c", 0);
            db.SortedSetAdd(key, "d", 0);
            db.SortedSetAdd(key, "e", 0);
            db.SortedSetAdd(key, "f", 0);
            db.SortedSetAdd(key, "g", 0);

            var result = (string[])db.Execute("ZREVRANGEBYLEX", key, max, min, "LIMIT", offset, count);
            CollectionAssert.AreEqual(expected, result);
        }

        [Test]
        [TestCase("(a", "(a", new string[] { })]
        public void CanDoZRevRangeByLexWithoutLimit(string min, string max, string[] expected)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myzset";
            db.SortedSetAdd(key, "a", 0);
            db.SortedSetAdd(key, "b", 0);
            db.SortedSetAdd(key, "c", 0);
            db.SortedSetAdd(key, "d", 0);
            db.SortedSetAdd(key, "e", 0);
            db.SortedSetAdd(key, "f", 0);
            db.SortedSetAdd(key, "g", 0);

            var result = (string[])db.Execute("ZREVRANGEBYLEX", key, max, min);
            ClassicAssert.AreEqual(expected, result);
        }

        [Test]
        [TestCase("user1:obj1", "user1:objA", new[] { "Hello", "World" }, new[] { 1.0, 2.0 }, new[] { "Hello", "World" }, new[] { 1.0, 2.0 })] // Normal case
        [TestCase("user1:emptySet", "user1:objB", new string[] { }, new double[] { }, new string[] { }, new double[] { })] // Empty set
        [TestCase("user1:nonExistingKey", "user1:objC", new string[] { }, new double[] { }, new string[] { }, new double[] { })] // Non-existing key
        [TestCase("user1:obj2", "user1:objD", new[] { "Alpha", "Beta", "Gamma" }, new[] { 1.0, 2.0, 3.0 }, new[] { "Beta", "Gamma" }, new[] { 2.0, 3.0 }, -2, -1)] // Negative range
        public void CheckSortedSetRangeStoreSE(string key, string destinationKey, string[] elements, double[] scores, string[] expectedElements, double[] expectedScores, int start = 0, int stop = -1)
        {
            int expectedCount = expectedElements.Length;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var keyValues = elements.Zip(scores, (e, s) => new SortedSetEntry(e, s)).ToArray();

            // Set up sorted set if elements are provided
            if (keyValues.Length > 0)
            {
                db.SortedSetAdd(key, keyValues);
            }

            var actualCount = db.SortedSetRangeAndStore(key, destinationKey, start, stop);
            ClassicAssert.AreEqual(expectedCount, actualCount);

            var actualMembers = db.SortedSetRangeByScoreWithScores(destinationKey);
            ClassicAssert.AreEqual(expectedCount, actualMembers.Length);

            for (int i = 0; i < expectedCount; i++)
            {
                ClassicAssert.AreEqual(expectedElements[i], actualMembers[i].Element.ToString());
                ClassicAssert.AreEqual(expectedScores[i], actualMembers[i].Score);
            }
        }

        [Test]
        [TestCase("set1", "dest1", new[] { "a", "b", "c", "d" }, new[] { 1.0, 2.0, 3.0, 4.0 }, "BYSCORE", "(2", "4", "", 2, new[] { "c", "d" }, new[] { 3.0, 4.0 }, Description = "ZRANGESTORE BYSCORE with exclusive min")]
        [TestCase("set1", "dest1", new[] { "a", "b", "c", "d" }, new[] { 1.0, 2.0, 3.0, 4.0 }, "BYSCORE", "2", "(4", "", 2, new[] { "b", "c" }, new[] { 2.0, 3.0 }, Description = "ZRANGESTORE BYSCORE with exclusive max")]
        [TestCase("set1", "dest1", new[] { "a", "b", "c", "d" }, new[] { 1.0, 2.0, 3.0, 4.0 }, "BYSCORE REV", "4", "1", "", 4, new[] { "a", "b", "c", "d" }, new[] { 1.0, 2.0, 3.0, 4.0 }, Description = "ZRANGESTORE BYSCORE with REV")]
        [TestCase("set1", "dest1", new[] { "a", "b", "c", "d" }, new[] { 1.0, 2.0, 3.0, 4.0 }, "BYSCORE", "2", "4", "LIMIT 1 1", 1, new[] { "c" }, new[] { 3.0 }, Description = "ZRANGESTORE BYSCORE with LIMIT")]
        public void CheckSortedSetRangeStoreByScoreSE(string sourceKey, string destKey, string[] sourceElements, double[] sourceScores, string options, string min, string max, string limit,
            int expectedCount, string[] expectedElements, double[] expectedScores)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var entries = sourceElements.Zip(sourceScores, (e, s) => new SortedSetEntry(e, s)).ToArray();
            db.SortedSetAdd(sourceKey, entries);

            var command = $"{destKey} {sourceKey} {min} {max} {options} {limit}".Trim().Split(" ");
            var result = db.Execute("ZRANGESTORE", command);
            ClassicAssert.AreEqual(expectedCount, int.Parse(result.ToString()));

            var actualMembers = db.SortedSetRangeByScoreWithScores(destKey);
            ClassicAssert.AreEqual(expectedElements.Length, actualMembers.Length);

            for (int i = 0; i < expectedElements.Length; i++)
            {
                ClassicAssert.AreEqual(expectedElements[i], actualMembers[i].Element.ToString());
                ClassicAssert.AreEqual(expectedScores[i], actualMembers[i].Score);
            }
        }

        [Test]
        [TestCase("set1", "dest1", new[] { "a", "b", "c", "d" }, new[] { 1.0, 1.0, 1.0, 1.0 }, "BYLEX", "[b", "[d", "", 3, new[] { "b", "c", "d" }, Description = "ZRANGESTORE BYLEX with inclusive range")]
        [TestCase("set1", "dest1", new[] { "a", "b", "c", "d" }, new[] { 1.0, 1.0, 1.0, 1.0 }, "BYLEX", "(b", "(d", "", 1, new[] { "c" }, Description = "ZRANGESTORE BYLEX with exclusive range")]
        [TestCase("set1", "dest1", new[] { "a", "b", "c", "d" }, new[] { 1.0, 1.0, 1.0, 1.0 }, "BYLEX REV", "[d", "[b", "", 3, new[] { "b", "c", "d" }, Description = "ZRANGESTORE BYLEX with REV")]
        [TestCase("set1", "dest1", new[] { "a", "b", "c", "d" }, new[] { 1.0, 1.0, 1.0, 1.0 }, "BYLEX", "[b", "[d", "LIMIT 1 1", 1, new[] { "c" }, Description = "ZRANGESTORE BYLEX with LIMIT")]
        public void CheckSortedSetRangeStoreByLexSE(string sourceKey, string destKey, string[] sourceElements, double[] sourceScores, string options, string min, string max, string limit,
            int expectedCount, string[] expectedElements)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var entries = sourceElements.Zip(sourceScores, (e, s) => new SortedSetEntry(e, s)).ToArray();
            db.SortedSetAdd(sourceKey, entries);

            var command = $"{destKey} {sourceKey} {min} {max} {options} {limit}".Trim().Split();
            var result = db.Execute("ZRANGESTORE", command);
            ClassicAssert.AreEqual(expectedCount, int.Parse(result.ToString()));

            var actualMembers = db.SortedSetRangeByScore(destKey);
            ClassicAssert.AreEqual(expectedElements.Length, actualMembers.Length);

            for (int i = 0; i < expectedElements.Length; i++)
            {
                ClassicAssert.AreEqual(expectedElements[i], actualMembers[i].ToString());
            }
        }

        [Test]
        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void TestCheckSortedSetRangeStoreWithExistingDestinationKeySE(RedisProtocol protocol)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
            var db = redis.GetDatabase(0);

            var sourceKey = "sourceKey";
            var destinationKey = "destKey";

            // Set up source sorted set
            var sourceElements = new[] { "a", "b", "c", "d" };
            var sourceScores = new[] { 1.0, 2.0, 3.0, 4.0 };
            var sourceEntries = sourceElements.Zip(sourceScores, (e, s) => new SortedSetEntry(e, s)).ToArray();
            db.SortedSetAdd(sourceKey, sourceEntries);

            // Set up existing destination sorted set
            db.StringSet(destinationKey, "dummy");

            // Expected elements after range store
            var expectedElements = new[] { "b", "c" };
            var expectedScores = new[] { 2.0, 3.0 };

            var actualCount = db.SortedSetRangeAndStore(sourceKey, destinationKey, 1, 2);

            Assert.That(actualCount, Is.EqualTo(expectedElements.Length));

            var actualMembers = db.SortedSetRangeByScoreWithScores(destinationKey);
            Assert.That(actualMembers.Length, Is.EqualTo(expectedElements.Length));

            for (int i = 0; i < expectedElements.Length; i++)
            {
                Assert.That(actualMembers[i].Element.ToString(), Is.EqualTo(expectedElements[i]));
                Assert.That(actualMembers[i].Score, Is.EqualTo(expectedScores[i]));
            }
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
            ClassicAssert.AreEqual(Math.Min(count, entries.Length), poppedItems.Length);

            if (count == 1)
            {
                var element = poppedItems[0];
                ClassicAssert.AreEqual("a", (string)element[0]);
                ClassicAssert.AreEqual("1", (string)element[1]);
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
                    var element = valuesAndScores[i];
                    ClassicAssert.AreEqual(expectedValues[i], (string)element[0]);
                    ClassicAssert.AreEqual(expectedScores[i], (double)element[1]);
                }
            }
            else
            {
                ClassicAssert.IsTrue(result.IsNull);
            }
        }

        [Test]
        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void SortedSetMultiPopWithFirstKeyEmptyOnSecondPopTest(RedisProtocol protocol)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
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

        [Test]
        public void CanDoZInterWithSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Setup test data
            db.SortedSetAdd("zset1",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("three", 3)
            ]);

            db.SortedSetAdd("zset2",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("four", 4)
            ]);

            db.SortedSetAdd("zset3",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("three", 3),
                new SortedSetEntry("five", 5)
            ]);

            // Test basic intersection
            var result = db.SortedSetCombine(SetOperation.Intersect, [new RedisKey("zset1"), new RedisKey("zset2")]);
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.AreEqual("one", result[0].ToString());
            ClassicAssert.AreEqual("two", result[1].ToString());

            // Test three-way intersection
            result = db.SortedSetCombine(SetOperation.Intersect, [new RedisKey("zset1"), new RedisKey("zset2"), new RedisKey("zset3")]);
            ClassicAssert.AreEqual(1, result.Length);
            ClassicAssert.AreEqual("one", result[0].ToString());

            // Test with scores
            var resultWithScores = db.SortedSetCombineWithScores(SetOperation.Intersect, [new RedisKey("zset1"), new RedisKey("zset2")]);
            ClassicAssert.AreEqual(2, resultWithScores.Length);
            ClassicAssert.AreEqual("one", resultWithScores[0].Element.ToString());
            ClassicAssert.AreEqual(2, resultWithScores[0].Score);
            ClassicAssert.AreEqual("two", resultWithScores[1].Element.ToString());
            ClassicAssert.AreEqual(4, resultWithScores[1].Score);
        }

        [Test]
        [TestCase(2, "ZINTER 2 zset1 zset2", new[] { "one", "two" }, new[] { 2.0, 4.0 }, Description = "Basic intersection")]
        [TestCase(3, "ZINTER 3 zset1 zset2 zset3", new[] { "one" }, new[] { 3.0 }, Description = "Three-way intersection")]
        [TestCase(2, "ZINTER 2 zset1 zset2 WITHSCORES", new[] { "one", "two" }, new[] { 2.0, 4.0 }, Description = "With scores")]
        [TestCase(2, "ZINTER 2 zset1 zset2 WEIGHTS 2 3 WITHSCORES", new[] { "one", "two" }, new[] { 5.0, 10.0 }, Description = "With weights 2,3 multiplied by scores")]
        [TestCase(2, "ZINTER 2 zset1 zset2 AGGREGATE MAX WITHSCORES", new[] { "one", "two" }, new[] { 1.0, 2.0 }, Description = "Using maximum of scores")]
        [TestCase(2, "ZINTER 2 zset1 zset2 AGGREGATE MIN WITHSCORES", new[] { "one", "two" }, new[] { 1.0, 2.0 }, Description = "Using minimum of scores")]
        [TestCase(2, "ZINTER 2 zset1 zset2 WEIGHTS 2 3 AGGREGATE SUM WITHSCORES", new[] { "one", "two" }, new[] { 5.0, 10.0 }, Description = "Weights with sum aggregation")]
        public void CanDoZInterWithSE(int numKeys, string command, string[] expectedValues, double[] expectedScores)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Setup test data
            db.SortedSetAdd("zset1",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("three", 3)
            ]);

            db.SortedSetAdd("zset2",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("four", 4)
            ]);

            db.SortedSetAdd("zset3",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("three", 3),
                new SortedSetEntry("five", 5)
            ]);

            // Test intersection operation
            if (command.Contains("WITHSCORES"))
            {
                var resultWithScores = db.SortedSetCombineWithScores(SetOperation.Intersect,
                    command.Contains("WEIGHTS") ? [new RedisKey("zset1"), new RedisKey("zset2")] :
                        [.. Enumerable.Range(1, numKeys).Select(i => new RedisKey($"zset{i}"))],
                    command.Contains("WEIGHTS") ? [2.0, 3.0] : null,
                    command.Contains("MAX") ? Aggregate.Max :
                    command.Contains("MIN") ? Aggregate.Min : Aggregate.Sum);

                ClassicAssert.AreEqual(expectedValues.Length, resultWithScores.Length);
                for (int i = 0; i < expectedValues.Length; i++)
                {
                    ClassicAssert.AreEqual(expectedValues[i], resultWithScores[i].Element.ToString());
                    ClassicAssert.AreEqual(expectedScores[i], resultWithScores[i].Score);
                }
            }
            else
            {
                var result = db.SortedSetCombine(SetOperation.Intersect,
                    [.. Enumerable.Range(1, numKeys).Select(i => new RedisKey($"zset{i}"))]);

                ClassicAssert.AreEqual(expectedValues.Length, result.Length);
                for (int i = 0; i < expectedValues.Length; i++)
                {
                    ClassicAssert.AreEqual(expectedValues[i], result[i].ToString());
                }
            }
        }

        [Test]
        public void CanDoZInterCardWithSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Setup test data
            db.SortedSetAdd("zset1",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("three", 3)
            ]);

            db.SortedSetAdd("zset2",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("four", 4)
            ]);

            db.SortedSetAdd("zset3",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("three", 3),
                new SortedSetEntry("five", 5)
            ]);

            // Test basic intersection cardinality
            var result = (long)db.Execute("ZINTERCARD", "2", "zset1", "zset2");
            ClassicAssert.AreEqual(2, result);

            // Test three-way intersection cardinality
            result = (long)db.Execute("ZINTERCARD", "3", "zset1", "zset2", "zset3");
            ClassicAssert.AreEqual(1, result);

            // Test with limit
            result = (long)db.Execute("ZINTERCARD", "2", "zset1", "zset2", "LIMIT", "1");
            ClassicAssert.AreEqual(1, result);
        }

        [Test]
        public void CanDoZInterStoreWithSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Setup test data
            db.SortedSetAdd("zset1",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("three", 3)
            ]);

            db.SortedSetAdd("zset2",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("four", 4)
            ]);

            // Test basic intersection store
            var result = db.SortedSetCombineAndStore(SetOperation.Intersect, "dest", [new RedisKey("zset1"), new RedisKey("zset2")]);
            ClassicAssert.AreEqual(2, result);

            var storedValues = db.SortedSetRangeByScoreWithScores("dest");
            ClassicAssert.AreEqual(2, storedValues.Length);
            ClassicAssert.AreEqual("one", storedValues[0].Element.ToString());
            ClassicAssert.AreEqual(2, storedValues[0].Score); // Sum of scores
            ClassicAssert.AreEqual("two", storedValues[1].Element.ToString());
            ClassicAssert.AreEqual(4, storedValues[1].Score); // Sum of scores

            // Test with weights
            var weights = new double[] { 2, 3 };
            result = db.SortedSetCombineAndStore(SetOperation.Intersect, "dest", [new RedisKey("zset1"), new RedisKey("zset2")], weights);
            ClassicAssert.AreEqual(2, result);

            storedValues = db.SortedSetRangeByScoreWithScores("dest");
            ClassicAssert.AreEqual(2, storedValues.Length);
            ClassicAssert.AreEqual("one", storedValues[0].Element.ToString());
            ClassicAssert.AreEqual(5, storedValues[0].Score); // Weighted sum
            ClassicAssert.AreEqual("two", storedValues[1].Element.ToString());
            ClassicAssert.AreEqual(10, storedValues[1].Score); // Weighted sum

            // Test with MAX aggregate
            var result2 = (long)db.Execute("ZINTERSTORE", "dest", "2", "zset1", "zset2", "AGGREGATE", "MAX");
            ClassicAssert.AreEqual(2, result2);

            storedValues = db.SortedSetRangeByScoreWithScores("dest");
            ClassicAssert.AreEqual(2, storedValues.Length);
            ClassicAssert.AreEqual(1, storedValues[0].Score); // MAX of scores
            ClassicAssert.AreEqual(2, storedValues[1].Score); // MAX of scores

            // Test error cases
            var ex = Assert.Throws<RedisServerException>(() => db.Execute("ZINTERSTORE", "dest"));
            ClassicAssert.AreEqual(string.Format(CmdStrings.GenericErrWrongNumArgs, "ZINTERSTORE"), ex.Message);
        }

        [Test]
        [TestCase("SUM", new double[] { 3, 5, 6, 7 }, new string[] { "c", "a", "d", "b" }, Description = "Tests ZUNION with SUM aggregate")]
        [TestCase("MIN", new double[] { 1, 2, 3, 6 }, new string[] { "a", "b", "c", "d" }, Description = "Tests ZUNION with MIN aggregate")]
        [TestCase("MAX", new double[] { 3, 4, 5, 6 }, new string[] { "c", "a", "b", "d" }, Description = "Tests ZUNION with MAX aggregate")]
        public void CanUseZUnionWithAggregateOption(string aggregateType, double[] expectedScores, string[] expectedElements)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Setup test data
            db.SortedSetAdd("zset1",
            [
                new SortedSetEntry("a", 1),
                new SortedSetEntry("b", 2),
                new SortedSetEntry("c", 3)
            ]);
            db.SortedSetAdd("zset2",
            [
                new SortedSetEntry("a", 4),
                new SortedSetEntry("b", 5),
                new SortedSetEntry("d", 6)
            ]);

            var result = db.SortedSetCombineWithScores(SetOperation.Union, ["zset1", "zset2"],
                weights: null, aggregate: aggregateType switch
                {
                    "SUM" => Aggregate.Sum,
                    "MIN" => Aggregate.Min,
                    "MAX" => Aggregate.Max,
                    _ => throw new ArgumentException("Invalid aggregate type")
                });

            ClassicAssert.AreEqual(expectedScores.Length, result.Length);
            for (int i = 0; i < result.Length; i++)
            {
                ClassicAssert.AreEqual(expectedScores[i], result[i].Score);
                ClassicAssert.AreEqual(expectedElements[i], result[i].Element.ToString());
            }
        }

        [Test]
        [TestCase(new double[] { 2, 3 }, new double[] { 6, 14, 18, 19 }, new string[] { "c", "a", "d", "b" }, Description = "Tests ZUNION with multiple weights")]
        public void CanUseZUnionWithWeights(double[] weights, double[] expectedScores, string[] expectedElements)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Setup test data
            db.SortedSetAdd("zset1", [
                new("a", 1),
                new("b", 2),
                new("c", 3)
            ]);
            db.SortedSetAdd("zset2", [
                new("a", 4),
                new("b", 5),
                new("d", 6)
            ]);

            var result = db.SortedSetCombineWithScores(SetOperation.Union,
                ["zset1", "zset2"],
                weights: weights);

            ClassicAssert.AreEqual(expectedScores.Length, result.Length);
            for (int i = 0; i < result.Length; i++)
            {
                ClassicAssert.AreEqual(expectedScores[i], result[i].Score);
                ClassicAssert.AreEqual(expectedElements[i], result[i].Element.ToString());
            }
        }

        [Test]
        [TestCase("SUM", new double[] { 3, 5, 6, 7 }, new string[] { "c", "a", "d", "b" }, Description = "Tests ZUNIONSTORE with SUM aggregate")]
        [TestCase("MIN", new double[] { 1, 2, 3, 6 }, new string[] { "a", "b", "c", "d" }, Description = "Tests ZUNIONSTORE with MIN aggregate")]
        [TestCase("MAX", new double[] { 3, 4, 5, 6 }, new string[] { "c", "a", "b", "d" }, Description = "Tests ZUNIONSTORE with MAX aggregate")]
        public void CanUseZUnionStoreWithAggregateOption(string aggregateType, double[] expectedScores, string[] expectedElements)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Setup test data
            db.SortedSetAdd("zset1",
            [
                new SortedSetEntry("a", 1),
                new SortedSetEntry("b", 2),
                new SortedSetEntry("c", 3)
            ]);
            db.SortedSetAdd("zset2",
            [
                new SortedSetEntry("a", 4),
                new SortedSetEntry("b", 5),
                new SortedSetEntry("d", 6)
            ]);

            db.SortedSetCombineAndStore(SetOperation.Union, "zset3", ["zset1", "zset2"],
                weights: null, aggregate: aggregateType switch
                {
                    "SUM" => Aggregate.Sum,
                    "MIN" => Aggregate.Min,
                    "MAX" => Aggregate.Max,
                    _ => throw new ArgumentException("Invalid aggregate type")
                });

            var result = db.SortedSetRangeByRankWithScores("zset3");

            ClassicAssert.AreEqual(expectedScores.Length, result.Length);
            for (int i = 0; i < result.Length; i++)
            {
                ClassicAssert.AreEqual(expectedScores[i], result[i].Score);
                ClassicAssert.AreEqual(expectedElements[i], result[i].Element.ToString());
            }
        }

        [Test]
        public void CanUseZUnionStoreWithNonEmptyDestinationKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Setup test data
            db.SortedSetAdd("zset1",
            [
                new SortedSetEntry("a", 1),
                new SortedSetEntry("b", 2),
                new SortedSetEntry("c", 3)
            ]);
            db.SortedSetAdd("zset2",
            [
                new SortedSetEntry("a", 4),
                new SortedSetEntry("b", 5),
                new SortedSetEntry("d", 6)
            ]);

            // Add some data to the destination key
            db.SortedSetAdd("zset3",
            [
                new SortedSetEntry("x", 10),
                new SortedSetEntry("y", 20)
            ]);

            db.SortedSetCombineAndStore(SetOperation.Union, "zset3", ["zset1", "zset2"], weights: null, aggregate: Aggregate.Sum);

            var result = db.SortedSetRangeByRankWithScores("zset3");

            var expectedScores = new double[] { 3, 5, 6, 7 };
            var expectedElements = new string[] { "c", "a", "d", "b" };

            ClassicAssert.AreEqual(expectedScores.Length, result.Length);
            for (int i = 0; i < result.Length; i++)
            {
                ClassicAssert.AreEqual(expectedScores[i], result[i].Score);
                ClassicAssert.AreEqual(expectedElements[i], result[i].Element.ToString());
            }
        }

        [Test]
        [TestCase(new double[] { 2, 3 }, new double[] { 6, 14, 18, 19 }, new string[] { "c", "a", "d", "b" }, Description = "Tests ZUNIONSTORE with multiple weights")]
        public void CanUseZUnionStoreWithWeights(double[] weights, double[] expectedScores, string[] expectedElements)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Setup test data
            db.SortedSetAdd("zset1", [
                new("a", 1),
                new("b", 2),
                new("c", 3)
            ]);
            db.SortedSetAdd("zset2", [
                new("a", 4),
                new("b", 5),
                new("d", 6)
            ]);

            db.SortedSetCombineAndStore(SetOperation.Union, "zset3", ["zset1", "zset2"], weights: weights);

            var result = db.SortedSetRangeByRankWithScores("zset3");

            ClassicAssert.AreEqual(expectedScores.Length, result.Length);
            for (int i = 0; i < result.Length; i++)
            {
                ClassicAssert.AreEqual(expectedScores[i], result[i].Score);
                ClassicAssert.AreEqual(expectedElements[i], result[i].Element.ToString());
            }
        }

        [Test]
        public async Task CanDoSortedSetCollect()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServers().First();
            db.SortedSetAdd("mysortedset",
            [
                new SortedSetEntry("member1", 1),
                new SortedSetEntry("member2", 2),
                new SortedSetEntry("member3", 3),
                new SortedSetEntry("member4", 4),
                new SortedSetEntry("member5", 5),
                new SortedSetEntry("member6", 6)
            ]);

            var result = db.Execute("ZPEXPIRE", "mysortedset", "500", "MEMBERS", "2", "member1", "member2");
            var results = (RedisResult[])result;
            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);
            ClassicAssert.AreEqual(1, (long)results[1]);

            result = db.Execute("ZPEXPIRE", "mysortedset", "1500", "MEMBERS", "2", "member3", "member4");
            results = (RedisResult[])result;
            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);
            ClassicAssert.AreEqual(1, (long)results[1]);

            var orginalMemory = (long)db.Execute("MEMORY", "USAGE", "mysortedset");

            await Task.Delay(600);

            var newMemory = (long)db.Execute("MEMORY", "USAGE", "mysortedset");
            ClassicAssert.AreEqual(newMemory, orginalMemory);

            var collectResult = (string)db.Execute("ZCOLLECT", "mysortedset");
            ClassicAssert.AreEqual("OK", collectResult);

            newMemory = (long)db.Execute("MEMORY", "USAGE", "mysortedset");
            ClassicAssert.Less(newMemory, orginalMemory);
            orginalMemory = newMemory;

            await Task.Delay(1100);

            newMemory = (long)db.Execute("MEMORY", "USAGE", "mysortedset");
            ClassicAssert.AreEqual(newMemory, orginalMemory);

            collectResult = (string)db.Execute("ZCOLLECT", "*");
            ClassicAssert.AreEqual("OK", collectResult);

            newMemory = (long)db.Execute("MEMORY", "USAGE", "mysortedset");
            ClassicAssert.Less(newMemory, orginalMemory);
        }

        [Test]
        public async Task CanDoSortedSetExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.SortedSetAdd("mysortedset", [new SortedSetEntry("member1", 1), new SortedSetEntry("member2", 2), new SortedSetEntry("member3", 3), new SortedSetEntry("member4", 4), new SortedSetEntry("member5", 5), new SortedSetEntry("member6", 6)]);

            var result = db.Execute("ZEXPIRE", "mysortedset", "3", "MEMBERS", "3", "member1", "member5", "nonexistmember");
            var results = (RedisResult[])result;
            ClassicAssert.AreEqual(3, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);
            ClassicAssert.AreEqual(1, (long)results[1]);
            ClassicAssert.AreEqual(-2, (long)results[2]);

            result = db.Execute("ZPEXPIRE", "mysortedset", "3000", "MEMBERS", "2", "member2", "nonexistmember");
            results = (RedisResult[])result;
            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);
            ClassicAssert.AreEqual(-2, (long)results[1]);

            result = db.Execute("ZEXPIREAT", "mysortedset", DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeSeconds().ToString(), "MEMBERS", "2", "member3", "nonexistmember");
            results = (RedisResult[])result;
            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);
            ClassicAssert.AreEqual(-2, (long)results[1]);

            result = db.Execute("ZPEXPIREAT", "mysortedset", DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeMilliseconds().ToString(), "MEMBERS", "2", "member4", "nonexistmember");
            results = (RedisResult[])result;
            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);
            ClassicAssert.AreEqual(-2, (long)results[1]);

            var ttl = (RedisResult[])db.Execute("ZTTL", "mysortedset", "MEMBERS", "2", "member1", "nonexistmember");
            ClassicAssert.AreEqual(2, ttl.Length);
            ClassicAssert.LessOrEqual((long)ttl[0], 3);
            ClassicAssert.Greater((long)ttl[0], 1);
            ClassicAssert.AreEqual(-2, (long)results[1]);

            ttl = (RedisResult[])db.Execute("ZPTTL", "mysortedset", "MEMBERS", "2", "member1", "nonexistmember");
            ClassicAssert.AreEqual(2, ttl.Length);
            ClassicAssert.LessOrEqual((long)ttl[0], 3000);
            ClassicAssert.Greater((long)ttl[0], 1000);
            ClassicAssert.AreEqual(-2, (long)results[1]);

            ttl = (RedisResult[])db.Execute("ZEXPIRETIME", "mysortedset", "MEMBERS", "2", "member1", "nonexistmember");
            ClassicAssert.AreEqual(2, ttl.Length);
            ClassicAssert.LessOrEqual((long)ttl[0], DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeSeconds());
            ClassicAssert.Greater((long)ttl[0], DateTimeOffset.UtcNow.AddSeconds(1).ToUnixTimeSeconds());
            ClassicAssert.AreEqual(-2, (long)results[1]);

            ttl = (RedisResult[])db.Execute("ZPEXPIRETIME", "mysortedset", "MEMBERS", "2", "member1", "nonexistmember");
            ClassicAssert.AreEqual(2, ttl.Length);
            ClassicAssert.LessOrEqual((long)ttl[0], DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeMilliseconds());
            ClassicAssert.Greater((long)ttl[0], DateTimeOffset.UtcNow.AddSeconds(1).ToUnixTimeMilliseconds());
            ClassicAssert.AreEqual(-2, (long)results[1]);

            results = (RedisResult[])db.Execute("ZPERSIST", "mysortedset", "MEMBERS", "3", "member5", "member6", "nonexistmember");
            ClassicAssert.AreEqual(3, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);  // 1 the expiration was removed.
            ClassicAssert.AreEqual(-1, (long)results[1]); // -1 if the member exists but has no associated expiration set.
            ClassicAssert.AreEqual(-2, (long)results[2]);

            await Task.Delay(3500);

            var items = db.SortedSetRangeByRankWithScores("mysortedset");
            ClassicAssert.AreEqual(2, items.Length);
            ClassicAssert.AreEqual("member5", items[0].Element.ToString());
            ClassicAssert.AreEqual(5, items[0].Score);
            ClassicAssert.AreEqual("member6", items[1].Element.ToString());
            ClassicAssert.AreEqual(6, items[1].Score);

            result = db.Execute("ZEXPIRE", "mysortedset", "0", "MEMBERS", "1", "member5");
            results = (RedisResult[])result;
            ClassicAssert.AreEqual(1, results.Length);
            ClassicAssert.AreEqual(2, (long)results[0]);

            result = db.Execute("ZEXPIREAT", "mysortedset", DateTimeOffset.UtcNow.AddSeconds(-1).ToUnixTimeSeconds().ToString(), "MEMBERS", "1", "member6");
            results = (RedisResult[])result;
            ClassicAssert.AreEqual(1, results.Length);
            ClassicAssert.AreEqual(2, (long)results[0]);

            items = db.SortedSetRangeByRankWithScores("mysortedset");
            ClassicAssert.AreEqual(0, items.Length);
        }

        [Test]
        public async Task CanDoSortedSetExpireLTM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.EndPoint);

            string[] smallExpireKeys = ["user:user0", "user:user1"];
            string[] largeExpireKeys = ["user:user2", "user:user3"];

            foreach (var key in smallExpireKeys)
            {
                db.SortedSetAdd(key,
                [
                    new SortedSetEntry("Field1", 1),
                    new SortedSetEntry("Field2", 2)
                ]);
                db.Execute("ZEXPIRE", key, "2", "MEMBERS", "1", "Field1");
            }

            foreach (var key in largeExpireKeys)
            {
                db.SortedSetAdd(key,
                [
                    new SortedSetEntry("Field1", 1),
                    new SortedSetEntry("Field2", 2)
                ]);
                db.Execute("ZEXPIRE", key, "4", "MEMBERS", "1", "Field1");
            }

            // Create LTM (larger than memory) DB by inserting 100 keys
            for (int i = 4; i < 100; i++)
            {
                var key = "user:user" + i;
                db.SortedSetAdd(key,
                [
                    new SortedSetEntry("Field1", 1),
                    new SortedSetEntry("Field2", 2)
                ]);
            }

            var info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true);
            // Ensure data has spilled to disk
            ClassicAssert.Greater(info.HeadAddress, info.BeginAddress);

            await Task.Delay(2000);

            var result = db.SortedSetScore(smallExpireKeys[0], "Field1");
            ClassicAssert.IsNull(result);
            result = db.SortedSetScore(smallExpireKeys[1], "Field1");
            ClassicAssert.IsNull(result);
            result = db.SortedSetScore(largeExpireKeys[0], "Field1");
            ClassicAssert.IsNotNull(result);
            result = db.SortedSetScore(largeExpireKeys[1], "Field1");
            ClassicAssert.IsNotNull(result);
            var ttl = db.SortedSetRangeByScoreWithScores(largeExpireKeys[0], 1, 1);
            ClassicAssert.AreEqual(ttl.Length, 1);
            ClassicAssert.Greater(ttl[0].Score, 0);
            ClassicAssert.LessOrEqual(ttl[0].Score, 2000);
            ttl = db.SortedSetRangeByScoreWithScores(largeExpireKeys[1], 1, 1);
            ClassicAssert.AreEqual(ttl.Length, 1);
            ClassicAssert.Greater(ttl[0].Score, 0);
            ClassicAssert.LessOrEqual(ttl[0].Score, 2000);

            await Task.Delay(2000);

            result = db.SortedSetScore(largeExpireKeys[0], "Field1");
            ClassicAssert.IsNull(result);
            result = db.SortedSetScore(largeExpireKeys[1], "Field1");
            ClassicAssert.IsNull(result);

            var data = db.SortedSetRangeByRankWithScores("user:user4");
            ClassicAssert.AreEqual(2, data.Length);
            ClassicAssert.AreEqual("Field1", data[0].Element.ToString());
            ClassicAssert.AreEqual(1, data[0].Score);
            ClassicAssert.AreEqual("Field2", data[1].Element.ToString());
            ClassicAssert.AreEqual(2, data[1].Score);
        }

        [Test]
        public void CanDoSortedSetExpireWithNonExistKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var result = db.Execute("ZEXPIRE", "mysortedset", "3", "MEMBERS", "1", "member1");
            var results = (RedisResult[])result;
            ClassicAssert.AreEqual(1, results.Length);
            ClassicAssert.AreEqual(-2, (long)results[0]);
        }

        [Test]
        [TestCase("ZEXPIRE", "NX", Description = "Set expiry only when no expiration exists")]
        [TestCase("ZEXPIRE", "XX", Description = "Set expiry only when expiration exists")]
        [TestCase("ZEXPIRE", "GT", Description = "Set expiry only when new TTL is greater")]
        [TestCase("ZEXPIRE", "LT", Description = "Set expiry only when new TTL is less")]
        [TestCase("ZPEXPIRE", "NX", Description = "Set expiry only when no expiration exists")]
        [TestCase("ZPEXPIRE", "XX", Description = "Set expiry only when expiration exists")]
        [TestCase("ZPEXPIRE", "GT", Description = "Set expiry only when new TTL is greater")]
        [TestCase("ZPEXPIRE", "LT", Description = "Set expiry only when new TTL is less")]
        [TestCase("ZEXPIREAT", "NX", Description = "Set expiry only when no expiration exists")]
        [TestCase("ZEXPIREAT", "XX", Description = "Set expiry only when expiration exists")]
        [TestCase("ZEXPIREAT", "GT", Description = "Set expiry only when new TTL is greater")]
        [TestCase("ZEXPIREAT", "LT", Description = "Set expiry only when new TTL is less")]
        [TestCase("ZPEXPIREAT", "NX", Description = "Set expiry only when no expiration exists")]
        [TestCase("ZPEXPIREAT", "XX", Description = "Set expiry only when expiration exists")]
        [TestCase("ZPEXPIREAT", "GT", Description = "Set expiry only when new TTL is greater")]
        [TestCase("ZPEXPIREAT", "LT", Description = "Set expiry only when new TTL is less")]
        public void CanDoSortedSetExpireWithOptions(string command, string option)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SortedSetAdd("mysortedset",
            [
                new SortedSetEntry("member1", 1),
                new SortedSetEntry("member2", 2),
                new SortedSetEntry("member3", 3),
                new SortedSetEntry("member4", 4)
            ]);

            (var expireTimeMember1, var expireTimeMember3, var newExpireTimeMember) = command switch
            {
                "ZEXPIRE" => ("2", "6", "4"),
                "ZPEXPIRE" => ("2000", "6000", "4000"),
                "ZEXPIREAT" => (DateTimeOffset.UtcNow.AddSeconds(2).ToUnixTimeSeconds().ToString(), DateTimeOffset.UtcNow.AddSeconds(6).ToUnixTimeSeconds().ToString(), DateTimeOffset.UtcNow.AddSeconds(4).ToUnixTimeSeconds().ToString()),
                "ZPEXPIREAT" => (DateTimeOffset.UtcNow.AddSeconds(2).ToUnixTimeMilliseconds().ToString(), DateTimeOffset.UtcNow.AddSeconds(6).ToUnixTimeMilliseconds().ToString(), DateTimeOffset.UtcNow.AddSeconds(4).ToUnixTimeMilliseconds().ToString()),
                _ => throw new ArgumentException("Invalid command")
            };

            // First set TTL for member1 only
            db.Execute(command, "mysortedset", expireTimeMember1, "MEMBERS", "1", "member1");
            db.Execute(command, "mysortedset", expireTimeMember3, "MEMBERS", "1", "member3");

            // Try setting TTL with option
            var result = (RedisResult[])db.Execute(command, "mysortedset", newExpireTimeMember, option, "MEMBERS", "3", "member1", "member2", "member3");

            switch (option)
            {
                case "NX":
                    ClassicAssert.AreEqual(0, (long)result[0]); // member1 has TTL
                    ClassicAssert.AreEqual(1, (long)result[1]); // member2 no TTL
                    ClassicAssert.AreEqual(0, (long)result[2]); // member3 has TTL
                    break;
                case "XX":
                    ClassicAssert.AreEqual(1, (long)result[0]); // member1 has TTL
                    ClassicAssert.AreEqual(0, (long)result[1]); // member2 no TTL
                    ClassicAssert.AreEqual(1, (long)result[2]); // member3 has TTL
                    break;
                case "GT":
                    ClassicAssert.AreEqual(1, (long)result[0]); // 4 > 2
                    ClassicAssert.AreEqual(0, (long)result[1]); // no TTL = infinite
                    ClassicAssert.AreEqual(0, (long)result[2]); // 4 !> 6
                    break;
                case "LT":
                    ClassicAssert.AreEqual(0, (long)result[0]); // 4 !< 2
                    ClassicAssert.AreEqual(1, (long)result[1]); // no TTL = infinite
                    ClassicAssert.AreEqual(1, (long)result[2]); // 4 < 6
                    break;
            }
        }

        [Test]
        public void CanDoSortedSetExpireWithAofRecovery()
        {
            // Test AOF recovery of sorted set entries with expiry

            var key1 = "key1";
            var key2 = "key2";
            SortedSetEntry[] values1_1 = [new SortedSetEntry("val1_1", 1.1), new SortedSetEntry("val1_2", 1.2)];
            SortedSetEntry[] values1_2 = [new SortedSetEntry("val1_3", 1.3), new SortedSetEntry("val1_4", 1.4)];
            SortedSetEntry[] values2_1 = [new SortedSetEntry("val2_1", 2.1), new SortedSetEntry("val2_2", 2.2)];
            SortedSetEntry[] values2_2 = [new SortedSetEntry("val2_3", 2.3), new SortedSetEntry("val2_4", 2.4)];

            var expireTime = DateTimeOffset.UtcNow + TimeSpan.FromMinutes(1);

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Set 1st sorted set, add long expiry to 1 entry
                db.SortedSetAdd(key1, values1_1);
                db.Execute("ZEXPIREAT", key1, expireTime.ToUnixTimeSeconds(), "MEMBERS", 1, "val1_1");

                // Set 2nd sorted set, add short expiry to 1 entry
                db.SortedSetAdd(key2, values2_1);
                db.Execute("ZEXPIRE", key2, 1, "MEMBERS", 1, "val2_1");

                // Wait for short expiry to pass
                Thread.Sleep(2000);

                // Add more entries to 1st and 2nd sorted sets
                db.SortedSetAdd(key1, values1_2);
                db.SortedSetAdd(key2, values2_2);

                // Add longer expiry to entry in 2nd sorted set
                db.Execute("ZPEXPIRE", key2, 15000, "MEMBERS", 1, "val2_2");
                Thread.Sleep(2000);

                // Verify 1st sorted set contains all added entries
                var recoveredValues = db.SortedSetRangeByScoreWithScores(key1);
                CollectionAssert.AreEqual(values1_1.Union(values1_2), recoveredValues);

                // Verify expiry times of entries in 1st sorted set
                var recoveredValuesExpTime = (RedisResult[])db.Execute("ZEXPIRETIME", key1, "MEMBERS", 2, "val1_1", "val1_2");
                ClassicAssert.IsNotNull(recoveredValuesExpTime);
                ClassicAssert.AreEqual(2, recoveredValuesExpTime!.Length);
                Assert.That(expireTime.ToUnixTimeSeconds(), Is.EqualTo((long)recoveredValuesExpTime[0]).Within(1));
                ClassicAssert.AreEqual(-1, (long)recoveredValuesExpTime[1]);

                // Verify 2nd sorted set contains all added entries except 1 expired entry
                recoveredValues = db.SortedSetRangeByScoreWithScores(key2);
                CollectionAssert.AreEqual(values2_1.Skip(1).Union(values2_2), recoveredValues);

                // Verify 2nd sorted set entries ttls
                var recoveredValuesTtl = (RedisResult[])db.Execute("ZTTL", key2, "MEMBERS", 4, "val2_1", "val2_2", "val2_3", "val2_4");
                ClassicAssert.IsNotNull(recoveredValuesTtl);
                ClassicAssert.AreEqual(4, recoveredValuesTtl!.Length);
                ClassicAssert.AreEqual(-2, (long)recoveredValuesTtl[0]);
                ClassicAssert.Less((long)recoveredValuesTtl[1], 13);
                ClassicAssert.Greater((long)recoveredValuesTtl[1], 0);
                ClassicAssert.AreEqual(-1, (long)recoveredValuesTtl[2]);
                ClassicAssert.AreEqual(-1, (long)recoveredValuesTtl[3]);
            }

            // Commit to AOF and restart server
            server.Store.CommitAOF(true);
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Verify 1st sorted set contains all added entries
                var recoveredValues = db.SortedSetRangeByScoreWithScores(key1);
                CollectionAssert.AreEqual(values1_1.Union(values1_2), recoveredValues);

                // Verify expiry times of entries in 1st sorted set
                var recoveredValuesExpTime = (RedisResult[])db.Execute("ZEXPIRETIME", key1, "MEMBERS", 2, "val1_1", "val1_2");
                ClassicAssert.IsNotNull(recoveredValuesExpTime);
                ClassicAssert.AreEqual(2, recoveredValuesExpTime!.Length);
                Assert.That(expireTime.ToUnixTimeSeconds(), Is.EqualTo((long)recoveredValuesExpTime[0]).Within(1));
                ClassicAssert.AreEqual(-1, (long)recoveredValuesExpTime[1]);

                // Verify 2nd sorted set contains all added entries except 1 expired entry
                recoveredValues = db.SortedSetRangeByScoreWithScores(key2);
                CollectionAssert.AreEqual(values2_1.Skip(1).Union(values2_2), recoveredValues);

                // Verify 2nd sorted set entries ttls
                var recoveredValuesTtl = (RedisResult[])db.Execute("ZPTTL", key2, "MEMBERS", 4, "val2_1", "val2_2", "val2_3", "val2_4");
                ClassicAssert.IsNotNull(recoveredValuesTtl);
                ClassicAssert.AreEqual(4, recoveredValuesTtl!.Length);
                ClassicAssert.AreEqual(-2, (long)recoveredValuesTtl[0]);
                ClassicAssert.Less((long)recoveredValuesTtl[1], 13000);
                ClassicAssert.Greater((long)recoveredValuesTtl[1], 0);
                ClassicAssert.AreEqual(-1, (long)recoveredValuesTtl[2]);
                ClassicAssert.AreEqual(-1, (long)recoveredValuesTtl[3]);
            }
        }

        [Test]
        public async Task ZDiffWithExpiredAndExpiringItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted sets
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            db.SortedSetAdd("key2", "a", 1);
            db.SortedSetAdd("key2", "b", 2);
            db.SortedSetAdd("key2", "c", 3);

            // Set expiration for some items in key1
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "c");
            db.Execute("ZPEXPIRE", "key1", "500", "MEMBERS", "1", "d");

            // Set expiration for matching items in key2
            db.Execute("ZPEXPIRE", "key2", "200", "MEMBERS", "1", "a");

            await Task.Delay(300);

            // Perform ZDIFF
            var diff = db.SortedSetCombine(SetOperation.Difference, ["key1", "key2"]);
            ClassicAssert.AreEqual(2, diff.Length); // Only "d" and "e" should remain

            // Perform ZDIFF with scores
            var diffWithScores = db.SortedSetCombineWithScores(SetOperation.Difference, ["key1", "key2"]);
            ClassicAssert.AreEqual(2, diffWithScores.Length);
            ClassicAssert.AreEqual("d", diffWithScores[0].Element.ToString());
            ClassicAssert.AreEqual(4, diffWithScores[0].Score);
            ClassicAssert.AreEqual("e", diffWithScores[1].Element.ToString());
            ClassicAssert.AreEqual(5, diffWithScores[1].Score);

            await Task.Delay(300);

            // Perform ZDIFF again after more items have expired
            diff = db.SortedSetCombine(SetOperation.Difference, ["key1", "key2"]);
            ClassicAssert.AreEqual(1, diff.Length); // Only "e" should remain

            // Perform ZDIFF with scores again
            diffWithScores = db.SortedSetCombineWithScores(SetOperation.Difference, ["key1", "key2"]);
            ClassicAssert.AreEqual(1, diffWithScores.Length);
            ClassicAssert.AreEqual("e", diffWithScores[0].Element.ToString());
            ClassicAssert.AreEqual(5, diffWithScores[0].Score);
        }

        [Test]
        public async Task ZDiffStoreWithExpiredAndExpiringItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted sets
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            db.SortedSetAdd("key2", "a", 1);
            db.SortedSetAdd("key2", "b", 2);
            db.SortedSetAdd("key2", "c", 3);

            // Set expiration for some items in key1
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "c");
            db.Execute("ZPEXPIRE", "key1", "500", "MEMBERS", "1", "d");

            // Set expiration for matching items in key2
            db.Execute("ZPEXPIRE", "key2", "200", "MEMBERS", "1", "a");

            await Task.Delay(300);

            // Perform ZDIFFSTORE
            var diffStoreCount = db.SortedSetCombineAndStore(SetOperation.Difference, "key3", ["key1", "key2"]);
            ClassicAssert.AreEqual(2, diffStoreCount); // Only "d" and "e" should remain

            // Verify the stored result
            var diffStoreResult = db.SortedSetRangeByRankWithScores("key3");
            ClassicAssert.AreEqual(2, diffStoreResult.Length);
            ClassicAssert.AreEqual("d", diffStoreResult[0].Element.ToString());
            ClassicAssert.AreEqual(4, diffStoreResult[0].Score);
            ClassicAssert.AreEqual("e", diffStoreResult[1].Element.ToString());
            ClassicAssert.AreEqual(5, diffStoreResult[1].Score);

            await Task.Delay(300);

            // Perform ZDIFFSTORE again after more items have expired
            diffStoreCount = db.SortedSetCombineAndStore(SetOperation.Difference, "key3", ["key1", "key2"]);
            ClassicAssert.AreEqual(1, diffStoreCount); // Only "e" should remain

            // Verify the stored result again
            diffStoreResult = db.SortedSetRangeByRankWithScores("key3");
            ClassicAssert.AreEqual(1, diffStoreResult.Length);
            ClassicAssert.AreEqual("e", diffStoreResult[0].Element.ToString());
            ClassicAssert.AreEqual(5, diffStoreResult[0].Score);
        }

        [Test]
        public async Task ZIncrByWithExpiringAndExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);

            // Set expiration for some items in key1
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "1", "a");

            await Task.Delay(10);

            // Try to increment the score of an expiring item
            var newScore = db.SortedSetIncrement("key1", "a", 5);
            ClassicAssert.AreEqual(6, newScore);

            // Check the TTL of the expiring item
            var ttl = db.Execute("ZPTTL", "key1", "MEMBERS", "1", "a");
            ClassicAssert.LessOrEqual((long)ttl, 200);
            ClassicAssert.Greater((long)ttl, 0);

            await Task.Delay(200);

            // Check the item has expired
            ttl = db.Execute("ZPTTL", "key1", "MEMBERS", "1", "a");
            ClassicAssert.AreEqual(-2, (long)ttl);

            // Try to increment the score of an already expired item
            newScore = db.SortedSetIncrement("key1", "a", 5);
            ClassicAssert.AreEqual(5, newScore);

            // Verify the item is added back with the new score
            var score = db.SortedSetScore("key1", "a");
            ClassicAssert.AreEqual(5, score);
        }

        [Test]
        public async Task ZInterWithExpiredAndExpiringItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted sets
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            db.SortedSetAdd("key2", "a", 1);
            db.SortedSetAdd("key2", "b", 2);
            db.SortedSetAdd("key2", "c", 3);

            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "1", "c");
            db.Execute("ZPEXPIRE", "key1", "500", "MEMBERS", "1", "b");
            db.Execute("ZPEXPIRE", "key2", "200", "MEMBERS", "1", "a");

            var inter = db.SortedSetCombine(SetOperation.Intersect, ["key1", "key2"]);
            ClassicAssert.AreEqual(3, inter.Length);

            await Task.Delay(300);

            var interWithScores = db.SortedSetCombineWithScores(SetOperation.Intersect, ["key1", "key2"]);
            ClassicAssert.AreEqual(1, interWithScores.Length);  // Only "b" should remain
            ClassicAssert.AreEqual("b", interWithScores[0].Element.ToString());
            ClassicAssert.AreEqual(4, interWithScores[0].Score); // Sum of scores

            await Task.Delay(300);

            inter = db.SortedSetCombine(SetOperation.Intersect, ["key1", "key2"]);
            ClassicAssert.AreEqual(0, inter.Length);
        }

        [Test]
        public async Task ZInterCardWithExpiredAndExpiringItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            db.SortedSetAdd("key2", "a", 1);
            db.SortedSetAdd("key2", "b", 2);
            db.SortedSetAdd("key2", "c", 3);

            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "1", "c");
            db.Execute("ZPEXPIRE", "key1", "500", "MEMBERS", "1", "b");
            db.Execute("ZPEXPIRE", "key2", "200", "MEMBERS", "1", "a");

            await Task.Delay(300);

            var interCardCount = (long)db.Execute("ZINTERCARD", "2", "key1", "key2");
            ClassicAssert.AreEqual(1, interCardCount); // Only "b" should remain

            await Task.Delay(300);

            interCardCount = (long)db.Execute("ZINTERCARD", "2", "key1", "key2");
            ClassicAssert.AreEqual(0, interCardCount); // No items should remain
        }

        [Test]
        public async Task ZInterStoreWithExpiredAndExpiringItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            db.SortedSetAdd("key2", "a", 1);
            db.SortedSetAdd("key2", "b", 2);
            db.SortedSetAdd("key2", "c", 3);

            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "1", "c");
            db.Execute("ZPEXPIRE", "key1", "500", "MEMBERS", "1", "b");
            db.Execute("ZPEXPIRE", "key2", "200", "MEMBERS", "1", "a");

            await Task.Delay(300);

            var interStoreCount = db.SortedSetCombineAndStore(SetOperation.Intersect, "key3", ["key1", "key2"]);
            ClassicAssert.AreEqual(1, interStoreCount); // Only "b" should remain

            var interStoreResult = db.SortedSetRangeByRankWithScores("key3");
            ClassicAssert.AreEqual(1, interStoreResult.Length);
            ClassicAssert.AreEqual("b", interStoreResult[0].Element.ToString());
            ClassicAssert.AreEqual(4, interStoreResult[0].Score); // Sum of scores

            await Task.Delay(300);

            interStoreCount = db.SortedSetCombineAndStore(SetOperation.Intersect, "key3", ["key1", "key2"]);
            ClassicAssert.AreEqual(0, interStoreCount); // No items should remain

            interStoreResult = db.SortedSetRangeByRankWithScores("key3");
            ClassicAssert.AreEqual(0, interStoreResult.Length);
        }

        [Test]
        public async Task ZLexCountWithExpiredAndExpiringItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "3", "a", "e", "c");
            db.Execute("ZPEXPIRE", "key1", "500", "MEMBERS", "1", "b");

            await Task.Delay(300);

            var lexCount = (int)db.Execute("ZLEXCOUNT", "key1", "-", "+"); // SortedSetLengthByValue will check - and + to [- and [+
            ClassicAssert.AreEqual(2, lexCount); // Only "b" and "d" should remain

            var lexCountRange = db.SortedSetLengthByValue("key1", "b", "d", Exclude.Stop);
            ClassicAssert.AreEqual(1, lexCountRange); // Only "b" should remain within the range

            await Task.Delay(300);

            lexCount = (int)db.Execute("ZLEXCOUNT", "key1", "-", "+");
            ClassicAssert.AreEqual(1, lexCount); // Only "d" should remain

            lexCountRange = db.SortedSetLengthByValue("key1", "b", "d");
            ClassicAssert.AreEqual(1, lexCountRange); // Only "d" should remain within the range
        }

        [Test]
        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public async Task ZMPopWithExpiredItems(RedisProtocol protocol)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
            var db = redis.GetDatabase(0);

            db.SortedSetAdd("key0", "x", 1);
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for the minimum and maximum items
            db.Execute("ZPEXPIRE", "key0", "200", "MEMBERS", "1", "x");
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZMPOP with MIN option
            var result = db.Execute("ZMPOP", 2, "key0", "key1", "MIN", "COUNT", 2);
            ClassicAssert.IsNotNull(result);
            var popResult = (RedisResult[])result;
            ClassicAssert.AreEqual("key1", (string)popResult[0]);

            var poppedItems = (RedisResult[])popResult[1];
            ClassicAssert.AreEqual(2, poppedItems.Length);
            ClassicAssert.AreEqual("b", (string)poppedItems[0][0]);
            ClassicAssert.AreEqual("2", (string)poppedItems[0][1]);
            ClassicAssert.AreEqual("c", (string)poppedItems[1][0]);
            ClassicAssert.AreEqual("3", (string)poppedItems[1][1]);

            // Perform ZMPOP with MAX option
            result = db.Execute("ZMPOP", 2, "key0", "key1", "MAX", "COUNT", 2);
            ClassicAssert.IsNotNull(result);
            popResult = (RedisResult[])result;
            ClassicAssert.AreEqual("key1", (string)popResult[0]);

            poppedItems = (RedisResult[])popResult[1];
            ClassicAssert.AreEqual(1, poppedItems.Length);
            ClassicAssert.AreEqual("d", (string)poppedItems[0][0]);
            ClassicAssert.AreEqual("4", (string)poppedItems[0][1]);
        }

        [Test]
        public async Task ZMScoreWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for the minimum and maximum items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            var scores = db.SortedSetScores("key1", ["a", "b", "c", "d", "e"]);
            ClassicAssert.AreEqual(5, scores.Length);
            ClassicAssert.IsNull(scores[0]); // "a" should be expired
            ClassicAssert.AreEqual(2, scores[1]); // "b" should remain
            ClassicAssert.AreEqual(3, scores[2]); // "c" should remain
            ClassicAssert.AreEqual(4, scores[3]); // "d" should remain
            ClassicAssert.IsNull(scores[4]); // "e" should be expired
        }

        [Test]
        public async Task ZPopMaxWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for the minimum and maximum items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "c", "e");

            await Task.Delay(300);

            // Perform ZPOPMAX
            var result = db.SortedSetPop("key1", Order.Descending);
            ClassicAssert.IsNotNull(result);
            ClassicAssert.AreEqual("d", result.Value.Element.ToString());
            ClassicAssert.AreEqual(4, result.Value.Score);

            // Perform ZPOPMAX with COUNT option
            var results = db.SortedSetPop("key1", 2, Order.Descending);
            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual("b", results[0].Element.ToString());
            ClassicAssert.AreEqual(2, results[0].Score);
            ClassicAssert.AreEqual("a", results[1].Element.ToString());
            ClassicAssert.AreEqual(1, results[1].Score);
        }

        [Test]
        public async Task ZPopMinWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for the minimum and middle items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "c");

            await Task.Delay(300);

            // Perform ZPOPMIN
            var result = db.SortedSetPop("key1", Order.Ascending);
            ClassicAssert.IsNotNull(result);
            ClassicAssert.AreEqual("b", result.Value.Element.ToString());
            ClassicAssert.AreEqual(2, result.Value.Score);

            // Perform ZPOPMIN with COUNT option
            var results = db.SortedSetPop("key1", 2, Order.Ascending);
            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual("d", results[0].Element.ToString());
            ClassicAssert.AreEqual(4, results[0].Score);
            ClassicAssert.AreEqual("e", results[1].Element.ToString());
            ClassicAssert.AreEqual(5, results[1].Score);
        }

        [Test]
        public async Task ZRandMemberWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZRANDMEMBER
            var randMember = db.SortedSetRandomMember("key1");

            ClassicAssert.IsFalse(randMember.IsNull);
            ClassicAssert.IsTrue(new[] { "b", "c", "d" }.Contains(randMember.ToString()));

            // Perform ZRANDMEMBER with count
            var randMembers = db.SortedSetRandomMembers("key1", 4);
            ClassicAssert.AreEqual(3, randMembers.Length);
            CollectionAssert.AreEquivalent(new[] { "b", "c", "d" }, randMembers.Select(member => member.ToString()).ToList());

            // Perform ZRANDMEMBER with count and WITHSCORES
            var randMembersWithScores = db.SortedSetRandomMembersWithScores("key1", 4);
            ClassicAssert.AreEqual(3, randMembersWithScores.Length);
            CollectionAssert.AreEquivalent(new[] { "b", "c", "d" }, randMembersWithScores.Select(member => member.Element.ToString()).ToList());
        }

        [Test]
        public async Task ZRangeWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZRANGE with BYSCORE option
            var result = (RedisValue[])db.Execute("ZRANGE", "key1", "1", "5", "BYSCORE");
            ClassicAssert.AreEqual(3, result.Length);
            CollectionAssert.AreEqual(new[] { "b", "c", "d" }, result.Select(r => r.ToString()).ToList());

            // Perform ZRANGE with BYLEX option
            result = (RedisValue[])db.Execute("ZRANGE", "key1", "[b", "[d", "BYLEX");
            ClassicAssert.AreEqual(3, result.Length);
            CollectionAssert.AreEqual(new[] { "b", "c", "d" }, result.Select(r => r.ToString()).ToList());

            // Perform ZRANGE with REV option
            result = (RedisValue[])db.Execute("ZRANGE", "key1", "5", "1", "BYSCORE", "REV");
            ClassicAssert.AreEqual(3, result.Length);
            CollectionAssert.AreEqual(new[] { "d", "c", "b" }, result.Select(r => r.ToString()).ToList());

            // Perform ZRANGE with LIMIT option
            result = (RedisValue[])db.Execute("ZRANGE", "key1", "1", "5", "BYSCORE", "LIMIT", "1", "2");
            ClassicAssert.AreEqual(2, result.Length);
            CollectionAssert.AreEqual(new[] { "c", "d" }, result.Select(r => r.ToString()).ToList());
        }

        [Test]
        public async Task ZRangeByLexWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 1);
            db.SortedSetAdd("key1", "c", 1);
            db.SortedSetAdd("key1", "d", 1);
            db.SortedSetAdd("key1", "e", 1);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZRANGEBYLEX with expired items
            var result = (RedisResult[])db.Execute("ZRANGEBYLEX", "key1", "[a", "[e");
            ClassicAssert.AreEqual(3, result.Length);
            CollectionAssert.AreEqual(new[] { "b", "c", "d" }, result.Select(r => r.ToString()).ToList());
        }

        [Test]
        public async Task ZRangeByScoreWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZRANGEBYSCORE
            var result = (RedisValue[])db.Execute("ZRANGEBYSCORE", "key1", "1", "5");
            ClassicAssert.AreEqual(3, result.Length);
            CollectionAssert.AreEqual(new[] { "b", "c", "d" }, result.Select(r => r.ToString()).ToList());

            // Perform ZRANGEBYSCORE with expired items
            result = (RedisValue[])db.Execute("ZRANGEBYSCORE", "key1", "1", "5");
            ClassicAssert.AreEqual(3, result.Length);
            CollectionAssert.AreEqual(new[] { "b", "c", "d" }, result.Select(r => r.ToString()).ToList());
        }

        [Test]
        public async Task ZRangeStoreWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZRANGESTORE with BYSCORE option
            db.Execute("ZRANGESTORE", "key2", "key1", "1", "5", "BYSCORE");
            var result = db.SortedSetRangeByRank("key2");
            ClassicAssert.AreEqual(3, result.Length);
            CollectionAssert.AreEqual(new[] { "b", "c", "d" }, result.Select(r => r.ToString()).ToList());

            // Perform ZRANGESTORE with BYLEX option
            db.Execute("ZRANGESTORE", "key2", "key1", "[b", "[d", "BYLEX");
            result = db.SortedSetRangeByRank("key2");
            ClassicAssert.AreEqual(3, result.Length);
            CollectionAssert.AreEqual(new[] { "b", "c", "d" }, result.Select(r => r.ToString()).ToList());

            // Perform ZRANGESTORE with REV option
            db.Execute("ZRANGESTORE", "key2", "key1", "5", "1", "BYSCORE", "REV");
            result = db.SortedSetRangeByRank("key2");
            ClassicAssert.AreEqual(3, result.Length);
            CollectionAssert.AreEqual(new[] { "b", "c", "d" }, result.Select(r => r.ToString()).ToList());

            // Perform ZRANGESTORE with LIMIT option
            db.Execute("ZRANGESTORE", "key2", "key1", "1", "5", "BYSCORE", "LIMIT", "1", "2");
            result = db.SortedSetRangeByRank("key2");
            ClassicAssert.AreEqual(2, result.Length);
            CollectionAssert.AreEqual(new[] { "c", "d" }, result.Select(r => r.ToString()).ToList());
        }

        [Test]
        public async Task ZRankWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZRANK
            var rank = db.SortedSetRank("key1", "a");
            ClassicAssert.IsNull(rank); // "a" should be expired

            rank = db.SortedSetRank("key1", "b");
            ClassicAssert.AreEqual(0, rank); // "b" should be at rank 0
        }

        [Test]
        public async Task ZRemWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZREM on expired and non-expired items
            var removedCount = db.SortedSetRemove("key1", ["a", "b", "e"]);
            ClassicAssert.AreEqual(1, removedCount); // "a" and "e" should be expired, "b" should be removed

            // Verify remaining items in the sorted set
            var remainingItems = db.SortedSetRangeByRank("key1");
            ClassicAssert.AreEqual(2, remainingItems.Length);
            CollectionAssert.AreEqual(new[] { "c", "d" }, remainingItems.Select(r => r.ToString()).ToList());
        }

        [Test]
        public async Task ZRemRangeByLexWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 1);
            db.SortedSetAdd("key1", "c", 1);
            db.SortedSetAdd("key1", "d", 1);
            db.SortedSetAdd("key1", "e", 1);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZREMRANGEBYLEX with expired items
            var removedCount = db.Execute("ZREMRANGEBYLEX", "key1", "[a", "[e");
            ClassicAssert.AreEqual(3, (int)removedCount); // Only "b", "c", and "d" should be removed

            // Verify remaining items in the sorted set
            var remainingItems = db.SortedSetRangeByRank("key1");
            ClassicAssert.AreEqual(0, remainingItems.Length); // All items should be removed
        }

        [Test]
        public async Task ZRemRangeByRankWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZREMRANGEBYRANK with expired items
            var removedCount = db.Execute("ZREMRANGEBYRANK", "key1", 0, 1);
            ClassicAssert.AreEqual(2, (int)removedCount); // Only "b" and "c" should be removed

            // Verify remaining items in the sorted set
            var remainingItems = db.SortedSetRangeByRank("key1");
            ClassicAssert.AreEqual(1, remainingItems.Length);
            CollectionAssert.AreEqual(new[] { "d" }, remainingItems.Select(r => r.ToString()).ToList());
        }

        [Test]
        public async Task ZRemRangeByScoreWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZREMRANGEBYSCORE with expired items
            var removedCount = db.Execute("ZREMRANGEBYSCORE", "key1", 1, 5);
            ClassicAssert.AreEqual(3, (int)removedCount); // Only "b", "c", and "d" should be removed

            // Verify remaining items in the sorted set
            var remainingItems = db.SortedSetRangeByRank("key1");
            ClassicAssert.AreEqual(0, remainingItems.Length); // All items should be removed
        }

        [Test]
        public async Task ZRevRangeWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZREVRANGE with expired items
            var result = db.Execute("ZREVRANGE", "key1", 0, -1);
            var items = (RedisValue[])result;
            ClassicAssert.AreEqual(3, items.Length); // Only "b", "c", and "d" should remain
            CollectionAssert.AreEqual(new[] { "d", "c", "b" }, items.Select(r => r.ToString()).ToList());
        }

        [Test]
        public async Task ZRevRangeByLexWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 1);
            db.SortedSetAdd("key1", "c", 1);
            db.SortedSetAdd("key1", "d", 1);
            db.SortedSetAdd("key1", "e", 1);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZREVRANGEBYLEX with expired items
            var result = db.Execute("ZREVRANGEBYLEX", "key1", "[e", "[a");
            var items = (RedisValue[])result;
            ClassicAssert.AreEqual(3, items.Length); // Only "b", "c", and "d" should remain
            CollectionAssert.AreEqual(new[] { "d", "c", "b" }, items.Select(r => r.ToString()).ToList());
        }

        [Test]
        public async Task ZRevRangeByScoreWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZREVRANGEBYSCORE with expired items
            var result = db.Execute("ZREVRANGEBYSCORE", "key1", 5, 1);
            var items = (RedisValue[])result;
            ClassicAssert.AreEqual(3, items.Length); // Only "b", "c", and "d" should remain
            CollectionAssert.AreEqual(new[] { "d", "c", "b" }, items.Select(r => r.ToString()).ToList());
        }

        [Test]
        public async Task ZRevRankWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");

            await Task.Delay(300);

            // Perform ZREVRANK on expired and non-expired items
            var result = db.Execute("ZREVRANK", "key1", "a");
            ClassicAssert.True(result.IsNull); // "a" should be expired

            result = db.Execute("ZREVRANK", "key1", "b");
            ClassicAssert.AreEqual(2, (int)result); // "b" should be at reverse rank 2
        }

        [Test]
        [TestCase(2, Description = "RESP2 output")]
        [TestCase(3, Description = "RESP3 output")]
        public async Task ZRespOutput(byte respVersion)
        {
            using var c = TestUtils.GetGarnetClientSession(raw: true);
            c.Connect();

            var response = await c.ExecuteAsync("HELLO", respVersion.ToString());

            response = await c.ExecuteAsync("ZADD", "z", "0", "a", "1", "b");
            ClassicAssert.AreEqual(":2\r\n", response);

            var expectedResponse = (respVersion >= 3) ?
                        "*2\r\n*2\r\n$1\r\na\r\n,0\r\n*2\r\n$1\r\nb\r\n,1\r\n" :
                        "*4\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n$1\r\n1\r\n";
            response = await c.ExecuteAsync("ZRANGE", "z", "0", "-1", "WITHSCORES");
            ClassicAssert.AreEqual(expectedResponse, response);
            response = await c.ExecuteAsync("ZUNION", "2", "z", "nx", "WITHSCORES");
            ClassicAssert.AreEqual(expectedResponse, response);
            response = await c.ExecuteAsync("ZDIFF", "2", "z", "nx", "WITHSCORES");
            ClassicAssert.AreEqual(expectedResponse, response);

            response = await c.ExecuteAsync("ZMPOP", "1", "z", "MIN");
            if (respVersion >= 3)
                ClassicAssert.AreEqual("*2\r\n$1\r\nz\r\n*1\r\n*2\r\n$1\r\na\r\n,0\r\n", response);
            else
                ClassicAssert.AreEqual("*2\r\n$1\r\nz\r\n*1\r\n*2\r\n$1\r\na\r\n$1\r\n0\r\n", response);
            response = await c.ExecuteAsync("ZRANDMEMBER", "z", "1", "WITHSCORES");
            if (respVersion >= 3)
                ClassicAssert.AreEqual("*1\r\n*2\r\n$1\r\nb\r\n,1\r\n", response);
            else
                ClassicAssert.AreEqual("*2\r\n$1\r\nb\r\n$1\r\n1\r\n", response);
            response = await c.ExecuteAsync("ZPOPMAX", "z", "1");
            if (respVersion >= 3)
                ClassicAssert.AreEqual("*1\r\n*2\r\n$1\r\nb\r\n,1\r\n", response);
            else
                ClassicAssert.AreEqual("*2\r\n$1\r\nb\r\n$1\r\n1\r\n", response);
        }

        [Test]
        public async Task ZScanWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            // Set expiration for some items
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "e");
            db.Execute("ZPEXPIRE", "key1", "1000", "MEMBERS", "1", "c");

            await Task.Delay(300);

            // Perform ZSCAN
            var result = db.Execute("ZSCAN", "key1", "0");
            var items = (RedisResult[])result;
            var cursor = (long)items[0];
            var elements = (RedisValue[])items[1];

            // Verify that expired items are not returned
            ClassicAssert.AreEqual(6, elements.Length); // Only "b", "c", and "d" should remain
            CollectionAssert.AreEqual(new[] { "b", "2", "c", "3", "d", "4" }, elements.Select(r => r.ToString()).ToList());
            ClassicAssert.AreEqual(0, cursor); // Ensure the cursor indicates the end of the collection
        }

        [Test]
        public async Task ZScoreWithExpiringAndExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted set
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);

            // Set expiration for some items in key1
            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "1", "a");

            await Task.Delay(10);

            // Check the score of an expiring item
            var score = db.SortedSetScore("key1", "a");
            ClassicAssert.AreEqual(1, score);

            // Check the TTL of the expiring item
            var ttl = db.Execute("ZPTTL", "key1", "MEMBERS", "1", "a");
            ClassicAssert.LessOrEqual((long)ttl, 200);
            ClassicAssert.Greater((long)ttl, 0);

            await Task.Delay(200);

            // Check the item has expired
            ttl = db.Execute("ZPTTL", "key1", "MEMBERS", "1", "a");
            ClassicAssert.AreEqual(-2, (long)ttl);

            // Check the score of an already expired item
            score = db.SortedSetScore("key1", "a");
            ClassicAssert.IsNull(score);

            // Check the score of a non-expiring item
            score = db.SortedSetScore("key1", "b");
            ClassicAssert.AreEqual(2, score);
        }

        [Test]
        public async Task ZUnionWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted sets
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            db.SortedSetAdd("key2", "a", 1);
            db.SortedSetAdd("key2", "b", 2);
            db.SortedSetAdd("key2", "c", 3);

            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "c");
            db.Execute("ZPEXPIRE", "key1", "500", "MEMBERS", "1", "b");
            db.Execute("ZPEXPIRE", "key2", "200", "MEMBERS", "1", "a");

            var union = db.SortedSetCombine(SetOperation.Union, ["key1", "key2"]);
            ClassicAssert.AreEqual(5, union.Length);

            await Task.Delay(300);

            var unionWithScores = db.SortedSetCombineWithScores(SetOperation.Union, ["key1", "key2"]);
            ClassicAssert.AreEqual(4, unionWithScores.Length);
        }

        [Test]
        public async Task ZUnionStoreWithExpiredItems()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add items to the sorted sets
            db.SortedSetAdd("key1", "a", 1);
            db.SortedSetAdd("key1", "b", 2);
            db.SortedSetAdd("key1", "c", 3);
            db.SortedSetAdd("key1", "d", 4);
            db.SortedSetAdd("key1", "e", 5);

            db.SortedSetAdd("key2", "a", 1);
            db.SortedSetAdd("key2", "b", 2);
            db.SortedSetAdd("key2", "c", 3);

            db.Execute("ZPEXPIRE", "key1", "200", "MEMBERS", "2", "a", "c");
            db.Execute("ZPEXPIRE", "key1", "1000", "MEMBERS", "1", "b");
            db.Execute("ZPEXPIRE", "key2", "200", "MEMBERS", "1", "a");

            var unionStoreCount = db.SortedSetCombineAndStore(SetOperation.Union, "key3", ["key1", "key2"]);
            ClassicAssert.AreEqual(5, unionStoreCount);
            var unionStoreResult = db.SortedSetRangeByRankWithScores("key3");
            ClassicAssert.AreEqual(5, unionStoreResult.Length);

            await Task.Delay(300);

            unionStoreCount = db.SortedSetCombineAndStore(SetOperation.Union, "key3", ["key1", "key2"]);
            ClassicAssert.AreEqual(4, unionStoreCount);
            unionStoreResult = db.SortedSetRangeByRankWithScores("key3");
            ClassicAssert.AreEqual(4, unionStoreResult.Length);
            CollectionAssert.AreEquivalent(new[] { "b", "c", "d", "e" }, unionStoreResult.Select(x => x.Element.ToString()));
        }

        [Test]
        public void ZInterWithFirstKeyNotExisting()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SortedSetAdd("zset2",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("four", 4)
            ]);

            var result = db.SortedSetCombine(SetOperation.Intersect, [new RedisKey("nonexistentkey"), new RedisKey("zset2")]);
            ClassicAssert.AreEqual(0, result.Length);

            var resultWithScores = db.SortedSetCombineWithScores(SetOperation.Intersect, [new RedisKey("nonexistentkey"), new RedisKey("zset2")]);
            ClassicAssert.AreEqual(0, resultWithScores.Length);
        }

        [Test]
        public void ZInterStoreWithFirstKeyNotExisting()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SortedSetAdd("zset2",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("four", 4)
            ]);

            var result = db.SortedSetCombineAndStore(SetOperation.Intersect, "dest", [new RedisKey("nonexistentkey"), new RedisKey("zset2")]);
            ClassicAssert.AreEqual(0, result);

            var exists = db.KeyExists("dest");
            ClassicAssert.IsFalse(exists);
        }

        [Test]
        public void ZInterCardWithFirstKeyNotExisting()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SortedSetAdd("zset2",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("four", 4)
            ]);

            var result = (long)db.Execute("ZINTERCARD", "2", "nonexistentkey", "zset2");
            ClassicAssert.AreEqual(0, result);

            result = (long)db.Execute("ZINTERCARD", "2", "nonexistentkey", "zset2", "LIMIT", "10");
            ClassicAssert.AreEqual(0, result);
        }

        [Test]
        public void ZDiffWithFirstKeyNotExisting()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SortedSetAdd("zset2",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("four", 4)
            ]);

            var diff = db.SortedSetCombine(SetOperation.Difference, [new RedisKey("nonexistentkey"), new RedisKey("zset2")]);
            ClassicAssert.AreEqual(0, diff.Length);

            var diffWithScores = db.SortedSetCombineWithScores(SetOperation.Difference, [new RedisKey("nonexistentkey"), new RedisKey("zset2")]);
            ClassicAssert.AreEqual(0, diffWithScores.Length);
        }

        [Test]
        public void ZDiffStoreWithFirstKeyNotExisting()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SortedSetAdd("zset2",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("four", 4)
            ]);

            db.SortedSetAdd("dest",
            [
                new SortedSetEntry("existing", 100)
            ]);

            var result = db.SortedSetCombineAndStore(SetOperation.Difference, "dest", [new RedisKey("nonexistentkey"), new RedisKey("zset2")]);
            ClassicAssert.AreEqual(0, result);

            var exists = db.KeyExists("dest");
            ClassicAssert.IsFalse(exists);
        }

        [Test]
        public void ZUnionWithFirstKeyNotExisting()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SortedSetAdd("zset2",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("four", 4)
            ]);

            var result = db.SortedSetCombine(SetOperation.Union, [new RedisKey("nonexistentkey"), new RedisKey("zset2")]);
            ClassicAssert.AreEqual(3, result.Length);
            CollectionAssert.AreEqual(new string[] { "one", "two", "four" }, result.Select(r => r.ToString()).ToArray());

            var resultWithScores = db.SortedSetCombineWithScores(SetOperation.Union, [new RedisKey("nonexistentkey"), new RedisKey("zset2")]);
            ClassicAssert.AreEqual(3, resultWithScores.Length);
            ClassicAssert.AreEqual("one", resultWithScores[0].Element.ToString());
            ClassicAssert.AreEqual(1, resultWithScores[0].Score);
            ClassicAssert.AreEqual("two", resultWithScores[1].Element.ToString());
            ClassicAssert.AreEqual(2, resultWithScores[1].Score);
            ClassicAssert.AreEqual("four", resultWithScores[2].Element.ToString());
            ClassicAssert.AreEqual(4, resultWithScores[2].Score);
        }

        [Test]
        public void ZUnionStoreWithFirstKeyNotExisting()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SortedSetAdd("zset2",
            [
                new SortedSetEntry("one", 1),
                new SortedSetEntry("two", 2),
                new SortedSetEntry("four", 4)
            ]);
            db.SortedSetAdd("dest",
            [
                new SortedSetEntry("existing", 100)
            ]);

            var result = db.SortedSetCombineAndStore(SetOperation.Union, "dest", [new RedisKey("nonexistentkey"), new RedisKey("zset2")]);
            ClassicAssert.AreEqual(3, result);

            var storedValues = db.SortedSetRangeByScoreWithScores("dest");
            ClassicAssert.AreEqual(3, storedValues.Length);
            ClassicAssert.AreEqual("one", storedValues[0].Element.ToString());
            ClassicAssert.AreEqual(1, storedValues[0].Score);
            ClassicAssert.AreEqual("two", storedValues[1].Element.ToString());
            ClassicAssert.AreEqual(2, storedValues[1].Score);
            ClassicAssert.AreEqual("four", storedValues[2].Element.ToString());
            ClassicAssert.AreEqual(4, storedValues[2].Score);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommands("ZCOUNT board 500 700", "PING");
            expectedResponse = ":4\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZCOUNT board 500 700", bytesPerSend);
            expectedResponse = ":4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // get a range by index with scores
            response = lightClientRequest.SendCommandChunks("ZRANGE board 0 -1 WITHSCORES", bytesSent, 7);
            expectedResponse = "*6\r\n$3\r\none\r\n$1\r\n1\r\n$3\r\ntwo\r\n$1\r\n2\r\n$5\r\nthree\r\n$1\r\n3\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board 2 3", 2);
            expectedResponse = "*1\r\n$5\r\nthree\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board -2 -1", 3);
            expectedResponse = "*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board -2 -1 WITHSCORES", 5);
            expectedResponse = "*4\r\n$3\r\ntwo\r\n$1\r\n2\r\n$5\r\nthree\r\n$1\r\n3\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board -50 -1 WITHSCORES", 7);
            expectedResponse = "*6\r\n$3\r\none\r\n$1\r\n1\r\n$3\r\ntwo\r\n$1\r\n2\r\n$5\r\nthree\r\n$1\r\n3\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board -50 -10 WITHSCORES", 1);
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board 2 1 WITHSCORES", 1);
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board -1 -2 WITHSCORES", 1);
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board 50 60 WITHSCORES", 1);
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board (1 +inf BYSCORE LIMIT 1 1", 2);
            expectedResponse = "*1\r\n$5\r\nthree\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZRANGE board (1 +inf BYSCORE LIMIT 1 1", bytesSent, 2);
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        // ZRANGEBSTORE
        [Test]
        [TestCase("user1:obj1", "user1:objA", new[] { "Hello", "World" }, new[] { 1.0, 2.0 }, new[] { "Hello", "World" }, new[] { 1.0, 2.0 })] // Normal case
        [TestCase("user1:emptySet", "user1:objB", new string[] { }, new double[] { }, new string[] { }, new double[] { })] // Empty set
        [TestCase("user1:nonExistingKey", "user1:objC", new string[] { }, new double[] { }, new string[] { }, new double[] { })] // Non-existing key
        [TestCase("user1:obj2", "user1:objD", new[] { "Alpha", "Beta", "Gamma" }, new[] { 1.0, 2.0, 3.0 }, new[] { "Beta", "Gamma" }, new[] { 2.0, 3.0 }, -2, -1)] // Negative range
        public void CheckSortedSetRangeStoreLC(string key, string destinationKey, string[] elements, double[] scores, string[] expectedElements, double[] expectedScores, int start = 0, int stop = -1)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            byte[] response;
            string expectedResponse;

            // Setup initial sorted set if elements exist
            if (elements.Length > 0)
            {
                var addCommand = $"ZADD {key} " + string.Join(" ", elements.Zip(scores, (e, s) => $"{s} {e}"));
                response = lightClientRequest.SendCommand(addCommand);
                expectedResponse = $":{elements.Length}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            }

            // Execute ZRANGESTORE
            var rangeStoreCommand = $"ZRANGESTORE {destinationKey} {key} {start} {stop}";
            response = lightClientRequest.SendCommand(rangeStoreCommand);
            expectedResponse = $":{expectedElements.Length}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Verify stored result using ZRANGE
            if (expectedElements.Length > 0)
            {
                var verifyCommand = $"ZRANGE {destinationKey} 0 -1 WITHSCORES";
                response = lightClientRequest.SendCommand(verifyCommand, expectedElements.Length * 2 + 1);
                var expectedItems = new List<string>
                {
                    $"*{expectedElements.Length * 2}"
                };

                for (var i = 0; i < expectedElements.Length; i++)
                {
                    expectedItems.Add($"${expectedElements[i].Length}");
                    expectedItems.Add(expectedElements[i]);
                    expectedItems.Add($"${expectedScores[i].ToString().Length}");
                    expectedItems.Add(expectedScores[i].ToString());
                }
                expectedResponse = string.Join("\r\n", expectedItems) + "\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            }
            else
            {
                var verifyCommand = $"ZRANGE {destinationKey} 0 -1";
                response = lightClientRequest.SendCommand(verifyCommand);
                expectedResponse = "*0\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            }
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // 1 < score <= 5
            response = lightClientRequest.SendCommands("ZRANGEBYSCORE board (1 5", "PING", 3, 1);
            expectedResponse = "*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // 1 < score <= 5
            response = lightClientRequest.SendCommandChunks("ZRANGE board 5 2 BYSCORE REV", bytesSent, 3);
            expectedResponse = "*2\r\n$5\r\nthree\r\n$3\r\ntwo\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // 1 < score <= 5
            response = lightClientRequest.SendCommands("ZRANGE board 5 2 BYSCORE REV", "PING", 3, 1);
            expectedResponse = "*2\r\n$5\r\nthree\r\n$3\r\ntwo\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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

            expectedResponse = "*0\r\n";
            response = lightClientRequest.Execute("ZRANGEBYSCORE mysales -inf +inf WITHSCORES LIMIT 4 0", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);
        }

        [Test]
        public void CanDoZRangeByLex()
        {
            //ZRANGE key min max BYLEX [WITHSCORES]
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 0 a 0 b 0 c 0 d 0 e 0 f 0 g");

            // Test range by lex order
            response = lightClientRequest.SendCommand("ZRANGE board (a (d BYLEX", 3);
            var expectedResponse = "*2\r\n$1\r\nb\r\n$1\r\nc\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // By lex with different range
            response = lightClientRequest.SendCommand("ZRANGE board [aaa (g BYLEX", 6);
            expectedResponse = "*5\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\nf\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // By lex with different range
            response = lightClientRequest.SendCommand("ZRANGE board - [c BYLEX", 4);
            expectedResponse = "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // ZRANGEBYLEX Synonym
            response = lightClientRequest.SendCommand("ZRANGEBYLEX board - [c", 4);
            //expectedResponse = "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // By lex when the mininum is over the maximum value in zset
            response = lightClientRequest.SendCommand("ZRANGEBYLEX board [x [z");
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Test infinites
            response = lightClientRequest.SendCommand("ZRANGE board - - BYLEX");
            //expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board - (+ BYLEX");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board + - BYLEX");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board + + BYLEX");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board - + BYLEX");
            expectedResponse = "*7\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\nf\r\n$1\r\ng\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board [+ + BYLEX");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoZRangeByLexReverse()
        {
            //ZRANGE key min max BYLEX REV [WITHSCORES]
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 0 a 0 b 0 c 0 d 0 e 0 f 0 g");

            // get a range by lex order
            response = lightClientRequest.SendCommand("ZRANGE board (a (d BYLEX REV", 1);
            var expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // get a range by lex order
            response = lightClientRequest.SendCommand("ZRANGE board (d (a BYLEX REV", 3);
            expectedResponse = "*2\r\n$1\r\nc\r\n$1\r\nb\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //by lex with different range
            response = lightClientRequest.SendCommand("ZRANGE board [g (aaa BYLEX REV", 6);
            expectedResponse = "*6\r\n$1\r\ng\r\n$1\r\nf\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //by lex with different range
            response = lightClientRequest.SendCommand("ZRANGE board [c - BYLEX REV", 4);
            expectedResponse = "*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // ZREVRANGEBYLEX Synonym
            response = lightClientRequest.SendCommand("ZREVRANGEBYLEX board [c - REV", 4);
            //expectedResponse = "*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Test infinites
            response = lightClientRequest.SendCommand("ZRANGE board + - BYLEX REV", 6);
            expectedResponse = "*7\r\n$1\r\ng\r\n$1\r\nf\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoZRangeByLexWithLimit()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD mycity 0 Delhi 0 London 0 Paris 0 Tokyo 0 NewYork 0 Seoul");
            response = lightClientRequest.SendCommand("ZRANGE mycity (London + BYLEX", 5);
            var expectedResponse = "*4\r\n$7\r\nNewYork\r\n$5\r\nParis\r\n$5\r\nSeoul\r\n$5\r\nTokyo\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE mycity - + BYLEX LIMIT 2 3", 4);
            expectedResponse = "*3\r\n$7\r\nNewYork\r\n$5\r\nParis\r\n$5\r\nSeoul\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // ZRANGEBYLEX Synonym
            response = lightClientRequest.SendCommand("ZRANGEBYLEX mycity - + LIMIT 2 3", 4);
            //expectedResponse = "*3\r\n$7\r\nNewYork\r\n$5\r\nParis\r\n$5\r\nSeoul\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // LIMIT x 0
            response = lightClientRequest.SendCommand("ZRANGEBYLEX mycity - + LIMIT 2 0");
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanDoZRangeByIndexReverse(int bytesSent)
        {
            //ZRANGE key min max REV
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 0 a 0 b 0 c 0 d 0 e 0 f");

            // get a range by lex order
            response = lightClientRequest.SendCommandChunks("ZRANGE board 0 -1 REV", bytesSent, 7);

            var expectedResponse = "*6\r\n$1\r\nf\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE board 0 -1 REV", 7);
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZREVRANGE cities -2 -1 WITHSCORES", bytesSent, 5);
            expectedResponse = "*4\r\n$9\r\nHyderabad\r\n$6\r\n700000\r\n$5\r\nDelhi\r\n$6\r\n100000\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanUseZUnion(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("ZADD zset1 1 uno 2 due 3 tre 4 quattro");
            lightClientRequest.SendCommand("ZADD zset2 1 uno 2 due 3 tre 4 quattro 5 cinque 6 sei");

            // Basic ZUNION
            var response = lightClientRequest.SendCommandChunks("ZUNION 2 zset1 zset2", bytesSent, 7);
            var expectedResponse = "*6\r\n$3\r\nuno\r\n$3\r\ndue\r\n$6\r\ncinque\r\n$3\r\nsei\r\n$3\r\ntre\r\n$7\r\nquattro\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // ZUNION with WITHSCORES
            response = lightClientRequest.SendCommandChunks("ZUNION 2 zset1 zset2 WITHSCORES", bytesSent, 13);
            expectedResponse = "*12\r\n$3\r\nuno\r\n$1\r\n2\r\n$3\r\ndue\r\n$1\r\n4\r\n$6\r\ncinque\r\n$1\r\n5\r\n$3\r\nsei\r\n$1\r\n6\r\n$3\r\ntre\r\n$1\r\n6\r\n$7\r\nquattro\r\n$1\r\n8";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanUseZUnionStore(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("ZADD zset1 1 uno 2 due 3 tre 4 quattro");
            lightClientRequest.SendCommand("ZADD zset2 1 uno 2 due 3 tre 4 quattro 5 cinque 6 sei");

            // Basic ZUNIONSTORE
            var response = lightClientRequest.SendCommandChunks("ZUNIONSTORE destset 2 zset1 zset2", bytesSent);
            var expectedResponse = ":6\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Verify stored result
            response = lightClientRequest.SendCommandChunks("ZRANGE destset 0 -1 WITHSCORES", bytesSent, 13);
            expectedResponse = "*12\r\n$3\r\nuno\r\n$1\r\n2\r\n$3\r\ndue\r\n$1\r\n4\r\n$6\r\ncinque\r\n$1\r\n5\r\n$3\r\nsei\r\n$1\r\n6\r\n$3\r\ntre\r\n$1\r\n6\r\n$7\r\nquattro\r\n$1\r\n8\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase(10, "MIN", 1, "*2\r\n$5\r\nboard\r\n*1\r\n*2\r\n$3\r\none\r\n$1\r\n1\r\n", Description = "Pop minimum with small chunk size")]
        [TestCase(100, "MAX", 3, "*2\r\n$5\r\nboard\r\n*3\r\n*2\r\n$4\r\nfive\r\n$1\r\n5\r\n*2\r\n$4\r\nfour\r\n$1\r\n4\r\n*2\r\n$5\r\nthree\r\n$1\r\n3\r\n", Description = "Pop maximum with large chunk size")]
        public void CanDoZMPopLC(int bytesSent, string direction, int count, string expectedResponse)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("ZADD board 1 one 2 two 3 three 4 four 5 five");

            var response = lightClientRequest.SendCommandChunks($"ZMPOP 1 board {direction} COUNT {count}", bytesSent);
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoZMPopWithMultipleKeysLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("ZADD board1 1 one 2 two");
            lightClientRequest.SendCommand("ZADD board2 3 three 4 four");

            var response = lightClientRequest.SendCommand("ZMPOP 2 board1 board2 MIN");
            var expectedResponse = "*2\r\n$6\r\nboard1\r\n*1\r\n*2\r\n$3\r\none\r\n$1\r\n1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }


        [Test]
        public void CanUseZUnionWithMultipleOptions()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("ZADD zset1 1 uno 2 due 3 tre");
            lightClientRequest.SendCommand("ZADD zset2 4 uno 5 due 6 quattro");

            // Test WEIGHTS and AGGREGATE together
            var response = lightClientRequest.SendCommand("ZUNION 2 zset1 zset2 WEIGHTS 2 3 AGGREGATE MAX WITHSCORES");
            var expectedResponse = "*8\r\n$3\r\ntre\r\n$1\r\n6\r\n$3\r\nuno\r\n$2\r\n12\r\n$3\r\ndue\r\n$2\r\n15\r\n$7\r\nquattro\r\n$2\r\n18\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZCOUNT nokey 12 232 4343 5454", bytesSent);
            expectedResponse = FormatWrongNumOfArgsError("ZCOUNT");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // no arguments
            response = lightClientRequest.SendCommandChunks("ZCOUNT nokey", bytesSent);
            expectedResponse = FormatWrongNumOfArgsError("ZCOUNT");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // not found key
            response = lightClientRequest.SendCommandChunks("ZCOUNT nokey", bytesSent);
            expectedResponse = FormatWrongNumOfArgsError("ZCOUNT");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZCOUNT nokey 1 2", bytesSent);
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommands("ZCOUNT nokey 1 2", "PING");
            expectedResponse = ":0\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZCARD newboard", bytesSent);
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //get the number of elements in the Sorted Set
            response = lightClientRequest.SendCommand("ZCARD board");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZCARD board", bytesSent);
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZINCRBY board -590 Tom");
            expectedResponse = "$3\r\n-20\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZINCRBY board -590", bytesSent);
            expectedResponse = FormatWrongNumOfArgsError("ZINCRBY");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //using key exists but non existing member
            response = lightClientRequest.SendCommandChunks("ZINCRBY board 10 Julia", bytesSent);
            expectedResponse = "$2\r\n10\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZCARD board", bytesSent);
            expectedResponse = ":4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanManageNoParametersInZIncrbyLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("ZINCRBY nokey", "PING");
            var expectedResponse = $"{FormatWrongNumOfArgsError("ZINCRBY")}+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanUseZRevRange()
        {
            //ZREVRANGE key start stop [WITHSCORES]
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 10 a 20 b 30 c 40 d 50 e 60 f");

            // get a range by lex order
            response = lightClientRequest.SendCommand("ZREVRANGE board 0 -1", 7);
            var expectedResponse = "*6\r\n$1\r\nf\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // including scores
            response = lightClientRequest.SendCommand("ZREVRANGE board 0 -1 WITHSCORES", 13);
            expectedResponse = "*12\r\n$1\r\nf\r\n$2\r\n60\r\n$1\r\ne\r\n$2\r\n50\r\n$1\r\nd\r\n$2\r\n40\r\n$1\r\nc\r\n$2\r\n30\r\n$1\r\nb\r\n$2\r\n20\r\n$1\r\na\r\n$2\r\n10\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZREVRANGE board 0 1", 3);
            expectedResponse = "*2\r\n$1\r\nf\r\n$1\r\ne\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanUseZRevRangeByScore()
        {
            //ZREVRANGESCORE key start stop [WITHSCORES] [LIMIT offset count]
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZADD board 10 a 20 b 30 c 40 d 50 e 60 f");

            // get a reverse range by score order
            response = lightClientRequest.SendCommand("ZREVRANGEBYSCORE board 70 0", 7);
            var expectedResponse = "*6\r\n$1\r\nf\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // including scores
            response = lightClientRequest.SendCommand("ZREVRANGEBYSCORE board 60 10 WITHSCORES", 13);
            expectedResponse = "*12\r\n$1\r\nf\r\n$2\r\n60\r\n$1\r\ne\r\n$2\r\n50\r\n$1\r\nd\r\n$2\r\n40\r\n$1\r\nc\r\n$2\r\n30\r\n$1\r\nb\r\n$2\r\n20\r\n$1\r\na\r\n$2\r\n10\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZREVRANGEBYSCORE board +inf 45", 3);
            expectedResponse = "*2\r\n$1\r\nf\r\n$1\r\ne\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Test LIMITs
            response = lightClientRequest.SendCommand("ZREVRANGEBYSCORE board 70 45 LIMIT 0 1", 2);
            expectedResponse = "*1\r\n$1\r\nf\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZREVRANGEBYSCORE board 70 45 LIMIT -1 1");
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Exclusions should be reversed too
            response = lightClientRequest.SendCommand("ZREVRANGEBYSCORE board +inf (40", 3);
            expectedResponse = "*2\r\n$1\r\nf\r\n$1\r\ne\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZRANK board Tom INVALIDOPTION", bytesSent);
            expectedResponse = "-ERR syntax error\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZRANK board Tom", bytesSent);
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZRANK board Tom withscore", bytesSent);
            expectedResponse = "*2\r\n:2\r\n$3\r\n560\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZREVRANK board Tom INVALIDOPTION", bytesSent);
            expectedResponse = "-ERR syntax error\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZREVRANK board Tom", bytesSent);
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZREVRANK board Kendra", bytesSent);
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZREVRANK board Dave", bytesSent);
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZREVRANK board Dave WITHSCORE", bytesSent);
            expectedResponse = "*2\r\n:2\r\n$3\r\n340\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZREMRANGEBYLEX myzset [alpha [omega", bytesSent);
            expectedResponse = ":6\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZRANGE myzset 0 -1", bytesSent, 5);
            expectedResponse = "*4\r\n$5\r\nALPHA\r\n$4\r\naaaa\r\n$3\r\nzap\r\n$3\r\nzip\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZREMRANGEBYLEX myzset [alpha [omega", bytesSent);
            expectedResponse = ":6\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZREMRANGEBYLEX myzset =a .", bytesSent);
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZRANGE myzset 0 -1", bytesSent, 5);
            expectedResponse = "*4\r\n$5\r\nALPHA\r\n$4\r\naaaa\r\n$3\r\nzap\r\n$3\r\nzip\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZREMRANGEBYRANK board a b", bytesSent);
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZREMRANGEBYRANK board 0 1", bytesSent);
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommands("ZREMRANGEBYRANK board 0 1", "PING");
            expectedResponse = ":1\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZCARD board", bytesSent);
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZREMRANGEBYSCORE board a b", bytesSent);
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_FLOAT)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZLEXCOUNT board [b [f", bytesSent);
            expectedResponse = ":5\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZLEXCOUNT board *d 8", bytesSent);
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZPOPMIN board 3", bytesSent, 5);
            expectedResponse = "*4\r\n$3\r\ntwo\r\n$1\r\n2\r\n$5\r\nthree\r\n$1\r\n3\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // ZRANDMEMBER [count [WITHSCORES]]
            response = lightClientRequest.SendCommandChunks("ZRANDMEMBER dadi 3 WITHSCORES", bytesSent, 7);
            expectedResponse = "*6\r\n"; // 3 keyvalue pairs
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZRANDMEMBER dadi 1 WITHSCORES", bytesSent, 3);
            expectedResponse = "*2\r\n"; // 2 elements
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZRANDMEMBER dadi 0 WITHSCORES", bytesSent);
            expectedResponse = "*0\r\n"; // Empty List
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            var response = lightClientRequest.SendCommandChunks("ZDIFF 2 dadi seconddadi WITHSCORES", bytesSent, 5);
            var expectedResponse = "*4\r\n$6\r\ncinque\r\n$1\r\n5\r\n$3\r\nsei\r\n$1\r\n6\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("ZDIFF 2 dadi seconddadi", bytesSent, 3);
            expectedResponse = "*2\r\n$6\r\ncinque\r\n$3\r\nsei\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase(10)]
        [TestCase(30)]
        [TestCase(100)]
        public void CanUseZDiffSTORE(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            //zdiff withscores
            var response = lightClientRequest.SendCommandChunks("ZDIFFSTORE desKey 2 dadi seconddadi", bytesSent);
            var expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            lightClientRequest.SendCommand("ZADD dadi 1 uno 2 due 3 tre 4 quattro 5 cinque 6 sei");
            lightClientRequest.SendCommand("ZADD seconddadi 1 uno 2 due 3 tre 4 quattro");

            response = lightClientRequest.SendCommandChunks("ZDIFFSTORE desKey 2 dadi seconddadi", bytesSent);
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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

            var response = lightClientRequest.SendCommandChunks("ZDIFF 3 zset1 zset2 zset3", bytesSent, 3);
            var expectedResponse = "*2\r\n$6\r\ncinque\r\n$3\r\nsei\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Zdiff withscores
            response = lightClientRequest.SendCommandChunks("ZDIFF 3 zset1 zset2 zset3 WITHSCORES", bytesSent, 5);
            expectedResponse = "*4\r\n$6\r\ncinque\r\n$1\r\n5\r\n$3\r\nsei\r\n$1\r\n6\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanUseZDiffWithNull()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("ZADD zset1 1 uno 2 due 3 tre 4 quattro 5 cinque 6 sei");
            var response = lightClientRequest.SendCommand("ZDIFF 2 zsetnotfound zset1");
            var expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZDIFF 2 zsetnotfound zset1notfound");
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanManageNotExistingKeySortedSetCommandsReadOps()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand("ZCARD nokey");
            var expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZSCORE nokey a");
            expectedResponse = "$-1\r\n"; // NULL
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANK noboard a");
            expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGE unekey 0 1");
            expectedResponse = "*0\r\n"; //empty array
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZRANGEBYSCORE nonekey 0 1");
            expectedResponse = "*0\r\n"; //empty array
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZREVRANGE nonekey 0 1");
            expectedResponse = "*0\r\n"; //empty array
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZREVRANK nonekey 0");
            expectedResponse = "$-1\r\n"; // NULL
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZREVRANGE nonekey 0 1");
            expectedResponse = "*0\r\n"; //empty array
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZLEXCOUNT nonekey [a [f");
            expectedResponse = ":0\r\n"; //integer reply
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //without the additional count argument, the command returns nil when key does not exist.
            response = lightClientRequest.SendCommand("ZRANDMEMBER nonekey");
            expectedResponse = "$-1\r\n"; //nil when key does not exist.
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //when the additional count argument is passed, the command returns an array of elements,
            //or an empty array when key does not exist.
            response = lightClientRequest.SendCommand("ZRANDMEMBER nonekey 1");
            expectedResponse = "*0\r\n"; //empty array
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZDIFF 2 i i");
            expectedResponse = "*0\r\n"; //empty array
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZDIFF 1 nonekey");
            expectedResponse = "*0\r\n"; //empty array
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanManageNotExistingKeySortedSetCommandsRMWOps()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand("ZPOPMAX nokey");
            var expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZREM nokey a");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //for this case the boundaries arguments are wrong, in redis this validation occurs
            //before the validation of a non existing key, but we are not executing the backend until
            //the key is validated first.
            response = lightClientRequest.SendCommand("ZREMRANGEBYLEX nokey 0 1");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //testing out only a nonexisting key
            response = lightClientRequest.SendCommand("ZREMRANGEBYLEX nokey [a [b");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZREMRANGEBYLEX iii [a [b");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZREM nokey a");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZREMRANGEBYRANK nokey 0 1");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZREMRANGEBYSCORE nokey 0 1");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            lightClientRequest.SendCommand("ZADD zset1 1 uno 2 due 3 tre 4 quattro 5 cinque 6 sei");

            response = lightClientRequest.SendCommand("ZREMRANGEBYLEX zset1 0 1");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_MIN_MAX_NOT_VALID_STRING)}\r\n"; ;
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            var response = lightClientRequest.SendCommand("ZPOPMAX nokey");
            var expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // When the additional count argument is passed, the command returns an array of elements,
            // Or an empty array when key does not exist.
            response = lightClientRequest.SendCommand("ZRANDMEMBER nokey 1");
            expectedResponse = "*0\r\n"; //empty array
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZCARD nokey");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }


        [Test]
        public void CanManageAddAndDelete()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommandChunks("ZADD board 340 Dave 400 Kendra 560 Tom 650 Barbara 690 Jennifer 690 Peter", 100);
            var expectedResponse = ":6\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZPOPMAX board", 3);
            expectedResponse = "*2\r\n$5\r\nPeter\r\n$3\r\n690\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZPOPMAX board 2", 5);
            expectedResponse = "*4\r\n$8\r\nJennifer\r\n$3\r\n690\r\n$7\r\nBarbara\r\n$3\r\n650\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommands("DEL board", "PING");
            expectedResponse = ":1\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Check the key is null or empty
            response = lightClientRequest.SendCommand("ZPOPMAX board");
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

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
            var key = "MySSKey";

            var response = lightClientRequest.SendCommand($"ZADD {key} 1 a 2 b 3 c");
            var expectedResponse = ":3\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"WATCH {key}");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MULTI");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"ZREM {key} a");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            await Task.Run(() => UpdateSortedSetKey(key));

            response = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // This sequence should work
            response = lightClientRequest.SendCommand("MULTI");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"ZADD {key} 7 g");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // This should commit
            response = lightClientRequest.SendCommand("EXEC", 2);
            expectedResponse = "*1\r\n:1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanUseMultiTest()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            byte[] response;

            response = lightClientRequest.SendCommand("ZADD MySSKey 1 a 2 b 3 c");
            var expectedResponse = ":3\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MULTI");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZREM MySSKey a");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZADD MySSKey 7 g");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("EXEC", 3);
            expectedResponse = "*2\r\n:1\r\n:1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanFastForwardExtraArguments()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("ZSCORE foo bar foo bar foo");
            var expectedResponse = FormatWrongNumOfArgsError("ZSCORE");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Add a large number
            response = lightClientRequest.SendCommand("ZADD zset1 -9007199254740992 uno");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("ZSCORE zset1 uno");
            expectedResponse = $"$17\r\n-9007199254740992\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoZaddWithInvalidInput()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var key = "zset1";
            var response = lightClientRequest.SendCommand($"ZADD {key} 1 uno 2 due 3 tre 4 quattro 5 cinque foo bar");
            var expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_NOT_VALID_FLOAT)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            key = "zset2";
            response = lightClientRequest.SendCommand($"ZADD {key} 1 uno 2 due 3 tre foo bar 4 quattro 5 cinque");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            key = "zset3";
            response = lightClientRequest.SendCommand($"ZADD {key} foo bar 1 uno 2 due 3 tre 4 quattro 5 cinque");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // ZREVRANGE
            response = lightClientRequest.SendCommand($"ZREVRANGE {keys[0]} 0 -1");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // ZREMRANGEBYLEX
            response = lightClientRequest.SendCommand($"ZREMRANGEBYLEX {keys[0]} {values[1][0]} {values[1][1]}");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // ZREVRANGEBYSCORE
            response = lightClientRequest.SendCommand($"ZREVRANGEBYSCORE {keys[0]} 0 -1");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        #endregion

        private static void SendCommandWithoutKey(string command, LightClientRequest lightClientRequest)
        {
            var response = lightClientRequest.SendCommand(command);
            var expectedResponse = FormatWrongNumOfArgsError(command);
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        private static void UpdateSortedSetKey(string keyName)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand($"ZADD {keyName} 4 d");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        private static string FormatWrongNumOfArgsError(string commandName) => $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, commandName)}\r\n";

        [Test]
        [TestCase(2, "ZINTER 2 zset1 zset2", Description = "Basic intersection")]
        [TestCase(3, "ZINTER 3 zset1 zset2 zset3", Description = "Three-way intersection")]
        [TestCase(2, "ZINTER 2 zset1 zset2 WITHSCORES", Description = "With scores")]
        [TestCase(3, "ZINTER 3 zset1 zset2 nx WITHSCORES", Description = "With nonexisting key")]
        public void CanDoZInter(int numKeys, string command)
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            // Setup test data
            lightClientRequest.SendCommand("ZADD zset1 1 one 2 two 3 three");
            lightClientRequest.SendCommand("ZADD zset2 1 one 2 two 4 four");
            lightClientRequest.SendCommand("ZADD zset3 1 one 3 three 5 five");

            var response = lightClientRequest.SendCommand(command);
            if (command.Contains("WITHSCORES"))
            {
                if (numKeys == 2)
                {
                    var expectedResponse = "*4\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n4\r\n";
                    TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
                }
                else if (numKeys == 3)
                {
                    var expectedResponse = "*0\r\n";
                    TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
                }
            }
            else
            {
                if (numKeys == 2)
                {
                    var expectedResponse = "*2\r\n$3\r\none\r\n$3\r\ntwo\r\n";
                    TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
                }
                else if (numKeys == 3)
                {
                    var expectedResponse = "*1\r\n$3\r\none\r\n";
                    TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
                }
            }
        }

        [Test]
        [TestCase("ZINTERCARD 2 zset1 zset2", 2, Description = "Basic intersection cardinality")]
        [TestCase("ZINTERCARD 3 zset1 zset2 zset3", 1, Description = "Three-way intersection cardinality")]
        [TestCase("ZINTERCARD 2 zset1 zset2 LIMIT 1", 1, Description = "With limit")]
        [TestCase("ZINTERCARD 2 zset1 zset2 LIMIT 0", 2, Description = "With unlimited limit")]
        public void CanDoZInterCard(string command, int expectedCount)
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            // Setup test data
            lightClientRequest.SendCommand("ZADD zset1 1 one 2 two 3 three");
            lightClientRequest.SendCommand("ZADD zset2 1 one 2 two 4 four");
            lightClientRequest.SendCommand("ZADD zset3 1 one 3 three 5 five");

            var response = lightClientRequest.SendCommand(command);
            var expectedResponse = $":{expectedCount}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase("ZINTERSTORE dest 2 zset1 zset2", 2, Description = "Basic intersection store")]
        [TestCase("ZINTERSTORE dest 2 zset1 zset2 WEIGHTS 2 3", 2, Description = "With weights")]
        [TestCase("ZINTERSTORE dest 2 zset1 zset2 AGGREGATE MAX", 2, Description = "With MAX aggregation")]
        [TestCase("ZINTERSTORE dest 2 zset1 zset2 AGGREGATE MIN", 2, Description = "With MIN aggregation")]
        [TestCase("ZINTERSTORE dest 3 zset1 zset2 nx", 0, Description = "With nonexisting key")]
        public void CanDoZInterStore(string command, int expectedCount)
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            // Setup test data
            lightClientRequest.SendCommand("ZADD zset1 1 one 2 two 3 three");
            lightClientRequest.SendCommand("ZADD zset2 1 one 2 two 4 four");

            var response = lightClientRequest.SendCommand(command);
            var expectedResponse = $":{expectedCount}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Verify stored results
            response = lightClientRequest.SendCommand("ZRANGE dest 0 -1 WITHSCORES");
            if (command.Contains("WEIGHTS"))
            {
                expectedResponse = "*4\r\n$3\r\none\r\n$1\r\n5\r\n$3\r\ntwo\r\n$2\r\n10\r\n";
            }
            else if (command.Contains("MAX"))
            {
                expectedResponse = "*4\r\n$3\r\none\r\n$1\r\n1\r\n$3\r\ntwo\r\n$1\r\n2\r\n";
            }
            else if (command.Contains("MIN"))
            {
                expectedResponse = "*4\r\n$3\r\none\r\n$1\r\n1\r\n$3\r\ntwo\r\n$1\r\n2\r\n";
            }
            else if (command.Contains("nx"))
            {
                expectedResponse = "*0\r\n";
            }
            else
            {
                expectedResponse = "*4\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n4\r\n";
            }
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void ZInterResultOrder()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            // Setup test data
            lightClientRequest.SendCommand("ZADD zset1 1 a 5 e 4 f 5 g");
            lightClientRequest.SendCommand("ZADD zset2 4 e 4 f");

            var response = lightClientRequest.SendCommand("ZINTER 2 zset2 zset1 WITHSCORES");
            // ZINTER result should obey sortedset order invariant, 
            var expectedResponse = "*4\r\n$1\r\nf\r\n$1\r\n8\r\n$1\r\ne\r\n$1\r\n9";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }
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