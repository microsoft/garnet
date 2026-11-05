// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespSortTests : TestBase
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

        static string[] Sort(IDatabase db, params object[] args) => (string[])db.Execute("SORT", args);

        static string[] SortRO(IDatabase db, params object[] args) => (string[])db.Execute("SORT_RO", args);

        [Test]
        [TestCase("SORT")]
        [TestCase("SORT_RO")]
        public void SortListNumeric(string command)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.ListRightPush("l", ["3", "1", "2", "10", "-1.5", "+inf"]);

            ClassicAssert.AreEqual(new[] { "-1.5", "1", "2", "3", "10", "+inf" }, (string[])db.Execute(command, "l"));
            ClassicAssert.AreEqual(new[] { "-1.5", "1", "2", "3", "10", "+inf" }, (string[])db.Execute(command, "l", "ASC"));
            ClassicAssert.AreEqual(new[] { "+inf", "10", "3", "2", "1", "-1.5" }, (string[])db.Execute(command, "l", "DESC"));

            // Sorting does not modify the source
            ClassicAssert.AreEqual(new[] { "3", "1", "2", "10", "-1.5", "+inf" }, db.ListRange("l").Select(v => (string)v).ToArray());
        }

        [Test]
        public void SortNumericTiesAndInvalidScores()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Equal scores fall back to comparing the elements
            db.ListRightPush("l", ["1.0", "1", "2", "01", "-0", "Infinity"]);
            ClassicAssert.AreEqual(new[] { "-0", "01", "1", "1.0", "2", "Infinity" }, Sort(db, "l"));
            ClassicAssert.AreEqual(new[] { "Infinity", "2", "1.0", "1", "01", "-0" }, Sort(db, "l", "DESC"));

            // Like Redis, the whole value must parse: no whitespace, no empty value, no NaN, no overflow
            foreach (var invalid in new[] { " 2", "2 ", "", "nan", "1e400" })
            {
                db.KeyDelete("bad");
                db.ListRightPush("bad", ["1", invalid]);
                var ex = Assert.Throws<RedisServerException>(() => Sort(db, "bad"), invalid);
                ClassicAssert.AreEqual("ERR One or more scores can't be converted into double", ex.Message);
            }
        }

        [Test]
        public void SortAlpha()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.ListRightPush("l", ["b", "a", "10", "9", "ab"]);

            ClassicAssert.AreEqual(new[] { "10", "9", "a", "ab", "b" }, Sort(db, "l", "ALPHA"));
            ClassicAssert.AreEqual(new[] { "b", "ab", "a", "9", "10" }, SortRO(db, "l", "alpha", "desc"));

            var ex = Assert.Throws<RedisServerException>(() => Sort(db, "l"));
            ClassicAssert.AreEqual("ERR One or more scores can't be converted into double", ex.Message);

            db.ListRightPush("nan", ["1", "nan"]);
            ex = Assert.Throws<RedisServerException>(() => Sort(db, "nan"));
            ClassicAssert.AreEqual("ERR One or more scores can't be converted into double", ex.Message);
        }

        [Test]
        public void SortSetAndSortedSet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SetAdd("s", ["5", "3", "4", "1", "2"]);
            ClassicAssert.AreEqual(new[] { "1", "2", "3", "4", "5" }, Sort(db, "s"));
            ClassicAssert.AreEqual(new[] { "4", "3" }, Sort(db, "s", "DESC", "LIMIT", "1", "2"));

            // Sorted sets are sorted by their members, not their scores
            db.SortedSetAdd("z", [new("1", 30), new("2", 20), new("3", 10)]);
            ClassicAssert.AreEqual(new[] { "1", "2", "3" }, Sort(db, "z"));
            ClassicAssert.AreEqual(new[] { "3", "2", "1" }, Sort(db, "z", "DESC"));

            // Without sorting, sorted set members come back in score order, reversed by DESC
            ClassicAssert.AreEqual(new[] { "3", "2", "1" }, Sort(db, "z", "BY", "nosort"));
            ClassicAssert.AreEqual(new[] { "1", "2", "3" }, Sort(db, "z", "BY", "nosort", "DESC"));
            ClassicAssert.AreEqual(new[] { "2" }, Sort(db, "z", "BY", "nosort", "LIMIT", "1", "1"));

            // Without sorting, lists keep their order, reversed by DESC before LIMIT is applied
            db.ListRightPush("l", ["3", "1", "2"]);
            ClassicAssert.AreEqual(new[] { "3", "1", "2" }, Sort(db, "l", "BY", "nosort"));
            ClassicAssert.AreEqual(new[] { "2", "1", "3" }, Sort(db, "l", "BY", "nosort", "DESC"));
            ClassicAssert.AreEqual(new[] { "2", "1" }, Sort(db, "l", "BY", "nosort", "DESC", "LIMIT", "0", "2"));
        }

        [Test]
        public void SortLimit()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.ListRightPush("l", ["5", "4", "3", "2", "1"]);

            ClassicAssert.AreEqual(new[] { "2", "3" }, Sort(db, "l", "LIMIT", "1", "2"));
            ClassicAssert.AreEqual(new[] { "1", "2" }, Sort(db, "l", "LIMIT", "-10", "2"));
            ClassicAssert.AreEqual(new[] { "4", "5" }, Sort(db, "l", "LIMIT", "3", "100"));
            ClassicAssert.AreEqual(new[] { "3", "4", "5" }, Sort(db, "l", "LIMIT", "2", "-1"));
            ClassicAssert.IsEmpty(Sort(db, "l", "LIMIT", "5", "1"));
            ClassicAssert.IsEmpty(Sort(db, "l", "LIMIT", "1", "0"));

            // The last LIMIT wins
            ClassicAssert.AreEqual(new[] { "1" }, Sort(db, "l", "LIMIT", "1", "2", "LIMIT", "0", "1"));

            // A count near long.MaxValue does not overflow the end index
            ClassicAssert.AreEqual(new[] { "3", "4", "5" }, Sort(db, "l", "LIMIT", "2", long.MaxValue.ToString()));
            ClassicAssert.AreEqual(new[] { "5" }, Sort(db, "l", "LIMIT", "4", long.MaxValue.ToString()));
            ClassicAssert.IsEmpty(Sort(db, "l", "LIMIT", long.MaxValue.ToString(), long.MaxValue.ToString()));
        }

        [Test]
        public void SortByAndGetPatterns()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.ListRightPush("ids", ["1", "2", "3", "4"]);
            db.StringSet("weight_1", "30");
            db.StringSet("weight_2", "10");
            db.StringSet("weight_3", "20");
            // weight_4 is missing and sorts as 0
            db.StringSet("name_1", "one");
            db.StringSet("name_3", "three");
            db.HashSet("user:1", [new("age", "41"), new("city", "Lisbon")]);
            db.HashSet("user:2", [new("age", "7")]);
            db.HashSet("user:3", [new("age", "19")]);
            db.HashSet("user:4", [new("age", "88")]);

            ClassicAssert.AreEqual(new[] { "4", "2", "3", "1" }, Sort(db, "ids", "BY", "weight_*"));
            ClassicAssert.AreEqual(new[] { "1", "3", "2", "4" }, Sort(db, "ids", "BY", "weight_*", "DESC"));
            ClassicAssert.AreEqual(new[] { "2", "3", "1", "4" }, Sort(db, "ids", "BY", "user:*->age"));

            // GET returns one entry per pattern for every element; missing keys and fields are nil
            ClassicAssert.AreEqual(new[] { null, "4", null, "2", "three", "3", "one", "1" },
                Sort(db, "ids", "BY", "weight_*", "GET", "name_*", "GET", "#"));
            ClassicAssert.AreEqual(new[] { "Lisbon", null }, Sort(db, "ids", "LIMIT", "0", "2", "GET", "user:*->city"));

            // A GET pattern with no '*' does not name a key, and wrong types resolve to nil
            ClassicAssert.AreEqual(new string[] { null, null }, Sort(db, "ids", "LIMIT", "0", "2", "GET", "weight_1"));
            ClassicAssert.AreEqual(new string[] { null, null }, Sort(db, "ids", "LIMIT", "0", "2", "GET", "user:*"));
            ClassicAssert.AreEqual(new string[] { null, null }, Sort(db, "ids", "LIMIT", "0", "2", "GET", "weight_*->age"));

            // A trailing "->" is part of the key name
            db.StringSet("odd_1->", "x");
            ClassicAssert.AreEqual(new[] { "x" }, Sort(db, "ids", "LIMIT", "0", "1", "GET", "odd_*->"));

            // BY with ALPHA sorts missing values first
            db.StringSet("tag_1", "b");
            db.StringSet("tag_2", "a");
            ClassicAssert.AreEqual(new[] { "3", "4", "2", "1" }, Sort(db, "ids", "BY", "tag_*", "ALPHA"));

            // BY values that are not numbers fail a numeric sort
            var ex = Assert.Throws<RedisServerException>(() => Sort(db, "ids", "BY", "tag_*"));
            ClassicAssert.AreEqual("ERR One or more scores can't be converted into double", ex.Message);

            ClassicAssert.AreEqual(new[] { "41", "7" }, SortRO(db, "ids", "BY", "nosort", "GET", "user:*->age", "LIMIT", "0", "2"));
        }

        [Test]
        public void SortGetSkipsExpiredHashFields()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.ListRightPush("ids", ["1"]);
            db.HashSet("h:1", [new("live", "a"), new("gone", "b")]);
            db.Execute("HPEXPIRE", "h:1", "1", "FIELDS", "1", "gone");
            Thread.Sleep(50);

            ClassicAssert.AreEqual(new[] { "a", null }, Sort(db, "ids", "GET", "h:*->live", "GET", "h:*->gone"));
        }

        [Test]
        public void SortStore()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SetAdd("s", ["3", "1", "2"]);
            db.StringSet("val_1", "a");
            db.StringSet("val_3", "c");

            ClassicAssert.AreEqual(3, (long)db.Execute("SORT", "s", "STORE", "dst"));
            ClassicAssert.AreEqual(new[] { "1", "2", "3" }, db.ListRange("dst").Select(v => (string)v).ToArray());

            // Replaces an existing value of any type, and stores missing GET values as empty strings
            db.StringSet("dst2", "old");
            ClassicAssert.AreEqual(6, (long)db.Execute("SORT", "s", "DESC", "GET", "#", "GET", "val_*", "STORE", "dst2"));
            ClassicAssert.AreEqual(RedisType.List, db.KeyType("dst2"));
            ClassicAssert.AreEqual(new[] { "3", "c", "2", "", "1", "a" }, db.ListRange("dst2").Select(v => (string)v).ToArray());

            // An empty result deletes the destination
            ClassicAssert.AreEqual(0, (long)db.Execute("SORT", "missing", "STORE", "dst2"));
            ClassicAssert.IsFalse(db.KeyExists("dst2"));
            ClassicAssert.AreEqual(0, (long)db.Execute("SORT", "s", "LIMIT", "10", "1", "STORE", "dst"));
            ClassicAssert.IsFalse(db.KeyExists("dst"));

            // Storing onto the source key replaces it
            db.ListRightPush("l", ["2", "3", "1"]);
            ClassicAssert.AreEqual(3, (long)db.Execute("SORT", "l", "STORE", "l"));
            ClassicAssert.AreEqual(new[] { "1", "2", "3" }, db.ListRange("l").Select(v => (string)v).ToArray());

            // A failed numeric sort stores nothing
            db.ListRightPush("bad", ["x"]);
            Assert.Throws<RedisServerException>(() => db.Execute("SORT", "bad", "STORE", "dst3"));
            ClassicAssert.IsFalse(db.KeyExists("dst3"));

            // The destination must be the argument after the first STORE token (the one the key spec locks), so a
            // repeated STORE or a STORE-named operand before it is refused instead of writing an unlocked key
            var ex = Assert.Throws<RedisServerException>(() => db.Execute("SORT", "s", "STORE", "dst4", "STORE", "dst5"));
            ClassicAssert.AreEqual("ERR SORT STORE destination must follow the first STORE argument", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => db.Execute("SORT", "s", "GET", "store", "STORE", "dst5"));
            ClassicAssert.AreEqual("ERR SORT STORE destination must follow the first STORE argument", ex.Message);
            ClassicAssert.IsFalse(db.KeyExists("dst4"));
            ClassicAssert.IsFalse(db.KeyExists("dst5"));
        }

        [Test]
        public async Task SortStoreWakesBlockedListConsumers()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            using var blockedRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var blockedDb = blockedRedis.GetDatabase(0);

            db.ListRightPush("src", ["2", "1"]);

            var blpop = blockedDb.ExecuteAsync("BLPOP", "dst", "10");
            await Task.Delay(300);
            ClassicAssert.IsFalse(blpop.IsCompleted);

            ClassicAssert.AreEqual(2, (long)db.Execute("SORT", "src", "STORE", "dst"));

            var completed = await Task.WhenAny(blpop, Task.Delay(TimeSpan.FromSeconds(5)));
            ClassicAssert.AreSame(blpop, completed, "BLPOP was not woken by SORT STORE");
            var popped = (RedisResult[])await blpop;
            ClassicAssert.AreEqual("dst", (string)popped[0]);
            ClassicAssert.AreEqual("1", (string)popped[1]);
            ClassicAssert.AreEqual(new[] { "2" }, db.ListRange("dst").Select(v => (string)v).ToArray());
        }

        [Test]
        public void SortStoreUnsortedSetIsSortedLexicographically()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SetAdd("s", ["b", "10", "a", "9"]);
            ClassicAssert.AreEqual(4, (long)db.Execute("SORT", "s", "BY", "nosort", "STORE", "dst"));
            ClassicAssert.AreEqual(new[] { "10", "9", "a", "b" }, db.ListRange("dst").Select(v => (string)v).ToArray());
        }

        [Test]
        public void SortStackExchangeClient()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.ListRightPush("l", ["c", "a", "b"]);
            db.StringSet("w_a", "3");
            db.StringSet("w_b", "1");
            db.StringSet("w_c", "2");

            var sorted = db.Sort("l", sortType: SortType.Alphabetic);
            ClassicAssert.AreEqual(new[] { "a", "b", "c" }, sorted.Select(v => (string)v).ToArray());

            sorted = db.Sort("l", by: "w_*", order: Order.Descending, skip: 0, take: 2);
            ClassicAssert.AreEqual(new[] { "a", "c" }, sorted.Select(v => (string)v).ToArray());

            ClassicAssert.AreEqual(3, db.SortAndStore("dst", "l", by: "w_*"));
            ClassicAssert.AreEqual(new[] { "b", "c", "a" }, db.ListRange("dst").Select(v => (string)v).ToArray());
        }

        [Test]
        public void SortMissingKeyAndErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            ClassicAssert.IsEmpty(Sort(db, "missing"));
            ClassicAssert.IsEmpty(SortRO(db, "missing", "BY", "w_*", "GET", "#"));

            db.StringSet("str", "1");
            db.HashSet("hash", "f", "1");
            var ex = Assert.Throws<RedisServerException>(() => Sort(db, "str"));
            ClassicAssert.AreEqual("WRONGTYPE Operation against a key holding the wrong kind of value.", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => SortRO(db, "hash"));
            ClassicAssert.AreEqual("WRONGTYPE Operation against a key holding the wrong kind of value.", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => db.Execute("SORT", "str", "STORE", "dst"));
            ClassicAssert.AreEqual("WRONGTYPE Operation against a key holding the wrong kind of value.", ex.Message);

            db.ListRightPush("l", ["1"]);
            ex = Assert.Throws<RedisServerException>(() => Sort(db, "l", "FOO"));
            ClassicAssert.AreEqual("ERR syntax error", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => SortRO(db, "l", "STORE", "dst"));
            ClassicAssert.AreEqual("ERR syntax error", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => Sort(db, "l", "LIMIT", "1"));
            ClassicAssert.AreEqual("ERR syntax error", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => Sort(db, "l", "BY"));
            ClassicAssert.AreEqual("ERR syntax error", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => Sort(db, "l", "LIMIT", "a", "1"));
            ClassicAssert.AreEqual("ERR value is not an integer or out of range.", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => db.Execute("SORT"));
            StringAssert.Contains("wrong number of arguments", ex.Message);
        }

        [Test]
        public void SortInsideTransaction()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.ListRightPush("l", ["3", "1", "2"]);
            db.StringSet("w_1", "1");

            // The source and the STORE destination are locked through the command's key specifications
            var tran = db.CreateTransaction();
            var sortTask = tran.ExecuteAsync("SORT", "l", "DESC");
            var storeTask = tran.ExecuteAsync("SORT", "l", "LIMIT", "0", "2", "STORE", "dst");
            var getTask = tran.ExecuteAsync("SORT_RO", "l", "BY", "nosort", "GET", "#");
            ClassicAssert.IsTrue(tran.Execute());
            ClassicAssert.AreEqual(new[] { "3", "2", "1" }, (string[])sortTask.Result);
            ClassicAssert.AreEqual(2, (long)storeTask.Result);
            ClassicAssert.AreEqual(new[] { "3", "1", "2" }, (string[])getTask.Result);
            ClassicAssert.AreEqual(new[] { "1", "2" }, db.ListRange("dst").Select(v => (string)v).ToArray());

            // Keys read through BY/GET patterns are only known at run time, so they cannot be locked up front
            tran = db.CreateTransaction();
            var byTask = tran.ExecuteAsync("SORT", "l", "BY", "w_*");
            ClassicAssert.IsTrue(tran.Execute());
            var ex = Assert.Throws<AggregateException>(() => _ = byTask.Result);
            ClassicAssert.AreEqual("ERR SORT BY and GET patterns that read other keys are not supported inside a transaction", ex.InnerException.Message);
        }

        [Test]
        public async Task SortPatternReadsAreAtomicUnderConcurrentWrites()
        {
            const int Elements = 16;
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            for (var i = 0; i < Elements; i++)
            {
                db.ListRightPush("ids", i.ToString());
                db.StringSet($"w_{i}", "0");
                db.StringSet($"d_{i}", "0");
            }

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            // Each writer transaction updates w_x and d_x together and churns the source list, so a SORT that reads
            // them under its locks must always see w_x == d_x.
            var writer = Task.Run(async () =>
            {
                var random = new Random(42);
                var version = 0;
                while (!cts.IsCancellationRequested)
                {
                    var id = random.Next(Elements).ToString();
                    var value = (++version).ToString();
                    var tran = db.CreateTransaction();
                    _ = tran.StringSetAsync($"w_{id}", value);
                    _ = tran.StringSetAsync($"d_{id}", value);
                    _ = tran.ListRightPushAsync("ids", id);
                    _ = tran.ListLeftPopAsync("ids");
                    _ = await tran.ExecuteAsync();
                }
            });

            var checks = 0;
            while (!cts.IsCancellationRequested)
            {
                var result = (string[])await db.ExecuteAsync("SORT", "ids", "BY", "w_*", "GET", "w_*", "GET", "d_*");
                ClassicAssert.AreEqual(Elements * 2, result.Length);

                var previous = long.MinValue;
                for (var i = 0; i < result.Length; i += 2)
                {
                    ClassicAssert.AreEqual(result[i], result[i + 1], "BY/GET keys were read from different versions");
                    var weight = long.Parse(result[i]);
                    ClassicAssert.GreaterOrEqual(weight, previous);
                    previous = weight;
                }
                checks++;
            }

            await writer;
            ClassicAssert.Greater(checks, 0);
        }

        [Test]
        public async Task SortStoreRecoversFromAof()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, lowMemory: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.SortedSetAdd("z", [new("b", 1), new("a", 2), new("c", 3)]);
                db.StringSet("gone", "x");
                ClassicAssert.AreEqual(3, (long)db.Execute("SORT", "z", "ALPHA", "STORE", "dst"));
                ClassicAssert.AreEqual(0, (long)db.Execute("SORT", "missing", "STORE", "gone"));
            }

            _ = await server.Store.CommitAOFAsync(default);
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, lowMemory: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(new[] { "a", "b", "c" }, db.ListRange("dst").Select(v => (string)v).ToArray());
                ClassicAssert.IsFalse(db.KeyExists("gone"));
            }
        }

        [Test]
        public void SortClusterModeRequiresSameSlotPatterns()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: true, lowMemory: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            ClassicAssert.AreEqual("OK", (string)db.Execute("CLUSTER", "ADDSLOTSRANGE", "0", "16383"));

            db.ListRightPush("{u}:ids", ["2", "1"]);
            db.StringSet("{u}:w_1", "2");
            db.StringSet("{u}:w_2", "1");

            // Patterns pinned to the key's slot with a hash tag work
            ClassicAssert.AreEqual(new[] { "2", "1" }, Sort(db, "{u}:ids", "BY", "{u}:w_*"));
            ClassicAssert.AreEqual(new[] { "2", "1" }, Sort(db, "{u}:ids", "BY", "{u}:w_*", "GET", "#"));
            ClassicAssert.AreEqual(new[] { "2", "1" }, Sort(db, "{u}:ids", "BY", "nosort"));

            var ex = Assert.Throws<RedisServerException>(() => Sort(db, "{u}:ids", "BY", "w_*"));
            ClassicAssert.AreEqual("ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => Sort(db, "{u}:ids", "GET", "{other}:w_*"));
            ClassicAssert.AreEqual("ERR GET option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.", ex.Message);

            // SORT substitutes literally, so a backslash does not hide a hash tag: keys from \{v}{u}:w_* hash on {v}
            ex = Assert.Throws<RedisServerException>(() => Sort(db, "{u}:ids", "GET", "\\{v}{u}:w_*"));
            ClassicAssert.AreEqual("ERR GET option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => Sort(db, "{u}:ids", "BY", "\\{v}{u}:w_*"));
            ClassicAssert.AreEqual("ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.", ex.Message);

            // The STORE destination is a key of the command, so it is subject to the cross-slot check
            ClassicAssert.AreEqual(2, (long)db.Execute("SORT", "{u}:ids", "STORE", "{u}:dst"));
            ex = Assert.Throws<RedisServerException>(() => db.Execute("SORT", "{u}:ids", "STORE", "{v}:dst"));
            StringAssert.StartsWith("CROSSSLOT", ex.Message);

            // An optional keyword key spec whose keyword is absent contributes no keys (this also covers GEORADIUS)
            db.GeoAdd("{u}:geo", 13.361389, 38.115556, "Palermo");
            var geo = (RedisResult[])db.Execute("GEORADIUS", "{u}:geo", "15", "37", "200", "km", "WITHDIST");
            ClassicAssert.AreEqual(1, geo.Length);
        }
    }
}