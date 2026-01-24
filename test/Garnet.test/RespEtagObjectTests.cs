// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespEtagObjectTests
    {
        private GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            var useReviv =
                TestContext.CurrentContext.Test.Arguments.FirstOrDefault(a => a is RevivificationMode) is
                    RevivificationMode revivMode
                    ? revivMode == RevivificationMode.UseReviv
                    : false;

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: false, useReviv: useReviv);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void SortedSetAddWithEtagTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add new sorted set with Etag
            var results = (string[])db.Execute("EXECWITHETAG", "ZADD", "key1", "1", "a", "2", "b");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(2, long.Parse(results[0]!)); // 2 elements added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1

            // Add existing field to sorted set with Etag flag
            results = (string[])db.Execute("EXECWITHETAG", "ZADD", "key1", "1", "a");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(0, long.Parse(results[0]!)); // 0 elements added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1

            // Add existing field to sorted set without Etag flag
            var result = (string)db.Execute("ZADD", "key1", "2", "b");
            ClassicAssert.AreEqual(0, long.Parse(result!)); // 0 elements added

            // Verify Etag did not advance
            result = (string)db.Execute("GETETAG", "key1");
            ClassicAssert.AreEqual(1, long.Parse(result!)); // Etag 1

            // Add non-existing field to sorted set with Etag flag
            results = (string[])db.Execute("EXECWITHETAG", "ZADD", "key1", "3", "c");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(1, long.Parse(results[0]!)); // 1 element added
            ClassicAssert.AreEqual(2, long.Parse(results[1]!)); // Etag 2

            // Add non-existing field to sorted set without Etag flag
            result = (string)db.Execute("ZADD", "key1", "4", "d");
            ClassicAssert.AreEqual(1, long.Parse(result!)); // 1 element added

            // Verify Etag advanced
            result = (string)db.Execute("GETETAG", "key1");
            ClassicAssert.AreEqual(3, long.Parse(result!)); // Etag 3

            // Add new sorted set without Etag
            result = (string)db.Execute("ZADD", "key2", "1", "a", "2", "b");
            ClassicAssert.IsNotNull(result);
            ClassicAssert.AreEqual(2, long.Parse(result!)); // 2 elements added

            // Verify Etag is 0
            result = (string)db.Execute("GETETAG", "key2");
            ClassicAssert.AreEqual(0, long.Parse(result!)); // Etag 0

            // Add existing field to sorted set with Etag flag
            results = (string[])db.Execute("EXECWITHETAG", "ZADD", "key2", "1", "a");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(0, long.Parse(results[0]!)); // 0 elements added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1

            // Add non-existing field to sorted set with Etag flag
            results = (string[])db.Execute("EXECWITHETAG", "ZADD", "key2", "3", "c");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(1, long.Parse(results[0]!)); // 1 element added
            ClassicAssert.AreEqual(2, long.Parse(results[1]!)); // Etag 2
        }

        [Test]
        public void SortedSetBlockingPopWithEtagTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key1";

            // Add new sorted set with Etag
            var results = (string[])db.Execute("EXECWITHETAG", "ZADD", key, "1", "a", "2", "b", "3", "c");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(3, long.Parse(results[0]!)); // 3 elements added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1

            // Perform a blocking pop
            var response = lightClientRequest.SendCommand($"BZMPOP 1 1 {key} MIN", 2);
            var expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n*1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Verify new sorted set cardinality
            var card = db.SortedSetLength(key);
            ClassicAssert.AreEqual(2, card);

            // Verify Etag advanced
            var result = (string)db.Execute("GETETAG", key);
            ClassicAssert.AreEqual(2, long.Parse(result!)); // Etag 2
        }

        [Test]
        public void SortedSetRMWOpsWithEtagTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "key1";

            // Add new sorted set with Etag
            var results = (string[])db.Execute("EXECWITHETAG", "ZADD", key, "1", "a", "2", "b", "3", "c", "4", "d");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(4, long.Parse(results[0]!)); // 3 elements added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1

            // ZINCRBY
            var score = db.SortedSetIncrement(key, "a", 1);
            ClassicAssert.AreEqual(2, score); // New score 2

            // Verify Etag advanced
            var result = (string)db.Execute("GETETAG", key);
            ClassicAssert.AreEqual(2, long.Parse(result!)); // Etag 2

            // ZMPOP
            var members = db.SortedSetPop(key, 1, Order.Descending);
            ClassicAssert.AreEqual(1, members.Length);

            // Verify Etag advanced
            result = (string)db.Execute("GETETAG", key);
            ClassicAssert.AreEqual(3, long.Parse(result!)); // Etag 3

            // ZREM
            var removed = db.SortedSetRemove(key, "b");
            ClassicAssert.IsTrue(removed);

            // Verify Etag advanced
            result = (string)db.Execute("GETETAG", key);
            ClassicAssert.AreEqual(4, long.Parse(result!)); // Etag 4
        }

        [Test]
        public void SortedSetAddConditionalEtagTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add new sorted set with Etag when etag matches 0
            var results = (string[])db.Execute("EXECIFMATCH", "0", "ZADD", "key1", "1", "a", "2", "b");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(2, long.Parse(results[0]!)); // 2 elements added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1

            // Add new sorted set with Etag when etag matches 1
            results = (string[])db.Execute("EXECIFMATCH", "1", "ZADD", "key2", "1", "a", "2", "b");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(2, long.Parse(results[0]!)); // 2 elements added
            ClassicAssert.AreEqual(2, long.Parse(results[1]!)); // Etag 2

            // Add non-existing field to sorted set when etag < 1
            results = (string[])db.Execute("EXECIFGREATER", "1", "ZADD", "key1", "3", "c");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.IsNull(results[0]); // Command not executed
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1

            // Add non-existing field to sorted set when etag < 2
            results = (string[])db.Execute("EXECIFGREATER", "2", "ZADD", "key1", "3", "c");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(1, long.Parse(results[0]!)); // 1 element added
            ClassicAssert.AreEqual(2, long.Parse(results[1]!)); // Etag 2

            // Add new sorted set with Etag when etag matches 2
            results = (string[])db.Execute("EXECIFMATCH", "2", "ZADD", "key1", "4", "d");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(1, long.Parse(results[0]!)); // 1 element added
            ClassicAssert.AreEqual(3, long.Parse(results[1]!)); // Etag 3
        }

        [Test]
        public void SortedSetRemoveConditionalEtagTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SortedSetAdd("key1", [new SortedSetEntry("a", 1), new SortedSetEntry("b", 2)]);

            // Remove item from sorted set without Etag when etag matches 0
            var results = (string[])db.Execute("EXECIFMATCH", "0", "ZREM", "key1", "a");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(1, long.Parse(results[0]!)); // 1 element removed
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1
        }

        [Test]  
        public void SortedSetLengthConditionalEtagTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "key1";

            db.SortedSetAdd(key, [new SortedSetEntry("a", 1), new SortedSetEntry("b", 2)]);

            // Get sorted set length with Etag when etag matches 0
            var results = (string[])db.Execute("EXECIFMATCH", 0, "ZCARD", key);
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(2, long.Parse(results[0]!)); // Length 2
            ClassicAssert.AreEqual(0, long.Parse(results[1]!)); // Etag 0

            results = (string[])db.Execute("EXECWITHETAG", "ZADD", key, "1", "a", "2", "b", "3", "c", "4", "d");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(2, long.Parse(results[0]!)); // 2 elements added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1

            // Get sorted set length with Etag when etag matches 1
            results = (string[])db.Execute("EXECIFMATCH", 1, "ZCARD", key);
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(4, long.Parse(results[0]!)); // Length 4
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1

            // Get sorted set length with Etag when etag does not match
            results = (string[])db.Execute("EXECIFMATCH", 2, "ZCARD", key);
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.IsNull(results[0]); // Command not executed
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1
        }

        [Test]
        public void Test()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key1 = "key1";
            var key2 = "key2";
            var key3 = "key3";

            var results = (string[])db.Execute("EXECWITHETAG", "ZADD", key3, 1, "a");
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(1, long.Parse(results[0]!)); // 1 element added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1

            db.SortedSetAdd(key1, [new SortedSetEntry("b", 2)]);
            db.SortedSetAdd(key2, [new SortedSetEntry("c", 3)]);

            var result = db.SortedSetCombineAndStore(SetOperation.Union, key3, key1, key2);
            ClassicAssert.AreEqual(2, result);

            // Verify Etag advanced
            var etag = (long)db.Execute("GETETAG", key3);
            ClassicAssert.AreEqual(2, etag); // Etag 2
        }
    }
}