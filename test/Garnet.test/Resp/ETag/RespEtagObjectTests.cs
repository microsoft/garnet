// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ETag
{
    [AllureNUnit]
    [TestFixture]
    public class RespETagObjectTests : AllureTestBase
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
        public void SortedSetAddWithETagTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add new sorted set with ETag
            var results = (string[])db.ExecWithETag("ZADD", "key1", "1", "a", "2", "b");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(2, long.Parse(results[0]!)); // 2 elements added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // ETag 1

            // Add existing field to sorted set with ETag flag
            results = (string[])db.ExecWithETag("ZADD", "key1", "1", "a");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(0, long.Parse(results[0]!)); // 0 elements added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // ETag 1

            // Add existing field to sorted set without ETag flag
            var result = (string)db.Execute("ZADD", "key1", "2", "b");
            ClassicAssert.AreEqual(0, long.Parse(result!)); // 0 elements added

            // Verify ETag did not advance
            result = (string)db.Execute("GETETAG", "key1");
            ClassicAssert.AreEqual(1, long.Parse(result!)); // ETag 1

            // Add non-existing field to sorted set with ETag flag
            results = (string[])db.ExecWithETag("ZADD", "key1", "3", "c");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(1, long.Parse(results[0]!)); // 1 element added
            ClassicAssert.AreEqual(2, long.Parse(results[1]!)); // ETag 2

            // Add non-existing field to sorted set without ETag flag
            result = (string)db.Execute("ZADD", "key1", "4", "d");
            ClassicAssert.AreEqual(1, long.Parse(result!)); // 1 element added

            // Verify ETag advanced
            result = (string)db.Execute("GETETAG", "key1");
            ClassicAssert.AreEqual(3, long.Parse(result!)); // ETag 3

            // Add new sorted set without ETag
            result = (string)db.Execute("ZADD", "key2", "1", "a", "2", "b");
            ClassicAssert.IsNotNull(result);
            ClassicAssert.AreEqual(2, long.Parse(result!)); // 2 elements added

            // Verify ETag is 0
            result = (string)db.Execute("GETETAG", "key2");
            ClassicAssert.AreEqual(0, long.Parse(result!)); // ETag 0

            // Add existing field to sorted set with ETag flag
            results = (string[])db.ExecWithETag("ZADD", "key2", "1", "a");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(0, long.Parse(results[0]!)); // 0 elements added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // ETag 1

            // Add non-existing field to sorted set with ETag flag
            results = (string[])db.ExecWithETag("ZADD", "key2", "3", "c");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(1, long.Parse(results[0]!)); // 1 element added
            ClassicAssert.AreEqual(2, long.Parse(results[1]!)); // ETag 2
        }

        [Test]
        public void SortedSetBlockingPopWithETagTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key1";

            // Add new sorted set with ETag
            var results = (string[])db.ExecWithETag("ZADD", key, "1", "a", "2", "b", "3", "c");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(3, long.Parse(results[0]!)); // 3 elements added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // ETag 1

            // Perform a blocking pop
            var response = lightClientRequest.SendCommand($"BZMPOP 1 1 {key} MIN", 2);
            var expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n*1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Verify new sorted set cardinality
            var card = db.SortedSetLength(key);
            ClassicAssert.AreEqual(2, card);

            // Verify ETag advanced
            var result = (string)db.Execute("GETETAG", key);
            ClassicAssert.AreEqual(2, long.Parse(result!)); // ETag 2
        }

        [Test]
        public void SortedSetRMWOpsWithETagTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "key1";

            // Add new sorted set with ETag
            var results = (string[])db.ExecWithETag("ZADD", key, "1", "a", "2", "b", "3", "c", "4", "d");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(4, long.Parse(results[0]!)); // 3 elements added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // ETag 1

            // ZINCRBY
            var score = db.SortedSetIncrement(key, "a", 1);
            ClassicAssert.AreEqual(2, score); // New score 2

            // Verify ETag advanced
            var result = (string)db.Execute("GETETAG", key);
            ClassicAssert.AreEqual(2, long.Parse(result!)); // ETag 2

            // ZMPOP
            var members = db.SortedSetPop(key, 1, Order.Descending);
            ClassicAssert.AreEqual(1, members.Length);

            // Verify ETag advanced
            result = (string)db.Execute("GETETAG", key);
            ClassicAssert.AreEqual(3, long.Parse(result!)); // ETag 3

            // ZREM
            var removed = db.SortedSetRemove(key, "b");
            ClassicAssert.IsTrue(removed);

            // Verify ETag advanced
            result = (string)db.Execute("GETETAG", key);
            ClassicAssert.AreEqual(4, long.Parse(result!)); // ETag 4
        }

        [Test]
        public void SortedSetAddConditionalETagTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add new sorted set with ETag when etag matches 0
            var results = (string[])db.ExecIfMatch(0, "ZADD", "key1", "1", "a", "2", "b");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(2, long.Parse(results[0]!)); // 2 elements added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // ETag 1

            // Add new sorted set with ETag when etag matches 1
            results = (string[])db.ExecIfMatch(1, "ZADD", "key2", "1", "a", "2", "b");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(2, long.Parse(results[0]!)); // 2 elements added
            ClassicAssert.AreEqual(2, long.Parse(results[1]!)); // ETag 2

            // Add non-existing field to sorted set when etag < 1
            results = (string[])db.ExecIfGreater(1, "ZADD", "key1", "3", "c");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.IsNull(results[0]); // Command not executed
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // ETag 1

            // Add non-existing field to sorted set when etag < 2
            results = (string[])db.ExecIfGreater(2, "ZADD", "key1", "3", "c");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(1, long.Parse(results[0]!)); // 1 element added
            ClassicAssert.AreEqual(2, long.Parse(results[1]!)); // ETag 2

            // Add new sorted set with ETag when etag matches 2
            results = (string[])db.ExecIfMatch(2, "ZADD", "key1", "4", "d");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(1, long.Parse(results[0]!)); // 1 element added
            ClassicAssert.AreEqual(3, long.Parse(results[1]!)); // ETag 3
        }

        [Test]
        public void SortedSetRemoveConditionalETagTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SortedSetAdd("key1", [new SortedSetEntry("a", 1), new SortedSetEntry("b", 2)]);

            // Remove item from sorted set without ETag when etag matches 0
            var results = (string[])db.ExecIfMatch(0, "ZREM", "key1", "a");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(1, long.Parse(results[0]!)); // 1 element removed
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // ETag 1
        }

        [Test]
        public void SortedSetLengthConditionalETagTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "key1";

            db.SortedSetAdd(key, [new SortedSetEntry("a", 1), new SortedSetEntry("b", 2)]);

            // Get sorted set length with ETag when etag matches 0
            var results = (string[])db.ExecIfMatch(0, "ZCARD", key);
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(2, long.Parse(results[0]!)); // Length 2
            ClassicAssert.AreEqual(0, long.Parse(results[1]!)); // ETag 0

            results = (string[])db.ExecWithETag("ZADD", key, "1", "a", "2", "b", "3", "c", "4", "d");
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(2, long.Parse(results[0]!)); // 2 elements added
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // ETag 1

            // Get sorted set length with ETag when etag matches 1
            results = (string[])db.ExecIfMatch(1, "ZCARD", key);
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(4, long.Parse(results[0]!)); // Length 4
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // ETag 1

            // Get sorted set length with ETag when etag does not match
            results = (string[])db.ExecIfMatch(2, "ZCARD", key);
            ClassicAssert.IsNotNull(results);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.IsNull(results[0]); // Command not executed
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // ETag 1
        }
    }
}