// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using System.Text;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Tests for the OBJECT command (ENCODING / REFCOUNT / IDLETIME / FREQ / HELP).
    /// OBJECT ENCODING reports Garnet's actual internal representation of each value.
    /// </summary>
    [TestFixture]
    public class RespObjectCommandTests : TestBase
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
            server.Dispose();
            TestUtils.OnTearDown();
        }

        private static string GetEncoding(IDatabase db, string key)
            => db.Execute("OBJECT", ["ENCODING", key]).ToString();

        [Test]
        public void ObjectEncodingStringTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Garnet stores string values as raw bytes (inline or overflow) and has no native
            // integer or embedded-string representation, so all strings report "raw".
            db.StringSet("s:int", "12345");
            ClassicAssert.AreEqual("raw", GetEncoding(db, "s:int"));

            db.StringSet("s:short", "hello");
            ClassicAssert.AreEqual("raw", GetEncoding(db, "s:short"));

            db.StringSet("s:long", new string('a', 100));
            ClassicAssert.AreEqual("raw", GetEncoding(db, "s:long"));

            db.StringSet("s:empty", "");
            ClassicAssert.AreEqual("raw", GetEncoding(db, "s:empty"));
        }

        [Test]
        public void ObjectEncodingHashTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Garnet stores hashes as a hashtable regardless of size.
            db.HashSet("h:small", [new HashEntry("f", "v")]);
            ClassicAssert.AreEqual("hashtable", GetEncoding(db, "h:small"));

            var entries = Enumerable.Range(0, 600).Select(i => new HashEntry($"f{i}", $"v{i}")).ToArray();
            db.HashSet("h:many", entries);
            ClassicAssert.AreEqual("hashtable", GetEncoding(db, "h:many"));
        }

        [Test]
        public void ObjectEncodingHashWithFieldExpirationTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Field TTLs do not change Garnet's internal representation.
            db.HashSet("h:ttl", [new HashEntry("f", "v")]);
            db.Execute("HEXPIRE", ["h:ttl", "1000", "FIELDS", "1", "f"]);
            ClassicAssert.AreEqual("hashtable", GetEncoding(db, "h:ttl"));
        }

        [Test]
        public void ObjectEncodingListTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Garnet stores lists as a linked list, reported as "quicklist" regardless of size.
            db.ListRightPush("l:small", [new RedisValue("a"), new RedisValue("b")]);
            ClassicAssert.AreEqual("quicklist", GetEncoding(db, "l:small"));

            var values = Enumerable.Range(0, 300).Select(i => new RedisValue($"e{i}")).ToArray();
            db.ListRightPush("l:big", values);
            ClassicAssert.AreEqual("quicklist", GetEncoding(db, "l:big"));
        }

        [Test]
        public void ObjectEncodingSetTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Garnet stores sets as a hashtable regardless of contents (no intset/listpack).
            db.SetAdd("set:int", [new RedisValue("1"), new RedisValue("2"), new RedisValue("3")]);
            ClassicAssert.AreEqual("hashtable", GetEncoding(db, "set:int"));

            db.SetAdd("set:str", [new RedisValue("a"), new RedisValue("b"), new RedisValue("c")]);
            ClassicAssert.AreEqual("hashtable", GetEncoding(db, "set:str"));

            var members = Enumerable.Range(0, 200).Select(i => new RedisValue($"m{i}")).ToArray();
            db.SetAdd("set:many", members);
            ClassicAssert.AreEqual("hashtable", GetEncoding(db, "set:many"));
        }

        [Test]
        public void ObjectEncodingSortedSetTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Garnet stores sorted sets as a skiplist regardless of size.
            db.SortedSetAdd("z:small", [new SortedSetEntry("a", 1)]);
            ClassicAssert.AreEqual("skiplist", GetEncoding(db, "z:small"));

            var entries = Enumerable.Range(0, 200).Select(i => new SortedSetEntry($"e{i}", i)).ToArray();
            db.SortedSetAdd("z:many", entries);
            ClassicAssert.AreEqual("skiplist", GetEncoding(db, "z:many"));
        }

        [Test]
        public void ObjectEncodingMissingKeyReturnsNil()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var result = db.Execute("OBJECT", ["ENCODING", "nonexistent"]);
            ClassicAssert.IsTrue(result.IsNull);
        }

        [Test]
        public void ObjectRefcountTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("k", "v");
            ClassicAssert.AreEqual(1, (long)db.Execute("OBJECT", ["REFCOUNT", "k"]));

            var missing = db.Execute("OBJECT", ["REFCOUNT", "nonexistent"]);
            ClassicAssert.IsTrue(missing.IsNull);
        }

        [Test]
        public void ObjectIdletimeTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("k", "v");
            ClassicAssert.AreEqual(0, (long)db.Execute("OBJECT", ["IDLETIME", "k"]));

            var missing = db.Execute("OBJECT", ["IDLETIME", "nonexistent"]);
            ClassicAssert.IsTrue(missing.IsNull);
        }

        [Test]
        public void ObjectFreqTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("k", "v");

            // OBJECT FREQ is not supported (Garnet does not track access frequency), so it errors.
            var ex = Assert.Throws<RedisServerException>(() => db.Execute("OBJECT", ["FREQ", "k"]));
            StringAssert.Contains("not supported", ex.Message);

            // A missing key still returns nil.
            var missing = db.Execute("OBJECT", ["FREQ", "nonexistent"]);
            ClassicAssert.IsTrue(missing.IsNull);
        }

        [Test]
        public void ObjectHelpTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var result = (RedisResult[])db.Execute("OBJECT", ["HELP"]);
            ClassicAssert.IsNotNull(result);
            Assert.That(result.Length, Is.GreaterThan(0));
            StringAssert.Contains("OBJECT", result[0].ToString());
        }

        [Test]
        public void ObjectErrorsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Unknown subcommand.
            var ex = Assert.Throws<RedisServerException>(() => db.Execute("OBJECT", ["FOO", "k"]));
            StringAssert.Contains("unknown subcommand", ex.Message);

            // Missing key argument.
            ex = Assert.Throws<RedisServerException>(() => db.Execute("OBJECT", ["ENCODING"]));
            StringAssert.Contains("wrong number of arguments", ex.Message);

            // Bare OBJECT.
            ex = Assert.Throws<RedisServerException>(() => db.Execute("OBJECT", []));
            StringAssert.Contains("wrong number of arguments", ex.Message);
        }

        [Test]
        public void ObjectEncodingRawProtocolTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            lightClientRequest.SendCommand("SET s 12345");
            var response = lightClientRequest.SendCommand("OBJECT ENCODING s");
            var expected = "$3\r\nraw\r\n";
            ClassicAssert.AreEqual(expected, Encoding.ASCII.GetString(response, 0, expected.Length));

            // Missing key -> nil.
            response = lightClientRequest.SendCommand("OBJECT ENCODING missing");
            expected = "$-1\r\n";
            ClassicAssert.AreEqual(expected, Encoding.ASCII.GetString(response, 0, expected.Length));
        }
    }
}