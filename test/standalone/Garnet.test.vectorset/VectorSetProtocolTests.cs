// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class VectorSetProtocolTests : TestBase
    {
        private GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableVectorSetPreview: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void VADD(RedisProtocol protocol)
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            var added = Add(db, "vectors", "first");
            AssertType(added, protocol, ResultType.Integer, ResultType.Boolean);
            ClassicAssert.AreEqual(1, (int)added);
            ClassicAssert.AreEqual(1, (int)db.Execute("VCARD", "vectors"));
            AssertConnectionAlive(db);
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void VCARD(RedisProtocol protocol)
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            AssertInteger(db.Execute("VCARD", "missing"), protocol, 0);
            Add(db, "vectors", "first");
            AssertInteger(db.Execute("VCARD", "vectors"), protocol, 1);
            AssertConnectionAlive(db);
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void VDIM(RedisProtocol protocol)
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            Add(db, "vectors", "first");
            AssertInteger(db.Execute("VDIM", "vectors"), protocol, 3);
            var error = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VDIM", "missing"));
            StringAssert.Contains("Key not found", error.Message);
            AssertConnectionAlive(db);
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void VEMB(RedisProtocol protocol)
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            Add(db, "vectors", "first");
            var embedding = db.Execute("VEMB", ["vectors", "first"]);
            AssertType(embedding, protocol, ResultType.Array, ResultType.Array);
            var coordinates = (RedisResult[])embedding;
            ClassicAssert.AreEqual(3, coordinates.Length);
            AssertType(coordinates[0], protocol, ResultType.BulkString, ResultType.Double);
            ClassicAssert.AreEqual(1, (double)coordinates[0]);

            var raw = (RedisResult[])db.Execute("VEMB", ["vectors", "first", "RAW"]);
            ClassicAssert.AreEqual(3, raw.Length);
            ClassicAssert.AreEqual("fp32", (string)raw[0]);
            AssertType(raw[1], protocol, ResultType.BulkString, ResultType.BulkString);
            AssertType(raw[2], protocol, ResultType.BulkString, ResultType.Double);
            ClassicAssert.IsTrue(double.IsFinite((double)raw[2]));

            var missing = db.Execute("VEMB", ["vectors", "missing"]);
            AssertType(missing, protocol, ResultType.Array, ResultType.Array);
            ClassicAssert.AreEqual(0, missing.Length);
            AssertConnectionAlive(db);
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void VEMBRawQ8(RedisProtocol protocol)
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            db.Execute("VADD", ["vectors", "VALUES", "3", "1", "0", "0", "first", "Q8"]);
            var raw = (RedisResult[])db.Execute("VEMB", ["vectors", "first", "RAW"]);
            ClassicAssert.AreEqual(4, raw.Length);
            ClassicAssert.AreEqual("q8", (string)raw[0]);
            AssertType(raw[2], protocol, ResultType.BulkString, ResultType.Double);
            AssertType(raw[3], protocol, ResultType.BulkString, ResultType.Double);
            AssertConnectionAlive(db);
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void VGETATTR(RedisProtocol protocol)
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            Add(db, "vectors", "first", "{\"id\":1}");
            var attributes = db.Execute("VGETATTR", ["vectors", "first"]);
            AssertType(attributes, protocol, ResultType.BulkString, ResultType.BulkString);
            ClassicAssert.AreEqual("{\"id\":1}", (string)attributes);

            var missing = db.Execute("VGETATTR", ["vectors", "missing"]);
            ClassicAssert.IsTrue(missing.IsNull);
            AssertType(missing, protocol, ResultType.BulkString, ResultType.Null);
            var missingKey = db.Execute("VGETATTR", ["missing", "first"]);
            ClassicAssert.IsTrue(missingKey.IsNull);
            AssertType(missingKey, protocol, ResultType.BulkString, ResultType.Null);
            AssertConnectionAlive(db);
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void VINFO(RedisProtocol protocol)
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            Add(db, "vectors", "first");
            var info = db.Execute("VINFO", "vectors");
            AssertType(info, protocol, ResultType.Array, ResultType.Array);
            var fields = (RedisResult[])info;
            ClassicAssert.AreEqual(14, fields.Length);
            ClassicAssert.AreEqual("size", (string)fields[12]);
            ClassicAssert.AreEqual("1", (string)fields[13]);

            var missing = db.Execute("VINFO", "missing");
            ClassicAssert.IsTrue(missing.IsNull);
            AssertType(missing, protocol, ResultType.Array, ResultType.Null);
            AssertConnectionAlive(db);
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void VISMEMBER(RedisProtocol protocol)
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            Add(db, "vectors", "first");
            AssertBoolean(db.Execute("VISMEMBER", ["vectors", "first"]), protocol, true);
            AssertBoolean(db.Execute("VISMEMBER", ["vectors", "missing"]), protocol, false);
            AssertBoolean(db.Execute("VISMEMBER", ["missing", "first"]), protocol, false);
            AssertConnectionAlive(db);
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void VLINKS(RedisProtocol protocol)
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            for (var i = 0; i < 32; i++)
                Add(db, "vectors", $"element-{i}");

            var links = (RedisResult[])db.Execute("VLINKS", ["vectors", "element-0"]);
            ClassicAssert.Greater(links.Length, 0);
            AssertType(links[0], protocol, ResultType.Array, ResultType.Array);
            ClassicAssert.AreEqual(1, links[0].Length);

            var scored = (RedisResult[])db.Execute("VLINKS", ["vectors", "element-0", "WITHSCORES"]);
            ClassicAssert.Greater(scored.Length, 0);
            AssertType(scored[0], protocol, ResultType.Array, ResultType.Map);
            var pair = (RedisResult[])scored[0];
            ClassicAssert.AreEqual(2, pair.Length);
            AssertType(pair[1], protocol, ResultType.BulkString, ResultType.Double);

            var missing = db.Execute("VLINKS", ["vectors", "missing"]);
            ClassicAssert.IsTrue(missing.IsNull);
            AssertType(missing, protocol, ResultType.BulkString, ResultType.Null);
            AssertConnectionAlive(db);
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void VRANDMEMBER(RedisProtocol protocol)
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            var missing = db.Execute("VRANDMEMBER", "missing");
            ClassicAssert.IsTrue(missing.IsNull);
            AssertType(missing, protocol, ResultType.BulkString, ResultType.Null);
            var empty = db.Execute("VRANDMEMBER", ["missing", "2"]);
            AssertType(empty, protocol, ResultType.Array, ResultType.Array);
            ClassicAssert.AreEqual(0, empty.Length);

            Add(db, "vectors", "first");
            Add(db, "vectors", "second");
            var member = db.Execute("VRANDMEMBER", "vectors");
            AssertType(member, protocol, ResultType.BulkString, ResultType.BulkString);
            ClassicAssert.IsTrue((string)member is "first" or "second");
            var members = (RedisResult[])db.Execute("VRANDMEMBER", ["vectors", "2"]);
            ClassicAssert.AreEqual(2, members.Length);
            foreach (var result in members)
                AssertType(result, protocol, ResultType.BulkString, ResultType.BulkString);

            AssertConnectionAlive(db);
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void VREM(RedisProtocol protocol)
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            Add(db, "vectors", "first");
            AssertBoolean(db.Execute("VREM", ["vectors", "first"]), protocol, true);
            AssertBoolean(db.Execute("VREM", ["vectors", "first"]), protocol, false);
            AssertBoolean(db.Execute("VREM", ["missing", "first"]), protocol, false);
            AssertConnectionAlive(db);
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void VSETATTR(RedisProtocol protocol)
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            Add(db, "vectors", "first");
            AssertBoolean(db.Execute("VSETATTR", ["vectors", "first", "{\"id\":1}"]), protocol, true);
            AssertBoolean(db.Execute("VSETATTR", ["vectors", "missing", "{\"id\":1}"]), protocol, false);
            AssertBoolean(db.Execute("VSETATTR", ["missing", "first", "{\"id\":1}"]), protocol, false);
            ClassicAssert.AreEqual("{\"id\":1}", (string)db.Execute("VGETATTR", ["vectors", "first"]));
            AssertConnectionAlive(db);
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void VSIM(RedisProtocol protocol)
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            Add(db, "vectors", "first", "{\"id\":1}");
            Add(db, "vectors", "second", "{\"id\":2}");
            object[] search = ["vectors", "VALUES", "3", "1", "0", "0", "COUNT", "2", "EF", "40"];

            var ids = (RedisResult[])db.Execute("VSIM", search);
            ClassicAssert.AreEqual(2, ids.Length);
            foreach (var result in ids)
                AssertType(result, protocol, ResultType.BulkString, ResultType.BulkString);

            var scored = db.Execute("VSIM", [.. search, "WITHSCORES"]);
            AssertType(scored, protocol, ResultType.Array, ResultType.Map);
            if (protocol == RedisProtocol.Resp2)
            {
                var values = (RedisResult[])scored;
                ClassicAssert.AreEqual(4, values.Length);
                AssertType(values[1], protocol, ResultType.BulkString, ResultType.Double);
            }
            else
            {
                var values = scored.ToDictionary();
                ClassicAssert.AreEqual(2, values.Count);
                foreach (var result in values.Values)
                    ClassicAssert.AreEqual(ResultType.Double, result.Resp3Type);
            }

            var withAttributes = db.Execute("VSIM", [.. search, "WITHATTRIBS", "WITHSCORES"]);
            AssertType(withAttributes, protocol, ResultType.Array, ResultType.Map);
            if (protocol == RedisProtocol.Resp3)
            {
                var values = withAttributes.ToDictionary();
                ClassicAssert.AreEqual(2, values.Count);
                foreach (var result in values.Values)
                {
                    ClassicAssert.AreEqual(ResultType.Array, result.Resp3Type);
                    ClassicAssert.AreEqual(2, result.Length);
                    ClassicAssert.AreEqual(ResultType.Double, result[0].Resp3Type);
                    ClassicAssert.AreEqual(ResultType.BulkString, result[1].Resp3Type);
                }
            }
            else
            {
                var values = (RedisResult[])withAttributes;
                ClassicAssert.AreEqual(6, values.Length);
                AssertType(values[1], protocol, ResultType.BulkString, ResultType.Double);
                AssertType(values[2], protocol, ResultType.BulkString, ResultType.BulkString);
            }

            var empty = db.Execute("VSIM", ["missing", "VALUES", "3", "1", "0", "0", "COUNT", "2"]);
            AssertType(empty, protocol, ResultType.Array, ResultType.Array);
            ClassicAssert.AreEqual(0, empty.Length);
            AssertConnectionAlive(db);
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void WrongType(RedisProtocol protocol)
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();
            ClassicAssert.IsTrue(db.StringSet("string", "not-vectors"));

            (string Command, object[] Arguments)[] commands =
            [
                ("VADD", ["string", "VALUES", "3", "1", "0", "0", "first", "NOQUANT"]),
                ("VCARD", ["string"]),
                ("VDIM", ["string"]),
                ("VEMB", ["string", "first"]),
                ("VGETATTR", ["string", "first"]),
                ("VINFO", ["string"]),
                ("VISMEMBER", ["string", "first"]),
                ("VLINKS", ["string", "first"]),
                ("VRANDMEMBER", ["string"]),
                ("VREM", ["string", "first"]),
                ("VSETATTR", ["string", "first", "{}"]),
                ("VSIM", ["string", "VALUES", "3", "1", "0", "0", "COUNT", "2"]),
            ];

            foreach (var (command, arguments) in commands)
            {
                var error = ClassicAssert.Throws<RedisServerException>(() => db.Execute(command, arguments), command);
                StringAssert.StartsWith("WRONGTYPE", error.Message, command);
                AssertConnectionAlive(db);
            }
        }

        private static ConnectionMultiplexer Connect(RedisProtocol protocol) => ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));

        private static RedisResult Add(IDatabase db, string key, string element, string attributes = null)
        {
            return attributes is null
                ? db.Execute("VADD", [key, "VALUES", "3", "1", "0", "0", element, "NOQUANT"])
                : db.Execute("VADD", [key, "VALUES", "3", "1", "0", "0", element, "NOQUANT", "SETATTR", attributes]);
        }

        private static void AssertType(RedisResult result, RedisProtocol protocol, ResultType resp2, ResultType resp3)
            => ClassicAssert.AreEqual(protocol == RedisProtocol.Resp2 ? resp2 : resp3, protocol == RedisProtocol.Resp2 ? result.Resp2Type : result.Resp3Type);

        private static void AssertInteger(RedisResult result, RedisProtocol protocol, int value)
        {
            AssertType(result, protocol, ResultType.Integer, ResultType.Integer);
            ClassicAssert.AreEqual(value, (int)result);
        }

        private static void AssertBoolean(RedisResult result, RedisProtocol protocol, bool value)
        {
            AssertType(result, protocol, ResultType.Integer, ResultType.Boolean);
            ClassicAssert.AreEqual(value ? 1 : 0, (int)result);
        }

        private static void AssertConnectionAlive(IDatabase db) => ClassicAssert.AreEqual("PONG", (string)db.Execute("PING"));
    }
}
