// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture(RedisProtocol.Resp2)]
    [TestFixture(RedisProtocol.Resp3)]
    public class VectorSetProtocolTests : TestBase
    {
        private enum SearchFields { Ids, Scores, Attributes, ScoresAndAttributes }

        private readonly RedisProtocol protocol;
        private GarnetServer server;

        public VectorSetProtocolTests(RedisProtocol protocol) => this.protocol = protocol;

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

        [Test]
        public void VADD()
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            var added = Add(db, "vectors", "first");
            AssertType(added, protocol, ResultType.Integer, ResultType.Boolean);
            ClassicAssert.AreEqual(1, (int)added);
            ClassicAssert.AreEqual(1, (int)db.Execute("VCARD", "vectors"));
            AssertConnectionAlive(db);
        }

        [Test]
        public void VCARD()
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            AssertInteger(db.Execute("VCARD", "missing"), protocol, 0);
            Add(db, "vectors", "first");
            AssertInteger(db.Execute("VCARD", "vectors"), protocol, 1);
            AssertConnectionAlive(db);
        }

        [Test]
        public void VDIM()
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            Add(db, "vectors", "first");
            AssertInteger(db.Execute("VDIM", "vectors"), protocol, 3);
            var error = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VDIM", "missing"));
            StringAssert.Contains("Key not found", error.Message);
            AssertConnectionAlive(db);
        }

        [Test]
        public void VEMB()
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
            var missingRaw = db.Execute("VEMB", ["missing", "first", "RAW"]);
            AssertType(missingRaw, protocol, ResultType.Array, ResultType.Array);
            ClassicAssert.AreEqual(0, missingRaw.Length);
            AssertConnectionAlive(db);
        }

        [Test]
        public void VEMBRawQ8()
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

        [Test]
        public void VGETATTR()
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

            Add(db, "vectors", "without-attributes");
            var missingAttribute = db.Execute("VGETATTR", ["vectors", "without-attributes"]);
            ClassicAssert.IsTrue(missingAttribute.IsNull);
            AssertType(missingAttribute, protocol, ResultType.BulkString, ResultType.Null);
            AssertConnectionAlive(db);
        }

        [Test]
        public void VINFO()
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            db.Execute("VADD", ["vectors", "REDUCE", "2", "VALUES", "3", "1", "0", "0", "first", "NOQUANT", "EF", "41", "M", "7"]);
            var info = db.Execute("VINFO", "vectors");
            AssertVInfoIntegerFields(info, protocol, ("input-vector-dimensions", 3), ("reduced-dimensions", 2), ("build-exploration-factor", 41), ("num-links", 7), ("size", 1));

            var missing = db.Execute("VINFO", "missing");
            ClassicAssert.IsTrue(missing.IsNull);
            AssertType(missing, protocol, ResultType.Array, ResultType.Null);
            AssertConnectionAlive(db);
        }

        [Test]
        public void VISMEMBER()
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            Add(db, "vectors", "first");
            AssertBoolean(db.Execute("VISMEMBER", ["vectors", "first"]), protocol, true);
            AssertBoolean(db.Execute("VISMEMBER", ["vectors", "missing"]), protocol, false);
            AssertBoolean(db.Execute("VISMEMBER", ["missing", "first"]), protocol, false);
            AssertConnectionAlive(db);
        }

        [Test]
        public void VLINKS()
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            var missing = db.Execute("VLINKS", ["missing", "element-0"]);
            ClassicAssert.IsTrue(missing.IsNull);
            AssertType(missing, protocol, ResultType.BulkString, ResultType.Null);

            db.Execute("VADD", ["vectors", "VALUES", "3", "1", "0", "0", "element-0", "NOQUANT"]);
            AssertEmptyArray(db.Execute("VLINKS", ["vectors", "element-0"]), protocol);
            AssertEmptyArray(db.Execute("VLINKS", ["vectors", "element-0", "WITHSCORES"]), protocol);

            for (var i = 1; i < 32; i++)
                db.Execute("VADD", ["vectors", "VALUES", "3", "1", i, "0", $"element-{i}", "NOQUANT"]);

            var links = (RedisResult[])db.Execute("VLINKS", ["vectors", "element-0"]);
            var scored = (RedisResult[])db.Execute("VLINKS", ["vectors", "element-0", "WITHSCORES"]);
            ClassicAssert.Greater(links.Length, 0);
            ClassicAssert.AreEqual(links.Length, scored.Length);

            var neighbors = new HashSet<string>();
            foreach (var link in links)
            {
                AssertType(link, protocol, ResultType.Array, ResultType.Array);
                var item = (RedisResult[])link;
                ClassicAssert.AreEqual(1, item.Length);
                AssertType(item[0], protocol, ResultType.BulkString, ResultType.BulkString);
                ClassicAssert.IsTrue(neighbors.Add((string)item[0]));
            }

            foreach (var link in scored)
            {
                AssertType(link, protocol, ResultType.Array, ResultType.Map);
                var pair = (RedisResult[])link;
                ClassicAssert.AreEqual(2, pair.Length);
                AssertType(pair[0], protocol, ResultType.BulkString, ResultType.BulkString);
                AssertType(pair[1], protocol, ResultType.BulkString, ResultType.Double);
                ClassicAssert.IsTrue(neighbors.Remove((string)pair[0]));
                ClassicAssert.IsTrue(double.IsFinite((double)pair[1]));
            }

            ClassicAssert.IsEmpty(neighbors);
            AssertConnectionAlive(db);
        }

        [Test]
        public void VRANDMEMBER()
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

        [Test]
        public void VREM()
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            Add(db, "vectors", "first");
            AssertBoolean(db.Execute("VREM", ["vectors", "first"]), protocol, true);
            AssertBoolean(db.Execute("VREM", ["vectors", "first"]), protocol, false);
            AssertBoolean(db.Execute("VREM", ["missing", "first"]), protocol, false);
            AssertConnectionAlive(db);
        }

        [Test]
        public void VSETATTR()
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

        [Test]
        public void VSIM()
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            Add(db, "vectors", "first", "{\"id\":1}");
            Add(db, "vectors", "second", "{\"id\":2}");
            object[] search = ["vectors", "VALUES", "3", "1", "0", "0", "COUNT", "2", "EF", "40"];
            (string Id, string Attribute)[] expected = [("first", "{\"id\":1}"), ("second", "{\"id\":2}")];
            AssertSearchReply(db.Execute("VSIM", search), protocol, SearchFields.Ids, expected);
            AssertSearchReply(db.Execute("VSIM", [.. search, "WITHSCORES"]), protocol, SearchFields.Scores, expected);
            AssertSearchReply(db.Execute("VSIM", [.. search, "WITHATTRIBS", "WITHSCORES"]), protocol, SearchFields.ScoresAndAttributes, expected);

            object[] missing = ["missing", "VALUES", "3", "1", "0", "0", "COUNT", "2"];
            object[][] options = [[], ["WITHSCORES"], ["WITHATTRIBS"], ["WITHSCORES", "WITHATTRIBS"]];
            foreach (var flags in options)
                AssertEmptyArray(db.Execute("VSIM", [.. missing, .. flags]), protocol);

            AssertConnectionAlive(db);
        }

        [Test]
        public void VSIMFilteredComplexReplies()
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            Add(db, "vectors", "first", "{\"id\":1}");
            Add(db, "vectors", "second", "{\"id\":2}");
            Add(db, "vectors", "third", "{\"id\":3}");
            object[] search = ["vectors", "ELE", "first", "COUNT", "3", "EF", "40", "FILTER", ".id == 2"];
            var expected = ("second", "{\"id\":2}");
            AssertSearchReply(db.Execute("VSIM", search), protocol, SearchFields.Ids, expected);
            AssertSearchReply(db.Execute("VSIM", [.. search, "WITHSCORES"]), protocol, SearchFields.Scores, expected);
            AssertSearchReply(db.Execute("VSIM", [.. search, "WITHATTRIBS"]), protocol, SearchFields.Attributes, expected);
            AssertSearchReply(db.Execute("VSIM", [.. search, "WITHSCORES", "WITHATTRIBS"]), protocol, SearchFields.ScoresAndAttributes, expected);
            AssertSearchReply(db.Execute("VSIM", ["vectors", "ELE", "first", "COUNT", "3", "FILTER", ".id == 99", "WITHSCORES", "WITHATTRIBS"]), protocol, SearchFields.ScoresAndAttributes);
            AssertConnectionAlive(db);
        }

        [Test]
        public void VSIMMissingAttribute()
        {
            using var redis = Connect(protocol);
            var db = redis.GetDatabase();

            Add(db, "vectors", "first", "{\"id\":1}");
            Add(db, "vectors", "second");
            var result = db.Execute("VSIM", ["vectors", "ELE", "first", "COUNT", "2", "WITHATTRIBS", "WITHSCORES"]);
            AssertSearchReply(result, protocol, SearchFields.ScoresAndAttributes, ("first", "{\"id\":1}"), ("second", null));
            AssertConnectionAlive(db);
        }

        [Test]
        public void WrongType()
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

        private static void AssertSearchReply(RedisResult result, RedisProtocol protocol, SearchFields fields, params (string Id, string Attribute)[] expected)
        {
            var withScores = fields is SearchFields.Scores or SearchFields.ScoresAndAttributes;
            var withAttributes = fields is SearchFields.Attributes or SearchFields.ScoresAndAttributes;
            var remaining = new Dictionary<string, string>(expected.Length);
            foreach (var (id, attribute) in expected)
                remaining.Add(id, attribute);

            if (protocol == RedisProtocol.Resp3 && fields != SearchFields.Ids)
            {
                AssertType(result, protocol, ResultType.Array, ResultType.Map);
                var entries = result.ToDictionary();
                ClassicAssert.AreEqual(expected.Length, entries.Count);
                foreach (var entry in entries)
                {
                    var id = (string)entry.Key;
                    ClassicAssert.IsTrue(remaining.Remove(id, out var attribute), $"Unexpected or duplicate result: {id}");
                    if (withScores && withAttributes)
                    {
                        AssertType(entry.Value, protocol, ResultType.Array, ResultType.Array);
                        var pair = (RedisResult[])entry.Value;
                        ClassicAssert.AreEqual(2, pair.Length);
                        AssertScore(pair[0], protocol);
                        AssertAttribute(pair[1], protocol, attribute);
                    }
                    else if (withScores)
                    {
                        AssertScore(entry.Value, protocol);
                    }
                    else
                    {
                        AssertAttribute(entry.Value, protocol, attribute);
                    }
                }
            }
            else
            {
                AssertType(result, protocol, ResultType.Array, ResultType.Array);
                var values = (RedisResult[])result;
                var width = 1 + (withScores ? 1 : 0) + (withAttributes ? 1 : 0);
                ClassicAssert.AreEqual(expected.Length * width, values.Length);
                for (var i = 0; i < values.Length; i += width)
                {
                    AssertType(values[i], protocol, ResultType.BulkString, ResultType.BulkString);
                    var id = (string)values[i];
                    ClassicAssert.IsTrue(remaining.Remove(id, out var attribute), $"Unexpected or duplicate result: {id}");
                    if (withScores)
                        AssertScore(values[i + 1], protocol);
                    if (withAttributes)
                        AssertAttribute(values[i + width - 1], protocol, attribute);
                }
            }

            ClassicAssert.AreEqual(0, remaining.Count);
        }

        private static void AssertScore(RedisResult result, RedisProtocol protocol)
        {
            AssertType(result, protocol, ResultType.BulkString, ResultType.Double);
            ClassicAssert.IsTrue(double.IsFinite((double)result));
        }

        private static void AssertAttribute(RedisResult result, RedisProtocol protocol, string expected)
        {
            if (expected is null)
            {
                AssertType(result, protocol, ResultType.BulkString, ResultType.Null);
                ClassicAssert.IsTrue(result.IsNull);
                return;
            }

            AssertType(result, protocol, ResultType.BulkString, ResultType.BulkString);
            ClassicAssert.AreEqual(expected, (string)result);
        }

        private static void AssertEmptyArray(RedisResult result, RedisProtocol protocol)
        {
            AssertType(result, protocol, ResultType.Array, ResultType.Array);
            ClassicAssert.AreEqual(0, result.Length);
        }

        private static void AssertType(RedisResult result, RedisProtocol protocol, ResultType resp2, ResultType resp3)
            => ClassicAssert.AreEqual(protocol == RedisProtocol.Resp2 ? resp2 : resp3, protocol == RedisProtocol.Resp2 ? result.Resp2Type : result.Resp3Type);

        private static void AssertVInfoIntegerFields(RedisResult info, RedisProtocol protocol, params (string Field, long Value)[] expected)
        {
            AssertType(info, protocol, ResultType.Array, ResultType.Map);
            var fields = new Dictionary<string, RedisResult>();
            if (protocol == RedisProtocol.Resp3)
            {
                foreach (var (key, value) in info.ToDictionary())
                    fields.Add((string)key, value);
            }
            else
            {
                var values = (RedisResult[])info;
                ClassicAssert.AreEqual(14, values.Length);
                for (var i = 0; i < values.Length; i += 2)
                    fields.Add((string)values[i], values[i + 1]);
            }

            ClassicAssert.AreEqual(7, fields.Count);
            foreach (var (field, value) in expected)
            {
                var result = fields[field];
                AssertType(result, protocol, ResultType.Integer, ResultType.Integer);
                ClassicAssert.AreEqual(value, (long)result, field);
            }
        }

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