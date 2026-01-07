// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespHashTests
    {
        GarnetServer server;

        static readonly HashEntry[] entries = new HashEntry[100];

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableReadCache: true, enableObjectStoreReadCache: true, enableAOF: true, lowMemory: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }


        #region SEClientTests

        [Test]
        public void CanSetEmpty()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", []);
            var exists = db.KeyExists("user:user1");
            ClassicAssert.IsFalse(exists);
        }

        [Test]
        public void CanSetAndGetOnePair()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite")]);
            string r = db.HashGet("user:user1", "Title");
            ClassicAssert.AreEqual("Tsavorite", r);
        }

        [Test]
        public async Task CanSetAndGetOnePairWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite")]);
            db.HashFieldExpire("user:user1", ["Title"], TimeSpan.FromMilliseconds(100));
            string r = db.HashGet("user:user1", "Title");
            ClassicAssert.AreEqual("Tsavorite", r);
            await Task.Delay(200);
            r = db.HashGet("user:user1", "Title");
            ClassicAssert.IsNull(r);
        }

        [Test]
        public async Task CanSetWithExpireAndRemoveExpireByCallingSetAgain()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite")]);
            db.HashFieldExpire("user:user1", ["Title"], TimeSpan.FromMilliseconds(100));
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite")]);
            await Task.Delay(200);
            string r = db.HashGet("user:user1", "Title");
            ClassicAssert.AreEqual("Tsavorite", r);
        }

        // Covers the fix of #954.
        [Test]
        public void CanFieldPersistAndGetTimeToLive()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "user:user1";
            var field = "field1";

            db.HashSet(key, [new HashEntry(field, "v1")]);
            db.HashFieldExpire(key, [field], TimeSpan.FromHours(1));
            db.HashFieldPersist(key, [field]);
            var ttl = db.HashFieldGetTimeToLive(key, [field]);
            ClassicAssert.AreEqual(-1, ttl[0]);
        }

        [Test]
        public void CanSetAndGetOnePairLarge()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var str = new string(new char[150000]);
            db.HashSet("user:user1", [new HashEntry("Title", str)]);
            string r = db.HashGet("user:user1", "Title");
            ClassicAssert.AreEqual(str, r);
            string r2 = db.HashGet("user:user1", "Title2");
            ClassicAssert.AreEqual(null, r2);
        }

        [Test]
        public void CanSetAndGetMultiPair()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            for (int i = 0; i < entries.Length; i++)
            {
                var item = new HashEntry($"item-{i + 1}", (i + 1) * 100);
                entries[i] = item;
            }
            for (int j = 0; j < 100; j++)
            {
                db.HashSet($"user:user-{j + 1}", entries);
            }

            string r = db.HashGet("user:user-22", "item-98");
            ClassicAssert.AreEqual("9800", r);
        }


        [Test]
        public void CanSetAndGetMultiplePairs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021")]);
            var result = db.HashGet("user:user1", [new RedisValue("Title"), new RedisValue("Year")]);
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.AreEqual("Tsavorite", result[0].ToString());
            ClassicAssert.AreEqual("2021", result[1].ToString());
        }

        [Test]
        public void CanDelSingleField()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021")]);
            var result = db.HashDelete(new RedisKey("user:user1"), new RedisValue("Title"));
            ClassicAssert.AreEqual(true, result);
            string resultGet = db.HashGet("user:user1", "Year");
            ClassicAssert.AreEqual("2021", resultGet);
        }

        [Test]
        public void CanDelWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021")]);
            db.HashFieldExpire("user:user1", ["Title"], TimeSpan.FromMilliseconds(100));
            var result = db.HashDelete(new RedisKey("user:user1"), new RedisValue("Title"));
            ClassicAssert.AreEqual(true, result);
            string resultGet = db.HashGet("user:user1", "Year");
            ClassicAssert.AreEqual("2021", resultGet);
        }


        [Test]
        public void CanDeleteMultipleFields()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Example", "One")]);
            var result = db.HashDelete(new RedisKey("user:user1"), [new RedisValue("Title"), new RedisValue("Year")]);
            string resultGet = db.HashGet("user:user1", "Example");
            ClassicAssert.AreEqual("One", resultGet);

            result = db.HashDelete(new RedisKey("user:user1"), [new RedisValue("Example")]);
            ClassicAssert.AreEqual(1, result);
            var exists = db.KeyExists("user:user1");
            ClassicAssert.IsFalse(exists);
        }

        [Test]
        public void CanDeleteMultipleFieldsWithNonExistingField()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021")]);
            var result = db.HashDelete(new RedisKey("user:user1"), [new RedisValue("Title"), new RedisValue("Year"), new RedisValue("Unknown")]);
            ClassicAssert.AreEqual(2, result);
        }

        [Test]
        public void CanDoHLen()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme")]);
            var result = db.HashLength("user:user1");
            ClassicAssert.AreEqual(3, result);
        }

        [Test]
        public async Task CanDoHLenWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme")]);
            db.HashFieldExpire("user:user1", ["Title"], TimeSpan.FromMilliseconds(100));
            var result = db.HashLength("user:user1");
            ClassicAssert.AreEqual(3, result);
            await Task.Delay(150);
            result = db.HashLength("user:user1");
            ClassicAssert.AreEqual(2, result);
            db.HashSet("user:user1", [new HashEntry("Year", "new2021")]);  // Trigger deletion of expired field
            result = db.HashLength("user:user1");
            ClassicAssert.AreEqual(2, result);
        }

        [Test]
        public void CanDoGetAll()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            HashEntry[] hashEntries =
                [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme")];
            db.HashSet("user:user1", hashEntries);
            var result = db.HashGetAll("user:user1");
            ClassicAssert.AreEqual(hashEntries.Length, result.Length);
            ClassicAssert.AreEqual(hashEntries.Length, result.Select(r => r.Name).Distinct().Count());
            ClassicAssert.IsTrue(hashEntries.OrderBy(e => e.Name).SequenceEqual(result.OrderBy(r => r.Name)));
        }

        [Test]
        public async Task CanDoGetAllWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            HashEntry[] hashEntries =
                [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme")];
            db.HashSet("user:user1", hashEntries);
            db.HashFieldExpire("user:user1", ["Title"], TimeSpan.FromMilliseconds(100));

            var result = db.HashGetAll("user:user1");
            ClassicAssert.AreEqual(hashEntries.Length, result.Length);
            ClassicAssert.AreEqual(hashEntries.Length, result.Select(r => r.Name).Distinct().Count());
            ClassicAssert.IsTrue(hashEntries.OrderBy(e => e.Name).SequenceEqual(result.OrderBy(r => r.Name)));

            await Task.Delay(200);

            result = db.HashGetAll("user:user1");
            ClassicAssert.AreEqual(hashEntries.Length - 1, result.Length);
            ClassicAssert.IsTrue(hashEntries.Skip(1).OrderBy(e => e.Name).SequenceEqual(result.OrderBy(r => r.Name)));

            db.HashSet("user:user1", [new HashEntry("Year", "new2021")]);  // Trigger deletion of expired field
            result = db.HashGetAll("user:user1");
            ClassicAssert.AreEqual(hashEntries.Length - 1, result.Length);
            ClassicAssert.IsTrue(hashEntries.Skip(1).Select(x => x.Value == "2021" ? new HashEntry(x.Name, "new2021") : x).OrderBy(e => e.Name).SequenceEqual(result.OrderBy(r => r.Name)));
        }

        [Test]
        public void CanDoHExists()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme")]);
            var result = db.HashExists(new RedisKey("user:user1"), new RedisValue("Company"));
            ClassicAssert.AreEqual(true, result);

            result = db.HashExists(new RedisKey("user:userfoo"), "Title");
            ClassicAssert.AreEqual(false, result);
        }

        [Test]
        public async Task CanDoHExistsWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme")]);
            db.HashFieldExpire("user:user1", ["Title"], TimeSpan.FromMilliseconds(100));

            var result = db.HashExists(new RedisKey("user:user1"), "Title");
            ClassicAssert.IsTrue(result);

            await Task.Delay(200);

            result = db.HashExists(new RedisKey("user:user1"), "Title");
            ClassicAssert.IsFalse(result);

            db.HashSet("user:user1", [new HashEntry("Year", "new2021")]);  // Trigger deletion of expired field
            result = db.HashExists(new RedisKey("user:user1"), "Title");
            ClassicAssert.IsFalse(result);
        }

        [Test]
        public void CanDoHStrLen()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user.user1", [new HashEntry("Title", "Tsavorite")]);
            long r = db.HashStringLength("user.user1", "Title");
            ClassicAssert.AreEqual(9, r, 0);
            r = db.HashStringLength("user.user1", "NoExist");
            ClassicAssert.AreEqual(0, r, 0);
            r = db.HashStringLength("user.user2", "Title");
            ClassicAssert.AreEqual(0, r, 0);
        }

        [Test]
        public async Task CanDoHStrLenWithExire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite")]);
            db.HashFieldExpire("user:user1", ["Title"], TimeSpan.FromMilliseconds(100));

            long r = db.HashStringLength("user:user1", "Title");
            ClassicAssert.AreEqual(9, r);

            await Task.Delay(200);

            r = db.HashStringLength("user:user1", "Title");
            ClassicAssert.AreEqual(0, r);

            db.HashSet("user:user1", [new HashEntry("Year", "new2021")]);  // Trigger deletion of expired field
            r = db.HashStringLength("user:user1", "Title");
            ClassicAssert.AreEqual(0, r);
        }

        [Test]
        public void CanDoHKeys()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme")]);
            var result = db.HashKeys("user:user1");
            ClassicAssert.AreEqual(3, result.Length);

            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Title")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Year")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Company")));
        }

        [Test]
        public async Task CanDoHKeysWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme")]);
            db.HashFieldExpire("user:user1", ["Title"], TimeSpan.FromMilliseconds(100));
            var result = db.HashKeys("user:user1");
            ClassicAssert.AreEqual(3, result.Length);
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Title")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Year")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Company")));

            await Task.Delay(200);

            result = db.HashKeys("user:user1");
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Year")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Company")));

            db.HashSet("user:user1", [new HashEntry("Year", "new2021")]);  // Trigger deletion of expired field

            result = db.HashKeys("user:user1");
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Year")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Company")));
        }


        [Test]
        public void CanDoHVals()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme")]);
            var result = db.HashValues("user:user1");
            ClassicAssert.AreEqual(3, result.Length);

            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Tsavorite")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("2021")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Acme")));
        }

        [Test]
        public async Task CanDoHValsWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme")]);
            db.HashFieldExpire("user:user1", ["Title"], TimeSpan.FromMilliseconds(100));
            var result = db.HashValues("user:user1");
            ClassicAssert.AreEqual(3, result.Length);
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Tsavorite")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("2021")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Acme")));

            await Task.Delay(200);

            result = db.HashValues("user:user1");
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("2021")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Acme")));

            db.HashSet("user:user1", [new HashEntry("Year", "new2021")]);  // Trigger deletion of expired field

            result = db.HashValues("user:user1");
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("new2021")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.Equals("Acme")));
        }


        [Test]
        public void CanDoHIncrBy()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Field1", "StringValue"), new HashEntry("Field2", "1")]);
            Assert.Throws<RedisServerException>(() => db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field1"), 4));
            Assert.Throws<RedisServerException>(() => db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field2"), double.PositiveInfinity));
            var result = db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field2"), -4);
            ClassicAssert.AreEqual(-3, result);
            //new Key
            result = db.HashIncrement(new RedisKey("user:user2"), new RedisValue("Field2"), 4);
            ClassicAssert.AreEqual(4, result);
            // make sure the new hash object was created
            var getResult = db.HashGet("user:user2", "Field2");
            ClassicAssert.AreEqual(4, ((int?)getResult));
        }

        [Test]
        public async Task CanDoHIncrByWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Field1", "StringValue"), new HashEntry("Field2", "1")]);
            db.HashFieldExpire("user:user1", ["Field2"], TimeSpan.FromMilliseconds(100));
            var result = db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field2"), -4);
            ClassicAssert.AreEqual(-3, result);

            await Task.Delay(200);

            result = db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field2"), -4);
            ClassicAssert.AreEqual(-4, result);
        }

        [Test]
        public void CanDoHIncrByLTM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create LTM (larger than memory) DB by inserting 100 keys
            for (int i = 0; i < 100; i++)
                db.HashSet("user:user" + i, [new HashEntry("Field1", "StringValue"), new HashEntry("Field2", "1")]);

            Assert.Throws<RedisServerException>(() => db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field1"), 4));
            var result = db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field2"), -4);
            ClassicAssert.AreEqual(-3, result);

            // Test new key
            result = db.HashIncrement(new RedisKey("user:user100"), new RedisValue("Field2"), 4);
            ClassicAssert.AreEqual(4, result);
            // make sure the new hash object was created
            var getResult = db.HashGet("user:user100", "Field2");
            ClassicAssert.AreEqual(4, ((int?)getResult));
        }

        [Test]
        public void CanDoHashDecrement()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Field1", "StringValue"), new HashEntry("Field2", "1")]);
            var result = db.HashDecrement(new RedisKey("user:user1"), new RedisValue("Field2"), 4);
            ClassicAssert.AreEqual(-3, result);
        }

        [Test]
        public void CheckHashIncrementDoublePrecision()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Field1", "1.1111111111")]);
            var result = db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field1"), 2.2222222222);
            ClassicAssert.AreEqual(3.3333333333, result, 1e-15);
        }

        [Test]
        public async Task CheckHashIncrementDoublePrecisionWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Field1", "1.1111111111")]);
            db.HashFieldExpire("user:user1", ["Field1"], TimeSpan.FromMilliseconds(100));
            var result = db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field1"), 2.2222222222);
            ClassicAssert.AreEqual(3.3333333333, result, 1e-15);

            await Task.Delay(200);

            result = db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field1"), 2.2222222222);
            ClassicAssert.AreEqual(2.2222222222, result, 1e-15);
        }

        [Test]
        public void CanDoHSETNXCommand()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet(new RedisKey("user:user1"), new RedisValue("Field"), new RedisValue("Hello"), When.NotExists);
            string result = db.HashGet("user:user1", "Field");
            ClassicAssert.AreEqual("Hello", result);
            db.HashSet(new RedisKey("user:user1"), new RedisValue("Field"), new RedisValue("World"), When.NotExists);
            result = db.HashGet("user:user1", "Field");
            ClassicAssert.AreEqual("Hello", result);
        }

        [Test]
        public async Task CanDoHSETNXCommandWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet(new RedisKey("user:user1"), new RedisValue("Field"), new RedisValue("Hello"));
            db.HashFieldExpire("user:user1", ["Field"], TimeSpan.FromMilliseconds(100));
            db.HashSet(new RedisKey("user:user1"), new RedisValue("Field"), new RedisValue("Hello"), When.NotExists);

            await Task.Delay(200);

            string result = db.HashGet("user:user1", "Field");
            ClassicAssert.IsNull(result); // SetNX should not reset the expiration
        }

        [Test]
        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void CanDoRandomField(RedisProtocol protocol)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
            var db = redis.GetDatabase(0);

            var hashKey = new RedisKey("user:user1");
            HashEntry[] hashEntries =
                [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme")];
            var hashDict = hashEntries.ToDictionary(e => e.Name, e => e.Value);
            db.HashSet(hashKey, hashEntries);

            // Check HRANDFIELD with wrong number of arguments
            var ex = Assert.Throws<RedisServerException>(() => db.Execute("HRANDFIELD", hashKey, 3, "WITHVALUES", "bla"));
            var expectedMessage = string.Format(CmdStrings.GenericErrWrongNumArgs, nameof(RespCommand.HRANDFIELD));
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedMessage, ex.Message);

            // Check HRANDFIELD with non-numeric count
            ex = Assert.Throws<RedisServerException>(() => db.Execute("HRANDFIELD", hashKey, "bla"));
            expectedMessage = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedMessage, ex.Message);

            // Check HRANDFIELD with syntax error
            ex = Assert.Throws<RedisServerException>(() => db.Execute("HRANDFIELD", hashKey, 3, "withvalue"));
            expectedMessage = Encoding.ASCII.GetString(CmdStrings.RESP_SYNTAX_ERROR);
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedMessage, ex.Message);

            // HRANDFIELD without count
            var field = db.HashRandomField(hashKey);
            ClassicAssert.IsFalse(field.IsNull);
            ClassicAssert.Contains(field, hashDict.Keys);

            // HRANDFIELD with positive count (distinct)
            var fields = db.HashRandomFields(hashKey, 2);
            ClassicAssert.AreEqual(2, fields.Length);
            ClassicAssert.AreEqual(2, fields.Distinct().Count());
            ClassicAssert.IsTrue(fields.All(hashDict.ContainsKey));

            // HRANDFIELD with positive count (distinct) with values
            var fieldsWithValues = db.HashRandomFieldsWithValues(hashKey, 2);
            ClassicAssert.AreEqual(2, fieldsWithValues.Length);
            ClassicAssert.AreEqual(2, fieldsWithValues.Distinct().Count());
            ClassicAssert.IsTrue(fieldsWithValues.All(e => hashDict.ContainsKey(e.Name) && hashDict[e.Name] == e.Value));

            // HRANDFIELD with positive count (distinct) greater than hash cardinality
            fields = db.HashRandomFields(hashKey, 5);
            ClassicAssert.AreEqual(fields.Length, fields.Length);
            ClassicAssert.AreEqual(fields.Length, fields.Distinct().Count());
            ClassicAssert.IsTrue(fields.All(hashDict.ContainsKey));

            // HRANDFIELD with negative count (non-distinct)
            fields = db.HashRandomFields(hashKey, -8);
            ClassicAssert.AreEqual(8, fields.Length);
            ClassicAssert.GreaterOrEqual(3, fields.Distinct().Count());
            ClassicAssert.IsTrue(fields.All(hashDict.ContainsKey));

            // HRANDFIELD with negative count (non-distinct) with values
            fieldsWithValues = db.HashRandomFieldsWithValues(hashKey, -8);
            ClassicAssert.AreEqual(8, fieldsWithValues.Length);
            ClassicAssert.GreaterOrEqual(3, fieldsWithValues.Select(e => e.Name).Distinct().Count());
            ClassicAssert.IsTrue(fieldsWithValues.All(e => hashDict.ContainsKey(e.Name) && hashDict[e.Name] == e.Value));
        }

        [Test]
        public async Task CanDoRandomFieldWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var hashKey = new RedisKey("user:user1");
            HashEntry[] hashEntries = [new HashEntry("Title", "Tsavorite")];
            db.HashSet(hashKey, hashEntries);
            db.HashFieldExpire("user:user1", ["Title"], TimeSpan.FromMilliseconds(100));
            string field = db.HashRandomField(hashKey);
            ClassicAssert.AreEqual(field, "Title");

            await Task.Delay(200);

            field = db.HashRandomField(hashKey);
            ClassicAssert.IsNull(field);
        }

        [Test]
        public async Task CanDoRandomFieldsWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var hashKey = new RedisKey("user:user1");
            HashEntry[] hashEntries = [new HashEntry("Title", "Tsavorite")];
            db.HashSet(hashKey, hashEntries);
            db.HashFieldExpire("user:user1", ["Title"], TimeSpan.FromMilliseconds(100));
            var field = db.HashRandomFields(hashKey, 10).Select(x => (string)x).ToArray();
            ClassicAssert.AreEqual(field.Length, 1);
            ClassicAssert.AreEqual("Title", field[0]);

            await Task.Delay(200);

            field = db.HashRandomFields(hashKey, 10).Select(x => (string)x).ToArray();
            ClassicAssert.AreEqual(field.Length, 0);
        }

        [Test]
        public void HashRandomFieldEmptyHash()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var hashKey = new RedisKey("user:user1");
            var singleField = db.HashRandomField(hashKey);
            var multiFields = db.HashRandomFields(hashKey, 3);

            // this should return an empty array
            var withValues = db.HashRandomFieldsWithValues(hashKey, 3);

            ClassicAssert.AreEqual(RedisValue.Null, singleField);
            ClassicAssert.IsEmpty(multiFields);
            ClassicAssert.IsEmpty(withValues);
        }

        [Test]
        public void CanDoHashScan()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // HSCAN non existing key
            var members = db.HashScan("foo");
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsEmpty(members, "HSCAN non existing key failed.");

            db.HashSet("user:user1", [new HashEntry("name", "Alice"), new HashEntry("email", "email@example.com"), new HashEntry("age", "30")]);

            // HSCAN without key
            var e = Assert.Throws<RedisServerException>(() => db.Execute("HSCAN"));
            var expectedErrorMessage = string.Format(CmdStrings.GenericErrWrongNumArgs, nameof(HashOperation.HSCAN));
            ClassicAssert.AreEqual(expectedErrorMessage, e.Message);

            // HSCAN without parameters
            members = db.HashScan("user:user1");
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsTrue(members.Count() == 3, "HSCAN without MATCH failed.");

            db.HashSet("user:user789", [new HashEntry("email", "email@example.com"), new HashEntry("email1", "email1@example.com"), new HashEntry("email2", "email2@example.com"), new HashEntry("email3", "email3@example.com"), new HashEntry("age", "25")]);

            // HSCAN with match
            members = db.HashScan("user:user789", "email*");
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsTrue(members.Count() == 4, "HSCAN with MATCH failed.");

            members = db.HashScan("user:user789", "age", 5000);
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsTrue(members.Count() == 1, "HSCAN with MATCH failed.");

            members = db.HashScan("user:user789", "*");
            ClassicAssert.IsTrue(members.Count() == 5, "HSCAN with MATCH failed.");

            var fields = db.HashScanNoValues("user:user789", "*");
            ClassicAssert.IsTrue(fields.Count() == 5, "HSCAN with MATCH failed.");
            CollectionAssert.AreEquivalent(new[] { "email", "email1", "email2", "email3", "age" }, fields.Select(f => f.ToString()));

            RedisResult result = db.Execute("HSCAN", "user:user789", "0", "MATCH", "*", "COUNT", "2", "NOVALUES");
            ClassicAssert.IsTrue(result.Length == 2);
            var fieldsStr = ((RedisResult[])result[1]).Select(x => (string)x).ToArray();
            ClassicAssert.IsTrue(fieldsStr.Length == 2, "HSCAN with MATCH failed.");
            CollectionAssert.AreEquivalent(new[] { "email", "email1" }, fieldsStr);
        }

        [Test]
        public async Task CanDoHashScanWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.HashSet("user:user789", [new HashEntry("email", "email@example.com"), new HashEntry("email1", "email1@example.com"), new HashEntry("email2", "email2@example.com"), new HashEntry("email3", "email3@example.com"), new HashEntry("age", "25")]);
            db.HashFieldExpire("user:user789", ["email"], TimeSpan.FromMilliseconds(100));

            var members = db.HashScan("user:user789", "email*");
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsTrue(members.Count() == 4, "HSCAN with MATCH failed.");

            await Task.Delay(200);

            // HSCAN with match
            members = db.HashScan("user:user789", "email*");
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsTrue(members.Count() == 3, "HSCAN with MATCH failed.");
        }


        [Test]
        public void CanDoHashScanWithCursor()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create a new array of HashEntry
            var hashEntries = new HashEntry[1000];
            for (int i = 0; i < hashEntries.Length; i++)
            {
                // Add random values for new HashEntry array
                hashEntries[i] = new HashEntry("key" + i, "value" + i);
            }

            // Hashset with the hashentries
            db.HashSet("userhs", hashEntries);

            int pageSize = 45;
            var response = db.HashScan("userhs", "*", pageSize: pageSize, cursor: 0);
            var cursor = ((IScanningCursor)response);
            var j = 0;
            long pageNumber = 0;
            long pageOffset = 0;

            // Consume the enumeration
            foreach (var i in response)
            {
                // Represents the *active* page of results (not the pending/next page of results as returned by SCAN/HSCAN/ZSCAN/SSCAN)
                pageNumber = cursor.Cursor;

                // The offset into the current page.
                pageOffset = cursor.PageOffset;
                j++;
            }

            // Assert the end of the enumeration was reached
            ClassicAssert.AreEqual(hashEntries.Length, j);

            // Assert the cursor is at the end of the enumeration
            ClassicAssert.AreEqual(pageNumber + pageOffset, hashEntries.Length - 1);

            var l = response.LastOrDefault();
            ClassicAssert.AreEqual(l.Name, $"key{hashEntries.Length - 1}");
        }

        [Test]
        public async Task CanDoHMGET()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var hashkey = "testGetMulti";

            db.KeyDelete(hashkey, CommandFlags.FireAndForget);

            RedisValue[] fields = ["foo", "bar", "blop"];
            var arr0 = await db.HashGetAsync(hashkey, fields);

            db.HashSet(hashkey, "foo", "abc", flags: CommandFlags.FireAndForget);
            db.HashSet(hashkey, "bar", "def", flags: CommandFlags.FireAndForget);

            var arr1 = await db.HashGetAsync(hashkey, fields);
            var arr2 = await db.HashGetAsync(hashkey, fields);

            ClassicAssert.AreEqual(3, arr0.Length);
#nullable enable
            ClassicAssert.Null((string?)arr0[0]);
            ClassicAssert.Null((string?)arr0[1]);
            ClassicAssert.Null((string?)arr0[2]);

            ClassicAssert.AreEqual(3, arr1.Length);
            ClassicAssert.AreEqual("abc", (string?)arr1[0]);
            ClassicAssert.AreEqual("def", (string?)arr1[1]);
            ClassicAssert.Null((string?)arr1[2]);

            ClassicAssert.AreEqual(3, arr2.Length);
            ClassicAssert.AreEqual("abc", (string?)arr2[0]);
            ClassicAssert.AreEqual("def", (string?)arr2[1]);
            ClassicAssert.Null((string?)arr2[2]);
#nullable disable
        }

        [Test]
        public async Task CanDoHMGETWithExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.HashSet("user:user789", [new HashEntry("email", "email@example.com"), new HashEntry("email1", "email1@example.com"), new HashEntry("email2", "email2@example.com"), new HashEntry("email3", "email3@example.com"), new HashEntry("age", "25")]);
            db.HashFieldExpire("user:user789", ["email"], TimeSpan.FromMilliseconds(100));

            var members = (string[])db.Execute("HMGET", "user:user789", "email", "email1");
            ClassicAssert.AreEqual("email@example.com", members[0]);
            ClassicAssert.AreEqual("email1@example.com", members[1]);

            await Task.Delay(200);

            members = (string[])db.Execute("HMGET", "user:user789", "email", "email1");
            ClassicAssert.IsNull(members[0]);
            ClassicAssert.AreEqual("email1@example.com", members[1]);

            db.HashSet("user:user789", [new HashEntry("email2", "newemail2@example.com")]);  // Trigger deletion of expired field

            members = (string[])db.Execute("HMGET", "user:user789", "email", "email1");
            ClassicAssert.IsNull(members[0]);
            ClassicAssert.AreEqual("email1@example.com", members[1]);
        }


        [Test]
        public async Task CanDoHGETALL()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var hashkey = "testGetAll";

            _ = await db.KeyDeleteAsync(hashkey);

            var result0 = db.HashGetAllAsync(hashkey);

            _ = await db.HashSetAsync(hashkey, "foo", "abc");
            _ = await db.HashSetAsync(hashkey, "bar", "def");

            var result1 = db.HashGetAllAsync(hashkey);

            ClassicAssert.IsEmpty(redis.Wait(result0));
            var result = redis.Wait(result1).ToStringDictionary();
            ClassicAssert.AreEqual(2, result.Count);
            ClassicAssert.AreEqual("abc", result["foo"]);
            ClassicAssert.AreEqual("def", result["bar"]);
        }
        [Test]
        public async Task CanDoHMSETMultipleTimes()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var hashKey = "testCanDoHMSET";
            var hashMapKey = "TestKey";

            await db.KeyDeleteAsync(hashKey);

            var val0 = await db.HashGetAsync(hashKey, hashMapKey);
            await db.HashSetAsync(hashKey, [new HashEntry(hashMapKey, "TestValue1")]);

            var val1 = await db.HashGetAsync(hashKey, hashMapKey);
            await db.HashSetAsync(hashKey, [new HashEntry(hashMapKey, "TestValue2")]);

            var val2 = await db.HashGetAsync(hashKey, hashMapKey);

#nullable enable
            ClassicAssert.Null((string?)val0);
            ClassicAssert.AreEqual("TestValue1", (string?)val1);
            ClassicAssert.AreEqual("TestValue2", (string?)val2);
#nullable disable
        }

        [Test]
        public async Task CanDoHSETWhenAlwaysAsync()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var hashkey = "testWhenAlways";

            await db.KeyDeleteAsync(hashkey);

            var result1 = await db.HashSetAsync(hashkey, "foo", "bar", When.Always, CommandFlags.None);
            var result2 = await db.HashSetAsync(hashkey, "foo2", "bar", When.Always, CommandFlags.None);
            var result3 = await db.HashSetAsync(hashkey, "foo", "bar", When.Always, CommandFlags.None);
            var result4 = await db.HashSetAsync(hashkey, "foo", "bar2", When.Always, CommandFlags.None);

            ClassicAssert.True(result1, "Initial set key 1");
            ClassicAssert.True(result2, "Initial set key 2");

            // Fields modified *but not added* should be a zero/false.
            ClassicAssert.False(result3, "Duplicate set key 1");
            ClassicAssert.False(result4, "Duplicate se key 1 variant");
        }

        [Test]
        public async Task CanDoHashSetWithNX()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var hashkey = "testWhenNotExists";

            var del = await db.KeyDeleteAsync(hashkey);

            var val0 = await db.HashGetAsync(hashkey, "field");
            var set0 = await db.HashSetAsync(hashkey, "field", "value1", When.NotExists);
            var val1 = await db.HashGetAsync(hashkey, "field");
            var set1 = await db.HashSetAsync(hashkey, "field", "value2", When.NotExists);
            var val2 = await db.HashGetAsync(hashkey, "field");

            var set2 = await db.HashSetAsync(hashkey, "field-blob", Encoding.UTF8.GetBytes("value3"), When.NotExists);
            var val3 = await db.HashGetAsync(hashkey, "field-blob");
            var set3 = await db.HashSetAsync(hashkey, "field-blob", Encoding.UTF8.GetBytes("value3"), When.NotExists);

            ClassicAssert.IsFalse(del);
#nullable enable
            ClassicAssert.Null((string?)val0);
            ClassicAssert.True(set0);
            ClassicAssert.AreEqual("value1", (string?)val1);
            ClassicAssert.False(set1);
            ClassicAssert.AreEqual("value1", (string?)val2);

            ClassicAssert.True(set2);
            ClassicAssert.AreEqual("value3", (string?)val3);
            ClassicAssert.False(set3);
#nullable disable
        }

        [Test]
        public void CheckEmptyHashKeyRemoved()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var key = new RedisKey("user1:hash");
            var db = redis.GetDatabase(0);

            db.HashSet(key, [new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021")]);

            var result = db.HashDelete(key, new RedisValue("Title"));
            ClassicAssert.IsTrue(result);
            result = db.HashDelete(key, new RedisValue("Year"));
            ClassicAssert.IsTrue(result);

            var keyExists = db.KeyExists(key);
            ClassicAssert.IsFalse(keyExists);
        }

        [Test]
        public void CheckHashOperationsOnWrongTypeObjectSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var keys = new[] { new RedisKey("user1:obj1"), new RedisKey("user1:obj2") };
            var key1Values = new[] { new RedisValue("Hello"), new RedisValue("World") };
            var key2Values = new[] { new RedisValue("Hola"), new RedisValue("Mundo") };
            var values = new[] { key1Values, key2Values };
            RedisValue[][] hashFields =
            [
                [new RedisValue("K1_H1"), new RedisValue("K1_H2")],
                [new RedisValue("K2_H1"), new RedisValue("K2_H2")]
            ];
            var hashEntries = hashFields.Select((h, idx) => h
                    .Zip(values[idx], (n, v) => new HashEntry(n, v)).ToArray()).ToArray();

            // Set up different type objects
            RespTestsUtils.SetUpTestObjects(db, GarnetObjectType.List, keys, values);

            // HGET
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashGet(keys[0], hashFields[0][0]));
            // HMGET
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashGet(keys[0], hashFields[0]));
            // HSET
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashSet(keys[0], hashFields[0][0], values[0][0]));
            // HMSET
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashSet(keys[0], hashEntries[0]));
            // HSETNX
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashSet(keys[0], hashFields[0][0], values[0][0], When.NotExists));
            // HLEN
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashLength(keys[0]));
            // HDEL
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashDelete(keys[0], hashFields[0]));
            // HEXISTS
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashExists(keys[0], hashFields[0][0]));
            // HGETALL
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashGetAll(keys[0]));
            // HKEYS
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashKeys(keys[0]));
            // HVALS
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashValues(keys[0]));
            // HINCRBY
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashIncrement(keys[0], hashFields[0][0], 2L));
            // HINCRBYFLOAT
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashIncrement(keys[0], hashFields[0][0], 2.2));
            // HRANDFIELD
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashRandomField(keys[0]));
            // HSCAN
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashScan(keys[0], new RedisValue("*")).FirstOrDefault());
            //HSTRLEN
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.HashStringLength(keys[0], hashFields[0][0]));
        }

        [Test]
        public async Task CanDoHashExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("myhash", [new HashEntry("field1", "hello"), new HashEntry("field2", "world"), new HashEntry("field3", "value3"), new HashEntry("field4", "value4"), new HashEntry("field5", "value5"), new HashEntry("field6", "value6")]);

            var result = db.Execute("HEXPIRE", "myhash", "4", "FIELDS", "3", "field1", "field5", "nonexistfield");
            var results = (RedisResult[])result;
            ClassicAssert.AreEqual(3, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);
            ClassicAssert.AreEqual(1, (long)results[1]);
            ClassicAssert.AreEqual(-2, (long)results[2]);

            result = db.Execute("HPEXPIRE", "myhash", "4000", "FIELDS", "2", "field2", "nonexistfield");
            results = (RedisResult[])result;
            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);
            ClassicAssert.AreEqual(-2, (long)results[1]);

            result = db.Execute("HEXPIREAT", "myhash", DateTimeOffset.UtcNow.AddSeconds(4).ToUnixTimeSeconds().ToString(), "FIELDS", "2", "field3", "nonexistfield");
            results = (RedisResult[])result;
            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);
            ClassicAssert.AreEqual(-2, (long)results[1]);

            result = db.Execute("HPEXPIREAT", "myhash", DateTimeOffset.UtcNow.AddSeconds(4).ToUnixTimeMilliseconds().ToString(), "FIELDS", "2", "field4", "nonexistfield");
            results = (RedisResult[])result;
            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);
            ClassicAssert.AreEqual(-2, (long)results[1]);

            var ttl = (RedisResult[])db.Execute("HTTL", "myhash", "FIELDS", "2", "field1", "nonexistfield");
            ClassicAssert.AreEqual(2, ttl.Length);
            ClassicAssert.LessOrEqual((long)ttl[0], 4);
            ClassicAssert.Greater((long)ttl[0], 1);
            ClassicAssert.AreEqual(-2, (long)results[1]);

            ttl = (RedisResult[])db.Execute("HPTTL", "myhash", "FIELDS", "2", "field1", "nonexistfield");
            ClassicAssert.AreEqual(2, ttl.Length);
            ClassicAssert.LessOrEqual((long)ttl[0], 4000);
            ClassicAssert.Greater((long)ttl[0], 1000);
            ClassicAssert.AreEqual(-2, (long)results[1]);

            ttl = (RedisResult[])db.Execute("HEXPIRETIME", "myhash", "FIELDS", "2", "field1", "nonexistfield");
            ClassicAssert.AreEqual(2, ttl.Length);
            ClassicAssert.LessOrEqual((long)ttl[0], DateTimeOffset.UtcNow.AddSeconds(4).ToUnixTimeSeconds());
            ClassicAssert.Greater((long)ttl[0], DateTimeOffset.UtcNow.AddSeconds(1).ToUnixTimeSeconds());
            ClassicAssert.AreEqual(-2, (long)results[1]);

            ttl = (RedisResult[])db.Execute("HPEXPIRETIME", "myhash", "FIELDS", "2", "field1", "nonexistfield");
            ClassicAssert.AreEqual(2, ttl.Length);
            ClassicAssert.LessOrEqual((long)ttl[0], DateTimeOffset.UtcNow.AddSeconds(4).ToUnixTimeMilliseconds());
            ClassicAssert.Greater((long)ttl[0], DateTimeOffset.UtcNow.AddSeconds(1).ToUnixTimeMilliseconds());
            ClassicAssert.AreEqual(-2, (long)results[1]);

            results = (RedisResult[])db.Execute("HPERSIST", "myhash", "FIELDS", "3", "field5", "field6", "nonexistfield");
            ClassicAssert.AreEqual(3, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);  // 1 the expiration was removed.
            ClassicAssert.AreEqual(-1, (long)results[1]); // -1 if the field exists but has no associated expiration set.
            ClassicAssert.AreEqual(-2, (long)results[2]);

            await Task.Delay(4500);

            var items = db.HashGetAll("myhash");
            ClassicAssert.AreEqual(2, items.Length);
            ClassicAssert.AreEqual("field5", items[0].Name.ToString());
            ClassicAssert.AreEqual("value5", items[0].Value.ToString());
            ClassicAssert.AreEqual("field6", items[1].Name.ToString());
            ClassicAssert.AreEqual("value6", items[1].Value.ToString());

            result = db.Execute("HEXPIRE", "myhash", "0", "FIELDS", "1", "field5");
            results = (RedisResult[])result;
            ClassicAssert.AreEqual(1, results.Length);
            ClassicAssert.AreEqual(2, (long)results[0]);

            result = db.Execute("HEXPIREAT", "myhash", DateTimeOffset.UtcNow.AddSeconds(-1).ToUnixTimeSeconds().ToString(), "FIELDS", "1", "field6");
            results = (RedisResult[])result;
            ClassicAssert.AreEqual(1, results.Length);
            ClassicAssert.AreEqual(2, (long)results[0]);

            items = db.HashGetAll("myhash");
            ClassicAssert.AreEqual(0, items.Length);
        }

        [Test]
        public async Task CanDoHashExpireLTM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.EndPoint);

            string[] smallExpireKeys = ["user:user0", "user:user1"];
            string[] largeExpireKeys = ["user:user2", "user:user3"];

            foreach (var key in smallExpireKeys)
            {
                db.HashSet(key, [new HashEntry("Field1", "StringValue"), new HashEntry("Field2", "1")]);
                db.Execute("HEXPIRE", key, "2", "FIELDS", "1", "Field1");
            }

            foreach (var key in largeExpireKeys)
            {
                db.HashSet(key, [new HashEntry("Field1", "StringValue"), new HashEntry("Field2", "1")]);
                db.Execute("HEXPIRE", key, "4", "FIELDS", "1", "Field1");
            }

            // Create LTM (larger than memory) DB by inserting 100 keys
            for (int i = 4; i < 100; i++)
            {
                var key = "user:user" + i;
                db.HashSet(key, [new HashEntry("Field1", "StringValue"), new HashEntry("Field2", "1")]);
            }

            var info = TestUtils.GetStoreAddressInfo(server, includeReadCache: true, isObjectStore: true);
            // Ensure data has spilled to disk
            ClassicAssert.Greater(info.HeadAddress, info.BeginAddress);

            await Task.Delay(2000);

            var result = db.HashExists(smallExpireKeys[0], "Field1");
            ClassicAssert.IsFalse(result);
            result = db.HashExists(smallExpireKeys[1], "Field1");
            ClassicAssert.IsFalse(result);
            result = db.HashExists(largeExpireKeys[0], "Field1");
            ClassicAssert.IsTrue(result);
            result = db.HashExists(largeExpireKeys[1], "Field1");
            ClassicAssert.IsTrue(result);
            var ttl = db.HashFieldGetTimeToLive(largeExpireKeys[0], ["Field1"]);
            ClassicAssert.AreEqual(ttl.Length, 1);
            ClassicAssert.Greater(ttl[0], 0);
            ClassicAssert.LessOrEqual(ttl[0], 2000);
            ttl = db.HashFieldGetTimeToLive(largeExpireKeys[1], ["Field1"]);
            ClassicAssert.AreEqual(ttl.Length, 1);
            ClassicAssert.Greater(ttl[0], 0);
            ClassicAssert.LessOrEqual(ttl[0], 2000);

            await Task.Delay(2000);

            result = db.HashExists(largeExpireKeys[0], "Field1");
            ClassicAssert.IsFalse(result);
            result = db.HashExists(largeExpireKeys[1], "Field1");
            ClassicAssert.IsFalse(result);

            var data = db.HashGetAll("user:user4");
            ClassicAssert.AreEqual(2, data.Length);
            ClassicAssert.AreEqual("Field1", data[0].Name.ToString());
            ClassicAssert.AreEqual("StringValue", data[0].Value.ToString());
            ClassicAssert.AreEqual("Field2", data[1].Name.ToString());
            ClassicAssert.AreEqual("1", data[1].Value.ToString());
        }

        [Test]
        public void CanDoHashExpireWithAofRecovery()
        {
            // Test AOF recovery of hash entries with expiry

            var key1 = "key1";
            var key2 = "key2";
            HashEntry[] values1_1 = [new HashEntry("key1_1", "val1_1"), new HashEntry("key1_2", "val1_2")];
            HashEntry[] values1_2 = [new HashEntry("key1_3", "val1_3"), new HashEntry("key1_4", "val1_4")];
            HashEntry[] values2_1 = [new HashEntry("key2_1", "val2_1"), new HashEntry("key2_2", "val2_2")];
            HashEntry[] values2_2 = [new HashEntry("key2_3", "val2_3"), new HashEntry("key2_4", "val2_4")];

            var expireTime = DateTimeOffset.UtcNow + TimeSpan.FromMinutes(1);

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Set 1st hash set, add long expiry to 1 entry
                db.HashSet(key1, values1_1);
                db.Execute("HEXPIREAT", key1, expireTime.ToUnixTimeSeconds(), "FIELDS", 1, "key1_1");

                // Set 1st hash set, add short expiry to 1 entry
                db.HashSet(key2, values2_1);
                db.Execute("HEXPIRE", key2, 1, "FIELDS", 1, "key2_1");

                // Wait for short expiry to pass
                Thread.Sleep(2000);

                // Add more entries to 1st and 2nd hash sets
                db.HashSet(key1, values1_2);
                db.HashSet(key2, values2_2);

                // Add longer expiry to entry in 2nd hash set
                db.Execute("HPEXPIRE", key2, 15000, "FIELDS", 1, "key2_2");
                Thread.Sleep(2000);

                // Verify 1st hash set contains all added entries
                var recoveredValues = db.HashGetAll(key1);
                CollectionAssert.AreEquivalent(values1_1.Union(values1_2), recoveredValues);

                // Verify expiry times of entries in 1st hash set
                var recoveredValuesExpTime = (RedisResult[])db.Execute("HEXPIRETIME", key1, "FIELDS", 2, "key1_1", "key1_2");
                ClassicAssert.IsNotNull(recoveredValuesExpTime);
                ClassicAssert.AreEqual(2, recoveredValuesExpTime!.Length);
                Assert.That(expireTime.ToUnixTimeSeconds(), Is.EqualTo((long)recoveredValuesExpTime[0]).Within(1));
                ClassicAssert.AreEqual(-1, (long)recoveredValuesExpTime[1]);

                // Verify 2nd hash set contains all added entries except 1 expired entry
                recoveredValues = db.HashGetAll(key2);
                CollectionAssert.AreEquivalent(values2_1.Skip(1).Union(values2_2), recoveredValues);

                // Verify 2nd hash set entries ttls
                var recoveredValuesTtl = (RedisResult[])db.Execute("HTTL", key2, "FIELDS", 4, "key2_1", "key2_2", "key2_3", "key2_4");
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

                // Verify 1st hash set contains all added entries
                var recoveredValues = db.HashGetAll(key1);
                CollectionAssert.AreEquivalent(values1_1.Union(values1_2), recoveredValues);

                // Verify expiry times of entries in 1st hash set
                var recoveredValuesExpTime = (RedisResult[])db.Execute("HEXPIRETIME", key1, "FIELDS", 2, "key1_1", "key1_2");
                ClassicAssert.IsNotNull(recoveredValuesExpTime);
                ClassicAssert.AreEqual(2, recoveredValuesExpTime!.Length);
                Assert.That(expireTime.ToUnixTimeSeconds(), Is.EqualTo((long)recoveredValuesExpTime[0]).Within(1));
                ClassicAssert.AreEqual(-1, (long)recoveredValuesExpTime[1]);

                // Verify 2nd hash set contains all added entries except 1 expired entry
                recoveredValues = db.HashGetAll(key2);
                CollectionAssert.AreEquivalent(values2_1.Skip(1).Union(values2_2), recoveredValues);

                // Verify 2nd hash set entries ttls
                var recoveredValuesTtl = (RedisResult[])db.Execute("HPTTL", key2, "FIELDS", 4, "key2_1", "key2_2", "key2_3", "key2_4");
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
        public void CanDoHashExpireWithNonExistKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var result = db.Execute("HEXPIRE", "myhash", "3", "FIELDS", "1", "field1");
            var results = (RedisResult[])result;
            ClassicAssert.AreEqual(1, results.Length);
            ClassicAssert.AreEqual(-2, (long)results[0]);
        }

        [Test]
        public async Task CanDoHashCollect()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServers().First();
            db.HashSet("myhash", [new HashEntry("field1", "hello"), new HashEntry("field2", "world"), new HashEntry("field3", "value3"), new HashEntry("field4", "value4"), new HashEntry("field5", "value5"), new HashEntry("field6", "value6")]);

            var result = db.Execute("HPEXPIRE", "myhash", "500", "FIELDS", "2", "field1", "field2");
            var results = (RedisResult[])result;
            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);
            ClassicAssert.AreEqual(1, (long)results[1]);

            result = db.Execute("HPEXPIRE", "myhash", "1500", "FIELDS", "2", "field3", "field4");
            results = (RedisResult[])result;
            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);
            ClassicAssert.AreEqual(1, (long)results[1]);

            var orginalMemory = (long)db.Execute("MEMORY", "USAGE", "myhash");

            await Task.Delay(600);

            var newMemory = (long)db.Execute("MEMORY", "USAGE", "myhash");
            ClassicAssert.AreEqual(newMemory, orginalMemory);

            var collectResult = (string)db.Execute("HCOLLECT", "myhash");
            ClassicAssert.AreEqual("OK", collectResult);

            newMemory = (long)db.Execute("MEMORY", "USAGE", "myhash");
            ClassicAssert.Less(newMemory, orginalMemory);
            orginalMemory = newMemory;

            await Task.Delay(1100);

            newMemory = (long)db.Execute("MEMORY", "USAGE", "myhash");
            ClassicAssert.AreEqual(newMemory, orginalMemory);

            collectResult = (string)db.Execute("HCOLLECT", "*");
            ClassicAssert.AreEqual("OK", collectResult);

            newMemory = (long)db.Execute("MEMORY", "USAGE", "myhash");
            ClassicAssert.Less(newMemory, orginalMemory);
        }

        [Test]
        [TestCase("HEXPIRE", "NX", Description = "Set expiry only when no expiration exists")]
        [TestCase("HEXPIRE", "XX", Description = "Set expiry only when expiration exists")]
        [TestCase("HEXPIRE", "GT", Description = "Set expiry only when new TTL is greater")]
        [TestCase("HEXPIRE", "LT", Description = "Set expiry only when new TTL is less")]
        [TestCase("HPEXPIRE", "NX", Description = "Set expiry only when no expiration exists")]
        [TestCase("HPEXPIRE", "XX", Description = "Set expiry only when expiration exists")]
        [TestCase("HPEXPIRE", "GT", Description = "Set expiry only when new TTL is greater")]
        [TestCase("HPEXPIRE", "LT", Description = "Set expiry only when new TTL is less")]
        [TestCase("HEXPIREAT", "NX", Description = "Set expiry only when no expiration exists")]
        [TestCase("HEXPIREAT", "XX", Description = "Set expiry only when expiration exists")]
        [TestCase("HEXPIREAT", "GT", Description = "Set expiry only when new TTL is greater")]
        [TestCase("HEXPIREAT", "LT", Description = "Set expiry only when new TTL is less")]
        [TestCase("HPEXPIREAT", "NX", Description = "Set expiry only when no expiration exists")]
        [TestCase("HPEXPIREAT", "XX", Description = "Set expiry only when expiration exists")]
        [TestCase("HPEXPIREAT", "GT", Description = "Set expiry only when new TTL is greater")]
        [TestCase("HPEXPIREAT", "LT", Description = "Set expiry only when new TTL is less")]
        public void CanDoHashExpireWithOptions(string command, string option)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.HashSet("myhash", [new HashEntry("field1", "hello"), new HashEntry("field2", "world"), new HashEntry("field3", "welcome"), new HashEntry("field4", "back")]);

            (var expireTimeField1, var expireTimeField3, var newExpireTimeField) = command switch
            {
                "HEXPIRE" => ("2", "6", "4"),
                "HPEXPIRE" => ("2000", "6000", "4000"),
                "HEXPIREAT" => (DateTimeOffset.UtcNow.AddSeconds(2).ToUnixTimeSeconds().ToString(), DateTimeOffset.UtcNow.AddSeconds(6).ToUnixTimeSeconds().ToString(), DateTimeOffset.UtcNow.AddSeconds(4).ToUnixTimeSeconds().ToString()),
                "HPEXPIREAT" => (DateTimeOffset.UtcNow.AddSeconds(2).ToUnixTimeMilliseconds().ToString(), DateTimeOffset.UtcNow.AddSeconds(6).ToUnixTimeMilliseconds().ToString(), DateTimeOffset.UtcNow.AddSeconds(4).ToUnixTimeMilliseconds().ToString()),
                _ => throw new ArgumentException("Invalid command")
            };

            // First set TTL for field1 only
            db.Execute(command, "myhash", expireTimeField1, "FIELDS", "1", "field1");
            db.Execute(command, "myhash", expireTimeField3, "FIELDS", "1", "field3");

            // Try setting TTL with option
            var result = (RedisResult[])db.Execute(command, "myhash", newExpireTimeField, option, "FIELDS", "3", "field1", "field2", "field3");

            switch (option)
            {
                case "NX":
                    ClassicAssert.AreEqual(0, (long)result[0]); // field1 has TTL
                    ClassicAssert.AreEqual(1, (long)result[1]); // field2 no TTL
                    ClassicAssert.AreEqual(0, (long)result[2]); // field1 has TTL
                    break;
                case "XX":
                    ClassicAssert.AreEqual(1, (long)result[0]); // field1 has TTL
                    ClassicAssert.AreEqual(0, (long)result[1]); // field2 no TTL
                    ClassicAssert.AreEqual(1, (long)result[2]); // field1 has TTL
                    break;
                case "GT":
                    ClassicAssert.AreEqual(1, (long)result[0]); // 20 > 10
                    ClassicAssert.AreEqual(0, (long)result[1]); // no TTL = infinite
                    ClassicAssert.AreEqual(0, (long)result[2]); // 20 !> 30
                    break;
                case "LT":
                    ClassicAssert.AreEqual(0, (long)result[0]); // 20 !< 10
                    ClassicAssert.AreEqual(1, (long)result[1]); // no TTL = infinite
                    ClassicAssert.AreEqual(1, (long)result[2]); // 20 < 30
                    break;
            }
        }

        #endregion 

        #region LightClientTests


        /// <summary>
        /// HSET used explictly always returns the number of fields that were added.
        /// HSET can manage more than one field in the input
        /// </summary>
        [Test]
        [TestCase(50)]
        public void CanSetAndGetOnepairLC(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommandChunks("HSET myhash field1 myvalue", bytesSent);
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase(30)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanSetAndGetMultiplepairLC(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommandChunks("HSET myhash field1 field1value field2 field2value field3 field3value field4 field4value", bytesSent);
            var expectedResponse = ":4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":680\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // multiple get
            response = lightClientRequest.SendCommand("HMGET myhash field1 field2", 3);
            expectedResponse = "*2\r\n$11\r\nfield1value\r\n$11\r\nfield2value\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        public void CanSetAndGetMultiplepairandAllChunks(int bytesPerSend)
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommandChunks("HSET myhashone field1 field1value field2 field2value", bytesPerSend);
            var expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("HGET myhashone field1", bytesPerSend);
            expectedResponse = "$11\r\nfield1value\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("HSET myhash field1 field1value field2 field2value field3 field3value field4 field4value", bytesPerSend);
            expectedResponse = ":4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //get all keys
            response = lightClientRequest.SendCommandChunks("HGETALL myhash", bytesPerSend, 9);
            expectedResponse = "*8\r\n$6\r\nfield1\r\n$11\r\nfield1value\r\n$6\r\nfield2\r\n$11\r\nfield2value\r\n$6\r\nfield3\r\n$11\r\nfield3value\r\n$6\r\nfield4\r\n$11\r\nfield4value\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //get only keys
            response = lightClientRequest.SendCommandChunks("HKEYS myhash", bytesPerSend, 5);
            expectedResponse = "*4\r\n$6\r\nfield1\r\n$6\r\nfield2\r\n$6\r\nfield3\r\n$6\r\nfield4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanSetAndGetMultiplePairsSecondUseCaseLC()
        {
            // this UT shows hset when updating an existing value
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 field1value field2 field2value");
            var expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // only update one field
            lightClientRequest.SendCommand("HSET myhash field1 field1valueupdated");
            response = lightClientRequest.SendCommand("HGET myhash field1");
            expectedResponse = "$18\r\nfield1valueupdated\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDeleteOnepairLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 field1value field2 field2value");
            var expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":408\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("HDEL myhash field1");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":272\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //HDEL with nonexisting key
            response = lightClientRequest.SendCommand("HDEL foo bar");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoGetAllLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 field1value field2 field2value field3 field3value field4 field4value");
            var expectedResponse = ":4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //get all keys
            expectedResponse = "*8\r\n$6\r\nfield1\r\n$11\r\nfield1value\r\n$6\r\nfield2\r\n$11\r\nfield2value\r\n$6\r\nfield3\r\n$11\r\nfield3value\r\n$6\r\nfield4\r\n$11\r\nfield4value\r\n";
            response = lightClientRequest.SendCommand("HGETALL myhash", 9);
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoHExistsLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 myvalue");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // get an existing field
            response = lightClientRequest.SendCommand("HEXISTS myhash field1");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // get an nonexisting field
            response = lightClientRequest.SendCommand("HEXISTS myhash field0");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //non existing hash
            response = lightClientRequest.SendCommand("HEXISTS foo field0");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //missing paramenters
            response = lightClientRequest.SendCommand("HEXISTS foo");
            expectedResponse = FormatWrongNumOfArgsError("HEXISTS");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoHStrLenLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 myvalue");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // get an existing field
            response = lightClientRequest.SendCommand("HSTRLEN myhash field1");
            expectedResponse = ":7\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // get an nonexisting field
            response = lightClientRequest.SendCommand("HSTRLEN myhash field0");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //non existing hash
            response = lightClientRequest.SendCommand("HSTRLEN foo field0");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //missing paramenters
            response = lightClientRequest.SendCommand("HSTRLEN foo");
            expectedResponse = FormatWrongNumOfArgsError("HSTRLEN");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //too many paramenters
            response = lightClientRequest.SendCommand("HSTRLEN foo field0 field1");
            expectedResponse = FormatWrongNumOfArgsError("HSTRLEN");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoIncrByLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 1");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":264\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // do hincrby
            response = lightClientRequest.SendCommand("HINCRBY myhash field1 4");
            expectedResponse = ":5\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":264\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoIncrByFloatLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field 10.50");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":264\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("HINCRBYFLOAT myhash field 0.1");
            expectedResponse = "$4\r\n10.6\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":264\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // exponential notation
            response = lightClientRequest.SendCommand("HSET myhash field2 5.0e3");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":392\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommands("HINCRBYFLOAT myhash field2 2.0e2", "PING HELLO");
            expectedResponse = "$4\r\n5200\r\n$5\r\nHELLO\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":392\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoHRANDFIELDCommandLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HMSET coin heads obverse tails reverse edge null");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Check correct case when not an integer value in COUNT parameter
            response = lightClientRequest.SendCommand("HRANDFIELD coin A WITHVALUES");
            expectedResponse = "-ERR value is not an integer or out of range.\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Check correct error message when incorrect number of parameters
            response = lightClientRequest.SendCommand("HRANDFIELD");
            expectedResponse = FormatWrongNumOfArgsError("HRANDFIELD");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            for (var i = 0; i < 3; i++)
            {
                response = lightClientRequest.SendCommand("HRANDFIELD coin 3 WITHVALUES", 7);
                expectedResponse = "*6\r\n"; // 3 keyvalue pairs
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            }

            for (var i = 0; i < 3; i++)
            {
                string s = Encoding.ASCII.GetString(lightClientRequest.SendCommand("HRANDFIELD coin", 1));
                int startIndexField = s.IndexOf('\n') + 1;
                int endIndexField = s.IndexOf('\n', startIndexField) - 1;
                string fieldValue = s.Substring(startIndexField, endIndexField - startIndexField);
                var foundInSet = ("heads tails edge").IndexOf(fieldValue, StringComparison.InvariantCultureIgnoreCase);
                ClassicAssert.IsTrue(foundInSet >= 0);
            }

            for (int i = 0; i < 3; i++)
            {
                response = lightClientRequest.SendCommand("HRANDFIELD coin -5 WITHVALUES", 11);
                expectedResponse = "*10\r\n"; // 5 keyvalue pairs
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            }
        }

        [Test]
        public void CanDoKeyNotFoundCommandLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HMSET coin heads obverse tails reverse edge null");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommands("HGET coin nokey", "PING", 1, 1);
            expectedResponse = "$-1\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoNOTFOUNDSSKEYLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HGET foo bar", "PING", 1, 1);
            var expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }
        #endregion

        [Test]
        [TestCase(2, Description = "RESP2 output")]
        [TestCase(3, Description = "RESP3 output")]
        public async Task HRespOutput(byte respVersion)
        {
            using var c = TestUtils.GetGarnetClientSession(raw: true);
            c.Connect();

            var response = await c.ExecuteAsync("HELLO", respVersion.ToString());

            response = await c.ExecuteAsync("HSET", "h", "a", "0");
            ClassicAssert.AreEqual(":1\r\n", response);

            response = await c.ExecuteAsync("HGETALL", "h");
            if (respVersion >= 3)
                ClassicAssert.AreEqual("%1\r\n$1\r\na\r\n$1\r\n0\r\n", response);
            else
                ClassicAssert.AreEqual("*2\r\n$1\r\na\r\n$1\r\n0\r\n", response);

            response = await c.ExecuteAsync("HRANDFIELD", "h", "1", "WITHVALUES");
            if (respVersion >= 3)
                ClassicAssert.AreEqual("*1\r\n*2\r\n$1\r\na\r\n$1\r\n0\r\n", response);
            else
                ClassicAssert.AreEqual("*2\r\n$1\r\na\r\n$1\r\n0\r\n", response);
        }

        #region TxnTests

        [Test]
        public async Task CanFailWhenUseMultiWatchTest()
        {
            var lightClientRequest = TestUtils.CreateRequest();

            var key = "myhash";

            var response = lightClientRequest.SendCommand($"HSET {key} field1 1");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"WATCH {key}");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MULTI");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"HINCRBY {key} field1 2");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            await Task.Run(() => UpdateHashMap(key));

            response = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // This sequence should work
            response = lightClientRequest.SendCommand("MULTI");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"HSET {key} field2 2");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // This should commit
            response = lightClientRequest.SendCommand("EXEC", 2);
            expectedResponse = "*1\r\n:1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // HMSET within MULTI/EXEC
            response = lightClientRequest.SendCommand("MULTI");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"HMSET {key} field4 4");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // This should commit
            response = lightClientRequest.SendCommand("EXEC", 2);
            expectedResponse = "*1\r\n+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        private static void UpdateHashMap(string keyName)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand($"HSET {keyName} field3 3");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        #endregion

        #region NegativeTestsLC

        [Test]
        public void CanDoNotFoundSKeyInHRANDFIELDLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HRANDFIELD foo -5 WITHVALUES", "PING", 1, 1);
            var expectedResponse = "*0\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoWrongParametersNumberHRANDFIELDLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HRANDFIELD foo", "PING", 1, 1);
            var expectedResponse = "$-1\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoNOTFOUNDSKeyInHVALSLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HVALS foo", "PING HELLO", 1, 1);
            var expectedResponse = "*0\r\n$5\r\nHELLO\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoWrongNumOfParametersInHINCRBYLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HINCRBY foo", "PING HELLO", 1, 1);
            var expectedResponse = $"{FormatWrongNumOfArgsError("HINCRBY")}$5\r\nHELLO\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoWrongNumOfParametersInHINCRBYFLOATLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HINCRBYFLOAT foo", "PING HELLO", 1, 1);
            var expectedResponse = $"{FormatWrongNumOfArgsError("HINCRBYFLOAT")}$5\r\nHELLO\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanIncrementBeyond32bits()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand($"HSET myhash int64 {1L + int.MaxValue}");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("HINCRBY myhash int64 1");
            expectedResponse = $":{2L + int.MaxValue}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }
        #endregion

        private static string FormatWrongNumOfArgsError(string commandName) => $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, commandName)}\r\n";
    }

}