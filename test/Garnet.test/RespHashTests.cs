// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
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
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
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
        public void CanDoHIncrBy()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", [new HashEntry("Field1", "StringValue"), new HashEntry("Field2", "1")]);
            Assert.Throws<RedisServerException>(() => db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field1"), 4));
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
        public void CanDoRandomField()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
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
            try
            {
                db.Execute("HSCAN");
                Assert.Fail();
            }
            catch (RedisServerException e)
            {
                var expectedErrorMessage = string.Format(CmdStrings.GenericErrWrongNumArgs, nameof(HashOperation.HSCAN));
                ClassicAssert.AreEqual(expectedErrorMessage, e.Message);
            }

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
            db.HashSet("myhash", [new HashEntry("field1", "hello"), new HashEntry("field2", "world"), new HashEntry("field3", "new")]);

            var result = db.Execute("HEXPIRE", "myhash", "2", "FIELDS", "2", "field1", "field2");
            var results = (RedisResult[])result;
            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]); // field1 success
            ClassicAssert.AreEqual(1, (long)results[1]); // field2 success

            var ttl = (RedisResult[])db.Execute("HTTL", "myhash", "FIELDS", "2", "field1", "field2");
            ClassicAssert.AreEqual(2, ttl.Length);
            ClassicAssert.LessOrEqual((long)ttl[0], 10);
            ClassicAssert.Greater((long)ttl[0], 0);
            ClassicAssert.LessOrEqual((long)ttl[1], 10);
            ClassicAssert.Greater((long)ttl[1], 0);

            await Task.Delay(2000);

            var items = db.HashGetAll("myhash");
            ClassicAssert.AreEqual(1, items.Length);
            ClassicAssert.AreEqual("field3", items[0].Name.ToString());
            ClassicAssert.AreEqual("new", items[0].Value.ToString());
        }

        [Test]
        [TestCase("NX", Description = "Set expiry only when no expiration exists")]
        [TestCase("XX", Description = "Set expiry only when expiration exists")]
        [TestCase("GT", Description = "Set expiry only when new TTL is greater")]
        [TestCase("LT", Description = "Set expiry only when new TTL is less")]
        public void CanDoHashExpireWithOptions(string option)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            
            db.HashSet("myhash", [new HashEntry("field1", "hello"), new HashEntry("field2", "world")]);

            // First set TTL for field1 only
            db.Execute("HEXPIRE", "myhash", "20", "FIELDS", "1", "field1");

            // Try setting TTL with option
            var result = (RedisResult[])db.Execute("HEXPIRE", "myhash", "10", option, "FIELDS", "2", "field1", "field2");
            
            switch (option)
            {
                case "NX":
                    ClassicAssert.AreEqual(0, (long)result[0]); // field1 has TTL
                    ClassicAssert.AreEqual(1, (long)result[1]); // field2 no TTL
                    break;
                case "XX":
                    ClassicAssert.AreEqual(1, (long)result[0]); // field1 has TTL
                    ClassicAssert.AreEqual(0, (long)result[1]); // field2 no TTL
                    break;
                case "GT":
                    // TODO: add 3rd field to check valid greater than
                    ClassicAssert.AreEqual(0, (long)result[0]); // 10 < 20
                    ClassicAssert.AreEqual(0, (long)result[1]); // no TTL = infinite
                    break;
                case "LT":
                    ClassicAssert.AreEqual(1, (long)result[0]); // 10 < 20
                    ClassicAssert.AreEqual(1, (long)result[1]); // no TTL = infinite
                    break;
            }
        }

        [Test] 
        public void CanDoHashExpireAt()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            
            db.HashSet("myhash", [new HashEntry("field1", "hello"), new HashEntry("field2", "world")]);

            var futureTime = DateTimeOffset.UtcNow.AddSeconds(30).ToUnixTimeSeconds();
            var result = (RedisResult[])db.Execute("HEXPIREAT", "myhash", futureTime.ToString(), "FIELDS", "2", "field1", "field2");
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.AreEqual(1L, (long)result[0]);
            ClassicAssert.AreEqual(1L, (long)result[1]);

            var ttl = (RedisResult[])db.Execute("HTTL", "myhash", "FIELDS", "2", "field1", "field2");
            ClassicAssert.IsTrue((long)ttl[0] <= 30);
            ClassicAssert.IsTrue((long)ttl[1] <= 30);
        }

        [Test]
        public void CanDoHashPreciseExpire()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            
            db.HashSet("myhash", [new HashEntry("field1", "hello"), new HashEntry("field2", "world")]);

            var result = (RedisResult[])db.Execute("HPEXPIRE", "myhash", "1000", "FIELDS", "2", "field1", "field2");
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.AreEqual(1L, (long)result[0]);
            ClassicAssert.AreEqual(1L, (long)result[1]);

            var pttl = (RedisResult[])db.Execute("HPTTL", "myhash", "FIELDS", "2", "field1", "field2");
            ClassicAssert.IsTrue((long)pttl[0] <= 1000);
            ClassicAssert.IsTrue((long)pttl[1] <= 1000);
        }

        [Test]
        public void CanDoHashPreciseExpireAt()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            
            db.HashSet("myhash", [new HashEntry("field1", "hello"), new HashEntry("field2", "world")]);

            var futureTimeMs = DateTimeOffset.UtcNow.AddSeconds(30).ToUnixTimeMilliseconds();
            var result = (RedisResult[])db.Execute("HPEXPIREAT", "myhash", futureTimeMs.ToString(), "FIELDS", "2", "field1", "field2"); 
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.AreEqual(1L, (long)result[0]);
            ClassicAssert.AreEqual(1L, (long)result[1]);

            var pttl = (RedisResult[])db.Execute("HPTTL", "myhash", "FIELDS", "2", "field1", "field2");
            ClassicAssert.IsTrue((long)pttl[0] <= 30000);
            ClassicAssert.IsTrue((long)pttl[1] <= 30000);
        }

        [Test]
        public void TestHashExpireEdgeCases()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            
            // Test with non-existent key
            var result = (RedisResult[])db.Execute("HEXPIRE", "nonexistent", "10", "FIELDS", "1", "field1");
            ClassicAssert.AreEqual(1, result.Length);
            ClassicAssert.AreEqual(-2L, (long)result[0]); // Key doesn't exist

            // Test with non-existent fields
            db.HashSet("myhash", "field1", "hello");
            result = (RedisResult[])db.Execute("HEXPIRE", "myhash", "10", "FIELDS", "2", "field1", "nonexistent");
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.AreEqual(1L, (long)result[0]); // Existing field
            ClassicAssert.AreEqual(-2L, (long)result[1]); // Non-existent field

            // Test with zero TTL (should delete fields)
            result = (RedisResult[])db.Execute("HEXPIRE", "myhash", "0", "FIELDS", "1", "field1");
            ClassicAssert.AreEqual(1, result.Length);
            ClassicAssert.AreEqual(1L, (long)result[0]);
            ClassicAssert.IsFalse(db.HashExists("myhash", "field1"));

            // Test with negative TTL (should delete fields)
            db.HashSet("myhash", "field1", "hello");
            result = (RedisResult[])db.Execute("HEXPIRE", "myhash", "-1", "FIELDS", "1", "field1");
            ClassicAssert.AreEqual(1, result.Length);
            ClassicAssert.AreEqual(1L, (long)result[0]);
            ClassicAssert.IsFalse(db.HashExists("myhash", "field1"));
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
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
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
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":680\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // multiple get
            var result = lightClientRequest.SendCommand("HMGET myhash field1 field2", 3);
            expectedResponse = "*2\r\n$11\r\nfield1value\r\n$11\r\nfield2value\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        [TestCase(10)]
        [TestCase(50)]
        public void CanSetAndGetMultiplepairandAllChunks(int bytesPerSend)
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommandChunks("HSET myhashone field1 field1value field2 field2value", bytesPerSend);
            var expectedResponse = ":2\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var result = lightClientRequest.SendCommandChunks("HGET myhashone field1", bytesPerSend);
            expectedResponse = "$11\r\nfield1value\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("HSET myhash field1 field1value field2 field2value field3 field3value field4 field4value", bytesPerSend);
            expectedResponse = ":4\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //get all keys
            expectedResponse = "*8\r\n$6\r\nfield1\r\n$11\r\nfield1value\r\n$6\r\nfield2\r\n$11\r\nfield2value\r\n$6\r\nfield3\r\n$11\r\nfield3value\r\n$6\r\nfield4\r\n$11\r\nfield4value\r\n";
            result = lightClientRequest.SendCommandChunks("HGETALL myhash", bytesPerSend, 9);
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //get only keys
            expectedResponse = "*4\r\n$6\r\nfield1\r\n$6\r\nfield2\r\n$6\r\nfield3\r\n$6\r\nfield4\r\n";
            result = lightClientRequest.SendCommandChunks("HKEYS myhash", bytesPerSend, 5);
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);


        }


        [Test]
        public void CanSetAndGetMultiplePairsSecondUseCaseLC()
        {
            // this UT shows hset when updating an existing value
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 field1value field2 field2value");
            var expectedResponse = ":2\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // only update one field
            lightClientRequest.SendCommand("HSET myhash field1 field1valueupdated");
            var result = lightClientRequest.SendCommand("HGET myhash field1");
            expectedResponse = "$18\r\nfield1valueupdated\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDeleteOnepairLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 field1value field2 field2value");
            var expectedResponse = ":2\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":408\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var result = lightClientRequest.SendCommand("HDEL myhash field1");
            expectedResponse = ":1\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":272\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //HDEL with nonexisting key
            result = lightClientRequest.SendCommand("HDEL foo bar");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void CanDoGetAllLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 field1value field2 field2value field3 field3value field4 field4value");
            var expectedResponse = ":4\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
            //get all keys
            expectedResponse = "*8\r\n$6\r\nfield1\r\n$11\r\nfield1value\r\n$6\r\nfield2\r\n$11\r\nfield2value\r\n$6\r\nfield3\r\n$11\r\nfield3value\r\n$6\r\nfield4\r\n$11\r\nfield4value\r\n";
            var result = lightClientRequest.SendCommand("HGETALL myhash", 9);
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoHExistsLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 myvalue");
            var expectedResponse = ":1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // get an existing field
            response = lightClientRequest.SendCommand("HEXISTS myhash field1");
            expectedResponse = ":1\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // get an nonexisting field
            response = lightClientRequest.SendCommand("HEXISTS myhash field0");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //non existing hash
            response = lightClientRequest.SendCommand("HEXISTS foo field0");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //missing paramenters
            response = lightClientRequest.SendCommand("HEXISTS foo");
            expectedResponse = FormatWrongNumOfArgsError("HEXISTS");
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoHStrLenLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 myvalue");
            var expectedResponse = ":1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // get an existing field
            response = lightClientRequest.SendCommand("HSTRLEN myhash field1");
            expectedResponse = ":7\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // get an nonexisting field
            response = lightClientRequest.SendCommand("HSTRLEN myhash field0");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //non existing hash
            response = lightClientRequest.SendCommand("HSTRLEN foo field0");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //missing paramenters
            response = lightClientRequest.SendCommand("HSTRLEN foo");
            expectedResponse = FormatWrongNumOfArgsError("HSTRLEN");
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //too many paramenters
            response = lightClientRequest.SendCommand("HSTRLEN foo field0 field1");
            expectedResponse = FormatWrongNumOfArgsError("HSTRLEN");
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoIncrByLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 1");
            var expectedResponse = ":1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":264\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // do hincrby
            response = lightClientRequest.SendCommand("HINCRBY myhash field1 4");
            expectedResponse = ":5\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":264\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void CanDoIncrByFloatLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field 10.50");
            var expectedResponse = ":1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":264\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("HINCRBYFLOAT myhash field 0.1");
            expectedResponse = "$4\r\n10.6\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":264\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // exponential notation
            response = lightClientRequest.SendCommand("HSET myhash field2 5.0e3");
            expectedResponse = ":1\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":392\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommands("HINCRBYFLOAT myhash field2 2.0e2", "PING HELLO");
            expectedResponse = "$4\r\n5200\r\n$5\r\nHELLO\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":392\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoHRANDFIELDCommandLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HMSET coin heads obverse tails reverse edge null");
            var expectedResponse = "+OK\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // Check correct case when not an integer value in COUNT parameter
            response = lightClientRequest.SendCommand("HRANDFIELD coin A WITHVALUES");
            expectedResponse = "-ERR value is not an integer or out of range.\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // Check correct error message when incorrect number of parameters
            response = lightClientRequest.SendCommand("HRANDFIELD");
            expectedResponse = FormatWrongNumOfArgsError("HRANDFIELD");
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);


            for (int i = 0; i < 3; i++)
            {
                response = lightClientRequest.SendCommand("HRANDFIELD coin 3 WITHVALUES", 7);
                expectedResponse = "*6\r\n"; // 3 keyvalue pairs
                actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
                ClassicAssert.AreEqual(expectedResponse, actualValue);
            }

            for (int i = 0; i < 3; i++)
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
                actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
                ClassicAssert.AreEqual(expectedResponse, actualValue);
            }
        }

        [Test]
        public void CanDoKeyNotFoundCommandLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HMSET coin heads obverse tails reverse edge null");
            var expectedResponse = "+OK\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommands("HGET coin nokey", "PING", 1, 1);
            expectedResponse = "$-1\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoNOTFOUNDSSKEYLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HGET foo bar", "PING", 1, 1);
            var expectedResponse = "$-1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        #endregion


        #region TxnTests

        [Test]
        public async Task CanFailWhenUseMultiWatchTest()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            byte[] res;

            string key = "myhash";

            res = lightClientRequest.SendCommand($"HSET {key} field1 1");
            string expectedResponse = ":1\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand($"WATCH {key}");
            expectedResponse = "+OK\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("MULTI");
            expectedResponse = "+OK\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand($"HINCRBY {key} field1 2");
            expectedResponse = "+QUEUED\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            await Task.Run(() => UpdateHashMap(key));

            res = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // This sequence should work
            res = lightClientRequest.SendCommand("MULTI");
            expectedResponse = "+OK\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand($"HSET {key} field2 2");
            expectedResponse = "+QUEUED\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // This should commit
            res = lightClientRequest.SendCommand("EXEC", 2);
            expectedResponse = "*1\r\n:1\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // HMSET within MULTI/EXEC
            res = lightClientRequest.SendCommand("MULTI");
            expectedResponse = "+OK\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand($"HMSET {key} field4 4");
            expectedResponse = "+QUEUED\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // This should commit
            res = lightClientRequest.SendCommand("EXEC", 2);
            expectedResponse = "*1\r\n+OK\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
        }


        private static void UpdateHashMap(string keyName)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            byte[] res = lightClientRequest.SendCommand($"HSET {keyName} field3 3");
            string expectedResponse = ":1\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
        }

        #endregion

        #region NegativeTestsLC

        [Test]
        public void CanDoNotFoundSKeyInHRANDFIELDLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HRANDFIELD foo -5 WITHVALUES", "PING", 1, 1);
            var expectedResponse = "*0\r\n+PONG\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoWrongParametersNumberHRANDFIELDLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HRANDFIELD foo", "PING", 1, 1);
            var expectedResponse = "$-1\r\n+PONG\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoNOTFOUNDSKeyInHVALSLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HVALS foo", "PING HELLO", 1, 1);
            var expectedResponse = "*0\r\n$5\r\nHELLO\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoWrongNumOfParametersInHINCRBYLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HINCRBY foo", "PING HELLO", 1, 1);
            var expectedResponse = $"{FormatWrongNumOfArgsError("HINCRBY")}$5\r\nHELLO\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoWrongNumOfParametersInHINCRBYFLOATLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HINCRBYFLOAT foo", "PING HELLO", 1, 1);
            var expectedResponse = $"{FormatWrongNumOfArgsError("HINCRBYFLOAT")}$5\r\nHELLO\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        #endregion

        private static string FormatWrongNumOfArgsError(string commandName) => $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, commandName)}\r\n";
    }

}