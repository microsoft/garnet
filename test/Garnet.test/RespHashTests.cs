// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
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
        public void CanSetAndGetOnePair()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Title", "Tsavorite") });
            string r = db.HashGet("user:user1", "Title");
            Assert.AreEqual("Tsavorite", r);
        }

        [Test]
        public void CanSetAndGetOnePairLarge()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var str = new string(new char[150000]);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Title", str) });
            string r = db.HashGet("user:user1", "Title");
            Assert.AreEqual(str, r);
            string r2 = db.HashGet("user:user1", "Title2");
            Assert.AreEqual(null, r2);
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
            Assert.AreEqual("9800", r);
        }


        [Test]
        public void CanSetAndGetMultiplePairs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021") });
            var result = db.HashGet("user:user1", new RedisValue[] { new RedisValue("Title"), new RedisValue("Year") });
            Assert.AreEqual(2, result.Length);
            Assert.AreEqual("Tsavorite", result[0].ToString());
            Assert.AreEqual("2021", result[1].ToString());
        }



        [Test]
        public void CanDelSingleField()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021") });
            var result = db.HashDelete(new RedisKey("user:user1"), new RedisValue("Title"));
            Assert.AreEqual(true, result);
            string resultGet = db.HashGet("user:user1", "Year");
            Assert.AreEqual("2021", resultGet);
        }


        [Test]
        public void CanDeleleteMultipleFields()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Example", "One") });
            var result = db.HashDelete(new RedisKey("user:user1"), new RedisValue[] { new RedisValue("Title"), new RedisValue("Year") });
            string resultGet = db.HashGet("user:user1", "Example");
            Assert.AreEqual("One", resultGet);
        }

        [Test]
        public void CanDeleleteMultipleFieldsWithNonExistingField()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021") });
            var result = db.HashDelete(new RedisKey("user:user1"), new RedisValue[] { new RedisValue("Title"), new RedisValue("Year"), new RedisValue("Unknown") });
            Assert.AreEqual(2, result);
        }

        [Test]
        public void CanDoHLen()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme") });
            var result = db.HashLength("user:user1");
            Assert.AreEqual(3, result);
        }

        [Test]
        public void CanDoGetAll()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme") });
            HashEntry[] result = db.HashGetAll("user:user1");
            Assert.AreEqual(3, result.Length);
        }


        [Test]
        public void CanDoHExists()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme") });
            var result = db.HashExists(new RedisKey("user:user1"), new RedisValue("Company"));
            Assert.AreEqual(true, result);

            result = db.HashExists(new RedisKey("user:userfoo"), "Title");
            Assert.AreEqual(false, result);
        }


        [Test]
        public void CanDoHKeys()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme") });
            var result = db.HashKeys("user:user1");
            Assert.AreEqual(3, result.Length);

            Assert.IsTrue(Array.Exists(result, t => t.Equals("Title")));
            Assert.IsTrue(Array.Exists(result, t => t.Equals("Year")));
            Assert.IsTrue(Array.Exists(result, t => t.Equals("Company")));
        }


        [Test]
        public void CanDoHVals()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Title", "Tsavorite"), new HashEntry("Year", "2021"), new HashEntry("Company", "Acme") });
            var result = db.HashValues("user:user1");
            Assert.AreEqual(3, result.Length);

            Assert.IsTrue(Array.Exists(result, t => t.Equals("Tsavorite")));
            Assert.IsTrue(Array.Exists(result, t => t.Equals("2021")));
            Assert.IsTrue(Array.Exists(result, t => t.Equals("Acme")));
        }


        [Test]
        public void CanDoHIncrBy()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Field1", "StringValue"), new HashEntry("Field2", "1") });
            Assert.Throws<RedisServerException>(() => db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field1"), 4));
            var result = db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field2"), -4);
            Assert.AreEqual(-3, result);
            //new Key
            result = db.HashIncrement(new RedisKey("user:user2"), new RedisValue("Field2"), 4);
            Assert.AreEqual(4, result);
            // make sure the new hash object was created
            var getResult = db.HashGet("user:user2", "Field2");
            Assert.AreEqual(4, ((int?)getResult));
        }

        [Test]
        public void CanDoHIncrByLTM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create LTM (larger than memory) DB by inserting 100 keys
            for (int i = 0; i < 100; i++)
                db.HashSet("user:user" + i, new HashEntry[] { new HashEntry("Field1", "StringValue"), new HashEntry("Field2", "1") });

            Assert.Throws<RedisServerException>(() => db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field1"), 4));
            var result = db.HashIncrement(new RedisKey("user:user1"), new RedisValue("Field2"), -4);
            Assert.AreEqual(-3, result);

            // Test new key
            result = db.HashIncrement(new RedisKey("user:user100"), new RedisValue("Field2"), 4);
            Assert.AreEqual(4, result);
            // make sure the new hash object was created
            var getResult = db.HashGet("user:user100", "Field2");
            Assert.AreEqual(4, ((int?)getResult));
        }

        [Test]
        public void CanDoHashDecrement()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet("user:user1", new HashEntry[] { new HashEntry("Field1", "StringValue"), new HashEntry("Field2", "1") });
            var result = db.HashDecrement(new RedisKey("user:user1"), new RedisValue("Field2"), 4);
            Assert.AreEqual(-3, result);
        }

        [Test]
        public void CanDoHSETNXCommand()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.HashSet(new RedisKey("user:user1"), new RedisValue("Field"), new RedisValue("Hello"), When.NotExists);
            string result = db.HashGet("user:user1", "Field");
            Assert.AreEqual("Hello", result);
            db.HashSet(new RedisKey("user:user1"), new RedisValue("Field"), new RedisValue("World"), When.NotExists);
            result = db.HashGet("user:user1", "Field");
            Assert.AreEqual("Hello", result);
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

            Assert.AreEqual(RedisValue.Null, singleField);
            Assert.IsEmpty(multiFields);
            Assert.IsEmpty(withValues);
        }

        [Test]
        public void CanDoHashScan()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // HSCAN non existing key
            var members = db.HashScan("foo");
            Assert.IsTrue(((IScanningCursor)members).Cursor == 0);
            Assert.IsTrue(members.Count() == 0, "HSCAN non existing key failed.");

            db.HashSet("user:user1", new HashEntry[] { new HashEntry("name", "Alice"), new HashEntry("email", "email@example.com"), new HashEntry("age", "30") });

            // HSCAN without parameters
            members = db.HashScan("user:user1");
            Assert.IsTrue(((IScanningCursor)members).Cursor == 0);
            Assert.IsTrue(members.Count() == 3, "HSCAN without MATCH failed.");

            db.HashSet("user:user789", new HashEntry[] { new HashEntry("email", "email@example.com"), new HashEntry("email1", "email1@example.com"), new HashEntry("email2", "email2@example.com"), new HashEntry("email3", "email3@example.com"), new HashEntry("age", "25") });

            // HSCAN with match
            members = db.HashScan("user:user789", "email*");
            Assert.IsTrue(((IScanningCursor)members).Cursor == 0);
            Assert.IsTrue(members.Count() == 4, "HSCAN with MATCH failed.");

            members = db.HashScan("user:user789", "age", 5000);
            Assert.IsTrue(((IScanningCursor)members).Cursor == 0);
            Assert.IsTrue(members.Count() == 1, "HSCAN with MATCH failed.");

            members = db.HashScan("user:user789", "*");
            Assert.IsTrue(members.Count() == 5, "HSCAN with MATCH failed.");
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
            Assert.AreEqual(hashEntries.Length, j);

            // Assert the cursor is at the end of the enumeration
            Assert.AreEqual(pageNumber + pageOffset, hashEntries.Length - 1);

            var l = response.LastOrDefault();
            Assert.AreEqual(l.Name, $"key{hashEntries.Length - 1}");
        }

        [Test]
        public async Task CanDoHMGET()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var hashkey = "testGetMulti";

            db.KeyDelete(hashkey, CommandFlags.FireAndForget);

            RedisValue[] fields = { "foo", "bar", "blop" };
            var arr0 = await db.HashGetAsync(hashkey, fields);

            db.HashSet(hashkey, "foo", "abc", flags: CommandFlags.FireAndForget);
            db.HashSet(hashkey, "bar", "def", flags: CommandFlags.FireAndForget);

            var arr1 = await db.HashGetAsync(hashkey, fields);
            var arr2 = await db.HashGetAsync(hashkey, fields);

            Assert.AreEqual(3, arr0.Length);
#nullable enable
            Assert.Null((string?)arr0[0]);
            Assert.Null((string?)arr0[1]);
            Assert.Null((string?)arr0[2]);

            Assert.AreEqual(3, arr1.Length);
            Assert.AreEqual("abc", (string?)arr1[0]);
            Assert.AreEqual("def", (string?)arr1[1]);
            Assert.Null((string?)arr1[2]);

            Assert.AreEqual(3, arr2.Length);
            Assert.AreEqual("abc", (string?)arr2[0]);
            Assert.AreEqual("def", (string?)arr2[1]);
            Assert.Null((string?)arr2[2]);
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

            Assert.IsEmpty(redis.Wait(result0));
            var result = redis.Wait(result1).ToStringDictionary();
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("abc", result["foo"]);
            Assert.AreEqual("def", result["bar"]);
        }

        [Test]
        public async Task CanDoHSETWhenAlwaysAsync()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var hashkey = "testWhenAlways";

            db.KeyDelete(hashkey, CommandFlags.FireAndForget);

            var result1 = await db.HashSetAsync(hashkey, "foo", "bar", When.Always, CommandFlags.None);
            var result2 = await db.HashSetAsync(hashkey, "foo2", "bar", When.Always, CommandFlags.None);
            var result3 = await db.HashSetAsync(hashkey, "foo", "bar", When.Always, CommandFlags.None);
            var result4 = await db.HashSetAsync(hashkey, "foo", "bar2", When.Always, CommandFlags.None);

            Assert.True(result1, "Initial set key 1");
            Assert.True(result2, "Initial set key 2");

            // Fields modified *but not added* should be a zero/false.
            Assert.False(result3, "Duplicate set key 1");
            Assert.False(result4, "Duplicate se key 1 variant");
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

            Assert.IsFalse(del);
#nullable enable
            Assert.Null((string?)val0);
            Assert.True(set0);
            Assert.AreEqual("value1", (string?)val1);
            Assert.False(set1);
            Assert.AreEqual("value1", (string?)val2);

            Assert.True(set2);
            Assert.AreEqual("value3", (string?)val3);
            Assert.False(set3);
#nullable disable

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
            Assert.AreEqual(expectedResponse, actualValue);
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
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":680\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            // multiple get
            var result = lightClientRequest.SendCommand("HMGET myhash field1 field2", 3);
            expectedResponse = "*2\r\n$11\r\nfield1value\r\n$11\r\nfield2value\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
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
            Assert.AreEqual(expectedResponse, actualValue);

            var result = lightClientRequest.SendCommandChunks("HGET myhashone field1", bytesPerSend);
            expectedResponse = "$11\r\nfield1value\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommandChunks("HSET myhash field1 field1value field2 field2value field3 field3value field4 field4value", bytesPerSend);
            expectedResponse = ":4\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            //get all keys
            expectedResponse = "*8\r\n$6\r\nfield1\r\n$11\r\nfield1value\r\n$6\r\nfield2\r\n$11\r\nfield2value\r\n$6\r\nfield3\r\n$11\r\nfield3value\r\n$6\r\nfield4\r\n$11\r\nfield4value\r\n";
            result = lightClientRequest.SendCommandChunks("HGETALL myhash", bytesPerSend, 9);
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            //get only keys
            expectedResponse = "*4\r\n$6\r\nfield1\r\n$6\r\nfield2\r\n$6\r\nfield3\r\n$6\r\nfield4\r\n";
            result = lightClientRequest.SendCommandChunks("HKEYS myhash", bytesPerSend, 5);
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);


        }


        [Test]
        public void CanSetAndGetMultiplePairsSecondUseCaseLC()
        {
            // this UT shows hset when updating an existing value
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 field1value field2 field2value");
            var expectedResponse = ":2\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            // only update one field
            lightClientRequest.SendCommand("HSET myhash field1 field1valueupdated");
            var result = lightClientRequest.SendCommand("HGET myhash field1");
            expectedResponse = "$18\r\nfield1valueupdated\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDeleteOnepairLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 field1value field2 field2value");
            var expectedResponse = ":2\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":408\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            var result = lightClientRequest.SendCommand("HDEL myhash field1");
            expectedResponse = ":1\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":272\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            //HDEL with nonexisting key
            result = lightClientRequest.SendCommand("HDEL foo bar");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void CanDoGetAllLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 field1value field2 field2value field3 field3value field4 field4value");
            var expectedResponse = ":4\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
            //get all keys
            expectedResponse = "*8\r\n$6\r\nfield1\r\n$11\r\nfield1value\r\n$6\r\nfield2\r\n$11\r\nfield2value\r\n$6\r\nfield3\r\n$11\r\nfield3value\r\n$6\r\nfield4\r\n$11\r\nfield4value\r\n";
            var result = lightClientRequest.SendCommand("HGETALL myhash", 9);
            actualValue = Encoding.ASCII.GetString(result).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoHExistsLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 myvalue");
            var expectedResponse = ":1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            // get an existing field
            response = lightClientRequest.SendCommand("HEXISTS myhash field1");
            expectedResponse = ":1\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            // get an nonexisting field
            response = lightClientRequest.SendCommand("HEXISTS myhash field0");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            //non existing hash
            response = lightClientRequest.SendCommand("HEXISTS foo field0");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            //missing paramenters
            response = lightClientRequest.SendCommand("HEXISTS foo");
            expectedResponse = "-ERR wrong number of arguments for HEXISTS command.\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void CanDoIncrByLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field1 1");
            var expectedResponse = ":1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":264\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            // do hincrby
            response = lightClientRequest.SendCommand("HINCRBY myhash field1 4");
            expectedResponse = ":5\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":264\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void CanDoIncrByFloatLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash field 10.50");
            var expectedResponse = ":1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":264\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("HINCRBYFLOAT myhash field 0.1");
            expectedResponse = "$4\r\n10.6\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":264\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            // exponential notation
            response = lightClientRequest.SendCommand("HSET myhash field2 5.0e3");
            expectedResponse = ":1\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":392\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommands("HINCRBYFLOAT myhash field2 2.0e2", "PING HELLO");
            expectedResponse = "$4\r\n5200\r\n$5\r\nHELLO\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand("MEMORY USAGE myhash");
            expectedResponse = ":392\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoHRANDFIELDCommandLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HMSET coin heads obverse tails reverse edge null");
            var expectedResponse = "+OK\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            // Check correct case when not an integer value in COUNT parameter
            response = lightClientRequest.SendCommand("HRANDFIELD coin A WITHVALUES");
            expectedResponse = "-ERR value is not an integer or out of range.\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            // Check correct error message when incorrect number of parameters
            response = lightClientRequest.SendCommand("HRANDFIELD");
            expectedResponse = "-ERR wrong number of arguments for HRANDFIELD command.";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);


            for (int i = 0; i < 3; i++)
            {
                response = lightClientRequest.SendCommand("HRANDFIELD coin 3 WITHVALUES", 7);
                expectedResponse = "*6\r\n"; // 3 keyvalue pairs
                actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
                Assert.AreEqual(expectedResponse, actualValue);
            }

            for (int i = 0; i < 3; i++)
            {
                string s = Encoding.ASCII.GetString(lightClientRequest.SendCommand("HRANDFIELD coin", 1));
                int startIndexField = s.IndexOf('\n') + 1;
                int endIndexField = s.IndexOf('\n', startIndexField) - 1;
                string fieldValue = s.Substring(startIndexField, endIndexField - startIndexField);
                var foundInSet = ("heads tails edge").IndexOf(fieldValue, StringComparison.InvariantCultureIgnoreCase);
                Assert.IsTrue(foundInSet >= 0);
            }

            for (int i = 0; i < 3; i++)
            {
                response = lightClientRequest.SendCommand("HRANDFIELD coin -5 WITHVALUES", 11);
                expectedResponse = "*10\r\n"; // 5 keyvalue pairs
                actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
                Assert.AreEqual(expectedResponse, actualValue);
            }
        }

        [Test]
        public void CanDoKeyNotFoundCommandLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HMSET coin heads obverse tails reverse edge null");
            var expectedResponse = "+OK\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommands("HGET coin nokey", "PING", 1, 1);
            expectedResponse = "$-1\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoNOTFOUNDSSKEYLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HGET foo bar", "PING", 1, 1);
            var expectedResponse = "$-1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
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
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand($"WATCH {key}");
            expectedResponse = "+OK\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("MULTI");
            expectedResponse = "+OK\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand($"HINCRBY {key} field1 2");
            expectedResponse = "+QUEUED\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            await Task.Run(() => UpdateHashMap(key));

            res = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "$-1";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // This sequence should work
            res = lightClientRequest.SendCommand("MULTI");
            expectedResponse = "+OK\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand($"HSET {key} field2 2");
            expectedResponse = "+QUEUED\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // This should commit
            res = lightClientRequest.SendCommand("EXEC", 2);
            expectedResponse = "*1\r\n:1\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
        }


        private void UpdateHashMap(string keyName)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            byte[] res = lightClientRequest.SendCommand($"HSET {keyName} field3 3");
            string expectedResponse = ":1\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
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
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoWrongParametersNumberHRANDFIELDLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HRANDFIELD foo", "PING", 1, 1);
            var expectedResponse = "$-1\r\n+PONG\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoNOTFOUNDSKeyInHVALSLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HVALS foo", "PING HELLO", 1, 1);
            var expectedResponse = "*0\r\n$5\r\nHELLO\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoWrongNumOfParametersInHINCRBYLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HINCRBY foo", "PING HELLO", 1, 1);
            var expectedResponse = "-ERR wrong number of arguments for HINCRBY command.\r\n$5\r\nHELLO\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoWrongNumOfParametersInHINCRBYFLOATLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("HINCRBYFLOAT foo", "PING HELLO", 1, 1);
            var expectedResponse = "-ERR wrong number of arguments for HINCRBYFLOAT command.\r\n$5\r\nHELLO\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }

        #endregion

    }

}