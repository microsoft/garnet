// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using SetOperation = StackExchange.Redis.SetOperation;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class RespSetTest : AllureTestBase
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        #region SEClientTests
        [Test]
        [TestCase("")]
        [TestCase("myset")]
        public void CandDoSaddBasic(string key)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var result = db.SetAdd(key, "Hello");
            ClassicAssert.IsTrue(result);

            result = db.SetAdd(key, "World");
            ClassicAssert.IsTrue(result);

            result = db.SetAdd(key, "World");
            ClassicAssert.IsFalse(result);

            var emptySetKey = $"{key}_empty";
            var added = db.SetAdd(key, []);
            ClassicAssert.AreEqual(0, added);

            result = db.KeyExists(emptySetKey);
            ClassicAssert.IsFalse(result);
        }

        [Test]
        public void CheckEmptySetKeyRemoved()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var key = new RedisKey("user1:set");
            var db = redis.GetDatabase(0);
            var members = new[] { new RedisValue("Hello"), new RedisValue("World") };
            var result = db.SetAdd(key, members);
            ClassicAssert.AreEqual(2, result);

            var actualMembers = db.SetPop(key, 2);
            ClassicAssert.AreEqual(members.Length, actualMembers.Length);

            var keyExists = db.KeyExists(key);
            ClassicAssert.IsFalse(keyExists);
        }

        [Test]
        public void CanAddAndListMembers()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var result = db.SetAdd(new RedisKey("user1:set"), ["Hello", "World", "World"]);
            ClassicAssert.AreEqual(2, result);

            var members = db.SetMembers(new RedisKey("user1:set"));
            ClassicAssert.AreEqual(2, members.Length);

            var response = db.Execute("MEMORY", "USAGE", "user1:set");
            var actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            var expectedResponse = 312;
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanCheckIfMemberExistsInSet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = new RedisKey("user1:set");

            db.KeyDelete(key);

            db.SetAdd(key, ["Hello", "World"]);

            var existingMemberExists = db.SetContains(key, "Hello");
            ClassicAssert.IsTrue(existingMemberExists);

            var nonExistingMemberExists = db.SetContains(key, "NonExistingMember");
            ClassicAssert.IsFalse(nonExistingMemberExists);

            var setDoesNotExist = db.SetContains("NonExistingSet", "AnyMember");
            ClassicAssert.IsFalse(setDoesNotExist);
        }

        [Test]
        public void CanAddAndGetAllMembersWithPendingStatus()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var nVals = 100;
            RedisValue[] values = new RedisValue[nVals];
            for (int i = 0; i < 100; i++)
            {
                values[i] = ($"val-{i + 1}");
            }

            for (int j = 0; j < 25; j++)
            {
                var nAdded = db.SetAdd($"Set_Test-{j + 1}", values);
                ClassicAssert.AreEqual(nVals, nAdded);
            }

            var members = db.SetMembers(new RedisKey("Set_Test-10"));
            ClassicAssert.AreEqual(100, members.Length);
        }

        [Test]
        public void CanReturnEmptySet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            _ = db.SetMembers(new RedisKey("myset"));

            var response = db.Execute("MEMORY", "USAGE", "myset");
            var actualValue = ResultType.Integer == response.Resp2Type ? int.Parse(response.ToString()) : -1;
            var expectedResponse = -1;
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanDoMembersWhenEmptyKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var empty = "";

            var addResult = db.SetAdd(empty, ["one", "two", "three", "four", "five"]);
            ClassicAssert.AreEqual(5, addResult);

            var result = db.SetMembers(empty);
            ClassicAssert.AreEqual(5, result.Length);
            var strResult = result.Select(r => r.ToString());
            var expectedResult = new[] { "one", "two", "three", "four", "five" };
            ClassicAssert.IsTrue(expectedResult.OrderBy(t => t).SequenceEqual(strResult.OrderBy(t => t)));
        }

        [Test]
        public void CanRemoveField()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = new RedisKey("user1:set");
            var result = db.SetAdd(key, ["ItemOne", "ItemTwo", "ItemThree", "ItemFour"]);
            ClassicAssert.AreEqual(4, result);

            var existingMemberExists = db.SetContains(key, "ItemOne");
            ClassicAssert.IsTrue(existingMemberExists, "Existing member 'ItemOne' does not exist in the set.");

            var memresponse = db.Execute("MEMORY", "USAGE", "user1:set");
            var actualValue = ResultType.Integer == memresponse.Resp2Type ? Int32.Parse(memresponse.ToString()) : -1;
            var expectedResponse = 464;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var response = db.SetRemove(key, new RedisValue("ItemOne"));
            ClassicAssert.AreEqual(true, response);

            memresponse = db.Execute("MEMORY", "USAGE", "user1:set");
            actualValue = ResultType.Integer == memresponse.Resp2Type ? Int32.Parse(memresponse.ToString()) : -1;
            expectedResponse = 392;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = db.SetRemove(key, new RedisValue("ItemFive"));
            ClassicAssert.AreEqual(false, response);

            memresponse = db.Execute("MEMORY", "USAGE", "user1:set");
            actualValue = ResultType.Integer == memresponse.Resp2Type ? Int32.Parse(memresponse.ToString()) : -1;
            expectedResponse = 392;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var longResponse = db.SetRemove(key, ["ItemTwo", "ItemThree"]);
            ClassicAssert.AreEqual(2, longResponse);

            memresponse = db.Execute("MEMORY", "USAGE", "user1:set");
            actualValue = ResultType.Integer == memresponse.Resp2Type ? Int32.Parse(memresponse.ToString()) : -1;
            expectedResponse = 240;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var members = db.SetMembers(key);
            ClassicAssert.AreEqual(1, members.Length);

            response = db.SetRemove(key, new RedisValue("ItemFour"));
            ClassicAssert.IsTrue(response);

            var exists = db.KeyExists(key);
            ClassicAssert.IsFalse(exists);
        }

        [Test]
        public void CanUseSScanNoParameters()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // SSCAN without key
            var e = Assert.Throws<RedisServerException>(() => db.Execute("SSCAN"));
            var expectedErrorMessage = string.Format(CmdStrings.GenericErrWrongNumArgs, nameof(Garnet.server.SetOperation.SSCAN));
            ClassicAssert.AreEqual(expectedErrorMessage, e.Message);

            // Use setscan on non existing key
            var items = db.SetScan(new RedisKey("foo"), new RedisValue("*"), pageSize: 10);
            ClassicAssert.IsEmpty(items, "Failed to use SetScan on non existing key");

            RedisValue[] entries = ["item-a", "item-b", "item-c", "item-d", "item-e", "item-aaa"];

            // Add some items
            var added = db.SetAdd("myset", entries);
            ClassicAssert.AreEqual(entries.Length, added);

            var members = db.SetScan(new RedisKey("myset"), new RedisValue("*"));
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsTrue(members.Count() == entries.Length);

            int i = 0;
            foreach (var item in members)
            {
                ClassicAssert.IsTrue(entries[i++].Equals(item));
            }

            // No matching elements
            members = db.SetScan(new RedisKey("myset"), new RedisValue("x"));
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsEmpty(members);
        }

        [Test]
        public void CanUseSScanWithMatch()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add some items
            var added = db.SetAdd("myset", ["aa", "bb", "cc", "dd", "ee", "aaf"]);
            ClassicAssert.AreEqual(6, added);

            var members = db.SetScan(new RedisKey("myset"), new RedisValue("*aa"));
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsTrue(members.Count() == 1);
            ClassicAssert.IsTrue(members.ElementAt(0).Equals("aa"));
        }

        [Test]
        public void CanUseSScanWithCollection()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "myset";
            // Add some items
            var r = new Random();

            // Fill a new Set with 1000 random items 
            int n = 1000;
            var entries = new RedisValue[n];

            for (int i = 0; i < n; i++)
            {
                var memberId = r.Next(0, 10000000);
                entries[i] = new RedisValue($"member:{memberId}");
            }

            var setLen = db.SetAdd(key, entries);
            var members = db.SetScan(key, new RedisValue("member:*"), (Int32)setLen);
            ClassicAssert.IsTrue(((IScanningCursor)members).Cursor == 0);
            ClassicAssert.IsTrue(members.Count() == setLen);
        }

        [Test]
        public void CanDoSScanWithCursor()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myset";

            // create a new array of Set
            var setEntries = new RedisValue[1000];
            for (int i = 0; i < setEntries.Length; i++)
            {
                setEntries[i] = new RedisValue("value:" + i);
            }

            // set with items
            db.SetAdd(key, setEntries);

            int pageSize = 40;
            var response = db.SetScan(key, "*", pageSize: pageSize, cursor: 0);
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
            ClassicAssert.AreEqual(setEntries.Length, j);

            // Assert the cursor is at the end of the enumeration
            ClassicAssert.AreEqual(pageNumber + pageOffset, setEntries.Length - 1);

            var l = response.LastOrDefault();
            ClassicAssert.AreEqual(l, $"value:{setEntries.Length - 1}");
        }

        [Test]
        public void CanDoSetUnion()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var redisValues1 = new RedisValue[] { "item-a", "item-b", "item-c", "item-d" };
            var result = db.SetAdd(new RedisKey("key1"), redisValues1);
            ClassicAssert.AreEqual(4, result);

            result = db.SetAdd(new RedisKey("key2"), ["item-c"]);
            ClassicAssert.AreEqual(1, result);

            result = db.SetAdd(new RedisKey("key3"), ["item-a", "item-c", "item-e"]);
            ClassicAssert.AreEqual(3, result);

            var members = db.SetCombine(SetOperation.Union, ["key1", "key2", "key3"]);
            RedisValue[] entries = ["item-a", "item-b", "item-c", "item-d", "item-e"];
            ClassicAssert.AreEqual(5, members.Length);
            // assert two arrays are equal ignoring order
            ClassicAssert.IsTrue(members.OrderBy(x => x).SequenceEqual(entries.OrderBy(x => x)));

            members = db.SetCombine(SetOperation.Union, ["key1", "key2", "key3", "_not_exists"]);
            ClassicAssert.AreEqual(5, members.Length);
            ClassicAssert.IsTrue(members.OrderBy(x => x).SequenceEqual(entries.OrderBy(x => x)));

            members = db.SetCombine(SetOperation.Union, ["_not_exists_1", "_not_exists_2", "_not_exists_3"]);
            ClassicAssert.IsEmpty(members);

            members = db.SetCombine(SetOperation.Union, ["_not_exists_1", "key1", "_not_exists_2", "_not_exists_3"]);
            ClassicAssert.AreEqual(4, members.Length);
            ClassicAssert.IsTrue(members.OrderBy(x => x).SequenceEqual(redisValues1.OrderBy(x => x)));

            members = db.SetCombine(SetOperation.Union, ["key1", "key2"]);
            ClassicAssert.AreEqual(4, members.Length);
            ClassicAssert.IsTrue(members.OrderBy(x => x).SequenceEqual(redisValues1.OrderBy(x => x)));

            var e = Assert.Throws<RedisServerException>(() => db.SetCombine(SetOperation.Union, []));
            ClassicAssert.AreEqual(string.Format(CmdStrings.GenericErrWrongNumArgs, "SUNION"), e.Message);
        }

        [Test]
        [TestCase("key")]
        [TestCase("")]
        public void CanDoSetUnionStore(string key)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key1 = "key1";
            var key1Value = new RedisValue[] { "a", "b", "c" };

            var key2 = "key2";
            var key2Value = new RedisValue[] { "c", "d", "e" };

            var key3 = "key3";
            var key3Value = new RedisValue[] { };

            var key4 = "key4";
            var key4Value = new RedisValue[] { };

            var addResult = db.SetAdd(key1, key1Value);
            ClassicAssert.AreEqual(3, addResult);
            addResult = db.SetAdd(key2, key2Value);
            ClassicAssert.AreEqual(3, addResult);
            addResult = db.SetAdd(key3, key3Value);
            ClassicAssert.AreEqual(0, addResult);
            addResult = db.SetAdd(key4, key4Value);
            ClassicAssert.AreEqual(0, addResult);

            var result = db.SetCombineAndStore(SetOperation.Union, key, key1, key2);
            ClassicAssert.AreEqual(5, result);

            var membersResult = db.SetMembers(key);
            ClassicAssert.AreEqual(5, membersResult.Length);
            var strResult = membersResult.Select(m => m.ToString()).ToArray();
            var expectedResult = new[] { "a", "b", "c", "d", "e" };
            ClassicAssert.IsTrue(expectedResult.OrderBy(t => t).SequenceEqual(strResult.OrderBy(t => t)));

            result = db.SetCombineAndStore(SetOperation.Union, key, key3, key4);
            ClassicAssert.AreEqual(0, result);

            var exists = db.KeyExists(key);
            ClassicAssert.IsFalse(exists);
        }


        [Test]
        public void CanDoSetInter()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var redisValues1 = new RedisValue[] { "item-a", "item-b", "item-c", "item-d" };
            var result = db.SetAdd(new RedisKey("key1"), redisValues1);
            ClassicAssert.AreEqual(4, result);

            result = db.SetAdd(new RedisKey("key2"), ["item-c"]);
            ClassicAssert.AreEqual(1, result);

            result = db.SetAdd(new RedisKey("key3"), ["item-a", "item-c", "item-e"]);
            ClassicAssert.AreEqual(3, result);

            var members = db.SetCombine(SetOperation.Intersect, ["key1", "key2", "key3"]);
            RedisValue[] entries = ["item-c"];
            ClassicAssert.AreEqual(1, members.Length);
            // assert two arrays are equal ignoring order
            ClassicAssert.IsTrue(members.OrderBy(x => x).SequenceEqual(entries.OrderBy(x => x)));

            members = db.SetCombine(SetOperation.Intersect, ["key1", "key2", "key3", "_not_exists"]);
            ClassicAssert.IsEmpty(members);

            members = db.SetCombine(SetOperation.Intersect, ["_not_exists_1", "_not_exists_2", "_not_exists_3"]);
            ClassicAssert.IsEmpty(members);


            var e = Assert.Throws<RedisServerException>(() => db.SetCombine(SetOperation.Intersect, []));
            ClassicAssert.AreEqual(string.Format(CmdStrings.GenericErrWrongNumArgs, "SINTER"), e.Message);
        }

        [Test]
        public void CanDoSetInterStore()
        {
            string key = "key";
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key1 = "key1";
            var key1Value = new RedisValue[] { "a", "b", "c" };

            var key2 = "key2";
            var key2Value = new RedisValue[] { "c", "d", "e" };

            var key3 = "key3";
            var key3Value = new RedisValue[] { "d", "e" };

            var addResult = db.SetAdd(key1, key1Value);
            ClassicAssert.AreEqual(key1Value.Length, addResult);
            addResult = db.SetAdd(key2, key2Value);
            ClassicAssert.AreEqual(key2Value.Length, addResult);
            addResult = db.SetAdd(key3, key3Value);
            ClassicAssert.AreEqual(key3Value.Length, addResult);

            var result = db.SetCombineAndStore(SetOperation.Intersect, key, key1, key2);
            ClassicAssert.AreEqual(1, result);

            var membersResult = db.SetMembers(key);
            ClassicAssert.AreEqual(1, membersResult.Length);
            var strResult = membersResult.Select(m => m.ToString()).ToArray();
            var expectedResult = new[] { "c" };
            ClassicAssert.IsTrue(expectedResult.SequenceEqual(strResult));

            result = db.SetCombineAndStore(SetOperation.Intersect, key, key1, key3);
            ClassicAssert.AreEqual(0, result);

            var exists = db.KeyExists(key);
            ClassicAssert.IsFalse(exists);
        }


        [Test]
        [TestCase("key1", "key2")]
        [TestCase("", "key2")]
        [TestCase("key1", "")]
        public void CanDoSdiff(string key1, string key2)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key1Value = new RedisValue[] { "a", "b", "c", "d" };

            var key2Value = new RedisValue[] { "c" };

            var addResult = db.SetAdd(key1, key1Value);
            ClassicAssert.AreEqual(4, addResult);
            addResult = db.SetAdd(key2, key2Value);
            ClassicAssert.AreEqual(1, addResult);

            var result = db.SetCombine(SetOperation.Difference, key1, key2);
            ClassicAssert.AreEqual(3, result.Length);
            var strResult = result.Select(r => r.ToString()).ToArray();
            var expectedResult = new[] { "a", "b", "d" };
            ClassicAssert.IsTrue(expectedResult.OrderBy(t => t).SequenceEqual(strResult.OrderBy(t => t)));
            ClassicAssert.IsFalse(strResult.Contains("c"));

            var key3 = "key3";
            var key3Value = new RedisValue[] { "a", "c", "e" };

            addResult = db.SetAdd(key3, key3Value);
            ClassicAssert.AreEqual(3, addResult);

            result = db.SetCombine(SetOperation.Difference, [new RedisKey(key1), new RedisKey(key2), new RedisKey(key3)]);
            ClassicAssert.AreEqual(2, result.Length);
            strResult = [.. result.Select(r => r.ToString())];
            expectedResult = ["b", "d"];
            ClassicAssert.IsTrue(expectedResult.OrderBy(t => t).SequenceEqual(strResult.OrderBy(t => t)));

            ClassicAssert.IsFalse(strResult.Contains("c"));
            ClassicAssert.IsFalse(strResult.Contains("e"));
        }

        [Test]
        public void CanDoSdiffStoreOverwrittenKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "key";

            var key1 = "key1";
            var key1Value = new RedisValue[] { "a", "b", "c", "d" };

            var key2 = "key2";
            var key2Value = new RedisValue[] { "c" };

            var addResult = db.SetAdd(key1, key1Value);
            ClassicAssert.AreEqual(4, addResult);
            addResult = db.SetAdd(key2, key2Value);
            ClassicAssert.AreEqual(1, addResult);

            var result = db.SetCombineAndStore(SetOperation.Difference, key, key1, key2);
            ClassicAssert.AreEqual(3, int.Parse(result.ToString()));

            var membersResult = db.SetMembers("key");
            ClassicAssert.AreEqual(3, membersResult.Length);
            var strResult = membersResult.Select(m => m.ToString()).ToArray();
            var expectedResult = new[] { "a", "b", "d" };
            ClassicAssert.IsTrue(expectedResult.OrderBy(t => t).SequenceEqual(strResult.OrderBy(t => t)));
            ClassicAssert.IsFalse(Array.Exists(membersResult, t => t.ToString().Equals("c")));

            var key3 = "key3";
            var key3Value = new RedisValue[] { "a", "b", "c" };
            var key4 = "key4";
            var key4Value = new RedisValue[] { "a", "b" };

            addResult = db.SetAdd(key3, key3Value);
            ClassicAssert.AreEqual(3, addResult);
            addResult = db.SetAdd(key4, key4Value);
            ClassicAssert.AreEqual(2, addResult);

            result = db.SetCombineAndStore(SetOperation.Difference, key, key3, key4);
            ClassicAssert.AreEqual(1, (int)result);

            membersResult = db.SetMembers("key");
            ClassicAssert.AreEqual(1, membersResult.Length);
            ClassicAssert.IsTrue(Array.Exists(membersResult, t => t.ToString().Equals("c")));

            var key5 = "key5";
            var key5Value = new RedisValue[] { "a", "b", "c" };

            addResult = db.SetAdd(key5, key5Value);
            ClassicAssert.AreEqual(3, addResult);

            result = db.SetCombineAndStore(SetOperation.Difference, key, key3, key5);
            ClassicAssert.AreEqual(0, (int)result);

            var exists = db.KeyExists(key);
            ClassicAssert.IsFalse(exists);
        }

        [Test]
        [TestCase("myset", "myotherset")]
        [TestCase("", "myotherset")]
        [TestCase("myset", "")]
        public void CanDoSmoveBasic(string source, string destination)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var addResult = db.SetAdd(source, ["one"]);
            ClassicAssert.AreEqual(1, addResult);
            addResult = db.SetAdd(source, ["two"]);
            ClassicAssert.AreEqual(1, addResult);

            addResult = db.SetAdd(destination, ["three"]);
            ClassicAssert.AreEqual(1, addResult);

            var result = db.SetMove(source, destination, "two");
            ClassicAssert.IsTrue(result);

            var membersResult = db.SetMembers(source);
            ClassicAssert.AreEqual(1, membersResult.Length);

            var strResult = membersResult.Select(r => r.ToString()).ToArray();
            var expectedResult = new[] { "one" };
            ClassicAssert.IsTrue(expectedResult.OrderBy(t => t).SequenceEqual(strResult.OrderBy(t => t)));

            membersResult = db.SetMembers(destination);
            strResult = [.. membersResult.Select(r => r.ToString())];
            expectedResult = ["three", "two"];
            ClassicAssert.IsTrue(expectedResult.OrderBy(t => t).SequenceEqual(strResult.OrderBy(t => t)));

            result = db.SetMove(source, destination, "one");
            ClassicAssert.IsTrue(result);

            var exists = db.KeyExists(source);
            ClassicAssert.IsFalse(exists);

            membersResult = db.SetMembers(destination);
            strResult = [.. membersResult.Select(r => r.ToString())];
            expectedResult = ["three", "two", "one"];
            ClassicAssert.IsTrue(expectedResult.OrderBy(t => t).SequenceEqual(strResult.OrderBy(t => t)));
        }

        [Test]
        public void CanDoSRANDMEMBERWithCountCommandSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = new RedisKey("myset");
            var values = new HashSet<RedisValue> { new("one"), new("two"), new("three"), new("four"), new("five") };

            // Check SRANDMEMBER with non-existing key
            var member = db.SetRandomMember(key);
            ClassicAssert.IsTrue(member.IsNull);

            // Check SRANDMEMBER with non-existing key and count
            var members = db.SetRandomMembers(key, 3);
            ClassicAssert.IsEmpty(members);

            // Check ZRANDMEMBER with wrong number of arguments
            var ex = Assert.Throws<RedisServerException>(() => db.Execute("SRANDMEMBER", key, 3, "bla"));
            var expectedMessage = string.Format(CmdStrings.GenericErrWrongNumArgs, nameof(RespCommand.SRANDMEMBER));
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedMessage, ex.Message);

            // Check SRANDMEMBER with non-numeric count
            ex = Assert.Throws<RedisServerException>(() => db.Execute("SRANDMEMBER", key, "bla"));
            expectedMessage = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedMessage, ex.Message);

            // Add items to set
            var added = db.SetAdd(key, [.. values]);
            ClassicAssert.AreEqual(values.Count, added);

            // Check SRANDMEMBER without count
            member = db.SetRandomMember(key);
            ClassicAssert.IsTrue(values.Contains(member));

            // Check SRANDMEMBER with positive count (distinct)
            members = db.SetRandomMembers(key, 3);
            ClassicAssert.AreEqual(3, members.Length);
            ClassicAssert.AreEqual(3, members.Distinct().Count());
            ClassicAssert.IsTrue(members.All(values.Contains));

            // Check SRANDMEMBER with positive count (distinct) larger than set cardinality
            members = db.SetRandomMembers(key, 6);
            ClassicAssert.AreEqual(values.Count, members.Length);
            ClassicAssert.AreEqual(values.Count, members.Distinct().Count());
            ClassicAssert.IsTrue(members.All(values.Contains));

            // Check SRANDMEMBER with negative count (non-distinct)
            members = db.SetRandomMembers(key, -6);
            ClassicAssert.AreEqual(6, members.Length);
            ClassicAssert.GreaterOrEqual(values.Count, members.Distinct().Count());
            ClassicAssert.IsTrue(members.All(values.Contains));
        }

        [Test]
        [TestCase("1,2,3", "2,3,4", 2, null, Description = "Basic intersection")]
        [TestCase("1,2,3", "", 0, null, Description = "Intersection with empty set")]
        [TestCase("1,2,3", "4,5,6", 0, null, Description = "No intersection")]
        [TestCase("1,1,1", "1,1,1", 1, null, Description = "Sets with duplicate values")]
        [TestCase("", "", 0, null, Description = "Both sets empty")]
        [TestCase("1,2,3,4,5", "2,3,4,5,6", 1, "1", Description = "Basic intersection with limit")]
        [TestCase("1,2,3", "2,3,4", 2, "5", Description = "Limit greater than intersection")]
        [TestCase("1,2,3,4,5", "2,3,4,5,6", 4, "0", Description = "Zero limit")]
        public void CanDoSinterCard(string values1, string values2, long expectedCount, string limit)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add values to first set
            foreach (var value in values1.Split(',', StringSplitOptions.RemoveEmptyEntries))
            {
                db.SetAdd("key1", value);
            }

            // Add values to second set
            foreach (var value in values2.Split(',', StringSplitOptions.RemoveEmptyEntries))
            {
                db.SetAdd("key2", value);
            }

            var result = limit == null ?
                (long)db.Execute("SINTERCARD", 2, "key1", "key2") :
                (long)db.Execute("SINTERCARD", 2, "key1", "key2", "LIMIT", limit);

            ClassicAssert.AreEqual(expectedCount, result);

            // Test with non-existing keys
            result = (long)db.Execute("SINTERCARD", 2, "nonexistent1", "nonexistent2");
            ClassicAssert.AreEqual(0, result);
        }

        [Test]
        public void CanDoSinterCardThowsErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("SINTERCARD", 2, "key1"));

            ex = Assert.Throws<RedisServerException>(() => db.Execute("SINTERCARD", 2, "key1", "key2", "LIMIT"));

            ex = Assert.Throws<RedisServerException>(() => db.Execute("SINTERCARD", 2, "key1", "key2", "LIMIT", "not_a_number"));
        }

        [Test]
        public void SInterWithFirstKeyNotExisting()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SetAdd("set2",
            [
                new RedisValue("one"),
                new RedisValue("two"),
                new RedisValue("four")
            ]);

            var result = db.SetCombine(SetOperation.Intersect, [new RedisKey("nonexistentkey"), new RedisKey("set2")]);
            ClassicAssert.AreEqual(0, result.Length);
        }

        [Test]
        public void SInterStoreWithFirstKeyNotExisting()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SetAdd("set2",
            [
                new RedisValue("one"),
                new RedisValue("two"),
                new RedisValue("four")
            ]);

            var result = db.SetCombineAndStore(SetOperation.Intersect, "dest", [new RedisKey("nonexistentkey"), new RedisKey("set2")]);
            ClassicAssert.AreEqual(0, result);

            var exists = db.KeyExists("dest");
            ClassicAssert.IsFalse(exists);
        }

        [Test]
        public void SDiffWithFirstKeyNotExisting()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SetAdd("set2",
            [
                new RedisValue("one"),
                new RedisValue("two"),
                new RedisValue("four")
            ]);

            var diff = db.SetCombine(SetOperation.Difference, [new RedisKey("nonexistentkey"), new RedisKey("set2")]);
            ClassicAssert.AreEqual(0, diff.Length);
        }

        [Test]
        public void SDiffStoreWithFirstKeyNotExisting()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SetAdd("set2",
            [
                new RedisValue("one"),
                new RedisValue("two"),
                new RedisValue("four")
            ]);
            db.SetAdd("dest",
            [
                new RedisValue("existing")
            ]);

            var result = db.SetCombineAndStore(SetOperation.Difference, "dest", [new RedisKey("nonexistentkey"), new RedisKey("set2")]);
            ClassicAssert.AreEqual(0, result);
            var exists = db.KeyExists("dest");
            ClassicAssert.IsFalse(exists);
        }

        [Test]
        public void SUnionWithFirstKeyNotExisting()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SetAdd("set2",
            [
                new RedisValue("one"),
                new RedisValue("two"),
                new RedisValue("four")
            ]);

            var result = db.SetCombine(SetOperation.Union, [new RedisKey("nonexistentkey"), new RedisKey("set2")]);
            ClassicAssert.AreEqual(3, result.Length);
            ClassicAssert.IsTrue(result.Contains("one"));
            ClassicAssert.IsTrue(result.Contains("two"));
            ClassicAssert.IsTrue(result.Contains("four"));
        }

        [Test]
        public void SUnionStoreWithFirstKeyNotExisting()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SetAdd("set2",
            [
                new RedisValue("one"),
                new RedisValue("two"),
                new RedisValue("four")
            ]);
            db.SetAdd("dest",
            [
                new RedisValue("existing")
            ]);

            var result = db.SetCombineAndStore(SetOperation.Union, "dest", [new RedisKey("nonexistentkey"), new RedisKey("set2")]);
            ClassicAssert.AreEqual(3, result);
            var members = db.SetMembers("dest");
            ClassicAssert.AreEqual(3, members.Length);
            ClassicAssert.IsTrue(members.Contains("one"));
            ClassicAssert.IsTrue(members.Contains("two"));
            ClassicAssert.IsTrue(members.Contains("four"));
        }

        #endregion


        #region LightClientTests


        [Test]
        public void CanAddAndListMembersLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SADD myset \"Hello\"");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SADD myset \"World\"");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            expectedResponse = ":0\r\n";
            response = lightClientRequest.SendCommand("SADD myset \"World\"");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SMEMBERS myset", 3);
            expectedResponse = "*2\r\n$7\r\n\"Hello\"\r\n$7\r\n\"World\"\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanCheckIfMemberExistsInSetLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand("SADD myset \"Hello\"");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SADD myset \"World\"");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SISMEMBER myset \"Hello\"");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SISMEMBER myset \"NonExistingMember\"");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SISMEMBER NonExistingSet \"AnyMember\"");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Missing arguments
            response = lightClientRequest.SendCommand("SISMEMBER myset");
            expectedResponse = $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, "SISMEMBER")}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Extra arguments
            response = lightClientRequest.SendCommand("SISMEMBER myset \"Hello\" \"ExtraArg\"");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSCARDCommandLC()
        {
            CreateSet();
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SCARD myset");
            var expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanReturnEmptySetLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SMEMBERS otherset", 1);

            // Empty array
            var expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSREMLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SADD myset ItemOne ItemTwo ItemThree ItemFour");
            var expectedResponse = ":4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SREM myset World");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SREM myset ItemOne");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SREM myset ItemTwo ItemThree");
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSCARDCommandsLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("SCARD fooset", "PING", 1, 1);
            var expectedResponse = ":0\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSRANDMEMBERWithCountCommandLC()
        {
            var myset = new HashSet<string> { "one", "two", "three", "four", "five" };

            // Check SRANDMEMBER with non-existing key
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SRANDMEMBER myset");
            var expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Check SRANDMEMBER with non-existing key and count
            response = lightClientRequest.SendCommand("SRANDMEMBER myset 3");
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            CreateLongSet();

            response = lightClientRequest.SendCommand("SRANDMEMBER myset", 1);
            var strLen = Encoding.ASCII.GetString(response).Substring(1, 1);
            var item = Encoding.ASCII.GetString(response).Substring(4, Int32.Parse(strLen));
            ClassicAssert.IsTrue(myset.Contains(item));

            // Get three random members
            response = lightClientRequest.SendCommand("SRANDMEMBER myset 3", 3);
            TestUtils.AssertEqualUpToExpectedLength("*", response);

            var strResponse = Encoding.ASCII.GetString(response);
            var arrLenEndIdx = strResponse.IndexOf("\r\n", StringComparison.InvariantCultureIgnoreCase);
            ClassicAssert.IsTrue(arrLenEndIdx > 1);

            var strArrLen = strResponse.AsSpan().Slice(1, arrLenEndIdx - 1);
            ClassicAssert.IsTrue(int.TryParse(strArrLen, out var arrLen));
            ClassicAssert.AreEqual(3, arrLen);

            // Get 6 random members and verify that at least two elements are the same
            response = lightClientRequest.SendCommand("SRANDMEMBER myset -6", 6);
            var strReponse = Encoding.ASCII.GetString(response);
            arrLenEndIdx = strReponse.IndexOf("\r\n", StringComparison.InvariantCultureIgnoreCase);
            strArrLen = strReponse.AsSpan().Slice(1, arrLenEndIdx - 1);
            ClassicAssert.IsTrue(int.TryParse(strArrLen, out arrLen));

            var members = new HashSet<string>();
            var repeatedMembers = false;
            for (var i = 0; i < arrLen; i++)
            {
                var member = strReponse.Substring(arrLenEndIdx + 2, response.Length - arrLenEndIdx - 5);
                if (members.Contains(member))
                {
                    repeatedMembers = true;
                    break;
                }
                members.Add(member);
            }

            ClassicAssert.IsTrue(repeatedMembers, "At least two members are repeated.");
        }

        [Test]
        public void CanDoSPOPCommandLC()
        {
            var myset = new HashSet<string>
            {
                "one",
                "two",
                "three",
                "four",
                "five"
            };

            CreateLongSet();

            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SPOP myset");
            var strLen = Encoding.ASCII.GetString(response, 1, 1);
            var item = Encoding.ASCII.GetString(response, 4, int.Parse(strLen));
            ClassicAssert.IsTrue(myset.Contains(item));

            response = lightClientRequest.SendCommand("SCARD myset");
            var expectedResponse = ":4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSPOPWithCountCommandLC()
        {
            CreateLongSet();

            var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SPOP myset 3", 3);
            TestUtils.AssertEqualUpToExpectedLength("*", response);

            var strResponse = Encoding.ASCII.GetString(response);
            var arrLenEndIdx = strResponse.IndexOf("\r\n", StringComparison.InvariantCultureIgnoreCase);
            ClassicAssert.IsTrue(arrLenEndIdx > 1);

            var strArrLen = strResponse.AsSpan().Slice(1, arrLenEndIdx - 1);
            ClassicAssert.IsTrue(int.TryParse(strArrLen, out var arrLen));
            ClassicAssert.AreEqual(3, arrLen);

            response = lightClientRequest.SendCommands("SCARD myset", "PING", 1, 1);
            var expectedResponse = ":2\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Test for popping set until empty
            response = lightClientRequest.SendCommand("SPOP myset 2", 2);
            TestUtils.AssertEqualUpToExpectedLength("*", response);

            strResponse = Encoding.ASCII.GetString(response);
            arrLenEndIdx = strResponse.IndexOf("\r\n", StringComparison.InvariantCultureIgnoreCase);
            ClassicAssert.IsTrue(arrLenEndIdx > 1);

            strArrLen = strResponse.AsSpan().Slice(1, arrLenEndIdx - 1);
            ClassicAssert.IsTrue(int.TryParse(strArrLen, out arrLen));
            ClassicAssert.AreEqual(2, arrLen);
        }

        [Test]
        public void CanDoSPOPWithMoreCountThanSetSizeCommandLC()
        {
            CreateLongSet();

            var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand("SPOP myset 10", 5);
            TestUtils.AssertEqualUpToExpectedLength("*", response);

            var strResponse = Encoding.ASCII.GetString(response);
            var arrLenEndIdx = strResponse.IndexOf("\r\n", StringComparison.InvariantCultureIgnoreCase);
            ClassicAssert.IsTrue(arrLenEndIdx > 1);

            var strArrLen = strResponse.AsSpan().Slice(1, arrLenEndIdx - 1);
            ClassicAssert.IsTrue(int.TryParse(strArrLen, out var arrLen));
            ClassicAssert.IsTrue(arrLen == 5);

            var lightClientRequest2 = TestUtils.CreateRequest();
            response = lightClientRequest2.SendCommand("SADD myset one");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest2.SendCommand("SCARD myset");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSMOVECommandLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            // source set
            lightClientRequest.SendCommand("SADD \"mySourceSet\" \"oneS\"");
            lightClientRequest.SendCommand("SADD \"mySourceSet\" \"twoS\"");
            lightClientRequest.SendCommand("SADD \"mySourceSet\" \"threeS\"");
            lightClientRequest.SendCommand("SADD \"mySourceSet\" \"fourS\"");
            lightClientRequest.SendCommand("SADD \"mySourceSet\" \"common\"");

            // destination set
            lightClientRequest.SendCommand("SADD \"myDestinationSet\" \"oneD\"");
            lightClientRequest.SendCommand("SADD \"myDestinationSet\" \"twoD\"");
            lightClientRequest.SendCommand("SADD \"myDestinationSet\" \"threeD\"");
            lightClientRequest.SendCommand("SADD \"myDestinationSet\" \"fourD\"");
            lightClientRequest.SendCommand("SADD \"myDestinationSet\" \"common\"");

            var expectedSuccessfulResponse = ":1\r\n";
            var expectedFailureResponse = ":0\r\n";

            // Successful move
            var response = lightClientRequest.SendCommand("SMOVE \"mySourceSet\" \"myDestinationSet\" \"oneS\"");
            TestUtils.AssertEqualUpToExpectedLength(expectedSuccessfulResponse, response);

            // Source set contains member
            response = lightClientRequest.SendCommand("SISMEMBER \"mySourceSet\" \"oneS\"");
            TestUtils.AssertEqualUpToExpectedLength(expectedFailureResponse, response);

            // Destination set contains member
            response = lightClientRequest.SendCommand("SISMEMBER \"myDestinationSet\" \"oneS\"");
            TestUtils.AssertEqualUpToExpectedLength(expectedSuccessfulResponse, response);

            // Source set doesn't exist
            response = lightClientRequest.SendCommand("SMOVE \"someRandomSet\" \"mySourceSet\" \"twoS\"");
            TestUtils.AssertEqualUpToExpectedLength(expectedFailureResponse, response);

            // Destination set doesn't exist
            response = lightClientRequest.SendCommand("SMOVE \"mySourceSet\" \"someRandomSet\" \"twoS\"");
            TestUtils.AssertEqualUpToExpectedLength(expectedSuccessfulResponse, response);

            // Value not in source
            response = lightClientRequest.SendCommand("SMOVE \"mySourceSet\" \"mySourceSet\" \"notAValue\"");
            TestUtils.AssertEqualUpToExpectedLength(expectedFailureResponse, response);

            // Move into self
            response = lightClientRequest.SendCommand("SMOVE \"mySourceSet\" \"mySourceSet\" \"twoS\"");
            TestUtils.AssertEqualUpToExpectedLength(expectedFailureResponse, response);

            // Common value
            response = lightClientRequest.SendCommand("SMOVE \"mySourceSet\" \"myDestinationSet\" \"common\"");
            TestUtils.AssertEqualUpToExpectedLength(expectedSuccessfulResponse, response);

            // Source set contains member
            response = lightClientRequest.SendCommand("SISMEMBER \"mySourceSet\" \"common\"");
            TestUtils.AssertEqualUpToExpectedLength(expectedFailureResponse, response);

            // Destination set contains member
            response = lightClientRequest.SendCommand("SISMEMBER \"myDestinationSet\" \"common\"");
            TestUtils.AssertEqualUpToExpectedLength(expectedSuccessfulResponse, response);
        }

        [Test]
        public async Task CanDoSMOVECommandGC()
        {
            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            //If set doesn't exist, then return 0.
            var response = await db.ExecuteForLongResultAsync("SMOVE", ["sourceSet", "destinationSet", "value"]);
            ClassicAssert.AreEqual(response, 0);
            await db.ExecuteForStringResultAsync("SADD", ["sourceSet", "sourceValue", "commonValue"]);
            await db.ExecuteForStringResultAsync("SADD", ["destinationSet", "destinationValue", "commonValue"]);

            //Same key.
            response = await db.ExecuteForLongResultAsync("SMOVE", ["sourceSet", "sourceSet", "sourceValue"]);
            ClassicAssert.AreEqual(response, 0);

            //Move non-common member.
            response = await db.ExecuteForLongResultAsync("SMOVE", ["sourceSet", "destinationSet", "sourceValue"]);
            ClassicAssert.AreEqual(response, 1);
            ClassicAssert.AreEqual(await db.ExecuteForLongResultAsync("SCARD", ["sourceSet"]), 1);
            ClassicAssert.AreEqual(await db.ExecuteForLongResultAsync("SCARD", ["destinationSet"]), 3);

            var sourceSetMembers = await db.ExecuteForStringArrayResultAsync("SMEMBERS", ["sourceSet"]);
            var destinationSetMembers = await db.ExecuteForStringArrayResultAsync("SMEMBERS", ["destinationSet"]);
            ClassicAssert.IsFalse(sourceSetMembers.Contains("sourceValue"));
            ClassicAssert.IsTrue(destinationSetMembers.Contains("sourceValue"));

            //Move common member.
            response = await db.ExecuteForLongResultAsync("SMOVE", ["sourceSet", "destinationSet", "commonValue"]);
            ClassicAssert.AreEqual(response, 1);
            ClassicAssert.AreEqual(await db.ExecuteForLongResultAsync("SCARD", ["sourceSet"]), 0);
            ClassicAssert.AreEqual(await db.ExecuteForLongResultAsync("SCARD", ["destinationSet"]), 3);

            sourceSetMembers = await db.ExecuteForStringArrayResultAsync("SMEMBERS", ["sourceSet"]);
            destinationSetMembers = await db.ExecuteForStringArrayResultAsync("SMEMBERS", ["destinationSet"]);
            ClassicAssert.IsFalse(sourceSetMembers.Contains("commonValue"));
            ClassicAssert.IsTrue(destinationSetMembers.Contains("commonValue"));
        }

        [Test]
        public void MultiWithNonExistingSet()
        {
            var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand("MULTI");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            //create set
            response = lightClientRequest.SendCommand("SADD MySet ItemOne");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("EXEC", 2);
            expectedResponse = "*1\r\n:1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SMEMBERS MySet", 2);
            expectedResponse = "*1\r\n$7\r\nItemOne\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSetUnionLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SADD myset ItemOne ItemTwo ItemThree ItemFour");
            var expectedResponse = ":4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SUNION myset another_set", 5);
            expectedResponse = "*4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            lightClientRequest.SendCommand("SADD another_set ItemOne ItemFive ItemTwo ItemSix ItemSeven");
            response = lightClientRequest.SendCommand("SUNION myset another_set", 8);
            expectedResponse = "*7\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SUNION myset no_exist_set", 5);
            expectedResponse = "*4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SUNION no_exist_set myset no_exist_set another_set", 8);
            expectedResponse = "*7\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SUNION myset", 5);
            expectedResponse = "*4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SUNION");
            expectedResponse = $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, "SUNION")}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSunionStoreLC()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            _ = lightClientRequest.SendCommand("SADD key1 a b c");
            _ = lightClientRequest.SendCommand("SADD key2 c d e");
            var response = lightClientRequest.SendCommand("SUNIONSTORE key key1 key2");
            var expectedResponse = ":5\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SMEMBERS key");
            expectedResponse = "*5\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSdiffLC()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("SADD key1 a b c d");
            lightClientRequest.SendCommand("SADD key2 c");
            lightClientRequest.SendCommand("SADD key3 a c e");
            var response = lightClientRequest.SendCommand("SDIFF key1 key2 key3");
            var expectedResponse = "*2\r\n$1\r\nb\r\n$1\r\nd\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }


        [Test]
        public void CanDoSinterLC()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("SADD key1 a b c d");
            lightClientRequest.SendCommand("SADD key2 c");
            lightClientRequest.SendCommand("SADD key3 a c e");
            var response = lightClientRequest.SendCommand("SINTER key1 key2 key3");
            var expectedResponse = "*1\r\n$1\r\nc\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void IntersectWithEmptySetReturnEmptySet()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("SADD key1 a");

            var response = lightClientRequest.SendCommand("SINTER key1 key2");
            var expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void IntersectWithNoKeysReturnError()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SINTER");
            var expectedResponse = "-ERR wrong number of arguments for 'SINTER' command\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void IntersectAndStoreWithNoKeysReturnError()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SINTERSTORE");
            var expectedResponse = "-ERR wrong number of arguments for 'SINTERSTORE' command\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }


        [Test]
        public void IntersectAndStoreWithNotExisingSetsOverwitesDestinationSet()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("SADD key a");

            var response = lightClientRequest.SendCommand("SINTERSTORE key key1 key2 key3");
            var expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SMEMBERS key");
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void IntersectAndStoreWithNoSetsReturnErrWrongNumArgs()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SINTERSTORE key");
            var expectedResponse = $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, "SINTERSTORE")}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSinterStoreLC()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("SADD key1 a b c d");
            lightClientRequest.SendCommand("SADD key2 c");
            lightClientRequest.SendCommand("SADD key3 a c e");
            var response = lightClientRequest.SendCommand("SINTERSTORE key key1 key2 key3");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SMEMBERS key");
            expectedResponse = "*1\r\n$1\r\nc\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase("")]
        [TestCase("key")]
        public void CanDoSdiffStoreLC(string key)
        {
            var lightClientRequest = TestUtils.CreateRequest();
            _ = lightClientRequest.SendCommand("SADD key1 a b c d");
            _ = lightClientRequest.SendCommand("SADD key2 c");
            _ = lightClientRequest.SendCommand("SADD key3 a c e");
            var response = lightClientRequest.SendCommand($"SDIFFSTORE {key} key1 key2 key3");
            var expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SMEMBERS {key}");
            expectedResponse = "*2\r\n$1\r\nb\r\n$1\r\nd\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase("1,2,3", "2,3,4", "2", Description = "Basic intersection")]
        [TestCase("1,2,3", "", "0", Description = "Intersection with empty set")]
        [TestCase("1,2,3", "4,5,6", "0", Description = "No intersection")]
        [TestCase("1,1,1", "1,1,1", "1", Description = "Sets with duplicate values")]
        [TestCase("", "", "0", Description = "Both sets empty")]
        public void CanDoSinterCardLC(string values1, string values2, string expectedCount)
        {
            var lightClientRequest = TestUtils.CreateRequest();

            // Add values to first set
            foreach (var value in values1.Split(',', StringSplitOptions.RemoveEmptyEntries))
            {
                lightClientRequest.SendCommand($"SADD key1 {value}");
            }

            // Add values to second set
            foreach (var value in values2.Split(',', StringSplitOptions.RemoveEmptyEntries))
            {
                lightClientRequest.SendCommand($"SADD key2 {value}");
            }

            var response = lightClientRequest.SendCommand("SINTERCARD 2 key1 key2");
            var expectedResponse = $":{expectedCount}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Test with non-existing keys
            response = lightClientRequest.SendCommand("SINTERCARD 2 nonexistent1 nonexistent2");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }
        #endregion


        #region NegativeTests

        [Test]
        public void CanDoSCARDCommandWhenKeyDoesNotExistLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SCARD fooset");
            var expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSPOPCommandWhenKeyDoesNotExistLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SPOP fooset");
            var expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanUseNotExistingSetwithSMembers()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SMEMBERS foo");
            var expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSdiffWhenKeyDoesNotExisting()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SDIFF foo");
            var expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSdiffStoreWhenMemberKeysNotExisting()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SDIFFSTORE key key1 key2 key3");
            var expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SMEMBERS key");
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSunionStoreWhenMemberKeysNotExisting()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SUNIONSTORE key key1 key2 key3");
            var expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SMEMBERS key");
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoSinterStoreWhenMemberKeysNotExisting()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SINTERSTORE key key1 key2 key3");
            var expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SMEMBERS key");
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CheckSetOperationsOnWrongTypeObjectSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var keys = new[] { new RedisKey("user1:obj1"), new RedisKey("user1:obj2") };
            var key1Values = new[] { new RedisValue("Hello"), new RedisValue("World") };
            var key2Values = new[] { new RedisValue("Hola"), new RedisValue("Mundo") };
            var values = new[] { key1Values, key2Values };

            // Set up different type objects
            RespTestsUtils.SetUpTestObjects(db, GarnetObjectType.List, keys, values);

            // SADD
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SetAdd(keys[0], values[0]));
            // SREM
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SetRemove(keys[0], values[0]));
            // SPOP
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SetPop(keys[0], 2));
            // SMEMBERS
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SetMembers(keys[0]));
            // SCARD
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SetLength(keys[0]));
            // SSCAN
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SetScan(keys[0], new RedisValue("*")).FirstOrDefault());
            // SMOVE
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SetMove(keys[0], keys[1], values[0][0]));
            // SRANDMEMBER
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SetRandomMember(keys[0]));
            // SISMEMBER
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SetContains(keys[0], values[0][0]));
            // SUNION
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SetCombine(SetOperation.Union, keys[0], keys[1]));
            // SUNIONSTORE
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SetCombineAndStore(SetOperation.Union, keys[0], [keys[1]]));
            // SDIFF
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SetCombine(SetOperation.Difference, keys[0], keys[1]));
            // SDIFFSTORE
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() =>
                db.SetCombineAndStore(SetOperation.Difference, keys[0], [keys[1]]));
            // SINTER
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SetCombine(SetOperation.Intersect, keys[0], keys[1]));
            // SINTERSTORE
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.SetCombineAndStore(SetOperation.Intersect, keys[0], [keys[1]]));
        }

        #endregion


        #region commonmethods

        private static void CreateSet()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SADD myset \"Hello\"", 1);
            response = lightClientRequest.SendCommand("SADD myset \"World\"", 1);
        }

        private static void CreateLongSet()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SADD myset one", 1);
            response = lightClientRequest.SendCommand("SADD myset two", 1);
            response = lightClientRequest.SendCommand("SADD myset three", 1);
            response = lightClientRequest.SendCommand("SADD myset four", 1);
            response = lightClientRequest.SendCommand("SADD myset five", 1);
        }
        #endregion

        #region SMISMEMBER

        [Test]
        [TestCase("Value1,Value2,Value3,Value4,Value5", "Value3,Value6", "true,false")]
        [TestCase("Value1,Value2,Value5", "InvalidA,Value1,Value5,InvalidB", "false,true,true,false")]
        [TestCase("Value1", "Value1", "true")]
        [TestCase("Value1", "Value2", "false")]
        public void CheckIfMemberExistsInSetLC(string valuesInput, string findInput, string expectedInput)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "KeyA";
            var values = valuesInput.Split(",");
            var find = findInput.Split(",");
            var expectedResult = expectedInput.Split(",").Select(x => bool.Parse(x)).ToArray();

            foreach (var value in values)
            {
                db.SetAdd(key, value);
            }

            var actualResult = db.SetContains(key, [.. find.Select(x => (RedisValue)x)]);

            CollectionAssert.AreEqual(expectedResult, actualResult);
        }

        [Test]
        public void CheckIfMemberExistsWithNoExistKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "KeyA";
            RedisValue[] find = ["Value1", "Value2"];
            bool[] expectedResult = [false, false];

            var actualResult = db.SetContains(key, find);

            CollectionAssert.AreEqual(expectedResult, actualResult);
        }

        [Test]
        public void CheckIfMemberExistsWithInvalidParam()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "KeyA";

            Assert.Throws<RedisServerException>(() => db.Execute("SMISMEMBER", key));
        }

        #endregion
    }
}