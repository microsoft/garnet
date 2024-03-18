// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespSetTests
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
        public void CanAddAndListMembers()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var result = db.SetAdd(new RedisKey("user1:set"), new RedisValue[] { "Hello", "World", "World" });
            Assert.AreEqual(2, result);

            var members = db.SetMembers(new RedisKey("user1:set"));
            Assert.AreEqual(2, members.Length);

            var response = db.Execute("MEMORY", "USAGE", "user1:set");
            var actualValue = ResultType.Integer == response.Type ? Int32.Parse(response.ToString()) : -1;
            var expectedResponse = 272;
            Assert.AreEqual(expectedResponse, actualValue);
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
                Assert.AreEqual(nVals, nAdded);
            }

            var members = db.SetMembers(new RedisKey("Set_Test-10"));
            Assert.AreEqual(100, members.Length);
        }


        [Test]
        public void CanReturnEmptySet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var members = db.SetMembers(new RedisKey("myset"));

            var response = db.Execute("MEMORY", "USAGE", "myset");
            var actualValue = ResultType.Integer == response.Type ? Int32.Parse(response.ToString()) : -1;
            var expectedResponse = -1;
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CanRemoveField()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var result = db.SetAdd(new RedisKey("user1:set"), new RedisValue[] { "ItemOne", "ItemTwo", "ItemThree", "ItemFour" });
            Assert.AreEqual(4, result);

            var memresponse = db.Execute("MEMORY", "USAGE", "user1:set");
            var actualValue = ResultType.Integer == memresponse.Type ? Int32.Parse(memresponse.ToString()) : -1;
            var expectedResponse = 424;
            Assert.AreEqual(expectedResponse, actualValue);

            var response = db.SetRemove(new RedisKey("user1:set"), new RedisValue("ItemOne"));
            Assert.AreEqual(true, response);

            memresponse = db.Execute("MEMORY", "USAGE", "user1:set");
            actualValue = ResultType.Integer == memresponse.Type ? Int32.Parse(memresponse.ToString()) : -1;
            expectedResponse = 352;
            Assert.AreEqual(expectedResponse, actualValue);

            response = db.SetRemove(new RedisKey("user1:set"), new RedisValue("ItemFive"));
            Assert.AreEqual(false, response);

            memresponse = db.Execute("MEMORY", "USAGE", "user1:set");
            actualValue = ResultType.Integer == memresponse.Type ? Int32.Parse(memresponse.ToString()) : -1;
            expectedResponse = 352;
            Assert.AreEqual(expectedResponse, actualValue);

            var longResponse = db.SetRemove(new RedisKey("user1:set"), new RedisValue[] { "ItemTwo", "ItemThree" });
            Assert.AreEqual(2, longResponse);

            memresponse = db.Execute("MEMORY", "USAGE", "user1:set");
            actualValue = ResultType.Integer == memresponse.Type ? Int32.Parse(memresponse.ToString()) : -1;
            expectedResponse = 200;
            Assert.AreEqual(expectedResponse, actualValue);

            var members = db.SetMembers(new RedisKey("user1:set"));
            Assert.AreEqual(1, members.Length);
        }

        [Test]
        public void CanUseSScanNoParameters()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Use setscan on non existing key
            var items = db.SetScan(new RedisKey("foo"), new RedisValue("*"), pageSize: 10);
            Assert.IsTrue(items.Count() == 0, "Failed to use SetScan on non existing key");

            RedisValue[] entries = new RedisValue[] { "item-a", "item-b", "item-c", "item-d", "item-e", "item-aaa" };

            // Add some items
            var added = db.SetAdd("myset", entries);
            Assert.AreEqual(entries.Length, added);

            var members = db.SetScan(new RedisKey("myset"), new RedisValue("*"));
            Assert.IsTrue(((IScanningCursor)members).Cursor == 0);
            Assert.IsTrue(members.Count() == entries.Length);

            int i = 0;
            foreach (var item in members)
            {
                Assert.IsTrue(entries[i++].Equals(item));
            }

            // No matching elements
            members = db.SetScan(new RedisKey("myset"), new RedisValue("x"));
            Assert.IsTrue(((IScanningCursor)members).Cursor == 0);
            Assert.IsTrue(members.Count() == 0);
        }

        [Test]
        public void CanUseSScanWithMatch()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add some items
            var added = db.SetAdd("myset", new RedisValue[] { "aa", "bb", "cc", "dd", "ee", "aaf" });
            Assert.AreEqual(6, added);

            var members = db.SetScan(new RedisKey("myset"), new RedisValue("*aa"));
            Assert.IsTrue(((IScanningCursor)members).Cursor == 0);
            Assert.IsTrue(members.Count() == 1);
            Assert.IsTrue(members.ElementAt(0).Equals("aa"));
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
            Assert.IsTrue(((IScanningCursor)members).Cursor == 0);
            Assert.IsTrue(members.Count() == setLen);
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
            Assert.AreEqual(setEntries.Length, j);

            // Assert the cursor is at the end of the enumeration
            Assert.AreEqual(pageNumber + pageOffset, setEntries.Length - 1);

            var l = response.LastOrDefault();
            Assert.AreEqual(l, $"value:{setEntries.Length - 1}");
        }



        #endregion


        #region LightClientTests


        [Test]
        public void CanAddAndListMembersLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SADD myset \"Hello\"");
            var expectedResponse = ":1\r\n";
            var strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);

            response = lightClientRequest.SendCommand("SADD myset \"World\"");
            strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);

            expectedResponse = ":0\r\n";
            response = lightClientRequest.SendCommand("SADD myset \"World\"");
            strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);

            response = lightClientRequest.SendCommand("SMEMBERS myset", 3);
            expectedResponse = "*2\r\n$7\r\n\"Hello\"\r\n$7\r\n\"World\"\r\n";
            strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);

        }


        [Test]
        public void CanDoSCARDCommandLC()
        {
            CreateSet();
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SCARD myset");
            var expectedResponse = ":2\r\n";
            var strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);
        }


        [Test]
        public void CanReturnEmptySetLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SMEMBERS otherset", 1);

            // Empty array
            var expectedResponse = "*0\r\n";
            var strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);
        }

        [Test]
        public void CanDoSREMLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SADD myset ItemOne ItemTwo ItemThree ItemFour");
            var expectedResponse = ":4\r\n";
            var strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);

            response = lightClientRequest.SendCommand("SREM myset World");
            expectedResponse = ":0\r\n";
            strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);

            response = lightClientRequest.SendCommand("SREM myset ItemOne");
            expectedResponse = ":1\r\n";
            strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);

            response = lightClientRequest.SendCommand("SREM myset ItemTwo ItemThree");
            expectedResponse = ":2\r\n";
            strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);
        }

        [Test]
        public void CanDoSCARDCommandsLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("SCARD fooset", "PING", 1, 1);
            var expectedResponse = ":0\r\n+PONG\r\n";
            var strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);
        }

        [Test]
        public void CanDoSPOPCommandLC()
        {
            var myset = new HashSet<string>();
            myset.Add("one");
            myset.Add("two");
            myset.Add("three");
            myset.Add("four");
            myset.Add("five");

            CreateLongSet();

            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SPOP myset");
            var strLen = Encoding.ASCII.GetString(response).Substring(1, 1);
            var item = Encoding.ASCII.GetString(response).Substring(4, Int32.Parse(strLen));
            Assert.IsTrue(myset.Contains(item));

            response = lightClientRequest.SendCommand("SCARD myset");
            var expectedResponse = ":4\r\n";
            var strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);
        }

        [Test]
        public void CanDoSPOPWithCountCommandLC()
        {
            var myset = new HashSet<string>();
            myset.Add("one");
            myset.Add("two");
            myset.Add("three");
            myset.Add("four");
            myset.Add("five");

            CreateLongSet();

            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SPOP myset 3", 4);
            var strLen = Encoding.ASCII.GetString(response).Substring(1, 1);
            Assert.AreEqual(3, Int32.Parse(strLen));

            var secondResponse = lightClientRequest.SendCommands("SCARD myset", "PING", 1, 1);
            var expectedResponse = ":2\r\n+PONG\r\n";
            var strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);
        }


        [Test]
        public void MultiWithNonExistingSet()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            byte[] res;

            string expectedResponse = "+OK\r\n";

            res = lightClientRequest.SendCommand("MULTI");
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            //create set
            res = lightClientRequest.SendCommand("SADD MySet ItemOne");
            expectedResponse = "+QUEUED\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("EXEC", 2);
            expectedResponse = "*1\r\n:1\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("SMEMBERS MySet", 2);
            expectedResponse = "*1\r\n$7\r\nItemOne\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
        }

        #endregion


        #region NegativeTests

        [Test]
        public void CanDoSCARDCommandWhenKeyDoesNotExistLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SCARD fooset");
            var expectedResponse = ":0\r\n";
            var strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);
        }

        [Test]
        public void CanDoSPOPCommandWhenKeyDoesNotExistLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SPOP fooset");
            var expectedResponse = "$-1\r\n";
            var strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);
        }

        [Test]
        public void CanUseNotExistingSetwithSMembers()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SMEMBERS foo");
            var expectedResponse = "*0\r\n";
            var strResponse = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, strResponse);
        }

        #endregion


        #region commonmethods

        private void CreateSet()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SADD myset \"Hello\"", 1);
            response = lightClientRequest.SendCommand("SADD myset \"World\"", 1);
        }

        private void CreateLongSet()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("SADD myset one", 1);
            response = lightClientRequest.SendCommand("SADD myset two", 1);
            response = lightClientRequest.SendCommand("SADD myset three", 1);
            response = lightClientRequest.SendCommand("SADD myset four", 1);
            response = lightClientRequest.SendCommand("SADD myset five", 1);
        }
        #endregion
    }
}