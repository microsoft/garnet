// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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
    class RespListTests
    {
        GarnetServer server;
        Random r;

        [SetUp]
        public void Setup()
        {
            r = new Random(674386);
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

        [Test]
        public void BasicLPUSHAndLPOP()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "List_Test";
            var val = "Value-0";

            int nVals = 1;
            //a entry added to the list
            var nAdded = db.ListLeftPush(key, val);
            ClassicAssert.AreEqual(nVals, nAdded);

            var result = db.Execute("MEMORY", "USAGE", key);
            var actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            var expectedResponse = 184;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            string popval = db.ListLeftPop(key);
            ClassicAssert.AreEqual(val, popval);

            var keyExists = db.KeyExists(key);
            ClassicAssert.IsFalse(keyExists);

            result = db.Execute("MEMORY", "USAGE", key);
            ClassicAssert.IsTrue(result.IsNull);
        }

        [Test]
        public void MultiLPUSHAndLTRIMWithMemoryCheck()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "List_Test";
            var nVals = 10;
            RedisValue[] values = new RedisValue[nVals];
            for (int i = 0; i < 10; i++)
            {
                values[i] = ("val_" + i.ToString());
            }
            var nAdded = db.ListLeftPush(key, values);
            ClassicAssert.AreEqual(nVals, nAdded);

            var result = db.Execute("MEMORY", "USAGE", key);
            var actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            var expectedResponse = 904;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            db.ListTrim(key, 1, 5);

            var nLen = db.ListLength(key);
            ClassicAssert.AreEqual(5, nLen);

            result = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            expectedResponse = 504;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //all elements remain
            db.ListTrim(key, 0, -1);
            nLen = db.ListLength(key);
            ClassicAssert.AreEqual(5, nLen);

            result = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            expectedResponse = 504;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            db.ListTrim(key, 0, -3);
            nLen = db.ListLength(key);
            ClassicAssert.AreEqual(3, nLen);

            var vals = db.ListRange(key, 0, -1);
            ClassicAssert.AreEqual("val_8", vals[0].ToString());
            ClassicAssert.AreEqual("val_7", vals[1].ToString());
            ClassicAssert.AreEqual("val_6", vals[2].ToString());

            db.ListTrim(key, -4, -4);
            var exists = db.KeyExists(key);
            ClassicAssert.IsFalse(exists);
        }

        private static object[] LTrimTestCases = [
            new object[] { 0, 0, new[] { 0 } },
            new object[] { -2, -1, new[] { 8, 9 } },
            new object[] { -2, -2, new[] { 8 } },
            new object[] { 3, 5, new[] { 3, 4, 5 } },
            new object[] { -12, 0, new[] { 0 } },
            new object[] { -12, 2, new[] { 0, 1, 2 } },
            new object[] { -12, -7, new[] { 0, 1, 2, 3 } },
            new object[] { -15, -11, Array.Empty<int>() },
            new object[] { 8, 8, new[] { 8 } },
            new object[] { 8, 12, new[] { 8, 9 } },
            new object[] { 9, 12, new[] { 9 } },
            new object[] { 10, 12, Array.Empty<int>() },
            new object[] { 5, 3, Array.Empty<int>() },
            new object[] { -3, -5, Array.Empty<int>() }
        ];

        [Test]
        [TestCaseSource(nameof(LTrimTestCases))]
        public void MultiRPUSHAndLTRIM(int startIdx, int stopIdx, int[] expectedRemainingIdx)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "List_Test";
            var nVals = 10;
            var values = new RedisValue[nVals];
            for (var i = 0; i < 10; i++)
            {
                values[i] = "val_" + i;
            }
            var nAdded = db.ListRightPush(key, values);
            ClassicAssert.AreEqual(nVals, nAdded);

            db.ListTrim(key, startIdx, stopIdx);
            var nLen = db.ListLength(key);
            ClassicAssert.AreEqual(expectedRemainingIdx.Length, nLen);
            var remainingVals = db.ListRange(key);
            for (var i = 0; i < remainingVals.Length; i++)
            {
                ClassicAssert.AreEqual(values[expectedRemainingIdx[i]], remainingVals[i].ToString());
            }
        }

        [Test]
        public void MultiLPUSHAndLLENWithPendingStatus()
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
                var nAdded = db.ListLeftPush($"List_Test-{j + 1}", values);
                ClassicAssert.AreEqual(nVals, nAdded);
            }

            long nLen = db.ListLength("List_Test-10");
            ClassicAssert.AreEqual(100, nLen);
        }

        [Test]
        public void BasicLPUSHAndLTRIM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "List_Test";
            var val = "Value-0";

            int nVals = 1;
            //a entry added to the list
            var nAdded = db.ListLeftPush(key, val);
            ClassicAssert.AreEqual(nVals, nAdded);

            long nLen = db.ListLength(key);
            db.ListTrim(key, 0, 5);

            long nLen1 = db.ListLength(key);
            ClassicAssert.AreEqual(nLen1, 1);

            db.ListTrim(key, 0, -1);
            nLen1 = db.ListLength(key);
            ClassicAssert.AreEqual(nLen1, 1);
        }

        [Test]
        public void BasicLPUSHAndLRANGE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "List_Test";
            var nVals = 3;
            RedisValue[] values = new RedisValue[nVals];
            for (int i = 0; i < nVals; i++)
            {
                values[i] = ("val_" + i.ToString());
            }
            var nAdded = db.ListLeftPush(key, values);
            ClassicAssert.AreEqual(nVals, nAdded);

            long nLen = db.ListLength(key);
            var vals = db.ListRange(key, 0, 0);
            ClassicAssert.AreEqual(1, vals.Length);

            vals = null;
            vals = db.ListRange(key, -3, 2);
            ClassicAssert.AreEqual(3, vals.Length);

            vals = null;
            vals = db.ListRange(key, -100, 100);
            ClassicAssert.AreEqual(3, vals.Length);

            vals = null;
            vals = db.ListRange(key, 5, 10);
            ClassicAssert.AreEqual(0, vals.Length);
        }

        [Test]
        public void BasicRPUSHAndLINDEX()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "List_Test";
            var nVals = 3;
            RedisValue[] values = new RedisValue[nVals];
            for (int i = 0; i < nVals; i++)
            {
                values[i] = ("val_" + i.ToString());
            }
            var nAdded = db.ListRightPush(key, values);
            ClassicAssert.AreEqual(nVals, nAdded);

            long nLen = db.ListLength(key);
            int nIndex = -1;
            var val = db.ListGetByIndex(key, nIndex);
            ClassicAssert.AreEqual(val, values[nVals + nIndex]);

            nIndex = 2;
            val = db.ListGetByIndex(key, nIndex);
            ClassicAssert.AreEqual(val, values[nIndex]);

            nIndex = 3;
            val = db.ListGetByIndex(key, nIndex);
            ClassicAssert.AreEqual(true, val.IsNull);
        }

        [Test]
        public void BasicRPUSHAndLINSERT()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "List_Test";
            var nVals = 3;
            RedisValue[] values = new RedisValue[nVals];
            for (int i = 0; i < nVals; i++)
            {
                values[i] = ("val_" + i.ToString());
            }
            var nAdded = db.ListRightPush(key, values);
            ClassicAssert.AreEqual(nVals, nAdded);

            var result = db.Execute("MEMORY", "USAGE", key);
            var actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            var expectedResponse = 344;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            long nLen = db.ListLength(key);
            var insert_val = "val_test1";
            // test before
            var ret = db.ListInsertBefore(key, "val_1", insert_val);

            nLen = db.ListLength(key);
            var val = db.ListGetByIndex(key, 1);
            ClassicAssert.AreEqual(val, insert_val);

            result = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            expectedResponse = 432;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // test after
            insert_val = "val_test2";
            db.ListInsertAfter(key, "val_0", insert_val);
            val = db.ListGetByIndex(key, 1);
            ClassicAssert.AreEqual(val, insert_val);

            result = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            expectedResponse = 520;
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void BasicRPUSHAndLREM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "List_Test";
            var nVals = 6;
            RedisValue[] values = new RedisValue[nVals];
            for (int i = 0; i < nVals; i += 2)
            {
                values[i] = ("val_" + i.ToString());
                values[i + 1] = ("val_" + i.ToString());

            }
            var nAdded = db.ListRightPush(key, values);
            ClassicAssert.AreEqual(nVals, nAdded);

            var result = db.Execute("MEMORY", "USAGE", key);
            var actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            var expectedResponse = 584;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            long nLen = db.ListLength(key);
            var ret = db.ListRemove(key, "val_0", 2);
            ClassicAssert.AreEqual(ret, 2);

            result = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            expectedResponse = 424;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            ret = db.ListRemove(key, "val_4", -1);
            nLen = db.ListLength(key);
            ClassicAssert.AreEqual(nLen, 3);

            result = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            expectedResponse = 344;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            ret = db.ListRemove(key, "val_2", 0);
            ClassicAssert.AreEqual(2, ret);

            ret = db.ListRemove(key, "val_4", 0);
            ClassicAssert.AreEqual(1, ret);

            var exists = db.KeyExists(key);
            ClassicAssert.IsFalse(exists);
        }

        [Test]
        public void MultiLPUSHAndLPOPV1()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "List_Test";

            var nVals = 10;
            RedisValue[] values = new RedisValue[nVals];
            for (int i = 0; i < 10; i++)
            {
                values[i] = ("val_" + i.ToString());
            }
            var nAdded = db.ListLeftPush(key, values);
            ClassicAssert.AreEqual(nVals, nAdded);

            var result = db.Execute("MEMORY", "USAGE", key);
            var actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            var expectedResponse = 904;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            long nLen = db.ListLength(key);

            string popval = string.Empty;
            while (true)
            {
                popval = db.ListLeftPop(key);
                ClassicAssert.AreEqual(values[nVals - 1], popval);
                nVals--;

                if (nVals == 0)
                    break;
            }

            // list is empty, the code should return (nil)
            popval = db.ListLeftPop(key);
            ClassicAssert.IsNull(popval);

            var keyExists = db.KeyExists(key);
            ClassicAssert.IsFalse(keyExists);

            result = db.Execute("MEMORY", "USAGE", key);
            ClassicAssert.IsTrue(result.IsNull);
        }

        [Test]
        public void MultiLPUSHAndLPOPV2()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "List_Test";

            var nVals = 10;
            RedisValue[] values = new RedisValue[nVals];
            for (int i = 0; i < 10; i++)
            {
                values[i] = ("val_" + i.ToString());
            }
            var nAdded = db.ListLeftPush(key, values);
            ClassicAssert.AreEqual(nVals, nAdded);

            var result = db.Execute("MEMORY", "USAGE", key);
            var actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            var expectedResponse = 904;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            long nLen = db.ListLength(key);
            var ret = db.Execute("LPOP", key, "2");
            nLen = db.ListLength(key);
            ClassicAssert.AreEqual(nLen, 8);

            result = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            expectedResponse = 744;
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void MultiRPUSHAndRPOP()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "List_Test";

            var nVals = 10;
            RedisValue[] values = new RedisValue[nVals];
            for (int i = 0; i < 10; i++)
            {
                values[i] = ("val_" + i.ToString());
            }
            var nAdded = db.ListRightPush(key, values);
            ClassicAssert.AreEqual(nVals, nAdded);

            var result = db.Execute("MEMORY", "USAGE", key);
            var actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            var expectedResponse = 904;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            string popval = string.Empty;
            nVals = 9;
            while (true)
            {
                popval = db.ListRightPop(key);
                ClassicAssert.AreEqual(values[nVals], popval);
                nVals--;

                if (nVals < 0)
                    break;
            }

            // list is empty, the code should return (nil)
            popval = db.ListLeftPop(key);
            ClassicAssert.IsNull(popval);

            var keyExists = db.KeyExists(key);
            ClassicAssert.IsFalse(keyExists);

            result = db.Execute("MEMORY", "USAGE", key);
            ClassicAssert.IsTrue(result.IsNull);

            var pushed = db.ListRightPush(key, []);
            ClassicAssert.AreEqual(0, pushed);

            keyExists = db.KeyExists(key);
            ClassicAssert.IsFalse(keyExists);

            pushed = db.ListLeftPush(key, []);
            ClassicAssert.AreEqual(0, pushed);

            keyExists = db.KeyExists(key);
            ClassicAssert.IsFalse(keyExists);
        }

        [Test]
        public void BasicRPUSHAndRPOP()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "List_Test";

            int nVals = 1;
            //a entry added to the list
            var nAdded = db.ListRightPush(key, "Value-0");
            ClassicAssert.AreEqual(nVals, nAdded);
        }

        [Test]
        public void CanDoRPopLPush()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mylist";
            db.ListRightPush(key, "Value-one");
            db.ListRightPush(key, "Value-two");
            db.ListRightPush(key, "Value-three");

            var result = db.ListRightPopLeftPush("mylist", "myotherlist");
            ClassicAssert.AreEqual("Value-three", result.ToString());

            var response = db.Execute("MEMORY", "USAGE", key);
            var actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            var expectedResponse = 272;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var lrange = db.ListRange(key, 0, -1);
            ClassicAssert.AreEqual(2, lrange.Length);
            ClassicAssert.AreEqual("Value-one", lrange[0].ToString());
            ClassicAssert.AreEqual("Value-two", lrange[1].ToString());

            lrange = db.ListRange("myotherlist", 0, -1);
            ClassicAssert.AreEqual(1, lrange.Length);
            ClassicAssert.AreEqual("Value-three", lrange[0].ToString());

            result = db.ListRightPopLeftPush(key, key);
            ClassicAssert.AreEqual("Value-two", result.ToString());

            response = db.Execute("MEMORY", "USAGE", key);
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 272;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            lrange = db.ListRange(key, 0, -1);
            ClassicAssert.AreEqual(2, lrange.Length);
            ClassicAssert.AreEqual("Value-two", lrange[0].ToString());
            ClassicAssert.AreEqual("Value-one", lrange[1].ToString());
        }

        [Test]
        public void CanDoLRANGEbasic()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mylist";
            _ = db.ListRightPush(key, "one");
            _ = db.ListRightPush(key, "two");
            _ = db.ListRightPush(key, "three");

            var result = db.ListRange(key, 0, 0);
            ClassicAssert.AreEqual(1, result.Length);
            ClassicAssert.IsTrue(Array.Exists(result, t => t.ToString().Equals("one")));

            result = db.ListRange(key, -3, 2);
            ClassicAssert.AreEqual(3, result.Length);
            ClassicAssert.IsTrue(Array.Exists(result, t => t.ToString().Equals("one")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.ToString().Equals("two")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.ToString().Equals("three")));

            result = db.ListRange(key, -100, 100);
            ClassicAssert.AreEqual(3, result.Length);
            ClassicAssert.IsTrue(Array.Exists(result, t => t.ToString().Equals("one")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.ToString().Equals("two")));
            ClassicAssert.IsTrue(Array.Exists(result, t => t.ToString().Equals("three")));

            result = db.ListRange(key, 5, 100);
            ClassicAssert.AreEqual(0, result.Length);
        }

        [Test]
        public void CanDoLRANGEcorrect()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mylist";
            _ = db.ListRightPush(key, "a");
            _ = db.ListRightPush(key, "b");
            _ = db.ListRightPush(key, "c");
            _ = db.ListRightPush(key, "d");
            _ = db.ListRightPush(key, "e");
            _ = db.ListRightPush(key, "f");
            _ = db.ListRightPush(key, "g");

            var result = db.ListRange(key, -10, -7);
            ClassicAssert.AreEqual(1, result.Length);
            ClassicAssert.IsTrue(result[0].ToString().Equals("a"));

            result = db.ListRange(key, -4, -2);
            ClassicAssert.AreEqual(3, result.Length);
            ClassicAssert.IsTrue(result[0].ToString().Equals("d"));
            ClassicAssert.IsTrue(result[1].ToString().Equals("e"));
            ClassicAssert.IsTrue(result[2].ToString().Equals("f"));

            result = db.ListRange(key, -1, -1);
            ClassicAssert.AreEqual(1, result.Length);
            ClassicAssert.IsTrue(result[0].ToString().Equals("g"));

            result = db.ListRange(key, -3, 3);
            ClassicAssert.AreEqual(0, result.Length);

            result = db.ListRange(key, -3, 4);
            ClassicAssert.AreEqual(1, result.Length);
            ClassicAssert.IsTrue(result[0].ToString().Equals("e"));

            result = db.ListRange(key, -4, 4);
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.IsTrue(result[0].ToString().Equals("d"));
            ClassicAssert.IsTrue(result[1].ToString().Equals("e"));

            result = db.ListRange(key, 0, 0);
            ClassicAssert.AreEqual(1, result.Length);
            ClassicAssert.IsTrue(result[0].ToString().Equals("a"));

            result = db.ListRange(key, 1, 2);
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.IsTrue(result[0].ToString().Equals("b"));
            ClassicAssert.IsTrue(result[1].ToString().Equals("c"));

            result = db.ListRange(key, 3, 3);
            ClassicAssert.AreEqual(1, result.Length);
            ClassicAssert.IsTrue(result[0].ToString().Equals("d"));

            result = db.ListRange(key, 4, 4);
            ClassicAssert.AreEqual(1, result.Length);
            ClassicAssert.IsTrue(result[0].ToString().Equals("e"));

            result = db.ListRange(key, 5, 10);
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.IsTrue(result[0].ToString().Equals("f"));
            ClassicAssert.IsTrue(result[1].ToString().Equals("g"));
        }

        [Test]
        public void CanDoLSETbasic()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mylist";
            var values = new RedisValue[] { "one", "two", "three" };
            var pushResult = db.ListRightPush(key, values);
            ClassicAssert.AreEqual(3, pushResult);

            db.ListSetByIndex(key, 0, "four");
            db.ListSetByIndex(key, -2, "five");

            var result = db.ListRange(key, 0, -1);
            var strResult = result.Select(r => r.ToString()).ToArray();
            ClassicAssert.AreEqual(3, result.Length);
            var expected = new[] { "four", "five", "three" };
            ClassicAssert.IsTrue(expected.SequenceEqual(strResult));
        }

        #region GarnetClientTests

        [Test]
        public async Task CanDoRPopLPushGC()
        {
            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            //If source does not exist, the value nil is returned and no operation is performed.
            var response = await db.ExecuteForStringResultAsync("RPOPLPUSH", ["mylist", "myotherlist"]);
            ClassicAssert.AreEqual(null, response);

            await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "one"]);
            await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "two"]);
            await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "three"]);

            response = await db.ExecuteForStringResultAsync("RPOPLPUSH", ["mylist", "myotherlist"]);
            ClassicAssert.AreEqual("three", response);

            var responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["mylist", "0", "-1"]);
            var expectedResponseArray = new string[] { "one", "two" };
            ClassicAssert.AreEqual(expectedResponseArray, responseArray);

            responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["myotherlist", "0", "-1"]);
            expectedResponseArray = ["three"];
            ClassicAssert.AreEqual(expectedResponseArray, responseArray);

            // if source and destination are the same 
            //the operation is equivalent to removing the last element from the list and pushing it as first element of the list,
            //so it can be considered as a list rotation command.
            response = await db.ExecuteForStringResultAsync("RPOPLPUSH", ["mylist", "mylist"]);
            ClassicAssert.AreEqual("two", response);

            responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["mylist", "0", "-1"]);
            expectedResponseArray = ["two", "one"];
            ClassicAssert.AreEqual(expectedResponseArray, responseArray);
        }


        [Test]
        public async Task CanUseLMoveGC()
        {
            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            // Test for Operation direction error.
            var exception = Assert.ThrowsAsync<Exception>(async () =>
            {
                await db.ExecuteForStringResultAsync("LMOVE", ["mylist", "myotherlist", "right", "lef"]);
            });
            ClassicAssert.AreEqual("ERR syntax error", exception.Message);

            //If source does not exist, the value nil is returned and no operation is performed.
            var response = await db.ExecuteForStringResultAsync("LMOVE", ["mylist", "myotherlist", "RIGHT", "LEFT"]);
            ClassicAssert.AreEqual(null, response);

            await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "one"]);
            await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "two"]);
            await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "three"]);

            response = await db.ExecuteForStringResultAsync("LMOVE", ["mylist", "myotherlist", "RIGHT", "LEFT"]);
            ClassicAssert.AreEqual("three", response);

            var responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["mylist", "0", "-1"]);
            var expectedResponseArray = new string[] { "one", "two" };
            ClassicAssert.AreEqual(expectedResponseArray, responseArray);

            responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["myotherlist", "0", "-1"]);
            expectedResponseArray = ["three"];
            ClassicAssert.AreEqual(expectedResponseArray, responseArray);

            response = await db.ExecuteForStringResultAsync("LMOVE", ["mylist", "myotherlist", "LEFT", "RIGHT"]);
            ClassicAssert.AreEqual("one", response);

            responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["mylist", "0", "-1"]);
            expectedResponseArray = ["two"];
            ClassicAssert.AreEqual(expectedResponseArray, responseArray);

            responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["myotherlist", "0", "-1"]);
            expectedResponseArray = ["three", "one"];
            ClassicAssert.AreEqual(expectedResponseArray, responseArray);

            // if source and destination are the same 
            //the operation is equivalent to a list rotation command.
            response = await db.ExecuteForStringResultAsync("LMOVE", ["mylist", "mylist", "LEFT", "RIGHT"]);
            ClassicAssert.AreEqual("two", response);

            response = await db.ExecuteForStringResultAsync("LMOVE", ["myotherlist", "myotherlist", "LEFT", "RIGHT"]);
            ClassicAssert.AreEqual("three", response);

            responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["myotherlist", "0", "-1"]);
            expectedResponseArray = ["one", "three"];
            ClassicAssert.AreEqual(expectedResponseArray, responseArray);
        }

        [Test]
        public async Task CanUseLMoveWithCaseInsensitiveDirectionGC()
        {
            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "one"]);
            await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "two"]);
            await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "three"]);

            var response = await db.ExecuteForStringResultAsync("LMOVE", ["mylist", "myotherlist", "right", "left"]);
            ClassicAssert.AreEqual("three", response);

            var responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["mylist", "0", "-1"]);
            var expectedResponseArray = new string[] { "one", "two" };
            ClassicAssert.AreEqual(expectedResponseArray, responseArray);

            responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["myotherlist", "0", "-1"]);
            expectedResponseArray = ["three"];
            ClassicAssert.AreEqual(expectedResponseArray, responseArray);

            response = await db.ExecuteForStringResultAsync("LMOVE", ["mylist", "myotherlist", "LeFT", "RIghT"]);
            ClassicAssert.AreEqual("one", response);

            responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["mylist", "0", "-1"]);
            expectedResponseArray = ["two"];
            ClassicAssert.AreEqual(expectedResponseArray, responseArray);

            responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["myotherlist", "0", "-1"]);
            expectedResponseArray = ["three", "one"];
            ClassicAssert.AreEqual(expectedResponseArray, responseArray);
        }

        [Test]
        public async Task CanUseLMoveWithCancellationTokenGC()
        {
            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "one"]);
            await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "two"]);
            await db.ExecuteForStringResultAsync("RPUSH", ["mylist", "three"]);

            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;
            var response = await db.ExecuteForStringResultWithCancellationAsync("LMOVE", ["mylist", "myotherlist", "RIGHT", "LEFT"], token);
            ClassicAssert.AreEqual("three", response);

            //check contents of mylist sorted set
            var responseArray = await db.ExecuteForStringArrayResultAsync("LRANGE", ["mylist", "0", "-1"]);
            var expectedResponseArray = new string[] { "one", "two" };
            ClassicAssert.AreEqual(expectedResponseArray, responseArray);

            //Assert the cancellation is seen
            tokenSource.Cancel();
            var t = db.ExecuteForStringResultWithCancellationAsync("LMOVE", ["myotherlist", "myotherlist", "LEFT", "RIGHT"], tokenSource.Token);
            Assert.Throws<OperationCanceledException>(() => t.Wait(tokenSource.Token));

            tokenSource.Dispose();
        }
        #endregion

        #region LightClientTests

        [Test]
        public void CanReturnEmptyArrayinListLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            lightClientRequest.SendCommands("RPUSH mylist Hello", "PING", 1, 1);
            lightClientRequest.SendCommands("RPUSH mylist foo", "PING", 1, 1);
            lightClientRequest.SendCommands("RPUSH mylist bar", "PING", 1, 1);

            var response = lightClientRequest.SendCommands("LRANGE mylist 0 -1", "PING", 4, 1);
            var expectedResponse = "*3\r\n$5\r\nHello\r\n$3\r\nfoo\r\n$3\r\nbar\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommands("LRANGE mylist 5 10", "PING", 1, 1);
            expectedResponse = "*0\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanReturnNilWhenNonExistingListLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("LINSERT mykey BEFORE \"hola\" \"bye\"", "PING", 1, 1);
            //0 if key does not exist
            var expectedResponse = ":0\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanReturnErrorWhenMissingParametersLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("LINSERT mykey", "PING", 1, 1);
            var expectedResponse = $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, "LINSERT")}\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoPushAndTrimLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("RPUSH mylist one");
            lightClientRequest.SendCommand("RPUSH mylist two");
            var response = lightClientRequest.SendCommand("RPUSH mylist three");
            var expectedResponse = ":3\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("LTRIM mylist 1 -1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("LRANGE mylist 0 -1", 3);
            expectedResponse = "*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoLInsertBeforeAndAfterLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("RPUSH mylist Hello");
            lightClientRequest.SendCommand("RPUSH mylist World");
            // Use Before
            var response = lightClientRequest.SendCommand("LINSERT mylist BEFORE World There");
            var expectedResponse = ":3\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("LRANGE mylist 0 -1", 4);
            expectedResponse = "*3\r\n$5\r\nHello\r\n$5\r\nThere\r\n$5\r\nWorld\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Use After
            response = lightClientRequest.SendCommand("LINSERT mylist AFTER World Bye");
            expectedResponse = ":4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("LRANGE mylist 0 -1", 5);
            expectedResponse = "*4\r\n$5\r\nHello\r\n$5\r\nThere\r\n$5\r\nWorld\r\n$3\r\nBye\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoLInsertWithNoElementLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("RPUSH mylist Hello");
            lightClientRequest.SendCommand("RPUSH mylist World");
            // Use Before
            var response = lightClientRequest.SendCommand("LINSERT mylist BEFORE There Today");
            var expectedResponse = ":-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanSendErrorInWrongTypeLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("HSET myhash onekey onepair");
            lightClientRequest.SendCommand("LINSERT myhash BEFORE one two");
            var expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanDoLSETbasicLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            _ = lightClientRequest.SendCommand("RPUSH mylist one two three");
            var response = lightClientRequest.SendCommand("LSET mylist 0 four");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanReturnErrorLSETWhenNosuchkey()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("LSET mylist 0 four");
            var expectedResponse = "-ERR no such key\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanReturnErrorLSETWhenIndexNotInteger()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            _ = lightClientRequest.SendCommand("RPUSH mylist one two three");
            var response = lightClientRequest.SendCommand("LSET mylist a four");
            var expectedResponse = "-ERR value is not an integer or out of range.\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanReturnErrorLSETWhenIndexOutRange()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            _ = lightClientRequest.SendCommand("RPUSH mylist one two three");
            var response = lightClientRequest.SendCommand("LSET mylist 10 four");
            // 
            var expectedResponse = "-ERR index out of range";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("LSET mylist -100 four");
            expectedResponse = "-ERR index out of range";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CanReturnErrorLSETWhenArgumentsWrong()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            _ = lightClientRequest.SendCommand("RPUSH mylist one two three");
            var response = lightClientRequest.SendCommand("LSET mylist a");
            var expectedResponse = "-ERR wrong number of arguments for 'LSET'";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        #endregion

        [Test]
        public void CanHandleNoPrexistentKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int iter = 100;
            var key = "lkey";

            for (int i = 0; i < iter; i++)
            {
                //LLEN
                var count = db.ListLength(key);
                ClassicAssert.AreEqual(0, count);
                count = db.ListLength(key);
                ClassicAssert.AreEqual(0, count);
                ClassicAssert.IsFalse(db.KeyExists(key));

                //LPOP
                var result = db.ListLeftPop(key);
                ClassicAssert.IsTrue(result.IsNull);
                result = db.ListLeftPop(key);
                ClassicAssert.IsTrue(result.IsNull);
                ClassicAssert.IsFalse(db.KeyExists(key));

                //RPOP
                result = db.ListRightPop(key);
                ClassicAssert.IsTrue(result.IsNull);
                result = db.ListRightPop(key);
                ClassicAssert.IsTrue(result.IsNull);
                ClassicAssert.IsFalse(db.KeyExists(key));

                //LPOP count
                var resultArray = db.ListLeftPop(key, 100);
                ClassicAssert.AreEqual(Array.Empty<RedisValue>(), resultArray);
                resultArray = db.ListLeftPop(key, 100);
                ClassicAssert.AreEqual(Array.Empty<RedisValue>(), resultArray);
                ClassicAssert.IsFalse(db.KeyExists(key));

                //RPOP count
                resultArray = db.ListRightPop(key, 100);
                ClassicAssert.AreEqual(Array.Empty<RedisValue>(), resultArray);
                resultArray = db.ListRightPop(key, 100);
                ClassicAssert.AreEqual(Array.Empty<RedisValue>(), resultArray);
                ClassicAssert.IsFalse(db.KeyExists(key));

                //LRANGE
                resultArray = db.ListRange(key);
                ClassicAssert.AreEqual(Array.Empty<RedisValue>(), resultArray);
                resultArray = db.ListRange(key);
                ClassicAssert.AreEqual(Array.Empty<RedisValue>(), resultArray);
                ClassicAssert.IsFalse(db.KeyExists(key));

                //LINDEX
                result = db.ListGetByIndex(key, 15);
                ClassicAssert.IsTrue(result.IsNull);
                result = db.ListGetByIndex(key, 15);
                ClassicAssert.IsTrue(result.IsNull);
                ClassicAssert.IsFalse(db.KeyExists(key));

                //LTRIM
                db.ListTrim(key, 0, 15);
                db.ListTrim(key, 0, 15);
                db.ListTrim(key, 0, 15);
                ClassicAssert.IsFalse(db.KeyExists(key));

                //LREM
                count = db.ListRemove(key, "hello", 100);
                ClassicAssert.AreEqual(0, count);
                count = db.ListRemove(key, "hello", 100);
                ClassicAssert.AreEqual(0, count);
                ClassicAssert.IsFalse(db.KeyExists(key));
            }
        }

        [Test]
        [Repeat(10)]
        public void ListPushPopStressTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 10;
            int ppCount = 100;
            //string[] keys = new string[keyCount];
            HashSet<string> keys = [];
            for (int i = 0; i < keyCount; i++)
                while (!keys.Add(r.Next().ToString())) { }

            ClassicAssert.AreEqual(keyCount, keys.Count, "Unique key initialization failed!");

            var keyArray = keys.ToArray();
            Task[] tasks = new Task[keyArray.Length << 1];
            for (int i = 0; i < tasks.Length; i += 2)
            {
                int idx = i;
                tasks[i] = Task.Run(async () =>
                {
                    var key = keyArray[idx >> 1];
                    for (int j = 0; j < ppCount; j++)
                        await db.ListLeftPushAsync(key, j);
                });

                tasks[i + 1] = Task.Run(() =>
                {
                    var key = keyArray[idx >> 1];
                    for (int j = 0; j < ppCount; j++)
                    {
                        var value = db.ListRightPop(key);
                        while (value.IsNull)
                        {
                            Thread.Yield();
                            value = db.ListRightPop(key);
                        }
                        ClassicAssert.IsTrue((int)value >= 0 && (int)value < ppCount, "Pop value inconsistency");
                    }
                });
            }
            Task.WaitAll(tasks);

            foreach (var key in keyArray)
            {
                var count = db.ListLength(key);
                ClassicAssert.AreEqual(0, count);
            }
        }

        [Test]
        [TestCase(10)]
        [TestCase(24)]
        [TestCase(100)]
        public void CanDoLMoveChunks(int bytesPerSend)
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            lightClientRequest.SendCommandChunks("RPUSH mylist value-one", bytesPerSend);
            lightClientRequest.SendCommandChunks("RPUSH mylist value-two", bytesPerSend);
            lightClientRequest.SendCommandChunks("RPUSH mylist value-three", bytesPerSend);

            var expectedResponse = "$11\r\nvalue-three\r\n";
            var response = lightClientRequest.SendCommandChunks("RPOPLPUSH mylist myotherlist", bytesPerSend);
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            expectedResponse = "*2\r\n$9\r\nvalue-one\r\n$9\r\nvalue-two\r\n";
            response = lightClientRequest.SendCommandChunks("LRANGE mylist 0 -1", bytesPerSend, 3);
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [Category("LMOVE")]
        public void CanDoBasicLMove()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key1 = new RedisKey("mykey1");
            var key1Values = new[] { new RedisValue("myval1"), new RedisValue("myval2"), new RedisValue("myval3") };

            var key2 = new RedisKey("mykey2");
            var key2Values = new[] { new RedisValue("myval4") };

            var pushed = db.ListRightPush(key1, key1Values);
            ClassicAssert.AreEqual(3, pushed);
            pushed = db.ListRightPush(key2, key2Values);
            ClassicAssert.AreEqual(1, pushed);

            var result = db.ListMove(key1, key2, ListSide.Right, ListSide.Left);
            ClassicAssert.AreEqual(key1Values[2], result);
            result = db.ListMove(key1, key2, ListSide.Right, ListSide.Left);
            ClassicAssert.AreEqual(key1Values[1], result);
            result = db.ListMove(key1, key2, ListSide.Right, ListSide.Left);
            ClassicAssert.AreEqual(key1Values[0], result);

            var members = db.ListRange(key2);
            var keys = key1Values.Union(key2Values).ToArray();
            ClassicAssert.AreEqual(keys, members);

            result = db.ListMove(key2, key2, ListSide.Right, ListSide.Right);
            ClassicAssert.AreEqual(key2Values[0], result);

            members = db.ListRange(key2);
            ClassicAssert.AreEqual(keys, members);

            result = db.ListMove(key2, key2, ListSide.Left, ListSide.Left);
            ClassicAssert.AreEqual(key1Values[0], result);

            members = db.ListRange(key2);
            ClassicAssert.AreEqual(keys, members);

            var exists = db.KeyExists(key1);
            ClassicAssert.IsFalse(exists);
        }

        [Test]
        public void CanDoLPopMultipleValues()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var vals = new RedisValue[10];

            for (int i = 1; i <= vals.Length; i++)
            {
                vals[i - 1] = new RedisValue($"valkey-{i}");
            }
            db.ListLeftPush("mylist", vals);
            db.ListRightPop("mylist", 5);
            ClassicAssert.IsTrue(db.ListLength("mylist") == 5);
        }

        [Test]
        public void CanDoLPushXRpushX()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var vals = new RedisValue[10];

            for (int i = 1; i <= vals.Length; i++)
            {
                vals[i - 1] = new RedisValue($"valkey-{i}");
            }

            //this should not create any list
            var result = db.ListLeftPush("mylist", vals, When.Exists);
            ClassicAssert.IsTrue(result == 0);

            var response = db.Execute("MEMORY", "USAGE", "mylist");
            var actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            var expectedResponse = -1;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            // this should create the list
            result = db.ListLeftPush("mylist", vals);
            ClassicAssert.IsTrue(result == 10);

            response = db.Execute("MEMORY", "USAGE", "mylist");
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 904;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //this should not create a new list
            result = db.ListRightPush("myaux-list", vals, When.Exists);
            ClassicAssert.IsTrue(result == 0);

            response = db.Execute("MEMORY", "USAGE", "myaux-list");
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = -1;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            //this should create the list
            result = db.ListRightPush("myaux-list", vals, When.Always);
            ClassicAssert.IsTrue(result == 10);

            response = db.Execute("MEMORY", "USAGE", "myaux-list");
            actualValue = ResultType.Integer == response.Resp2Type ? Int32.Parse(response.ToString()) : -1;
            expectedResponse = 912;
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void CanDoLPushxRPushx()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand("HSET mylist field1 value");

            //this operation should affect only if key already exists and holds a list.
            lightClientRequest.SendCommand("LPUSHX mylist value-two");
            lightClientRequest.SendCommand("RPUSHX mylist value-one");
            response = lightClientRequest.SendCommand("LLEN mylist");

            var expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CheckEmptyListKeyRemoved()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var key = new RedisKey("user1:list");
            var db = redis.GetDatabase(0);
            var values = new[] { new RedisValue("Hello"), new RedisValue("World") };
            var result = db.ListRightPush(key, values);
            ClassicAssert.AreEqual(2, result);

            var actualMembers = db.ListRightPop(key, 2);
            ClassicAssert.AreEqual(values.Length, actualMembers.Length);

            var keyExists = db.KeyExists(key);
            ClassicAssert.IsFalse(keyExists);
        }

        [Test]
        public void CanDoBasicLMPOP()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key1 = new RedisKey("mykey1");
            var key1Values = new[] { new RedisValue("myval1"), new RedisValue("myval2"), new RedisValue("myval3") };

            var key2 = new RedisKey("mykey2");
            var key2Values = new[] { new RedisValue("myval4") };

            var pushed = db.ListRightPush(key1, key1Values);
            ClassicAssert.AreEqual(3, pushed);
            pushed = db.ListRightPush(key2, key2Values);
            ClassicAssert.AreEqual(1, pushed);

            var result = db.ListLeftPop([new RedisKey("test")], 3);
            ClassicAssert.True(result.IsNull);

            result = db.ListRightPop([new RedisKey("test")], 3);
            ClassicAssert.True(result.IsNull);

            result = db.ListLeftPop([key1, key2], 3);
            ClassicAssert.AreEqual(key1, result.Key);
            ClassicAssert.AreEqual(key1Values, result.Values);

            result = db.ListRightPop([new RedisKey("test"), key2], 2);
            ClassicAssert.AreEqual(key2, result.Key);
            key2Values.Reverse();
            ClassicAssert.AreEqual(key2Values, result.Values);
        }

        [Test]
        public void CanDoLMPOPLeftWithoutCount()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key1 = new RedisKey("mykey1");
            var key1Values = new[] { new RedisValue("myval1"), new RedisValue("myval2"), new RedisValue("myval3") };
            var pushed = db.ListRightPush(key1, key1Values);
            ClassicAssert.AreEqual(3, pushed);

            var response = db.Execute("LMPOP", "1", key1.ToString(), "LEFT");

            var result = response.Resp2Type == ResultType.Array ? (string[])response : [];
            ClassicAssert.AreEqual(new string[] { key1.ToString(), key1Values[0].ToString() }, result);
        }

        [Test]
        public void CanDoLMPOPRightMultipleTimes()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key1 = new RedisKey("mykey1");
            var key1Values = new[] { new RedisValue("myval1"), new RedisValue("myval2"), new RedisValue("myval3") };
            var pushed = db.ListLeftPush(key1, key1Values);
            ClassicAssert.AreEqual(3, pushed);

            ListPopResult result;

            for (var i = 0; i < key1Values.Length; i++)
            {
                result = db.ListRightPop([key1], 1);
                ClassicAssert.AreEqual(key1, result.Key);
                ClassicAssert.AreEqual(key1Values[i], result.Values.FirstOrDefault());
            }

            result = db.ListRightPop([key1], 1);
            ClassicAssert.True(result.IsNull);
        }

        [Test]
        public void CanDoRejectBadLMPOPCommand()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var exception = Assert.Throws<RedisServerException>(() => db.Execute("LMPOP", "2", "one", "LEFT"));
            ClassicAssert.AreEqual("ERR syntax error", exception.Message);

            exception = Assert.Throws<RedisServerException>(() => db.Execute("LMPOP", "2", "one", "two"));
            ClassicAssert.AreEqual("ERR syntax error", exception.Message);

            exception = Assert.Throws<RedisServerException>(() => db.Execute("LMPOP", "1", "one", "LEFT", "COUNT"));
            ClassicAssert.AreEqual("ERR syntax error", exception.Message);
        }

        // Issue 945
        [Test]
        public void CanHandleLowerCaseBefore()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            lightClientRequest.SendCommands("RPUSH mylist a", "PING", 1, 1);
            lightClientRequest.SendCommands("RPUSH mylist c", "PING", 1, 1);
            lightClientRequest.SendCommands("LINSERT mylist before c b", "PING", 1, 1);

            var response = lightClientRequest.SendCommands("LRANGE mylist 0 -1", "PING", 4, 1);
            var expectedResponse = "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void CheckListOperationsOnWrongTypeObjectSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var keys = new[] { new RedisKey("user1:obj1"), new RedisKey("user1:obj2") };
            var key1Values = new[] { new RedisValue("Hello"), new RedisValue("World") };
            var key2Values = new[] { new RedisValue("Hola"), new RedisValue("Mundo") };
            var values = new[] { key1Values, key2Values };

            // Set up different type objects
            RespTestsUtils.SetUpTestObjects(db, GarnetObjectType.Set, keys, values);

            // LPOP
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListLeftPop(keys[0]));
            // LPUSH
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListLeftPush(keys[0], values[0]));
            // LPUSHX
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListLeftPush(keys[0], values[0], When.Exists));
            // RPOP
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListRightPop(keys[0]));
            // RPUSH
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListRightPush(keys[0], values[0]));
            // RPUSHX
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListRightPush(keys[0], values[0], When.Exists));
            // LLEN
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListLength(keys[0]));
            // LTRIM
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListTrim(keys[0], 2, 5));
            // LRANGE
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListRange(keys[0], 2, 5));
            // LINDEX
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListGetByIndex(keys[0], 2));
            // LINSERT
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListInsertAfter(keys[0], values[0][0], values[0][1]));
            // LREM
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListRemove(keys[0], values[0][0]));
            // RPOPLPUSH
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListRightPopLeftPush(keys[0], keys[1]));
            // LMOVE
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListMove(keys[0], keys[1], ListSide.Left, ListSide.Right));
            // LSET
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListSetByIndex(keys[0], 2, values[0][1]));
            // LMPOP LEFT
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListLeftPop(keys, 2));
            // LMPOP RIGHT
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.ListRightPop(keys, 3));
        }

        #region LPOS

        [Test]
        [TestCase("a,c,b,c,d", "a", 0)]
        [TestCase("a,c,b,c,adc", "adc", 4)]
        [TestCase("a,c,b,c,d", "c", 1)]
        [TestCase("av,123,bs,c,d", "e", null)]
        public void LPOSWithoutOptions(string items, string find, int? expectedIndex)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "KeyA";
            string[] arguments = [key, .. items.Split(",")];

            db.Execute("RPUSH", arguments);

            var actualIndex = (int?)db.Execute("LPOS", key, find);

            ClassicAssert.AreEqual(expectedIndex, actualIndex);
        }

        [Test]
        public void LPOSWithInvalidKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "KeyA";

            var result = db.Execute("LPOS", key, "nx");
            ClassicAssert.IsTrue(result.IsNull);

            result = db.Execute("LPOS", key, "nx", "COUNT", "3");
            ClassicAssert.IsFalse(result.IsNull);
            ClassicAssert.AreEqual(0, result.Length);

            _ = db.ListLeftPush(key, "e");

            result = db.Execute("LPOS", key, "nx");
            ClassicAssert.IsTrue(result.IsNull);

            result = db.Execute("LPOS", key, "nx", "COUNT", "3");
            ClassicAssert.IsFalse(result.IsNull);
            ClassicAssert.AreEqual(0, result.Length);
        }

        [Test]
        [TestCase("a,c,b,c,d", "c", "1", "rank,1")]
        [TestCase("a,c,b,c,d", "c", "3", "RANK,2")]
        [TestCase("a,c,b,c,d", "c", "null", "rank,3")]
        [TestCase("a,c,b,c,d", "c", "3", "RANK,-1")]
        [TestCase("a,c,b,c,d", "c", "1", "rank,-2")]
        [TestCase("a,c,b,c,d", "c", "null", "RANK,-3")]
        [TestCase("a,c,b,c,d", "a", "null", "rank,2")]
        [TestCase("a,c,b,c,d", "b", "2", "count,2")]
        [TestCase("a,c,b,c,d", "c", "1", "count,1")]
        [TestCase("a,c,b,c,d", "c", "1,3", "COUNT,2")]
        [TestCase("a,c,b,c,d", "c", "1,3", "count,3")]
        [TestCase("a,c,b,c,d", "c", "1,3", "count,0")]
        [TestCase("a,c,b,c,d", "c", "1", "maxlen,0")]
        [TestCase("a,c,b,c,d", "c", "null", "MAXLEN,1")]
        [TestCase("a,c,b,c,d", "c", "1", "maxlen,2")]
        [TestCase("a,c,b,c,d", "c", "null", "rank,-1,maxlen,1")]
        [TestCase("a,c,b,c,d", "c", "3", "rank,-1,maxlen,2")]
        [TestCase("a,c,b,c,d", "c", "null", "rank,-2,maxlen,2")]
        [TestCase("a,c,b,c,d", "c", "null", "rank,1,maxlen,1")]
        [TestCase("a,c,b,c,d", "c", "1", "rank,1,maxlen,2")]
        [TestCase("a,c,b,c,d", "c", "null", "rank,2,maxlen,2")]
        [TestCase("a,c,b,c,d", "c", "3,1", "rank,-1,maxlen,0,count,0")]
        [TestCase("a,c,b,c,d", "c", "3", "rank,-1,maxlen,0,count,1")]
        [TestCase("a,c,b,c,d", "c", "1,3", "rank,1,maxlen,0,count,0")]
        [TestCase("a,c,b,c,d", "c", "1", "rank,1,maxlen,0,count,1")]
        [TestCase("z,b,z,d,e,a,b,c,d,e,a,b,c,d,e,a,b,c,d,e,a,b,c,z,z", "z", "0,2,23,24", "count,0")]  // Test for buffer copy
        public void LPOSWithOptions(string items, string find, string expectedIndexs, string options)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "KeyA";
            string[] pushArguments = [key, .. items.Split(",")];
            string[] lopsArguments = [key, find, .. options.Split(",")];
            var expectedIndexInts = expectedIndexs.Split(",").Select(ToNullableInt).ToList();

            db.Execute("RPUSH", pushArguments);

            if (!options.Contains("count", StringComparison.InvariantCultureIgnoreCase))
            {
                var actualIndex = (int?)db.Execute("LPOS", lopsArguments);

                ClassicAssert.AreEqual(expectedIndexInts[0], actualIndex);
            }
            else
            {
                var actualIndex = (int[])db.Execute("LPOS", lopsArguments);

                ClassicAssert.AreEqual(expectedIndexInts.Count, actualIndex.Length);
                foreach (var index in expectedIndexInts.Zip(actualIndex))
                {
                    ClassicAssert.AreEqual(index.First, index.Second);
                }
            }
        }

        [Test]
        [TestCase("a,c,b,c,d", "c", "1", null, 1, 0)]
        [TestCase("a,c,b,c,d", "c", "3", null, -1, 0)]
        [TestCase("a,c,b,c,d", "c", "1,3", 2, 1, 0)]
        [TestCase("a,c,b,c,d", "c", "3,1", 2, -1, 0)]
        [TestCase("a,c,b,c,d", "c", "1", 2, 1, 3)]
        public void LPOSWithListPosition(string items, string find, string expectedIndexs, int? count, int rank, int maxLength)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "KeyA";
            string[] pushArguments = [key, .. items.Split(",")];
            var expectedIndexInts = expectedIndexs.Split(",").Select(ToNullableInt).ToList();

            db.Execute("RPUSH", pushArguments);

            if (!count.HasValue)
            {
                var actualIndex = db.ListPosition(key, find, rank, maxLength);

                ClassicAssert.AreEqual(expectedIndexInts[0], actualIndex);
            }
            else
            {
                var actualIndexs = db.ListPositions(key, find, count.Value, rank, maxLength);

                ClassicAssert.AreEqual(expectedIndexInts.Count, actualIndexs.Length);
                foreach (var index in expectedIndexInts.Zip(actualIndexs))
                {
                    ClassicAssert.AreEqual(index.First, index.Second);
                }
            }
        }

        [Test]
        [TestCase("a,c,b,c,d", "c", "1", "rank,0")]
        [TestCase("a,c,b,c,d", "c", "3", "count,-1")]
        [TestCase("a,c,b,c,d", "c", "null", "MAXLEN,-5")]
        [TestCase("a,c,b,c,d", "c", "null", "rand,2")]
        [TestCase("a,c,b,c,d", "c", "null", "rank,1,count,-1")]
        public void LPOSWithInvalidOptions(string items, string find, string expectedIndexs, string options)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "KeyA";
            string[] pushArguments = [key, .. items.Split(",")];
            string[] lopsArguments = [key, find, .. options.Split(",")];
            var expectedIndexInts = expectedIndexs.Split(",").Select(ToNullableInt).ToList();
            db.Execute("RPUSH", pushArguments);

            Assert.Throws<RedisServerException>(() => db.Execute("LPOS", lopsArguments));
        }

        private int? ToNullableInt(string s)
        {
            int i;
            if (int.TryParse(s, out i)) return i;
            return null;
        }

        #endregion
    }
}