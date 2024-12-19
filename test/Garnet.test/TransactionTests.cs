// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class TransactionTests
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        public void SetUpWithLowMemory()
        {
            TearDown();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();
        }

        [Test]
        public void TxnSetTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var tran = db.CreateTransaction();
            string value1 = "abcdefg1";
            string value2 = "abcdefg2";

            tran.StringSetAsync("mykey1", value1);
            tran.StringSetAsync("mykey2", value2);
            bool committed = tran.Execute();

            string string1 = db.StringGet("mykey1");
            string string2 = db.StringGet("mykey2");

            ClassicAssert.AreEqual(string1, value1);
            ClassicAssert.AreEqual(string2, value2);
        }

        [Test]
        public void TxnGetTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string value1 = "abcdefg1";
            string value2 = "abcdefg2";
            db.StringSet("mykey1", value1);
            db.StringSet("mykey2", value2);

            var tran = db.CreateTransaction();
            var t1 = tran.StringGetAsync("mykey1");
            var t2 = tran.StringGetAsync("mykey2");
            bool committed = tran.Execute();


            ClassicAssert.AreEqual(t1.Result, value1);
            ClassicAssert.AreEqual(t2.Result, value2);
        }

        [Test]
        public void TxnGetSetTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string value1 = "abcdefg1";
            string value2 = "abcdefg2";
            db.StringSet("mykey1", value1);

            var tran = db.CreateTransaction();
            var t1 = tran.StringGetAsync("mykey1");
            var t2 = tran.StringSetAsync("mykey2", value2);
            bool committed = tran.Execute();

            string string2 = db.StringGet("mykey2");
            ClassicAssert.AreEqual(t1.Result, value1);
            ClassicAssert.AreEqual(string2, value2);
        }


        [Test]
        public void LargeTxn([Values(512, 2048, 8192)] int size)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string value = "abcdefg";
            string key = "mykey";
            for (int i = 0; i < size; i++)
                db.StringSet(key + i, value + i);

            Task<RedisValue>[] results = new Task<RedisValue>[size];
            var tran = db.CreateTransaction();
            for (int i = 0; i < size * 2; i++)
            {
                if (i % 2 == 0)
                    results[i / 2] = tran.StringGetAsync(key + i / 2);
                else
                {
                    int counter = (i - 1) / 2 + size;
                    tran.StringSetAsync(key + counter, value + counter);
                }
            }
            bool committed = tran.Execute();
            for (int i = 0; i < size * 2; i++)
            {
                if (i % 2 == 0)
                    ClassicAssert.AreEqual(results[i / 2].Result, value + i / 2);
                else
                {
                    int counter = (i - 1) / 2 + size;
                    string res = db.StringGet(key + counter);
                    ClassicAssert.AreEqual(res, value + counter);
                }
            }
        }

        [Test]
        public void LargeTxnWatch([Values(512, 2048, 4096)] int size)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string value = "abcdefg";
            string key = "mykey";
            for (int i = 0; i < size; i++)
                db.StringSet(key + i, value + i);

            Task<RedisValue>[] results = new Task<RedisValue>[size];
            var tran = db.CreateTransaction();
            for (int i = 0; i < size * 2; i++)
            {
                if (i % 2 == 0)
                {
                    tran.AddCondition(Condition.StringEqual(key + i / 2, value + i / 2));
                    results[i / 2] = tran.StringGetAsync(key + i / 2);
                }
                else
                {
                    int counter = (i - 1) / 2 + size;
                    tran.StringSetAsync(key + counter, value + counter);
                }
            }
            bool committed = tran.Execute();
            for (int i = 0; i < size * 2; i++)
            {
                if (i % 2 == 0)
                    ClassicAssert.AreEqual(results[i / 2].Result, value + i / 2);
                else
                {
                    int counter = (i - 1) / 2 + size;
                    string res = db.StringGet(key + counter);
                    ClassicAssert.AreEqual(res, value + counter);
                }
            }
        }

        [Test]
        public async Task SimpleWatchTest()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            byte[] res;

            res = lightClientRequest.SendCommand("SET key1 value1");
            string expectedResponse = "+OK\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("WATCH key1");
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("MULTI");
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("GET key1");
            expectedResponse = "+QUEUED\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
            res = lightClientRequest.SendCommand("SET key2 value2");
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            await Task.Run(() => updateKey("key1", "value1_updated"));

            res = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");
            lightClientRequest.SendCommand("GET key1");
            lightClientRequest.SendCommand("SET key2 value2");
            res = lightClientRequest.SendCommand("EXEC");

            expectedResponse = "*2\r\n$14\r\nvalue1_updated\r\n+OK\r\n";

            ClassicAssert.AreEqual(
                expectedResponse,
                Encoding.ASCII.GetString(res.AsSpan().Slice(0, expectedResponse.Length)));
        }

        [Test]
        public async Task WatchTestWithSetWithEtag()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            byte[] res;

            string expectedResponse = ":0\r\n";
            res = lightClientRequest.SendCommand("SETWITHETAG key1 value1");
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            expectedResponse = "+OK\r\n";
            res = lightClientRequest.SendCommand("WATCH key1");
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("MULTI");
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("GET key1");
            expectedResponse = "+QUEUED\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
            res = lightClientRequest.SendCommand("SET key2 value2");
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            await Task.Run(() => updateKey("key1", "value1_updated", retainEtag: true));

            res = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");

            lightClientRequest.SendCommand("GET key1");
            lightClientRequest.SendCommand("SET key2 value2");
            // check that all the etag commands can be called inside a transaction
            lightClientRequest.SendCommand("SETWITHETAG key3 value2");
            lightClientRequest.SendCommand("GETWITHETAG key3");
            lightClientRequest.SendCommand("GETIFNOTMATCH key3 0");
            lightClientRequest.SendCommand("SETIFMATCH key3 anotherVal 0");
            lightClientRequest.SendCommand("SETWITHETAG key3 arandomval RETAINETAG");

            res = lightClientRequest.SendCommand("EXEC");

            expectedResponse = "*7\r\n$14\r\nvalue1_updated\r\n+OK\r\n:0\r\n*2\r\n:0\r\n$6\r\nvalue2\r\n+NOTCHANGED\r\n*2\r\n:1\r\n$10\r\nanotherVal\r\n:2\r\n";
            string response = Encoding.ASCII.GetString(res.AsSpan().Slice(0, expectedResponse.Length));
            ClassicAssert.AreEqual(response, expectedResponse);

            // check if we still have the appropriate etag on the key we had set
            var otherLighClientRequest = TestUtils.CreateRequest();
            res = otherLighClientRequest.SendCommand("GETWITHETAG key1");
            expectedResponse = "*2\r\n:1\r\n$14\r\nvalue1_updated\r\n";
            response = Encoding.ASCII.GetString(res.AsSpan().Slice(0, expectedResponse.Length));
            ClassicAssert.AreEqual(response, expectedResponse);
        }

        [Test]
        public async Task WatchNonExistentKey()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            byte[] res;

            string expectedResponse = "+OK\r\n";

            res = lightClientRequest.SendCommand("SET key2 value2");
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("WATCH key1");
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("MULTI");
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("GET key2");
            expectedResponse = "+QUEUED\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
            res = lightClientRequest.SendCommand("SET key3 value3");
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            await Task.Run(() => updateKey("key1", "value1"));

            res = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");
            lightClientRequest.SendCommand("GET key1");
            lightClientRequest.SendCommand("SET key2 value2");
            res = lightClientRequest.SendCommand("EXEC");

            expectedResponse = "*2\r\n$6\r\nvalue1\r\n+OK\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

        }

        [Test]
        public async Task WatchKeyFromDisk()
        {
            SetUpWithLowMemory();
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (int i = 0; i < 1000; i++)
                    db.StringSet("key" + i, "value" + i);
            }

            var lightClientRequest = TestUtils.CreateRequest();
            byte[] res;


            // this key should be in the disk
            res = lightClientRequest.SendCommand("WATCH key1");
            string expectedResponse = "+OK\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("MULTI");
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("GET key900");
            expectedResponse = "+QUEUED\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
            res = lightClientRequest.SendCommand("SET key901 value901_updated");
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            await Task.Run(() => updateKey("key1", "value1_updated"));

            res = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");
            lightClientRequest.SendCommand("GET key900");
            lightClientRequest.SendCommand("SET key901 value901_updated");
            res = lightClientRequest.SendCommand("EXEC");

            var buffer_str = System.Text.Encoding.Default.GetString(res);

            expectedResponse = "*2\r\n$8\r\nvalue900\r\n+OK\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
        }

        private static void updateKey(string key, string value, bool retainEtag = false)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            string command = $"SET {key} {value}";
            command += retainEtag ? " RETAINETAG" : "";
            byte[] res = lightClientRequest.SendCommand(command);
            string expectedResponse = "+OK\r\n";
            ClassicAssert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
        }
    }
}