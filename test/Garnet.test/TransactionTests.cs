// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using NUnit.Framework;
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

            Assert.AreEqual(string1, value1);
            Assert.AreEqual(string2, value2);
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


            Assert.AreEqual(t1.Result, value1);
            Assert.AreEqual(t2.Result, value2);
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
            Assert.AreEqual(t1.Result, value1);
            Assert.AreEqual(string2, value2);
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
                    Assert.AreEqual(results[i / 2].Result, value + i / 2);
                else
                {
                    int counter = (i - 1) / 2 + size;
                    string res = db.StringGet(key + counter);
                    Assert.AreEqual(res, value + counter);
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
                    Assert.AreEqual(results[i / 2].Result, value + i / 2);
                else
                {
                    int counter = (i - 1) / 2 + size;
                    string res = db.StringGet(key + counter);
                    Assert.AreEqual(res, value + counter);
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
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("WATCH key1");
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("MULTI");
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("GET key1");
            expectedResponse = "+QUEUED\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
            res = lightClientRequest.SendCommand("SET key2 value2");
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            await Task.Run(() => updateKey("key1", "value1_updated"));

            res = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "$-1";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");
            lightClientRequest.SendCommand("GET key1");
            lightClientRequest.SendCommand("SET key2 value2");
            res = lightClientRequest.SendCommand("EXEC");

            expectedResponse = "*2\r\n$14\r\nvalue1_updated\r\n+OK\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

        }


        [Test]
        public async Task WatchNonExistentKey()
        {
            var lightClientRequest = TestUtils.CreateRequest();
            byte[] res;

            string expectedResponse = "+OK\r\n";

            res = lightClientRequest.SendCommand("SET key2 value2");
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("WATCH key1");
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("MULTI");
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("GET key2");
            expectedResponse = "+QUEUED\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
            res = lightClientRequest.SendCommand("SET key3 value3");
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            await Task.Run(() => updateKey("key1", "value1"));

            res = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "$-1";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");
            lightClientRequest.SendCommand("GET key1");
            lightClientRequest.SendCommand("SET key2 value2");
            res = lightClientRequest.SendCommand("EXEC");

            expectedResponse = "*2\r\n$6\r\nvalue1\r\n+OK\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

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
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("MULTI");
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            res = lightClientRequest.SendCommand("GET key900");
            expectedResponse = "+QUEUED\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
            res = lightClientRequest.SendCommand("SET key901 value901_updated");
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            await Task.Run(() => updateKey("key1", "value1_updated"));

            res = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "$-1";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");
            lightClientRequest.SendCommand("GET key900");
            lightClientRequest.SendCommand("SET key901 value901_updated");
            res = lightClientRequest.SendCommand("EXEC");

            var buffer_str = System.Text.Encoding.Default.GetString(res);

            expectedResponse = "*2\r\n$8\r\nvalue900\r\n+OK\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
        }

        private void updateKey(string key, string value)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            byte[] res = lightClientRequest.SendCommand("SET " + key + " " + value);
            string expectedResponse = "+OK\r\n";
            Assert.AreEqual(res.AsSpan().Slice(0, expectedResponse.Length).ToArray(), expectedResponse);
        }
    }
}