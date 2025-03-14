// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
        public void TxnExecuteTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var tran = db.CreateTransaction();
            string value1 = "abcdefg1";
            string value2 = "abcdefg2";

            tran.ExecuteAsync("SET", ["mykey1", value1]);
            tran.ExecuteAsync("SET", ["mykey2", value2]);
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

            var response = lightClientRequest.SendCommand("SET key1 value1");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("WATCH key1");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MULTI");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GET key1");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            response = lightClientRequest.SendCommand("SET key2 value2");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            await Task.Run(() => updateKey("key1", "value1_updated"));

            response = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");
            lightClientRequest.SendCommand("GET key1");
            lightClientRequest.SendCommand("SET key2 value2");
            response = lightClientRequest.SendCommand("EXEC");

            expectedResponse = "*2\r\n$14\r\nvalue1_updated\r\n+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public async Task WatchTestWithSetWithEtag()
        {
            var lightClientRequest = TestUtils.CreateRequest();

            var expectedResponse = ":1\r\n";
            var response = lightClientRequest.SendCommand("SET key1 value1 WITHETAG");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            expectedResponse = "+OK\r\n";
            response = lightClientRequest.SendCommand("WATCH key1");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MULTI");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GET key1");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SET key2 value2");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            await Task.Run(() =>
            {
                using var lightClientRequestCopy = TestUtils.CreateRequest();
                string command = "SET key1 value1_updated WITHETAG";
                lightClientRequestCopy.SendCommand(command);
            });

            response = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");

            lightClientRequest.SendCommand("GET key1");
            lightClientRequest.SendCommand("SET key2 value2");
            // check that all the etag commands can be called inside a transaction
            lightClientRequest.SendCommand("SET key3 value2 WITHETAG");
            lightClientRequest.SendCommand("GETWITHETAG key3");
            lightClientRequest.SendCommand("GETIFNOTMATCH key3 1");
            lightClientRequest.SendCommand("SETIFMATCH key3 anotherVal 1");
            lightClientRequest.SendCommand("SET key3 arandomval WITHETAG");

            response = lightClientRequest.SendCommand("EXEC");

            expectedResponse = "*7\r\n$14\r\nvalue1_updated\r\n+OK\r\n:1\r\n*2\r\n:1\r\n$6\r\nvalue2\r\n*2\r\n:1\r\n$-1\r\n*2\r\n:2\r\n$-1\r\n:3\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // check if we still have the appropriate etag on the key we had set
            var otherLighClientRequest = TestUtils.CreateRequest();
            response = otherLighClientRequest.SendCommand("GETWITHETAG key1");
            expectedResponse = "*2\r\n:2\r\n$14\r\nvalue1_updated\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public async Task WatchNonExistentKey()
        {
            var lightClientRequest = TestUtils.CreateRequest();

            var expectedResponse = "+OK\r\n";
            var response = lightClientRequest.SendCommand("SET key2 value2");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("WATCH key1");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MULTI");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GET key2");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SET key3 value3");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            await Task.Run(() => updateKey("key1", "value1"));

            response = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");
            lightClientRequest.SendCommand("GET key1");
            lightClientRequest.SendCommand("SET key2 value2");
            response = lightClientRequest.SendCommand("EXEC");

            expectedResponse = "*2\r\n$6\r\nvalue1\r\n+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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

            // this key should be in the disk
            var response = lightClientRequest.SendCommand("WATCH key1");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MULTI");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GET key900");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SET key901 value901_updated");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            await Task.Run(() => updateKey("key1", "value1_updated"));

            response = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");
            lightClientRequest.SendCommand("GET key900");
            lightClientRequest.SendCommand("SET key901 value901_updated");
            response = lightClientRequest.SendCommand("EXEC");

            expectedResponse = "*2\r\n$8\r\nvalue900\r\n+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        private static void updateKey(string key, string value)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var command = $"SET {key} {value}";
            var response = lightClientRequest.SendCommand(command);

            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }
    }
}