// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class RespTlsTests : AllureTestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public void TlsSingleSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(disablePubSub: true, useTLS: true));
            var db = redis.GetDatabase(0);

            string origValue = "abcdefg";
            db.StringSet("mykey", origValue);

            string retValue = db.StringGet("mykey");

            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public async Task TlsSingleSetGetGarnetClient()
        {
            using var db = TestUtils.GetGarnetClient(useTLS: true);
            db.Connect();

            string origValue = "abcdefg";
            await db.StringSetAsync("mykey", origValue);

            string retValue = await db.StringGetAsync("mykey");

            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public async Task SingleUnicodeSetGetGarnetClient()
        {
            using var db = TestUtils.GetGarnetClient(useTLS: true);
            db.Connect();

            string origValue = "笑い男";
            await db.StringSetAsync("mykey", origValue);

            string retValue = await db.StringGetAsync("mykey");

            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public void TlsMultiSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(disablePubSub: true, useTLS: true));
            var db = redis.GetDatabase(0);

            const int length = 15000;
            KeyValuePair<RedisKey, RedisValue>[] input = new KeyValuePair<RedisKey, RedisValue>[length];
            for (int i = 0; i < length; i++)
                input[i] = new KeyValuePair<RedisKey, RedisValue>(i.ToString(), i.ToString());

            var result = db.StringSet(input);
            ClassicAssert.IsTrue(result);

            var value = db.StringGet([.. input.Select(e => e.Key)]);
            ClassicAssert.AreEqual(length, value.Length);

            for (int i = 0; i < length; i++)
                ClassicAssert.AreEqual(input[i].Value, value[i]);
        }

        [Test]
        public void TlsLargeSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(disablePubSub: true, useTLS: true));
            var db = redis.GetDatabase(0);

            const int length = (1 << 19) + 100;
            var value = new byte[length];

            for (int i = 0; i < length; i++)
                value[i] = (byte)((byte)'a' + ((byte)i % 26));

            var result = db.StringSet("mykey", value);
            ClassicAssert.IsTrue(result);

            var retvalue = (byte[])db.StringGet("mykey");

            ClassicAssert.IsTrue(new ReadOnlySpan<byte>(value).SequenceEqual(new ReadOnlySpan<byte>(retvalue)));
        }

        [Test]
        public void TlsSetExpiry()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(useTLS: true));
            var db = redis.GetDatabase(0);

            string origValue = "abcdefghij";
            db.StringSet("mykey", origValue, TimeSpan.FromSeconds(1));

            string retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(origValue, retValue);

            Thread.Sleep(2000);
            retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(null, retValue);
        }

        [Test]
        public void TlsSetExpiryNx()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(useTLS: true));
            var db = redis.GetDatabase(0);

            string origValue = "abcdefghij";
            db.StringSet("mykey", origValue, TimeSpan.FromSeconds(3), When.NotExists, CommandFlags.None);

            string retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(origValue, retValue);

            Thread.Sleep(4000);
            retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(null, retValue);
        }

        [Test]
        public void TlsSetXx()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(useTLS: true));
            var db = redis.GetDatabase(0);

            string key = "mykey";
            string origValue = "abcdefghij";

            var result = db.StringSet(key, origValue, null, When.Exists, CommandFlags.None);
            ClassicAssert.IsFalse(result);

            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(null, retValue);

            result = db.StringSet(key, origValue);
            ClassicAssert.IsTrue(result);

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(origValue, retValue);

            string newValue = "01234";

            result = db.StringSet(key, newValue, TimeSpan.FromSeconds(1), When.Exists, CommandFlags.None);
            ClassicAssert.IsTrue(result);

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue, retValue);
        }

        [Test]
        public void TlsSetExpiryIncr()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(useTLS: true));
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = -100000;
            var strKey = "key1";
            db.StringSet(strKey, nVal, TimeSpan.FromSeconds(1));

            long n = db.StringIncrement(strKey);
            long nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(-99999, nRetVal);

            n = db.StringIncrement(strKey);
            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(-99998, nRetVal);

            Thread.Sleep(5000);

            // Expired key, restart increment
            n = db.StringIncrement(strKey);
            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(1, nRetVal);
        }

        [Test]
        public void TlsLockTakeRelease()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(useTLS: true));
            var db = redis.GetDatabase(0);

            string key = "lock-key";
            string value = "lock-value";

            var success = db.LockTake(key, value, TimeSpan.FromSeconds(100));
            ClassicAssert.IsTrue(success);

            success = db.LockTake(key, value, TimeSpan.FromSeconds(100));
            ClassicAssert.IsFalse(success);

            success = db.LockRelease(key, value);
            ClassicAssert.IsTrue(success);

            success = db.LockRelease(key, value);
            ClassicAssert.IsFalse(success);

            success = db.LockTake(key, value, TimeSpan.FromSeconds(100));
            ClassicAssert.IsTrue(success);

            success = db.LockRelease(key, value);
            ClassicAssert.IsTrue(success);

            // Test auto-lock-release
            success = db.LockTake(key, value, TimeSpan.FromSeconds(1));
            ClassicAssert.IsTrue(success);

            Thread.Sleep(2000);
            success = db.LockTake(key, value, TimeSpan.FromSeconds(1));
            ClassicAssert.IsTrue(success);

            success = db.LockRelease(key, value);
            ClassicAssert.IsTrue(success);
        }

        [Test]
        [TestCase("key1", 1000)]
        public void TlsSingleDecr(string strKey, int nVal)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(useTLS: true));
            var db = redis.GetDatabase(0);

            // Key storing integer
            db.StringSet(strKey, nVal);
            long n = db.StringDecrement(strKey);
            long nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
        }

        [Test]
        [TestCase(-1000, 100)]
        [TestCase(-1000, -9000)]
        [TestCase(-10000, 9000)]
        [TestCase(9000, 10000)]
        public void TlsSingleDecrBy(long nVal, long nDecr)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(useTLS: true));
            var db = redis.GetDatabase(0);
            // Key storing integer val
            var strKey = "key1";
            db.StringSet(strKey, nVal);
            long n = db.StringDecrement(strKey, nDecr);

            int nRetVal = Convert.ToInt32(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
        }

        [Test]
        public void TlsSingleIncrNoKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(useTLS: true));
            var db = redis.GetDatabase(0);

            // Key storing integer
            var strKey = "key1";
            db.StringIncrement(strKey);

            int retVal = Convert.ToInt32(db.StringGet(strKey));

            ClassicAssert.AreEqual(1, retVal);
        }

        [Test]
        public void TlsSingleDelete()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(useTLS: true));
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = 100;
            var strKey = "key1";
            db.StringSet(strKey, nVal);
            db.KeyDelete(strKey);
            var retVal = Convert.ToBoolean(db.StringGet(strKey));
            ClassicAssert.AreEqual(retVal, false);
        }

        [Test]
        public void TlsSingleExists()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(useTLS: true));
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = 100;
            var strKey = "key1";
            db.StringSet(strKey, nVal);

            bool fExists = db.KeyExists("key1", CommandFlags.None);
            ClassicAssert.AreEqual(fExists, true);

            fExists = db.KeyExists("key2", CommandFlags.None);
            ClassicAssert.AreEqual(fExists, false);
        }

        [Test]
        public void TlsSingleRename()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(useTLS: true));
            var db = redis.GetDatabase(0);

            string origValue = "test1";
            db.StringSet("key1", origValue);

            db.KeyRename("key1", "key2");
            string retValue = db.StringGet("key2");

            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public void TlsCanSelectCommand()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(useTLS: true));
            var db = redis.GetDatabase(0);
            var reply = db.Execute("SELECT", "0");
            ClassicAssert.IsTrue(reply.ToString() == "OK");
            Assert.Throws<RedisServerException>(() => db.Execute("SELECT", "17"));

            //select again the def db
            db.Execute("SELECT", "0");
        }

        [Test]
        public void TlsCanSelectCommandLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest(useTLS: true, countResponseType: CountResponseType.Bytes);

            var expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_DB_INDEX_OUT_OF_RANGE)}\r\n+PONG\r\n";
            var response = lightClientRequest.Execute("SELECT 17", "PING", expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, response);
        }

        [Test]
        public void TlsPlainTextCommandLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest(useTLS: false, countResponseType: CountResponseType.Bytes);
            Assert.Throws<GarnetException>(() => lightClientRequest.Execute("PING", 1));
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void TlsCanDoCommandsInChunks(int bytesSent)
        {
            // SETEX
            using var lightClientRequest = TestUtils.CreateRequest(useTLS: true, countResponseType: CountResponseType.Bytes);

            var expectedResponse = "+OK\r\n";
            var response = lightClientRequest.Execute("SETEX mykey 1 abcdefghij", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            // GET
            expectedResponse = "$10\r\nabcdefghij\r\n";
            response = lightClientRequest.Execute("GET mykey", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            Thread.Sleep(2000);

            // GET
            expectedResponse = "$-1\r\n";
            response = lightClientRequest.Execute("GET mykey", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            // DECR            
            expectedResponse = "+OK\r\n";
            response = lightClientRequest.Execute("SET mykeydecr 1", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = ":0\r\n";
            response = lightClientRequest.Execute("DECR mykeydecr", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "$1\r\n0\r\n";
            response = lightClientRequest.Execute("GET mykeydecr", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            // DEL
            expectedResponse = ":1\r\n";
            response = lightClientRequest.Execute("DEL mykeydecr", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "$-1\r\n";
            response = lightClientRequest.Execute("GET mykeydecr", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            // EXISTS
            expectedResponse = ":0\r\n";
            response = lightClientRequest.Execute("EXISTS mykeydecr", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            // SET
            expectedResponse = "+OK\r\n";
            response = lightClientRequest.Execute("SET mykey 1", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            // RENAME
            expectedResponse = "+OK\r\n";
            response = lightClientRequest.Execute("RENAME mykey mynewkey", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            // GET
            expectedResponse = "$1\r\n1\r\n";
            response = lightClientRequest.Execute("GET mynewkey", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);
        }


        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void TlsCanSetGetCommandsChunks(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest(useTLS: true, countResponseType: CountResponseType.Bytes);
            var sb = new StringBuilder();

            for (int i = 1; i <= 100; i++)
            {
                sb.Append($" mykey-{i} {i * 10}");
            }

            // MSET
            var expectedResponse = "+OK\r\n";
            var response = lightClientRequest.Execute($"MSET{sb}", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = ":100\r\n";
            response = lightClientRequest.Execute($"DBSIZE", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            sb.Clear();
            for (int i = 1; i <= 100; i++)
            {
                sb.Append($" mykey-{i}");
            }

            // MGET
            expectedResponse = "*100\r\n$2\r\n10\r\n$2\r\n20\r\n$2\r\n30\r\n$2\r\n40\r\n$2\r\n50\r\n$2\r\n60\r\n$2\r\n70\r\n$2\r\n80\r\n$2\r\n90\r\n$3\r\n100\r\n$3\r\n110\r\n$3\r\n120\r\n$3\r\n130\r\n$3\r\n140\r\n$3\r\n150\r\n$3\r\n160\r\n$3\r\n170\r\n$3\r\n180\r\n$3\r\n190\r\n$3\r\n200\r\n$3\r\n210\r\n$3\r\n220\r\n$3\r\n230\r\n$3\r\n240\r\n$3\r\n250\r\n$3\r\n260\r\n$3\r\n270\r\n$3\r\n280\r\n$3\r\n290\r\n$3\r\n300\r\n$3\r\n310\r\n$3\r\n320\r\n$3\r\n330\r\n$3\r\n340\r\n$3\r\n350\r\n$3\r\n360\r\n$3\r\n370\r\n$3\r\n380\r\n$3\r\n390\r\n$3\r\n400\r\n$3\r\n410\r\n$3\r\n420\r\n$3\r\n430\r\n$3\r\n440\r\n$3\r\n450\r\n$3\r\n460\r\n$3\r\n470\r\n$3\r\n480\r\n$3\r\n490\r\n$3\r\n500\r\n$3\r\n510\r\n$3\r\n520\r\n$3\r\n530\r\n$3\r\n540\r\n$3\r\n550\r\n$3\r\n560\r\n$3\r\n570\r\n$3\r\n580\r\n$3\r\n590\r\n$3\r\n600\r\n$3\r\n610\r\n$3\r\n620\r\n$3\r\n630\r\n$3\r\n640\r\n$3\r\n650\r\n$3\r\n660\r\n$3\r\n670\r\n$3\r\n680\r\n$3\r\n690\r\n$3\r\n700\r\n$3\r\n710\r\n$3\r\n720\r\n$3\r\n730\r\n$3\r\n740\r\n$3\r\n750\r\n$3\r\n760\r\n$3\r\n770\r\n$3\r\n780\r\n$3\r\n790\r\n$3\r\n800\r\n$3\r\n810\r\n$3\r\n820\r\n$3\r\n830\r\n$3\r\n840\r\n$3\r\n850\r\n$3\r\n860\r\n$3\r\n870\r\n$3\r\n880\r\n$3\r\n890\r\n$3\r\n900\r\n$3\r\n910\r\n$3\r\n920\r\n$3\r\n930\r\n$3\r\n940\r\n$3\r\n950\r\n$3\r\n960\r\n$3\r\n970\r\n$3\r\n980\r\n$3\r\n990\r\n$4\r\n1000\r\n";
            response = lightClientRequest.Execute($"MGET{sb}", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);
        }
    }
}