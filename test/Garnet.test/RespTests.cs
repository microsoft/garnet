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
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespTests
    {
        GarnetServer server;
        Random r;

        [SetUp]
        public void Setup()
        {
            r = new Random(674386);
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        /// <summary>
        /// Ensure that all RespCommand IDs are unique.
        /// </summary>
        [Test]
        public void UniqueRespCommandIds()
        {
            var ids = (RespCommand[])Enum.GetValues(typeof(RespCommand));

            // Isolate command IDs that exist more than once in the array
            var duplicateIds = ids.GroupBy(e => e).Where(e => e.Count() > 1).Select(e => e.First());

            Assert.IsEmpty(duplicateIds, "Found ambiguous command IDs");
        }

        [Test]
        public void SingleSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "abcdefg";
            db.StringSet("mykey", origValue);

            string retValue = db.StringGet("mykey");

            Assert.AreEqual(origValue, retValue);
        }

        [Test]
        public async Task SingleSetGetGarnetClient()
        {
            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            string origValue = "abcdefg";
            await db.StringSetAsync("mykey", origValue);

            string retValue = await db.StringGetAsync("mykey");

            Assert.AreEqual(origValue, retValue);
        }

        [Test]
        public void MultiSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const int length = 15000;
            KeyValuePair<RedisKey, RedisValue>[] input = new KeyValuePair<RedisKey, RedisValue>[length];
            for (int i = 0; i < length; i++)
                input[i] = new KeyValuePair<RedisKey, RedisValue>(i.ToString(), i.ToString());

            // MSET NX - non-existing values
            var result = db.StringSet(input, When.NotExists);
            Assert.IsTrue(result);

            var value = db.StringGet(input.Select(e => e.Key).ToArray());
            Assert.AreEqual(length, value.Length);

            for (int i = 0; i < length; i++)
                Assert.AreEqual(input[i].Value, value[i]);

            // MSET
            result = db.StringSet(input);
            Assert.IsTrue(result);

            value = db.StringGet(input.Select(e => e.Key).ToArray());
            Assert.AreEqual(length, value.Length);

            for (int i = 0; i < length; i++)
                Assert.AreEqual(input[i].Value, value[i]);

            // MSET NX - existing values
            for (int i = 0; i < length; i++)
                input[i] = new KeyValuePair<RedisKey, RedisValue>(i.ToString(), (i + 1).ToString());

            result = db.StringSet(input, When.NotExists);
            Assert.IsFalse(result);

            value = db.StringGet(input.Select(e => e.Key).ToArray());
            Assert.AreEqual(length, value.Length);

            for (int i = 0; i < length; i++)
                Assert.AreEqual(new RedisValue(i.ToString()), value[i]);

            // MSET NX - non-existing and existing values
            for (int i = 0; i < length; i++)
                input[i] = new KeyValuePair<RedisKey, RedisValue>((i % 2 == 0 ? i : i + length).ToString(), (i + length).ToString());

            result = db.StringSet(input, When.NotExists);
            Assert.IsTrue(result);

            value = db.StringGet(input.Select(e => e.Key).ToArray());
            Assert.AreEqual(length, value.Length);

            for (int i = 0; i < length; i++)
            {
                Assert.AreEqual(i % 2 == 0 ? new RedisValue((int.Parse(input[i].Value) - length).ToString()) :
                        input[i].Value, value[i]);
            }
        }

        [Test]
        public void LargeSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const int length = (1 << 19) + 100;
            var value = new byte[length];

            for (int i = 0; i < length; i++)
                value[i] = (byte)((byte)'a' + ((byte)i % 26));

            var result = db.StringSet("mykey", value);
            Assert.IsTrue(result);

            var retvalue = (byte[])db.StringGet("mykey");

            Assert.IsTrue(new ReadOnlySpan<byte>(value).SequenceEqual(new ReadOnlySpan<byte>(retvalue)));
        }

        [Test]
        public void SetExpiry()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "abcdefghij";
            db.StringSet("mykey", origValue, TimeSpan.FromSeconds(1));

            string retValue = db.StringGet("mykey");
            Assert.AreEqual(origValue, retValue);

            Thread.Sleep(2000);
            retValue = db.StringGet("mykey");
            Assert.AreEqual(null, retValue);
        }

        [Test]
        public void SetExpiryHighPrecision()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "abcdefghij";
            db.StringSet("mykey", origValue, TimeSpan.FromSeconds(1.9));

            string retValue = db.StringGet("mykey");
            Assert.AreEqual(origValue, retValue);

            Thread.Sleep(1000);
            retValue = db.StringGet("mykey");
            Assert.AreEqual(origValue, retValue);

            Thread.Sleep(2000);
            retValue = db.StringGet("mykey");
            Assert.AreEqual(null, retValue);
        }

        [Test]
        public void SetExpiryNx()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "abcdefghij";
            db.StringSet("mykey", origValue, TimeSpan.FromSeconds(3), When.NotExists, CommandFlags.None);

            string retValue = db.StringGet("mykey");
            Assert.AreEqual(origValue, retValue);

            Thread.Sleep(4000);
            retValue = db.StringGet("mykey");
            Assert.AreEqual(null, retValue);
        }

        [Test]
        public void SetXx()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "mykey";
            string origValue = "abcdefghij";

            var result = db.StringSet(key, origValue, null, When.Exists, CommandFlags.None);
            Assert.IsFalse(result);

            string retValue = db.StringGet(key);
            Assert.AreEqual(null, retValue);

            result = db.StringSet(key, origValue);
            Assert.IsTrue(result);

            retValue = db.StringGet(key);
            Assert.AreEqual(origValue, retValue);

            string newValue = "01234";

            result = db.StringSet(key, newValue, TimeSpan.FromSeconds(10), When.Exists, CommandFlags.None);
            Assert.IsTrue(result);

            retValue = db.StringGet(key);
            Assert.AreEqual(newValue, retValue);
        }

        [Test]
        public void SetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "mykey";
            string origValue = "abcdefghijklmnopqrst";

            // Initial set
            var result = db.StringSet(key, origValue);
            Assert.IsTrue(result);
            string retValue = db.StringGet(key);
            Assert.AreEqual(origValue, retValue);

            // Smaller new value without expiration
            string newValue1 = "abcdefghijklmnopqrs";
            retValue = db.StringSetAndGet(key, newValue1, null, When.Always, CommandFlags.None);
            Assert.AreEqual(origValue, retValue);
            retValue = db.StringGet(key);
            Assert.AreEqual(newValue1, retValue);

            // Smaller new value with KeepTtl
            string newValue2 = "abcdefghijklmnopqr";
            retValue = db.StringSetAndGet(key, newValue2, null, true, When.Always, CommandFlags.None);
            Assert.AreEqual(newValue1, retValue);
            retValue = db.StringGet(key);
            Assert.AreEqual(newValue2, retValue);
            var expiry = db.KeyTimeToLive(key);
            Assert.IsNull(expiry);

            // Smaller new value with expiration
            string newValue3 = "01234";
            retValue = db.StringSetAndGet(key, newValue3, TimeSpan.FromSeconds(10), When.Exists, CommandFlags.None);
            Assert.AreEqual(newValue2, retValue);
            retValue = db.StringGet(key);
            Assert.AreEqual(newValue3, retValue);
            expiry = db.KeyTimeToLive(key);
            Assert.IsTrue(expiry.Value.TotalSeconds > 0);

            // Larger new value with expiration
            string newValue4 = "abcdefghijklmnopqrstabcdefghijklmnopqrst";
            retValue = db.StringSetAndGet(key, newValue4, TimeSpan.FromSeconds(100), When.Exists, CommandFlags.None);
            Assert.AreEqual(newValue3, retValue);
            retValue = db.StringGet(key);
            Assert.AreEqual(newValue4, retValue);
            expiry = db.KeyTimeToLive(key);
            Assert.IsTrue(expiry.Value.TotalSeconds > 0);

            // Smaller new value without expiration
            string newValue5 = "0123401234";
            retValue = db.StringSetAndGet(key, newValue5, null, When.Exists, CommandFlags.None);
            Assert.AreEqual(newValue4, retValue);
            retValue = db.StringGet(key);
            Assert.AreEqual(newValue5, retValue);
            expiry = db.KeyTimeToLive(key);
            Assert.IsNull(expiry);

            // Larger new value without expiration
            string newValue6 = "abcdefghijklmnopqrstabcdefghijklmnopqrst";
            retValue = db.StringSetAndGet(key, newValue6, null, When.Always, CommandFlags.None);
            Assert.AreEqual(newValue5, retValue);
            retValue = db.StringGet(key);
            Assert.AreEqual(newValue6, retValue);
            expiry = db.KeyTimeToLive(key);
            Assert.IsNull(expiry);
        }


        [Test]
        public void SetExpiryIncr()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = -100000;
            var strKey = "key1";
            db.StringSet(strKey, nVal, TimeSpan.FromSeconds(1));

            long n = db.StringIncrement(strKey);
            long nRetVal = Convert.ToInt64(db.StringGet(strKey));
            Assert.AreEqual(n, nRetVal);
            Assert.AreEqual(-99999, nRetVal);

            n = db.StringIncrement(strKey);
            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            Assert.AreEqual(n, nRetVal);
            Assert.AreEqual(-99998, nRetVal);

            Thread.Sleep(5000);

            // Expired key, restart increment
            n = db.StringIncrement(strKey);
            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            Assert.AreEqual(n, nRetVal);
            Assert.AreEqual(1, nRetVal);
        }

        [Test]
        public void SetOptionsCaseSensitivityTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "csKey";
            var value = 1;
            var setCommand = "SET";
            var ttlCommand = "TTL";
            var okResponse = "OK";

            // xx
            var resp = (string)db.Execute($"{setCommand}", key, value, "xx");
            Assert.IsNull(resp);

            Assert.IsTrue(db.StringSet(key, value));

            // nx
            resp = (string)db.Execute($"{setCommand}", key, value, "nx");
            Assert.IsNull(resp);

            // ex
            resp = (string)db.Execute($"{setCommand}", key, value, "ex", "1");
            Assert.AreEqual(okResponse, resp);
            Thread.Sleep(TimeSpan.FromSeconds(1.1));
            resp = (string)db.Execute($"{ttlCommand}", key);
            Assert.IsTrue(int.TryParse(resp, out var ttl) && ttl == -1);

            // px
            resp = (string)db.Execute($"{setCommand}", key, value, "px", "1000");
            Assert.AreEqual(okResponse, resp);
            Thread.Sleep(TimeSpan.FromSeconds(1.1));
            resp = (string)db.Execute($"{ttlCommand}", key);
            Assert.IsTrue(int.TryParse(resp, out ttl) && ttl == -1);

            // keepttl
            Assert.IsTrue(db.StringSet(key, 1, TimeSpan.FromMinutes(1)));
            resp = (string)db.Execute($"{setCommand}", key, value, "keepttl");
            Assert.AreEqual(okResponse, resp);
            resp = (string)db.Execute($"{ttlCommand}", key);
            Assert.IsTrue(int.TryParse(resp, out ttl) && ttl > 0 && ttl < 60);

            // ex .. nx, non-existing key
            Assert.IsTrue(db.KeyDelete(key));
            resp = (string)db.Execute($"{setCommand}", key, value, "ex", "1", "nx");
            Assert.AreEqual(okResponse, resp);
            Thread.Sleep(TimeSpan.FromSeconds(1.1));
            resp = (string)db.Execute($"{ttlCommand}", key);
            Assert.IsTrue(int.TryParse(resp, out ttl) && ttl == -1);

            // ex .. nx, existing key
            Assert.IsTrue(db.StringSet(key, value));
            resp = (string)db.Execute($"{setCommand}", key, value, "ex", "1", "nx");
            Assert.IsNull(resp);

            // ex .. xx, non-existing key
            Assert.IsTrue(db.KeyDelete(key));
            resp = (string)db.Execute($"{setCommand}", key, value, "ex", "1", "xx");
            Assert.IsNull(resp);

            // ex .. xx, existing key
            Assert.IsTrue(db.StringSet(key, value));
            resp = (string)db.Execute($"{setCommand}", key, value, "ex", "1", "xx");
            Assert.AreEqual(okResponse, resp);
            Thread.Sleep(TimeSpan.FromSeconds(1.1));
            resp = (string)db.Execute($"{ttlCommand}", key);
            Assert.IsTrue(int.TryParse(resp, out ttl) && ttl == -1);

            // px .. nx, non-existing key
            Assert.IsTrue(db.KeyDelete(key));
            resp = (string)db.Execute($"{setCommand}", key, value, "px", "1000", "nx");
            Assert.AreEqual(okResponse, resp);
            Thread.Sleep(TimeSpan.FromSeconds(1.1));
            resp = (string)db.Execute($"{ttlCommand}", key);
            Assert.IsTrue(int.TryParse(resp, out ttl) && ttl == -1);

            // px .. nx, existing key
            Assert.IsTrue(db.StringSet(key, value));
            resp = (string)db.Execute($"{setCommand}", key, value, "px", "1000", "nx");
            Assert.IsNull(resp);

            // px .. xx, non-existing key
            Assert.IsTrue(db.KeyDelete(key));
            resp = (string)db.Execute($"{setCommand}", key, value, "px", "1000", "xx");
            Assert.IsNull(resp);

            // px .. xx, existing key
            Assert.IsTrue(db.StringSet(key, value));
            resp = (string)db.Execute($"{setCommand}", key, value, "px", "1000", "xx");
            Assert.AreEqual(okResponse, resp);
            Thread.Sleep(TimeSpan.FromSeconds(1.1));
            resp = (string)db.Execute($"{ttlCommand}", key);
            Assert.IsTrue(int.TryParse(resp, out ttl) && ttl == -1);
        }

        [Test]
        public void LockTakeRelease()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "lock-key";
            string value = "lock-value";

            var success = db.LockTake(key, value, TimeSpan.FromSeconds(100));
            Assert.IsTrue(success);

            success = db.LockTake(key, value, TimeSpan.FromSeconds(100));
            Assert.IsFalse(success);

            success = db.LockRelease(key, value);
            Assert.IsTrue(success);

            success = db.LockRelease(key, value);
            Assert.IsFalse(success);

            success = db.LockTake(key, value, TimeSpan.FromSeconds(100));
            Assert.IsTrue(success);

            success = db.LockRelease(key, value);
            Assert.IsTrue(success);

            // Test auto-lock-release
            success = db.LockTake(key, value, TimeSpan.FromSeconds(1));
            Assert.IsTrue(success);

            Thread.Sleep(2000);
            success = db.LockTake(key, value, TimeSpan.FromSeconds(1));
            Assert.IsTrue(success);

            success = db.LockRelease(key, value);
            Assert.IsTrue(success);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void SingleIncr(int bytesPerSend)
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseLength: true);

            // Key storing integer
            var nVal = -100000;
            var strKey = "key1";

            var expectedResponse = "+OK\r\n";
            var response = lightClientRequest.Execute($"SET {strKey} {nVal}", expectedResponse.Length, bytesPerSend);
            Assert.AreEqual(expectedResponse, response);

            expectedResponse = "$7\r\n-100000\r\n";
            response = lightClientRequest.Execute($"GET {strKey}", expectedResponse.Length, bytesPerSend);
            Assert.AreEqual(expectedResponse, response);

            expectedResponse = ":-99999\r\n";
            response = lightClientRequest.Execute($"INCR {strKey}", expectedResponse.Length, bytesPerSend);
            Assert.AreEqual(expectedResponse, response);

            expectedResponse = "$6\r\n-99999\r\n";
            response = lightClientRequest.Execute($"GET {strKey}", expectedResponse.Length, bytesPerSend);
            Assert.AreEqual(expectedResponse, response);
        }

        [Test]
        [TestCase(9999, 10)]
        [TestCase(9999, 50)]
        [TestCase(9999, 100)]
        public void SingleIncrBy(long nIncr, int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseLength: true);

            // Key storing integer
            var nVal = 1000;
            var strKey = "key1";

            var expectedResponse = "+OK\r\n";
            var response = lightClientRequest.Execute($"SET {strKey} {nVal}", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            expectedResponse = "$4\r\n1000\r\n";
            response = lightClientRequest.Execute($"GET {strKey}", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            expectedResponse = $":{nIncr + nVal}\r\n";
            response = lightClientRequest.Execute($"INCRBY {strKey} {nIncr}", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            expectedResponse = $"${(nIncr + nVal).ToString().Length}\r\n{nIncr + nVal}\r\n";
            response = lightClientRequest.Execute($"GET {strKey}", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);
        }

        [Test]
        [TestCase("key1", 1000)]
        public void SingleDecr(string strKey, int nVal)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Key storing integer
            db.StringSet(strKey, nVal);
            long n = db.StringDecrement(strKey);
            long nRetVal = Convert.ToInt64(db.StringGet(strKey));
            Assert.AreEqual(n, nRetVal);
        }

        [Test]
        [TestCase(-1000, 100)]
        [TestCase(-1000, -9000)]
        [TestCase(-10000, 9000)]
        [TestCase(9000, 10000)]
        public void SingleDecrBy(long nVal, long nDecr)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            // Key storing integer val
            var strKey = "key1";
            db.StringSet(strKey, nVal);
            long n = db.StringDecrement(strKey, nDecr);

            int nRetVal = Convert.ToInt32(db.StringGet(strKey));
            Assert.AreEqual(n, nRetVal);
        }

        [Test]
        public void SingleDecrByNoKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            long decrBy = 1000;

            // Key storing integer
            var strKey = "key1";
            db.StringDecrement(strKey, decrBy);

            var retValStr = db.StringGet(strKey).ToString();
            int retVal = Convert.ToInt32(retValStr);

            Assert.AreEqual(-decrBy, retVal);
        }

        [Test]
        public void SingleIncrNoKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Key storing integer
            var strKey = "key1";
            db.StringIncrement(strKey);

            int retVal = Convert.ToInt32(db.StringGet(strKey));

            Assert.AreEqual(1, retVal);

            // Key storing integer
            strKey = "key2";
            db.StringDecrement(strKey);

            retVal = Convert.ToInt32(db.StringGet(strKey));

            Assert.AreEqual(-1, retVal);
        }

        [Test]
        public void SingleDelete()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = 100;
            var strKey = "key1";
            db.StringSet(strKey, nVal);
            db.KeyDelete(strKey);
            var retVal = Convert.ToBoolean(db.StringGet(strKey));
            Assert.AreEqual(retVal, false);
        }

        [Test]
        public void SingleDeleteWithObjectStoreDisabled()
        {
            TearDown();

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: true);
            server.Start();

            var key = "delKey";
            var value = "1234";
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.StringSet(key, value);

            var resp = (string)db.StringGet(key);
            Assert.AreEqual(resp, value);

            var respDel = db.KeyDelete(key);
            Assert.IsTrue(respDel);

            respDel = db.KeyDelete(key);
            Assert.IsFalse(respDel);
        }

        private string GetRandomString(int len)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, len)
                .Select(s => s[r.Next(s.Length)]).ToArray());
        }

        [Test]
        public void SingleDeleteWithObjectStoreDisable_LTM()
        {
            TearDown();

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, DisableObjects: true);
            server.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 5;
            int valLen = 256;
            int keyLen = 8;

            List<Tuple<string, string>> data = new();
            for (int i = 0; i < keyCount; i++)
            {
                data.Add(new Tuple<string, string>(GetRandomString(keyLen), GetRandomString(valLen)));
                var pair = data.Last();
                db.StringSet(pair.Item1, pair.Item2);
            }


            for (int i = 0; i < keyCount; i++)
            {
                var pair = data[i];

                var resp = (string)db.StringGet(pair.Item1);
                Assert.AreEqual(resp, pair.Item2);

                var respDel = db.KeyDelete(pair.Item1);
                resp = (string)db.StringGet(pair.Item1);
                Assert.IsNull(resp);

                respDel = db.KeyDelete(pair.Item2);
                Assert.IsFalse(respDel);
            }
        }

        [Test]
        public void MultiKeyDelete([Values] bool withoutObjectStore)
        {
            if (withoutObjectStore)
            {
                TearDown();
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: true);
                server.Start();
            }

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 10;
            int valLen = 16;
            int keyLen = 8;

            List<Tuple<string, string>> data = new();
            for (int i = 0; i < keyCount; i++)
            {
                data.Add(new Tuple<string, string>(GetRandomString(keyLen), GetRandomString(valLen)));
                var pair = data.Last();
                db.StringSet(pair.Item1, pair.Item2);
            }

            var keys = data.Select(x => (RedisKey)x.Item1).ToArray();
            var keysDeleted = db.KeyDeleteAsync(keys);
            keysDeleted.Wait();
            Assert.AreEqual(keysDeleted.Result, 10);

            var keysDel = db.KeyDelete(keys);
            Assert.AreEqual(keysDel, 0);
        }

        [Test]
        public void MultiKeyDeleteObjectStore()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 10;
            int setCount = 3;
            int valLen = 16;
            int keyLen = 8;

            List<string> keys = new();
            for (int i = 0; i < keyCount; i++)
            {
                keys.Add(GetRandomString(keyLen));
                var key = keys.Last();

                for (int j = 0; j < setCount; j++)
                {
                    var member = GetRandomString(valLen);
                    db.SetAdd(key, member);
                }
            }

            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();
            var keysDeleted = db.KeyDeleteAsync(redisKeys);
            keysDeleted.Wait();
            Assert.AreEqual(keysDeleted.Result, 10);

            var keysDel = db.KeyDelete(redisKeys);
            Assert.AreEqual(keysDel, 0);
        }

        [Test]
        public void MultiKeyUnlink([Values] bool withoutObjectStore)
        {
            if (withoutObjectStore)
            {
                TearDown();
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: true);
                server.Start();
            }

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 10;
            int valLen = 16;
            int keyLen = 8;

            List<Tuple<string, string>> data = new();
            for (int i = 0; i < keyCount; i++)
            {
                data.Add(new Tuple<string, string>(GetRandomString(keyLen), GetRandomString(valLen)));
                var pair = data.Last();
                db.StringSet(pair.Item1, pair.Item2);
            }

            var keys = data.Select(x => (object)x.Item1).ToArray();
            var keysDeleted = (string)db.Execute("unlink", keys);
            Assert.AreEqual(10, int.Parse(keysDeleted));

            keysDeleted = (string)db.Execute("unlink", keys);
            Assert.AreEqual(0, int.Parse(keysDeleted));
        }

        [Test]
        public void MultiKeyUnlinkObjectStore()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 10;
            int setCount = 3;
            int valLen = 16;
            int keyLen = 8;

            List<string> keys = new();
            for (int i = 0; i < keyCount; i++)
            {
                keys.Add(GetRandomString(keyLen));
                var key = keys.Last();

                for (int j = 0; j < setCount; j++)
                {
                    var member = GetRandomString(valLen);
                    db.SetAdd(key, member);
                }
            }

            var redisKey = keys.Select(x => (object)x).ToArray();
            var keysDeleted = (string)db.Execute("unlink", redisKey);
            Assert.AreEqual(Int32.Parse(keysDeleted), 10);

            keysDeleted = (string)db.Execute("unlink", redisKey);
            Assert.AreEqual(Int32.Parse(keysDeleted), 0);
        }

        [Test]
        public void SingleExists([Values] bool withoutObjectStore)
        {
            if (withoutObjectStore)
            {
                TearDown();
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: true);
                server.Start();
            }
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = 100;
            var strKey = "key1";
            Assert.IsFalse(db.KeyExists(strKey));
            db.StringSet(strKey, nVal);

            bool fExists = db.KeyExists("key1", CommandFlags.None);
            Assert.AreEqual(fExists, true);

            fExists = db.KeyExists("key2", CommandFlags.None);
            Assert.AreEqual(fExists, false);
        }

        [Test]
        public void SingleExistsObject()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "key";
            Assert.IsFalse(db.KeyExists(key));

            var listData = new RedisValue[] { "a", "b", "c", "d" };
            var count = db.ListLeftPush(key, listData);
            Assert.AreEqual(4, count);
            Assert.True(db.KeyExists(key));
        }

        [Test]
        public void MultipleExistsKeysAndObjects()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var count = db.ListLeftPush("listKey", new RedisValue[] { "a", "b", "c", "d" });
            Assert.AreEqual(4, count);

            var zaddItems = db.SortedSetAdd("zset:test", new SortedSetEntry[] { new SortedSetEntry("a", 1), new SortedSetEntry("b", 2) });
            Assert.AreEqual(2, zaddItems);

            db.StringSet("foo", "bar");

            var exists = db.KeyExists(new RedisKey[] { "key", "listKey", "zset:test", "foo" });
            Assert.AreEqual(3, exists);
        }


        [Test]
        public void SingleRename()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "test1";
            db.StringSet("key1", origValue);

            db.KeyRename("key1", "key2");
            string retValue = db.StringGet("key2");

            Assert.AreEqual(origValue, retValue);

            origValue = db.StringGet("key1");
            Assert.AreEqual(null, origValue);
        }

        [Test]
        public void SingleRenameKeyEdgeCase([Values] bool withoutObjectStore)
        {
            if (withoutObjectStore)
            {
                TearDown();
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: true);
                server.Start();
            }
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            //1. Key rename does not exist
            try
            {
                var res = db.KeyRename("key1", "key2");
            }
            catch (Exception ex)
            {
                Assert.AreEqual("ERR no such key", ex.Message);
            }

            //2. Key rename oldKey.Equals(newKey)
            string origValue = "test1";
            db.StringSet("key1", origValue);
            bool renameRes = db.KeyRename("key1", "key1");
            Assert.IsTrue(renameRes);
            string retValue = db.StringGet("key1");
            Assert.AreEqual(origValue, retValue);
        }

        [Test]
        public void SingleRenameObjectStore()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origList = new RedisValue[] { "a", "b", "c", "d" };
            var key1 = "lkey1";
            var count = db.ListRightPush(key1, origList);
            Assert.AreEqual(4, count);

            var result = db.ListRange(key1);
            Assert.AreEqual(origList, result);

            var key2 = "lkey2";
            var rb = db.KeyRename(key1, key2);
            Assert.IsTrue(rb);
            result = db.ListRange(key1);
            Assert.AreEqual(Array.Empty<RedisValue>(), result);

            result = db.ListRange(key2);
            Assert.AreEqual(origList, result);
        }

        [Test]
        public void CanSelectCommand()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var reply = db.Execute("SELECT", "0");
            Assert.IsTrue(reply.ToString() == "OK");
            Assert.Throws<RedisServerException>(() => db.Execute("SELECT", "1"));

            //select again the def db
            db.Execute("SELECT", "0");
        }

        [Test]
        public void CanSelectCommandLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseLength: true);

            var expectedResponse = "-ERR invalid database index.\r\n+PONG\r\n";
            var response = lightClientRequest.Execute("SELECT 1", "PING", expectedResponse.Length);
            Assert.AreEqual(expectedResponse, response);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanDoCommandsInChunks(int bytesSent)
        {
            // SETEX
            using var lightClientRequest = TestUtils.CreateRequest(countResponseLength: true);

            var expectedResponse = "+OK\r\n";
            var response = lightClientRequest.Execute("SETEX mykey 1 abcdefghij", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            // GET
            expectedResponse = "$10\r\nabcdefghij\r\n";
            response = lightClientRequest.Execute("GET mykey", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            Thread.Sleep(2000);

            // GET
            expectedResponse = "$-1\r\n";
            response = lightClientRequest.Execute("GET mykey", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            // DECR            
            expectedResponse = "+OK\r\n";
            response = lightClientRequest.Execute("SET mykeydecr 1", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            expectedResponse = ":0\r\n";
            response = lightClientRequest.Execute("DECR mykeydecr", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            expectedResponse = "$1\r\n0\r\n";
            response = lightClientRequest.Execute("GET mykeydecr", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            // DEL
            expectedResponse = ":1\r\n";
            response = lightClientRequest.Execute("DEL mykeydecr", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            expectedResponse = "$-1\r\n";
            response = lightClientRequest.Execute("GET mykeydecr", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            // EXISTS
            expectedResponse = ":0\r\n";
            response = lightClientRequest.Execute("EXISTS mykeydecr", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            // SET
            expectedResponse = "+OK\r\n";
            response = lightClientRequest.Execute("SET mykey 1", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            // RENAME
            expectedResponse = "+OK\r\n";
            response = lightClientRequest.Execute("RENAME mykey mynewkey", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            // GET
            expectedResponse = "$1\r\n1\r\n";
            response = lightClientRequest.Execute("GET mynewkey", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);
        }


        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanSetGetCommandsChunks(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseLength: true);
            var sb = new StringBuilder();

            for (int i = 1; i <= 100; i++)
            {
                sb.Append($" mykey-{i} {i * 10}");
            }

            // MSET
            var expectedResponse = "+OK\r\n";
            var response = lightClientRequest.Execute($"MSET{sb}", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            expectedResponse = ":100\r\n";
            response = lightClientRequest.Execute($"DBSIZE", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);

            sb.Clear();
            for (int i = 1; i <= 100; i++)
            {
                sb.Append($" mykey-{i}");
            }

            // MGET
            expectedResponse = "*100\r\n$2\r\n10\r\n$2\r\n20\r\n$2\r\n30\r\n$2\r\n40\r\n$2\r\n50\r\n$2\r\n60\r\n$2\r\n70\r\n$2\r\n80\r\n$2\r\n90\r\n$3\r\n100\r\n$3\r\n110\r\n$3\r\n120\r\n$3\r\n130\r\n$3\r\n140\r\n$3\r\n150\r\n$3\r\n160\r\n$3\r\n170\r\n$3\r\n180\r\n$3\r\n190\r\n$3\r\n200\r\n$3\r\n210\r\n$3\r\n220\r\n$3\r\n230\r\n$3\r\n240\r\n$3\r\n250\r\n$3\r\n260\r\n$3\r\n270\r\n$3\r\n280\r\n$3\r\n290\r\n$3\r\n300\r\n$3\r\n310\r\n$3\r\n320\r\n$3\r\n330\r\n$3\r\n340\r\n$3\r\n350\r\n$3\r\n360\r\n$3\r\n370\r\n$3\r\n380\r\n$3\r\n390\r\n$3\r\n400\r\n$3\r\n410\r\n$3\r\n420\r\n$3\r\n430\r\n$3\r\n440\r\n$3\r\n450\r\n$3\r\n460\r\n$3\r\n470\r\n$3\r\n480\r\n$3\r\n490\r\n$3\r\n500\r\n$3\r\n510\r\n$3\r\n520\r\n$3\r\n530\r\n$3\r\n540\r\n$3\r\n550\r\n$3\r\n560\r\n$3\r\n570\r\n$3\r\n580\r\n$3\r\n590\r\n$3\r\n600\r\n$3\r\n610\r\n$3\r\n620\r\n$3\r\n630\r\n$3\r\n640\r\n$3\r\n650\r\n$3\r\n660\r\n$3\r\n670\r\n$3\r\n680\r\n$3\r\n690\r\n$3\r\n700\r\n$3\r\n710\r\n$3\r\n720\r\n$3\r\n730\r\n$3\r\n740\r\n$3\r\n750\r\n$3\r\n760\r\n$3\r\n770\r\n$3\r\n780\r\n$3\r\n790\r\n$3\r\n800\r\n$3\r\n810\r\n$3\r\n820\r\n$3\r\n830\r\n$3\r\n840\r\n$3\r\n850\r\n$3\r\n860\r\n$3\r\n870\r\n$3\r\n880\r\n$3\r\n890\r\n$3\r\n900\r\n$3\r\n910\r\n$3\r\n920\r\n$3\r\n930\r\n$3\r\n940\r\n$3\r\n950\r\n$3\r\n960\r\n$3\r\n970\r\n$3\r\n980\r\n$3\r\n990\r\n$4\r\n1000\r\n";
            response = lightClientRequest.Execute($"MGET{sb}", expectedResponse.Length, bytesSent);
            Assert.AreEqual(expectedResponse, response);
        }

        [Test]
        public void PersistTTLTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "expireKey";
            var val = "expireValue";
            var expire = 2;

            db.StringSet(key, val);
            db.KeyExpire(key, TimeSpan.FromSeconds(expire));

            var time = db.KeyTimeToLive(key);
            Assert.IsTrue(time.Value.TotalSeconds > 0);

            db.KeyExpire(key, TimeSpan.FromSeconds(expire));
            db.KeyPersist(key);

            Thread.Sleep((expire + 1) * 1000);

            var _val = db.StringGet(key);
            Assert.AreEqual(val, _val.ToString());

            time = db.KeyTimeToLive(key);
            Assert.IsNull(time);
        }

        [Test]
        public void ObjectTTLTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "expireKey";
            var expire = 2;

            db.SortedSetAdd(key, key, 1.0);
            db.KeyExpire(key, TimeSpan.FromSeconds(expire));

            var time = db.KeyTimeToLive(key);
            Assert.IsNotNull(time);
            Assert.IsTrue(time.Value.TotalSeconds > 0);
        }

        [Test]
        public void PersistTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int expire = 100;
            var keyA = "keyA";
            db.StringSet(keyA, keyA);
            var response = db.KeyPersist(keyA);
            Assert.IsFalse(response);

            db.KeyExpire(keyA, TimeSpan.FromSeconds(expire));
            var time = db.KeyTimeToLive(keyA);
            Assert.IsTrue(time.Value.Seconds > 0);

            response = db.KeyPersist(keyA);
            Assert.IsTrue(response);

            time = db.KeyTimeToLive(keyA);
            Assert.IsTrue(time == null);

            var value = db.StringGet(keyA);
            Assert.AreEqual(value, keyA);

            var noKey = "noKey";
            response = db.KeyPersist(noKey);
            Assert.IsFalse(response);
        }

        [Test]
        [TestCase("EXPIRE")]
        [TestCase("PEXPIRE")]
        public void KeyExpireStringTest(string command)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "keyA";
            db.StringSet(key, key);

            var value = db.StringGet(key);
            Assert.AreEqual(key, (string)value);

            if (command.Equals("EXPIRE"))
                db.KeyExpire(key, TimeSpan.FromSeconds(1));
            else
                db.Execute(command, new object[] { key, 1000 });

            Thread.Sleep(1500);

            value = db.StringGet(key);
            Assert.AreEqual(null, (string)value);
        }

        [Test]
        [TestCase("EXPIRE")]
        [TestCase("PEXPIRE")]
        public void KeyExpireObjectTest(string command)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "keyA";
            db.SortedSetAdd(key, new SortedSetEntry[] { new SortedSetEntry("element", 1.0) });

            var value = db.SortedSetScore(key, "element");
            Assert.AreEqual(1.0, value);

            var exp = db.KeyExpire(key, command.Equals("EXPIRE") ? TimeSpan.FromSeconds(1) : TimeSpan.FromMilliseconds(1000));
            Assert.IsTrue(exp);

            Thread.Sleep(1500);

            value = db.SortedSetScore(key, "element");
            Assert.AreEqual(null, value);
        }

        [Test]
        [TestCase("EXPIRE")]
        [TestCase("PEXPIRE")]
        public void KeyExpireOptionsTest(string command)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "keyA";
            object[] args = new object[] { key, 1000, "" };
            db.StringSet(key, key);

            args[2] = "XX";// XX -- Set expiry only when the key has an existing expiry
            bool resp = (bool)db.Execute($"{command}", args);
            Assert.IsFalse(resp);//XX return false no existing expiry

            args[2] = "NX";// NX -- Set expiry only when the key has no expiry
            resp = (bool)db.Execute($"{command}", args);
            Assert.IsTrue(resp);// NX return true no existing expiry

            args[2] = "NX";// NX -- Set expiry only when the key has no expiry
            resp = (bool)db.Execute($"{command}", args);
            Assert.IsFalse(resp);// NX return false existing expiry

            args[1] = 50;
            args[2] = "XX";// XX -- Set expiry only when the key has an existing expiry
            resp = (bool)db.Execute($"{command}", args);
            Assert.IsTrue(resp);// XX return true existing expiry
            var time = db.KeyTimeToLive(key);
            Assert.IsTrue(time.Value.TotalSeconds <= (double)((int)args[1]) && time.Value.TotalSeconds > 0);

            args[1] = 1;
            args[2] = "GT";// GT -- Set expiry only when the new expiry is greater than current one
            resp = (bool)db.Execute($"{command}", args);
            Assert.IsFalse(resp); // GT return false new expiry < current expiry

            args[1] = 1000;
            args[2] = "GT";// GT -- Set expiry only when the new expiry is greater than current one
            resp = (bool)db.Execute($"{command}", args);
            Assert.IsTrue(resp); // GT return true new expiry > current expiry
            time = db.KeyTimeToLive(key);

            if (command.Equals("EXPIRE"))
                Assert.IsTrue(time.Value.TotalSeconds > 500);
            else
                Assert.IsTrue(time.Value.TotalMilliseconds > 500);

            args[1] = 2000;
            args[2] = "LT";// LT -- Set expiry only when the new expiry is less than current one
            resp = (bool)db.Execute($"{command}", args);
            Assert.IsFalse(resp); // LT return false new expiry > current expiry

            args[1] = 15;
            args[2] = "LT";// LT -- Set expiry only when the new expiry is less than current one
            resp = (bool)db.Execute($"{command}", args);
            Assert.IsTrue(resp); // LT return true new expiry < current expiry
            time = db.KeyTimeToLive(key);

            if (command.Equals("EXPIRE"))
                Assert.IsTrue(time.Value.TotalSeconds <= (double)((int)args[1]) && time.Value.TotalSeconds > 0);
            else
                Assert.IsTrue(time.Value.TotalMilliseconds <= (double)((int)args[1]) && time.Value.TotalMilliseconds > 0);
        }

        [Test]
        public void GetSliceTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "rangeKey";
            string value = "0123456789";

            var resp = (string)db.StringGetRange(key, 2, 10);
            Assert.AreEqual(null, resp);
            Assert.AreEqual(true, db.StringSet(key, value));

            //0,0
            resp = (string)db.StringGetRange(key, 0, 0);
            Assert.AreEqual("0", resp);

            //actual value
            resp = (string)db.StringGetRange(key, 0, -1);
            Assert.AreEqual(value, resp);

            #region testA
            //s[2,len] s < e & e = len
            resp = (string)db.StringGetRange(key, 2, 10);
            Assert.AreEqual(value.Substring(2), resp);

            //s[2,len] s < e & e = len - 1
            resp = (string)db.StringGetRange(key, 2, 9);
            Assert.AreEqual(value.Substring(2), resp);

            //s[2,len] s < e < len
            resp = (string)db.StringGetRange(key, 2, 5);
            Assert.AreEqual(value.Substring(2, 4), resp);

            //s[2,len] s < len < e
            resp = (string)db.StringGetRange(key, 2, 15);
            Assert.AreEqual(value.Substring(2), resp);

            //s[4,len] e < s < len
            resp = (string)db.StringGetRange(key, 4, 2);
            Assert.AreEqual("", resp);

            //s[4,len] e < 0 < s < len
            resp = (string)db.StringGetRange(key, 4, -2);
            Assert.AreEqual(value.Substring(4, 5), resp);

            //s[4,len] e < -len < 0 < s < len
            resp = (string)db.StringGetRange(key, 4, -12);
            Assert.AreEqual("", resp);
            #endregion

            #region testB
            //-len < s < 0 < len < e
            resp = (string)db.StringGetRange(key, -4, 15);
            Assert.AreEqual(value.Substring(6, 4), resp);

            //-len < s < 0 < e < len where len + s > e
            resp = (string)db.StringGetRange(key, -4, 5);
            Assert.AreEqual("", resp);

            //-len < s < 0 < e < len where len + s < e
            resp = (string)db.StringGetRange(key, -4, 8);
            Assert.AreEqual(value.Substring(value.Length - 4, 2), resp);

            //-len < s < e < 0
            resp = (string)db.StringGetRange(key, -4, -1);
            Assert.AreEqual(value.Substring(value.Length - 4, 4), resp);

            //-len < e < s < 0
            resp = (string)db.StringGetRange(key, -4, -7);
            Assert.AreEqual("", resp);
            #endregion

            //range start > end > len
            resp = (string)db.StringGetRange(key, 17, 13);
            Assert.AreEqual("", resp);

            //range 0 > start > end
            resp = (string)db.StringGetRange(key, -1, -4);
            Assert.AreEqual("", resp);

            //equal offsets
            resp = db.StringGetRange(key, 4, 4);
            Assert.AreEqual("4", resp);

            //equal offsets
            resp = db.StringGetRange(key, -4, -4);
            Assert.AreEqual("6", resp);

            //equal offsets
            resp = db.StringGetRange(key, -100, -100);
            Assert.AreEqual("0", resp);

            //equal offsets
            resp = db.StringGetRange(key, -101, -101);
            Assert.AreEqual("9", resp);

            //start larger than end
            resp = db.StringGetRange(key, -1, -3);
            Assert.AreEqual("", resp);

            //2,-1 -> 2 9
            var negend = -1;
            resp = db.StringGetRange(key, 2, negend);
            Assert.AreEqual(value.Substring(2, 8), resp);

            //2,-3 -> 2 7
            negend = -3;
            resp = db.StringGetRange(key, 2, negend);
            Assert.AreEqual(value.Substring(2, 6), resp);

            //-5,-3 -> 5,7
            var negstart = -5;
            resp = db.StringGetRange(key, negstart, negend);
            Assert.AreEqual(value.Substring(5, 3), resp);
        }

        [Test]
        public void SetRangeTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "setRangeKey";
            string value = "0123456789";
            string newValue = "ABCDE";

            // new key, length 10, offset 0 -> 10 ("0123456789")
            var resp = (string)db.StringSetRange(key, 0, value);
            Assert.AreEqual("10", resp);
            resp = db.StringGet(key);
            Assert.AreEqual("0123456789", resp);
            Assert.IsTrue(db.KeyDelete(key));

            // new key, length 10, offset 5 -> 15 ("\0\0\0\0\00123456789")
            resp = db.StringSetRange(key, 5, value);
            Assert.AreEqual("15", resp);
            resp = db.StringGet(key);
            Assert.AreEqual("\0\0\0\0\00123456789", resp);
            Assert.IsTrue(db.KeyDelete(key));

            // new key, length 10, offset -1 -> RedisServerException ("ERR offset is out of range")
            try
            {
                db.StringSetRange(key, -1, value);
                Assert.Fail();
            }
            catch (RedisServerException ex)
            {
                Assert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERROFFSETOUTOFRANGE.ToArray()).TrimEnd().TrimStart('-'), ex.Message);
            }

            // existing key, length 10, offset 0, value length 5 -> 10 ("ABCDE56789")
            Assert.IsTrue(db.StringSet(key, value));
            resp = db.StringSetRange(key, 0, newValue);
            Assert.AreEqual("10", resp);
            resp = db.StringGet(key);
            Assert.AreEqual("ABCDE56789", resp);
            Assert.IsTrue(db.KeyDelete(key));

            // existing key, length 10, offset 5, value length 5 -> 10 ("01234ABCDE")
            Assert.IsTrue(db.StringSet(key, value));
            resp = db.StringSetRange(key, 5, newValue);
            Assert.AreEqual("10", resp);
            resp = db.StringGet(key);
            Assert.AreEqual("01234ABCDE", resp);
            Assert.IsTrue(db.KeyDelete(key));

            // existing key, length 10, offset 10, value length 5 -> 15 ("0123456789ABCDE")
            Assert.IsTrue(db.StringSet(key, value));
            resp = db.StringSetRange(key, 10, newValue);
            Assert.AreEqual("15", resp);
            resp = db.StringGet(key);
            Assert.AreEqual("0123456789ABCDE", resp);
            Assert.IsTrue(db.KeyDelete(key));

            // existing key, length 10, offset 15, value length 5 -> 20 ("0123456789\0\0\0\0\0ABCDE")
            Assert.IsTrue(db.StringSet(key, value));
            resp = db.StringSetRange(key, 15, newValue);
            Assert.AreEqual("20", resp);
            resp = db.StringGet(key);
            Assert.AreEqual("0123456789\0\0\0\0\0ABCDE", resp);
            Assert.IsTrue(db.KeyDelete(key));

            // existing key, length 10, offset -1, value length 5 -> RedisServerException ("ERR offset is out of range")
            Assert.IsTrue(db.StringSet(key, value));
            try
            {
                db.StringSetRange(key, -1, newValue);
                Assert.Fail();
            }
            catch (RedisServerException ex)
            {
                Assert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERROFFSETOUTOFRANGE.ToArray()).TrimEnd().TrimStart('-'), ex.Message);
            }
        }

        [Test]
        public void PingTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string result = (string)db.Execute("PING");
            Assert.AreEqual("PONG", result);
        }

        [Test]
        public void AskingTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string result = (string)db.Execute("ASKING");
            Assert.AreEqual("OK", result);
        }

        [Test]
        public void KeepTtlTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int expire = 3;
            var keyA = "keyA";
            var keyB = "keyB";
            db.StringSet(keyA, keyA);
            db.StringSet(keyB, keyB);

            db.KeyExpire(keyA, TimeSpan.FromSeconds(expire));
            db.KeyExpire(keyB, TimeSpan.FromSeconds(expire));

            db.StringSet(keyA, keyA, keepTtl: true);
            var time = db.KeyTimeToLive(keyA);
            Assert.IsTrue(time.Value.Ticks > 0);

            db.StringSet(keyB, keyB, keepTtl: false);
            time = db.KeyTimeToLive(keyB);
            Assert.IsTrue(time == null);

            Thread.Sleep(expire * 1000 + 100);

            string value = db.StringGet(keyA);
            Assert.AreEqual(null, value);

            value = db.StringGet(keyB);
            Assert.AreEqual(keyB, value);
        }

        [Test]
        public void StrlenTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            Assert.IsTrue(db.StringSet("mykey", "foo bar"));
            Assert.IsTrue(db.StringLength("mykey") == 7);
            Assert.IsTrue(db.StringLength("nokey") == 0);
        }

        [Test]
        public void TTLTestMilliseconds()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            var val = "myKeyValue";
            var expireTimeInMilliseconds = 3000;

            db.StringSet(key, val);
            db.KeyExpire(key, TimeSpan.FromMilliseconds(expireTimeInMilliseconds));

            //check TTL of the key in milliseconds
            var pttl = db.Execute("PTTL", key);

            Assert.IsTrue(long.TryParse(pttl.ToString(), out var pttlInMs));
            Assert.IsTrue(pttlInMs > 0);

            db.KeyPersist(key);
            Thread.Sleep(expireTimeInMilliseconds);

            var _val = db.StringGet(key);
            Assert.AreEqual(val, _val.ToString());

            var ttl = db.KeyTimeToLive(key);
            Assert.IsNull(ttl);
        }

        [Test]
        public void GetDelTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            var val = "myKeyValue";

            // Key Setup
            db.StringSet(key, val);
            var retval = db.StringGet(key);
            Assert.AreEqual(val, retval.ToString());

            retval = db.StringGetDelete(key);
            Assert.AreEqual(val, retval.ToString());

            // Try retrieving already deleted key
            retval = db.StringGetDelete(key);
            Assert.AreEqual(string.Empty, retval.ToString());

            // Try retrieving & deleting non-existent key
            retval = db.StringGetDelete("nonExistentKey");
            Assert.AreEqual(string.Empty, retval.ToString());

            // Key setup with metadata
            key = "myKeyWithMetadata";
            val = "myValueWithMetadata";
            db.StringSet(key, val, expiry: TimeSpan.FromSeconds(10000));
            retval = db.StringGet(key);
            Assert.AreEqual(val, retval.ToString());

            retval = db.StringGetDelete(key);
            Assert.AreEqual(val, retval.ToString());

            // Try retrieving already deleted key with metadata
            retval = db.StringGetDelete(key);
            Assert.AreEqual(string.Empty, retval.ToString());
        }

        [Test]
        public void AppendTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            var val = "myKeyValue";
            var val2 = "myKeyValue2";

            db.StringSet(key, val);
            var len = db.StringAppend(key, val2);
            Assert.AreEqual(val.Length + val2.Length, len);

            var _val = db.StringGet(key);
            Assert.AreEqual(val + val2, _val.ToString());

            // Test appending an empty string
            db.StringSet(key, val);
            var len1 = db.StringAppend(key, "");
            Assert.AreEqual(val.Length, len1);

            _val = db.StringGet(key);
            Assert.AreEqual(val, _val.ToString());

            // Test appending to a non-existent key
            var nonExistentKey = "nonExistentKey";
            var len2 = db.StringAppend(nonExistentKey, val2);
            Assert.AreEqual(val2.Length, len2);

            _val = db.StringGet(nonExistentKey);
            Assert.AreEqual(val2, _val.ToString());

            // Test appending to a key with a large value
            var largeVal = new string('a', 1000000);
            db.StringSet(key, largeVal);
            var len3 = db.StringAppend(key, val2);
            Assert.AreEqual(largeVal.Length + val2.Length, len3);

            // Test appending to a key with metadata
            var keyWithMetadata = "keyWithMetadata";
            db.StringSet(keyWithMetadata, val, TimeSpan.FromSeconds(10000));
            var len4 = db.StringAppend(keyWithMetadata, val2);
            Assert.AreEqual(val.Length + val2.Length, len4);

            _val = db.StringGet(keyWithMetadata);
            Assert.AreEqual(val + val2, _val.ToString());

            var time = db.KeyTimeToLive(keyWithMetadata);
            Assert.IsTrue(time.Value.TotalSeconds > 0);
        }

    }
}