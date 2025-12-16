// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespTests
    {
        GarnetServer server;
        Random r;
        private TaskFactory taskFactory = new();

        [SetUp]
        public void Setup()
        {
            r = new Random(674386);
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: false);
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
            var ids = Enum.GetValues<RespCommand>();

            // Isolate command IDs that exist more than once in the array
            var duplicateIds = ids.GroupBy(e => e).Where(e => e.Count() > 1).Select(e => e.First());

            ClassicAssert.IsEmpty(duplicateIds, "Found ambiguous command IDs");
        }

        /// <summary>
        /// Test that we recognize "write" ops correctly for metric purposes.
        /// </summary>
        [Test]
        public void OneIfWrite()
        {
            var wrong = new List<RespCommand>();

            foreach (var cmd in Enum.GetValues<RespCommand>())
            {
                if (cmd == RespCommand.NONE || cmd == RespCommand.INVALID)
                {
                    continue;
                }

                if (!RespCommandsInfo.TryGetRespCommandInfo(cmd, out var info))
                {
                    continue;
                }

                var isWrite = info.AclCategories.HasFlag(RespAclCategories.Write);
                var expected = isWrite ? 1UL : 0;

                if (expected != cmd.OneIfWrite())
                {
                    wrong.Add(cmd);
                }
            }

            ClassicAssert.IsEmpty(wrong, "These commands are incorrectly classified w.r.t. OneIfWrite");
        }

        /// <summary>
        /// Test that we recognize "read" ops correctly for metric purposes.
        /// </summary>
        [Test]
        public void OneIfRead()
        {
            var wrong = new List<RespCommand>();

            foreach (var cmd in Enum.GetValues<RespCommand>())
            {
                if (cmd == RespCommand.NONE || cmd == RespCommand.INVALID)
                {
                    continue;
                }

                if (!RespCommandsInfo.TryGetRespCommandInfo(cmd, out var info))
                {
                    continue;
                }

                var isRead = info.AclCategories.HasFlag(RespAclCategories.Read);
                var expected = isRead ? 1UL : 0;

                if (expected != cmd.OneIfRead())
                {
                    wrong.Add(cmd);
                }
            }

            ClassicAssert.IsEmpty(wrong, "These commands are incorrectly classified w.r.t. OneIfRead");
        }

        /// <summary>
        /// Test that we recognize "cluster" ops correctly.
        /// </summary>
        [Test]
        public void IsClusterSubCommand()
        {
            ClassicAssert.True(RespCommandsInfo.TryGetRespCommandInfo("CLUSTER", out var clusterCommand), "Couldn't load CLUSTER command details");
            ClassicAssert.IsNotNull(clusterCommand.SubCommands, "CLUSTER didn't have any subcommands");

            IEnumerable<RespCommand> clusterSubCommands = clusterCommand.SubCommands.Select(static s => s.Command);
            foreach (var cmd in Enum.GetValues<RespCommand>())
            {
                var expectedRes = clusterSubCommands.Contains(cmd);
                var actualRes = cmd.IsClusterSubCommand();

                ClassicAssert.AreEqual(expectedRes, actualRes, $"Mismatch for {cmd}");
            }
        }

        /// <summary>
        /// Tests RESTORE value that is not string
        /// </summary>
        [Test]
        public void TryRestoreKeyNonStringType()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var payload = new byte[]
            {
                0x14, 0xf, 0xf, 0x0, 0x0, 0x0, 0x1, 0x0, 0xc2, 0x86, 0x62, 0x69, 0x6b, 0x65, 0x3a, 0x31, 0x7, 0xc3, 0xbf, 0xb, 0x0, 0xc3, 0xaa, 0x33, 0x68, 0x7b, 0x2a, 0xc3, 0xa6, 0xc3, 0xbf, 0xc3, 0xb9
            };

            Assert.Throws<RedisServerException>(() => db.KeyRestore("mykey", payload));
        }

        /// <summary>
        /// Tests RESTORE command on existing key
        /// </summary>
        [Test]
        public void TryRestoreExistingKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("mykey", "val");

            var dump = db.KeyDump("mykey")!;

            Assert.Throws<RedisServerException>(() => db.KeyRestore("mykey", dump));
        }

        /// <summary>
        /// Tests that RESTORE command restores payload with 32 bit encoded length
        /// </summary>
        [Test]
        public void SingleRestore32Bit()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var valueBuilder = new StringBuilder();

            for (var i = 0; i < 16_383; i++)
                valueBuilder.Append('a');

            var val = valueBuilder.ToString();

            db.StringSet("mykey", val);

            var dump = db.KeyDump("mykey")!;

            db.KeyDelete("mykey");

            db.KeyRestore("mykey", dump);

            var value = db.StringGet("mykey");

            ClassicAssert.AreEqual(val, value.ToString());
        }

        /// <summary>
        /// Tests that RESTORE command restores payload with 14 bit encoded length
        /// </summary>
        [Test]
        public void SingleRestore14Bit()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var valueBuilder = new StringBuilder();

            for (var i = 0; i < 16_383 - 1; i++)
                valueBuilder.Append('a');

            var val = valueBuilder.ToString();

            db.StringSet("mykey", val);

            var dump = db.KeyDump("mykey")!;

            db.KeyDelete("mykey");

            db.KeyRestore("mykey", dump);

            var value = db.StringGet("mykey");

            ClassicAssert.AreEqual(val, value.ToString());
        }

        /// <summary>
        /// Tests that RESTORE command restores payload with 6 bit encoded length
        /// </summary>
        [Test]
        public void SingleRestore6Bit()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("mykey", "val");

            var dump = db.KeyDump("mykey")!;

            db.KeyDelete("mykey");

            db.KeyRestore("mykey", dump, TimeSpan.FromHours(3));

            var value = db.StringGet("mykey");

            ClassicAssert.AreEqual("val", value.ToString());
        }

        /// <summary>
        /// Tests that RESTORE command restores payload with 6 bit encoded length without TTL
        /// </summary>
        [Test]
        public void SingleRestore6BitWithoutTtl()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("mykey", "val");

            var dump = db.KeyDump("mykey")!;

            db.KeyDelete("mykey");

            db.KeyRestore("mykey", dump);

            var value = db.StringGet("mykey");

            ClassicAssert.AreEqual("val", value.ToString());
        }

        /// <summary>
        /// Tests that DUMP command returns payload with 6 bit encoded length
        /// </summary>
        [Test]
        public void SingleDump6Bit()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("mykey", "val");

            var dump = db.KeyDump("mykey");

            var expectedValue = new byte[]
            {
                 0x00, // value type 
                 0x03, // length of payload
                 0x76, 0x61, 0x6C,       // 'v', 'a', 'l'
                 0x0B, 0x00, // RDB version
            };

            var crc = new byte[]
            {
                 0xDB,
                 0x82,
                 0x3C,
                 0x30,
                 0x38,
                 0x78,
                 0x5A,
                 0x99
            };

            expectedValue = [.. expectedValue, .. crc];

            ClassicAssert.AreEqual(expectedValue, dump);
        }

        /// <summary>
        /// Tests that DUMP command returns payload with 14 bit encoded length
        /// </summary>
        [Test]
        public void SingleDump14Bit()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var valueBuilder = new StringBuilder();

            for (var i = 0; i < 16_383 - 1; i++)
                valueBuilder.Append('a');

            var value = valueBuilder.ToString();

            db.StringSet("mykey", value);

            var dump = db.KeyDump("mykey");

            var expectedValue = new byte[]
            {
                0x00, // value type
                0x7F, 0xFE, // length of payload
            };

            expectedValue = [.. expectedValue, .. Encoding.UTF8.GetBytes(value)];

            var rdbVersion = new byte[]
            {
                0x0B, 0x00, // RDB version
            };

            expectedValue = [.. expectedValue, .. rdbVersion];

            var crc = new byte[]
            {
                0x7C,
                0x09,
                0x2D,
                0x16,
                0x73,
                0xAE,
                0x7C,
                0xCF
            };

            expectedValue = [.. expectedValue, .. crc];

            ClassicAssert.AreEqual(expectedValue, dump);
        }

        /// <summary>
        /// Tests that DUMP command returns payload with 32 bit encoded length
        /// </summary>
        [Test]
        public void SingleDump32Bit()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var valueBuilder = new StringBuilder();

            for (var i = 0; i < 16_383 + 1; i++)
                valueBuilder.Append('a');

            var value = valueBuilder.ToString();

            db.StringSet("mykey", value);

            var dump = db.KeyDump("mykey");

            var expectedValue = new byte[]
            {
                0x00, // value type
                0x80, 0x00, 0x00, 0x40, 0x00, // length of payload
            };

            expectedValue = [.. expectedValue, .. Encoding.UTF8.GetBytes(value)];

            var rdbVersion = new byte[]
            {
                0x0B, 0x00, // RDB version
            };

            expectedValue = [.. expectedValue, .. rdbVersion];

            var crc = new byte[]
            {
                0x7F,
                0x73,
                0x7E,
                0xA9,
                0x87,
                0xD9,
                0x90,
                0x14
            };

            expectedValue = [.. expectedValue, .. crc];

            ClassicAssert.AreEqual(expectedValue, dump);
        }

        /// <summary>
        /// Tests DUMP on non string type which is currently not supported
        /// </summary>
        [Test]
        public void TryDumpKeyNonString()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.SetAdd("mykey", "val1");
            db.SetAdd("mykey", "val2");

            var value = db.KeyDump("mykey");

            ClassicAssert.AreEqual(null, value);
        }

        /// <summary>
        /// Try DUMP key that does not exist
        /// </summary>
        [Test]
        public void TryDumpKeyThatDoesNotExist()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var value = db.KeyDump("mykey");

            ClassicAssert.AreEqual(null, value);
        }

        [Test]
        public void SingleSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "abcdefg";
            db.StringSet("mykey", origValue);

            string retValue = db.StringGet("mykey");

            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public async Task SingleAsciiSetGetGarnetClient()
        {
            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            string origValue = "abcdefg";
            await db.StringSetAsync("mykey", origValue);

            string retValue = await db.StringGetAsync("mykey");

            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public async Task SingleUnicodeSetGetGarnetClient()
        {
            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            string origValue = "笑い男";
            await db.StringSetAsync("mykey", origValue);

            string retValue = await db.StringGetAsync("mykey");

            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public void MultiSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const int length = 15000;
            var input = new KeyValuePair<RedisKey, RedisValue>[length];
            var inputNX = new KeyValuePair<RedisKey, RedisValue>[length];
            var inputXX = new KeyValuePair<RedisKey, RedisValue>[length];

            for (int i = 0; i < length; i++)
                input[i] = new KeyValuePair<RedisKey, RedisValue>(i.ToString(), i.ToString());

            // MSET NX - non-existing values
            var result = db.StringSet(input, When.NotExists);
            ClassicAssert.IsTrue(result);

            var value = db.StringGet([.. input.Select(e => e.Key)]);
            ClassicAssert.AreEqual(length, value.Length);

            for (int i = 0; i < length; i++)
                ClassicAssert.AreEqual(input[i].Value, value[i]);

            // MSET
            result = db.StringSet(input);
            ClassicAssert.IsTrue(result);

            var keys = input.Select(e => e.Key).ToArray();
            value = db.StringGet(keys);
            ClassicAssert.AreEqual(length, value.Length);

            for (int i = 0; i < length; i++)
                ClassicAssert.AreEqual(input[i].Value, value[i]);

            // MSET NX - existing values
            for (int i = 0; i < length; i++)
                inputXX[i] = new KeyValuePair<RedisKey, RedisValue>(i.ToString(), (i + 1).ToString());

            result = db.StringSet(inputXX, When.NotExists);
            ClassicAssert.IsFalse(result);

            value = db.StringGet(keys);
            ClassicAssert.AreEqual(length, value.Length);

            // Should not change existing keys
            for (int i = 0; i < length; i++)
                ClassicAssert.AreEqual(new RedisValue(i.ToString()), value[i]);

            // MSET NX - non-existing and existing values
            for (int i = 0; i < length; i++)
                inputNX[i] = new KeyValuePair<RedisKey, RedisValue>((i % 2 == 0 ? i : i + length).ToString(), (i + length).ToString());

            result = db.StringSet(inputNX, When.NotExists);
            ClassicAssert.IsFalse(result);

            value = db.StringGet(keys);
            ClassicAssert.AreEqual(length, value.Length);

            for (int i = 0; i < length; i++)
            {
                ClassicAssert.AreEqual(new RedisValue(i.ToString()), value[i]);
            }

            // Should not create new keys if any existing keys were also specified
            var nxKeys = inputNX.Select(x => x.Key).Where(x => !keys.Contains(x)).Take(10).ToArray();
            value = db.StringGet(nxKeys);
            for (int i = 0; i < value.Length; i++)
            {
                ClassicAssert.IsEmpty(value[i].ToString());
            }
        }

        [Test]
        public void MultiSetNX()
        {
            var lightClientRequest = TestUtils.CreateRequest();

            // Set keys
            var response = lightClientRequest.SendCommand("MSETNX key1 5 key2 6");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // MSETNX command should fail since key exists
            response = lightClientRequest.SendCommand("MSETNX key3 7 key1 8");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Verify values
            response = lightClientRequest.SendCommand("GET key1");
            expectedResponse = "$1\r\n5\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GET key2");
            expectedResponse = "$1\r\n6\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Should not be set even though it was 'before' existing key1.
            response = lightClientRequest.SendCommand("GET key3");
            expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("HSET key4 first 1");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // MSETNX command should fail since key exists even if it's an object.
            response = lightClientRequest.SendCommand("MSETNX key3 7 key4 8");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
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
            ClassicAssert.IsTrue(result);

            var retvalue = (byte[])db.StringGet("mykey");

            ClassicAssert.IsTrue(new ReadOnlySpan<byte>(value).SequenceEqual(new ReadOnlySpan<byte>(retvalue)));
        }

        [Test]
        public void SetExpiry()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "abcdefghij";
            db.StringSet("mykey", origValue, TimeSpan.FromSeconds(1));

            string retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(origValue, retValue, "Get() before expiration");

            var actualDbSize = db.Execute("DBSIZE");
            ClassicAssert.AreEqual(1, (ulong)actualDbSize, "DBSIZE before expiration");

            var actualKeys = db.Execute("KEYS", ["*"]);
            ClassicAssert.AreEqual(1, ((RedisResult[])actualKeys).Length, "KEYS before expiration");

            var actualScan = db.Execute("SCAN", "0");
            ClassicAssert.AreEqual(1, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN before expiration");

            // Sleep to wait for expiration
            Thread.Sleep(2000);

            retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(null, retValue, "Get() after expiration");

            actualDbSize = db.Execute("DBSIZE");
            ClassicAssert.AreEqual(0, (ulong)actualDbSize, "DBSIZE after expiration");

            actualKeys = db.Execute("KEYS", ["*"]);
            ClassicAssert.AreEqual(0, ((RedisResult[])actualKeys).Length, "KEYS after expiration");

            actualScan = db.Execute("SCAN", "0");
            ClassicAssert.AreEqual(0, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN after expiration");
        }

        [Test]
        public void SetExpiryHighPrecision()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "abcdefghij";
            db.StringSet("mykey", origValue, TimeSpan.FromSeconds(1.9));

            string retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(origValue, retValue);

            Thread.Sleep(1000);
            retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(origValue, retValue);

            Thread.Sleep(2000);
            retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(null, retValue);
        }

        [Test]
        public void SetExpiryThenExpireAndAppend()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "user:string1";
            string value1 = "a";
            string value2 = "ab";
            var ttl = TimeSpan.FromMilliseconds(100);

            var ok = db.StringSet(key, value1, ttl);
            ClassicAssert.IsTrue(ok);

            Thread.Sleep(ttl * 2);

            var len = db.StringAppend(key, value2);
            ClassicAssert.IsTrue(len == 2);
        }

        [Test]
        public void SetExpiryNx()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            string origValue = "abcdefghij";
            var newLongerValue = "abcdefghijk"; // Longer value to test non in-place update
            var newShorterValue = "abcdefghi"; // Shorter value to test in-place update
            var expireTime = TimeSpan.FromSeconds(1);
            var ok = db.StringSet(key, origValue, expireTime, When.NotExists, CommandFlags.None);
            ClassicAssert.IsTrue(ok);

            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(origValue, retValue);

            Thread.Sleep(expireTime * 1.1);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(null, retValue);

            // Test non in-place update
            ok = db.StringSet(key, newLongerValue, expireTime, When.NotExists, CommandFlags.None);
            ClassicAssert.IsTrue(ok);

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newLongerValue, retValue);

            Thread.Sleep(expireTime * 1.1);

            // Test in-place update
            ok = db.StringSet(key, newShorterValue, expireTime, When.NotExists, CommandFlags.None);
            ClassicAssert.IsTrue(ok);

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newShorterValue, retValue);
        }

        [Test]
        public void SetAndGetExpiryNx()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            string origValue = "abcdefghij";
            var newValue = "xyz"; // New value that shouldn't be written
            var newLongerValue = "abcdefghijk"; // Longer value to test non in-place update
            var newShorterValue = "abcdefghi"; // Shorter value to test in-place update
            var expireTime = TimeSpan.FromSeconds(1);
            string actualValue = db.StringSetAndGet(key, origValue, expireTime, when: When.NotExists, CommandFlags.None);
            ClassicAssert.IsNull(actualValue);

            string retValue = db.StringSetAndGet(key, newValue, expireTime, when: When.NotExists, CommandFlags.None);
            ClassicAssert.AreEqual(origValue, retValue);

            Thread.Sleep(expireTime * 1.1);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(null, retValue);

            // Test non in-place update
            actualValue = db.StringSetAndGet(key, newLongerValue, expireTime, when: When.NotExists, CommandFlags.None);
            ClassicAssert.IsNull(actualValue);

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newLongerValue, retValue);

            Thread.Sleep(expireTime * 1.1);

            // Test in-place update
            actualValue = db.StringSetAndGet(key, newShorterValue, expireTime, when: When.NotExists, CommandFlags.None);
            ClassicAssert.IsNull(actualValue);

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newShorterValue, retValue);
        }

        [Test]
        public void SetXx()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
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

            result = db.StringSet(key, newValue, TimeSpan.FromSeconds(10), When.Exists, CommandFlags.None);
            ClassicAssert.IsTrue(result);

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue, retValue);
        }

        [Test]
        public void SetAndGetExpiryXx()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            string origValue = "abcdefghij";
            var newValue = "xyz"; // New value that shouldn't be written
            var newLongerValue = "abcdefghijk"; // Longer value to test non in-place update
            var newShorterValue = "abcdefghi"; // Shorter value to test in-place update
            var expireTime = TimeSpan.FromSeconds(1);
            string actualValue = db.StringSetAndGet(key, newValue, expireTime, when: When.Exists, CommandFlags.None);
            ClassicAssert.IsNull(actualValue);

            string retValue = db.StringGet(key);
            ClassicAssert.IsNull(retValue);

            db.StringSet(key, origValue);
            // Test non in-place update
            actualValue = db.StringSetAndGet(key, newLongerValue, expireTime, when: When.Exists, CommandFlags.None);
            ClassicAssert.AreEqual(origValue, actualValue);

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newLongerValue, retValue);

            Thread.Sleep(expireTime * 1.1);
            retValue = db.StringGet(key);
            ClassicAssert.IsNull(retValue);


            db.StringSet(key, origValue);
            // Test in-place update
            actualValue = db.StringSetAndGet(key, newShorterValue, expireTime, when: When.Exists, CommandFlags.None);
            ClassicAssert.AreEqual(origValue, actualValue);

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newShorterValue, retValue);

            Thread.Sleep(expireTime * 1.1);
            retValue = db.StringGet(key);
            ClassicAssert.IsNull(retValue);
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
            ClassicAssert.IsTrue(result);
            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(origValue, retValue);

            // Smaller new value without expiration
            string newValue1 = "abcdefghijklmnopqrs";
            retValue = db.StringSetAndGet(key, newValue1, null, When.Always, CommandFlags.None);
            ClassicAssert.AreEqual(origValue, retValue);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue1, retValue);

            // Smaller new value with KeepTtl
            string newValue2 = "abcdefghijklmnopqr";
            retValue = db.StringSetAndGet(key, newValue2, null, true, When.Always, CommandFlags.None);
            ClassicAssert.AreEqual(newValue1, retValue);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue2, retValue);
            var expiry = db.KeyTimeToLive(key);
            ClassicAssert.IsNull(expiry);

            // Smaller new value with expiration
            string newValue3 = "01234";
            retValue = db.StringSetAndGet(key, newValue3, TimeSpan.FromSeconds(10), When.Exists, CommandFlags.None);
            ClassicAssert.AreEqual(newValue2, retValue);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue3, retValue);
            expiry = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(expiry.Value.TotalSeconds > 0);

            // Larger new value with expiration
            string newValue4 = "abcdefghijklmnopqrstabcdefghijklmnopqrst";
            retValue = db.StringSetAndGet(key, newValue4, TimeSpan.FromSeconds(100), When.Exists, CommandFlags.None);
            ClassicAssert.AreEqual(newValue3, retValue);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue4, retValue);
            expiry = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(expiry.Value.TotalSeconds > 0);

            // Smaller new value without expiration
            string newValue5 = "0123401234";
            retValue = db.StringSetAndGet(key, newValue5, null, When.Exists, CommandFlags.None);
            ClassicAssert.AreEqual(newValue4, retValue);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue5, retValue);
            expiry = db.KeyTimeToLive(key);
            ClassicAssert.IsNull(expiry);

            // Larger new value without expiration
            string newValue6 = "abcdefghijklmnopqrstabcdefghijklmnopqrst";
            retValue = db.StringSetAndGet(key, newValue6, null, When.Always, CommandFlags.None);
            ClassicAssert.AreEqual(newValue5, retValue);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue6, retValue);
            expiry = db.KeyTimeToLive(key);
            ClassicAssert.IsNull(expiry);
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
        public void IncrDecrChangeDigitsWithExpiry()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Key storing integer
            var strKey = "key1";
            db.StringSet(strKey, 9, TimeSpan.FromSeconds(1000));

            long n = db.StringIncrement(strKey);
            long nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(10, nRetVal);

            n = db.StringDecrement(strKey);
            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(9, nRetVal);

            db.StringSet(strKey, 99, TimeSpan.FromSeconds(1000));
            n = db.StringIncrement(strKey);
            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(100, nRetVal);

            n = db.StringDecrement(strKey);
            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(99, nRetVal);

            db.StringSet(strKey, 999, TimeSpan.FromSeconds(1000));
            n = db.StringIncrement(strKey);
            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(1000, nRetVal);

            n = db.StringDecrement(strKey);
            nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
            ClassicAssert.AreEqual(999, nRetVal);
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
            ClassicAssert.IsNull(resp);

            ClassicAssert.IsTrue(db.StringSet(key, value));

            // nx
            resp = (string)db.Execute($"{setCommand}", key, value, "nx");
            ClassicAssert.IsNull(resp);

            // ex
            resp = (string)db.Execute($"{setCommand}", key, value, "ex", "1");
            ClassicAssert.AreEqual(okResponse, resp);
            Thread.Sleep(TimeSpan.FromSeconds(1.1));
            resp = (string)db.Execute($"{ttlCommand}", key);
            ClassicAssert.IsTrue(int.TryParse(resp, out var ttl));
            ClassicAssert.AreEqual(-2, ttl);

            // px
            resp = (string)db.Execute($"{setCommand}", key, value, "px", "1000");
            ClassicAssert.AreEqual(okResponse, resp);
            Thread.Sleep(TimeSpan.FromSeconds(1.1));
            resp = (string)db.Execute($"{ttlCommand}", key);
            ClassicAssert.IsTrue(int.TryParse(resp, out ttl));
            ClassicAssert.AreEqual(-2, ttl);

            // keepttl
            ClassicAssert.IsTrue(db.StringSet(key, 1, TimeSpan.FromMinutes(1)));
            resp = (string)db.Execute($"{setCommand}", key, value, "keepttl");
            ClassicAssert.AreEqual(okResponse, resp);
            resp = (string)db.Execute($"{ttlCommand}", key);
            ClassicAssert.IsTrue(int.TryParse(resp, out ttl) && ttl > 0 && ttl < 60);

            // ex .. nx, non-existing key
            ClassicAssert.IsTrue(db.KeyDelete(key));
            resp = (string)db.Execute($"{setCommand}", key, value, "ex", "1", "nx");
            ClassicAssert.AreEqual(okResponse, resp);
            Thread.Sleep(TimeSpan.FromSeconds(1.1));
            resp = (string)db.Execute($"{ttlCommand}", key);
            ClassicAssert.IsTrue(int.TryParse(resp, out ttl));
            ClassicAssert.AreEqual(-2, ttl);

            // ex .. nx, existing key
            ClassicAssert.IsTrue(db.StringSet(key, value));
            resp = (string)db.Execute($"{setCommand}", key, value, "ex", "1", "nx");
            ClassicAssert.IsNull(resp);

            // ex .. xx, non-existing key
            ClassicAssert.IsTrue(db.KeyDelete(key));
            resp = (string)db.Execute($"{setCommand}", key, value, "ex", "1", "xx");
            ClassicAssert.IsNull(resp);

            // ex .. xx, existing key
            ClassicAssert.IsTrue(db.StringSet(key, value));
            resp = (string)db.Execute($"{setCommand}", key, value, "ex", "1", "xx");
            ClassicAssert.AreEqual(okResponse, resp);
            Thread.Sleep(TimeSpan.FromSeconds(1.1));
            resp = (string)db.Execute($"{ttlCommand}", key);
            ClassicAssert.IsTrue(int.TryParse(resp, out ttl));
            ClassicAssert.AreEqual(-2, ttl);

            // px .. nx, non-existing key
            ClassicAssert.IsTrue(db.KeyDelete(key));
            resp = (string)db.Execute($"{setCommand}", key, value, "px", "1000", "nx");
            ClassicAssert.AreEqual(okResponse, resp);
            Thread.Sleep(TimeSpan.FromSeconds(1.1));
            resp = (string)db.Execute($"{ttlCommand}", key);
            ClassicAssert.IsTrue(int.TryParse(resp, out ttl));
            ClassicAssert.AreEqual(-2, ttl);

            // px .. nx, existing key
            ClassicAssert.IsTrue(db.StringSet(key, value));
            resp = (string)db.Execute($"{setCommand}", key, value, "px", "1000", "nx");
            ClassicAssert.IsNull(resp);

            // px .. xx, non-existing key
            ClassicAssert.IsTrue(db.KeyDelete(key));
            resp = (string)db.Execute($"{setCommand}", key, value, "px", "1000", "xx");
            ClassicAssert.IsNull(resp);

            // px .. xx, existing key
            ClassicAssert.IsTrue(db.StringSet(key, value));
            resp = (string)db.Execute($"{setCommand}", key, value, "px", "1000", "xx");
            ClassicAssert.AreEqual(okResponse, resp);
            Thread.Sleep(TimeSpan.FromSeconds(1.1));
            resp = (string)db.Execute($"{ttlCommand}", key);
            ClassicAssert.IsTrue(int.TryParse(resp, out ttl));
            ClassicAssert.AreEqual(-2, ttl);
        }

        [Test]
        public void LockTakeRelease()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
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
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void SingleIncr(int bytesPerSend)
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Bytes);

            // Key storing integer
            var nVal = -100000;
            var strKey = "key1";

            var expectedResponse = "+OK\r\n";
            var response = lightClientRequest.Execute($"SET {strKey} {nVal}", expectedResponse.Length, bytesPerSend);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "$7\r\n-100000\r\n";
            response = lightClientRequest.Execute($"GET {strKey}", expectedResponse.Length, bytesPerSend);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = ":-99999\r\n";
            response = lightClientRequest.Execute($"INCR {strKey}", expectedResponse.Length, bytesPerSend);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "$6\r\n-99999\r\n";
            response = lightClientRequest.Execute($"GET {strKey}", expectedResponse.Length, bytesPerSend);
            ClassicAssert.AreEqual(expectedResponse, response);
        }

        [Test]
        [TestCase(9999, 10)]
        [TestCase(9999, 50)]
        [TestCase(9999, 100)]
        public void SingleIncrBy(long nIncr, int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Bytes);

            // Key storing integer
            var nVal = 1000;
            var strKey = "key1";

            var expectedResponse = "+OK\r\n";
            var response = lightClientRequest.Execute($"SET {strKey} {nVal}", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "$4\r\n1000\r\n";
            response = lightClientRequest.Execute($"GET {strKey}", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = $":{nIncr + nVal}\r\n";
            response = lightClientRequest.Execute($"INCRBY {strKey} {nIncr}", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = $"${(nIncr + nVal).ToString().Length}\r\n{nIncr + nVal}\r\n";
            response = lightClientRequest.Execute($"GET {strKey}", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);
        }

        [Test]
        [TestCase("key1", 1000)]
        [TestCase("key1", 0)]
        public void SingleDecr(string strKey, int nVal)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Key storing integer
            db.StringSet(strKey, nVal);
            long n = db.StringDecrement(strKey);
            ClassicAssert.AreEqual(nVal - 1, n);
            long nRetVal = Convert.ToInt64(db.StringGet(strKey));
            ClassicAssert.AreEqual(n, nRetVal);
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
            ClassicAssert.AreEqual(n, nRetVal);
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

            ClassicAssert.AreEqual(-decrBy, retVal);
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

            ClassicAssert.AreEqual(1, retVal);

            // Key storing integer
            strKey = "key2";
            db.StringDecrement(strKey);

            retVal = Convert.ToInt32(db.StringGet(strKey));

            ClassicAssert.AreEqual(-1, retVal);
        }

        [Test]
        [TestCase(RespCommand.INCR, true)]
        [TestCase(RespCommand.DECR, true)]
        [TestCase(RespCommand.INCRBY, true)]
        [TestCase(RespCommand.DECRBY, true)]
        [TestCase(RespCommand.INCRBY, false)]
        [TestCase(RespCommand.DECRBY, false)]
        public void SimpleIncrementInvalidValue(RespCommand cmd, bool initialize)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string[] values = ["", "7 3", "02+(34", "笑い男", "01", "-01", "7ab"];

            for (var i = 0; i < values.Length; i++)
            {
                var key = $"key{i}";
                var exception = false;
                if (initialize)
                {
                    var resp = db.StringSet(key, values[i]);
                    ClassicAssert.AreEqual(true, resp);
                }
                try
                {
                    _ = cmd switch
                    {
                        RespCommand.INCR => db.StringIncrement(key),
                        RespCommand.DECR => db.StringDecrement(key),
                        RespCommand.INCRBY => initialize ? db.StringIncrement(key, 10L) : (long)db.Execute("INCRBY", [key, values[i]]),
                        RespCommand.DECRBY => initialize ? db.StringDecrement(key, 10L) : (long)db.Execute("DECRBY", [key, values[i]]),
                        _ => throw new Exception($"Command {cmd} not supported!"),
                    };
                }
                catch (Exception ex)
                {
                    exception = true;
                    var msg = ex.Message;
                    ClassicAssert.AreEqual("ERR value is not an integer or out of range.", msg);
                }
                ClassicAssert.IsTrue(exception);
            }
        }

        [Test]
        [TestCase(RespCommand.INCR)]
        [TestCase(RespCommand.DECR)]
        [TestCase(RespCommand.INCRBY)]
        [TestCase(RespCommand.DECRBY)]
        public void SimpleIncrementOverflow(RespCommand cmd)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var exception = false;

            var key = "test";
            try
            {
                switch (cmd)
                {
                    case RespCommand.INCR:
                        _ = db.StringSet(key, long.MaxValue.ToString());
                        _ = db.StringIncrement(key);
                        break;
                    case RespCommand.DECR:
                        _ = db.StringSet(key, long.MinValue.ToString());
                        _ = db.StringDecrement(key);
                        break;
                    case RespCommand.INCRBY:
                        _ = db.Execute("INCRBY", [key, ulong.MaxValue.ToString()]);
                        break;
                    case RespCommand.DECRBY:
                        _ = db.Execute("DECRBY", [key, ulong.MaxValue.ToString()]);
                        break;
                }
            }
            catch (Exception ex)
            {
                exception = true;
                var msg = ex.Message;
                ClassicAssert.AreEqual("ERR value is not an integer or out of range.", msg);
            }
            ClassicAssert.IsTrue(exception);
        }

        [Test]
        public void SimpleIncrementByFloatWithNoKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key1";
            var incrByValue = 10.5;
            var expectedResult = incrByValue;

            var actualResultStr = (string)db.Execute("INCRBYFLOAT", [key, incrByValue]);
            var actualResultRawStr = db.StringGet(key);

            var actualResult = double.Parse(actualResultStr, CultureInfo.InvariantCulture);
            var actualResultRaw = double.Parse((string)actualResultRawStr, CultureInfo.InvariantCulture);

            Assert.That(actualResult, Is.EqualTo(expectedResult).Within(1.0 / Math.Pow(10, 15)));
            Assert.That(actualResult, Is.EqualTo(actualResultRaw).Within(1.0 / Math.Pow(10, 15)));
        }

        [Test]
        [TestCase(0, 0.1)]
        [TestCase(0.1, 12.5)]
        [TestCase(12.6, 0)]
        [TestCase(10, 10)]
        [TestCase(910151, 0.23659)]
        [TestCase(663.12336412, 12342.3)]
        [TestCase(10, -110)]
        [TestCase(110, -110.234)]
        [TestCase(-2110.95255555, -110.234)]
        [TestCase(-2110.95255555, 100000.526654512219412)]
        [TestCase(double.MaxValue, double.MinValue)]
        public void SimpleIncrementByFloat(double initialValue, double incrByValue)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key1";
            db.StringSet(key, initialValue);
            var expectedResult = initialValue + incrByValue;

            var actualResultStr = (string)db.Execute("INCRBYFLOAT", [key, incrByValue]);
            var actualResultRawStr = db.StringGet(key);

            var actualResult = double.Parse(actualResultStr, CultureInfo.InvariantCulture);
            var actualResultRaw = double.Parse((string)actualResultRawStr, CultureInfo.InvariantCulture);

            Assert.That(actualResult, Is.EqualTo(expectedResult).Within(1.0 / Math.Pow(10, 15)));
            Assert.That(actualResult, Is.EqualTo(actualResultRaw).Within(1.0 / Math.Pow(10, 15)));
        }

        [Test]
        [TestCase("abc", 10)]
        [TestCase(10, "xyz")]
        [TestCase(10, "inf")]
        [TestCase(double.PositiveInfinity, double.NegativeInfinity)]
        public void SimpleIncrementByFloatWithInvalidFloat(object initialValue, object incrByValue)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key1";
            if (initialValue is double)
                db.StringSet(key, (double)initialValue);
            else if (initialValue is string)
                db.StringSet(key, (string)initialValue);

            Assert.Throws<RedisServerException>(() => db.Execute("INCRBYFLOAT", key, incrByValue));
        }

        [Test]
        [TestCase(double.MinValue, double.MinValue)]
        [TestCase(double.MaxValue, double.MaxValue)]
        public void SimpleIncrementByFloatWithOutOfRangeFloat(object initialValue, object incrByValue)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key1";
            if (initialValue is double)
                db.StringSet(key, (double)initialValue);
            else if (initialValue is string)
                db.StringSet(key, (string)initialValue);

            // TODO: This is RedisServerException in the InPlaceUpdater call, but GetRMWModifiedFieldInfo currently throws RedisConnectionException.
            // This can be different in CIs vs. locally.
            Assert.That(() => db.Execute("INCRBYFLOAT", key, incrByValue),
                    Throws.TypeOf<RedisServerException>().Or.TypeOf<RedisConnectionException>());
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
            ClassicAssert.AreEqual(retVal, false);
        }

        [Test]
        public void SingleDeleteWithObjectStoreDisabled()
        {
            TearDown();

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: true);
            server.Start();

            var key = "delKey";
            var value = "1234";
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.StringSet(key, value);

            var resp = (string)db.StringGet(key);
            ClassicAssert.AreEqual(resp, value);

            var respDel = db.KeyDelete(key);
            ClassicAssert.IsTrue(respDel);

            respDel = db.KeyDelete(key);
            ClassicAssert.IsFalse(respDel);
        }

        [Test]
        public void SingleDeleteWithObjectStoreDisable_LTM()
        {
            TearDown();

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, disableObjects: true);
            server.Start();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 5;
            int valLen = 256;
            int keyLen = 8;

            List<Tuple<string, string>> data = [];
            for (int i = 0; i < keyCount; i++)
            {
                data.Add(new Tuple<string, string>(TestUtils.GetRandomString(keyLen), TestUtils.GetRandomString(valLen)));
                var pair = data.Last();
                db.StringSet(pair.Item1, pair.Item2);
            }


            for (int i = 0; i < keyCount; i++)
            {
                var pair = data[i];

                var resp = (string)db.StringGet(pair.Item1);
                ClassicAssert.AreEqual(resp, pair.Item2);

                var respDel = db.KeyDelete(pair.Item1);
                resp = (string)db.StringGet(pair.Item1);
                ClassicAssert.IsNull(resp);

                respDel = db.KeyDelete(pair.Item2);
                ClassicAssert.IsFalse(respDel);
            }
        }

        [Test]
        public void GarnetObjectStoreDisabledError()
        {
            TearDown();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var iter = 100;
            var mykey = "mykey";
            for (var i = 0; i < iter; i++)
            {
                var exception = Assert.Throws<RedisServerException>(() => _ = db.ListLength(mykey));
                ClassicAssert.AreEqual("ERR Garnet Exception: Object store is disabled", exception.Message);
            }

            // Ensure connection is still healthy
            for (var i = 0; i < iter; i++)
            {
                var myvalue = "myvalue" + i;
                var result = db.StringSet(mykey, myvalue);
                ClassicAssert.IsTrue(result);
                var returned = (string)db.StringGet(mykey);
                ClassicAssert.AreEqual(myvalue, returned);
            }
        }

        [Test]
        public void MultiKeyDelete([Values] bool withoutObjectStore)
        {
            if (withoutObjectStore)
            {
                TearDown();
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: true);
                server.Start();
            }

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 10;
            int valLen = 16;
            int keyLen = 8;

            List<Tuple<string, string>> data = [];
            for (int i = 0; i < keyCount; i++)
            {
                data.Add(new Tuple<string, string>(TestUtils.GetRandomString(keyLen), TestUtils.GetRandomString(valLen)));
                var pair = data.Last();
                db.StringSet(pair.Item1, pair.Item2);
            }

            var keys = data.Select(x => (RedisKey)x.Item1).ToArray();
            var keysDeleted = db.KeyDeleteAsync(keys);
            keysDeleted.Wait();
            ClassicAssert.AreEqual(keysDeleted.Result, 10);

            var keysDel = db.KeyDelete(keys);
            ClassicAssert.AreEqual(keysDel, 0);
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

            List<string> keys = [];
            for (int i = 0; i < keyCount; i++)
            {
                keys.Add(TestUtils.GetRandomString(keyLen));
                var key = keys.Last();

                for (int j = 0; j < setCount; j++)
                {
                    var member = TestUtils.GetRandomString(valLen);
                    db.SetAdd(key, member);
                }
            }

            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();
            var keysDeleted = db.KeyDeleteAsync(redisKeys);
            keysDeleted.Wait();
            ClassicAssert.AreEqual(keysDeleted.Result, 10);

            var keysDel = db.KeyDelete(redisKeys);
            ClassicAssert.AreEqual(keysDel, 0);
        }

        [Test]
        public void MultiKeyUnlink([Values] bool withoutObjectStore)
        {
            if (withoutObjectStore)
            {
                TearDown();
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: true);
                server.Start();
            }

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int keyCount = 10;
            int valLen = 16;
            int keyLen = 8;

            List<Tuple<string, string>> data = [];
            for (int i = 0; i < keyCount; i++)
            {
                data.Add(new Tuple<string, string>(TestUtils.GetRandomString(keyLen), TestUtils.GetRandomString(valLen)));
                var pair = data.Last();
                db.StringSet(pair.Item1, pair.Item2);
            }

            var keys = data.Select(x => (object)x.Item1).ToArray();
            var keysDeleted = (string)db.Execute("unlink", keys);
            ClassicAssert.AreEqual(10, int.Parse(keysDeleted));

            keysDeleted = (string)db.Execute("unlink", keys);
            ClassicAssert.AreEqual(0, int.Parse(keysDeleted));
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

            List<string> keys = [];
            for (int i = 0; i < keyCount; i++)
            {
                keys.Add(TestUtils.GetRandomString(keyLen));
                var key = keys.Last();

                for (int j = 0; j < setCount; j++)
                {
                    var member = TestUtils.GetRandomString(valLen);
                    db.SetAdd(key, member);
                }
            }

            var redisKey = keys.Select(x => (object)x).ToArray();
            var keysDeleted = (string)db.Execute("unlink", redisKey);
            ClassicAssert.AreEqual(Int32.Parse(keysDeleted), 10);

            keysDeleted = (string)db.Execute("unlink", redisKey);
            ClassicAssert.AreEqual(Int32.Parse(keysDeleted), 0);
        }

        [Test]
        public void SingleExists([Values] bool withoutObjectStore)
        {
            if (withoutObjectStore)
            {
                TearDown();
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: true);
                server.Start();
            }
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Key storing integer
            var nVal = 100;
            var strKey = "key1";
            ClassicAssert.IsFalse(db.KeyExists(strKey));
            db.StringSet(strKey, nVal);

            bool fExists = db.KeyExists("key1", CommandFlags.None);
            ClassicAssert.AreEqual(fExists, true);

            fExists = db.KeyExists("key2", CommandFlags.None);
            ClassicAssert.AreEqual(fExists, false);
        }

        [Test]
        public void SingleExistsObject()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "key";
            ClassicAssert.IsFalse(db.KeyExists(key));

            var listData = new RedisValue[] { "a", "b", "c", "d" };
            var count = db.ListLeftPush(key, listData);
            ClassicAssert.AreEqual(4, count);
            ClassicAssert.True(db.KeyExists(key));
        }

        [Test]
        public void MultipleExistsKeysAndObjects()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var count = db.ListLeftPush("listKey", ["a", "b", "c", "d"]);
            ClassicAssert.AreEqual(4, count);

            var zaddItems = db.SortedSetAdd("zset:test", [new SortedSetEntry("a", 1), new SortedSetEntry("b", 2)]);
            ClassicAssert.AreEqual(2, zaddItems);

            db.StringSet("foo", "bar");

            var exists = db.KeyExists(["key", "listKey", "zset:test", "foo"]);
            ClassicAssert.AreEqual(3, exists);
        }

        #region ExpireTime

        [Test]
        public void ExpiretimeWithStringValue()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string key = "key1";
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            db.StringSet(key, "test1", expireTimeSpan);

            var actualExpireTime = (long)db.Execute("EXPIRETIME", key);

            ClassicAssert.GreaterOrEqual(actualExpireTime, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            var expireExpireTime = DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds();
            ClassicAssert.LessOrEqual(actualExpireTime, expireExpireTime);
        }

        [Test]
        public void ExpiretimeWithUnknownKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var expireTime = (long)db.Execute("EXPIRETIME", "keyZ");

            ClassicAssert.AreEqual(-2, expireTime);
        }

        [Test]
        public void ExpiretimeWithNoKeyExpiration()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string key = "key1";
            db.StringSet(key, "test1");

            var expireTime = (long)db.Execute("EXPIRETIME", key);

            ClassicAssert.AreEqual(-1, expireTime);
        }

        [Test]
        public void ExpiretimeWithInvalidNumberOfArgs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var exception = Assert.Throws<RedisServerException>(() => db.Execute("EXPIRETIME"));
            Assert.That(exception.Message, Does.StartWith("ERR wrong number of arguments"));
        }

        [Test]
        public void ExpiretimeWithObjectValue()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key1";
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            var origList = new RedisValue[] { "a", "b", "c", "d" };
            var count = db.ListRightPush(key, origList);
            var expirySet = db.KeyExpire(key, expireTimeSpan);

            var actualExpireTime = (long)db.Execute("EXPIRETIME", key);

            ClassicAssert.GreaterOrEqual(actualExpireTime, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            var expireExpireTime = DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds();
            ClassicAssert.LessOrEqual(actualExpireTime, expireExpireTime);
        }

        [Test]
        public void ExpiretimeWithNoKeyExpirationForObjectValue()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key1";
            var origList = new RedisValue[] { "a", "b", "c", "d" };
            var count = db.ListRightPush(key, origList);

            var expireTime = (long)db.Execute("EXPIRETIME", key);

            ClassicAssert.AreEqual(-1, expireTime);
        }

        [Test]
        public void PExpiretimeWithStingValue()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string key = "key1";
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            db.StringSet(key, "test1", expireTimeSpan);

            var actualExpireTime = (long)db.Execute("PEXPIRETIME", key);

            ClassicAssert.GreaterOrEqual(actualExpireTime, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            var expireExpireTime = DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();
            ClassicAssert.LessOrEqual(actualExpireTime, expireExpireTime);
        }

        [Test]
        public void PExpiretimeWithUnknownKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var expireTime = (long)db.Execute("PEXPIRETIME", "keyZ");

            ClassicAssert.AreEqual(-2, expireTime);
        }

        [Test]
        public void PExpiretimeWithNoKeyExpiration()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string key = "key1";
            db.StringSet(key, "test1");

            var expireTime = (long)db.Execute("PEXPIRETIME", key);

            ClassicAssert.AreEqual(-1, expireTime);
        }

        [Test]
        public void PExpiretimeWithInvalidNumberOfArgs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var exception = Assert.Throws<RedisServerException>(() => db.Execute("PEXPIRETIME"));
            Assert.That(exception.Message, Does.StartWith("ERR wrong number of arguments"));
        }

        [Test]
        public void PExpiretimeWithObjectValue()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key1";
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            var origList = new RedisValue[] { "a", "b", "c", "d" };
            var count = db.ListRightPush(key, origList);
            var expirySet = db.KeyExpire(key, expireTimeSpan);

            var actualExpireTime = (long)db.Execute("PEXPIRETIME", key);

            ClassicAssert.GreaterOrEqual(actualExpireTime, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            var expireExpireTime = DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();
            ClassicAssert.LessOrEqual(actualExpireTime, expireExpireTime);
        }

        [Test]
        public void PExpiretimeWithNoKeyExpirationForObjectValue()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key1";
            var origList = new RedisValue[] { "a", "b", "c", "d" };
            var count = db.ListRightPush(key, origList);

            var expireTime = (long)db.Execute("PEXPIRETIME", key);

            ClassicAssert.AreEqual(-1, expireTime);
        }

        #endregion

        [Test]
        public void SingleRename()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "test1";
            db.StringSet("key1", origValue);

            db.KeyRename("key1", "key2");
            string retValue = db.StringGet("key2");

            ClassicAssert.AreEqual(origValue, retValue);

            origValue = db.StringGet("key1");
            ClassicAssert.AreEqual(null, origValue);
        }

        [Test]
        public void SingleRenameWithExpiry()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origValue = "test1";
            db.StringSet("key1", origValue, TimeSpan.FromMinutes(1));

            db.KeyRename("key1", "key2");
            string retValue = db.StringGet("key2");

            ClassicAssert.AreEqual(origValue, retValue);

            var ttl = db.KeyTimeToLive("key2");
            ClassicAssert.IsTrue(ttl.HasValue);
            ClassicAssert.Greater(ttl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(ttl.Value.TotalMilliseconds, TimeSpan.FromMinutes(1).TotalMilliseconds);
        }

        [Test]
        public void SingleRenameKeyEdgeCase([Values] bool withoutObjectStore)
        {
            if (withoutObjectStore)
            {
                TearDown();
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: true);
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
                ClassicAssert.AreEqual("ERR no such key", ex.Message);
            }

            //2. Key rename oldKey.Equals(newKey)
            string origValue = "test1";
            db.StringSet("key1", origValue);
            bool renameRes = db.KeyRename("key1", "key1");
            ClassicAssert.IsTrue(renameRes);
            string retValue = db.StringGet("key1");
            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public void SingleRenameObjectStore()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origList = new RedisValue[] { "a", "b", "c", "d" };
            var key1 = "lkey1";
            var count = db.ListRightPush(key1, origList);
            ClassicAssert.AreEqual(4, count);

            var result = db.ListRange(key1);
            ClassicAssert.AreEqual(origList, result);

            var key2 = "lkey2";
            var rb = db.KeyRename(key1, key2);
            ClassicAssert.IsTrue(rb);
            result = db.ListRange(key1);
            ClassicAssert.AreEqual(Array.Empty<RedisValue>(), result);

            result = db.ListRange(key2);
            ClassicAssert.AreEqual(origList, result);
        }

        [Test]
        public void SingleRenameObjectStoreWithExpiry()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origList = new RedisValue[] { "a", "b", "c", "d" };
            var key1 = "lkey1";
            var count = db.ListRightPush(key1, origList);
            ClassicAssert.AreEqual(4, count);

            var result = db.ListRange(key1);
            ClassicAssert.AreEqual(origList, result);

            var expirySet = db.KeyExpire("lkey1", TimeSpan.FromMinutes(1));
            ClassicAssert.IsTrue(expirySet);

            var key2 = "lkey2";
            var rb = db.KeyRename(key1, key2);
            ClassicAssert.IsTrue(rb);
            result = db.ListRange(key1);
            ClassicAssert.AreEqual(Array.Empty<RedisValue>(), result);

            result = db.ListRange(key2);
            ClassicAssert.AreEqual(origList, result);

            var ttl = db.KeyTimeToLive("lkey2");
            ClassicAssert.IsTrue(ttl.HasValue);
            ClassicAssert.Greater(ttl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(ttl.Value.TotalMilliseconds, TimeSpan.FromMinutes(1).TotalMilliseconds);
        }

        [Test]
        public void SingleRenameWithOldKeyAndNewKeyAsSame()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var origValue = "test1";
            var key = "key1";
            db.StringSet(key, origValue);

            var result = db.KeyRename(key, key);

            ClassicAssert.IsTrue(result);
            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(origValue, retValue);
        }

        #region RENAMENX

        [Test]
        public void SingleRenameNx()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "test1";
            db.StringSet("key1", origValue);

            var result = db.KeyRename("key1", "key2", When.NotExists);
            ClassicAssert.IsTrue(result);

            string retValue = db.StringGet("key2");
            ClassicAssert.AreEqual(origValue, retValue);

            origValue = db.StringGet("key1");
            ClassicAssert.AreEqual(null, origValue);
        }

        [Test]
        public void SingleRenameNxWithNewKeyAlreadyExist()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string origValue = "test1";
            string origValue2 = "test2";
            db.StringSet("key1", origValue);
            db.StringSet("key2", origValue2);

            var result = db.KeyRename("key1", "key2", When.NotExists);
            ClassicAssert.IsFalse(result);

            string retValue2 = db.StringGet("key2");
            ClassicAssert.AreEqual(origValue2, retValue2);

            string retValue1 = db.StringGet("key1");
            ClassicAssert.AreEqual(origValue, retValue1);
        }

        [Test]
        public void SingleRenameNxWithExpiry()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origValue = "test1";
            db.StringSet("key1", origValue, TimeSpan.FromMinutes(1));

            var result = db.KeyRename("key1", "key2", When.NotExists);
            ClassicAssert.IsTrue(result);

            string retValue = db.StringGet("key2");
            ClassicAssert.AreEqual(origValue, retValue);

            var ttl = db.KeyTimeToLive("key2");
            ClassicAssert.IsTrue(ttl.HasValue);
            ClassicAssert.Greater(ttl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(ttl.Value.TotalMilliseconds, TimeSpan.FromMinutes(1).TotalMilliseconds);
        }

        [Test]
        public void SingleRenameNxWithExpiryAndNewKeyAlreadyExist()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origValue = "test1";
            string origValue2 = "test2";
            db.StringSet("key1", origValue, TimeSpan.FromMinutes(1));
            db.StringSet("key2", origValue2, TimeSpan.FromMinutes(1));

            var result = db.KeyRename("key1", "key2", When.NotExists);
            ClassicAssert.IsFalse(result);

            string retValue = db.StringGet("key2");
            ClassicAssert.AreEqual(origValue2, retValue);

            var ttl = db.KeyTimeToLive("key2");
            ClassicAssert.IsTrue(ttl.HasValue);
            ClassicAssert.Greater(ttl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(ttl.Value.TotalMilliseconds, TimeSpan.FromMinutes(1).TotalMilliseconds);

            string retValue1 = db.StringGet("key1");
            CollectionAssert.AreEqual(origValue, retValue1);
        }

        [Test]
        public void SingleRenameNxObjectStore()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origList = new RedisValue[] { "a", "b", "c", "d" };
            var key1 = "lkey1";
            var count = db.ListRightPush(key1, origList);
            var result = db.ListRange(key1);
            var key2 = "lkey2";

            var rb = db.KeyRename(key1, key2, When.NotExists);
            ClassicAssert.IsTrue(rb);

            result = db.ListRange(key1);
            CollectionAssert.AreEqual(Array.Empty<RedisValue>(), result);

            result = db.ListRange(key2);
            CollectionAssert.AreEqual(origList, result);
        }

        [Test]
        public void SingleRenameNxObjectStoreWithNewKeyAlreadyExist()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origList = new RedisValue[] { "a", "b", "c", "d" };
            var origList2 = new RedisValue[] { "z", "y", "z" };
            var key1 = "lkey1";
            var key2 = "lkey2";
            db.ListRightPush(key1, origList);
            db.ListRightPush(key2, origList2);

            var rb = db.KeyRename(key1, key2, When.NotExists);
            ClassicAssert.IsFalse(rb);

            var result = db.ListRange(key1);
            ClassicAssert.AreEqual(origList, result);

            result = db.ListRange(key2);
            ClassicAssert.AreEqual(origList2, result);
        }

        [Test]
        public void SingleRenameNxObjectStoreWithExpiry()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origList = new RedisValue[] { "a", "b", "c", "d" };
            var key1 = "lkey1";
            var count = db.ListRightPush(key1, origList);
            var result = db.ListRange(key1);
            var expirySet = db.KeyExpire("lkey1", TimeSpan.FromMinutes(1));
            var key2 = "lkey2";

            var rb = db.KeyRename(key1, key2, When.NotExists);
            ClassicAssert.IsTrue(rb);

            result = db.ListRange(key1);
            ClassicAssert.AreEqual(Array.Empty<RedisValue>(), result);

            result = db.ListRange(key2);
            ClassicAssert.AreEqual(origList, result);

            var ttl = db.KeyTimeToLive(key2);
            ClassicAssert.IsTrue(ttl.HasValue);
            ClassicAssert.Greater(ttl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(ttl.Value.TotalMilliseconds, TimeSpan.FromMinutes(1).TotalMilliseconds);
        }

        [Test]
        public void SingleRenameNxObjectStoreWithExpiryAndNewKeyAlreadyExist()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var origList = new RedisValue[] { "a", "b", "c", "d" };
            var origList2 = new RedisValue[] { "x", "y", "z" };
            var key1 = "lkey1";
            var key2 = "lkey2";
            db.ListRightPush(key1, origList);
            db.ListRightPush(key2, origList2);
            var result = db.ListRange(key1);
            var expirySet = db.KeyExpire(key1, TimeSpan.FromMinutes(1));

            var rb = db.KeyRename(key1, key2, When.NotExists);
            ClassicAssert.IsFalse(rb);

            result = db.ListRange(key1);
            ClassicAssert.AreEqual(origList, result);

            result = db.ListRange(key2);
            ClassicAssert.AreEqual(origList2, result);

            var ttl = db.KeyTimeToLive(key1);
            ClassicAssert.IsTrue(ttl.HasValue);
            ClassicAssert.Greater(ttl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(ttl.Value.TotalMilliseconds, TimeSpan.FromMinutes(1).TotalMilliseconds);

            var ttl2 = db.KeyTimeToLive(key2);
            ClassicAssert.IsFalse(ttl2.HasValue);
        }

        [Test]
        public void SingleRenameNxWithKeyNotExist()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var exception = Assert.Throws<RedisServerException>(() => db.KeyRename("key1", "key2", When.NotExists));
            ClassicAssert.AreEqual("ERR no such key", exception.Message);
        }

        [Test]
        public void SingleRenameNxWithOldKeyAndNewKeyAsSame()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var origValue = "test1";
            var key = "key1";
            db.StringSet(key, origValue);

            var result = db.KeyRename(key, key, When.NotExists);

            ClassicAssert.IsTrue(result);
            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public void SingleRenameNXWithEtagSetOldAndNewKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var origValue = "test1";
            var key = "key1";
            var newKey = "key2";

            db.Execute("EXECWITHETAG", "SET", key, origValue);
            db.Execute("EXECWITHETAG", "SET", newKey, "foo");

            var result = db.KeyRename(key, newKey, When.NotExists);
            ClassicAssert.IsFalse(result);
        }

        [Test]
        public void SingleRenameNXWithEtagSetOldKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var origValue = "test1";
            var key = "key1";
            var newKey = "key2";

            db.Execute("EXECWITHETAG", "SET", key, origValue);

            var result = db.KeyRename(key, newKey, When.NotExists);
            ClassicAssert.IsTrue(result);

            string retValue = db.StringGet(newKey);
            ClassicAssert.AreEqual(origValue, retValue);

            var oldKeyRes = db.StringGet(key);
            ClassicAssert.IsTrue(oldKeyRes.IsNull);

            // Since the original key was set with etag, the new key should have an etag attached to it
            var etagRes = (RedisResult[])db.Execute("EXECWITHETAG", "GET", newKey);
            ClassicAssert.AreEqual(0, (long)etagRes[0]);
            ClassicAssert.AreEqual(origValue, etagRes[1].ToString());
        }

        #endregion

        [Test]
        public void CanSelectCommand()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var reply = db.Execute("SELECT", "0");
            ClassicAssert.IsTrue(reply.ToString() == "OK");
            Assert.Throws<RedisServerException>(() => db.Execute("SELECT", "17"));

            //select again the def db
            db.Execute("SELECT", "0");
        }

        [Test]
        public void CanSelectCommandLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Bytes);

            var expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_DB_INDEX_OUT_OF_RANGE)}\r\n+PONG\r\n";
            var response = lightClientRequest.Execute("SELECT 17", "PING", expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, response);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanDoCommandsInChunks(int bytesSent)
        {
            // SETEX
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Bytes);

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
        public void CanSetGetCommandsChunks(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Bytes);
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

        [Test]
        public void PersistTTLTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "expireKey";
            var val = "expireValue";
            var expire = 2;

            var ttl = db.Execute("TTL", key);
            ClassicAssert.AreEqual(-2, (int)ttl);

            db.StringSet(key, val);
            ttl = db.Execute("TTL", key);
            ClassicAssert.AreEqual(-1, (int)ttl);

            db.KeyExpire(key, TimeSpan.FromSeconds(expire));

            var time = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(time.Value.TotalSeconds > 0);

            db.KeyExpire(key, TimeSpan.FromSeconds(expire));
            db.KeyPersist(key);

            Thread.Sleep((expire + 1) * 1000);

            var _val = db.StringGet(key);
            ClassicAssert.AreEqual(val, _val.ToString());

            time = db.KeyTimeToLive(key);
            ClassicAssert.IsNull(time);
        }

        [Test]
        public void ObjectTTLTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "expireKey";
            var expire = 2;

            var ttl = db.Execute("TTL", key);
            ClassicAssert.AreEqual(-2, (int)ttl);

            db.SortedSetAdd(key, key, 1.0);
            ttl = db.Execute("TTL", key);
            ClassicAssert.AreEqual(-1, (int)ttl);

            db.KeyExpire(key, TimeSpan.FromSeconds(expire));

            var time = db.KeyTimeToLive(key);
            ClassicAssert.IsNotNull(time);
            ClassicAssert.IsTrue(time.Value.TotalSeconds > 0);
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
            ClassicAssert.IsFalse(response);

            db.KeyExpire(keyA, TimeSpan.FromSeconds(expire));
            var time = db.KeyTimeToLive(keyA);
            ClassicAssert.IsTrue(time.Value.TotalSeconds > 0);

            response = db.KeyPersist(keyA);
            ClassicAssert.IsTrue(response);

            time = db.KeyTimeToLive(keyA);
            ClassicAssert.IsTrue(time == null);

            var value = db.StringGet(keyA);
            ClassicAssert.AreEqual(value, keyA);

            var noKey = "noKey";
            response = db.KeyPersist(noKey);
            ClassicAssert.IsFalse(response);
        }

        [Test]
        public void PersistObjectTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int expire = 100;
            var keyA = "keyA";
            db.SortedSetAdd(keyA, [new SortedSetEntry("element", 1.0)]);
            var response = db.KeyPersist(keyA);
            ClassicAssert.IsFalse(response);

            db.KeyExpire(keyA, TimeSpan.FromSeconds(expire));
            var time = db.KeyTimeToLive(keyA);
            ClassicAssert.IsTrue(time.Value.TotalSeconds > 0);

            response = db.KeyPersist(keyA);
            ClassicAssert.IsTrue(response);

            time = db.KeyTimeToLive(keyA);
            ClassicAssert.IsTrue(time == null);

            var value = db.SortedSetScore(keyA, "element");
            ClassicAssert.AreEqual(1.0, value);
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
            ClassicAssert.AreEqual(key, (string)value);

            if (command.Equals("EXPIRE"))
            {
                var res = db.KeyExpire(key, TimeSpan.FromSeconds(1));
            }
            else
                db.Execute(command, [key, 1000]);

            Thread.Sleep(1500);

            value = db.StringGet(key);
            ClassicAssert.AreEqual(null, (string)value);
        }

        [Test]
        [TestCase("EXPIRE")]
        [TestCase("PEXPIRE")]
        public void KeyExpireObjectTest(string command)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "keyA";
            db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);

            var value = db.SortedSetScore(key, "element");
            ClassicAssert.AreEqual(1.0, value, "Get Score before expiration");

            var actualDbSize = db.Execute("DBSIZE");
            ClassicAssert.AreEqual(1, (ulong)actualDbSize, "DBSIZE before expiration");

            var actualKeys = db.Execute("KEYS", ["*"]);
            ClassicAssert.AreEqual(1, ((RedisResult[])actualKeys).Length, "KEYS before expiration");

            var actualScan = db.Execute("SCAN", "0");
            ClassicAssert.AreEqual(1, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN before expiration");

            var exp = db.KeyExpire(key, command.Equals("EXPIRE") ? TimeSpan.FromSeconds(1) : TimeSpan.FromMilliseconds(1000));
            ClassicAssert.IsTrue(exp);

            // Sleep to wait for expiration
            Thread.Sleep(1500);

            value = db.SortedSetScore(key, "element");
            ClassicAssert.AreEqual(null, value, "Get Score after expiration");

            actualDbSize = db.Execute("DBSIZE");
            ClassicAssert.AreEqual(0, (ulong)actualDbSize, "DBSIZE after expiration");

            actualKeys = db.Execute("KEYS", ["*"]);
            ClassicAssert.AreEqual(0, ((RedisResult[])actualKeys).Length, "KEYS after expiration");

            actualScan = db.Execute("SCAN", "0");
            ClassicAssert.AreEqual(0, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN after expiration");
        }

        [Test]
        public void KeyExpireOptionsTest([Values("EXPIRE", "PEXPIRE")] string command, [Values(false, true)] bool testCaseSensitivity)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "keyA";
            object[] args = [key, 1000, ""];
            db.StringSet(key, key);

            args[2] = testCaseSensitivity ? "Xx" : "XX";// XX -- Set expiry only when the key has an existing expiry
            bool resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsFalse(resp);//XX return false no existing expiry

            args[2] = testCaseSensitivity ? "nX" : "NX";// NX -- Set expiry only when the key has no expiry
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsTrue(resp);// NX return true no existing expiry

            args[2] = testCaseSensitivity ? "nx" : "NX";// NX -- Set expiry only when the key has no expiry
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsFalse(resp);// NX return false existing expiry

            args[1] = 50;
            args[2] = testCaseSensitivity ? "xx" : "XX";// XX -- Set expiry only when the key has an existing expiry
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsTrue(resp);// XX return true existing expiry

            var time = db.KeyTimeToLive(key);
            ClassicAssert.Greater(time.Value.TotalSeconds, 0);
            ClassicAssert.LessOrEqual(time.Value.TotalSeconds, (int)args[1]);

            args[1] = 1;
            args[2] = testCaseSensitivity ? "Gt" : "GT";// GT -- Set expiry only when the new expiry is greater than current one
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsFalse(resp); // GT return false new expiry < current expiry

            args[1] = 1000;
            args[2] = testCaseSensitivity ? "gT" : "GT";// GT -- Set expiry only when the new expiry is greater than current one
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsTrue(resp); // GT return true new expiry > current expiry
            time = db.KeyTimeToLive(key);

            ClassicAssert.Greater(command.Equals("EXPIRE") ?
                    time.Value.TotalSeconds : time.Value.TotalMilliseconds, 500);

            args[1] = 2000;
            args[2] = testCaseSensitivity ? "lt" : "LT";// LT -- Set expiry only when the new expiry is less than current one
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsFalse(resp); // LT return false new expiry > current expiry

            args[1] = 500;
            args[2] = testCaseSensitivity ? "lT" : "LT";// LT -- Set expiry only when the new expiry is less than current one
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsTrue(resp); // LT return true new expiry < current expiry
            time = db.KeyTimeToLive(key);

            ClassicAssert.Greater(time.Value.TotalSeconds, 0);

            ClassicAssert.LessOrEqual(command.Equals("EXPIRE") ?
                    time.Value.TotalSeconds : time.Value.TotalMilliseconds, (int)args[1]);
        }

        [Test]
        [TestCase("EXPIRE")]
        [TestCase("PEXPIRE")]
        public void KeyExpireBadOptionTests(string command)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            ClassicAssert.IsTrue(db.StringSet("foo", "bar"));

            // Invalid should be rejected
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute(command, "foo", "100", "Q"));
                ClassicAssert.AreEqual("ERR Unsupported option Q", exc.Message);
            }

            // None should be rejected
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute(command, "foo", "100", "None"));
                ClassicAssert.AreEqual("ERR Unsupported option None", exc.Message);
            }

            // Numeric equivalent should be rejected
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute(command, "foo", "100", "1"));
                ClassicAssert.AreEqual("ERR Unsupported option 1", exc.Message);
            }

            // Numeric out of bounds should be rejected
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute(command, "foo", "100", "128"));
                ClassicAssert.AreEqual("ERR Unsupported option 128", exc.Message);
            }
        }

        #region ExpireAt

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithStringAndObject(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
            }
            else
            {
                db.StringSet(key, "valueA");
            }

            var actualResult = (int)db.Execute(command, "key", expireTimeUnix);
            ClassicAssert.AreEqual(actualResult, 1);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, expireTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase("EXPIREAT")]
        [TestCase("PEXPIREAT")]
        public void KeyExpireAtWithUnknownKey(string command)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix);
            ClassicAssert.AreEqual(actualResult, 0);
        }

        [Test]
        [TestCase("EXPIREAT")]
        [TestCase("PEXPIREAT")]
        public void KeyExpireAtWithoutArgs(string command)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";

            Assert.Throws<RedisServerException>(() => db.Execute(command, key));
        }

        [Test]
        [TestCase("EXPIREAT")]
        [TestCase("PEXPIREAT")]
        public void KeyExpireAtWithUnknownArgs(string command)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            Assert.Throws<RedisServerException>(() => db.Execute(command, key, expireTimeUnix, "YY"));
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithNxOptionAndKeyHasExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var existingExpireTimeSpan = TimeSpan.FromMinutes(1);
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
                db.KeyExpire(key, existingExpireTimeSpan);
            }
            else
            {
                db.StringSet(key, "valueA", existingExpireTimeSpan);
            }
            var expireTimeSpan = TimeSpan.FromMinutes(10);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "Nx");
            ClassicAssert.AreEqual(actualResult, 0);

            // Test if the existing expiry time is still the same
            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, existingExpireTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithNxOptionAndKeyHasNoExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var expireTimeSpan = TimeSpan.FromMinutes(10);
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
            }
            else
            {
                db.StringSet(key, "valueA");
            }
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "nX");
            ClassicAssert.AreEqual(1, actualResult);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, expireTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithXxOptionAndKeyHasExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var existingExpireTimeSpan = TimeSpan.FromMinutes(1);
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
                db.KeyExpire(key, existingExpireTimeSpan);
            }
            else
            {
                db.StringSet(key, "valueA", existingExpireTimeSpan);
            }
            var expireTimeSpan = TimeSpan.FromMinutes(10);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "Xx");
            ClassicAssert.AreEqual(actualResult, 1);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, existingExpireTimeSpan.TotalMilliseconds);
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, expireTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithXxOptionAndKeyHasNoExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var expireTimeSpan = TimeSpan.FromMinutes(10);
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
            }
            else
            {
                db.StringSet(key, "valueA");
            }
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "xX");
            ClassicAssert.AreEqual(actualResult, 0);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsFalse(actualTtl.HasValue);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithGtOptionAndExistingKeyHasSmallerExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var existingExpireTimeSpan = TimeSpan.FromMinutes(1);
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
                db.KeyExpire(key, existingExpireTimeSpan);
            }
            else
            {
                db.StringSet(key, "valueA", existingExpireTimeSpan);
            }
            var expireTimeSpan = TimeSpan.FromMinutes(10);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "gT");
            ClassicAssert.AreEqual(actualResult, 1);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, existingExpireTimeSpan.TotalMilliseconds);
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, expireTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithGtOptionAndExistingKeyHasLargerExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var existingExpireTimeSpan = TimeSpan.FromMinutes(10);
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
                db.KeyExpire(key, existingExpireTimeSpan);
            }
            else
            {
                db.StringSet(key, "valueA", existingExpireTimeSpan);
            }
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "Gt");
            ClassicAssert.AreEqual(actualResult, 0);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, expireTimeSpan.TotalMilliseconds);
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, existingExpireTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithGtOptionAndExistingKeyNoExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
            }
            else
            {
                db.StringSet(key, "valueA");
            }
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "GT");
            ClassicAssert.AreEqual(actualResult, 0);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsFalse(actualTtl.HasValue);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithXxAndGtOptionAndExistingKeyHasSmallerExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var existingExpireTimeSpan = TimeSpan.FromMinutes(1);
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
                db.KeyExpire(key, existingExpireTimeSpan);
            }
            else
            {
                db.StringSet(key, "valueA", existingExpireTimeSpan);
            }
            var expireTimeSpan = TimeSpan.FromMinutes(10);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "xx", "GT");
            ClassicAssert.AreEqual(actualResult, 1);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, existingExpireTimeSpan.TotalMilliseconds);
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, expireTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithXxAndGtOptionAndExistingKeyHasLargerExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var existingExpireTimeSpan = TimeSpan.FromMinutes(10);
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
                db.KeyExpire(key, existingExpireTimeSpan);
            }
            else
            {
                db.StringSet(key, "valueA", existingExpireTimeSpan);
            }
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "gt", "XX");
            ClassicAssert.AreEqual(actualResult, 0);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, expireTimeSpan.TotalMilliseconds);
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, existingExpireTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithXxAndGtOptionAndExistingKeyNoExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
            }
            else
            {
                db.StringSet(key, "valueA");
            }
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "Gt", "xX");
            ClassicAssert.AreEqual(actualResult, 0);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsFalse(actualTtl.HasValue);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithLtOptionAndExistingKeyHasSmallerExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var existingExpireTimeSpan = TimeSpan.FromMinutes(1);
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
                db.KeyExpire(key, existingExpireTimeSpan);
            }
            else
            {
                db.StringSet(key, "valueA", existingExpireTimeSpan);
            }
            var expireTimeSpan = TimeSpan.FromMinutes(10);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "lT");
            ClassicAssert.AreEqual(actualResult, 0);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, existingExpireTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithLtOptionAndExistingKeyHasLargerExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var existingExpireTimeSpan = TimeSpan.FromMinutes(10);
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
                db.KeyExpire(key, existingExpireTimeSpan);
            }
            else
            {
                db.StringSet(key, "valueA", existingExpireTimeSpan);
            }
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "LT");
            ClassicAssert.AreEqual(actualResult, 1);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, expireTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithLtOptionAndExistingKeyNoExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
            }
            else
            {
                db.StringSet(key, "valueA");
            }
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "LT");
            ClassicAssert.AreEqual(actualResult, 1);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, expireTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithXxAndLtOptionAndExistingKeyHasSmallerExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var existingExpireTimeSpan = TimeSpan.FromMinutes(1);
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
                db.KeyExpire(key, existingExpireTimeSpan);
            }
            else
            {
                db.StringSet(key, "valueA", existingExpireTimeSpan);
            }
            var expireTimeSpan = TimeSpan.FromMinutes(10);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "LT", "XX");
            ClassicAssert.AreEqual(actualResult, 0);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, existingExpireTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithXxAndLtOptionAndExistingKeyHasLargerExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            var existingExpireTimeSpan = TimeSpan.FromMinutes(10);
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
                db.KeyExpire(key, existingExpireTimeSpan);
            }
            else
            {
                db.StringSet(key, "valueA", existingExpireTimeSpan);
            }
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "xX", "Lt");
            ClassicAssert.AreEqual(actualResult, 1);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, expireTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase("EXPIREAT", false)]
        [TestCase("EXPIREAT", true)]
        [TestCase("PEXPIREAT", false)]
        [TestCase("PEXPIREAT", true)]
        public void KeyExpireAtWithXxAndLtOptionAndExistingKeyNoExpire(string command, bool isObject)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            if (isObject)
            {
                db.SortedSetAdd(key, [new SortedSetEntry("element", 1.0)]);
            }
            else
            {
                db.StringSet(key, "valueA");
            }
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            var actualResult = (int)db.Execute(command, key, expireTimeUnix, "XX", "LT");
            ClassicAssert.AreEqual(actualResult, 0);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsFalse(actualTtl.HasValue);
        }

        [Test]
        [TestCase("EXPIREAT", "XX", "NX")]
        [TestCase("EXPIREAT", "NX", "GT")]
        [TestCase("EXPIREAT", "LT", "NX")]
        [TestCase("PEXPIREAT", "XX", "NX")]
        [TestCase("PEXPIREAT", "NX", "GT")]
        [TestCase("PEXPIREAT", "LT", "NX")]
        public void KeyExpireAtWithInvalidOptionCombination(string command, string optionA, string optionB)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "key";
            db.StringSet(key, "valueA");
            var expireTimeSpan = TimeSpan.FromMinutes(1);
            var expireTimeUnix = command == "EXPIREAT" ? DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeSeconds() : DateTimeOffset.UtcNow.Add(expireTimeSpan).ToUnixTimeMilliseconds();

            Assert.Throws<RedisServerException>(() => db.Execute(command, key, expireTimeUnix, optionA, optionA));
        }

        #endregion

        [Test]
        public async Task ReAddExpiredKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const string key = "x:expire_trap";

            // Set
            {
                db.KeyDelete(key);
                db.SetAdd(key, "v1");

                ClassicAssert.IsTrue(db.KeyExists(key), $"KeyExists after initial add");
                ClassicAssert.AreEqual("1", db.Execute("EXISTS", key).ToString(), "EXISTS after initial add");
                var actualScan = db.Execute("SCAN", "0");
                ClassicAssert.AreEqual(1, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN after initial ADD");

                db.KeyExpire(key, TimeSpan.FromSeconds(1));
                await Task.Delay(TimeSpan.FromSeconds(2));

                ClassicAssert.IsFalse(db.KeyExists(key), $"KeyExists after expiration");
                ClassicAssert.AreEqual("0", db.Execute("EXISTS", key).ToString(), "EXISTS after ADD expiration");
                actualScan = db.Execute("SCAN", "0");
                ClassicAssert.AreEqual(0, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN after ADD expiration");

                db.SetAdd(key, "v2");

                ClassicAssert.IsTrue(db.KeyExists(key), $"KeyExists after initial re-ADD");
                ClassicAssert.AreEqual("1", db.Execute("EXISTS", key).ToString(), "EXISTS after initial re-ADD");
                actualScan = db.Execute("SCAN", "0");
                ClassicAssert.AreEqual(1, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN after initial re-ADD");
            }
            // List
            {
                db.KeyDelete(key);
                db.ListRightPush(key, "v1");

                ClassicAssert.IsTrue(db.KeyExists(key), $"KeyExists after initial RPUSH");
                ClassicAssert.AreEqual("1", db.Execute("EXISTS", key).ToString(), "EXISTS after initial RPUSH");
                var actualScan = db.Execute("SCAN", "0");
                ClassicAssert.AreEqual(1, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN after initial RPUSH");

                db.KeyExpire(key, TimeSpan.FromSeconds(1));
                await Task.Delay(TimeSpan.FromSeconds(2));

                ClassicAssert.IsFalse(db.KeyExists(key), $"KeyExists after expiration");
                ClassicAssert.AreEqual("0", db.Execute("EXISTS", key).ToString(), "EXISTS after RPUSH expiration");
                actualScan = db.Execute("SCAN", "0");
                ClassicAssert.AreEqual(0, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN after RPUSH expiration");

                db.ListRightPush(key, "v2");

                ClassicAssert.IsTrue(db.KeyExists(key), $"KeyExists after initial re-RPUSH");
                ClassicAssert.AreEqual("1", db.Execute("EXISTS", key).ToString(), "EXISTS after initial re-RPUSH");
                actualScan = db.Execute("SCAN", "0");
                ClassicAssert.AreEqual(1, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN after initial re-RPUSH");
            }
            // Hash
            {
                db.KeyDelete(key);
                db.HashSet(key, "f1", "v1");

                ClassicAssert.IsTrue(db.KeyExists(key), $"KeyExists after initial HSET");
                ClassicAssert.AreEqual("1", db.Execute("EXISTS", key).ToString(), "EXISTS after initial HSET");
                var actualScan = db.Execute("SCAN", "0");
                ClassicAssert.AreEqual(1, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN after initial HSET");

                db.KeyExpire(key, TimeSpan.FromSeconds(1));
                await Task.Delay(TimeSpan.FromSeconds(2));

                ClassicAssert.IsFalse(db.KeyExists(key), $"KeyExists after expiration");
                ClassicAssert.AreEqual("0", db.Execute("EXISTS", key).ToString(), "EXISTS after HSET expiration");
                actualScan = db.Execute("SCAN", "0");
                ClassicAssert.AreEqual(0, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN after HSET expiration");

                db.HashSet(key, "f1", "v2");

                ClassicAssert.IsTrue(db.KeyExists(key), $"KeyExists after initial re-HSET");
                ClassicAssert.AreEqual("1", db.Execute("EXISTS", key).ToString(), "EXISTS after initial re-HSET");
                actualScan = db.Execute("SCAN", "0");
                ClassicAssert.AreEqual(1, ((RedisValue[])((RedisResult[])actualScan!)[1]).Length, "SCAN after initial re-HSET");
            }
        }

        [Test]
        public void MainObjectKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var server = redis.GetServers()[0];
            var db = redis.GetDatabase(0);

            const string key = "test:1";

            // Do StringSet
            ClassicAssert.IsTrue(db.StringSet(key, "v1"));

            // Do SetAdd using the same key, expected error
            Assert.Throws<RedisServerException>(() => db.SetAdd(key, "v2"),
                Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE));

            // One key "test:1" with a string value is expected
            var keys = server.Keys(db.Database, key).ToList();
            ClassicAssert.AreEqual(1, keys.Count);
            ClassicAssert.AreEqual(key, (string)keys[0]);
            var value = db.StringGet(key);
            ClassicAssert.AreEqual("v1", (string)value);

            // do ListRightPush using the same key, expected error
            Assert.Throws<RedisServerException>(() => db.ListRightPush(key, "v3"), Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE));

            // Delete the key
            ClassicAssert.IsTrue(db.KeyDelete(key));

            // Do SetAdd using the same key
            ClassicAssert.IsTrue(db.SetAdd(key, "v2"));

            // Do StringIncrement using the same key, expected error
            //Assert.Throws<RedisServerException>(() => db.StringIncrement(key), Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE));

            // One key "test:1" with a set value is expected
            keys = server.Keys(db.Database, key).ToList();
            ClassicAssert.AreEqual(1, keys.Count);
            ClassicAssert.AreEqual(key, (string)keys[0]);
            var members = db.SetMembers(key);
            ClassicAssert.AreEqual(1, members.Length);
            ClassicAssert.AreEqual("v2", (string)members[0]);
        }

        [Test]
        public void GetSliceTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "rangeKey";
            string value = "0123456789";

            var resp = (string)db.StringGetRange(key, 2, 10);
            ClassicAssert.AreEqual(string.Empty, resp);
            ClassicAssert.AreEqual(true, db.StringSet(key, value));

            //0,0
            resp = (string)db.StringGetRange(key, 0, 0);
            ClassicAssert.AreEqual("0", resp);

            //actual value
            resp = (string)db.StringGetRange(key, 0, -1);
            ClassicAssert.AreEqual(value, resp);

            #region testA
            //s[2,len] s < e & e = len
            resp = (string)db.StringGetRange(key, 2, 10);
            ClassicAssert.AreEqual(value.Substring(2), resp);

            //s[2,len] s < e & e = len - 1
            resp = (string)db.StringGetRange(key, 2, 9);
            ClassicAssert.AreEqual(value.Substring(2), resp);

            //s[2,len] s < e < len
            resp = (string)db.StringGetRange(key, 2, 5);
            ClassicAssert.AreEqual(value.Substring(2, 4), resp);

            //s[2,len] s < len < e
            resp = (string)db.StringGetRange(key, 2, 15);
            ClassicAssert.AreEqual(value.Substring(2), resp);

            //s[4,len] e < s < len
            resp = (string)db.StringGetRange(key, 4, 2);
            ClassicAssert.AreEqual("", resp);

            //s[4,len] e < 0 < s < len
            resp = (string)db.StringGetRange(key, 4, -2);
            ClassicAssert.AreEqual(value.Substring(4, 5), resp);

            //s[4,len] e < -len < 0 < s < len
            resp = (string)db.StringGetRange(key, 4, -12);
            ClassicAssert.AreEqual("", resp);
            #endregion

            #region testB
            //-len < s < 0 < len < e
            resp = (string)db.StringGetRange(key, -4, 15);
            ClassicAssert.AreEqual(value.Substring(6, 4), resp);

            //-len < s < 0 < e < len where len + s > e
            resp = (string)db.StringGetRange(key, -4, 5);
            ClassicAssert.AreEqual("", resp);

            //-len < s < 0 < e < len where len + s < e
            resp = (string)db.StringGetRange(key, -4, 8);
            ClassicAssert.AreEqual(value.Substring(value.Length - 4, 2), resp);

            //-len < s < e < 0
            resp = (string)db.StringGetRange(key, -4, -1);
            ClassicAssert.AreEqual(value.Substring(value.Length - 4, 4), resp);

            //-len < e < s < 0
            resp = (string)db.StringGetRange(key, -4, -7);
            ClassicAssert.AreEqual("", resp);
            #endregion

            //range start > end > len
            resp = (string)db.StringGetRange(key, 17, 13);
            ClassicAssert.AreEqual("", resp);

            //range 0 > start > end
            resp = (string)db.StringGetRange(key, -1, -4);
            ClassicAssert.AreEqual("", resp);

            //equal offsets
            resp = db.StringGetRange(key, 4, 4);
            ClassicAssert.AreEqual("4", resp);

            //equal offsets
            resp = db.StringGetRange(key, -4, -4);
            ClassicAssert.AreEqual("6", resp);

            //equal offsets
            resp = db.StringGetRange(key, -100, -100);
            ClassicAssert.AreEqual("0", resp);

            //equal offsets
            resp = db.StringGetRange(key, -101, -101);
            ClassicAssert.AreEqual("9", resp);

            //start larger than end
            resp = db.StringGetRange(key, -1, -3);
            ClassicAssert.AreEqual("", resp);

            //2,-1 -> 2 9
            var negend = -1;
            resp = db.StringGetRange(key, 2, negend);
            ClassicAssert.AreEqual(value.Substring(2, 8), resp);

            //2,-3 -> 2 7
            negend = -3;
            resp = db.StringGetRange(key, 2, negend);
            ClassicAssert.AreEqual(value.Substring(2, 6), resp);

            //-5,-3 -> 5,7
            var negstart = -5;
            resp = db.StringGetRange(key, negstart, negend);
            ClassicAssert.AreEqual(value.Substring(5, 3), resp);
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
            ClassicAssert.AreEqual("10", resp);
            resp = db.StringGet(key);
            ClassicAssert.AreEqual("0123456789", resp);
            ClassicAssert.IsTrue(db.KeyDelete(key));

            // new key, length 10, offset 5 -> 15 ("\0\0\0\0\00123456789")
            resp = db.StringSetRange(key, 5, value);
            ClassicAssert.AreEqual("15", resp);
            resp = db.StringGet(key);
            ClassicAssert.AreEqual("\0\0\0\0\00123456789", resp);
            ClassicAssert.IsTrue(db.KeyDelete(key));

            // new key, length 10, offset -1 -> RedisServerException ("ERR offset is out of range")
            var ex = Assert.Throws<RedisServerException>(() => db.StringSetRange(key, -1, value));
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_OFFSETOUTOFRANGE), ex.Message);

            // new key, length 10, offset invalid_offset -> RedisServerException ("ERR value is not an integer or out of range.")
            ex = Assert.Throws<RedisServerException>(() => db.Execute(nameof(RespCommand.SETRANGE), key, "invalid_offset", value));
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER), ex.Message);

            // existing key, length 10, offset 0, value length 5 -> 10 ("ABCDE56789")
            ClassicAssert.IsTrue(db.StringSet(key, value));
            resp = db.StringSetRange(key, 0, newValue);
            ClassicAssert.AreEqual("10", resp);
            resp = db.StringGet(key);
            ClassicAssert.AreEqual("ABCDE56789", resp);
            ClassicAssert.IsTrue(db.KeyDelete(key));

            // existing key, length 10, offset 5, value length 5 -> 10 ("01234ABCDE")
            ClassicAssert.IsTrue(db.StringSet(key, value));
            resp = db.StringSetRange(key, 5, newValue);
            ClassicAssert.AreEqual("10", resp);
            resp = db.StringGet(key);
            ClassicAssert.AreEqual("01234ABCDE", resp);
            ClassicAssert.IsTrue(db.KeyDelete(key));

            // existing key, length 10, offset 10, value length 5 -> 15 ("0123456789ABCDE")
            ClassicAssert.IsTrue(db.StringSet(key, value));
            resp = db.StringSetRange(key, 10, newValue);
            ClassicAssert.AreEqual("15", resp);
            resp = db.StringGet(key);
            ClassicAssert.AreEqual("0123456789ABCDE", resp);
            ClassicAssert.IsTrue(db.KeyDelete(key));

            // existing key, length 10, offset 15, value length 5 -> 20 ("0123456789\0\0\0\0\0ABCDE")
            ClassicAssert.IsTrue(db.StringSet(key, value));
            resp = db.StringSetRange(key, 15, newValue);
            ClassicAssert.AreEqual("20", resp);
            resp = db.StringGet(key);
            ClassicAssert.AreEqual("0123456789\0\0\0\0\0ABCDE", resp);
            ClassicAssert.IsTrue(db.KeyDelete(key));

            // existing key, length 10, offset -1, value length 5 -> RedisServerException ("ERR offset is out of range")
            ClassicAssert.IsTrue(db.StringSet(key, value));
            ex = Assert.Throws<RedisServerException>(() => db.StringSetRange(key, -1, newValue));
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_OFFSETOUTOFRANGE), ex.Message);
        }

        [Test]
        public void PingTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string result = (string)db.Execute("PING");
            ClassicAssert.AreEqual("PONG", result);
        }

        [Test]
        public void AskingTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string result = (string)db.Execute("ASKING");
            ClassicAssert.AreEqual("OK", result);
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
            ClassicAssert.IsTrue(time.Value.Ticks > 0);

            db.StringSet(keyB, keyB, keepTtl: false);
            time = db.KeyTimeToLive(keyB);
            ClassicAssert.IsTrue(time == null);

            Thread.Sleep(expire * 1000 + 100);

            string value = db.StringGet(keyA);
            ClassicAssert.AreEqual(null, value);

            ClassicAssert.AreEqual("OK", db.Execute("SET", keyA, keyA, "KEEPTTL")?.ToString());

            value = db.StringGet(keyB);
            ClassicAssert.AreEqual(keyB, value);
        }

        [Test]
        public void StrlenTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            ClassicAssert.IsTrue(db.StringSet("mykey", "foo bar"));
            ClassicAssert.AreEqual(7, db.StringLength("mykey"));
            ClassicAssert.AreEqual(0, db.StringLength("nokey"));
        }

        [Test]
        public void TTLTestMilliseconds()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            var val = "myKeyValue";
            var expireTimeInMilliseconds = 3000;

            var pttl = db.Execute("PTTL", key);
            ClassicAssert.AreEqual(-2, (int)pttl);

            db.StringSet(key, val);
            pttl = db.Execute("PTTL", key);
            ClassicAssert.AreEqual(-1, (int)pttl);

            db.KeyExpire(key, TimeSpan.FromMilliseconds(expireTimeInMilliseconds));

            //check TTL of the key in milliseconds
            pttl = db.Execute("PTTL", key);

            ClassicAssert.IsTrue(long.TryParse(pttl.ToString(), out var pttlInMs));
            ClassicAssert.IsTrue(pttlInMs > 0);

            db.KeyPersist(key);
            Thread.Sleep(expireTimeInMilliseconds);

            var _val = db.StringGet(key);
            ClassicAssert.AreEqual(val, _val.ToString());

            var ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsNull(ttl);
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
            ClassicAssert.AreEqual(val, retval.ToString());

            retval = db.StringGetDelete(key);
            ClassicAssert.AreEqual(val, retval.ToString());

            // Try retrieving already deleted key
            retval = db.StringGetDelete(key);
            ClassicAssert.AreEqual(string.Empty, retval.ToString());

            // Try retrieving & deleting non-existent key
            retval = db.StringGetDelete("nonExistentKey");
            ClassicAssert.AreEqual(string.Empty, retval.ToString());

            // Key setup with metadata
            key = "myKeyWithMetadata";
            val = "myValueWithMetadata";
            db.StringSet(key, val, expiry: TimeSpan.FromSeconds(10000));
            retval = db.StringGet(key);
            ClassicAssert.AreEqual(val, retval.ToString());

            retval = db.StringGetDelete(key);
            ClassicAssert.AreEqual(val, retval.ToString());

            // Try retrieving already deleted key with metadata
            retval = db.StringGetDelete(key);
            ClassicAssert.AreEqual(string.Empty, retval.ToString());
        }

        [Test]
        public void GetRole()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var server = redis.GetServers()[0];
            var role = server.Role();
            ClassicAssert.True(role is Role.Master);
            var master = role as Role.Master;
            ClassicAssert.AreEqual("master", master.Value);
        }

        [Test]
        public void AppendTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            var val = "myKeyValue";
            var val2 = "myKeyValue2";

            _ = db.StringSet(key, val);
            var len = db.StringAppend(key, val2);
            ClassicAssert.AreEqual(val.Length + val2.Length, len);

            var _val = db.StringGet(key);
            ClassicAssert.AreEqual(val + val2, _val.ToString());

            // Test appending an empty string
            _ = db.StringSet(key, val);
            var len1 = db.StringAppend(key, "");
            ClassicAssert.AreEqual(val.Length, len1);

            _val = db.StringGet(key);
            ClassicAssert.AreEqual(val, _val.ToString());

            // Test appending to a non-existent key
            var nonExistentKey = "nonExistentKey";
            var len2 = db.StringAppend(nonExistentKey, val2);
            ClassicAssert.AreEqual(val2.Length, len2);

            _val = db.StringGet(nonExistentKey);
            ClassicAssert.AreEqual(val2, _val.ToString());
        }

        [Test]
        public void AppendLargeStringValueTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key1 = "myKey1";
            var key2 = "myKey2";
            var val2 = "myKeyValue2";

            var largeVal = new string('a', 1000000);

            // Test appending to a key with a large value
            _ = db.StringSet(key1, largeVal);
            var len = db.StringAppend(key1, val2);
            ClassicAssert.AreEqual(largeVal.Length + val2.Length, len);

            // Test appending a large value to a key
            _ = db.StringSet(key2, val2);
            var len2 = db.StringAppend(key2, largeVal);
            ClassicAssert.AreEqual(largeVal.Length + val2.Length, len);
        }

        [Test]
        public void AppendWithExpirationTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "keyWithExpiration";
            var val = "myKeyValue";
            var val2 = "myKeyValue2";

            // Test appending to a key with expiration
            _ = db.StringSet(key, val, TimeSpan.FromSeconds(10000));
            var len = db.StringAppend(key, val2);
            ClassicAssert.AreEqual(val.Length + val2.Length, len);

            var _val = db.StringGet(key);
            ClassicAssert.AreEqual(val + val2, _val.ToString());

            var time = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(time.Value.TotalSeconds > 0);
        }

        [Test]
        public void HelloTest1()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Test "HELLO 2"
            var result = db.Execute("HELLO", "2");

            ClassicAssert.IsNotNull(result);
            ClassicAssert.AreEqual(ResultType.Array, result.Resp2Type);
            ClassicAssert.AreEqual(ResultType.Array, result.Resp3Type);
            var resultDict = result.ToDictionary();
            ClassicAssert.IsNotNull(resultDict);
            ClassicAssert.AreEqual(2, (int)resultDict["proto"]);
            ClassicAssert.AreEqual("master", (string)resultDict["role"]);

            // Test "HELLO 3"
            result = db.Execute("HELLO", "3");

            ClassicAssert.IsNotNull(result);
            ClassicAssert.AreEqual(ResultType.Array, result.Resp2Type);
            ClassicAssert.AreEqual(ResultType.Map, result.Resp3Type);
            resultDict = result.ToDictionary();
            ClassicAssert.IsNotNull(resultDict);
            ClassicAssert.AreEqual(3, (int)resultDict["proto"]);
            ClassicAssert.AreEqual("master", (string)resultDict["role"]);
        }

        [Test]
        public void HelloAuthErrorTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: RedisProtocol.Resp2));
            var db = redis.GetDatabase(0);

            // Failed HELLO should not change current protocol
            Assert.Throws<RedisServerException>(() => db.Execute("HELLO", "3", "AUTH", "NX", "NX"),
                           Encoding.ASCII.GetString(CmdStrings.RESP_WRONGPASS_INVALID_USERNAME_PASSWORD));
            var result = db.Execute("HELLO");

            ClassicAssert.IsNotNull(result);
            ClassicAssert.AreEqual(ResultType.Array, result.Resp2Type);
            ClassicAssert.AreEqual(ResultType.Array, result.Resp3Type);
            var resultDict = result.ToDictionary();
            ClassicAssert.IsNotNull(resultDict);

            // Should remain 2 since protocol change failed
            ClassicAssert.AreEqual(2, (int)resultDict["proto"]);
        }

        [Test]
        [TestCase([2, "$-1\r\n", "$1\r\n", "*4", '*'], Description = "RESP2 output")]
        [TestCase([3, "_\r\n", ",", "%2", '~'], Description = "RESP3 output")]
        public async Task RespOutputTests(byte respVersion, string expectedResponse, string doublePrefix, string mapPrefix, char setPrefix)
        {
            using var c = TestUtils.GetGarnetClientSession(raw: true);
            c.Connect();

            var response = await c.ExecuteAsync("HELLO", respVersion.ToString());

            response = await c.ExecuteAsync("GET", "nx");
            ClassicAssert.AreEqual(expectedResponse, response);
            response = await c.ExecuteAsync("GEODIST", "nx", "foo", "bar");
            ClassicAssert.AreEqual(expectedResponse, response);
            response = await c.ExecuteAsync("HGET", "nx", "nx");
            ClassicAssert.AreEqual(expectedResponse, response);
            response = await c.ExecuteAsync("LPOP", "nx");
            ClassicAssert.AreEqual(expectedResponse, response);
            response = await c.ExecuteAsync("LPOS", "nx", "foo");
            ClassicAssert.AreEqual(expectedResponse, response);
            response = await c.ExecuteAsync("SPOP", "nx");
            ClassicAssert.AreEqual(expectedResponse, response);
            response = await c.ExecuteAsync("ZSCORE", "nx", "foo");
            ClassicAssert.AreEqual(expectedResponse, response);

            response = await c.ExecuteAsync("BITFIELD", "bf", "OVERFLOW", "FAIL", "INCRBY", "u1", "1", "1");
            ClassicAssert.AreEqual("*1\r\n:1\r\n", response);
            response = await c.ExecuteAsync("BITFIELD", "bf", "OVERFLOW", "FAIL", "INCRBY", "u1", "1", "1");
            ClassicAssert.AreEqual("*1\r\n" + expectedResponse, response);

            response = await c.ExecuteAsync("SADD", "set", "foo", "bar");
            ClassicAssert.AreEqual(":2\r\n", response);
            response = await c.ExecuteAsync("SMEMBERS", "set");
            ClassicAssert.AreEqual(setPrefix + "2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", response);

            response = await c.ExecuteAsync("MSET", "s1", "foo", "s2", "bar");
            ClassicAssert.AreEqual("+OK\r\n", response);
            response = await c.ExecuteAsync("LCS", "s1", "s2", "IDX");
            ClassicAssert.AreEqual(mapPrefix + "\r\n$7\r\nmatches\r\n*0\r\n$3\r\nlen\r\n:0\r\n", response);

            response = await c.ExecuteAsync("ZADD", "z", "0", "a", "1", "b");
            ClassicAssert.AreEqual(":2\r\n", response);
            response = await c.ExecuteAsync("ZSCORE", "z", "a");
            ClassicAssert.AreEqual(doublePrefix + "0\r\n", response);
        }

        [Test]
        public void AsyncTest1()
        {
            // Set up low-memory database
            TearDown();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, disableObjects: true);
            server.Start();

            string firstKey = null, firstValue = null, lastKey = null, lastValue = null;

            // Load the data so that it spills to disk
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                var keyCount = 5;
                var valLen = 256;
                var keyLen = 8;

                List<Tuple<string, string>> data = [];
                for (var i = 0; i < keyCount; i++)
                {
                    lastKey = TestUtils.GetRandomString(keyLen);
                    lastValue = TestUtils.GetRandomString(valLen);
                    if (firstKey == null)
                    {
                        firstKey = lastKey;
                        firstValue = lastValue;
                    }
                    data.Add(new Tuple<string, string>(lastKey, lastValue));
                    var pair = data.Last();
                    db.StringSet(pair.Item1, pair.Item2);
                }
            }

            // We use newline counting for HELLO response as the exact length can vary slightly across versions
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Newlines);

            var expectedNewlineCount = 30; // 30 '\n' characters expected in response
            var response = lightClientRequest.Execute($"hello 3", expectedNewlineCount);
            ClassicAssert.IsTrue(response.Length is > 175 and < 190);

            // Switch to byte counting in response
            lightClientRequest.countResponseType = CountResponseType.Bytes;

            // Turn on async
            var expectedResponse = "+OK\r\n";
            response = lightClientRequest.Execute($"async on", expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, response);

            // Get in-memory data item
            expectedResponse = $"${lastValue.Length}\r\n{lastValue}\r\n";
            response = lightClientRequest.Execute($"GET {lastKey}", expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, response);

            // Get disk data item with async on
            expectedResponse = $"-ASYNC 0\r\n>3\r\n$5\r\nasync\r\n$1\r\n0\r\n${firstValue.Length}\r\n{firstValue}\r\n";
            response = lightClientRequest.Execute($"GET {firstKey}", expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, response);

            // Issue barrier command for async
            expectedResponse = "+OK\r\n";
            response = lightClientRequest.Execute($"async barrier", expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, response);

            // Turn off async
            expectedResponse = "+OK\r\n";
            response = lightClientRequest.Execute($"async off", expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, response);

            // Get disk data item with async off
            expectedResponse = $"${firstValue.Length}\r\n{firstValue}\r\n";
            response = lightClientRequest.Execute($"GET {firstKey}", expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, response);
        }

        [Test]
        public void ClientIdTest()
        {
            long id1;
            {
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                var db = redis.GetDatabase(0);

                var result = db.Execute("CLIENT", "ID");

                ClassicAssert.IsNotNull(result);
                ClassicAssert.AreEqual(ResultType.Integer, result.Resp2Type);

                id1 = (long)result;

                ClassicAssert.IsTrue(id1 > 0, "Client ids must be > 0");
            }

            long id2;
            {
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                var db = redis.GetDatabase(0);

                var result = db.Execute("CLIENT", "ID");

                ClassicAssert.IsNotNull(result);
                ClassicAssert.AreEqual(ResultType.Integer, result.Resp2Type);

                id2 = (long)result;

                ClassicAssert.IsTrue(id2 > 0, "Client ids must be > 0");
            }

            ClassicAssert.AreNotEqual(id1, id2, "CLIENT IDs must be unique");
            ClassicAssert.IsTrue(id2 > id1, "CLIENT IDs should be monotonic");

            {
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                var db = redis.GetDatabase(0);

                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("CLIENT", "ID", "foo"));
                ClassicAssert.AreEqual("ERR wrong number of arguments for 'client|id' command", exc.Message);
            }
        }

        [Test]
        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void ClientInfoTest(RedisProtocol protocol)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
            var db = redis.GetDatabase(0);

            var result = (string)db.Execute("CLIENT", "INFO");
            AssertExpectedClientFields(result, protocol);

            var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("CLIENT", "INFO", "foo"));
            ClassicAssert.AreEqual("ERR wrong number of arguments for 'client|info' command", exc.Message);
        }

        [Test]
        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void ClientListTest(RedisProtocol protocol)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
            var db = redis.GetDatabase(0);

            // List everything
            {
                var list = (string)db.Execute("CLIENT", "LIST");
                AssertExpectedClientFields(list, protocol);
            }

            // List by id
            {
                var id = (long)db.Execute("CLIENT", "ID");

                var list = (string)db.Execute("CLIENT", "LIST", "ID", id, 123);
                AssertExpectedClientFields(list, protocol);
            }

            // List by type
            {
                var list = (string)db.Execute("CLIENT", "LIST", "TYPE", "NORMAL");
                AssertExpectedClientFields(list, protocol);
            }
        }

        [Test]
        public void ClientListErrorTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Bad option
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("CLIENT", "LIST", "foo"));
                ClassicAssert.AreEqual("ERR syntax error", exc.Message);
            }

            // Missing type
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("CLIENT", "LIST", "TYPE"));
                ClassicAssert.AreEqual("ERR syntax error", exc.Message);
            }

            // Bad type
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("CLIENT", "LIST", "TYPE", "foo"));
                ClassicAssert.AreEqual("ERR Unknown client type 'foo'", exc.Message);
            }

            // Invalid type
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("CLIENT", "LIST", "TYPE", "Invalid"));
                ClassicAssert.AreEqual("ERR Unknown client type 'Invalid'", exc.Message);
            }

            // Numeric type
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("CLIENT", "LIST", "TYPE", "1"));
                ClassicAssert.AreEqual("ERR Unknown client type '1'", exc.Message);
            }

            // Missing id
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("CLIENT", "LIST", "ID"));
                ClassicAssert.AreEqual("ERR syntax error", exc.Message);
            }

            // Bad id
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("CLIENT", "LIST", "ID", "abc"));
                ClassicAssert.AreEqual("ERR Invalid client ID", exc.Message);
            }

            // Combo - Redis docs sort of imply this is supported, but that is not the case in testing
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("CLIENT", "LIST", "TYPE", "NORMAL", "ID", "1"));
                ClassicAssert.AreEqual("ERR syntax error", exc.Message);
            }
        }

        [Test]
        public async Task ClientKillTestAsync()
        {
            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var mainDB = mainConnection.GetDatabase(0);
            var mainId = (long)mainDB.Execute("CLIENT", "ID");

            // Kill old style (by remote endpoint)
            {
                using var targetConnection = await ConnectAsync();

                var targetId = await targetConnection.ExecuteForLongResultAsync("CLIENT", ["ID"]);

                var remoteEndpoint = GetFlagForSessionId(targetId, "addr", mainDB);

                // Kill acknowledged
                var res = mainDB.Execute("CLIENT", "KILL", remoteEndpoint);
                ClassicAssert.AreEqual("OK", (string)res);

                AssertNotConnected(targetConnection);
            }

            // Kill by id
            {
                using var targetConnection = await ConnectAsync();

                var targetId = await targetConnection.ExecuteForLongResultAsync("CLIENT", ["ID"]);

                // Count of killed connections
                var res = mainDB.Execute("CLIENT", "KILL", "ID", targetId);
                ClassicAssert.AreEqual(1, (int)res);

                AssertNotConnected(targetConnection);
            }

            // Kill by type = NORMAL
            {
                using var targetConnection = await ConnectAsync();

                // Count of killed connections
                var res = mainDB.Execute("CLIENT", "KILL", "TYPE", "NORMAL");

                // SE.Redis spins up multiple connections, so we can kill more than 1 (but at least 1) connection
                ClassicAssert.IsTrue((int)res >= 1);

                AssertNotConnected(targetConnection);
            }

            // Kill by type = PUBSUB
            {
                using var targetConnection = await ConnectAsync();

                _ = await targetConnection.ExecuteForStringResultAsync("SUBSCRIBE", ["foo"]);

                // Count of killed connections
                var res = mainDB.Execute("CLIENT", "KILL", "TYPE", "PUBSUB");

                // SE.Redis spins up multiple connections, so we can kill more than 1 (but at least 1) connection
                ClassicAssert.IsTrue((int)res >= 1);

                AssertNotConnected(targetConnection);
            }

            // KILL by addr
            {
                using var targetConnection = await ConnectAsync();

                var targetId = await targetConnection.ExecuteForLongResultAsync("CLIENT", ["ID"]);

                var remoteEndpoint = GetFlagForSessionId(targetId, "addr", mainDB);

                // Count of killed connections
                var res = mainDB.Execute("CLIENT", "KILL", "ADDR", remoteEndpoint);
                ClassicAssert.AreEqual(1, (int)res);

                AssertNotConnected(targetConnection);
            }

            // KILL by laddr
            {
                using var targetConnection = await ConnectAsync();

                var targetId = await targetConnection.ExecuteForLongResultAsync("CLIENT", ["ID"]);

                var localEndpoint = GetFlagForSessionId(targetId, "laddr", mainDB);

                // Count of killed connections
                var res = mainDB.Execute("CLIENT", "KILL", "LADDR", localEndpoint);

                // SE.Redis spins up multiple connections, so we can kill more than 1 (but at least 1) connection
                ClassicAssert.IsTrue((int)res >= 1);

                AssertNotConnected(targetConnection);
            }

            // KILL by maxage
            {
                using var targetConnection = await ConnectAsync();

                var targetId = await targetConnection.ExecuteForLongResultAsync("CLIENT", ["ID"]);

                while (true)
                {
                    var age = GetFlagForSessionId(targetId, "age", mainDB);
                    if (long.Parse(age) >= 2)
                    {
                        break;
                    }

                    await Task.Delay(1_000);
                }

                // Count of killed connections
                var res = mainDB.Execute("CLIENT", "KILL", "MAXAGE", 1);
                ClassicAssert.IsTrue((int)res >= 1);

                AssertNotConnected(targetConnection);
            }

            // KILL by multiple
            {
                using var targetConnection = await ConnectAsync();

                var targetId = await targetConnection.ExecuteForLongResultAsync("CLIENT", ["ID"]);

                var addr = GetFlagForSessionId(targetId, "addr", mainDB);

                // Count of killed connections
                var res = mainDB.Execute("CLIENT", "KILL", "ID", targetId, "MAXAGE", -1, "TYPE", "NORMAL", "SKIPME", "YES");
                ClassicAssert.AreEqual(1, (int)res);

                AssertNotConnected(targetConnection);
            }

            // KILL without SKIPME
            {
                using var targetConnection = await ConnectAsync();

                var targetId = await targetConnection.ExecuteForLongResultAsync("CLIENT", ["ID"]);

                try
                {
                    _ = mainDB.Execute("CLIENT", "KILL", "TYPE", "NORMAL", "SKIPME", "NO");
                }
                catch
                {
                    // This will kill the SE.Redis connection, so depending on ordering an exception may be observed
                }

                AssertNotConnected(targetConnection);
            }

            // Grab a flag=value out of CLIENT LIST
            static string GetFlagForSessionId(long sessionId, string flag, IDatabase db)
            {
                var list = (string)db.Execute("CLIENT", "LIST");
                var line = list.Split("\n").Single(l => l.Contains($"id={sessionId} "));

                string flagValue = null;

                foreach (var flagPair in line.Split(" "))
                {
                    if (flagPair.StartsWith($"{flag}="))
                    {
                        ClassicAssert.IsNull(flagValue, $"In {line}, found duplicate {flag}");

                        flagValue = flagPair[$"{flag}=".Length..];
                    }
                }

                ClassicAssert.NotNull(flagValue, $"In {line}, looking for {flag}");

                return flagValue;
            }

            // Create a GarnetClient that we'll try to kill later
            static async Task<GarnetClient> ConnectAsync()
            {
                var client = TestUtils.GetGarnetClient();
                await client.ConnectAsync();

                _ = await client.PingAsync();

                ClassicAssert.IsTrue(client.IsConnected);

                return client;
            }

            // Check that we really killed the connection backing a GarnetClient
            static void AssertNotConnected(GarnetClient client)
            {
                // Force the issue by attempting a command
                try
                {
                    client.Ping(static (_, __) => { });
                }
                catch { }

                ClassicAssert.IsFalse(client.IsConnected);
            }
        }

        [Test]
        public void ClientKillErrors()
        {
            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var mainDB = mainConnection.GetDatabase(0);

            // Errors that match Redis behavior
            {
                // No args
                {
                    var exc = ClassicAssert.Throws<RedisServerException>(() => mainDB.Execute("CLIENT", "KILL"));
                    ClassicAssert.AreEqual("ERR wrong number of arguments for 'CLIENT|KILL' command", exc.Message);
                }

                // Old style, not a known client
                {
                    var exc = ClassicAssert.Throws<RedisServerException>(() => mainDB.Execute("CLIENT", "KILL", "foo"));
                    ClassicAssert.AreEqual("ERR No such client", exc.Message);
                }

                // New style, bad filter
                {
                    var exc = ClassicAssert.Throws<RedisServerException>(() => mainDB.Execute("CLIENT", "KILL", "FOO", "bar"));
                    ClassicAssert.AreEqual("ERR syntax error", exc.Message);
                }

                // New style, ID not number
                {
                    var exc = ClassicAssert.Throws<RedisServerException>(() => mainDB.Execute("CLIENT", "KILL", "ID", "fizz"));
                    ClassicAssert.AreEqual("ERR client-id should be greater than 0", exc.Message);
                }

                // New style, TYPE invalid
                {
                    var exc = ClassicAssert.Throws<RedisServerException>(() => mainDB.Execute("CLIENT", "KILL", "TYPE", "buzz"));
                    ClassicAssert.AreEqual("ERR Unknown client type 'buzz'", exc.Message);
                }

                // New style, SKIPME invalid
                {
                    var exc = ClassicAssert.Throws<RedisServerException>(() => mainDB.Execute("CLIENT", "KILL", "SKIPME", "hello"));
                    ClassicAssert.AreEqual("ERR syntax error", exc.Message);
                }

                // New style, MAXAGE invalid
                {
                    var exc = ClassicAssert.Throws<RedisServerException>(() => mainDB.Execute("CLIENT", "KILL", "MAXAGE", "world"));
                    ClassicAssert.AreEqual("ERR syntax error", exc.Message);
                }
            }

            // Because Redis behavior seems to diverge from its documentation, Garnet fails safe in these cases
            {
                AssertDuplicateDefinitionError("ID", () => mainDB.Execute("CLIENT", "KILL", "ID", "123", "ID", "456"));
                AssertDuplicateDefinitionError("TYPE", () => mainDB.Execute("CLIENT", "KILL", "TYPE", "master", "TYPE", "normal"));
                AssertDuplicateDefinitionError("USER", () => mainDB.Execute("CLIENT", "KILL", "USER", "foo", "USER", "bar"));
                AssertDuplicateDefinitionError("ADDR", () => mainDB.Execute("CLIENT", "KILL", "ADDR", "123", "ADDR", "456"));
                AssertDuplicateDefinitionError("LADDR", () => mainDB.Execute("CLIENT", "KILL", "LADDR", "123", "LADDR", "456"));
                AssertDuplicateDefinitionError("SKIPME", () => mainDB.Execute("CLIENT", "KILL", "SKIPME", "YES", "SKIPME", "NO"));
                AssertDuplicateDefinitionError("MAXAGE", () => mainDB.Execute("CLIENT", "KILL", "MAXAGE", "123", "MAXAGE", "456"));
            }

            static void AssertDuplicateDefinitionError(string filter, TestDelegate shouldThrow)
            {
                var exc = ClassicAssert.Throws<RedisServerException>(shouldThrow);
                ClassicAssert.AreEqual($"ERR Filter '{filter}' defined multiple times", exc.Message);
            }
        }

        [Test]
        [Description("Basic case - Get name with default name")]
        public void ClientGetNameBasicTest()
        {
            var config = TestUtils.GetConfig();
            using var redis = ConnectionMultiplexer.Connect(config);
            var db = redis.GetDatabase(0);

            var result = (string)db.Execute("CLIENT", "GETNAME");
            ClassicAssert.AreEqual(config.ClientName, result);
        }

        [Test]
        [Description("Get name after setting a name")]
        public void ClientGetNameAfterSetNameTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("CLIENT", "SETNAME", "testname");
            var result = (string)db.Execute("CLIENT", "GETNAME");
            ClassicAssert.AreEqual("testname", result);

            // Test clearing client name
            db.Execute("CLIENT", "SETNAME", "");
            result = (string)db.Execute("CLIENT", "GETNAME");
            ClassicAssert.AreEqual(null, result);
        }

        [Test]
        [TestCase(false, "validname", true, "validname", Description = "Set valid name")]
        [TestCase(true, "validname", true, "validname", Description = "Set valid name")]
        [TestCase(false, "", true, Description = "Set empty name")]
        [TestCase(true, "", true, Description = "Set empty name")]
        [TestCase(false, "name with spaces", false, Description = "Set name with spaces")]
        [TestCase(true, "name with spaces", false, Description = "Set name with spaces")]
        public void ClientSetNameTest(bool hello, string name, bool shouldSucceed, string expectedName = null)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            if (shouldSucceed)
            {
                if (!hello)
                {
                    var result = (string)db.Execute("CLIENT", "SETNAME", name);
                    ClassicAssert.AreEqual("OK", result);
                }
                else
                {
                    db.Execute("HELLO", "2", "SETNAME", name);
                }

                var getName = (string)db.Execute("CLIENT", "GETNAME");
                ClassicAssert.AreEqual(expectedName, getName);
            }
            else
            {
                Assert.Throws<RedisServerException>(() => db.Execute("CLIENT", "SETNAME", name));
            }
        }

        [Test]
        [TestCase("LIB-NAME", "mylib", Description = "Set only library name")]
        [TestCase("LIB-VER", "1.0.0", Description = "Set only library version")]
        public void ClientSetInfoSingleOptionTest(string option, string value)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var result = (string)db.Execute("CLIENT", "SETINFO", option, value);
            ClassicAssert.AreEqual("OK", result);

            var actual = ((string)db.Execute("CLIENT", "INFO")).Split(" ").First(x => x.StartsWith(option, StringComparison.OrdinalIgnoreCase)).Substring(option.Length + 1).TrimEnd('\n');
            ClassicAssert.AreEqual(value, actual);
        }

        [Test]
        [Description("Test invalid SETINFO options")]
        public void ClientSetInfoInvalidOptionsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            Assert.Throws<RedisServerException>(() => db.Execute("CLIENT", "SETINFO", "INVALID-OPTION", "value"));
        }

        /// <summary>
        /// Check that list is non-empty, and has the minimum required fields.
        /// </summary>
        private static void AssertExpectedClientFields(string list, RedisProtocol protocol = RedisProtocol.Resp2)
        {
            var lines = list.Split("\n", StringSplitOptions.RemoveEmptyEntries);
            ClassicAssert.IsTrue(lines.Length >= 1);

            if (protocol == RedisProtocol.Resp3)
            {
                ClassicAssert.IsTrue(lines[0].StartsWith("txt:"));
                lines[0] = lines[0][4..];
            }

            foreach (var line in lines)
            {
                var flags = line.Split(" ");
                AssertField(line, flags, "id");
                AssertField(line, flags, "addr");
                AssertField(line, flags, "laddr");
                AssertField(line, flags, "age");
                AssertField(line, flags, "flags");
                AssertField(line, flags, "resp");
                AssertField(line, flags, "lib-name");
                AssertField(line, flags, "lib-ver");
            }

            // Check that a given flag is set
            static void AssertField(string line, string[] fields, string name)
            => ClassicAssert.AreEqual(1, fields.Count(f => f.StartsWith($"{name}=")), $"In {line}, expected single field {name}");
        }

        #region GETEX

        [Test]
        public void GetExpiryBasicTestWithSERedisApi()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "KeyA";
            var value = "ValueA";
            var expectedValue = "ValueA";
            var expireTimeSpan = TimeSpan.FromMinutes(1);

            db.StringSet(key, value);

            string actualValue = db.StringGetSetExpiry(key, expireTimeSpan);

            ClassicAssert.AreEqual(expectedValue, actualValue);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, 0);
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, expireTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase(null, null)]
        [TestCase(1, 1)]
        public void GetExpiryWithoutOptions(int? initialTimespanMins, int? expectedTimespanMins)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "KeyA";
            var value = "ValueA";
            var expectedValue = "ValueA";
            TimeSpan? initialTimeSpan = initialTimespanMins.HasValue ? TimeSpan.FromMinutes(initialTimespanMins.Value) : null;
            TimeSpan? expectedTimeSpan = expectedTimespanMins.HasValue ? TimeSpan.FromMinutes(expectedTimespanMins.Value) : null;

            if (initialTimeSpan.HasValue)
            {
                db.StringSet(key, value, initialTimeSpan);
            }
            else
            {
                db.StringSet(key, value);
            }

            var actualValue = (string)db.Execute("GETEX", key);

            ClassicAssert.AreEqual(expectedValue, actualValue);

            var actualTtl = db.KeyTimeToLive(key);

            if (expectedTimeSpan.HasValue)
            {
                ClassicAssert.IsTrue(actualTtl.HasValue);
                ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, 0);
                ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, expectedTimeSpan.Value.TotalMilliseconds);
            }
            else
            {
                ClassicAssert.IsFalse(actualTtl.HasValue);
            }
        }

        [Test]
        [TestCase(null, 1, false, false)]
        [TestCase(null, 1, true, false)]
        [TestCase(1, 2, false, false)]
        [TestCase(1, 2, true, false)]
        [TestCase(2, 1, false, false)]
        [TestCase(2, 1, true, false)]
        [TestCase(null, 1, false, true)]
        [TestCase(null, 1, true, true)]
        [TestCase(1, 2, false, true)]
        [TestCase(1, 2, true, true)]
        [TestCase(2, 1, false, true)]
        [TestCase(2, 1, true, true)]
        public void GetExpiryWithExpireOptions(int? initialTimespanMins, int newTimespanMins, bool isMilliseconds, bool isUnixTimestamp)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "KeyA";
            var value = "ValueA";
            var expectedValue = "ValueA";
            TimeSpan? initialTimeSpan = initialTimespanMins.HasValue ? TimeSpan.FromMinutes(initialTimespanMins.Value) : null;
            TimeSpan newTimeSpan = TimeSpan.FromMinutes(newTimespanMins);
            TimeSpan expectedTimeSpan = newTimeSpan;

            if (initialTimeSpan.HasValue)
            {
                db.StringSet(key, value, initialTimeSpan);
            }
            else
            {
                db.StringSet(key, value);
            }

            string actualValue = null;
            if (isMilliseconds && !isUnixTimestamp)
            {
                actualValue = (string)db.Execute("GETEX", key, "PX", newTimeSpan.TotalMilliseconds);
            }
            else if (isMilliseconds && isUnixTimestamp)
            {
                actualValue = (string)db.Execute("GETEX", key, "PXAT", DateTimeOffset.UtcNow.Add(newTimeSpan).ToUnixTimeMilliseconds());
            }
            else if (!isMilliseconds && !isUnixTimestamp)
            {
                actualValue = (string)db.Execute("GETEX", key, "EX", newTimeSpan.TotalSeconds);
            }
            else if (!isMilliseconds && isUnixTimestamp)
            {
                actualValue = (string)db.Execute("GETEX", key, "EXAT", DateTimeOffset.UtcNow.Add(newTimeSpan).ToUnixTimeSeconds());
            }


            ClassicAssert.AreEqual(expectedValue, actualValue);

            var actualTtl = db.KeyTimeToLive(key);

            ClassicAssert.IsTrue(actualTtl.HasValue);
            ClassicAssert.Greater(actualTtl.Value.TotalMilliseconds, expectedTimeSpan.TotalMilliseconds - 10000); // 10 seconds buffer
            ClassicAssert.LessOrEqual(actualTtl.Value.TotalMilliseconds, expectedTimeSpan.TotalMilliseconds);
        }

        [Test]
        [TestCase(null)]
        [TestCase(1)]
        public void GetExpiryWithPersistOptions(int? initialTimespanMins)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "KeyA";
            var value = "ValueA";
            var expectedValue = "ValueA";
            TimeSpan? initialTimeSpan = initialTimespanMins.HasValue ? TimeSpan.FromMinutes(initialTimespanMins.Value) : null;

            if (initialTimeSpan.HasValue)
            {
                db.StringSet(key, value, initialTimeSpan);
            }
            else
            {
                db.StringSet(key, value);
            }

            string actualValue = (string)db.Execute("GETEX", key, "PERSIST");
            ClassicAssert.AreEqual(expectedValue, actualValue);

            var actualTtl = db.KeyTimeToLive(key);
            ClassicAssert.IsFalse(actualTtl.HasValue);
        }

        [Test]
        public void GetExpiryWithUnknownKey()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "KeyA";

            var actualValue = (string)db.Execute("GETEX", key, "PERSIST");

            ClassicAssert.IsNull(actualValue);
        }

        [Test]
        [TestCase("EX,0")]
        [TestCase("EX,10,PERSIST")]
        [TestCase("EX,test")]
        [TestCase("EX,-1")]
        [TestCase("PXAT,0")]
        [TestCase("UNKNOWN")]
        public void GetExpiryWitInvalidOptions(string optionsInput)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "KeyA";
            var value = "ValueA";
            var options = optionsInput.Split(",");

            db.StringSet(key, value);

            Assert.Throws<RedisServerException>(() => db.Execute("GETEX", [key, .. options]));
        }

        #endregion

        #region GETSET

        [Test]
        public void GetSetWithExistingKey()
        {
            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var mainDB = mainConnection.GetDatabase(0);

            var key = "myKey";
            var val = "myKeyValue";
            var newValue = "myNewValue";

            mainDB.StringSet(key, val);

            string result = (string)mainDB.Execute("GETSET", key, newValue);  // Don't use StringGetSet as SE.Redis can chnage the underlying command to nondeprecated one anytime

            ClassicAssert.AreEqual(val, result);

            // Check the new value
            result = mainDB.StringGet(key);
            ClassicAssert.AreEqual(newValue, result);
        }

        [Test]
        public void GetSetWithNewKey()
        {
            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var mainDB = mainConnection.GetDatabase(0);

            var key = "myKey";
            var newValue = "myNewValue";

            string result = (string)mainDB.Execute("GETSET", key, newValue);  // Don't use StringGetSet as SE.Redis can chnage the underlying command to nondeprecated one anytime

            ClassicAssert.IsNull(result);

            // Check the new value
            result = mainDB.StringGet(key);
            ClassicAssert.AreEqual(newValue, result);
        }

        #endregion

        #region SETNX

        [Test]
        public void SetIfNotExistWithExistingKey()
        {
            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var mainDB = mainConnection.GetDatabase(0);

            var key = "myKey";
            var val = "myKeyValue";
            var newValue = "myNewValue";

            mainDB.StringSet(key, val);

            mainDB.Execute("SETNX", key, newValue);

            string result = mainDB.StringGet(key);
            ClassicAssert.AreEqual(val, result);
        }

        [Test]
        public void SetIfNotExistWithNewKey()
        {
            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var mainDB = mainConnection.GetDatabase(0);

            var key = "myKey";
            var newValue = "myNewValue";

            mainDB.Execute("SETNX", key, newValue);

            string result = mainDB.StringGet(key);
            ClassicAssert.AreEqual(newValue, result);
        }

        [Test]
        public void SetNXCorrectResponse()
        {
            var lightClientRequest = TestUtils.CreateRequest();

            // Set key
            var response = lightClientRequest.SendCommand("SETNX key1 2");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Setnx command should fail since key exists
            response = lightClientRequest.SendCommand("SETNX key1 3");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Be sure key wasn't modified
            response = lightClientRequest.SendCommand("GET key1");
            expectedResponse = "$1\r\n2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }
        #endregion

        #region SUBSTR

        [Test]
        [TestCase("my Key Value", 0, -1, "my Key Value")]
        [TestCase("my Key Value", 0, 4, "my Ke")]
        [TestCase("my Key Value", -3, -1, "lue")]
        [TestCase("abc", 0, 10, "abc")]
        public void SubStringWithOptions(string input, int start, int end, string expectedResult)
        {
            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var mainDB = mainConnection.GetDatabase(0);

            var key = "myKey";

            mainDB.StringSet(key, input);

            var actualResult = (string)mainDB.Execute("SUBSTR", key, start, end);

            ClassicAssert.AreEqual(expectedResult, actualResult);
        }

        #endregion

        #region LCS

        [Test]
        [TestCase("abc", "abc", "abc", Description = "Identical strings")]
        [TestCase("hello", "world", "o", Description = "Different strings with common subsequence")]
        [TestCase("", "abc", "", Description = "Empty first string")]
        [TestCase("abc", "", "", Description = "Empty second string")]
        [TestCase("", "", "", Description = "Both empty strings")]
        [TestCase("abc", "def", "", Description = "No common subsequence")]
        [TestCase("A string", "Another string", "A string", Description = "Strings with spaces")]
        [TestCase("ABCDEF", "ACDF", "ACDF", Description = "Multiple common subsequences")]
        [TestCase("ABCDEF", "ACEDF", "ACEF", Description = "Multiple common subsequences")]
        public void LCSBasicTest(string key1Value, string key2Value, string expectedLCS)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("key1", key1Value);
            db.StringSet("key2", key2Value);

            var result = (string)db.Execute("LCS", "key1", "key2");
            ClassicAssert.AreEqual(expectedLCS, result);
        }

        [Test]
        [TestCase("hello", "world", 1, Description = "Basic length check")]
        [TestCase("", "world", 0, Description = "Empty first string length")]
        [TestCase("hello", "", 0, Description = "Empty second string length")]
        [TestCase("", "", 0, Description = "Both empty strings length")]
        [TestCase("abc", "def", 0, Description = "No common subsequence length")]
        [TestCase("ABCDEF", "ACEDF", 4, Description = "Multiple common subsequences")]
        [TestCase("A string", "Another string", 8, Description = "Strings with spaces")]
        public void LCSWithLenOption(string key1Value, string key2Value, int expectedLength)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("key1", key1Value);
            db.StringSet("key2", key2Value);

            var result = (long)db.Execute("LCS", "key1", "key2", "LEN");
            ClassicAssert.AreEqual(expectedLength, result);
        }

        [Test]
        [TestCase("ABCDEF", "ACEDF", 4, new[] { 5, 4, 2, 0 }, new[] { 5, 4, 2, 0 }, new[] { 4, 2, 1, 0 }, new[] { 4, 2, 1, 0 }, Description = "Multiple matches")]
        [TestCase("hello", "world", 1, new[] { 4 }, new[] { 4 }, new[] { 1 }, new[] { 1 }, Description = "Basic IDX test")]
        [TestCase("abc", "def", 0, new int[] { }, new int[] { }, new int[] { }, new int[] { }, Description = "No matches")]
        [TestCase("", "", 0, new int[] { }, new int[] { }, new int[] { }, new int[] { }, Description = "Empty strings")]
        [TestCase("abc", "", 0, new int[] { }, new int[] { }, new int[] { }, new int[] { }, Description = "One empty string")]
        [TestCase("Hello World!", "Hello Earth!", 8, new[] { 11, 8, 0 }, new[] { 11, 8, 5 }, new[] { 11, 8, 0 }, new[] { 11, 8, 5 }, Description = "Multiple words with punctuation")]
        [TestCase("AAABBB", "AAABBB", 6, new[] { 0 }, new[] { 5 }, new[] { 0 }, new[] { 5 }, Description = "Identical strings")]
        [TestCase("    abc", "abc   ", 3, new[] { 4 }, new[] { 6 }, new[] { 0 }, new[] { 2 }, Description = "Strings with spaces")]
        public void LCSWithIdxOption(string key1Value, string key2Value, int expectedLength,
            int[] expectedKey1StartPositions, int[] expectedKey1EndPositions,
            int[] expectedKey2StartPositions, int[] expectedKey2EndPositions)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("key1", key1Value);
            db.StringSet("key2", key2Value);

            var result = db.Execute("LCS", "key1", "key2", "IDX");
            var resultDict = result.ToDictionary();

            ClassicAssert.AreEqual(expectedLength, (int)resultDict["len"]);

            if (expectedLength > 0)
            {
                var matches = (RedisResult[])resultDict["matches"];
                for (int i = 0; i < matches.Length; i++)
                {
                    var match = (RedisResult[])matches[i];
                    var positions1 = Array.ConvertAll((RedisValue[])match[0], x => (int)x);
                    var positions2 = Array.ConvertAll((RedisValue[])match[1], x => (int)x);

                    ClassicAssert.AreEqual(expectedKey1StartPositions[i], positions1[0]);
                    ClassicAssert.AreEqual(expectedKey1EndPositions[i], positions1[1]);
                    ClassicAssert.AreEqual(expectedKey2StartPositions[i], positions2[0]);
                    ClassicAssert.AreEqual(expectedKey2EndPositions[i], positions2[1]);
                }
            }
            else
            {
                ClassicAssert.IsEmpty((RedisResult[])resultDict["matches"]);
            }
        }

        [Test]
        [TestCase("ABCDEF", "ACEDF", 2, 0, new int[] { }, new int[] { }, new int[] { }, new int[] { }, Description = "Basic MINMATCHLEN test")]
        [TestCase("hello world12", "hello earth12", 3, 1, new[] { 0 }, new[] { 5 }, new[] { 0 }, new[] { 5 }, Description = "Multiple matches with spaces")]
        [TestCase("", "", 0, 0, new int[] { }, new int[] { }, new int[] { }, new int[] { }, Description = "Empty strings")]
        [TestCase("abc", "", 0, 0, new int[] { }, new int[] { }, new int[] { }, new int[] { }, Description = "One empty string")]
        [TestCase("abcdef", "abcdef", 1, 1, new[] { 0 }, new[] { 5 }, new[] { 0 }, new[] { 5 }, Description = "Identical strings")]
        [TestCase("AAABBBCCC", "AAACCC", 3, 2, new[] { 6, 0 }, new[] { 8, 2 }, new[] { 3, 0 }, new[] { 5, 2 }, Description = "Repeated characters")]
        [TestCase("Hello World!", "Hello Earth!", 4, 1, new[] { 0 }, new[] { 5 }, new[] { 0 }, new[] { 5 }, Description = "Words with punctuation")]
        [TestCase("    abc    ", "abc", 2, 1, new[] { 4 }, new[] { 6 }, new[] { 0 }, new[] { 2 }, Description = "Strings with leading/trailing spaces")]
        public void LCSWithIdxAndMinMatchLen(string key1Value, string key2Value, int minMatchLen,
            int expectedMatchCount, int[] expectedKey1StartPositions, int[] expectedKey1EndPositions,
            int[] expectedKey2StartPositions, int[] expectedKey2EndPositions)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("key1", key1Value);
            db.StringSet("key2", key2Value);

            var result = db.Execute("LCS", "key1", "key2", "IDX", "MINMATCHLEN", minMatchLen);
            var resultDict = result.ToDictionary();
            var matches = (RedisResult[])resultDict["matches"];

            ClassicAssert.AreEqual(expectedMatchCount, matches.Length);

            for (int i = 0; i < expectedMatchCount; i++)
            {
                var match = (RedisResult[])matches[i];
                var positions1 = Array.ConvertAll((RedisValue[])match[0], x => (int)x);
                var positions2 = Array.ConvertAll((RedisValue[])match[1], x => (int)x);

                ClassicAssert.AreEqual(expectedKey1StartPositions[i], positions1[0]);
                ClassicAssert.AreEqual(expectedKey1EndPositions[i], positions1[1]);
                ClassicAssert.AreEqual(expectedKey2StartPositions[i], positions2[0]);
                ClassicAssert.AreEqual(expectedKey2EndPositions[i], positions2[1]);
            }
        }

        [Test]
        [TestCase("ABCDEF", "ACEDF", 0, 4, new[] { 1, 1, 1, 1 }, new[] { 5, 4, 2, 0 }, new[] { 5, 4, 2, 0 }, new[] { 4, 2, 1, 0 }, new[] { 4, 2, 1, 0 }, Description = "Basic WITHMATCHLEN test")]
        [TestCase("hello world12", "hello earth12", 3, 1, new[] { 6 }, new[] { 0 }, new[] { 5 }, new[] { 0 }, new[] { 5 }, Description = "Multiple matches with lengths")]
        [TestCase("", "", 0, 0, new int[] { }, new int[] { }, new int[] { }, new int[] { }, new int[] { }, Description = "Empty strings")]
        [TestCase("abc", "", 0, 0, new int[] { }, new int[] { }, new int[] { }, new int[] { }, new int[] { }, Description = "One empty string")]
        [TestCase("abcdef", "abcdef", 0, 1, new[] { 6 }, new[] { 0 }, new[] { 5 }, new[] { 0 }, new[] { 5 }, Description = "Identical strings")]
        [TestCase("AAABBBCCC", "AAACCC", 3, 2, new[] { 3, 3 }, new[] { 6, 0 }, new[] { 8, 2 }, new[] { 3, 0 }, new[] { 5, 2 }, Description = "Repeated characters")]
        [TestCase("Hello World!", "Hello Earth!", 2, 1, new[] { 6 }, new[] { 0 }, new[] { 5 }, new[] { 0 }, new[] { 5 }, Description = "Words with punctuation")]
        [TestCase("    abc    ", "abc", 2, 1, new[] { 3 }, new[] { 4 }, new[] { 6 }, new[] { 0 }, new[] { 2 }, Description = "Strings with leading/trailing spaces")]
        public void LCSWithIdxAndWithMatchLen(string key1Value, string key2Value, int minMatchLen,
            int expectedMatchCount, int[] expectedMatchLengths, int[] expectedKey1StartPositions,
            int[] expectedKey1EndPositions, int[] expectedKey2StartPositions, int[] expectedKey2EndPositions)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("key1", key1Value);
            db.StringSet("key2", key2Value);

            var result = db.Execute("LCS", "key1", "key2", "IDX", "MINMATCHLEN", minMatchLen, "WITHMATCHLEN");
            var resultDict = result.ToDictionary();
            var matches = (RedisResult[])resultDict["matches"];

            ClassicAssert.AreEqual(expectedMatchCount, matches.Length);

            for (int i = 0; i < expectedMatchCount; i++)
            {
                var match = (RedisResult[])matches[i];
                var positions1 = Array.ConvertAll((RedisValue[])match[0], x => (int)x);
                var positions2 = Array.ConvertAll((RedisValue[])match[1], x => (int)x);

                ClassicAssert.AreEqual(expectedKey1StartPositions[i], positions1[0]);
                ClassicAssert.AreEqual(expectedKey1EndPositions[i], positions1[1]);
                ClassicAssert.AreEqual(expectedKey2StartPositions[i], positions2[0]);
                ClassicAssert.AreEqual(expectedKey2EndPositions[i], positions2[1]);
                ClassicAssert.AreEqual(expectedMatchLengths[i], (int)match[2]);
            }
        }

        [Test]
        public void LCSWithInvalidOptions()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("key1", "hello");
            db.StringSet("key2", "world");

            // Invalid option
            Assert.Throws<RedisServerException>(() => db.Execute("LCS", "key1", "key2", "INVALID"));

            // Invalid MINMATCHLEN value
            Assert.Throws<RedisServerException>(() => db.Execute("LCS", "key1", "key2", "IDX", "MINMATCHLEN", "abc"));
        }

        [Test]
        public void LCSWithNonExistentKeys()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Basic LCS with non-existent keys
            var result = (string)db.Execute("LCS", "nonexistent1", "nonexistent2");
            ClassicAssert.AreEqual("", result);

            // LCS LEN with non-existent keys
            result = db.Execute("LCS", "nonexistent1", "nonexistent2", "LEN").ToString();
            ClassicAssert.AreEqual("0", result);

            // LCS IDX with non-existent keys
            var idxResult = db.Execute("LCS", "nonexistent1", "nonexistent2", "IDX");
            var resultDict = idxResult.ToDictionary();
            ClassicAssert.AreEqual(0, (int)resultDict["len"]);
            ClassicAssert.IsEmpty((RedisResult[])resultDict["matches"]);
        }

        #endregion

        [Test]
        [TestCase("BLOCKING", "ERROR", true, Description = "Unblock normal client with ERROR")]
        [TestCase("BLOCKING", "TIMEOUT", true, Description = "Unblock normal client with TIMEOUT")]
        [TestCase("BLOCKING", null, true, Description = "Unblock normal client without mode")]
        public async Task ClientUnblockBasicTest(string clientType, string mode, bool expectedResult)
        {
            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var mainDB = mainConnection.GetDatabase(0);

            // Start blocking client
            using var blockingClient = TestUtils.CreateRequest();
            var clientIdResponse = Encoding.ASCII.GetString(blockingClient.SendCommand("CLIENT ID"));
            var clientId = clientIdResponse.Substring(1, clientIdResponse.IndexOf("\r\n") - 1);
            Task blockingTask = null;
            bool isError = false;
            if (clientType == "BLOCKING")
            {
                blockingTask = taskFactory.StartNew(() =>
                {
                    var startTime = Stopwatch.GetTimestamp();
                    var response = blockingClient.SendCommand("BLMPOP 10 1 keyA LEFT");
                    if (Encoding.ASCII.GetString(response).Substring(0, "-UNBLOCKED".Length) == "-UNBLOCKED")
                    {
                        isError = true;
                    }
                    var totalTime = Stopwatch.GetElapsedTime(startTime);
                    ClassicAssert.Less(totalTime.TotalSeconds, 9);
                });
            }
            // Add other mode like XREAD Readis steam here

            // Wait for client to enter blocking state
            await Task.Delay(1000);

            // Unblock from main connection
            var args = new List<string> { "UNBLOCK", clientId };
            if (mode != null)
            {
                args.Add(mode);
            }
            var unblockResult = (int)mainDB.Execute("CLIENT", [.. args]);
            ClassicAssert.AreEqual(expectedResult ? 1 : 0, unblockResult);
            blockingTask.Wait();

            if (mode == "ERROR")
            {
                ClassicAssert.IsTrue(isError);
            }
            else
            {
                ClassicAssert.IsFalse(isError);
            }

            // Attempt to unblock again will return 0
            unblockResult = (int)mainDB.Execute("CLIENT", [.. args]);
            ClassicAssert.AreEqual(0, unblockResult);
        }

        [Test]
        [TestCase(0, Description = "Guarantied concurrent unblock test")]
        [TestCase(3, Description = "Random chance unblock sucess/failed case")]
        [TestCase(20, Description = "Probable unblock failed case")]
        public async Task MultipleClientsUnblockAndAddTest(int numberOfItems)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "blockingList";
            var value = "testValue";

            // Start blocking client
            using var blockingClient = TestUtils.CreateRequest();
            var clientIdResponse = Encoding.ASCII.GetString(blockingClient.SendCommand("CLIENT ID"));
            var clientId = clientIdResponse.Substring(1, clientIdResponse.IndexOf("\r\n") - 1);

            string blockingResult = null;
            var blockingTask = Task.Run(() =>
            {
                var response = blockingClient.SendCommand($"BLMPOP 10 1 {key} LEFT COUNT 30");
                blockingResult = Encoding.ASCII.GetString(response);
            });

            // Wait for client to enter blocking state
            await Task.Delay(1000);

            // Start parallel unblock and add tasks
            var unblockTasks = new List<Task<int>>();
            var addTasks = new List<Task>();
            for (int i = 0; i < numberOfItems; i++)
            {
                var _i = i;
                addTasks.Add(Task.Run(() => redis.GetDatabase(0).ListLeftPush(key, $"{value}{_i}")));
            }

            for (int i = 0; i < 3; i++)
            {
                unblockTasks.Add(Task.Run(() => (int)redis.GetDatabase(0).Execute("CLIENT", "UNBLOCK", clientId, "ERROR")));
            }

            await Task.WhenAll(unblockTasks);
            await Task.WhenAll(addTasks);
            await blockingTask;

            var numberOfItemsReturned = Regex.Matches(blockingResult, value).Count;

            var listLength = db.ListLength(key);
            ClassicAssert.AreEqual(numberOfItems - numberOfItemsReturned, listLength);

            if (numberOfItemsReturned == 0)
            {
                ClassicAssert.IsTrue(blockingResult.StartsWith("-UNBLOCKED"));
                ClassicAssert.IsTrue(unblockTasks.Any(x => x.Result == 1));
            }
            else
            {
                ClassicAssert.IsTrue(unblockTasks.All(x => x.Result == 0));
            }
        }

        [Test]
        [TestCase(-1, Description = "Unblock with invalid client ID")]
        [TestCase(999999, Description = "Unblock with non-existent client ID")]
        public void ClientUnblockInvalidIdTest(int invalidId)
        {
            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var mainDB = mainConnection.GetDatabase(0);

            var unblockResult = (int)mainDB.Execute("CLIENT", "UNBLOCK", invalidId);
            ClassicAssert.AreEqual(0, unblockResult);
        }

        [Test]
        public void ClientUnblockInvalidModeTest()
        {
            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var mainDB = mainConnection.GetDatabase(0);

            Assert.Throws<RedisServerException>(() => mainDB.Execute("CLIENT", "UNBLOCK", 123, "INVALID"));
        }
    }
}