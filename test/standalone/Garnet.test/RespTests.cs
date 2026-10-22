// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net.Sockets;
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
    public class RespTests : TestBase
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            TestUtils.OnTearDown();
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
        /// RESTORE sets its TTL in milliseconds, including TTLs too long for an int.
        /// </summary>
        [Test]
        public void RestoreTtlIsInMilliseconds()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("mykey", "val");
            var dump = db.KeyDump("mykey")!;
            db.KeyDelete("mykey");

            foreach (var ttl in new[] { TimeSpan.FromHours(1), TimeSpan.FromDays(30) })
            {
                db.KeyRestore("mykey", dump, ttl);
                var restoredTtl = db.KeyTimeToLive("mykey");
                ClassicAssert.IsNotNull(restoredTtl, ttl.ToString());
                ClassicAssert.Greater(restoredTtl.Value, ttl - TimeSpan.FromMinutes(1), ttl.ToString());
                ClassicAssert.LessOrEqual(restoredTtl.Value, ttl, ttl.ToString());
                db.KeyDelete("mykey");
            }
        }

        /// <summary>
        /// RESTORE rejects a TTL that is not an integer, is negative, or gives an unrepresentable expiration, and creates no key.
        /// </summary>
        [Test]
        public void RestoreRejectsInvalidTtl()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("mykey", "val");
            var dump = db.KeyDump("mykey")!;
            db.KeyDelete("mykey");

            foreach (var (ttl, expected) in new (object, string)[]
            {
                ("abc", "ERR value is not an integer or out of range."),
                ("1.5", "ERR value is not an integer or out of range."),
                (-1, "ERR invalid expire time, must be >= 0"),
                (long.MaxValue, "ERR invalid expire time in 'restore' command"),
            })
            {
                var ex = ClassicAssert.Throws<RedisServerException>(() => db.Execute("RESTORE", "mykey", ttl, dump));
                ClassicAssert.AreEqual(expected, ex.Message, ttl.ToString());
                ClassicAssert.IsFalse(db.KeyExists("mykey"), ttl.ToString());
            }
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

            ClassicAssert.IsNull(value);
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
            await db.StringSetAsync("mykey", origValue).ConfigureAwait(false);

            string retValue = await db.StringGetAsync("mykey").ConfigureAwait(false);

            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public async Task SingleUnicodeSetGetGarnetClient()
        {
            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            string origValue = "笑い男";
            await db.StringSetAsync("mykey", origValue).ConfigureAwait(false);

            string retValue = await db.StringGetAsync("mykey").ConfigureAwait(false);

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
        public void StringCommandsWrongArityReturnErrorAndKeepSessionAlive()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            // Each malformed command is followed by PING. On unpatched main these
            // reach a handler that only Debug.Assert()s the argument count (or reads
            // the missing argument past the parse state), aborting the process in a
            // Debug build and reading out of bounds in Release, instead of replying
            // with an error. The trailing PONG proves the session stayed alive.
            void ExpectError(string cmd, string expectedError)
            {
                var response = lightClientRequest.SendCommands(cmd, "PING");
                TestUtils.AssertEqualUpToExpectedLength($"{expectedError}\r\n+PONG\r\n", response);
            }

            ExpectError("SET k", "-ERR wrong number of arguments for 'SET' command");
            ExpectError("SET", "-ERR wrong number of arguments for 'SET' command");
            ExpectError("SET k v EX", "-ERR syntax error");
            ExpectError("SET k v PX", "-ERR syntax error");
            ExpectError("GETSET k", "-ERR wrong number of arguments for 'GETSET' command");
            ExpectError("GETSET k v extra", "-ERR wrong number of arguments for 'GETSET' command");
            ExpectError("SETEX k", "-ERR wrong number of arguments for 'SETEX' command");
            ExpectError("SETEX k 10", "-ERR wrong number of arguments for 'SETEX' command");
            ExpectError("PSETEX k 10", "-ERR wrong number of arguments for 'PSETEX' command");
            ExpectError("SETRANGE k", "-ERR wrong number of arguments for 'SETRANGE' command");
            ExpectError("SETRANGE k 0", "-ERR wrong number of arguments for 'SETRANGE' command");
            ExpectError("APPEND k", "-ERR wrong number of arguments for 'APPEND' command");
            ExpectError("GETRANGE k", "-ERR wrong number of arguments for 'GETRANGE' command");
            ExpectError("GETRANGE k 0", "-ERR wrong number of arguments for 'GETRANGE' command");
            ExpectError("SUBSTR k", "-ERR wrong number of arguments for 'SUBSTR' command");
            ExpectError("SUBSTR k 0", "-ERR wrong number of arguments for 'SUBSTR' command");
            ExpectError("GETWITHETAG", "-ERR wrong number of arguments for 'GETWITHETAG' command");
            ExpectError("GETWITHETAG k extra", "-ERR wrong number of arguments for 'GETWITHETAG' command");
            ExpectError("GETIFNOTMATCH", "-ERR wrong number of arguments for 'GETIFNOTMATCH' command");
            ExpectError("GETIFNOTMATCH k", "-ERR wrong number of arguments for 'GETIFNOTMATCH' command");

            // Well-formed forms are unchanged.
            void ExpectOk(string cmd)
            {
                var response = lightClientRequest.SendCommands(cmd, "PING");
                TestUtils.AssertEqualUpToExpectedLength("+OK\r\n+PONG\r\n", response);
            }

            ExpectOk("SET a 1");
            ExpectOk("SETEX b 100 v");
            ExpectOk("PSETEX c 5000 v");

            var response = lightClientRequest.SendCommands("SETRANGE a 1 XY", "PING");
            TestUtils.AssertEqualUpToExpectedLength(":3\r\n+PONG\r\n", response);
            response = lightClientRequest.SendCommands("APPEND a Z", "PING");
            TestUtils.AssertEqualUpToExpectedLength(":4\r\n+PONG\r\n", response);
            response = lightClientRequest.SendCommands("GETSET a fresh", "PING");
            TestUtils.AssertEqualUpToExpectedLength("$4\r\n1XYZ\r\n+PONG\r\n", response);
            response = lightClientRequest.SendCommands("GETRANGE a 0 2", "PING");
            TestUtils.AssertEqualUpToExpectedLength("$3\r\nfre\r\n+PONG\r\n", response);
            response = lightClientRequest.SendCommands("SUBSTR a 0 2", "PING");
            TestUtils.AssertEqualUpToExpectedLength("$3\r\nfre\r\n+PONG\r\n", response);

            // SET is declared with arity -3 and the fast parser only routes it to the option
            // parser for array lengths 3..7, so a longer option-bearing SET reaches NetworkSET.
            // Redis accepts a repeated GET, and must not lose a trailing EX.
            response = lightClientRequest.SendCommands("SET a fresh2 GET GET GET GET GET GET", "PING");
            TestUtils.AssertEqualUpToExpectedLength("$5\r\nfresh\r\n+PONG\r\n", response);
            response = lightClientRequest.SendCommands("SET a fresh3 GET GET GET GET GET GET GET EX 100", "PING");
            TestUtils.AssertEqualUpToExpectedLength("$6\r\nfresh2\r\n+PONG\r\n", response);
            // The EX actually applied rather than being dropped.
            response = lightClientRequest.SendCommands("TTL a", "PING");
            TestUtils.AssertEqualUpToExpectedLength(":100\r\n+PONG\r\n", response);
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
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
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
        public void SetExpiryAcceptsInt64Value()
        {
            const long LargeMilliseconds = 4_294_967_296;
            const long LargeSeconds = 3_000_000_000;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            void AssertLargeTtl(string key, long expectedTtlMilliseconds)
            {
                var ttl = (long)db.Execute("PTTL", key);
                ClassicAssert.Greater(ttl, expectedTtlMilliseconds - 10_000);
                ClassicAssert.LessOrEqual(ttl, expectedTtlMilliseconds);
            }

            var result = db.Execute("SET", "set-px", "value", "PX", LargeMilliseconds);
            ClassicAssert.AreEqual("OK", result.ToString());
            AssertLargeTtl("set-px", LargeMilliseconds);

            result = db.Execute("SET", "set-ex", "value", "EX", LargeSeconds);
            ClassicAssert.AreEqual("OK", result.ToString());
            AssertLargeTtl("set-ex", LargeSeconds * 1000);

            result = db.Execute("SETEX", "setex", LargeSeconds, "value");
            ClassicAssert.AreEqual("OK", result.ToString());
            AssertLargeTtl("setex", LargeSeconds * 1000);

            result = db.Execute("PSETEX", "psetex", LargeMilliseconds, "value");
            ClassicAssert.AreEqual("OK", result.ToString());
            AssertLargeTtl("psetex", LargeMilliseconds);

            result = db.Execute("SET", "set-nx", "value", "PX", LargeMilliseconds, "NX");
            ClassicAssert.AreEqual("OK", result.ToString());
            AssertLargeTtl("set-nx", LargeMilliseconds);

            ClassicAssert.IsTrue(db.StringSet("set-xx-get", "old-value"));
            result = db.Execute("SET", "set-xx-get", "new-value", "PX", LargeMilliseconds, "XX", "GET");
            ClassicAssert.AreEqual("old-value", result.ToString());
            AssertLargeTtl("set-xx-get", LargeMilliseconds);
        }

        [Test]
        public void SetExpiryOutOfRangeIsRejectedWithoutKillingSession()
        {
            const string Key = nameof(SetExpiryOutOfRangeIsRejectedWithoutKillingSession);
            const string ExpectedError = "ERR invalid expire time in 'set' command";

            var faulted = false;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                redis.ConnectionFailed += delegate { faulted = true; };

                var db = redis.GetDatabase();
                ClassicAssert.IsTrue(db.StringSet(Key, "original"));

                var overflowSeconds = ((long.MaxValue - DateTimeOffset.UtcNow.Ticks) / TimeSpan.TicksPerSecond) + 1;

                void AssertInvalidExpiry(string command, params object[] args)
                {
                    var exception = ClassicAssert.Throws<RedisServerException>(() => db.Execute(command, args));
                    ClassicAssert.AreEqual(ExpectedError, exception.Message);
                    ClassicAssert.AreEqual("original", db.StringGet(Key).ToString());
                }

                AssertInvalidExpiry("SET", Key, "replacement", "EX", overflowSeconds);
                AssertInvalidExpiry("SET", Key, "replacement", "PX", long.MaxValue);
                AssertInvalidExpiry("SETEX", Key, overflowSeconds, "replacement");
                AssertInvalidExpiry("PSETEX", Key, long.MaxValue, "replacement");

                ClassicAssert.Greater(db.Ping(), TimeSpan.Zero);
            }

            ClassicAssert.IsFalse(faulted);
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
            ClassicAssert.IsTrue(int.TryParse(resp, out ttl) && ttl > 0 && ttl <= 60);

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
            // Key already expired from the "ex .. xx" block above, so DEL is a no-op (see issue #2244).
            ClassicAssert.IsFalse(db.KeyDelete(key));
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
            string[] values = ["", "7 3", "02+(34", "笑い男", "01", "-01", "7ab", "--5", "-+5", "+5", "+05", "+0", "-0"];

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
        [TestCase(1L)]
        [TestCase(100L)]
        public void SingleIncrementFromLongMinValue(long increment)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "test";
            db.StringSet(key, long.MinValue.ToString(CultureInfo.InvariantCulture));

            var result = increment == 1 ? db.StringIncrement(key) : db.StringIncrement(key, increment);
            var expected = long.MinValue + increment;

            ClassicAssert.AreEqual(expected, result);
            ClassicAssert.AreEqual(expected.ToString(CultureInfo.InvariantCulture), db.StringGet(key).ToString());
        }

        /// <summary>
        /// A stored long.MinValue is a valid integer on the copy-update path (record in the read-only region) and in the
        /// record sizing that precedes it. Decrementing it overflows and leaves the value unchanged.
        /// </summary>
        [Test]
        public void IncrementFromLongMinValueInReadOnlyRegion()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var store = server.Provider.StoreWrapper.store;
            var minValue = long.MinValue.ToString(CultureInfo.InvariantCulture);

            _ = db.StringSet("incr", minValue);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            ClassicAssert.AreEqual(long.MinValue + 1, db.StringIncrement("incr"));
            ClassicAssert.AreEqual((long.MinValue + 1).ToString(CultureInfo.InvariantCulture), (string)db.StringGet("incr"));

            _ = db.StringSet("decr", minValue);
            var ex = ClassicAssert.Throws<RedisServerException>(() => db.StringDecrement("decr"));
            ClassicAssert.AreEqual("ERR value is not an integer or out of range.", ex.Message);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            ex = ClassicAssert.Throws<RedisServerException>(() => db.StringDecrement("decr"));
            ClassicAssert.AreEqual("ERR value is not an integer or out of range.", ex.Message);
            ClassicAssert.AreEqual(minValue, (string)db.StringGet("decr"));
        }

        /// <summary>
        /// Incrementing a stored value that is not a number is an error reply on the in-place path and on the copy-update
        /// path (record in the read-only region), and leaves the value unchanged.
        /// </summary>
        [Test]
        public void IncrementOfNonNumberIsAnError([Values("INCR", "INCRBY", "DECR", "DECRBY", "INCRBYFLOAT")] string cmd)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var store = server.Provider.StoreWrapper.store;
            var expectedError = cmd == "INCRBYFLOAT" ? "ERR value is not a valid float" : "ERR value is not an integer or out of range.";

            _ = db.StringSet("key", "hello");
            foreach (var readOnlyRegion in new[] { false, true })
            {
                if (readOnlyRegion)
                    store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

                var ex = ClassicAssert.Throws<RedisServerException>(() => _ = cmd is "INCR" or "DECR" ? db.Execute(cmd, "key") : db.Execute(cmd, "key", "1"));
                ClassicAssert.AreEqual(expectedError, ex.Message);
                ClassicAssert.AreEqual("hello", (string)db.StringGet("key"));
            }
        }

        /// <summary>
        /// A stored integer must be canonical: INCR and DECR reject a '+' sign and -0 on the copy-update and in-place paths.
        /// </summary>
        [Test]
        public void IncrementOfNonCanonicalIntegerIsAnError([Values("+5", "+05", "+0", "-0")] string stored, [Values] bool readOnlyRegion)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var store = server.Provider.StoreWrapper.store;

            _ = db.StringSet("key", stored);
            if (readOnlyRegion)
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            foreach (var cmd in new[] { "INCR", "DECR" })
            {
                var ex = ClassicAssert.Throws<RedisServerException>(() => db.Execute(cmd, "key"));
                ClassicAssert.AreEqual("ERR value is not an integer or out of range.", ex.Message, cmd);
                ClassicAssert.AreEqual(stored, (string)db.StringGet("key"), cmd);
            }
        }

        /// <summary>
        /// DECRBY negates its argument, and -long.MinValue does not fit in a long.
        /// </summary>
        [Test]
        public void DecrementByLongMinValueIsRejected()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var minValue = long.MinValue.ToString(CultureInfo.InvariantCulture);

            _ = db.StringSet("existing", 5);
            var ex = ClassicAssert.Throws<RedisServerException>(() => db.Execute("DECRBY", "existing", minValue));
            ClassicAssert.AreEqual("ERR decrement would overflow", ex.Message);
            ClassicAssert.AreEqual("5", (string)db.StringGet("existing"));

            ex = ClassicAssert.Throws<RedisServerException>(() => db.Execute("DECRBY", "missing", minValue));
            ClassicAssert.AreEqual("ERR decrement would overflow", ex.Message);
            ClassicAssert.IsFalse(db.KeyExists("missing"));

            // The most negative decrement that can be negated is applied
            _ = db.StringSet("boundary", -5);
            var result = (long)db.Execute("DECRBY", "boundary", (-long.MaxValue).ToString(CultureInfo.InvariantCulture));
            ClassicAssert.AreEqual(long.MaxValue - 5, result);
        }

        /// <summary>
        /// Integer arguments must be canonical: a lone sign, a '+' sign and -0 are rejected. Read as 0, EXPIRE would delete the key.
        /// </summary>
        [Test]
        public void NonCanonicalIntegerArgumentsAreRejected([Values("-", "+", "+5", "+0", "-0")] string arg)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            _ = db.StringSet("key", "10");

            var ex = ClassicAssert.Throws<RedisServerException>(() => db.Execute("INCRBY", "key", arg));
            ClassicAssert.AreEqual("ERR value is not an integer or out of range.", ex.Message);
            ex = ClassicAssert.Throws<RedisServerException>(() => db.Execute("EXPIRE", "key", arg));
            ClassicAssert.AreEqual("ERR value is not an integer or out of range.", ex.Message);
            ex = ClassicAssert.Throws<RedisServerException>(() => db.Execute("SETRANGE", "key", arg, "x"));
            ClassicAssert.AreEqual("ERR value is not an integer or out of range.", ex.Message);
            ex = ClassicAssert.Throws<RedisServerException>(() => db.Execute("BITFIELD", "key", "GET", "u8", arg));
            ClassicAssert.AreEqual("ERR bit offset is not an integer or out of range", ex.Message);

            ClassicAssert.AreEqual("10", (string)db.StringGet("key"));
            ClassicAssert.IsNull(db.KeyTimeToLive("key"));
        }

        /// <summary>
        /// 32-bit integer arguments reject leading zeros, as 64-bit integer arguments do.
        /// </summary>
        [Test]
        public void LeadingZerosInIntegerArgumentsAreRejected()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            _ = db.StringSet("key", "0123456789");

            foreach (var arg in new[] { "01", "-01", "007" })
            {
                var ex = ClassicAssert.Throws<RedisServerException>(() => db.Execute("GETRANGE", "key", arg, "5"));
                ClassicAssert.AreEqual("ERR value is not an integer or out of range.", ex.Message);
                ex = ClassicAssert.Throws<RedisServerException>(() => db.Execute("SETRANGE", "key", arg, "x"));
                ClassicAssert.AreEqual("ERR value is not an integer or out of range.", ex.Message);
            }

            ClassicAssert.AreEqual("0123456789", (string)db.StringGet("key"));
            ClassicAssert.AreEqual("01", (string)db.Execute("GETRANGE", "key", "0", "1"));
        }

        /// <summary>
        /// A command argument whose magnitude is >= 2^64 (20+ digits) used to throw an uncaught
        /// RespParsingException out of the shared digit-parsing helper, which closed the connection
        /// and reported "ERR Protocol Error" instead of the ordinary out-of-range command error.
        /// A trailing PING on the same connection confirms the session survives.
        /// </summary>
        [Test]
        [TestCase("18446744073709551616")]     // 2^64
        [TestCase("-18446744073709551616")]    // -2^64
        [TestCase("99999999999999999999999")]  // much larger than 2^64
        public void HugeIntegerArgumentDoesNotCloseConnection(string hugeValue)
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            var notIntegerResponse = "-ERR value is not an integer or out of range.\r\n+PONG\r\n";

            lightClientRequest.SendCommand("SET incrkey 1");
            var response = lightClientRequest.SendCommands($"INCRBY incrkey {hugeValue}", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(notIntegerResponse, response);

            response = lightClientRequest.SendCommands($"PEXPIRE incrkey {hugeValue}", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(notIntegerResponse, response);

            lightClientRequest.SendCommand("RPUSH listkey a b c");
            response = lightClientRequest.SendCommands($"LRANGE listkey {hugeValue} 100", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(notIntegerResponse, response);

            response = lightClientRequest.SendCommands($"LINDEX listkey {hugeValue}", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(notIntegerResponse, response);

            // The key must not have been mutated by the rejected INCRBY.
            response = lightClientRequest.SendCommand("GET incrkey");
            TestUtils.AssertEqualUpToExpectedLength("$1\r\n1\r\n", response);
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
            var storedValue = (string)db.StringGet(key);

            // An increment that overflows to infinity is an error reply that leaves the value unchanged
            var ex = Assert.Throws<RedisServerException>(() => db.Execute("INCRBYFLOAT", key, incrByValue));
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_NAN_INFINITY_INCR), ex.Message);
            ClassicAssert.AreEqual(storedValue, (string)db.StringGet(key));
        }

        /// <summary>
        /// INCRBYFLOAT validates and sizes a copy-updated record (record in the read-only region) exactly as the in-place
        /// path does: a leading zero is a valid float, and a stored value that the update rejects is left unchanged.
        /// </summary>
        [Test]
        public void IncrementByFloatInReadOnlyRegion()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var store = server.Provider.StoreWrapper.store;

            _ = db.StringSet("leadingZero", "05");
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            ClassicAssert.AreEqual(123456794.125, db.StringIncrement("leadingZero", 123456789.125));
            ClassicAssert.AreEqual("123456794.125", (string)db.StringGet("leadingZero"));

            foreach (var (stored, increment) in new[] { ("--5", -4.0), ("-+5", 1.0), ("Infinity", 1.0), ("1e308", 1e308) })
            {
                _ = db.StringSet("rejected", stored);
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                _ = ClassicAssert.Throws<RedisServerException>(() => db.StringIncrement("rejected", increment), stored);
                ClassicAssert.AreEqual(stored, (string)db.StringGet("rejected"));
            }
        }

        [Test]
        public void IncrementByFloatRejectsNaN([Values("nan", "NaN", "-NaN")] string nan)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = ClassicAssert.Throws<RedisServerException>(() => db.Execute("INCRBYFLOAT", "missing", nan));
            ClassicAssert.AreEqual("ERR value is not a valid float", ex.Message);
            ClassicAssert.IsFalse(db.KeyExists("missing"));

            _ = db.StringSet("existing", "1.5");
            ex = ClassicAssert.Throws<RedisServerException>(() => db.Execute("INCRBYFLOAT", "existing", nan));
            ClassicAssert.AreEqual("ERR value is not a valid float", ex.Message);
            ClassicAssert.AreEqual("1.5", (string)db.StringGet("existing"));
        }

        /// <summary>
        /// INCRBYFLOAT stores digits that parse back to its result, in exactly the space it sizes for them: just below a power
        /// of ten, where a logarithm counts one digit too many, and at magnitudes up to the largest double.
        /// </summary>
        [Test]
        public void IncrementByFloatStoresExactDigits([Values] bool readOnlyRegion)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var store = server.Provider.StoreWrapper.store;

            foreach (var (stored, expected) in new[]
            {
                ("99.99999999999999", "99.99999999999999"),
                ("-99.99999999999999", "-99.99999999999999"),
                ("9999999999999998", "9999999999999998"),
                ("1e23", "99999999999999991611392"),
                ("1.7976931348623157e308", double.MaxValue.ToString("F0", CultureInfo.InvariantCulture)),
            })
            {
                // Created by the increment
                _ = db.KeyDelete("key");
                ClassicAssert.AreEqual(expected, (string)db.Execute("INCRBYFLOAT", "key", stored), stored);
                ClassicAssert.AreEqual(expected, (string)db.StringGet("key"), stored);

                // Updated by the increment, which parses the stored digits
                _ = db.StringSet("key", stored);
                if (readOnlyRegion)
                    store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                ClassicAssert.AreEqual(expected, (string)db.Execute("INCRBYFLOAT", "key", "0"), stored);
                ClassicAssert.AreEqual(expected, (string)db.Execute("INCRBYFLOAT", "key", "0"), stored);
                ClassicAssert.AreEqual(double.Parse(stored, CultureInfo.InvariantCulture), double.Parse((string)db.StringGet("key"), CultureInfo.InvariantCulture), stored);
            }
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
        public void DeleteExpiredObjectKeyReturnsZero()
        {
            // https://github.com/microsoft/garnet/issues/2244
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            db.ListRightPush(key, "listItem");
            db.KeyExpire(key, TimeSpan.FromMilliseconds(1));
            Thread.Sleep(50);

            var deleted = db.KeyDelete(key);
            ClassicAssert.IsFalse(deleted);
        }

        [Test]
        public void LlenOnExpiredStringKeyReturnsZero()
        {
            // https://github.com/microsoft/garnet/issues/2244: LLEN (object-store read) on an expired
            // string-typed key must report the key as missing (0), not WRONGTYPE.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            db.StringSet(key, "v", TimeSpan.FromMilliseconds(1));
            Thread.Sleep(50);

            var listLength = db.ListLength(key);
            ClassicAssert.AreEqual(0, listLength);
        }

        [Test]
        public void GetOnExpiredObjectKeyReturnsNil()
        {
            // Same root cause as the LLEN/DEL/SETNX bugs in #2244: a string-store read on an
            // expired object-typed key must report the key as missing, not WRONGTYPE.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            db.ListRightPush(key, "listItem");
            db.KeyExpire(key, TimeSpan.FromMilliseconds(1));
            Thread.Sleep(50);

            string result = db.StringGet(key);
            ClassicAssert.IsNull(result);
        }

        [Test]
        public void RPushOnExpiredStringKeyCreatesNewList()
        {
            // Same root cause as the LLEN/DEL/SETNX bugs in #2244: an object-store write on an
            // expired string-typed key must treat the key as missing and create a fresh value,
            // not report WRONGTYPE.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            db.StringSet(key, "oldStringValue");
            db.KeyExpire(key, TimeSpan.FromMilliseconds(1));
            Thread.Sleep(50);

            var newLength = db.ListRightPush(key, "listItem");
            ClassicAssert.AreEqual(1, newLength);

            var values = db.ListRange(key);
            ClassicAssert.AreEqual(1, values.Length);
            ClassicAssert.AreEqual("listItem", (string)values[0]);
        }

        [Test]
        public void ExpiryCommandOnExpiredKeyReturnsZero([Values(false, true)] bool isObject, [Values("EXPIRE", "PEXPIRE", "PERSIST")] string command)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            if (isObject)
                db.HashSet(key, "field", "value");
            else
                db.StringSet(key, "value");
            db.KeyExpire(key, TimeSpan.FromMilliseconds(1));
            Thread.Sleep(50);

            var reply = command == "PERSIST" ? db.Execute(command, key) : db.Execute(command, key, "100");
            ClassicAssert.AreEqual(ResultType.Integer, reply.Resp2Type);
            ClassicAssert.AreEqual(0, (int)reply);
            ClassicAssert.IsFalse(db.KeyExists(key));
        }

        [Test]
        public void SetBitOnExpiredKeyStartsFromZeroes()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // The expired value is as long as the bitmap SETBIT creates, so its record is reused without growing
            var key = "myKey";
            db.StringSet(key, "garnet-string", TimeSpan.FromMilliseconds(1));
            Thread.Sleep(50);

            ClassicAssert.IsFalse(db.StringSetBit(key, 100, true));
            var expected = new byte[13];
            expected[12] = 0x08;
            ClassicAssert.AreEqual(expected, (byte[])db.StringGet(key));
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

            db.Execute("SETWITHETAG", key, origValue);
            db.Execute("SETWITHETAG", newKey, "foo");

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

            db.Execute("SETWITHETAG", key, origValue);

            var result = db.KeyRename(key, newKey, When.NotExists);
            ClassicAssert.IsTrue(result);

            string retValue = db.StringGet(newKey);
            ClassicAssert.AreEqual(origValue, retValue);

            var oldKeyRes = db.StringGet(key);
            ClassicAssert.IsTrue(oldKeyRes.IsNull);
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
            ClassicAssert.AreEqual(-2, (long)ttl);

            db.StringSet(key, val);
            ttl = db.Execute("TTL", key);
            ClassicAssert.AreEqual(-1, (long)ttl);

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
        public void TtlRoundingTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // String key: SET + EXPIRE then immediately TTL should return the same value
            var key = "ttlRoundKey";
            var val = "value";
            var expire = 100;

            db.StringSet(key, val);
            db.KeyExpire(key, TimeSpan.FromSeconds(expire));
            var ttl = (long)db.Execute("TTL", key);
            // TTL should round to nearest second (matching Redis), not floor.
            // Allow 5s tolerance for network/scheduling delays.
            ClassicAssert.GreaterOrEqual(ttl, expire - 5, $"String TTL {ttl} too low (expected near {expire})");
            ClassicAssert.LessOrEqual(ttl, expire, $"String TTL {ttl} exceeds {expire}");

            // Object key: ZADD + EXPIRE then immediately TTL should return the same value
            var objKey = "ttlRoundObjKey";
            db.SortedSetAdd(objKey, "member", 1.0);
            db.KeyExpire(objKey, TimeSpan.FromSeconds(expire));
            ttl = (long)db.Execute("TTL", objKey);
            ClassicAssert.GreaterOrEqual(ttl, expire - 5, $"Object TTL {ttl} too low (expected near {expire})");
            ClassicAssert.LessOrEqual(ttl, expire, $"Object TTL {ttl} exceeds {expire}");
        }

        [Test]
        public void ObjectTTLTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "expireKey";
            var expire = 2;

            var ttl = db.Execute("TTL", key);
            ClassicAssert.AreEqual(-2, (long)ttl);

            db.SortedSetAdd(key, key, 1.0);
            ttl = db.Execute("TTL", key);
            ClassicAssert.AreEqual(-1, (long)ttl);

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
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
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

            var ttlSmall = command.Equals("EXPIRE") ? 50 : 50000; // 50 seconds or 50000 ms
            args[1] = ttlSmall;
            args[2] = testCaseSensitivity ? "xx" : "XX";// XX -- Set expiry only when the key has an existing expiry
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsTrue(resp);// XX return true existing expiry

            var time = db.KeyTimeToLive(key);
            ClassicAssert.Greater(time.Value.TotalSeconds, 0);
            ClassicAssert.LessOrEqual(time.Value.TotalSeconds, ttlSmall);

            args[1] = 1;
            args[2] = testCaseSensitivity ? "Gt" : "GT";// GT -- Set expiry only when the new expiry is greater than current one
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsFalse(resp); // GT return false new expiry < current expiry

            args[1] = command.Equals("EXPIRE") ? 1000 : 1000000; // 1000 seconds or 1000000 ms
            args[2] = testCaseSensitivity ? "gT" : "GT";// GT -- Set expiry only when the new expiry is greater than current one
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsTrue(resp); // GT return true new expiry > current expiry
            time = db.KeyTimeToLive(key);

            ClassicAssert.Greater(command.Equals("EXPIRE") ?
                    time.Value.TotalSeconds : time.Value.TotalMilliseconds, 500);

            args[1] = command.Equals("EXPIRE") ? 2000 : 2000000; // must be > GT value above
            args[2] = testCaseSensitivity ? "lt" : "LT";// LT -- Set expiry only when the new expiry is less than current one
            resp = (bool)db.Execute($"{command}", args);
            ClassicAssert.IsFalse(resp); // LT return false new expiry > current expiry

            args[1] = command.Equals("EXPIRE") ? 500 : 500000; // must be < GT value above
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

            // A brace in the rejected option used to reach string.Format a second time, because
            // AbortWithErrorMessage(string.Format(...)) bound to the params overload and re-formatted
            // the already-substituted text. That threw FormatException out of the handler and dropped
            // the connection instead of writing this error.
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute(command, "foo", "100", "{"));
                ClassicAssert.AreEqual("ERR Unsupported option {", exc.Message);
            }

            // The second option is reported when it is the one that is unsupported. Before this change
            // the first option was echoed back regardless of which one failed to parse.
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute(command, "foo", "100", "NX", "ZZ"));
                ClassicAssert.AreEqual("ERR Unsupported option ZZ", exc.Message);
            }

            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute(command, "foo", "100", "NX", "{"));
                ClassicAssert.AreEqual("ERR Unsupported option {", exc.Message);
            }
        }

        /// <summary>
        /// An expiry option pair that Garnet rejects must be answered by the error and nothing else,
        /// and must leave the key untouched. The trailing PING is what makes a second reply visible:
        /// LightClientRequest reads raw RESP, while a reply-counting client would absorb the extra
        /// reply and mis-associate every reply that follows it on the connection.
        /// </summary>
        [Test]
        public void KeyExpireIncompatibleOptionsSingleReplyTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            var incompatibleOptionsResponse = "-ERR NX and XX, GT or LT options at the same time are not compatible\r\n+PONG\r\n";
            var unsupportedOptionResponse = "-ERR Unsupported option ZZ\r\n+PONG\r\n";

            lightClientRequest.SendCommand("SET keyA valueA");

            // All four commands share the same option parsing, so all four must reject the pair identically.
            var response = lightClientRequest.SendCommands("EXPIRE keyA 100 NX XX", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(incompatibleOptionsResponse, response);

            response = lightClientRequest.SendCommands("PEXPIRE keyA 100000 NX XX", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(incompatibleOptionsResponse, response);

            response = lightClientRequest.SendCommands("EXPIREAT keyA 99999999999 NX XX", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(incompatibleOptionsResponse, response);

            response = lightClientRequest.SendCommands("PEXPIREAT keyA 99999999999000 NX XX", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(incompatibleOptionsResponse, response);

            response = lightClientRequest.SendCommands("EXPIRE keyA 100 GT LT", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(incompatibleOptionsResponse, response);

            // None of the rejected expiries may have been applied.
            response = lightClientRequest.SendCommand("TTL keyA");
            TestUtils.AssertEqualUpToExpectedLength(":-1\r\n", response);

            // A rejected expiry of 0 must not delete the key either.
            lightClientRequest.SendCommand("SET keyB valueB");

            response = lightClientRequest.SendCommands("EXPIRE keyB 0 NX XX", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(incompatibleOptionsResponse, response);

            response = lightClientRequest.SendCommand("EXISTS keyB");
            TestUtils.AssertEqualUpToExpectedLength(":1\r\n", response);

            // The unsupported option error must name the option that failed to parse, not the one before it.
            response = lightClientRequest.SendCommands("EXPIRE keyA 100 NX ZZ", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(unsupportedOptionResponse, response);

            // The accepted pairs and the sibling error paths keep replying exactly once.
            response = lightClientRequest.SendCommands("EXPIRE keyA 100 XX GT", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(":0\r\n+PONG\r\n", response);

            response = lightClientRequest.SendCommands("EXPIRE keyA 100 LT XX", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(":0\r\n+PONG\r\n", response);

            response = lightClientRequest.SendCommands("EXPIRE keyA 100 NX", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(":1\r\n+PONG\r\n", response);

            response = lightClientRequest.SendCommands("EXPIRE keyA 100 ZZ", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength(unsupportedOptionResponse, response);

            response = lightClientRequest.SendCommands("EXPIRE keyA -1 NX XX", "PING", 1, 1);
            TestUtils.AssertEqualUpToExpectedLength("-ERR invalid expire time, must be >= 0\r\n+PONG\r\n", response);
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

        [Test]
        public void KeyExpireOutOfRangeIsRejectedWithoutKillingSession()
        {
            const string Key = nameof(KeyExpireOutOfRangeIsRejectedWithoutKillingSession);

            var faulted = false;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                redis.ConnectionFailed += delegate { faulted = true; };

                var db = redis.GetDatabase();

                // Matches the exact repro from the reported issue: a large PEXPIRE on a key that does not exist
                ClassicAssert.IsFalse(db.KeyExists(Key));

                // milliseconds too far in the future
                var exc0 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("PEXPIRE", Key, "300000000000000"));
                ClassicAssert.AreEqual("ERR invalid expire time in 'pexpire' command", exc0.Message);

                // seconds too far in the future
                var exc1 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("EXPIRE", Key, "300000000000"));
                ClassicAssert.AreEqual("ERR invalid expire time in 'expire' command", exc1.Message);

                // unix time (seconds) too far in the future
                var exc2 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("EXPIREAT", Key, long.MaxValue.ToString()));
                ClassicAssert.AreEqual("ERR invalid expire time in 'expireat' command", exc2.Message);

                // unix time (ms) too far in the future
                var exc3 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("PEXPIREAT", Key, long.MaxValue.ToString()));
                ClassicAssert.AreEqual("ERR invalid expire time in 'pexpireat' command", exc3.Message);

                ClassicAssert.IsFalse(db.KeyExists(Key));
            }

            // Connection never faulted during operation - previously these all closed the connection with no reply
            ClassicAssert.IsFalse(faulted);
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
                await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);

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
                await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);

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
                await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);

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
            ClassicAssert.AreEqual(value.Substring(value.Length - 4, 3), resp);

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
            ClassicAssert.AreEqual("0", resp);

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

        /// <summary>
        /// GETRANGE on a key holding an empty string returns an empty string for any range.
        /// </summary>
        [Test]
        public void GetRangeEmptyValueTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "emptyRangeKey";
            ClassicAssert.IsTrue(db.StringSet(key, ""));

            var resp = (string)db.StringGetRange(key, -1, -1);
            ClassicAssert.AreEqual(string.Empty, resp);

            resp = (string)db.StringGetRange(key, -1, 0);
            ClassicAssert.AreEqual(string.Empty, resp);

            resp = (string)db.StringGetRange(key, 0, 0);
            ClassicAssert.AreEqual(string.Empty, resp);

            resp = (string)db.StringGetRange(key, 0, -1);
            ClassicAssert.AreEqual(string.Empty, resp);
        }

        /// <summary>
        /// GETRANGE offsets count back from the end when negative and are clamped to the value, never wrapped, for every
        /// combination of in-range, negative and out-of-range offsets.
        /// </summary>
        [Test]
        public void GetRangeClampsOffsetsToTheValue()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const string key = "rangeKey";
            const string value = "0123456789";
            _ = db.StringSet(key, value);

            int[] offsets = [.. Enumerable.Range(-15, 28), int.MinValue, int.MinValue + 1, int.MaxValue];
            foreach (var start in offsets)
            {
                foreach (var end in offsets)
                {
                    var actual = (string)db.StringGetRange(key, start, end);
                    ClassicAssert.AreEqual(ExpectedRange(value, start, end), actual, $"GETRANGE {start} {end}");
                }
            }

            static string ExpectedRange(string s, long start, long end)
            {
                long len = s.Length;
                if (start < 0 && end < 0 && start > end)
                    return string.Empty;
                if (start < 0)
                    start = len + start;
                if (end < 0)
                    end = len + end;
                if (start < 0)
                    start = 0;
                if (end < 0)
                    end = 0;
                if (end >= len)
                    end = len - 1;
                return start > end || len == 0 ? string.Empty : s.Substring((int)start, (int)(end - start + 1));
            }
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

            // offset + value length beyond the max record size -> RedisServerException
            // ("ERR string exceeds maximum allowed size (proto-max-bulk-len)"),
            // instead of the storage layer throwing and the connection being dropped
            ClassicAssert.IsTrue(db.KeyDelete(key));
            ex = Assert.Throws<RedisServerException>(() => db.StringSetRange(key, 600_000_000, newValue));
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_STRING_EXCEEDS_MAX_SIZE), ex.Message);

            // the connection must still be usable after the rejected command above
            ClassicAssert.IsTrue(db.StringSet(key, value));
            resp = db.StringGet(key);
            ClassicAssert.AreEqual(value, resp);
            ClassicAssert.IsTrue(db.KeyDelete(key));
        }

        /// <summary>
        /// APPEND rejects a result longer than the maximum string size, as SETRANGE does, and leaves the value unchanged.
        /// </summary>
        [Test]
        public void AppendBeyondMaxSizeIsRejected()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Build a value of exactly the maximum size on the server, without sending it over the wire
            var key = "maxSized";
            var maxSize = BitmapManager.MaxBitmapPayloadBytes;
            ClassicAssert.AreEqual(maxSize, (long)db.Execute("SETRANGE", key, maxSize - 1, "x"));

            var ex = Assert.Throws<RedisServerException>(() => db.StringAppend(key, "y"));
            ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_STRING_EXCEEDS_MAX_SIZE), ex.Message);
            ClassicAssert.AreEqual(maxSize, db.StringLength(key));

            // Appending nothing does not grow the value, so it is allowed
            ClassicAssert.AreEqual(maxSize, db.StringAppend(key, ""));
            ClassicAssert.IsTrue(db.KeyDelete(key));
        }

        /// <summary>
        /// APPEND to an expired key starts a new value, whether the expired record is updated in place or in the read-only region.
        /// </summary>
        [Test]
        public void AppendToExpiredKeyStartsNewValue([Values] bool readOnlyRegion)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var store = server.Provider.StoreWrapper.store;

            _ = db.StringSet("key", "old", TimeSpan.FromMilliseconds(10));
            if (readOnlyRegion)
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            Thread.Sleep(100);

            ClassicAssert.AreEqual(3, db.StringAppend("key", "new"));
            ClassicAssert.AreEqual("new", (string)db.StringGet("key"));
            ClassicAssert.IsNull(db.KeyTimeToLive("key"));
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

            db.StringSet(keyA, keyA, null, keepTtl: true);
            var time = db.KeyTimeToLive(keyA);
            ClassicAssert.IsTrue(time.Value.Ticks > 0);

            db.StringSet(keyB, keyB, null, keepTtl: false);
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
        public void SetKeepTtlLargerValueCopyUpdateTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var key = "keepTtlKey";
            var newValue = new string('x', 300);

            ClassicAssert.AreEqual("OK", db.Execute("SET", key, "value1", "EX", "100")?.ToString());
            ClassicAssert.AreEqual("OK", db.Execute("SET", key, newValue, "KEEPTTL")?.ToString());

            ClassicAssert.IsTrue(db.KeyExists(key));
            var ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsNotNull(ttl);
            ClassicAssert.IsTrue(ttl.Value.TotalSeconds > 90 && ttl.Value.TotalSeconds <= 100, $"Unexpected TTL {ttl}");
            ClassicAssert.AreEqual(newValue, (string)db.StringGet(key));
        }

        [Test]
        public void SetExNxOnExpiredRecordWithLargerValueTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            const int baseLength = 100;
            const int maxGrowth = 24;

            for (var growth = 0; growth <= maxGrowth; growth++)
                ClassicAssert.AreEqual("OK", db.Execute("SET", $"expNx{growth}", new string('a', baseLength), "PX", "100")?.ToString());

            Thread.Sleep(500);

            // The new value may fit the expired record without the expiration but not with it; the write must still be stored
            for (var growth = 0; growth <= maxGrowth; growth++)
            {
                var newValue = new string('b', baseLength + growth);
                ClassicAssert.AreEqual("OK", db.Execute("SET", $"expNx{growth}", newValue, "EX", "3600", "NX")?.ToString());
                ClassicAssert.AreEqual(newValue, (string)db.StringGet($"expNx{growth}"), $"Value lost with growth {growth}");
                ClassicAssert.IsNotNull(db.KeyTimeToLive($"expNx{growth}"));
            }
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
            ClassicAssert.AreEqual(-2, (long)pttl);

            db.StringSet(key, val);
            pttl = db.Execute("PTTL", key);
            ClassicAssert.AreEqual(-1, (long)pttl);

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

            var response = await c.ExecuteAsync("HELLO", respVersion.ToString()).ConfigureAwait(false);

            response = await c.ExecuteAsync("GET", "nx").ConfigureAwait(false);
            ClassicAssert.AreEqual(expectedResponse, response);
            response = await c.ExecuteAsync("GEODIST", "nx", "foo", "bar").ConfigureAwait(false);
            ClassicAssert.AreEqual(expectedResponse, response);
            response = await c.ExecuteAsync("HGET", "nx", "nx").ConfigureAwait(false);
            ClassicAssert.AreEqual(expectedResponse, response);
            response = await c.ExecuteAsync("LPOP", "nx").ConfigureAwait(false);
            ClassicAssert.AreEqual(expectedResponse, response);
            response = await c.ExecuteAsync("LPOS", "nx", "foo").ConfigureAwait(false);
            ClassicAssert.AreEqual(expectedResponse, response);
            response = await c.ExecuteAsync("SPOP", "nx").ConfigureAwait(false);
            ClassicAssert.AreEqual(expectedResponse, response);
            response = await c.ExecuteAsync("ZSCORE", "nx", "foo").ConfigureAwait(false);
            ClassicAssert.AreEqual(expectedResponse, response);

            response = await c.ExecuteAsync("BITFIELD", "bf", "OVERFLOW", "FAIL", "INCRBY", "u1", "1", "1").ConfigureAwait(false);
            ClassicAssert.AreEqual("*1\r\n:1\r\n", response);
            response = await c.ExecuteAsync("BITFIELD", "bf", "OVERFLOW", "FAIL", "INCRBY", "u1", "1", "1").ConfigureAwait(false);
            ClassicAssert.AreEqual("*1\r\n" + expectedResponse, response);

            response = await c.ExecuteAsync("SADD", "set", "foo", "bar").ConfigureAwait(false);
            ClassicAssert.AreEqual(":2\r\n", response);
            response = await c.ExecuteAsync("SMEMBERS", "set").ConfigureAwait(false);
            ClassicAssert.AreEqual(setPrefix + "2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", response);

            response = await c.ExecuteAsync("MSET", "s1", "foo", "s2", "bar").ConfigureAwait(false);
            ClassicAssert.AreEqual("+OK\r\n", response);
            response = await c.ExecuteAsync("LCS", "s1", "s2", "IDX").ConfigureAwait(false);
            ClassicAssert.AreEqual(mapPrefix + "\r\n$7\r\nmatches\r\n*0\r\n$3\r\nlen\r\n:0\r\n", response);

            response = await c.ExecuteAsync("ZADD", "z", "0", "a", "1", "b").ConfigureAwait(false);
            ClassicAssert.AreEqual(":2\r\n", response);
            response = await c.ExecuteAsync("ZSCORE", "z", "a").ConfigureAwait(false);
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

                var keyCount = 50;
                var valLen = 1024;
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
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol, allowAdmin: true));
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
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
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
            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var mainDB = mainConnection.GetDatabase(0);
            var mainId = (long)mainDB.Execute("CLIENT", "ID");

            // Kill old style (by remote endpoint)
            {
                using var targetConnection = await ConnectAsync().ConfigureAwait(false);

                var targetId = await targetConnection.ExecuteForLongResultAsync("CLIENT", ["ID"]).ConfigureAwait(false);

                var remoteEndpoint = GetFlagForSessionId(targetId, "addr", mainDB);

                // Kill acknowledged
                var res = mainDB.Execute("CLIENT", "KILL", remoteEndpoint);
                ClassicAssert.AreEqual("OK", (string)res);

                AssertNotConnected(targetConnection);
            }

            // Kill by id
            {
                using var targetConnection = await ConnectAsync().ConfigureAwait(false);

                var targetId = await targetConnection.ExecuteForLongResultAsync("CLIENT", ["ID"]).ConfigureAwait(false);

                // Count of killed connections
                var res = mainDB.Execute("CLIENT", "KILL", "ID", targetId);
                ClassicAssert.AreEqual(1, (int)res);

                AssertNotConnected(targetConnection);
            }

            // Kill by type = NORMAL
            {
                using var targetConnection = await ConnectAsync().ConfigureAwait(false);

                // Count of killed connections
                var res = mainDB.Execute("CLIENT", "KILL", "TYPE", "NORMAL");

                // SE.Redis spins up multiple connections, so we can kill more than 1 (but at least 1) connection
                ClassicAssert.IsTrue((int)res >= 1);

                AssertNotConnected(targetConnection);
            }

            // Kill by type = PUBSUB
            {
                using var targetConnection = await ConnectAsync().ConfigureAwait(false);

                _ = await targetConnection.ExecuteForStringResultAsync("SUBSCRIBE", ["foo"]).ConfigureAwait(false);

                // Count of killed connections
                var res = mainDB.Execute("CLIENT", "KILL", "TYPE", "PUBSUB");

                // SE.Redis spins up multiple connections, so we can kill more than 1 (but at least 1) connection
                ClassicAssert.IsTrue((int)res >= 1);

                AssertNotConnected(targetConnection);
            }

            // KILL by addr
            {
                using var targetConnection = await ConnectAsync().ConfigureAwait(false);

                var targetId = await targetConnection.ExecuteForLongResultAsync("CLIENT", ["ID"]).ConfigureAwait(false);

                var remoteEndpoint = GetFlagForSessionId(targetId, "addr", mainDB);

                // Count of killed connections
                var res = mainDB.Execute("CLIENT", "KILL", "ADDR", remoteEndpoint);
                ClassicAssert.AreEqual(1, (int)res);

                AssertNotConnected(targetConnection);
            }

            // KILL by laddr
            {
                using var targetConnection = await ConnectAsync().ConfigureAwait(false);

                var targetId = await targetConnection.ExecuteForLongResultAsync("CLIENT", ["ID"]).ConfigureAwait(false);

                var localEndpoint = GetFlagForSessionId(targetId, "laddr", mainDB);

                // Count of killed connections
                var res = mainDB.Execute("CLIENT", "KILL", "LADDR", localEndpoint);

                // SE.Redis spins up multiple connections, so we can kill more than 1 (but at least 1) connection
                ClassicAssert.IsTrue((int)res >= 1);

                AssertNotConnected(targetConnection);
            }

            // KILL by maxage
            {
                using var targetConnection = await ConnectAsync().ConfigureAwait(false);

                var targetId = await targetConnection.ExecuteForLongResultAsync("CLIENT", ["ID"]).ConfigureAwait(false);

                while (true)
                {
                    var age = GetFlagForSessionId(targetId, "age", mainDB);
                    if (long.Parse(age) >= 2)
                    {
                        break;
                    }

                    await Task.Delay(1_000).ConfigureAwait(false);
                }

                // Count of killed connections
                var res = mainDB.Execute("CLIENT", "KILL", "MAXAGE", 1);
                ClassicAssert.IsTrue((int)res >= 1);

                AssertNotConnected(targetConnection);
            }

            // KILL by multiple
            {
                using var targetConnection = await ConnectAsync().ConfigureAwait(false);

                var targetId = await targetConnection.ExecuteForLongResultAsync("CLIENT", ["ID"]).ConfigureAwait(false);

                var addr = GetFlagForSessionId(targetId, "addr", mainDB);

                // Count of killed connections
                var res = mainDB.Execute("CLIENT", "KILL", "ID", targetId, "MAXAGE", -1, "TYPE", "NORMAL", "SKIPME", "YES");
                ClassicAssert.AreEqual(1, (int)res);

                AssertNotConnected(targetConnection);
            }

            // KILL without SKIPME
            {
                using var targetConnection = await ConnectAsync().ConfigureAwait(false);

                var targetId = await targetConnection.ExecuteForLongResultAsync("CLIENT", ["ID"]).ConfigureAwait(false);

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
                await client.ConnectAsync().ConfigureAwait(false);

                _ = await client.PingAsync().ConfigureAwait(false);

                ClassicAssert.IsTrue(client.IsConnected);

                return client;
            }

            // Check that we really killed the connection backing a GarnetClient
            static void AssertNotConnected(GarnetClient client)
            {
                // IsConnected reads Socket.Connected, which reports the state as of the last completed
                // I/O. Once the server closes the connection the first send still succeeds locally and
                // only a following one observes the reset, so a single ping cannot establish that the
                // connection is gone. Keep issuing pings until the disconnect surfaces; a connection
                // that was not killed keeps answering them and still fails the assert below.
                var elapsed = Stopwatch.StartNew();
                while (client.IsConnected && elapsed.Elapsed < TimeSpan.FromSeconds(10))
                {
                    try
                    {
                        client.Ping(static (_, __) => { });
                    }
                    catch
                    {
                        break;
                    }

                    Thread.Sleep(10);
                }

                ClassicAssert.IsFalse(client.IsConnected);
            }
        }

        [Test]
        public void ClientKillErrors()
        {
            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
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
                db.StringSet(key, value, initialTimeSpan, keepTtl: false);
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
                db.StringSet(key, value, initialTimeSpan, keepTtl: false);
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
                db.StringSet(key, value, initialTimeSpan, keepTtl: false);
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

        [Test]
        public void GetExpiryOutOfRangeIsRejectedWithoutKillingSession()
        {
            const string Key = nameof(GetExpiryOutOfRangeIsRejectedWithoutKillingSession);

            var faulted = false;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                redis.ConnectionFailed += delegate { faulted = true; };

                var db = redis.GetDatabase();

                ClassicAssert.True(db.StringSet(Key, "Value"));

                // seconds too far in the future
                var exc0 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("GETEX", Key, "EX", long.MaxValue.ToString()));
                ClassicAssert.AreEqual("ERR invalid expire time in 'getex' command", exc0.Message);

                // milliseconds too far in the future
                var exc1 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("GETEX", Key, "PX", long.MaxValue.ToString()));
                ClassicAssert.AreEqual("ERR invalid expire time in 'getex' command", exc1.Message);

                // unix time too far in the future
                var exc2 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("GETEX", Key, "EXAT", long.MaxValue.ToString()));
                ClassicAssert.AreEqual("ERR invalid expire time in 'getex' command", exc2.Message);

                // unix time ms too far in the future
                var exc3 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("GETEX", Key, "PXAT", long.MaxValue.ToString()));
                ClassicAssert.AreEqual("ERR invalid expire time in 'getex' command", exc3.Message);

                var ticksSpace = long.MaxValue - DateTime.UtcNow.Ticks;
                var overflowSeconds = (ticksSpace / TimeSpan.TicksPerSecond) + 1;
                var overflowMilliseconds = (ticksSpace / TimeSpan.TicksPerMillisecond) + 1;

                // seconds overflow
                var exc4 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("GETEX", Key, "EX", overflowSeconds.ToString()));
                ClassicAssert.AreEqual("ERR expire time overflows date in 'getex' command", exc4.Message);

                // milliseconds overflow
                var exc5 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("GETEX", Key, "PX", overflowMilliseconds.ToString()));
                ClassicAssert.AreEqual("ERR expire time overflows date in 'getex' command", exc5.Message);

                // TTL unchanged and key undeleted
                var ttl = db.KeyTimeToLive(Key);
                ClassicAssert.IsNull(ttl);

                ClassicAssert.IsTrue(db.KeyExists(Key));
            }

            // Connection never faulted during operation
            ClassicAssert.IsFalse(faulted);
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

        [Test]
        public void SetNXOnExpiredObjectKeySucceeds()
        {
            // https://github.com/microsoft/garnet/issues/2244: an expired key must be treated as
            // non-existent, even when its stored type differs from the command being issued.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "myKey";
            var newValue = "myNewValue";

            db.ListRightPush(key, "listItem");
            db.KeyExpire(key, TimeSpan.FromMilliseconds(1));
            Thread.Sleep(50);

            var setNxResult = (long)db.Execute("SETNX", key, newValue);
            ClassicAssert.AreEqual(1, setNxResult);

            string result = db.StringGet(key);
            ClassicAssert.AreEqual(newValue, result);
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
            await Task.Delay(1000).ConfigureAwait(false);

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
        [TestCase("BLMPOP 10 1 keyA LEFT", Description = "BLMPOP replies to CLIENT UNBLOCK ERROR with the error only")]
        [TestCase("BLPOP keyA 10", Description = "BLPOP replies to CLIENT UNBLOCK ERROR with the error only")]
        [TestCase("BLMOVE keyA keyB LEFT LEFT 10", Description = "BLMOVE replies to CLIENT UNBLOCK ERROR with the error only")]
        [TestCase("BZMPOP 10 1 keyA MIN", Description = "BZMPOP replies to CLIENT UNBLOCK ERROR with the error only")]
        public async Task ClientUnblockWithErrorWritesSingleReplyTest(string blockingCommand)
        {
            var expectedError = "-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n";
            var expectedPong = "+PONG\r\n";

            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var mainDB = mainConnection.GetDatabase(0);

            // Start blocking client
            using var blockingClient = TestUtils.CreateRequest();
            var clientIdResponse = Encoding.ASCII.GetString(blockingClient.SendCommand("CLIENT ID"));
            var clientId = clientIdResponse.Substring(1, clientIdResponse.IndexOf("\r\n") - 1);

            string blockingResponse = null;
            var blockingTask = taskFactory.StartNew(() =>
            {
                var response = blockingClient.SendCommand(blockingCommand);
                blockingResponse = Encoding.ASCII.GetString(response, 0, expectedError.Length);
            });

            // Wait for client to enter blocking state
            await Task.Delay(1000).ConfigureAwait(false);

            // Unblock from main connection
            var unblockResult = (int)mainDB.Execute("CLIENT", "UNBLOCK", clientId, "ERROR");
            ClassicAssert.AreEqual(1, unblockResult);
            blockingTask.Wait();

            ClassicAssert.AreEqual(expectedError, blockingResponse);

            // The error is the only reply the blocking command may write - a second reply (a null)
            // would shift every subsequent reply on this connection by one
            var pingResponse = Encoding.ASCII.GetString(blockingClient.SendCommand("PING"), 0, expectedPong.Length);
            ClassicAssert.AreEqual(expectedPong, pingResponse, $"Expected PING to be answered with +PONG after unblocking {blockingCommand}");
        }

        [Test]
        [TestCase(0, Description = "Guarantied concurrent unblock test")]
        [TestCase(3, Description = "Random chance unblock sucess/failed case")]
        [TestCase(20, Description = "Probable unblock failed case")]
        public async Task MultipleClientsUnblockAndAddTest(int numberOfItems)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
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
            await Task.Delay(1000).ConfigureAwait(false);

            // Start parallel unblock and add tasks.
            // These use the asynchronous StackExchange.Redis APIs deliberately: the synchronous overloads block a
            // thread pool thread for the duration of the round trip, and issuing numberOfItems + 3 of them at once
            // exhausts the pool on a 2-core machine. The pool then injects replacement threads at roughly one per
            // second, so the commands reach the server several seconds late - after the blocking command has already
            // timed out - and the concurrency this test exists to exercise never actually happens.
            var unblockTasks = new List<Task<RedisResult>>();
            var addTasks = new List<Task>();
            for (int i = 0; i < numberOfItems; i++)
            {
                var _i = i;
                addTasks.Add(db.ListLeftPushAsync(key, $"{value}{_i}"));
            }

            for (int i = 0; i < 3; i++)
            {
                unblockTasks.Add(db.ExecuteAsync("CLIENT", "UNBLOCK", clientId, "ERROR"));
            }

            await Task.WhenAll(unblockTasks).ConfigureAwait(false);
            await Task.WhenAll(addTasks).ConfigureAwait(false);
            await blockingTask.ConfigureAwait(false);

            var numberOfItemsReturned = Regex.Matches(blockingResult, value).Count;

            var listLength = db.ListLength(key);
            ClassicAssert.AreEqual(numberOfItems - numberOfItemsReturned, listLength);

            if (numberOfItemsReturned == 0)
            {
                ClassicAssert.IsTrue(blockingResult.StartsWith("-UNBLOCKED"));
                ClassicAssert.IsTrue(unblockTasks.Any(x => (int)x.Result == 1));
            }
            else
            {
                ClassicAssert.IsTrue(unblockTasks.All(x => (int)x.Result == 0));
            }
        }

        /// <summary>
        /// The broker's first attempt on behalf of a new observer pops the item out of the list and only then
        /// hands it to the observer. CLIENT UNBLOCK targets the same observer as soon as it is registered, which
        /// is before that attempt runs. Unless the attempt and the hand-off are atomic with respect to the
        /// unblock, the unblock can claim the observer in between: the client is told it was unblocked and the
        /// item, already popped, belongs to nobody.
        /// <para>
        /// Two invariants are checked per round. The item is either handed to the popping client or still in
        /// the list, never neither; and the client gets exactly one reply for its BLPOP. A second reply
        /// desynchronizes the connection, so from then on every command reads the previous command's reply.
        /// PING is the probe for that -- its reply can only land where expected if BLPOP wrote exactly one.
        /// </para>
        /// <para>
        /// Raw sockets rather than StackExchange.Redis: it multiplexes, so its keep-alive cannot get past an
        /// in-flight blocking command on the same connection, and on a machine short of cores its watchdog
        /// tears the connection down. That failure arrives as a parse error or a dead socket, which masks the
        /// invariants under test rather than reporting them.
        /// </para>
        /// <para>
        /// The race it guards needs a completion to land inside a few instructions on the receive path, so it
        /// is reproduced by starving the server of cores:
        /// <c>DOTNET_PROCESSOR_COUNT=2 taskset -c 0,1 dotnet test ... --filter
        /// FullyQualifiedName~ClientUnblockRacingAPopNeverLoses</c>. Against the defect this fails within a
        /// handful of runs; with it fixed it stays green.
        /// </para>
        /// </summary>
        [Test]
        public void ClientUnblockRacingAPopNeverLosesTheItemAndWritesExactlyOneReply()
        {
            const int Workers = 8;
            const int RoundsPerWorker = 500;
            const string ValuePrefix = "unblockRaceValue";

            const string Pong = "+PONG\r\n";
            const string Unblocked = "-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n";

            var blockers = new Socket[Workers];
            var admins = new Socket[Workers];
            var clientIds = new string[Workers];
            for (var i = 0; i < Workers; i++)
            {
                blockers[i] = RawConnect();
                admins[i] = RawConnect();
                RawSend(blockers[i], "CLIENT", "ID");
                clientIds[i] = RawReadUntil(blockers[i], "\r\n")[1..^2];
            }

            var failures = new ConcurrentQueue<string>();
            var delivered = 0;
            var stop = false;

            try
            {
                Parallel.For(0, Workers, worker =>
                {
                    var key = $"unblockReplyCountKey:{worker}";
                    var blocker = blockers[worker];
                    var admin = admins[worker];
                    var clientId = clientIds[worker];

                    for (var round = 0; round < RoundsPerWorker; round++)
                    {
                        if (Volatile.Read(ref stop))
                            return;

                        // A per-round value makes a reply from an earlier round distinguishable from this round's.
                        var value = $"{ValuePrefix}{round}";
                        var popped = $"*2\r\n${key.Length}\r\n{key}\r\n${value.Length}\r\n{value}\r\n";
                        RawSend(admin, "RPUSH", key, value);
                        if (RawReadUntil(admin, "\r\n") != ":1\r\n")
                        {
                            failures.Enqueue($"worker {worker} round {round}: RPUSH did not return 1");
                            Volatile.Write(ref stop, true);
                            return;
                        }

                        // The item is already there, so the pop resolves on the broker's first attempt,
                        // which is what the unblock races.
                        RawSend(blocker, "BLPOP", key, "10");
                        RawSend(admin, "CLIENT", "UNBLOCK", clientId, "ERROR");
                        _ = RawReadUntil(admin, "\r\n");

                        RawSend(blocker, "PING");
                        var got = RawReadUntil(blocker, Pong);

                        var blpop = got[..^Pong.Length];
                        if (blpop != Unblocked && blpop != popped)
                        {
                            failures.Enqueue($"worker {worker} round {round}: expected exactly one BLPOP reply before the PONG, got {Escape(got)}");
                            Volatile.Write(ref stop, true);
                            return;
                        }

                        // The item was either handed to the popping client or is still in the list. Never neither.
                        var gotItem = blpop == popped;
                        RawSend(admin, "LLEN", key);
                        var remaining = RawReadUntil(admin, "\r\n");
                        var expected = gotItem ? ":0\r\n" : ":1\r\n";
                        if (remaining != expected)
                        {
                            failures.Enqueue($"worker {worker} round {round}: BLPOP {(gotItem ? "returned" : "did not return")} the item, LLEN was {Escape(remaining)}");
                            Volatile.Write(ref stop, true);
                            return;
                        }

                        if (gotItem)
                        {
                            _ = Interlocked.Increment(ref delivered);
                        }
                        else
                        {
                            RawSend(admin, "DEL", key);
                            _ = RawReadUntil(admin, "\r\n");
                        }
                    }
                });
            }
            finally
            {
                for (var i = 0; i < Workers; i++)
                {
                    blockers[i].Dispose();
                    admins[i].Dispose();
                }
            }

            ClassicAssert.IsEmpty(failures, string.Join(" ;; ", failures.Take(4)));

            // Guards against the race never being set up at all, e.g. if the unblock always arrived first.
            ClassicAssert.Greater(delivered, 0, "No round delivered the item, so the pop path was never exercised.");
        }

        static Socket RawConnect()
        {
            var s = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true, ReceiveTimeout = 30_000 };
            s.Connect(TestUtils.EndPoint);
            return s;
        }

        static void RawSend(Socket s, params string[] parts)
        {
            var sb = new StringBuilder();
            _ = sb.Append('*').Append(parts.Length).Append("\r\n");
            foreach (var p in parts)
                _ = sb.Append('$').Append(p.Length).Append("\r\n").Append(p).Append("\r\n");
            var bytes = Encoding.ASCII.GetBytes(sb.ToString());
            var sent = 0;
            while (sent < bytes.Length)
                sent += s.Send(bytes, sent, bytes.Length - sent, SocketFlags.None);
        }

        /// <summary>
        /// Reads until the accumulated response ends with <paramref name="suffix"/>, so that anything the
        /// server wrote before it is kept and can be reported.
        /// </summary>
        static string RawReadUntil(Socket s, string suffix)
        {
            var sb = new StringBuilder();
            var buf = new byte[8 * 1024];
            while (sb.Length < 64 * 1024)
            {
                int n;
                try
                {
                    n = s.Receive(buf);
                }
                catch (SocketException e)
                {
                    throw new Exception($"connection dropped after {Escape(sb.ToString())}: {e.SocketErrorCode}");
                }

                if (n == 0)
                    throw new Exception($"connection closed after {Escape(sb.ToString())}");

                _ = sb.Append(Encoding.ASCII.GetString(buf, 0, n));
                if (sb.Length >= suffix.Length && sb.ToString().EndsWith(suffix, StringComparison.Ordinal))
                    return sb.ToString();
            }

            throw new Exception($"never saw the end of the reply: {Escape(sb.ToString())}");
        }

        static string Escape(string s) => s.Replace("\r", "\\r").Replace("\n", "\\n");

        [Test]
        [TestCase(-1, Description = "Unblock with invalid client ID")]
        [TestCase(999999, Description = "Unblock with non-existent client ID")]
        public void ClientUnblockInvalidIdTest(int invalidId)
        {
            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var mainDB = mainConnection.GetDatabase(0);

            var unblockResult = (int)mainDB.Execute("CLIENT", "UNBLOCK", invalidId);
            ClassicAssert.AreEqual(0, unblockResult);
        }

        [Test]
        public void ClientUnblockInvalidModeTest()
        {
            using var mainConnection = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var mainDB = mainConnection.GetDatabase(0);

            Assert.Throws<RedisServerException>(() => mainDB.Execute("CLIENT", "UNBLOCK", 123, "INVALID"));
        }

        [Test]
        public void ExcessiveArgumentCountRejected()
        {
            // Send a RESP array header with an argument count exceeding the maximum allowed,
            // followed by a command name so parsing proceeds far enough to hit the guard.
            // The server should reject the command with a protocol error and close the connection.
            using var socket = new System.Net.Sockets.Socket(
                System.Net.Sockets.AddressFamily.InterNetwork,
                System.Net.Sockets.SocketType.Stream,
                System.Net.Sockets.ProtocolType.Tcp);
            socket.ReceiveTimeout = 5000;
            socket.Connect(System.Net.IPAddress.Loopback, TestUtils.TestPort);

            // *2000000\r\n$3\r\nFOO\r\n — array with 2M elements, command name "FOO"
            // The command will be identified as unknown but count (1999999) exceeds MaxParams (1,048,576)
            var payload = Encoding.ASCII.GetBytes("*2000000\r\n$3\r\nFOO\r\n");
            socket.Send(payload);

            // Read the response - server should send an error and close the connection
            var buffer = new byte[4096];
            var totalRead = 0;
            try
            {
                while (totalRead < buffer.Length)
                {
                    var bytesRead = socket.Receive(buffer, totalRead, buffer.Length - totalRead, System.Net.Sockets.SocketFlags.None);
                    if (bytesRead == 0) break;
                    totalRead += bytesRead;
                }
            }
            catch (System.Net.Sockets.SocketException)
            {
                // Connection may be reset by server - this is expected
            }

            var response = Encoding.ASCII.GetString(buffer, 0, totalRead);
            var expectedResponse =
                "-ERR unknown command\r\n" +
                "-ERR Protocol Error: RESP array argument count '1999999' exceeds maximum allowed count of '1048576'.\r\n";
            ClassicAssert.AreEqual(expectedResponse, response);

            // Same test with a known command (PING) — should still be rejected due to excessive count
            using var socket2 = new System.Net.Sockets.Socket(
                System.Net.Sockets.AddressFamily.InterNetwork,
                System.Net.Sockets.SocketType.Stream,
                System.Net.Sockets.ProtocolType.Tcp);
            socket2.ReceiveTimeout = 5000;
            socket2.Connect(System.Net.IPAddress.Loopback, TestUtils.TestPort);

            // *2000000\r\n$4\r\nPING\r\n — known command but excessive count
            var payload2 = Encoding.ASCII.GetBytes("*2000000\r\n$4\r\nPING\r\n");
            socket2.Send(payload2);

            var buffer2 = new byte[4096];
            var totalRead2 = 0;
            try
            {
                while (totalRead2 < buffer2.Length)
                {
                    var bytesRead = socket2.Receive(buffer2, totalRead2, buffer2.Length - totalRead2, System.Net.Sockets.SocketFlags.None);
                    if (bytesRead == 0) break;
                    totalRead2 += bytesRead;
                }
            }
            catch (System.Net.Sockets.SocketException)
            {
                // Connection may be reset by server - this is expected
            }

            var response2 = Encoding.ASCII.GetString(buffer2, 0, totalRead2);
            var expectedResponse2 =
                "-ERR Protocol Error: RESP array argument count '1999999' exceeds maximum allowed count of '1048576'.\r\n";
            ClassicAssert.AreEqual(expectedResponse2, response2);

            // Verify the server is still operational after rejecting the malicious connection
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.StringSet("testkey", "testvalue");
            ClassicAssert.AreEqual("testvalue", db.StringGet("testkey").ToString());
        }
    }
}