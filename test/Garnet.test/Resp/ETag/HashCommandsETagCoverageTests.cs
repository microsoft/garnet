// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ETag
{
    [TestFixture]
    public class HashCommandsETagCoverageTests : EtagCoverageTestsBase
    {
        static readonly RedisKey[] HashKeys = [KeyWithEtag, "hKey2", "hKey3"];

        static readonly HashEntry[][] HashData =
        [
            [new HashEntry("a1", 1), new HashEntry("a2", 1.2), new HashEntry("a3", "v1.3")],
            [new HashEntry("b1", "v2.1"), new HashEntry("b2", "v2.2")],
            [new HashEntry("c1", "v3.1")]
        ];

        [Test]
        public async Task HCollectETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0] };
            await CheckCommandsAsync(RespCommand.HCOLLECT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task HDelETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], HashData[0][0].Name };
            await CheckCommandsAsync(RespCommand.HDEL, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task HExpireETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], 2, "FIELDS", 1, HashData[0][0].Name };
            await CheckCommandsAsync(RespCommand.HEXPIRE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task HExpireAtETagAdvancedTestAsync()
        {
            var expireAt = DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeSeconds();
            var cmdArgs = new object[] { HashKeys[0], expireAt, "FIELDS", 1, HashData[0][0].Name };
            await CheckCommandsAsync(RespCommand.HEXPIREAT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task HIncrByETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], HashData[0][0].Name, 2 };
            await CheckCommandsAsync(RespCommand.HINCRBY, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(long.Parse(HashData[0][0].Value) + 2 , (long)result);
            }
        }

        [Test]
        public async Task HIncrByFloatETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], HashData[0][1].Name, 2.2 };
            await CheckCommandsAsync(RespCommand.HINCRBYFLOAT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(double.Parse(HashData[0][1].Value) + 2.2, (double)result);
            }
        }

        [Test]
        public async Task HPExpireETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], 2000, "FIELDS", 1, HashData[0][0].Name };
            await CheckCommandsAsync(RespCommand.HPEXPIRE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task HPExpireAtETagAdvancedTestAsync()
        {
            var expireAt = DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeMilliseconds();
            var cmdArgs = new object[] { HashKeys[0], expireAt, "FIELDS", 1, HashData[0][0].Name };
            await CheckCommandsAsync(RespCommand.HPEXPIREAT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task HPersistETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], "FIELDS", 1, HashData[0][0].Name };
            await CheckCommandsAsync(RespCommand.HPERSIST, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task HSetETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], HashData[1][0].Name, HashData[1][0].Value };
            await CheckCommandsAsync(RespCommand.HSET, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        public override void DataSetUp()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.KeyDelete(HashKeys);

            var hSetCmdArgs = new object[] { "HSET", HashKeys[0] }.Union(HashData[0]
                .SelectMany(e => new[] { e.Name.ToString(), e.Value.ToString() })).ToArray();
            var results = (string[])db.Execute("EXECWITHETAG", hSetCmdArgs);

            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(HashData[0].Length, long.Parse(results[0]!));
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1

            var result = (long)db.Execute("HEXPIRE", HashKeys[0], 3, "FIELDS", 1, HashData[0][0].Name);
            ClassicAssert.AreEqual(1, result);

            db.HashSet(HashKeys[1], HashData[1]);
            db.HashSet(HashKeys[2], HashData[2]);
        }
    }
}
