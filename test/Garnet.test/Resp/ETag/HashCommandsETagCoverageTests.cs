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
    public class HashCommandsETagCoverageTests : ETagCoverageTestsBase
    {
        static readonly RedisKey[] HashKeys = [KeysWithEtag[0], "hKey2", "hKey3"];

        static readonly HashEntry[][] HashData =
        [
            [new HashEntry("a1", 1), new HashEntry("a2", 1.2), new HashEntry("a3", "v1.3")],
            [new HashEntry("b1", "v2.1"), new HashEntry("b2", "v2.2")],
            [new HashEntry("c1", "v3.1")]
        ];

        [Test]
        public async Task HCollectETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { HashKeys[0] };
            await CheckCommandAsync(RespCommand.HCOLLECT, cmdArgs, VerifyResult, nxKey: nxKey);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task HDelETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], HashData[0][0].Name };
            await CheckCommandAsync(RespCommand.HDEL, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task HExistsETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], HashData[0][0].Name };
            await CheckCommandAsync(RespCommand.HEXISTS, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(true, (bool)result);
            }
        }

        [Test]
        public async Task HExpireETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], 2, "FIELDS", 1, HashData[0][0].Name };
            await CheckCommandAsync(RespCommand.HEXPIRE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task HExpireAtETagTestAsync()
        {
            var expireAt = DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeSeconds();
            var cmdArgs = new object[] { HashKeys[0], expireAt, "FIELDS", 1, HashData[0][0].Name };
            await CheckCommandAsync(RespCommand.HEXPIREAT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task HExpireTimeETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], "FIELDS", 1, HashData[0][0].Name };
            await CheckCommandAsync(RespCommand.HEXPIRETIME, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeSeconds();

                ClassicAssert.AreEqual(1, result.Length);
                ClassicAssert.GreaterOrEqual(expireTimestamp, (long)result[0]);
                ClassicAssert.Less(0, (long)result[0]);
            }
        }

        [Test]
        public async Task HGetETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], HashData[0][0].Name };
            await CheckCommandAsync(RespCommand.HGET, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(HashData[0][0].Value, (string)result);
            }
        }

        [Test]
        public async Task HGetAllETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0] };
            await CheckCommandAsync(RespCommand.HGETALL, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var entryCount = HashData[0].Length;
                ClassicAssert.AreEqual(entryCount * 2, result.Length);
                var entries = new HashEntry[entryCount];
                for (var i = 0; i < entryCount * 2; i += 2)
                {
                    entries[i / 2] = new HashEntry((string)result[i], (string)result[i + 1]);
                }

                CollectionAssert.AreEquivalent(HashData[0], entries);
            }
        }

        [Test]
        public async Task HIncrByETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], HashData[0][0].Name, 2 };
            await CheckCommandAsync(RespCommand.HINCRBY, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(long.Parse(HashData[0][0].Value) + 2 , (long)result);
            }
        }

        [Test]
        public async Task HIncrByFloatETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], HashData[0][1].Name, 2.2 };
            await CheckCommandAsync(RespCommand.HINCRBYFLOAT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(double.Parse(HashData[0][1].Value) + 2.2, (double)result);
            }
        }

        [Test]
        public async Task HKeysETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0] };
            await CheckCommandAsync(RespCommand.HKEYS, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var results = (string[])result;
                CollectionAssert.AreEquivalent(HashData[0].Select(d => (string)d.Name), results!);
            }
        }

        [Test]
        public async Task HLenETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0] };
            await CheckCommandAsync(RespCommand.HLEN, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(HashData[0].Length, (long)result);
            }
        }

        [Test]
        public async Task HMGetETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], HashData[0][0].Name, HashData[0][1].Name };
            await CheckCommandAsync(RespCommand.HMGET, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, result.Length);
                ClassicAssert.AreEqual(HashData[0][0].Value, (string)result[0]);
                ClassicAssert.AreEqual(HashData[0][1].Value, (string)result[1]);
            }
        }

        [Test]
        public async Task HMSetETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], HashData[1][0].Name, HashData[1][0].Value };
            await CheckCommandAsync(RespCommand.HMSET, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task HPExpireETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], 2000, "FIELDS", 1, HashData[0][0].Name };
            await CheckCommandAsync(RespCommand.HPEXPIRE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task HPExpireAtETagTestAsync()
        {
            var expireAt = DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeMilliseconds();
            var cmdArgs = new object[] { HashKeys[0], expireAt, "FIELDS", 1, HashData[0][0].Name };
            await CheckCommandAsync(RespCommand.HPEXPIREAT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task HPExpireTimeETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], "FIELDS", 1, HashData[0][0].Name };
            await CheckCommandAsync(RespCommand.HEXPIRETIME, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeMilliseconds();

                ClassicAssert.AreEqual(1, result.Length);
                ClassicAssert.GreaterOrEqual(expireTimestamp, (long)result[0]);
                ClassicAssert.Less(0, (long)result[0]);
            }
        }

        [Test]
        public async Task HPersistETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], "FIELDS", 1, HashData[0][0].Name };
            await CheckCommandAsync(RespCommand.HPERSIST, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task HPTtlETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], "FIELDS", 1, HashData[0][0].Name };
            await CheckCommandAsync(RespCommand.HPTTL, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var ttl = TimeSpan.FromSeconds(3).TotalMilliseconds;
                ClassicAssert.GreaterOrEqual(ttl, (long)result);
                ClassicAssert.Less(0, (long)result);
            }
        }

        [Test]
        public async Task HRandFieldETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0] };
            await CheckCommandAsync(RespCommand.HRANDFIELD, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                CollectionAssert.Contains(HashData[0].Select(d => (string)d.Name), (string)result);
            }
        }

        [Test]
        public async Task HScanETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], 0 };
            await CheckCommandAsync(RespCommand.HSCAN, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, result.Length);
            }
        }

        [Test]
        public async Task HSetETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { HashKeys[0], HashData[1][0].Name, HashData[1][0].Value };
            await CheckCommandAsync(RespCommand.HSET, cmdArgs, VerifyResult, nxKey: nxKey);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task HSetNxETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { HashKeys[0], HashData[1][0].Name, HashData[1][0].Value };
            await CheckCommandAsync(RespCommand.HSETNX, cmdArgs, VerifyResult, nxKey: nxKey);
            
            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task HStrLenETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], HashData[0][0].Name };
            await CheckCommandAsync(RespCommand.HSTRLEN, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual((((string)HashData[0][0].Value)!).Length, (long)result);
            }
        }

        [Test]
        public async Task HTtlETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0], "FIELDS", 1, HashData[0][0].Name };
            await CheckCommandAsync(RespCommand.HTTL, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.GreaterOrEqual(3, (long)result);
                ClassicAssert.Less(0, (long)result);
            }
        }

        [Test]
        public async Task HValsETagTestAsync()
        {
            var cmdArgs = new object[] { HashKeys[0] };
            await CheckCommandAsync(RespCommand.HVALS, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var results = (string[])result;
                CollectionAssert.AreEquivalent(HashData[0].Select(d => (string)d.Value), results!);
            }
        }

        public override void DataSetUp(bool nxKey = false)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.KeyDelete(HashKeys);
            if (nxKey) return;

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
