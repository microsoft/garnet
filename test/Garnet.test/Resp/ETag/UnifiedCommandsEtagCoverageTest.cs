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
    public class UnifiedCommandsETagCoverageTest : ETagCoverageTestsBase
    {
        static readonly RedisKey[] StringKeys = [KeysWithEtag[0], KeysWithEtag[1], KeysWithEtag[2]];

        static readonly string[] StringData = ["1", "2", "3"];

        static readonly RedisKey[] ListKeys = [KeysWithEtag[3], KeysWithEtag[4], KeysWithEtag[5]];

        static readonly RedisValue[][] ListData =
        [
            ["a1", "a2", "a3"],
            ["b1", "b2"],
            ["c1", "c2"],
        ];

        [Test]
        public async Task DelETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };
            var totalKeys = cmdArgs.Length;

            await CheckCommandAsync(RespCommand.DEL, cmdArgs, VerifyResult, totalKeys: totalKeys);

            cmdArgs = [StringKeys[0], StringKeys[1]];
            totalKeys = cmdArgs.Length;

            await CheckCommandAsync(RespCommand.DEL, cmdArgs, VerifyResult, totalKeys: totalKeys);

            cmdArgs = [ListKeys[0]];
            totalKeys = cmdArgs.Length;

            await CheckCommandAsync(RespCommand.DEL, cmdArgs, VerifyResult, [3], totalKeys: totalKeys);

            cmdArgs = [ListKeys[0], ListKeys[1]];
            totalKeys = cmdArgs.Length;

            await CheckCommandAsync(RespCommand.DEL, cmdArgs, VerifyResult, [3], totalKeys: totalKeys);

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(totalKeys, (long)result);
            }
        }

        [Test]
        public async Task ExistsETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };
            var totalKeys = cmdArgs.Length;

            await CheckCommandAsync(RespCommand.EXISTS, cmdArgs, VerifyResult, isReadOnly: true, totalKeys: totalKeys);

            cmdArgs = [StringKeys[0], StringKeys[1]];
            totalKeys = cmdArgs.Length;

            await CheckCommandAsync(RespCommand.EXISTS, cmdArgs, VerifyResult, isReadOnly: true, totalKeys: totalKeys);

            cmdArgs = [ListKeys[0]];
            totalKeys = cmdArgs.Length;

            await CheckCommandAsync(RespCommand.EXISTS, cmdArgs, VerifyResult, [3], isReadOnly: true, totalKeys: totalKeys);

            cmdArgs = [ListKeys[0], ListKeys[1]];
            totalKeys = cmdArgs.Length;

            await CheckCommandAsync(RespCommand.EXISTS, cmdArgs, VerifyResult, [3], isReadOnly: true, totalKeys: totalKeys);

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(totalKeys, (long)result);
            }
        }

        [Test]
        public async Task ExpireETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], 100 };

            await CheckCommandAsync(RespCommand.EXPIRE, cmdArgs, VerifyResult);

            cmdArgs = [ListKeys[0], 100];

            await CheckCommandAsync(RespCommand.EXPIRE, cmdArgs, VerifyResult, [3]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ExpireAtETagTestAsync()
        {
            var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();

            var cmdArgs = new object[] { StringKeys[0], expireTimestamp };

            await CheckCommandAsync(RespCommand.EXPIREAT, cmdArgs, VerifyResult);

            cmdArgs = [ListKeys[0], expireTimestamp];

            await CheckCommandAsync(RespCommand.EXPIREAT, cmdArgs, VerifyResult, [3]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ExpireTimeETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.EXPIRETIME, cmdArgs, VerifyResult, isReadOnly: true);

            cmdArgs = [ListKeys[1]];

            await CheckCommandAsync(RespCommand.EXPIRETIME, cmdArgs, VerifyResult, [3], isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(3).ToUnixTimeSeconds();

                ClassicAssert.GreaterOrEqual(expireTimestamp, (long)result);
                ClassicAssert.Less(0, (long)result);
            }
        }

        [Test]
        public async Task GetEtagETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.GETETAG, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task PExpireETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], 1000 };

            await CheckCommandAsync(RespCommand.PEXPIRE, cmdArgs, VerifyResult);

            cmdArgs = [ListKeys[0], 1000];

            await CheckCommandAsync(RespCommand.PEXPIRE, cmdArgs, VerifyResult, [3]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task PExpireAtETagTestAsync()
        {
            var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeMilliseconds().ToString();

            var cmdArgs = new object[] { StringKeys[0], expireTimestamp };

            await CheckCommandAsync(RespCommand.PEXPIREAT, cmdArgs, VerifyResult);

            cmdArgs = [ListKeys[0], expireTimestamp];

            await CheckCommandAsync(RespCommand.PEXPIREAT, cmdArgs, VerifyResult, [3]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task PExpireTimeETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.PEXPIRETIME, cmdArgs, VerifyResult, isReadOnly: true);

            cmdArgs = [ListKeys[1]];

            await CheckCommandAsync(RespCommand.PEXPIRETIME, cmdArgs, VerifyResult, [3], isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(3).ToUnixTimeMilliseconds();

                ClassicAssert.GreaterOrEqual(expireTimestamp, (long)result);
                ClassicAssert.Less(0, (long)result);
            }
        }

        [Test]
        public async Task PersistETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.PERSIST, cmdArgs, VerifyResult);

            cmdArgs = [ListKeys[1]];

            await CheckCommandAsync(RespCommand.PERSIST, cmdArgs, VerifyResult, [4]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task PTTLETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.PTTL, cmdArgs, VerifyResult, isReadOnly: true);

            cmdArgs = [ListKeys[1]];

            await CheckCommandAsync(RespCommand.PTTL, cmdArgs, VerifyResult, [3], isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var ttl = TimeSpan.FromMinutes(3).TotalMilliseconds;

                ClassicAssert.GreaterOrEqual(ttl, (long)result);
                ClassicAssert.Less(0, (long)result);
            }
        }

        [Test]
        public async Task RenameETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], StringKeys[1] };

            await CheckRenameCommandAsync(RespCommand.RENAME, cmdArgs, VerifyResult, 0, 1, false);

            cmdArgs = [ListKeys[0], ListKeys[1]];

            await CheckRenameCommandAsync(RespCommand.RENAME, cmdArgs, VerifyResult, 3, 4, false);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task RenameNxETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], StringKeys[2] };

            await CheckRenameCommandAsync(RespCommand.RENAMENX, cmdArgs, VerifyResult, 0, 2, true);

            cmdArgs = [ListKeys[0], ListKeys[2]];

            await CheckRenameCommandAsync(RespCommand.RENAMENX, cmdArgs, VerifyResult, 3, 5, true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task TTLETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.TTL, cmdArgs, VerifyResult, isReadOnly: true);

            cmdArgs = [ListKeys[1]];

            await CheckCommandAsync(RespCommand.TTL, cmdArgs, VerifyResult, [3], isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var ttl = TimeSpan.FromMinutes(3).TotalSeconds;

                ClassicAssert.GreaterOrEqual(ttl, (long)result);
                ClassicAssert.Less(0, (long)result);
            }
        }

        [Test]
        public async Task TypeETagTestAsync()
        {
            var isObj = false;
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.TYPE, cmdArgs, VerifyResult, isReadOnly: true);

            isObj = true;
            cmdArgs = [ListKeys[1]];

            await CheckCommandAsync(RespCommand.TYPE, cmdArgs, VerifyResult, [3], isReadOnly: true);

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(isObj ? "list" : "string", (string)result);
            }
        }

        [Test]
        public async Task UnlinkETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };
            var totalKeys = cmdArgs.Length;

            await CheckCommandAsync(RespCommand.UNLINK, cmdArgs, VerifyResult, totalKeys: totalKeys);

            cmdArgs = [StringKeys[0], StringKeys[1]];
            totalKeys = cmdArgs.Length;

            await CheckCommandAsync(RespCommand.UNLINK, cmdArgs, VerifyResult, totalKeys: totalKeys);

            cmdArgs = [ListKeys[0]];
            totalKeys = cmdArgs.Length;

            await CheckCommandAsync(RespCommand.UNLINK, cmdArgs, VerifyResult, [3], totalKeys: totalKeys);

            cmdArgs = [ListKeys[0], ListKeys[1]];
            totalKeys = cmdArgs.Length;

            await CheckCommandAsync(RespCommand.UNLINK, cmdArgs, VerifyResult, [3], totalKeys: totalKeys);

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(totalKeys, (long)result);
            }
        }

        public override void DataSetUp(bool nxKey = false)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.KeyDelete(StringKeys);
            db.KeyDelete(ListKeys);
            if (nxKey) return;

            for (var i = 0; i < 2; i++)
            {
                var setCmdArgs = new object[] { StringKeys[i], StringData[i] };
                var results = (string[])db.ExecWithEtag("SET", setCmdArgs);
                ClassicAssert.AreEqual(2, results!.Length);
                ClassicAssert.IsNull(results[0]);
                ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1
            }

            var expirationSet = db.KeyExpire(StringKeys[0], TimeSpan.FromMinutes(3));
            ClassicAssert.IsTrue(expirationSet);

            for (var i = 0; i < 2; i++)
            {
                var sAddCmdArgs = new object[] { ListKeys[i] }.Concat(ListData[i].Select(d => d.ToString())).ToArray();
                var results = (string[])db.ExecWithEtag("RPUSH", sAddCmdArgs);

                ClassicAssert.AreEqual(2, results!.Length);
                ClassicAssert.AreEqual(ListData[i].Length, long.Parse(results[0]!));
                ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1
            }

            expirationSet = db.KeyExpire(ListKeys[1], TimeSpan.FromMinutes(3));
            ClassicAssert.IsTrue(expirationSet);
        }

        private async Task CheckRenameCommandAsync(RespCommand command, object[] commandArgs, Action<RedisResult> verifyResult, int srcKeyWithEtag, int dstKeyWithEtag, bool dstKeyNx)
        {
            await using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            DataSetUp(nxKey: false);

            // Verify all keys set-up with ETags have an ETag of 1
            var etag = (long)await db.ExecuteAsync("GETETAG", KeysWithEtag[srcKeyWithEtag]);
            ClassicAssert.AreEqual(1, etag);

            if (!dstKeyNx)
            {
                etag = (long)await db.ExecuteAsync("GETETAG", KeysWithEtag[dstKeyWithEtag]);
                ClassicAssert.AreEqual(1, etag);
            }

            // Check running the command without a meta-command
            // Run the command and verify the result
            var result = await db.ExecuteAsync(command.ToString(), commandArgs);
            verifyResult(result);

            // Check source key name does not exist
            var exists = db.KeyExists(KeysWithEtag[srcKeyWithEtag]);
            ClassicAssert.IsFalse(exists);

            // Verify dest key etag post-command
            etag = (long)await db.ExecuteAsync("GETETAG", KeysWithEtag[dstKeyWithEtag]);

            // Verify expected ETag - 0 (RENAME without EXECWITHETAG zeroes out the etag)
            ClassicAssert.AreEqual(0, etag);

            // Reset the data
            DataSetUp(nxKey: false);

            // Add the EXECWITHETAG meta-command
            result = await db.ExecWithEtagAsync(command.ToString(), commandArgs);

            // Verify result & expected ETag (RENAME with EXECWITHETAG advances the etag)
            VerifyResultAndETag(result, verifyResult, dstKeyNx ? 1 : 2);
        }
    }
}
