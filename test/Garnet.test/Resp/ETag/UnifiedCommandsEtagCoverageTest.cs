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
    public class UnifiedCommandsEtagCoverageTest : EtagCoverageTestsBase
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
        public async Task DelETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.DEL, cmdArgs, VerifyResult);

            cmdArgs = [ListKeys[0]];

            await CheckCommandAsync(RespCommand.DEL, cmdArgs, VerifyResult, [3]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ExpireETagAdvancedTestAsync()
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
        public async Task ExpireAtETagAdvancedTestAsync()
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
        public async Task PExpireETagAdvancedTestAsync()
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
        public async Task PExpireAtETagAdvancedTestAsync()
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
        public async Task PersistETagAdvancedTestAsync()
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
        public async Task RenameETagAdvancedTestAsync()
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
        public async Task RenameNxETagAdvancedTestAsync()
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
        public async Task UnlinkETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.UNLINK, cmdArgs, VerifyResult);

            cmdArgs = [ListKeys[0]];

            await CheckCommandAsync(RespCommand.UNLINK, cmdArgs, VerifyResult, [3]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
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
                var setCmdArgs = new object[] { "SET", StringKeys[i], StringData[i] };
                var results = (string[])db.Execute("EXECWITHETAG", setCmdArgs);

                ClassicAssert.AreEqual(2, results!.Length);
                ClassicAssert.AreEqual("OK", results[0]);
                ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1
            }

            var expirationSet = db.KeyExpire(StringKeys[0], TimeSpan.FromMinutes(3));
            ClassicAssert.IsTrue(expirationSet);

            for (var i = 0; i < 2; i++)
            {
                var sAddCmdArgs = new object[] { "RPUSH", ListKeys[i] }.Union(ListData[i].Select(d => d.ToString())).ToArray();
                var results = (string[])db.Execute("EXECWITHETAG", sAddCmdArgs);

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

            // Verify expected ETag -
            // 1. If command overwrites / deletes the value - ETag should be 0 
            // 2. If command only changes the metadata of the record - ETag should remain 1
            // 3. Otherwise, ETag should advance to 2
            var expectedEtag = OverwriteCommands.Contains(command) || DeleteCommands.Contains(command) ? 0 : command.IsMetadataCommand() ? 1 : 2;
            ClassicAssert.AreEqual(expectedEtag, etag);
        }
    }
}
