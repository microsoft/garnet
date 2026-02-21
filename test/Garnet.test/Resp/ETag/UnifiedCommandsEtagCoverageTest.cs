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
        static readonly RedisKey[] StringKeys = [KeysWithEtag[0], KeysWithEtag[1], "key3"];

        static readonly string[] StringData = ["1", "2", "3"];

        static readonly RedisKey[] ListKeys = [KeysWithEtag[2], KeysWithEtag[3], "lKey3"];

        static readonly RedisValue[][] ListData =
        [
            ["a1", "a2", "a3"],
            ["b1", "b2"],
            ["c1", "c2"],
        ];

        [Test]
        public async Task ExpireETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], 100 };

            await CheckCommandAsync(RespCommand.EXPIRE, cmdArgs, VerifyResult, [2]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ExpireAtETagAdvancedTestAsync()
        {
            var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeSeconds().ToString();

            var cmdArgs = new object[] { ListKeys[0], expireTimestamp };

            await CheckCommandAsync(RespCommand.EXPIREAT, cmdArgs, VerifyResult, [2]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task PExpireETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], 1000 };

            await CheckCommandAsync(RespCommand.PEXPIRE, cmdArgs, VerifyResult, [2]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task PExpireAtETagAdvancedTestAsync()
        {
            var expireTimestamp = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeMilliseconds().ToString();

            var cmdArgs = new object[] { ListKeys[0], expireTimestamp };

            await CheckCommandAsync(RespCommand.PEXPIREAT, cmdArgs, VerifyResult, [2]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task PersistETagAdvancedTestAsync()
        {
            //var cmdArgs = new object[] { StringKeys[0] };

            //await CheckCommandAsync(RespCommand.PERSIST, cmdArgs, VerifyResult);

            var cmdArgs = new object[] { ListKeys[1] };

            await CheckCommandAsync(RespCommand.PERSIST, cmdArgs, VerifyResult, [3]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        public override void DataSetUp()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            //db.KeyDelete(StringKeys);
            db.KeyDelete(ListKeys);

            //var setCmdArgs = new object[] { "SET", StringKeys[0], StringData[0] };
            //var result = (long)db.Execute("EXECWITHETAG", setCmdArgs);

            //ClassicAssert.AreEqual(1, result); // Etag 1

            for (int i = 0; i < 2; i++)
            {
                var sAddCmdArgs = new object[] { "RPUSH", ListKeys[i] }.Union(ListData[i].Select(d => d.ToString())).ToArray();
                var results = (string[])db.Execute("EXECWITHETAG", sAddCmdArgs);

                ClassicAssert.AreEqual(2, results!.Length);
                ClassicAssert.AreEqual(ListData[i].Length, long.Parse(results[0]!));
                ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1
            }

            var expirationSet = db.KeyExpire(ListKeys[1], TimeSpan.FromMinutes(3));
            ClassicAssert.IsTrue(expirationSet);

            var result = db.ListRightPush(ListKeys[2], ListData[2]);
            ClassicAssert.AreEqual(ListData[2].Length, result);
        }
    }
}
