// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
        static readonly RedisKey[] StringKeys = [KeysWithEtag[0], "key2", "key3"];

        static readonly string[] StringData = ["1", "2", "3"];

        static readonly RedisKey[] ListKeys = [KeysWithEtag[1], "lKey2", "lKey3"];

        static readonly RedisValue[][] ListData =
        [
            ["a1", "a2", "a3"],
            ["b1", "b2"],
            ["c1", "c2"],
        ];

        [Test]
        public async Task ExpireETagAdvancedTestAsync()
        {
            //var cmdArgs = new object[] { StringKeys[0], 1 };

            //await CheckCommandAsync(RespCommand.EXPIRE, cmdArgs, VerifyResult);

            var cmdArgs = new object[] { ListKeys[0], 1 };

            await CheckCommandAsync(RespCommand.EXPIRE, cmdArgs, VerifyResult);

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

            var sAddCmdArgs = new object[] { "RPUSH", ListKeys[0] }.Union(ListData[0].Select(d => d.ToString())).ToArray();
            var results = (string[])db.Execute("EXECWITHETAG", sAddCmdArgs);

            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(ListData[0].Length, long.Parse(results[0]!));
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1

            var result = db.ListRightPush(ListKeys[1], ListData[1]);
            ClassicAssert.AreEqual(ListData[1].Length, result);

            result = db.ListRightPush(ListKeys[2], ListData[2]);
            ClassicAssert.AreEqual(ListData[2].Length, result);
        }
    }
}
