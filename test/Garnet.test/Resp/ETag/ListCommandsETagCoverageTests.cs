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
    public class ListCommandsETagCoverageTests : EtagCoverageTestsBase
    {
        static readonly RedisKey[] ListKeys = [KeysWithEtag[0], KeysWithEtag[1], "lKey3", "lKey4"];

        static readonly RedisValue[][] ListData =
        [
            ["a1", "a2", "a3"],
            ["b1", "b2", "b3"],
            ["c1", "c2", "c3"],
            ["d1", "c3"],
        ];

        [Test]
        public async Task LInsertETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], "AFTER", ListData[0][1], ListData[1][0] };

            await CheckCommandsAsync(RespCommand.LINSERT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(4, (long)result);
            }
        }

        [Test]
        public async Task LMoveETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[1], ListKeys[0], "LEFT", "RIGHT" };

            await CheckCommandsAsync(RespCommand.LMOVE, cmdArgs, VerifyResult, [0, 1]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[1][0], (string)result);
            }
        }

        [Test]
        public async Task LMPopETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { 1, ListKeys[0], "LEFT" };

            await CheckCommandsAsync(RespCommand.LMPOP, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(2, results!.Length);
                ClassicAssert.AreEqual(ListKeys[0], (string)results[0]);
                var values = (RedisResult[])results[1];
                ClassicAssert.AreEqual(1, values!.Length);
                ClassicAssert.AreEqual(ListData[0][0], (string)values[0]);
            }
        }

        [Test]
        public async Task LPopETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0] };

            await CheckCommandsAsync(RespCommand.LPOP, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[0][0], (string)result);
            }
        }

        [Test]
        public async Task LPushETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], ListData[1][0] };

            await CheckCommandsAsync(RespCommand.LPUSH, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[0].Length + 1, (long)result);
            }
        }

        [Test]
        public async Task LPushXETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], ListData[1][0] };

            await CheckCommandsAsync(RespCommand.LPUSHX, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[0].Length + 1, (long)result);
            }
        }

        [Test]
        public async Task LRemETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], 0, ListData[0][0] };

            await CheckCommandsAsync(RespCommand.LREM, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task RPopETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0] };

            await CheckCommandsAsync(RespCommand.RPOP, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[0][^1], (string)result);
            }
        }

        [Test]
        public async Task RPopLPushETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], ListKeys[1] };

            await CheckCommandsAsync(RespCommand.RPOPLPUSH, cmdArgs, VerifyResult, [0, 1]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[0][^1], (string)result);
            }
        }

        [Test]
        public async Task RPushETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], ListData[1][0] };

            await CheckCommandsAsync(RespCommand.RPUSH, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[0].Length + 1, (long)result);
            }
        }

        [Test]
        public async Task RPushXETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], ListData[1][0] };

            await CheckCommandsAsync(RespCommand.RPUSHX, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[0].Length + 1, (long)result);
            }
        }

        [Test]
        public async Task LSetETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], 0, ListData[1][0] };

            await CheckCommandsAsync(RespCommand.LSET, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task LTrimETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], 0, 1 };

            await CheckCommandsAsync(RespCommand.LTRIM, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        public override void DataSetUp()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.KeyDelete(ListKeys);

            for (var i = 0; i < 2; i++)
            {
                var sAddCmdArgs = new object[] { "RPUSH", ListKeys[i] }.Union(ListData[i].Select(d => d.ToString())).ToArray();
                var results = (string[])db.Execute("EXECWITHETAG", sAddCmdArgs);

                ClassicAssert.AreEqual(2, results!.Length);
                ClassicAssert.AreEqual(ListData[i].Length, long.Parse(results[0]!));
                ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1
            }

            var result = db.ListRightPush(ListKeys[2], ListData[2]);
            ClassicAssert.AreEqual(ListData[2].Length, result);

            result = db.ListRightPush(ListKeys[3], ListData[3]);
            ClassicAssert.AreEqual(ListData[3].Length, result);
        }
    }
}
