// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ETag
{
    [AllureNUnit]
    [TestFixture]
    public class ListCommandsETagCoverageTests : ETagCoverageTestsBase
    {
        static readonly RedisKey[] ListKeys = [KeysWithETag[0], KeysWithETag[1], "lKey3", "lKey4"];

        static readonly RedisValue[][] ListData =
        [
            ["a1", "a2", "a3"],
            ["b1", "b2", "b3"],
            ["c1", "c2", "c3"],
            ["d1", "c3"],
        ];

        [Test]
        public async Task LIndexETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], 1 };

            await CheckCommandAsync(RespCommand.LINDEX, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[0][1], (string)result);
            }
        }

        [Test]
        public async Task LInsertETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], "AFTER", ListData[0][1], ListData[1][0] };

            await CheckCommandAsync(RespCommand.LINSERT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(4, (long)result);
            }
        }

        [Test]
        public async Task LLenETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0] };

            await CheckCommandAsync(RespCommand.LLEN, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[0].Length, (long)result);
            }
        }

        [Test]
        public async Task LMoveETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[1], ListKeys[0], "LEFT", "RIGHT" };

            await CheckCommandAsync(RespCommand.LMOVE, cmdArgs, VerifyResult, [0, 1]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[1][0], (string)result);
            }
        }

        [Test]
        public async Task LMPopETagTestAsync()
        {
            var cmdArgs = new object[] { 1, ListKeys[0], "LEFT" };

            await CheckCommandAsync(RespCommand.LMPOP, cmdArgs, VerifyResult);

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
        public async Task LPopETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0] };

            await CheckCommandAsync(RespCommand.LPOP, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[0][0], (string)result);
            }
        }

        [Test]
        public async Task LPosETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], ListData[0][1] };

            await CheckCommandAsync(RespCommand.LPOS, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task LPushETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { ListKeys[0], ListData[1][0] };

            await CheckCommandAsync(RespCommand.LPUSH, cmdArgs, VerifyResult, nxKey: nxKey);

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(nxKey ? 1 : ListData[0].Length + 1, (long)result);
            }
        }

        [Test]
        public async Task LPushXETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], ListData[1][0] };

            await CheckCommandAsync(RespCommand.LPUSHX, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[0].Length + 1, (long)result);
            }
        }

        [Test]
        public async Task LRangeETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], 0, -1 };

            await CheckCommandAsync(RespCommand.LRANGE, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var results = (string[])result;
                CollectionAssert.AreEqual(ListData[0].Select(d => (string)d), results!);
            }
        }

        [Test]
        public async Task LRemETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], 0, ListData[0][0] };

            await CheckCommandAsync(RespCommand.LREM, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task RPopETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0] };

            await CheckCommandAsync(RespCommand.RPOP, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[0][^1], (string)result);
            }
        }

        [Test]
        public async Task RPopLPushETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], ListKeys[1] };

            await CheckCommandAsync(RespCommand.RPOPLPUSH, cmdArgs, VerifyResult, [0, 1]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[0][^1], (string)result);
            }
        }

        [Test]
        public async Task RPushETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { ListKeys[0], ListData[1][0] };

            await CheckCommandAsync(RespCommand.RPUSH, cmdArgs, VerifyResult, nxKey: nxKey);

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(nxKey ? 1 : ListData[0].Length + 1, (long)result);
            }
        }

        [Test]
        public async Task RPushXETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], ListData[1][0] };

            await CheckCommandAsync(RespCommand.RPUSHX, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(ListData[0].Length + 1, (long)result);
            }
        }

        [Test]
        public async Task LSetETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], 0, ListData[1][0] };

            await CheckCommandAsync(RespCommand.LSET, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task LTrimETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], 0, 1 };

            await CheckCommandAsync(RespCommand.LTRIM, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task BLMoveETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[1], ListKeys[0], "LEFT", "RIGHT", 5 };

            await CheckBlockingCommandAsync(RespCommand.BLMOVE, cmdArgs, VerifyResult, [0, 1]);

            static void VerifyResult(byte[] result)
            {
                var elem1 = ListData[1][0];
                var btExpectedResponse = $"${elem1.ToString().Length}\r\n{elem1.ToString()}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, result);
            }
        }

        [Test]
        public async Task BLPopETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], ListKeys[1], 5 };

            await CheckBlockingCommandAsync(RespCommand.BLPOP, cmdArgs, VerifyResult);

            static void VerifyResult(byte[] result)
            {
                var key1 = ListKeys[0].ToString();
                var elem1 = ListData[0][0];
                var btExpectedResponse =
                    $"*2\r\n${key1.Length}\r\n{key1}\r\n${elem1.ToString().Length}\r\n{elem1.ToString()}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, result);
            }
        }

        [Test]
        public async Task BLMPopETagTestAsync()
        {
            var cmdArgs = new object[] { 5, 2, ListKeys[0], ListKeys[1], "LEFT", "COUNT", 2 };

            await CheckBlockingCommandAsync(RespCommand.BLMPOP, cmdArgs, VerifyResult);

            static void VerifyResult(byte[] result)
            {
                var key1 = ListKeys[0].ToString();
                var elem1 = ListData[0][0];
                var elem2 = ListData[0][1];
                var btExpectedResponse =
                    $"*2\r\n${key1.Length}\r\n{key1}\r\n*2\r\n${elem1.ToString().Length}\r\n{elem1.ToString()}\r\n${elem2.ToString().Length}\r\n{elem2.ToString()}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, result);
            }
        }

        [Test]
        public async Task BRPopETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[0], ListKeys[1], 5 };

            await CheckBlockingCommandAsync(RespCommand.BRPOP, cmdArgs, VerifyResult);

            static void VerifyResult(byte[] result)
            {
                var key1 = ListKeys[0].ToString();
                var elem1 = ListData[0][^1];
                var btExpectedResponse =
                    $"*2\r\n${key1.Length}\r\n{key1}\r\n${elem1.ToString().Length}\r\n{elem1.ToString()}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, result);
            }
        }

        [Test]
        public async Task BRPopLPushETagTestAsync()
        {
            var cmdArgs = new object[] { ListKeys[1], ListKeys[0], 5 };

            await CheckBlockingCommandAsync(RespCommand.BRPOPLPUSH, cmdArgs, VerifyResult, [0, 1]);

            static void VerifyResult(byte[] result)
            {
                var elem1 = ListData[1][^1];
                var btExpectedResponse = $"${elem1.ToString().Length}\r\n{elem1.ToString()}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, result);
            }
        }

        public override void DataSetUp(bool nxKey = false)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.KeyDelete(ListKeys);
            if (nxKey) return;

            for (var i = 0; i < 2; i++)
            {
                var sAddCmdArgs = new object[] { ListKeys[i] }.Concat(ListData[i].Select(d => d.ToString())).ToArray();
                var results = (string[])db.ExecWithETag("RPUSH", sAddCmdArgs);

                ClassicAssert.AreEqual(2, results!.Length);
                ClassicAssert.AreEqual(ListData[i].Length, long.Parse(results[0]!));
                ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // ETag 1
            }

            var result = db.ListRightPush(ListKeys[2], ListData[2]);
            ClassicAssert.AreEqual(ListData[2].Length, result);

            result = db.ListRightPush(ListKeys[3], ListData[3]);
            ClassicAssert.AreEqual(ListData[3].Length, result);
        }
    }
}