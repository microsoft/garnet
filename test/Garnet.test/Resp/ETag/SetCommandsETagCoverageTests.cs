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
    public class SetCommandsETagCoverageTests : EtagCoverageTestsBase
    {
        static readonly RedisKey[] SetKeys = [KeysWithEtag[0], KeysWithEtag[1], "sKey3", "sKey4"];

        static readonly RedisValue[][] SetData =
        [
            ["a1", "a2", "a3"],
            ["b1", "b2", "b3"],
            ["c1", "c2", "c3"],
            ["d1", "c3"],
        ];

        [Test]
        public async Task SAddETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { SetKeys[0] }.Union(SetData[1].Select(d => d.ToString())).ToArray();

            await CheckCommandAsync(RespCommand.SADD, cmdArgs, VerifyResult, nxKey: nxKey);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(3, (long)result);
            }
        }

        [Test]
        public async Task SDiffStoreETagTestAsync()
        {
            var cmdArgs = new object[] { SetKeys[0], SetKeys[2], SetKeys[3] };
            await CheckCommandAsync(RespCommand.SDIFFSTORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, (long)result);
            }
        }

        [Test]
        public async Task SInterStoreETagTestAsync()
        {
            var cmdArgs = new object[] { SetKeys[0], SetKeys[2], SetKeys[3] };
            await CheckCommandAsync(RespCommand.SINTERSTORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }


        [Test]
        public async Task SMoveETagTestAsync()
        {
            var cmdArgs = new object[] { SetKeys[1], SetKeys[0], SetData[1][0] };
            await CheckCommandAsync(RespCommand.SMOVE, cmdArgs, VerifyResult, [0, 1]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task SPopETagTestAsync()
        {
            var cmdArgs = new object[] { SetKeys[0] };
            await CheckCommandAsync(RespCommand.SPOP, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.Contains((string)result, SetData[0].ToStringArray());
            }
        }

        [Test]
        public async Task SRemETagTestAsync()
        {
            var cmdArgs = new object[] { SetKeys[0], SetData[0][0] };
            await CheckCommandAsync(RespCommand.SREM, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task SUnionStoreETagTestAsync()
        {
            var cmdArgs = new object[] { SetKeys[0], SetKeys[2], SetKeys[3] };
            await CheckCommandAsync(RespCommand.SUNIONSTORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(4, (long)result);
            }
        }

        public override void DataSetUp(bool nxKey = false)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.KeyDelete(SetKeys);
            if (nxKey) return;

            for (var i = 0; i < 2; i++)
            {
                var sAddCmdArgs = new object[] { "SADD", SetKeys[i] }.Union(SetData[i].Select(d => d.ToString())).ToArray();
                var results = (string[])db.Execute("EXECWITHETAG", sAddCmdArgs);

                ClassicAssert.AreEqual(2, results!.Length);
                ClassicAssert.AreEqual(SetData[i].Length, long.Parse(results[0]!));
                ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1
            }

            var result = db.SetAdd(SetKeys[2], SetData[2]);
            ClassicAssert.AreEqual(SetData[2].Length, result);

            result = db.SetAdd(SetKeys[3], SetData[3]);
            ClassicAssert.AreEqual(SetData[3].Length, result);
        }
    }
}
