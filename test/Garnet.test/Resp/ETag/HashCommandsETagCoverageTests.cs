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
    public class HashCommandsETagCoverageTests : EtagCoverageTestsBase
    {
        static readonly RedisKey[] HashKeys = [KeyWithEtag, "hKey2", "hKey3"];

        static readonly HashEntry[][] HashData =
        [
            [new HashEntry("a1", "v1.1"), new HashEntry("a2", "v1.2"), new HashEntry("a3", "v1.3")],
            [new HashEntry("b1", "v2.1"), new HashEntry("b2", "v2.2")],
            [new HashEntry("c1", "v3.1")]
        ];

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

            db.HashSet(HashKeys[1], HashData[1]);
            db.HashSet(HashKeys[2], HashData[2]);
        }
    }
}
