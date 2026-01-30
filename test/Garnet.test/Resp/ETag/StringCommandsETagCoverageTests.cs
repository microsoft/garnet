// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ETag
{
    [TestFixture]
    public class StringCommandsETagCoverageTests : EtagCoverageTestsBase
    {
        static readonly RedisKey[] StringKeys = [KeyWithEtag, "key2", "key3"];

        static readonly string[] StringData = ["1", "2", "3"];

        [Test]
        public async Task StringAppendETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], StringData[1] };

            await CheckCommandsAsync(RespCommand.APPEND, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(StringData[0].Length + StringData[1].Length, (long)result);
            }
        }

        [Test]
        public async Task StringIncrETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandsAsync(RespCommand.INCR, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(long.Parse(StringData[0]) + 1, (long)result);
            }
        }

        public override void DataSetUp()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.KeyDelete(StringKeys);

            var setCmdArgs = new object[] { "SET", StringKeys[0], StringData[0] };
            var result = (long)db.Execute("EXECWITHETAG", setCmdArgs);

            ClassicAssert.AreEqual(1, result); // Etag 1
        }
    }
}
