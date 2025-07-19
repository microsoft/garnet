// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    class RespRevivificationTests
    {
        GarnetServer server;
        Random r;

        [SetUp]
        public void Setup()
        {
            r = new Random(335);
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            // Currently the issues are being caught in in-chain revivification. So we set the flag to use in-chain revivification only.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: true, useInChainRevivOnly: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public async Task RevivificationWithRmwWorksAsync()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // currently the revivification only works if there are more than 1 records in the hybrid log. 
            //await db.ExecuteAsync("SET", "key", "value");

            await db.ExecuteAsync("SET", "fooX", "baraaaaaaaaa");
            // shrink, 8 byte metadata + 1 byte value means 9 bytes in value
            await db.ExecuteAsync("SET", "foo", "b", "PX", 500);

            // wait for the key to be expired
            await Task.Delay(500);

            // Trigger active expiration, to tombstone the above key
            var exec = await db.ExecuteAsync("EXPDELSCAN");
            ClassicAssert.IsTrue(exec.Resp2Type != ResultType.Error);

            RedisResult[] res = (RedisResult[])exec;

            // try to reuse the tombstoned record since revivification is enabled
            // 9 byte value and same size key means this should be able to reuse but it crashes because we don't safe copy
            var setViaRmw = await db.ExecuteAsync("SET", "foo", "baraaaaaaaa", "NX");

            ClassicAssert.AreEqual("OK", setViaRmw.ToString());
        }
    }
}
