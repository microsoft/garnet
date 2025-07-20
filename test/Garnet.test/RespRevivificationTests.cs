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
            Func<IDatabase, Task> testRmwWorksViaSETNX = async (db) =>
            {
                // This should work since the key is tombstoned and we are reusing the tombstone record
                // try to reuse the tombstoned record since revivification is enabled
                // 11 byte value and same size key means this should be able to reuse but it crashes because we don't safe copy
                var setViaRmw = await db.ExecuteAsync("SET", "foo", "baraaaaaaaa", "NX");

                ClassicAssert.AreEqual("OK", setViaRmw.ToString());
            };

            await TestRevivifyAsync(testRmwWorksViaSETNX);
        }

        [Test]
        public async Task RevivificationWithRMWWorksViaSetIfGreater()
        {
            Func<IDatabase, Task> testRmwWorksViaSetIfGreater = async (db) =>
            {

                // should be able to reuse the tombstoned record value is 1 byte and etag space is 8 bytes == 9 bytes
                var res = (RedisResult[])await db.ExecuteAsync("SETIFGREATER", "foo", "c", "5");
                ClassicAssert.AreEqual(5, (long)res[0]);
                ClassicAssert.IsTrue(res[1].IsNull);
            };

            await TestRevivifyAsync(testRmwWorksViaSetIfGreater);
        }


        [Test]
        public async Task RevivificationWithRMWWorksViaAppend()
        {
            Func<IDatabase, Task> testRmwWorksViaSetIfGreater = async (db) =>
            {
                // new value is below 12 bytes so it should reuse in initial update
                var res = await db.ExecuteAsync("APPEND", "foo", "baraaaaaaaa");
                ClassicAssert.AreEqual(11, (int)res);
            };

            await TestRevivifyAsync(testRmwWorksViaSetIfGreater);
        }

        // TODO: add more

        private async Task TestRevivifyAsync(Func<IDatabase, Task> callbackFunc)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            await db.ExecuteAsync("SET", "fooX", "baraaaaaaaaa");
            // shrink, 8 byte metadata + 1 byte value means 9 bytes in value
            await db.ExecuteAsync("SET", "foo", "b", "PX", 500);

            // wait for the key to be expired
            await Task.Delay(500);

            // Trigger active expiration, to tombstone the above key
            var exec = await db.ExecuteAsync("EXPDELSCAN");
            ClassicAssert.IsTrue(exec.Resp2Type != ResultType.Error);

            RedisResult[] res = (RedisResult[])exec;

            await callbackFunc(db);

            // check if revivification stat shows in-chain revivification happened.
            var stats = await db.ExecuteAsync("INFO", "STOREREVIV");
            ClassicAssert.IsTrue(stats.ToString().Contains("Successful In-Chain: 1"), "Expected in-chain revivification to happen, but it did not.");
        }
    }
}
