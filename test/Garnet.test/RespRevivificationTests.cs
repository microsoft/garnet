// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
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

        // 8 byte metadata + 1 byte value means 9 bytes in value from the below default args
        string[] defaultInitialArgs = [
            "SET", "foo", "b", "PX", "500"
        ];

        [SetUp]
        public void Setup()
        {
            r = new Random(335);
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            SetupServerWithReviv(true);
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        private void SetupServerWithReviv(bool inChainOnly)
        {
            // Currently the issues are being caught in in-chain revivification. So we set the flag to use in-chain revivification only.
            if (inChainOnly)
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: true, useInChainRevivOnly: true, enableAOF: true, commitWait: true);
            else
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: true, useReviv: true, enableAOF: true, commitWait: true);

            server.Start();
        }

        [Test]
        public async Task RevivificationWithRmwWorksAsync()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            Func<IDatabase, Task> testRmwWorksViaSETNX = async (db) =>
            {
                // This should work since the key is tombstoned and we are reusing the tombstone record
                // try to reuse the tombstoned record since revivification is enabled
                // 11 byte value and same size key means this should be able to reuse but it crashes because we don't safe copy
                var setViaRmw = await db.ExecuteAsync("SET", "foo", "baraaaaaaaa", "NX");

                ClassicAssert.AreEqual("OK", setViaRmw.ToString());
            };

            await TestRevivifyAsync(testRmwWorksViaSETNX, defaultInitialArgs, redis);
        }

        [Test]
        public async Task RevivificationWithRmwWorksWhenNeedingShrinkingAndThenExpanding()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            Func<IDatabase, Task> testRmwWorksViaSETNX = async (db) =>
            {
                // This should work since the key is tombstoned and we are reusing the tombstone record
                // try to reuse the tombstoned record since revivification is enabled
                // much smaller value will the revivifiable record, will zero out the bytes before shrinking
                var setViaRmw = await db.ExecuteAsync("SET", "foo", "mars", "NX");
                ClassicAssert.AreEqual("OK", setViaRmw.ToString());
            };

            await TestRevivifyAsync(testRmwWorksViaSETNX, defaultInitialArgs, redis);

            // now when the above record is shrunk, I will delete it and reuse it again
            var db = redis.GetDatabase(0);

            await db.KeyDeleteAsync("foo");

            // now originally the key held a 1 byte value and 8 byte metadata, so inchain revivification of the exact same size should work
            await db.ExecuteAsync("SETIFGREATER", "foo", "b", "1");

            // check for revivification stats
            var stats = await db.ExecuteAsync("INFO", "STOREREVIV");
            ClassicAssert.IsTrue(stats.ToString().Contains("Successful In-Chain: 2"), "Expected in-chain revivification to happen the second time, but it did not.");
        }

        [Test]
        public async Task RevivificationWithRMWWorksViaSetIfGreater()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            Func<IDatabase, Task> testRmwWorksViaSetIfGreater = async (db) =>
            {

                // should be able to reuse the tombstoned record value is 1 byte and etag space is 8 bytes == 9 bytes
                var res = (RedisResult[])await db.ExecuteAsync("SETIFGREATER", "foo", "c", "5");
                ClassicAssert.AreEqual(5, (long)res[0]);
                ClassicAssert.IsTrue(res[1].IsNull);
            };

            await TestRevivifyAsync(testRmwWorksViaSetIfGreater, defaultInitialArgs, redis);
        }


        [Test]
        public async Task RevivificationWithRMWWorksViaAppend()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            Func<IDatabase, Task> testRmwWorksViaAppend = async (db) =>
            {
                // new value is below 12 bytes so it should reuse in initial update
                var res = await db.ExecuteAsync("APPEND", "foo", "baraaaaaaaa");
                ClassicAssert.AreEqual(11, (int)res);
            };

            await TestRevivifyAsync(testRmwWorksViaAppend, defaultInitialArgs, redis);
        }

        [Test]
        public async Task RevivificationWithRMWWorksViaSetBit()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            Func<IDatabase, Task> testRmwWorksViaSetBit = async (db) =>
            {
                // we need something that allocates 12 bytes internally in SETBIT
                var res = await db.ExecuteAsync("SETBIT", "foo", 95, 1);
                ClassicAssert.AreEqual(0, (int)res);
            };

            await TestRevivifyAsync(testRmwWorksViaSetBit, defaultInitialArgs, redis);
        }

        [Test]
        public async Task RevivificationWithRMWWorksViaBitfield()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            Func<IDatabase, Task> testRmwWorksViaBitfield = async (db) =>
            {
                // we need something that allocates 12 bytes internally in BITFIELD
                var res = await db.ExecuteAsync("BITFIELD", "foo", "INCRBY", "i64", "28", "1");
                ClassicAssert.AreEqual(1, (int)res);
            };

            await TestRevivifyAsync(testRmwWorksViaBitfield, defaultInitialArgs, redis);
        }

        private async Task TestRevivifyAsync(Func<IDatabase, Task> callbackFunc, string[] initialSettingArgs, IConnectionMultiplexer redis)
        {
            var db = redis.GetDatabase(0);

            await db.ExecuteAsync("SET", "foox", "bolognese");
            await db.ExecuteAsync(initialSettingArgs[0], initialSettingArgs.Skip(1).ToArray());

            // wait for the key to be expired
            await Task.Delay(600);

            // ---- Trigger active expiration, to tombstone the above key ---
            var exec = await db.ExecuteAsync("EXPDELSCAN");
            ClassicAssert.IsTrue(exec.Resp2Type != ResultType.Error);

            await callbackFunc(db);

            // check if revivification stat shows in-chain revivification happened.
            var stats = await db.ExecuteAsync("INFO", "STOREREVIV");
            ClassicAssert.IsTrue(stats.ToString().Contains("Successful In-Chain: 1"), "Expected in-chain revivification to happen, but it did not.");
        }

        // Below we test revivification via record pool when doing an RCU operation works via RMW.
        [Test]
        public async Task RevivificationWithRMWWorksForRecordPool()
        {
            server.Dispose(false);
            SetupServerWithReviv(false);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            await db.ExecuteAsync("SET", "foo", "c", "PX", 500);
            await db.ExecuteAsync("SET", "michael", "jordan");
            await db.ExecuteAsync("SET", "johnny", "x", "PX", 500); // big record, that we need to make sure active exp picks up for revivification

            await Task.Delay(600); // wait for the keys to expire

            // ---- Trigger active expiration, to tombstone the above key ---
            var exec = await db.ExecuteAsync("EXPDELSCAN");
            // when deleting via above, are we potentially resetting the value and it's metadata and stuff?
            ClassicAssert.IsTrue(exec.Resp2Type != ResultType.Error);

            // attempt to do an RCU operation that will reuse the tombstoned record via Recordpool
            await db.ExecuteAsync("SETIFGREATER", "michael", "j", 23);

            // confirm we did indeed use a reviv record
            var stats = await db.ExecuteAsync("INFO", "STOREREVIV");
            ClassicAssert.IsTrue(stats.ToString().Contains("Successful Takes: 1"), "Expected in-chain revivification to happen, but it did not.");

            var res = (RedisResult[])await db.ExecuteAsync("GETWITHETAG", "michael");

            ClassicAssert.AreEqual(23, (long)res[0], "Incorrect Etag.");
            ClassicAssert.AreEqual("j", res[1].ToString(), "Expected the value to be updated via RMW operation, but it was not.");
        }

        [Test]
        public async Task RevivifiedRecordsShouldStillEnqueueToAofViaRmwAndClearEtagState()
        {
            server.Dispose(false);
            SetupServerWithReviv(inChainOnly: true);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());

            var db = redis.GetDatabase(0);

            long startingAddr = server.Provider.StoreWrapper.appendOnlyFile.TailAddress;

            await db.StringSetAsync("arnold", "schwarzeneggar");
            long tailAddrAfterInsert = server.Provider.StoreWrapper.appendOnlyFile.TailAddress;
            ClassicAssert.IsTrue(tailAddrAfterInsert > startingAddr, "Expected AOF tail address to move forward on initial SET");

            // setup keys for revivifying
            await db.StringSetAsync("hoo", "kachakahookahooka");
            // both would move the tail address of the AOF forward
            long tailAddrAfterInsert2 = server.Provider.StoreWrapper.appendOnlyFile.TailAddress;
            ClassicAssert.IsTrue(tailAddrAfterInsert2 > tailAddrAfterInsert, "Expected AOF tail address to move forward on initial SET");
            await db.KeyDeleteAsync("hoo");
            long tailAddrAfterDelete = server.Provider.StoreWrapper.appendOnlyFile.TailAddress;
            ClassicAssert.IsTrue(tailAddrAfterDelete > tailAddrAfterInsert2, "Expected AOF tail address to move forward on DELETE");

            // inchain revivification of the exact same key should take place 
            await db.ExecuteAsync("SETIFGREATER", "hoo", "b", "1");
            long tailAddrAfterRevivifyRmw = server.Provider.StoreWrapper.appendOnlyFile.TailAddress;
            ClassicAssert.IsTrue(tailAddrAfterRevivifyRmw > tailAddrAfterDelete, "Expected AOF tail address to move forward on revivification RMW");

            // the unrelated read should not be affected by revivification state
            string result = await db.StringGetAsync("arnold");
            ClassicAssert.AreEqual("schwarzeneggar", result);

            // check for revivification stats
            var stats = await db.ExecuteAsync("INFO", "STOREREVIV");
            ClassicAssert.IsTrue(stats.ToString().Contains("Successful In-Chain: 1"), "Expected in-chain revivification to have happened");
        }

        [Test]
        public async Task RevivifiedRecordsShouldStillEnqueueToAofViaUpsert()
        {
            server.Dispose(false);
            SetupServerWithReviv(inChainOnly: true);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());

            var db = redis.GetDatabase(0);

            await db.StringSetAsync("fizz", "buzz");

            long startingAddr = server.Provider.StoreWrapper.appendOnlyFile.TailAddress;

            // setup keys for revivifying
            await db.StringSetAsync("hoo", "kachakahookahooka");
            // both would move the tail address of the AOF forward
            long tailAddrAfterInsert = server.Provider.StoreWrapper.appendOnlyFile.TailAddress;
            ClassicAssert.IsTrue(tailAddrAfterInsert > startingAddr, "Expected AOF tail address to move forward on initial SET");

            // in-chain tombstone
            await db.KeyDeleteAsync("hoo");
            long tailAddrAfterDelete = server.Provider.StoreWrapper.appendOnlyFile.TailAddress;
            ClassicAssert.IsTrue(tailAddrAfterDelete > tailAddrAfterInsert, "Expected AOF tail address to move forward on DELETE");

            // do an in-chain revivification
            await db.StringSetAsync("hoo", "b");
            long tailAddrAfterRevivifySet = server.Provider.StoreWrapper.appendOnlyFile.TailAddress;
            ClassicAssert.IsTrue(tailAddrAfterRevivifySet > tailAddrAfterDelete, "Expected AOF tail address to move forward on revivification SET");

            // make sure revivification stats reflect that things were indeed revivified
            var stats = await db.ExecuteAsync("INFO", "STOREREVIV");
            ClassicAssert.IsTrue(stats.ToString().Contains("Successful In-Chain: 1"), "Expected in-chain revivification to have happened");
        }
    }
}