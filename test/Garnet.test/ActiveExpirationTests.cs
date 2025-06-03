// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    class ActiveExpirationTests
    {
        private const int ActiveExpirationFreqSecs = 10;

        private GarnetServer server;

        [SetUp]
        public void Setup()
        {
            SetupServer(ActiveExpirationFreqSecs);
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }


        private void SetupServer(int activeExpSchedule)
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            // create server with active expiration enabled, TODO: need to turn on reviv to see log growth minimized from reviv
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, activeExpirationFrequencySecs: activeExpSchedule);
            server.Start();
        }

        [Test]
        public Task TestOnDemandActiveExpiration()
        {
            TearDown();

            // setup with scheduled active exp off
            SetupServer(-1);

            return TestActiveExpirationAsync(async (db) =>
            {
                // wait for the expiration of the items that were put in.
                await Task.Delay(TimeSpan.FromSeconds(ActiveExpirationFreqSecs));

                RedisResult[] res = (RedisResult[])db.Execute("ACTEXP");

                ClassicAssert.IsTrue(int.Parse(res[0].ToString()) > 0);
                ClassicAssert.IsTrue(int.Parse(res[1].ToString()) > 0);
            });
        }

        [Test]
        public Task TestScheduledActiveExpirationViaSimulation()
            => TestActiveExpirationAsync((_) => Task.Delay(TimeSpan.FromSeconds(ActiveExpirationFreqSecs)));

        private async Task TestActiveExpirationAsync(Func<IDatabase, Task> activeExpirationInvocation)
        {
            var untombstonedRecords = new List<string>();
            var tombstonedRecords = new List<string>();

            var random = new Random();
            for (int i = 0; i < 100; i++)
            {
                var toAddInList = random.Next(0, 100) < 50 ? tombstonedRecords : untombstonedRecords;
                toAddInList.Add(Guid.NewGuid().ToString());
            }

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Before Active expiration cycle no reviv has happened
                ClassicAssert.AreEqual(0, server.Provider.StoreWrapper.store.RevivificationManager.stats.successfulTakes, "stats should not be populated at the start");

                // add a set of records that will not expire in the duration of the test.
                PopulateStore(untombstonedRecords, db, (300, 400), forceExpirationaddition: false);
                // add a set of records that MUST expire within the duration of the test.
                PopulateStore(tombstonedRecords, db, (6, ActiveExpirationFreqSecs), forceExpirationaddition: true);

                var tailAddr = server.Provider.StoreWrapper.store.Log.TailAddress;

                // Pre Active expiration EVERYTHING exists
                CheckExistenceConditionOnAllKeys(db, tombstonedRecords, true, "All to be expired should still exist");
                CheckExistenceConditionOnAllKeys(db, untombstonedRecords, true, "All to be not expired should still exist");

                await activeExpirationInvocation(db);

                // check that revivification happened for expired record 
                // HK TODO: WHY TF NOT?
                ClassicAssert.IsTrue(server.Provider.StoreWrapper.store.RevivificationManager.stats.successfulAdds > 0, "Active expiration did not revivify as expected");

                // Post active expiration, expired records don't exist for sure. This can be fooled by passive expiration too, so check reviv metrics too
                CheckExistenceConditionOnAllKeys(db, tombstonedRecords, false, "All to be expired should no longer exist post gc");
                CheckExistenceConditionOnAllKeys(db, untombstonedRecords, true, "All to be not expired should exist post gc too");

                // now a subsequent request would not add to hlog growth because it will use the revivifaction pool
                ClassicAssert.IsTrue(db.StringSet(Guid.NewGuid().ToString(), Guid.NewGuid().ToString()), "failed to insert new record");
                ClassicAssert.AreEqual(tailAddr, server.Provider.StoreWrapper.store.Log.TailAddress, "Tail address moved forward so a new allocation was made, not expected.");
            }
        }

        // Helper to simulate workloads that use expiration in a given range across main store and object store
        private void PopulateStore(List<string> keys, IDatabase db, (int, int) allowedExpirationRange, bool forceExpirationaddition)
        {
            Random rnd = new Random();
            for (int i = 0; i < keys.Count; i++)
            {
                int randomInt = rnd.Next(allowedExpirationRange.Item1, allowedExpirationRange.Item2);
                bool isMainStore = randomInt < (allowedExpirationRange.Item2 + allowedExpirationRange.Item1) / 2;
                bool hasExpiration = randomInt % 2 == 0;
                if (isMainStore)
                {
                    db.StringSet(keys[i], Guid.NewGuid().ToString());
                }
                else
                {
                    db.SortedSetAdd(keys[i], Guid.NewGuid().ToString(), randomInt);
                }

                if (hasExpiration || forceExpirationaddition)
                    ClassicAssert.IsTrue(db.KeyExpire(keys[i], TimeSpan.FromSeconds(randomInt)));
            }
        }

        // Helper to check if all keys exist, or all keys dont exist in a given collection
        private void CheckExistenceConditionOnAllKeys(IDatabase db, List<string> keys, bool shouldExist, string assertionMsg)
        {
            foreach (string key in keys)
            {
                bool exists = db.KeyExists(key);

                if (exists && !shouldExist)
                {
                    // what is the expiration?
                    DateTime? timeOffExpiration = db.KeyExpireTime(key);
                    TimeSpan tsFromNow = timeOffExpiration.Value - DateTime.UtcNow;
                    ClassicAssert.IsTrue(false, $"key that should have expired by now, is expiring in {tsFromNow.TotalSeconds} seconds");
                }

                ClassicAssert.IsTrue(exists == shouldExist, assertionMsg);
            }
        }
    }
}