// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    class ExpiredKeyDeletionTests : AllureTestBase
    {
        private const int ExpiredKeyDeletionScanFrequencySecs = 10;

        private const int TotalNumKeysToCreate = 100;

        private GarnetServer server;

        [SetUp]
        public void Setup()
        {
            SetupServer(ExpiredKeyDeletionScanFrequencySecs);
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }


        private void SetupServer(int expiredKeyDeletionScanFrequencySecs)
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            // create server with active expiration enabled, TODO: need to turn on reviv to see log growth minimized from reviv
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, expiredKeyDeletionScanFrequencySecs: expiredKeyDeletionScanFrequencySecs, useReviv: true);
            server.Start();
        }

        [Test]
        public Task TestOnDemandExpiredKeyDeletionScan()
        {
            TearDown();

            // setup with scheduled active exp off
            SetupServer(-1);

            return TestExpiredKeyDeletionScanAsync(async (db, expectedKeysToExpire) =>
            {
                // wait for the expiration of the items that were put in.
                await Task.Delay(TimeSpan.FromSeconds(ExpiredKeyDeletionScanFrequencySecs));

                RedisResult[] res = (RedisResult[])db.Execute("EXPDELSCAN");

                ClassicAssert.IsTrue(int.Parse(res[0].ToString()) == expectedKeysToExpire);
                ClassicAssert.IsTrue(int.Parse(res[1].ToString()) == TotalNumKeysToCreate);
            });
        }

        [Test]
        public Task TestScheduledExpiredKeyDeletionViaSimulation()
            => TestExpiredKeyDeletionScanAsync((_, expectedKeysToExpire) => Task.Delay(TimeSpan.FromSeconds(ExpiredKeyDeletionScanFrequencySecs)));

        private async Task TestExpiredKeyDeletionScanAsync(Func<IDatabase, int, Task> expiredKeyDeletionScanInvocation)
        {
            var untombstonedRecords = new List<string>();
            var tombstonedRecords = new List<string>();

            var random = new Random();
            for (int i = 0; i < TotalNumKeysToCreate; i++)
            {
                var toAddInList = random.Next(0, 2) == 0 ? tombstonedRecords : untombstonedRecords;
                toAddInList.Add(Guid.NewGuid().ToString());
            }

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Before expired key deletion scan cycle, no reviv has happened
                ClassicAssert.AreEqual(0, server.Provider.StoreWrapper.store.RevivificationManager.stats.successfulTakes, "stats should not be populated at the start");

                int totalKeysThatWillExpire =
                    // add a set of records that will not expire in the duration of the test.
                    PopulateStore(untombstonedRecords, db, (300, 400), forceExpirationaddition: false)
                    +
                    // add a set of records that MUST expire within the duration of the test.
                    PopulateStore(tombstonedRecords, db, (6, ExpiredKeyDeletionScanFrequencySecs), forceExpirationaddition: true);

                var tailAddr = server.Provider.StoreWrapper.store.Log.TailAddress;

                // Pre expired key deletion scan, EVERYTHING exists
                CheckExistenceConditionOnAllKeys(db, tombstonedRecords, true, "All to be expired should still exist");
                CheckExistenceConditionOnAllKeys(db, untombstonedRecords, true, "All to be not expired should still exist");

                await expiredKeyDeletionScanInvocation(db, totalKeysThatWillExpire);

                // Merge reviv stats across sessions
                server.Provider.StoreWrapper.store.DumpRevivificationStats();

                // Check that revivification happened for expired record 
                ClassicAssert.IsTrue(server.Provider.StoreWrapper.store.RevivificationManager.stats.successfulAdds > 0, "Active expiration did not revivify for main store as expected");

                // Post expired key deletion scan, expired records don't exist for sure. This can be fooled by passive expiration too, so check reviv metrics too
                CheckExistenceConditionOnAllKeys(db, tombstonedRecords, false, "All to be expired should no longer exist post gc");
                CheckExistenceConditionOnAllKeys(db, untombstonedRecords, true, "All to be not expired should exist post gc too");

                // now a subsequent request would not add to hlog growth because it will use the revivifaction pool
                ClassicAssert.IsTrue(db.StringSet(Guid.NewGuid().ToString(), Guid.NewGuid().ToString()), "failed to insert new record");
                ClassicAssert.AreEqual(tailAddr, server.Provider.StoreWrapper.store.Log.TailAddress, "Tail address moved forward so a new allocation was made, not expected.");
            }
        }

        // Helper to simulate workloads that use expiration in a given range across main store and object store
        private int PopulateStore(List<string> keys, IDatabase db, (int, int) allowedExpirationRange, bool forceExpirationaddition)
        {
            int totalKeysThatWillExpire = 0;
            Random rnd = new Random();
            for (int i = 0; i < keys.Count; i++)
            {
                int expirationOrScore = rnd.Next(allowedExpirationRange.Item1, allowedExpirationRange.Item2);
                bool addString = rnd.Next(0, 2) == 0;
                bool hasExpiration = rnd.Next(0, 2) == 0;
                if (addString)
                    db.StringSet(keys[i], Guid.NewGuid().ToString());
                else
                    db.SortedSetAdd(keys[i], Guid.NewGuid().ToString(), expirationOrScore);

                if (hasExpiration || forceExpirationaddition)
                {
                    if (expirationOrScore < ExpiredKeyDeletionScanFrequencySecs)
                        totalKeysThatWillExpire++;
                    ClassicAssert.IsTrue(db.KeyExpire(keys[i], TimeSpan.FromSeconds(expirationOrScore)));
                }
            }

            return totalKeysThatWillExpire;
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