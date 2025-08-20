// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespInfoTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: true, latencyMonitor: true, metricsSamplingFreq: 1, lowMemory: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void ResetStatsTest()
        {
            TimeSpan metricsUpdateDelay = TimeSpan.FromSeconds(1.1);
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var infoResult = db.Execute("INFO").ToString();
            var infoResultArr = infoResult.Split("\r\n");
            var totalFound = infoResultArr.First(x => x.StartsWith("total_found"));
            ClassicAssert.AreEqual("total_found:0", totalFound, "Expected total_found to be 0 after starting the server.");

            var key = "myKey";
            var val = "myKeyValue";
            db.StringSet(key, val);
            ClassicAssert.AreEqual(val, db.StringGet(key).ToString());
            Thread.Sleep(metricsUpdateDelay);

            infoResult = db.Execute("INFO").ToString();
            infoResultArr = infoResult.Split("\r\n");
            totalFound = infoResultArr.First(x => x.StartsWith("total_found"));
            ClassicAssert.AreEqual("total_found:1", totalFound, "Expected total_foudn to be incremented to 1 after a successful request.");

            var result = db.Execute("INFO", "RESET");
            ClassicAssert.IsNotNull(result);
            Thread.Sleep(metricsUpdateDelay);

            infoResult = db.Execute("INFO").ToString();
            infoResultArr = infoResult.Split("\r\n");
            totalFound = infoResultArr.First(x => x.StartsWith("total_found"));
            ClassicAssert.AreEqual("total_found:0", totalFound, "Expected total_found to be reset to 0 after INFO RESET command");

            ClassicAssert.AreEqual(val, db.StringGet(key).ToString(), "Expected the value to match what was set earlier.");
            Thread.Sleep(metricsUpdateDelay);

            infoResult = db.Execute("INFO").ToString();
            infoResultArr = infoResult.Split("\r\n");
            totalFound = infoResultArr.First(x => x.StartsWith("total_found"));
            ClassicAssert.AreEqual("total_found:1", totalFound, "Expected total_found to be one after sending one successful request");
        }

        [Test]
        public async Task InfoHlogScanTest()
        {
            TimeSpan metricsUpdateDelay = TimeSpan.FromSeconds(1.1);
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // hydrate main store
            var startingHA = server.Provider.StoreWrapper.store.Log.HeadAddress;
            HydrateStore(db, (db, key, value) => db.StringSet(key, value), () => startingHA != server.Provider.StoreWrapper.store.Log.HeadAddress);
            // hydrate object store
            var startingHAObj = server.Provider.StoreWrapper.objectStore.Log.HeadAddress;
            HydrateStore(db, (db, key, value) => db.SetAdd(key, value), () => startingHAObj != server.Provider.StoreWrapper.objectStore.Log.HeadAddress);

            await Task.Delay(5);

            var objStoreStartHeadAddr = server.Provider.StoreWrapper.objectStore.Log.HeadAddress;
            while (objStoreStartHeadAddr == server.Provider.StoreWrapper.objectStore.Log.ReadOnlyAddress)
            {
                db.SetAdd(Guid.NewGuid().ToString(), Guid.NewGuid().ToString());
            }

            // now we have a differentiated region for mutable and immutable region in object store and main store
            var result = db.Execute("INFO", "HLOGSCAN");

            // HK TODO: Use regex to assert more info
            ClassicAssert.IsTrue(!result.IsNull);
        }

        private void HydrateStore(IDatabase db, Action<IDatabase, string, string> setAction, Func<bool> predicate)
        {
            const int numKeysToAtleastMake = 1000;
            const int percentKeysWithExpirationsButNotExpired = 30;
            const int percentKeysWithExpirationAndExpired = 30;
            const int percentageKeysToDeleteLater = 40;
            const int percentageKeysWeRcuLater = 20;

            var random = new Random();

            var keysWeRcuOn = new List<string>();
            var keysWeDelete = new List<string>();

            // add data till there is an immutable region, and atleast 500 records
            int totalRecords = 0;
            while (predicate() && totalRecords < numKeysToAtleastMake)
            {
                totalRecords++;
                var chance = random.Next(0, 100);
                var key = Guid.NewGuid().ToString();
                var value = Guid.NewGuid().ToString();

                bool eligibleForTombstoning = true;

                if (chance < percentKeysWithExpirationsButNotExpired)
                {
                    setAction(db, key, value);
                    db.KeyExpire(key, TimeSpan.FromHours(2));
                }
                else if (chance > percentKeysWithExpirationsButNotExpired &&
                    chance < percentKeysWithExpirationsButNotExpired + percentKeysWithExpirationAndExpired)
                {

                    setAction(db, key, value);
                    db.KeyExpire(key, TimeSpan.FromMilliseconds(2));
                    eligibleForTombstoning = false; // will be expired already
                }
                else
                {
                    setAction(db, key, value);
                }

                if (eligibleForTombstoning)
                {
                    chance = random.Next(0, 100);
                    // now decide whether we RCU or Delete this guy
                    if (chance < percentageKeysToDeleteLater)
                    {
                        keysWeDelete.Add(key);
                    }
                    else if (chance > percentageKeysToDeleteLater && chance < percentageKeysToDeleteLater + percentageKeysWeRcuLater)
                    {
                        keysWeRcuOn.Add(key);
                    }
                }
            }

            foreach (string keyTodel in keysWeDelete)
            {
                db.KeyDelete(keyTodel);
            }

            foreach (string keyToRcu in keysWeRcuOn)
            {
                setAction(db, keyToRcu, Guid.NewGuid().ToString());
            }
        }
    }
}
