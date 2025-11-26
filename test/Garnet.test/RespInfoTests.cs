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
            var metricsUpdateDelay = TimeSpan.FromSeconds(1.1);
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // hydrate
            var startingHA = server.Provider.StoreWrapper.store.Log.HeadAddress;
            var startingHAObj = server.Provider.StoreWrapper.objectStore.Log.HeadAddress;
            await Task.WhenAll(
                HydrateStore(db, (db, key, value) => db.StringSetAsync(key, value), () => startingHA == server.Provider.StoreWrapper.store.Log.HeadAddress),
                HydrateStore(db, (db, key, value) => db.SetAddAsync(key, value), () => startingHAObj == server.Provider.StoreWrapper.objectStore.Log.HeadAddress)
            );

            // Wait for the immediate expirations to kick in
            await Task.Delay(500);

            // now we have a differentiated region for mutable and immutable region in object store and main store
            var result = await db.ExecuteAsync("INFO", "HLOGSCAN");

            ClassicAssert.IsTrue(!result.IsNull);
        }

        private static async Task HydrateStore(IDatabase db, Func<IDatabase, string, string, Task> setAction, Func<bool> predicate)
        {
            const int numKeysToAtleastMake = 1000;
            const int percentKeysWithExpirationsButNotExpired = 30;
            const int percentKeysWithExpirationAndExpired = 30;
            const int percentKeysToDeleteLater = 40;
            const int percentKeysWeRcuLater = 20;
            var random = new Random();
            var keysWeRcuOn = new List<string>();
            var keysWeDelete = new List<string>();
            // add data till there is an immutable region, and atleast 500 records
            var totalRecords = 0;
            // keep going till head and tail departur and min key insertions hitting
            while (predicate() || totalRecords < numKeysToAtleastMake)
            {
                totalRecords++;
                var chance = random.Next(0, 100);
                var key = Guid.NewGuid().ToString();
                var value = Guid.NewGuid().ToString();

                var eligibleForTombstoning = true;

                if (chance < percentKeysWithExpirationsButNotExpired)
                {
                    await setAction(db, key, value);
                    _ = await db.KeyExpireAsync(key, TimeSpan.FromHours(2));
                }
                else if (chance > percentKeysWithExpirationsButNotExpired &&
                    chance < percentKeysWithExpirationsButNotExpired + percentKeysWithExpirationAndExpired)
                {
                    await setAction(db, key, value);
                    _ = await db.KeyExpireAsync(key, TimeSpan.FromMilliseconds(2));
                    eligibleForTombstoning = false; // will be expired already
                }
                else
                {
                    await setAction(db, key, value);
                }

                if (eligibleForTombstoning)
                {
                    chance = random.Next(0, 100);
                    // now decide whether we RCU or Delete this guy
                    if (chance < percentKeysToDeleteLater)
                    {
                        keysWeDelete.Add(key);
                    }
                    else if (chance > percentKeysToDeleteLater && chance < percentKeysToDeleteLater + percentKeysWeRcuLater)
                    {
                        keysWeRcuOn.Add(key);
                    }
                }
            }

            foreach (var keyTodel in keysWeDelete)
            {
                _ = await db.KeyDeleteAsync(keyTodel);
            }

            foreach (var keyToRcu in keysWeRcuOn)
            {
                await setAction(db, keyToRcu, Guid.NewGuid().ToString());
            }
        }
    }
}