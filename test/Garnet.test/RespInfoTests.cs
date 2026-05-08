// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class RespInfoTests : AllureTestBase
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
            TestUtils.OnTearDown();
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
        public void UptimeIncreasesAcrossInfoCalls()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            static long ParseUptime(string info) =>
                long.Parse(info.Split("\r\n").First(x => x.StartsWith("uptime_in_seconds:")).Split(':')[1]);

            var first = ParseUptime(db.Execute("INFO", "SERVER").ToString());
            ClassicAssert.GreaterOrEqual(first, 0);

            Thread.Sleep(TimeSpan.FromSeconds(1.1));

            var second = ParseUptime(db.Execute("INFO", "SERVER").ToString());
            ClassicAssert.Greater(second, first, "uptime_in_seconds should increase between INFO calls");
        }

        [Test]
        [TestCase("ALL")]
        [TestCase("DEFAULT")]
        [TestCase("EVERYTHING")]
        public void InfoSectionOptionsTest(string option)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var infoResult = db.Execute("INFO", option).ToString();
            ClassicAssert.IsNotNull(infoResult);
            ClassicAssert.IsNotEmpty(infoResult);

            // All options should include these core sections
            ClassicAssert.IsTrue(infoResult.Contains("# Server"), $"INFO {option} should contain Server section");
            ClassicAssert.IsTrue(infoResult.Contains("# Memory"), $"INFO {option} should contain Memory section");
            ClassicAssert.IsTrue(infoResult.Contains("# Stats"), $"INFO {option} should contain Stats section");
            ClassicAssert.IsTrue(infoResult.Contains("# Clients"), $"INFO {option} should contain Clients section");
            ClassicAssert.IsTrue(infoResult.Contains("# Keyspace"), $"INFO {option} should contain Keyspace section");

            // ALL excludes Modules section; DEFAULT and EVERYTHING include it
            if (option == "ALL")
            {
                ClassicAssert.IsFalse(infoResult.Contains("# Modules"), "INFO ALL should not contain Modules section");
            }
            else
            {
                ClassicAssert.IsTrue(infoResult.Contains("# Modules"), $"INFO {option} should contain Modules section");
            }

            // All three options are based on DefaultInfo which excludes expensive/verbose sections
            ClassicAssert.IsFalse(infoResult.Contains("MainStoreHashTableDistribution"), $"INFO {option} should not contain StoreHashTable section");
            ClassicAssert.IsFalse(infoResult.Contains("ObjectStoreHashTableDistribution"), $"INFO {option} should not contain ObjectStoreHashTable section");
            ClassicAssert.IsFalse(infoResult.Contains("MainStoreDeletedRecordRevivification"), $"INFO {option} should not contain StoreReviv section");
            ClassicAssert.IsFalse(infoResult.Contains("ObjectStoreDeletedRecordRevivification"), $"INFO {option} should not contain ObjectStoreReviv section");
            ClassicAssert.IsFalse(infoResult.Contains("MainStoreHLogScan"), $"INFO {option} should not contain HLogScan section");
            ClassicAssert.IsFalse(infoResult.Contains("# Commandstats"), $"INFO {option} should not contain Commandstats section");
        }

        [Test]
        public void InfoDefaultMatchesNoArgsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var infoNoArgs = db.Execute("INFO").ToString();
            var infoDefault = db.Execute("INFO", "DEFAULT").ToString();

            // Both should return the same set of section headers
            var noArgsSections = GetSectionHeaders(infoNoArgs);
            var defaultSections = GetSectionHeaders(infoDefault);

            CollectionAssert.AreEquivalent(noArgsSections, defaultSections,
                "INFO (no args) and INFO DEFAULT should return the same sections");
        }

        [Test]
        public void InfoAllWithModulesEqualsEverythingTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var infoEverything = db.Execute("INFO", "EVERYTHING").ToString();
            var infoAllModules = db.Execute("INFO", "ALL", "MODULES").ToString();

            var everythingSections = GetSectionHeaders(infoEverything);
            var allModulesSections = GetSectionHeaders(infoAllModules);

            CollectionAssert.AreEquivalent(everythingSections, allModulesSections,
                "INFO EVERYTHING and INFO ALL MODULES should return the same sections");
        }

        private static List<string> GetSectionHeaders(string infoOutput)
        {
            ClassicAssert.IsNotNull(infoOutput, "INFO output should not be null");
            ClassicAssert.IsNotEmpty(infoOutput, "INFO output should not be empty");

            return infoOutput.Split("\r\n")
                .Where(line => line.StartsWith("# "))
                .Select(line => line.TrimStart('#', ' '))
                .OrderBy(s => s)
                .ToList();
        }

        [Test]
        public async Task InfoHlogScanTest()
        {
            var metricsUpdateDelay = TimeSpan.FromSeconds(1.1);
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // hydrate
            var startingHA = server.Provider.StoreWrapper.store.Log.HeadAddress;
            await HydrateStore(db, (db, key, value) => db.StringSetAsync(key, value),
                () => startingHA == server.Provider.StoreWrapper.store.Log.HeadAddress).ConfigureAwait(false);

            // Wait for the immediate expirations to kick in
            await Task.Delay(500).ConfigureAwait(false);

            // now we have a differentiated region for mutable and immutable region in object store and main store
            var result = await db.ExecuteAsync("INFO", "HLOGSCAN").ConfigureAwait(false);

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
                    await setAction(db, key, value).ConfigureAwait(false);
                    _ = await db.KeyExpireAsync(key, TimeSpan.FromHours(2)).ConfigureAwait(false);
                }
                else if (chance > percentKeysWithExpirationsButNotExpired &&
                    chance < percentKeysWithExpirationsButNotExpired + percentKeysWithExpirationAndExpired)
                {
                    await setAction(db, key, value).ConfigureAwait(false);
                    _ = await db.KeyExpireAsync(key, TimeSpan.FromMilliseconds(2)).ConfigureAwait(false);
                    eligibleForTombstoning = false; // will be expired already
                }
                else
                {
                    await setAction(db, key, value).ConfigureAwait(false);
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
                _ = await db.KeyDeleteAsync(keyTodel).ConfigureAwait(false);
            }

            foreach (var keyToRcu in keysWeRcuOn)
            {
                await setAction(db, keyToRcu, Guid.NewGuid().ToString()).ConfigureAwait(false);
            }
        }
    }
}