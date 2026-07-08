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
    public class RespInfoTests : TestBase
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

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void ResetStatsTest(RedisProtocol protocol)
        {
            TimeSpan metricsUpdateDelay = TimeSpan.FromSeconds(1.1);
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
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

        [TestCase("ALL", RedisProtocol.Resp2)]
        [TestCase("ALL", RedisProtocol.Resp3)]
        [TestCase("DEFAULT", RedisProtocol.Resp2)]
        [TestCase("DEFAULT", RedisProtocol.Resp3)]
        [TestCase("EVERYTHING", RedisProtocol.Resp2)]
        [TestCase("EVERYTHING", RedisProtocol.Resp3)]
        public void InfoSectionOptionsTest(string option, RedisProtocol protocol)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
            var db = redis.GetDatabase(0);

            var infoResult = db.Execute("INFO", option).ToString();
            ClassicAssert.IsNotNull(infoResult);
            ClassicAssert.IsNotEmpty(infoResult);

            // All options should include these core sections
            ClassicAssert.IsTrue(infoResult.Contains("# Server"), $"INFO {option} should contain Server section");
            ClassicAssert.IsTrue(infoResult.Contains("# Memory"), $"INFO {option} should contain Memory section");
            ClassicAssert.IsTrue(infoResult.Contains("# Stats"), $"INFO {option} should contain Stats section");
            ClassicAssert.IsTrue(infoResult.Contains("# Clients"), $"INFO {option} should contain Clients section");

            // KEYSPACE is scan-based and therefore excluded from the default/ALL/EVERYTHING sets
            // (like HLOGSCAN); it is only produced on an explicit `INFO KEYSPACE` request.
            ClassicAssert.IsFalse(infoResult.Contains("# Keyspace"), $"INFO {option} should not contain Keyspace section");

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

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void InfoDefaultMatchesNoArgsTest(RedisProtocol protocol)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
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

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void InfoKeyspaceEmptyDatabaseTest(RedisProtocol protocol)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
            var db = redis.GetDatabase(0);

            var info = db.Execute("INFO", "KEYSPACE").ToString();

            ClassicAssert.IsTrue(info.Contains("# Keyspace"), "Keyspace section header should be present");
            // No database holds keys yet, so no dbN: lines should be emitted (matches Redis).
            ClassicAssert.IsNull(GetKeyspaceLine(info, 0), "Empty database should not be listed in the Keyspace section");
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void InfoKeyspaceCountsTest(RedisProtocol protocol)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: protocol));
            var db = redis.GetDatabase(0);

            // 3 string keys (2 with a TTL) + 2 list (object) keys (1 with a TTL) => keys=5, expires=3
            db.StringSet("str:1", "v1");
            db.StringSet("str:2", "v2");
            db.StringSet("str:3", "v3");
            ClassicAssert.IsTrue(db.KeyExpire("str:1", TimeSpan.FromHours(1)));
            ClassicAssert.IsTrue(db.KeyExpire("str:2", TimeSpan.FromHours(1)));

            db.ListLeftPush("list:1", [new RedisValue("a"), new RedisValue("b")]);
            db.ListLeftPush("list:2", [new RedisValue("c")]);
            ClassicAssert.IsTrue(db.KeyExpire("list:1", TimeSpan.FromHours(1)));

            var info = db.Execute("INFO", "KEYSPACE").ToString();
            var line = GetKeyspaceLine(info, 0);

            ClassicAssert.AreEqual("db0:keys=5,expires=3,avg_ttl=0", line);

            // The reported key count must be consistent with DBSIZE (same non-expired predicate).
            var dbSize = (long)db.Execute("DBSIZE");
            ClassicAssert.AreEqual(5, dbSize);
        }

        [Test]
        public void InfoKeyspaceExpiredKeysNotCountedTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("live", "v");
            db.StringSet("willexpire", "v");
            ClassicAssert.IsTrue(db.KeyExpire("willexpire", TimeSpan.FromMilliseconds(50)));

            // Poll (bounded) until the key is observed expired, rather than a fixed sleep, to avoid
            // flakiness on slow/loaded CI agents. The key may not be physically reaped yet, but the
            // scan filters expired records regardless.
            var expired = false;
            for (var i = 0; i < 100 && !expired; i++)
            {
                if (!db.KeyExists("willexpire"))
                {
                    expired = true;
                    break;
                }

                Thread.Sleep(20);
            }

            ClassicAssert.IsTrue(expired, "Key with a short TTL should have expired");

            var info = db.Execute("INFO", "KEYSPACE").ToString();
            var line = GetKeyspaceLine(info, 0);

            ClassicAssert.AreEqual("db0:keys=1,expires=0,avg_ttl=0", line);
        }

        [Test]
        public void InfoKeyspaceMultiDatabaseTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());

            var db0 = redis.GetDatabase(0);
            var db1 = redis.GetDatabase(1);

            // db0: 2 keys, 1 with a TTL; db1: 1 key, no TTL; db2: left empty.
            db0.StringSet("a", "1");
            db0.StringSet("b", "2");
            ClassicAssert.IsTrue(db0.KeyExpire("a", TimeSpan.FromHours(1)));

            db1.StringSet("c", "3");

            var info = db0.Execute("INFO", "KEYSPACE").ToString();

            ClassicAssert.AreEqual("db0:keys=2,expires=1,avg_ttl=0", GetKeyspaceLine(info, 0));
            ClassicAssert.AreEqual("db1:keys=1,expires=0,avg_ttl=0", GetKeyspaceLine(info, 1));
            // An untouched database must not appear.
            ClassicAssert.IsNull(GetKeyspaceLine(info, 2), "Empty database should not be listed in the Keyspace section");
        }

        private static string GetKeyspaceLine(string infoOutput, int dbId)
        {
            ClassicAssert.IsNotNull(infoOutput, "INFO output should not be null");
            var prefix = $"db{dbId}:";
            return infoOutput.Split("\r\n").FirstOrDefault(line => line.StartsWith(prefix));
        }
    }
}