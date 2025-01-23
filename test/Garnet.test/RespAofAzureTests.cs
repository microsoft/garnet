// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespAofAzureTests
    {
        GarnetApplication server;
        static readonly SortedSetEntry[] entries =
        [
            new SortedSetEntry("a", 1),
            new SortedSetEntry("b", 2),
            new SortedSetEntry("c", 3),
            new SortedSetEntry("d", 4),
            new SortedSetEntry("e", 5),
            new SortedSetEntry("f", 6),
            new SortedSetEntry("g", 7),
            new SortedSetEntry("h", 8),
            new SortedSetEntry("i", 9),
            new SortedSetEntry("j", 10)
        ];

        [SetUp]
        public async Task Setup()
        {
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, enableAOF: true, lowMemory: true, UseAzureStorage: true);
            await server.RunAsync();
        }

        [TearDown]
        public async Task TearDown()
        {
            if (server != null)
            {
                await server.StopAsync();
            }
        }

        [Test]
        public async Task AofUpsertStoreRecoverTest()
        {
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.StringSet("SeAofUpsertRecoverTestKey1", "SeAofUpsertRecoverTestValue1");
                db.StringSet("SeAofUpsertRecoverTestKey2", "SeAofUpsertRecoverTestValue2");
            }

            server.Store.CommitAOF(true);
            await server.StopAsync();
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, UseAzureStorage: true);
            await server.RunAsync();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                var recoveredValue = db.StringGet("SeAofUpsertRecoverTestKey1");
                ClassicAssert.AreEqual("SeAofUpsertRecoverTestValue1", recoveredValue.ToString());
                recoveredValue = db.StringGet("SeAofUpsertRecoverTestKey2");
                ClassicAssert.AreEqual("SeAofUpsertRecoverTestValue2", recoveredValue.ToString());
            }
        }

        [Test]
        public async Task AofUpsertStoreAutoCommitRecoverTest()
        {
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.StringSet("SeAofUpsertRecoverTestKey1", "SeAofUpsertRecoverTestValue1");
                db.StringSet("SeAofUpsertRecoverTestKey2", "SeAofUpsertRecoverTestValue2");
            }

            server.Store.WaitForCommit();
            await server.StopAsync();
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, UseAzureStorage: true);
            await server.RunAsync();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                var recoveredValue = db.StringGet("SeAofUpsertRecoverTestKey1");
                ClassicAssert.AreEqual("SeAofUpsertRecoverTestValue1", recoveredValue.ToString());
                recoveredValue = db.StringGet("SeAofUpsertRecoverTestKey2");
                ClassicAssert.AreEqual("SeAofUpsertRecoverTestValue2", recoveredValue.ToString());
            }
        }

        [Test]
        public async Task AofUpsertStoreAutoCommitCommitWaitRecoverTest()
        {
            await server.StopAsync();
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, tryRecover: false, enableAOF: true, commitWait: true, UseAzureStorage: true);
            await server.RunAsync();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.StringSet("SeAofUpsertRecoverTestKey1", "SeAofUpsertRecoverTestValue1");
                db.StringSet("SeAofUpsertRecoverTestKey2", "SeAofUpsertRecoverTestValue2");
            }
            await server.StopAsync();
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, UseAzureStorage: true);
            await server.RunAsync();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                var recoveredValue = db.StringGet("SeAofUpsertRecoverTestKey1");
                ClassicAssert.AreEqual("SeAofUpsertRecoverTestValue1", recoveredValue.ToString());
                recoveredValue = db.StringGet("SeAofUpsertRecoverTestKey2");
                ClassicAssert.AreEqual("SeAofUpsertRecoverTestValue2", recoveredValue.ToString());
            }
        }

        [Test]
        public async Task AofUpsertStoreCkptRecoverTest()
        {
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                db.StringSet("SeAofUpsertRecoverTestKey1", "SeAofUpsertRecoverTestValue1");
                db.StringSet("SeAofUpsertRecoverTestKey2", "SeAofUpsertRecoverTestValue2");

                // Issue and wait for DB save
                var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
                server.Save(SaveType.BackgroundSave);
                while (server.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);

                // Issue additional SET for AOF to process
                db.StringSet("SeAofUpsertRecoverTestKey3", "SeAofUpsertRecoverTestValue3");
            }

            server.Store.CommitAOF(true);
            await server.StopAsync();
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, UseAzureStorage: true);
            await server.RunAsync();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                var recoveredValue = db.StringGet("SeAofUpsertRecoverTestKey1");
                ClassicAssert.AreEqual("SeAofUpsertRecoverTestValue1", recoveredValue.ToString());
                recoveredValue = db.StringGet("SeAofUpsertRecoverTestKey2");
                ClassicAssert.AreEqual("SeAofUpsertRecoverTestValue2", recoveredValue.ToString());
                recoveredValue = db.StringGet("SeAofUpsertRecoverTestKey3");
                ClassicAssert.AreEqual("SeAofUpsertRecoverTestValue3", recoveredValue.ToString());
            }
        }

        [Test]
        public async Task AofRMWStoreRecoverTest()
        {
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.StringSet("SeAofUpsertRecoverTestKey1", "SeAofUpsertRecoverTestValue1", expiry: TimeSpan.FromDays(1), when: When.NotExists);
                db.StringSet("SeAofUpsertRecoverTestKey2", "SeAofUpsertRecoverTestValue2", expiry: TimeSpan.FromDays(1), when: When.NotExists);
            }

            server.Store.CommitAOF(true);
            await server.StopAsync();
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, UseAzureStorage: true);
            await server.RunAsync();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                var recoveredValue = db.StringGet("SeAofUpsertRecoverTestKey1");
                ClassicAssert.AreEqual("SeAofUpsertRecoverTestValue1", recoveredValue.ToString());
                recoveredValue = db.StringGet("SeAofUpsertRecoverTestKey2");
                ClassicAssert.AreEqual("SeAofUpsertRecoverTestValue2", recoveredValue.ToString());
            }
        }

        [Test]
        public async Task AofDeleteStoreRecoverTest()
        {
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.StringSet("SeAofDeleteRecoverTestKey1", "SeAofDeleteRecoverTestKey1");
                db.StringSet("SeAofDeleteRecoverTestKey2", "SeAofDeleteRecoverTestKey2");

                var val = (string)db.StringGet("SeAofDeleteRecoverTestKey1");
                ClassicAssert.AreEqual("SeAofDeleteRecoverTestKey1", val);

                val = (string)db.StringGet("SeAofDeleteRecoverTestKey2");
                ClassicAssert.AreEqual("SeAofDeleteRecoverTestKey2", val);

                db.KeyDelete("SeAofDeleteRecoverTestKey1");
                val = (string)db.StringGet("SeAofDeleteRecoverTestKey1");
                ClassicAssert.AreEqual(null, val);
            }

            server.Store.CommitAOF(true);
            await server.StopAsync();
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, UseAzureStorage: true);
            await server.RunAsync();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                var val = (string)db.StringGet("SeAofDeleteRecoverTestKey1");
                ClassicAssert.AreEqual(null, val);

                val = (string)db.StringGet("SeAofDeleteRecoverTestKey2");
                ClassicAssert.AreEqual("SeAofDeleteRecoverTestKey2", val);
            }
        }

        [Test]
        public async Task AofExpiryRMWStoreRecoverTest()
        {
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.StringSet("AofExpiryRMWStoreRecoverTestKey1", "AofExpiryRMWStoreRecoverTestValue1", expiry: TimeSpan.FromDays(1), when: When.NotExists);
                db.StringSet("AofExpiryRMWStoreRecoverTestKey2", "AofExpiryRMWStoreRecoverTestValue2", expiry: TimeSpan.FromSeconds(1), when: When.NotExists);
                Thread.Sleep(2000);
                db.StringSet("AofExpiryRMWStoreRecoverTestKey1", "AofExpiryRMWStoreRecoverTestValue3", expiry: TimeSpan.FromDays(1), when: When.NotExists);
                db.StringSet("AofExpiryRMWStoreRecoverTestKey2", "AofExpiryRMWStoreRecoverTestValue4", expiry: TimeSpan.FromSeconds(100), when: When.NotExists);

                var recoveredValue = db.StringGet("AofExpiryRMWStoreRecoverTestKey1");
                ClassicAssert.AreEqual("AofExpiryRMWStoreRecoverTestValue1", recoveredValue.ToString());
                recoveredValue = db.StringGet("AofExpiryRMWStoreRecoverTestKey2");
                ClassicAssert.AreEqual("AofExpiryRMWStoreRecoverTestValue4", recoveredValue.ToString());
            }

            server.Store.CommitAOF(true);
            await server.StopAsync();
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, UseAzureStorage: true);
            await server.RunAsync();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                var recoveredValue = db.StringGet("AofExpiryRMWStoreRecoverTestKey1");
                ClassicAssert.AreEqual("AofExpiryRMWStoreRecoverTestValue1", recoveredValue.ToString());
                recoveredValue = db.StringGet("AofExpiryRMWStoreRecoverTestKey2");
                ClassicAssert.AreEqual("AofExpiryRMWStoreRecoverTestValue4", recoveredValue.ToString());
            }
        }

        [Test]
        public async Task AofRMWObjectStoreRecoverTest()
        {
            var key = "AofRMWObjectStoreRecoverTestKey";

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                var added = db.SortedSetAdd(key, entries);

                var score = db.SortedSetScore(key, "a");
                ClassicAssert.True(score.HasValue);
                ClassicAssert.AreEqual(1, score.Value);

                score = db.SortedSetScore(key, "x");
                ClassicAssert.False(score.HasValue);
            }

            server.Store.CommitAOF(true);
            await server.StopAsync();
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, UseAzureStorage: true);
            await server.RunAsync();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                var score = db.SortedSetScore(key, "a");
                ClassicAssert.True(score.HasValue);
                ClassicAssert.AreEqual(1, score.Value);

                score = db.SortedSetScore(key, "x");
                ClassicAssert.False(score.HasValue);
            }
        }

        [Test]
        public async Task AofDeleteObjectStoreRecoverTest()
        {
            var key1 = "AofDeleteObjectStoreRecoverTestKey1";
            var key2 = "AofDeleteObjectStoreRecoverTestKey2";
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                var added = db.SortedSetAdd(key1, entries);
                var score = db.SortedSetScore(key1, "a");
                ClassicAssert.True(score.HasValue);
                ClassicAssert.AreEqual(1, score.Value);

                score = db.SortedSetScore(key1, "x");
                ClassicAssert.False(score.HasValue);

                added = db.SortedSetAdd(key2, entries);
                score = db.SortedSetScore(key2, "a");
                ClassicAssert.True(score.HasValue);
                ClassicAssert.AreEqual(1, score.Value);

                score = db.SortedSetScore(key2, "x");
                ClassicAssert.False(score.HasValue);

                db.KeyDelete(key1);
            }

            server.Store.CommitAOF(true);
            await server.StopAsync();
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, UseAzureStorage: true);
            await server.RunAsync();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                var exist1 = db.KeyExists(key1);
                ClassicAssert.IsFalse(exist1);
                var exist2 = db.KeyExists(key2);
                ClassicAssert.IsTrue(exist2);
            }
        }

        [Test]
        public async Task AofRMWObjectStoreCopyUpdateRecoverTest()
        {
            var key = "AofRMWObjectStoreRecoverTestKey";

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (int i = 0; i < 100; i++)
                {
                    SortedSetEntry[] entry = [new SortedSetEntry("a", 1), new SortedSetEntry("b", 2)];
                    db.SortedSetAdd(key + i, entry);

                    var score = db.SortedSetScore(key + i, "a");
                    ClassicAssert.True(score.HasValue);
                    ClassicAssert.AreEqual(1, score.Value);

                }
                SortedSetEntry[] newEntries = [new SortedSetEntry("bbbb", 4)];
                db.SortedSetAdd("AofRMWObjectStoreRecoverTestKey" + 1, newEntries);
            }
            server.Store.CommitAOF(true);
            await server.StopAsync();
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, UseAzureStorage: true);
            await server.RunAsync();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                var score = db.SortedSetScore(key + 1, "bbbb");
                ClassicAssert.True(score.HasValue);
                ClassicAssert.AreEqual(4, score.Value);

                score = db.SortedSetScore(key + 1, "x");
                ClassicAssert.False(score.HasValue);
            }
        }

        [Test]
        public async Task AofMultiRMWStoreCkptRecoverTest()
        {
            long ret = 0;
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
                ret = db.StringIncrement("key1", 2);
                ClassicAssert.AreEqual(2, ret);

                server.Save(SaveType.BackgroundSave);
                long lastSave = server.LastSave().Ticks;
                while (lastSave == DateTimeOffset.FromUnixTimeSeconds(0).Ticks)
                {
                    Thread.Yield();
                    lastSave = server.LastSave().Ticks;
                }

                ret = db.StringIncrement("key1", 2);
                ClassicAssert.AreEqual(4, ret);

                // Wait one second to ensure that the last save time is updated
                Thread.Sleep(1000);

                server.Save(SaveType.BackgroundSave);
                long lastSave2 = server.LastSave().Ticks;
                while (lastSave2 == lastSave)
                {
                    Thread.Yield();
                    lastSave2 = server.LastSave().Ticks;
                }

                ret = db.StringIncrement("key1", 2);
                ClassicAssert.AreEqual(6, ret);
            }

            server.Store.CommitAOF(true);
            await server.StopAsync();
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, UseAzureStorage: true);
            await server.RunAsync();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                ret = db.StringIncrement("key1", 2);
                ClassicAssert.AreEqual(8, ret);
            }
        }
    }
}