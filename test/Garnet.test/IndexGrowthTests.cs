﻿// Copyright (c) Microsoft Corporation.
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
    public class IndexGrowthTests
    {
        GarnetApplication server;
        private int indexResizeTaskDelaySeconds = 10;
        private int indexResizeWaitCycles = 2;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public async Task TearDown()
        {
            if (server != null)
            {
                await server.StopAsync();
            }
            
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public async Task IndexGrowthTest()
        {
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, lowMemory: true, indexSize: "64", indexMaxSize: "128", indexResizeFrequencySecs: indexResizeTaskDelaySeconds);
            await server.RunAsync();

            var store = server.Provider.StoreWrapper.store;

            RedisKey[] keys = ["abcdkey", "bcdekey", "cdefkey", "defgkey", "efghkey", "fghikey", "ghijkey", "hijkkey"];
            RedisValue[] values = ["abcdval", "bcdeval", "cdefval", "defgval", "efghval", "fghival", "ghijval", "hijkval"];

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(0, store.OverflowBucketAllocations);
                ClassicAssert.AreEqual(1, store.IndexSize);

                for (int i = 0; i < keys.Length; i++)
                {
                    db.StringSet(keys[i], values[i]);
                }

                ClassicAssert.AreEqual(values[0], db.StringGet(keys[0]).ToString());
                ClassicAssert.AreEqual(1, store.OverflowBucketAllocations);

                // Wait for the resizing to happen
                for (int waitCycles = 0; waitCycles < indexResizeWaitCycles; waitCycles++)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(indexResizeTaskDelaySeconds));
                    if (store.IndexSize > 1) break;
                }

                ClassicAssert.AreEqual(2, store.IndexSize);
                // Check if entry created before resizing is still accessible.
                ClassicAssert.AreEqual(values[0], db.StringGet(keys[0]).ToString());
            }
        }

        [Test]
        public async Task ObjectStoreIndexGrowthTest()
        {
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, lowMemory: true, objectStoreIndexSize: "64", objectStoreIndexMaxSize: "128", indexResizeFrequencySecs: indexResizeTaskDelaySeconds);
            await server.RunAsync();

            var objectStore = server.Provider.StoreWrapper.objectStore;

            RedisKey[] keys = ["abcdkey", "bcdekey", "cdefkey", "defgkey", "efghkey", "fghikey", "ghijkey", "hijkkey"];
            RedisValue[] values = ["abcdval", "bcdeval", "cdefval", "defgval", "efghval", "fghival", "ghijval", "hijkval"];

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(0, objectStore.OverflowBucketAllocations);
                ClassicAssert.AreEqual(1, objectStore.IndexSize);

                for (int i = 0; i < keys.Length; i++)
                {
                    db.SetAdd(keys[i], values[i]);
                }

                VerifyObjectStoreSetMembers(db, keys, values);
                ClassicAssert.AreEqual(1, objectStore.OverflowBucketAllocations);

                // Wait for the resizing to happen
                for (int waitCycles = 0; waitCycles < indexResizeWaitCycles; waitCycles++)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(indexResizeTaskDelaySeconds));
                    if (objectStore.IndexSize > 1) break;
                }

                ClassicAssert.AreEqual(2, objectStore.IndexSize);
                VerifyObjectStoreSetMembers(db, keys, values);
            }
        }

        private static void VerifyObjectStoreSetMembers(IDatabase db, RedisKey[] keys, RedisValue[] values)
        {
            for (int i = 0; i < keys.Length; i++)
            {
                var members = db.SetMembers(keys[i]);
                ClassicAssert.AreEqual(1, members.Length, $"key {keys[i]}");
                ClassicAssert.AreEqual(values[i], members[0].ToString());
            }
        }

        [Test]
        public async Task IndexGrowthTestWithDiskReadAndCheckpoint()
        {
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, lowMemory: true, indexSize: "512", indexMaxSize: "1k", indexResizeFrequencySecs: indexResizeTaskDelaySeconds);
            await server.RunAsync();

            var store = server.Provider.StoreWrapper.store;

            RedisKey[] keys = ["abcdkey", "bcdekey", "cdefkey", "defgkey", "efghkey", "fghikey", "ghijkey", "hijkkey"];
            RedisValue[] values = ["abcdval", "bcdeval", "cdefval", "defgval", "efghval", "fghival", "ghijval", "hijkval"];

            Random rnd = new Random(42);
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(8, store.IndexSize);

                for (int i = 0; i < keys.Length; i++)
                {
                    db.StringSet(keys[i], values[i]);
                }

                ClassicAssert.AreEqual(values[0], db.StringGet(keys[0]).ToString());

                // Add lot more entries to push earlier keys to disk as server is started with low memory
                for (int i = 0; i < 1000; i++)
                {
                    var randkey = "rand" + rnd.Next(1_000_000);
                    db.StringSet(randkey, "randval");
                }

                // Verify that the earlier keys are still accessible
                ClassicAssert.AreEqual(values[0], db.StringGet(keys[0]).ToString());

                //Wait for the resizing to happen
                for (int waitCycles = 0; waitCycles < indexResizeWaitCycles; waitCycles++)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(indexResizeTaskDelaySeconds));
                    if (store.IndexSize > 8) break;
                }

                ClassicAssert.AreEqual(16, store.IndexSize);

                // Check if entry created before resizing is still accessible.
                ClassicAssert.AreEqual(values[0], db.StringGet(keys[0]).ToString());

                // Issue and wait for DB save
                var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
                server.Save(SaveType.BackgroundSave);
                while (server.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);
            }

            //server.Dispose(false);
            await server.StopAsync();
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, tryRecover: true, lowMemory: true, indexSize: "512", indexMaxSize: "1k");
            await server.RunAsync();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                // Verify that entry created before checkpoint is still accessible
                ClassicAssert.AreEqual(values[0], db.StringGet(keys[0]).ToString());
            }
        }

        [Test]
        public async Task ObjectStoreIndexGrowthTestWithDiskReadAndCheckpoint()
        {
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, lowMemory: true, objectStoreIndexSize: "512", objectStoreIndexMaxSize: "1k", indexResizeFrequencySecs: indexResizeTaskDelaySeconds);
            await server.RunAsync();

            var objectStore = server.Provider.StoreWrapper.objectStore;

            RedisKey[] keys = ["abcdkey", "bcdekey", "cdefkey", "defgkey", "efghkey", "fghikey", "ghijkey", "hijkkey"];
            RedisValue[] values = ["abcdval", "bcdeval", "cdefval", "defgval", "efghval", "fghival", "ghijval", "hijkval"];

            Random rnd = new Random(42);
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(8, objectStore.IndexSize);

                for (int i = 0; i < keys.Length; i++)
                {
                    db.SetAdd(keys[i], values[i]);
                }

                VerifyObjectStoreSetMembers(db, keys, values);

                // Add lot more entries to push earlier keys to disk as server is started with low memory
                for (int i = 0; i < 1000; i++)
                {
                    var randkey = "rand" + rnd.Next(1_000_000);
                    db.SetAdd(randkey, "randval");
                }

                // Verify that the earlier keys are still accessible
                VerifyObjectStoreSetMembers(db, keys, values);

                //Wait for the resizing to happen
                for (int waitCycles = 0; waitCycles < indexResizeWaitCycles; waitCycles++)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(indexResizeTaskDelaySeconds));
                    if (objectStore.IndexSize > 8) break;
                }

                ClassicAssert.AreEqual(16, objectStore.IndexSize);

                // Check if entry created before resizing is still accessible.
                VerifyObjectStoreSetMembers(db, keys, values);

                // Issue and wait for DB save
                var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
                server.Save(SaveType.BackgroundSave);
                while (server.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);
            }

            //server.Dispose(false);
            await server.StopAsync();
            server = TestUtils.CreateGarnetApplication(TestUtils.MethodTestDir, tryRecover: true, lowMemory: true, objectStoreIndexSize: "512", objectStoreIndexMaxSize: "1k");
            await server.RunAsync();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                // Verify that entry created before checkpoint is still accessible
                VerifyObjectStoreSetMembers(db, keys, values);
            }
        }
    }
}