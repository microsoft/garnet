// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// SAVE and recovery when the checkpoint directory is on a filesystem whose direct-IO alignment (4 KiB) is larger than
    /// the store's log devices': an in-memory store, whose log device has 512-byte sectors, and a store with storage tiering
    /// on a 512-byte-sector log directory. The checkpoint devices enforce their alignment on every platform (see
    /// <see cref="StrictSectorCheckpointDeviceFactoryCreator"/>), so these tests fail if any checkpoint write or recovery
    /// read is misaligned.
    /// </summary>
    [TestFixture]
    public class CheckpointSectorSizeTests : TestBase
    {
        const int NumStringKeys = 2000;
        const int NumMembers = 500;

        GarnetServer server;
        StrictSectorCheckpointDeviceFactoryCreator deviceFactoryCreator;

        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        GarnetServer CreateServer(bool storageTier, bool tryRecover)
        {
            // Built through GetGarnetServerOptions rather than CreateGarnetServer because only the options object exposes
            // DeviceFactoryCreator, which is how the checkpoint devices' sector size is set.
            var options = TestUtils.GetGarnetServerOptions(
                checkpointDir: TestUtils.MethodTestDir,
                logDir: TestUtils.MethodTestDir,
                endpoint: TestUtils.EndPoint,
                enableCluster: false,
                tryRecover: tryRecover,
                disableStorageTier: !storageTier);

            deviceFactoryCreator = new StrictSectorCheckpointDeviceFactoryCreator(options.StoreCheckpointBaseDirectory);
            options.DeviceFactoryCreator = deviceFactoryCreator;
            return new GarnetServer(options);
        }

        [Test]
        public void SaveAndRecoverWithLargerCheckpointSector([Values] bool storageTier)
        {
            server = CreateServer(storageTier, tryRecover: false);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < NumStringKeys; i++)
                    ClassicAssert.IsTrue(db.StringSet($"string:{i}", $"value:{i}"));

                db.HashSet("hash", [.. Enumerable.Range(0, NumMembers).Select(i => new HashEntry($"field:{i}", $"value:{i}"))]);
                _ = db.ListRightPush("list", [.. Enumerable.Range(0, NumMembers).Select(i => (RedisValue)$"item:{i}")]);
                _ = db.SetAdd("set", [.. Enumerable.Range(0, NumMembers).Select(i => (RedisValue)$"member:{i}")]);
                _ = db.SortedSetAdd("zset", [.. Enumerable.Range(0, NumMembers).Select(i => new SortedSetEntry($"member:{i}", i))]);

                ClassicAssert.AreEqual("OK", db.Execute("SAVE").ToString());
            }

            ClassicAssert.Greater(deviceFactoryCreator.RequestCount, 0, "no checkpoint IO reached the 4 KiB-sector devices");
            ClassicAssert.AreEqual(0, deviceFactoryCreator.MisalignedRequestCount, "misaligned checkpoint IO");

            server.Dispose(false);
            server = CreateServer(storageTier, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < NumStringKeys; i++)
                    ClassicAssert.AreEqual($"value:{i}", db.StringGet($"string:{i}").ToString(), $"string:{i}");

                var hash = db.HashGetAll("hash").ToDictionary(e => e.Name.ToString(), e => e.Value.ToString());
                ClassicAssert.AreEqual(NumMembers, hash.Count);
                for (var i = 0; i < NumMembers; i++)
                    ClassicAssert.AreEqual($"value:{i}", hash[$"field:{i}"]);

                CollectionAssert.AreEqual(Enumerable.Range(0, NumMembers).Select(i => $"item:{i}"),
                    db.ListRange("list").Select(v => v.ToString()));
                CollectionAssert.AreEquivalent(Enumerable.Range(0, NumMembers).Select(i => $"member:{i}"),
                    db.SetMembers("set").Select(v => v.ToString()));
                CollectionAssert.AreEqual(Enumerable.Range(0, NumMembers).Select(i => $"member:{i}"),
                    db.SortedSetRangeByRank("zset").Select(v => v.ToString()));
            }

            ClassicAssert.AreEqual(0, deviceFactoryCreator.MisalignedRequestCount, "misaligned recovery IO");
        }
    }
}