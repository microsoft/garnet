// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Verifies that hash index memory beyond the configured index budget is charged against the log memory budget.
    /// </summary>
    /// <remarks>
    /// The hash index costs ~64/7 bytes per distinct key and holds an entry for every key, including keys whose
    /// records have been tiered to disk. Without this accounting the index grows outside every configured memory
    /// limit, so a node storing many small records exhausts RAM while the log stays exactly at its target size.
    /// </remarks>
    [TestFixture]
    public class IndexMemoryBudgetTests : TestBase
    {
        GarnetServer server;

        // 65536 main buckets. An overflow generation commits 8 MiB even when nearly empty, so the index must be
        // at least this large for its overflow ceiling (IndexOverflowThreshold percent of it) to exceed that floor.
        const string IndexSize = "4m";

        // Enough to spill past the 8 MiB floor and commit a third overflow page, while staying under the ceiling.
        const int NumKeys = 1_300_000;

        [SetUp]
        public void Setup()
        {
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        void StartServer()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                memorySize: "128m",
                indexSize: IndexSize,
                pageSize: "4m");
            server.Start();
        }

        static void Populate(int numKeys)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            for (var i = 0; i < numKeys; i++)
                db.StringSet($"key:{i}", "v", flags: CommandFlags.FireAndForget);
            _ = db.StringGet("key:0");
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        [TestCase(65_536, 0, false, 0, "")]
        [TestCase(70_000, 0, false, 0, "")]
        [TestCase(0, 0, false, 0, "")]
        [TestCase(65_536, 98_304, true, 0, "")]
        [TestCase(0, 98_304, true, 0, "")]
        [TestCase(65_536, 0, true, 0, "")]
        [TestCase(0, 0, true, 0, "")]
        [TestCase(70_000, 100_000, true, 1, "8m")]
        [TestCase(70_000, 100_000, true, 4, "8m")]
        public void ConfiguredMemoryBudgetCountsEachLogOnce(long mainMemory, long readCacheMemory, bool enableReadCache,
            int aofSublogCount, string indexMaxMemorySize)
        {
            const int PageSize = 4096;
            const int PageCount = 4;
            const long AofMemoryPerSublog = 65_536;
            var logger = new CapturingLogger();
            using var loggerFactory = LoggerFactory.Create(builder => builder.AddProvider(new CapturingLoggerProvider(logger)));
            var options = new GarnetServerOptions
            {
                EndPoints = [TestUtils.EndPoint],
                LogDir = TestUtils.MethodTestDir,
                CheckpointDir = TestUtils.MethodTestDir,
                EnableStorageTier = true,
                LogMemorySize = mainMemory.ToString(),
                PageSize = PageSize.ToString(),
                PageCount = PageCount,
                EnableReadCache = enableReadCache,
                ReadCacheMemorySize = readCacheMemory.ToString(),
                ReadCachePageSize = PageSize.ToString(),
                ReadCachePageCount = PageCount,
                IndexMemorySize = IndexSize,
                IndexMaxMemorySize = indexMaxMemorySize,
                EnableAOF = aofSublogCount > 0,
                AofPhysicalSublogCount = aofSublogCount > 0 ? aofSublogCount : 1,
                AofMemorySize = AofMemoryPerSublog.ToString(),
                AofPageSize = (2 * PageSize).ToString(),
                UseAofNullDevice = true,
                DisablePubSub = true,
                QuietMode = true
            };
            server = new GarnetServer(options, loggerFactory);

            var wrapper = server.Provider.StoreWrapper;
            var store = wrapper.store;
            var cacheTracker = wrapper.sizeTracker;
            Assert.That(cacheTracker?.TargetSize ?? 0, Is.EqualTo(mainMemory));
            Assert.That(cacheTracker?.ReadCacheTargetSize ?? 0, Is.EqualTo(enableReadCache ? readCacheMemory : 0));

            foreach (var (tracker, configuredMemory) in new[]
            {
                (store.Log.LogSizeTracker, mainMemory),
                (store.ReadCache?.LogSizeTracker, enableReadCache ? readCacheMemory : 0)
            })
            {
                if (configuredMemory == 0)
                {
                    Assert.That(tracker, Is.Null);
                    continue;
                }

                Assert.That(tracker.TargetSize, Is.EqualTo(configuredMemory));
                Assert.That(tracker.TargetDeltaRange, Is.EqualTo((configuredMemory + configuredMemory / 10, configuredMemory - configuredMemory / 50)));
                Assert.That(tracker.TotalSize, Is.EqualTo(tracker.logAccessor.MemorySizeBytes + tracker.LogHeapSizeBytes));
            }

            // The index budget is the larger of the index size and its max size, plus the floor the store's overflow allocator commits.
            var mainBucketBytes = indexMaxMemorySize.Length == 0 ? 4L << 20 : 8L << 20;
            var expectedIndexBudget = mainBucketBytes + store.IndexOverflowFloorSizeBytes;
            Assert.That(options.IndexMemoryBudgetBytes, Is.EqualTo(expectedIndexBudget));
            var expectedMainBudget = mainMemory > 0 ? mainMemory : PageCount * PageSize;
            var expectedReadCacheBudget = enableReadCache ? readCacheMemory > 0 ? readCacheMemory : PageCount * PageSize : 0;
            var expectedTotal = expectedIndexBudget + expectedMainBudget + expectedReadCacheBudget + aofSublogCount * AofMemoryPerSublog;
            var budgetEntry = logger.Entries.Single(entry => entry.State.ContainsKey("configMemoryLimit"));
            Assert.That(budgetEntry.State["configMemoryLimit"], Is.EqualTo(expectedTotal));
            var indexEntry = logger.Entries.Single(entry => entry.State.ContainsKey("keyCapacity"));
            Assert.That(indexEntry.State["indexMemoryLimit"], Is.EqualTo(expectedIndexBudget));
            Assert.That(indexEntry.State["keyCapacity"], Is.EqualTo(mainBucketBytes / 64 * 7));
        }

        [Test]
        public void IndexOverflowIsChargedToLogBudget()
        {
            StartServer();

            var storeWrapper = server.Provider.StoreWrapper;
            var store = storeWrapper.store;
            var tracker = store.Log.LogSizeTracker;

            ClassicAssert.IsNotNull(tracker);

            var budget = storeWrapper.serverOptions.IndexMemoryBudgetBytes;
            ClassicAssert.AreEqual(store.IndexSizeBytes + store.IndexOverflowFloorSizeBytes, budget,
                "With no index max size configured the index cannot grow, so the budget is the initial index size plus"
                + " the memory an overflow generation commits just by existing.");

            // An empty store has committed only that floor, so nothing is charged to the log yet.
            tracker.RefreshExternalMemorySize();
            ClassicAssert.AreEqual(0, tracker.ExternalMemorySize,
                "An index at its configured size with no overflow growth must not reduce the log budget.");

            var targetSizeBefore = tracker.TargetSize;

            Populate(NumKeys);

            tracker.RefreshExternalMemorySize();

            var expected = store.IndexTotalSizeBytes - budget;
            ClassicAssert.Greater(expected, 0,
                $"{NumKeys} keys must commit overflow pages beyond the floor with an index of {IndexSize}.");
            ClassicAssert.AreEqual(expected, tracker.ExternalMemorySize);

            // The charge reduces the log's effective budget, which is what makes the node shed log pages
            // instead of growing past the configured memory limit.
            ClassicAssert.AreEqual(targetSizeBefore, tracker.TargetSize,
                "The declared target size is unchanged; only the effective budget used for eviction is reduced.");
            ClassicAssert.Less(tracker.RemainingBudget, targetSizeBefore);
        }

        [Test]
        public void IndexMemoryIsReportedInInfoStore()
        {
            StartServer();
            Populate(NumKeys);

            var store = server.Provider.StoreWrapper.store;
            store.Log.LogSizeTracker.RefreshExternalMemorySize();

            var metrics = server.Metrics.GetInfoMetrics(InfoMetricsType.STORE);

            long GetMetric(string name)
            {
                var mi = metrics.FirstOrDefault(m => m.Name == name);
                ClassicAssert.IsNotNull(mi, $"INFO STORE is missing {name}");
                ClassicAssert.IsTrue(long.TryParse(mi.Value, out var value));
                return value;
            }

            var budget = GetMetric("IndexMemoryBudgetBytes");
            var total = GetMetric("IndexTotalMemorySizeBytes");
            var charged = GetMetric("IndexMemoryChargedToLogBudgetBytes");

            ClassicAssert.AreEqual(store.IndexTotalSizeBytes, total);
            ClassicAssert.Greater(total, budget);
            ClassicAssert.AreEqual(total - budget, charged);
        }

        /// <summary>
        /// A store whose checkpoint devices need 4 KiB sectors aligns its overflow-bucket pages to them, so its overflow
        /// floor exceeds the one at the minimum sector size. The budget includes the store's own floor, so an empty store
        /// still charges nothing to the log.
        /// </summary>
        [Test]
        public void LargerSectorOverflowFloorIsWithinTheBudget()
        {
            var options = TestUtils.GetGarnetServerOptions(TestUtils.MethodTestDir, TestUtils.MethodTestDir, TestUtils.EndPoint, enableCluster: false);
            options.IndexMemorySize = IndexSize;
            options.LogMemorySize = "128m";
            options.PageSize = "4m";
            options.DeviceFactoryCreator = new StrictSectorCheckpointDeviceFactoryCreator(options.StoreCheckpointBaseDirectory);
            server = new GarnetServer(options);
            server.Start();

            var storeWrapper = server.Provider.StoreWrapper;
            var store = storeWrapper.store;
            ClassicAssert.Greater(store.IndexOverflowFloorSizeBytes, KVSettings.OverflowBucketFloorMemorySize,
                "Overflow-bucket pages aligned to 4 KiB carry more slack than at the minimum sector size.");
            ClassicAssert.AreEqual(store.IndexSizeBytes + store.IndexOverflowFloorSizeBytes, storeWrapper.serverOptions.IndexMemoryBudgetBytes);

            var tracker = store.Log.LogSizeTracker;
            tracker.RefreshExternalMemorySize();
            ClassicAssert.AreEqual(0, tracker.ExternalMemorySize,
                "An empty store's overflow buckets commit exactly its floor, which the budget includes.");
        }

        /// <summary>Runs an action as an index resize enters <see cref="Phase.IN_PROGRESS_GROW"/>: after the resize installs the
        /// new generation and before its split frees the generation it replaces.</summary>
        sealed class IndexGrowCallback(Action onGrow) : IStateMachineCallback
        {
            public void BeforeEnteringState(SystemState next)
            {
                if (next.Phase == Phase.IN_PROGRESS_GROW)
                    onGrow();
            }
        }

        static long MinimumLogBudget<TStoreFunctions, TAllocator>(LogSizeTracker<TStoreFunctions, TAllocator> tracker, long pageSize)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => LogSizeTracker<TStoreFunctions, TAllocator>.MinTargetPageCount * pageSize;

        /// <summary>
        /// The generation an index resize replaces stays allocated until the split completes, so it is not charged against the
        /// log budget. Charging it would evict log pages for memory that is about to be freed, and without a storage tier the
        /// evicted records are lost.
        /// </summary>
        [Test]
        public void IndexResizeDoesNotChargeReplacedGenerationToLogBudget()
        {
            // No storage tier, so evicting a page removes its records. An overflow generation's floor exceeds this log budget.
            server = TestUtils.CreateGarnetServer(null, memorySize: "3m", indexSize: "512", pageSize: $"{TestUtils.MinKvLogPageSize}");
            server.Start();

            var storeWrapper = server.Provider.StoreWrapper;
            var store = storeWrapper.store;
            var tracker = store.Log.LogSizeTracker;
            var budget = storeWrapper.serverOptions.IndexMemoryBudgetBytes;

            const int numKeys = 2000;
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            for (var i = 0; i < numKeys; i++)
                db.StringSet($"key:{i}", "v", flags: CommandFlags.FireAndForget);
            ClassicAssert.AreEqual("v", (string)db.StringGet($"key:{numKeys - 1}"));

            var beginAddress = store.Log.BeginAddress;
            ClassicAssert.Greater(tracker.TotalSize, MinimumLogBudget(tracker, store.Log.PageSizeBytes),
                "The log must exceed its minimum budget, so that reducing the budget to that minimum evicts pages.");

            long indexTotalDuringSplit = 0, chargedDuringSplit = -1;
            var overBudgetDuringSplit = true;
            storeWrapper.DefaultDatabase.StateMachineDriver.UnsafeRegisterCallback(new IndexGrowCallback(() =>
            {
                indexTotalDuringSplit = store.IndexTotalSizeBytes;
                _ = tracker.RefreshExternalMemorySize();
                chargedDuringSplit = tracker.ExternalMemorySize;
                overBudgetDuringSplit = tracker.IsOverBudget;
            }));

            ClassicAssert.AreEqual("OK", (string)db.Execute("CONFIG", "SET", "index", "1024"));

            ClassicAssert.Greater(indexTotalDuringSplit - store.IndexTotalSizeBytes, tracker.TargetSize,
                "The replaced generation must exceed the log target, so that charging it would reduce the log to its minimum budget.");
            ClassicAssert.AreEqual(Math.Max(0, store.IndexTotalSizeBytes - budget), chargedDuringSplit,
                "During the split the log must be charged only for the generation the resize installs.");
            ClassicAssert.IsFalse(overBudgetDuringSplit, "The log must not be over budget during the split.");

            ClassicAssert.AreEqual(beginAddress, store.Log.BeginAddress, "No log page may be evicted.");
            for (var i = 0; i < numKeys; i++)
                ClassicAssert.IsTrue(db.KeyExists($"key:{i}"), $"key:{i} was lost");
        }
    }
}