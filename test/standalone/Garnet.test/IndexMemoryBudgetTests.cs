// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using Garnet.common;
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

        [Test]
        public void IndexOverflowIsChargedToLogBudget()
        {
            StartServer();

            var storeWrapper = server.Provider.StoreWrapper;
            var store = storeWrapper.store;
            var tracker = store.Log.LogSizeTracker;

            ClassicAssert.IsNotNull(tracker);

            var budget = storeWrapper.serverOptions.IndexMemoryBudgetBytes;
            ClassicAssert.AreEqual(store.IndexSizeBytes + KVSettings.OverflowBucketFloorMemorySize, budget,
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
    }
}