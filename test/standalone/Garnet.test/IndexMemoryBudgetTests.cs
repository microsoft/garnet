// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

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

        // Small enough that 100k keys overflow it by a wide margin, large enough that chains stay short.
        const string IndexSize = "64k";
        const int NumKeys = 100_000;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                memorySize: "64m",
                indexSize: IndexSize,
                pageSize: "4m");
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public void IndexOverflowIsChargedToLogBudget()
        {
            var storeWrapper = server.Provider.StoreWrapper;
            var store = storeWrapper.store;
            var tracker = store.Log.LogSizeTracker;

            ClassicAssert.IsNotNull(tracker);

            var budget = storeWrapper.serverOptions.IndexMemoryBudgetBytes;
            ClassicAssert.AreEqual(store.IndexSizeBytes, budget,
                "With no index max size configured the index cannot grow, so the budget is the initial index size.");

            // An empty store has only the overflow allocator's initial chunk, which is negligible.
            tracker.RefreshExternalMemorySize();
            var initialCharge = tracker.ExternalMemorySize;
            ClassicAssert.Less(initialCharge, budget / 16);

            var targetSizeBefore = tracker.TargetSize;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < NumKeys; i++)
                    db.StringSet($"key:{i}", "v", flags: CommandFlags.FireAndForget);
                _ = db.StringGet("key:0");
            }

            tracker.RefreshExternalMemorySize();

            var expected = store.IndexTotalSizeBytes - budget;
            ClassicAssert.Greater(expected, 0,
                $"{NumKeys} keys must spill into overflow buckets with an index of {IndexSize}.");
            ClassicAssert.AreEqual(expected, tracker.ExternalMemorySize);
            ClassicAssert.Greater(tracker.ExternalMemorySize, initialCharge * 16);

            // The charge reduces the log's effective budget, which is what makes the node shed log pages
            // instead of growing past the configured memory limit.
            ClassicAssert.AreEqual(targetSizeBefore, tracker.TargetSize,
                "The declared target size is unchanged; only the effective budget used for eviction is reduced.");
            ClassicAssert.Less(tracker.RemainingBudget, targetSizeBefore);
        }

        [Test]
        public void IndexMemoryIsReportedInInfoStore()
        {
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < NumKeys; i++)
                    db.StringSet($"key:{i}", "v", flags: CommandFlags.FireAndForget);
                _ = db.StringGet("key:0");
            }

            var store = server.Provider.StoreWrapper.store;
            server.Provider.StoreWrapper.store.Log.LogSizeTracker.RefreshExternalMemorySize();

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