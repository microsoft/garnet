// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Verifies the ceiling on hash index overflow buckets, expressed as a percentage of the main bucket count.
    /// </summary>
    /// <remarks>
    /// Overflow buckets hold hash entries that do not fit the main bucket array, so they grow with the number of
    /// distinct keys rather than with the size of the data. They are reclaimed only when the index grows, and index
    /// growth stops permanently once the index reaches its max size, so beyond that point the ceiling is the only
    /// thing bounding them. Because they chain linearly off a main bucket and are scanned by reads and upserts, the
    /// percentage is also the average chain length allowed.
    /// </remarks>
    [TestFixture]
    public class IndexOverflowThresholdTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        private void StartServer(string indexSize = "64k", string indexMaxSize = default, int indexOverflowThreshold = default)
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                memorySize: "64m",
                pageSize: "4m",
                indexSize: indexSize,
                indexMaxSize: indexMaxSize,
                indexOverflowThreshold: indexOverflowThreshold);
            server.Start();
        }

        [Test]
        public void CeilingScalesWithTheIndexSizeAtTheDefaultThreshold()
        {
            // The ceiling is a ratio, so it must track the index size rather than being a fixed byte figure. A fixed
            // ceiling is necessarily too loose for a small index (long chains) and too tight for a large one. Both
            // sizes here are above the allocator's minimum page table, so the ratio is not clamped.
            StartServer(indexSize: "16m");
            var smallCeiling = server.Provider.StoreWrapper.store.IndexOverflowMaxSizeBytes;

            TearDown();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            StartServer(indexSize: "64m");
            var largeCeiling = server.Provider.StoreWrapper.store.IndexOverflowMaxSizeBytes;

            ClassicAssert.AreEqual(4, largeCeiling / smallCeiling,
                "A 4x larger index must get a 4x larger overflow ceiling, holding the allowed chain length constant");
        }

        [Test]
        public void CeilingIsFlooredAtTheSmallestPageTableTheAllocatorBuilds()
        {
            // A tiny index scaled by the threshold lands below the two-page minimum the allocator can build. Clamping
            // up rather than failing is what lets a small index still serve a useful number of keys, at the cost of a
            // longer chain than the threshold nominally allows.
            StartServer(indexSize: "64k");

            var store = server.Provider.StoreWrapper.store;
            ClassicAssert.Greater(store.IndexOverflowMaxSizeBytes,
                store.IndexSizeBytes * KVSettings.DefaultIndexOverflowThreshold / 100);
            ClassicAssert.AreEqual(KVSettings.GetIndexOverflowMaxMemorySize(store.IndexSize, KVSettings.DefaultIndexOverflowThreshold),
                store.IndexOverflowMaxSizeBytes);
        }

        [Test]
        public void CeilingIsTheConfiguredPercentageOfTheMainBucketCount()
        {
            // The contract of the setting is exactly this arithmetic: the ceiling in buckets is the configured
            // percentage of the main bucket count. Pinning it keeps the meaning of the number from drifting.
            StartServer(indexSize: "16m", indexOverflowThreshold: 200);

            var store = server.Provider.StoreWrapper.store;
            ClassicAssert.AreEqual(store.IndexSizeBytes * 2, store.IndexOverflowMaxSizeBytes);
            ClassicAssert.AreEqual(200, store.IndexOverflowThreshold);
        }

        [Test]
        public void DefaultThresholdIsUsedWhenUnconfigured()
        {
            StartServer(indexSize: "16m");

            var store = server.Provider.StoreWrapper.store;
            ClassicAssert.AreEqual(KVSettings.DefaultIndexOverflowThreshold, store.IndexOverflowThreshold);
            ClassicAssert.AreEqual(store.IndexSizeBytes * KVSettings.DefaultIndexOverflowThreshold / 100,
                store.IndexOverflowMaxSizeBytes);
        }

        [Test]
        public void ThresholdAtOrBelowTheResizeThresholdIsRejected()
        {
            // Both settings are percentages of the same quantity, so the ordering between them is the whole invariant.
            // At or below the resize threshold the ceiling is reached before the resize that would have reclaimed those
            // overflow buckets, turning a recoverable growth step into a hard failure.
            var defaultResizeThreshold = new GarnetServerOptions().IndexResizeThreshold;

            var below = Assert.Throws<System.Exception>(() =>
                StartServer(indexOverflowThreshold: defaultResizeThreshold - 1));
            ClassicAssert.IsTrue(below.Message.Contains("resize threshold"), below.Message);

            var equal = Assert.Throws<System.Exception>(() =>
                StartServer(indexOverflowThreshold: defaultResizeThreshold));
            ClassicAssert.IsTrue(equal.Message.Contains("resize threshold"), equal.Message);
        }

        [Test]
        public void ThresholdJustAboveTheResizeThresholdIsAccepted()
        {
            // The boundary is strict inequality, not a margin. Pinning it keeps the validation from drifting into
            // rejecting configurations that are in fact safe.
            var defaultResizeThreshold = new GarnetServerOptions().IndexResizeThreshold;
            StartServer(indexOverflowThreshold: defaultResizeThreshold + 1);

            ClassicAssert.AreEqual(defaultResizeThreshold + 1,
                server.Provider.StoreWrapper.store.IndexOverflowThreshold);
        }

        [Test]
        public void DefaultThresholdClearsTheDefaultResizeThreshold()
        {
            // If this ever stopped holding, the default configuration itself would fail to start.
            ClassicAssert.Greater(KVSettings.DefaultIndexOverflowThreshold,
                new GarnetServerOptions().IndexResizeThreshold);
        }

        [Test]
        public void ThresholdIsReportedInInfoStore()
        {
            // The ceiling is only actionable if an operator can see it and the live overflow usage against it.
            StartServer(indexSize: "16m", indexOverflowThreshold: 400);

            var metrics = server.Metrics.GetInfoMetrics(InfoMetricsType.STORE);

            var threshold = metrics.FirstOrDefault(m => m.Name == "IndexOverflowThreshold");
            ClassicAssert.IsNotNull(threshold, "INFO STORE is missing IndexOverflowThreshold");
            ClassicAssert.AreEqual("400", threshold.Value);

            var ceiling = metrics.FirstOrDefault(m => m.Name == "IndexOverflowMaxMemorySizeBytes");
            ClassicAssert.IsNotNull(ceiling, "INFO STORE is missing IndexOverflowMaxMemorySizeBytes");
            ClassicAssert.AreEqual(server.Provider.StoreWrapper.store.IndexOverflowMaxSizeBytes.ToString(), ceiling.Value);
        }
    }
}