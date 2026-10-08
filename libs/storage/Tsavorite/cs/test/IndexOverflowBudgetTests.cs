// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.IndexOverflowBudget
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>;

    /// <summary>
    /// The overflow-bucket ceiling is a percentage of a generation's main bucket count, and an index grow discards the
    /// current generation and installs a fresh one, so each generation's ceiling must scale with its own size.
    /// </summary>
    [TestFixture]
    public class IndexOverflowBudgetTests : TestBase
    {
        private TsavoriteKV<LongStoreFunctions, LongAllocator> store;
        private IDevice log;

        [SetUp]
        public void Setup() => DeleteDirectory(MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir);
        }

        // Large enough that the threshold percentages under test land above the allocator's smallest page table, so a
        // change in the ceiling is observable rather than clamped away.
        private const long IndexSizeBytes = 1L << 22;

        private void CreateStore(int indexOverflowThreshold)
        {
            log = Devices.CreateLogDevice(System.IO.Path.Join(MethodTestDir, "IndexOverflowBudget.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = IndexSizeBytes,
                IndexOverflowThreshold = indexOverflowThreshold,
                LogDevice = log,
                PageSize = 1L << MinKvLogPageSizeBits,
                LogMemorySize = 1L << (MinKvLogPageSizeBits + 4)
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        [Test]
        [Category(TsavoriteKVTestCategory), Category(SmokeTestCategory)]
        public void ConfiguredThresholdIsAppliedToTheStore()
        {
            CreateStore(400);
            ClassicAssert.AreEqual(400, store.IndexOverflowThreshold);
            ClassicAssert.AreEqual(ExpectedCeiling(IndexSizeBytes, 400), store.IndexOverflowMaxSizeBytes);
        }

        [Test]
        [Category(TsavoriteKVTestCategory), Category(SmokeTestCategory)]
        public void UnconfiguredThresholdUsesTheDefault()
        {
            CreateStore(0);
            ClassicAssert.AreEqual(KVSettings.DefaultIndexOverflowThreshold, store.IndexOverflowThreshold);
            ClassicAssert.AreEqual(ExpectedCeiling(IndexSizeBytes, KVSettings.DefaultIndexOverflowThreshold),
                store.IndexOverflowMaxSizeBytes);
        }

        [Test]
        [Category(TsavoriteKVTestCategory), Category(SmokeTestCategory)]
        public void CeilingScalesWithTheIndexAcrossAGrow()
        {
            // An index grow replaces the overflow allocator with a fresh generation. The ceiling must be recomputed for
            // that generation: carrying the old byte ceiling forward would halve the allowed chain length at every grow,
            // and applying no ceiling at all would silently revert to the allocator default for the rest of the process.
            CreateStore(400);

            var sizeBeforeGrow = store.IndexSize;
            var ceilingBeforeGrow = store.IndexOverflowMaxSizeBytes;

            store.GrowIndexAsync().GetAwaiter().GetResult();
            ClassicAssert.AreEqual(sizeBeforeGrow * 2, store.IndexSize, "The index must actually have grown for this test to mean anything");

            ClassicAssert.AreEqual(400, store.IndexOverflowThreshold);
            ClassicAssert.AreEqual(ceilingBeforeGrow * 2, store.IndexOverflowMaxSizeBytes,
                "The new generation is twice the size, so its overflow ceiling must be twice as large");
        }

        /// <summary>
        /// The ceiling the allocator can actually express: the requested percentage of the main bucket count, rounded
        /// down to a whole allocator page and floored at the smallest page table the allocator builds.
        /// </summary>
        private static long ExpectedCeiling(long indexSizeBytes, int threshold)
            => KVSettings.GetIndexOverflowMaxMemorySize(indexSizeBytes / 64, threshold);

    }
}