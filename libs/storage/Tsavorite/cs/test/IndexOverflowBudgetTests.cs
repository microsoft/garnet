// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

        private void CreateStore(int indexOverflowThreshold) => store = CreateStore(indexOverflowThreshold, checkpointDir: null);

        private TsavoriteKV<LongStoreFunctions, LongAllocator> CreateStore(int indexOverflowThreshold, string checkpointDir)
        {
            log ??= Devices.CreateLogDevice(System.IO.Path.Join(MethodTestDir, "IndexOverflowBudget.log"), deleteOnClose: false);
            return new(new()
            {
                IndexSize = IndexSizeBytes,
                IndexOverflowThreshold = indexOverflowThreshold,
                LogDevice = log,
                PageSize = 1L << MinKvLogPageSizeBits,
                LogMemorySize = 1L << (MinKvLogPageSizeBits + 4),
                CheckpointDir = checkpointDir
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
            // A grow replaces the overflow allocator with a fresh generation, whose ceiling must be recomputed for its
            // own size rather than carried forward or left at the allocator default.
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
        /// Recovery installs the checkpoint's index, which may be larger than the configured one, so the overflow
        /// ceiling must be recomputed for the table actually installed. An allocator left at the configured size would
        /// cap a recovered index below the chain length it ran with before the checkpoint.
        /// </summary>
        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void RecoveryRebuildsTheCeilingForTheRecoveredIndex()
        {
            const int Threshold = 400;
            var mainBuckets = IndexSizeBytes / 64;

            Guid token;
            using (var grown = CreateStore(Threshold, checkpointDir: MethodTestDir))
            {
                // The index is only checkpointed and recovered when it holds something.
                using (var session = grown.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var keyNum = 0L; keyNum < 128; keyNum++)
                    {
                        var valueNum = keyNum;
                        _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref keyNum)), SpanByte.FromPinnedVariable(ref valueNum));
                    }
                }

                grown.GrowIndexAsync().GetAwaiter().GetResult();
                ClassicAssert.AreEqual(mainBuckets * 2, grown.IndexSize, "The index must actually have grown for this test to mean anything");

                var (succeeded, checkpointToken) = grown.TakeFullCheckpointAsync(CheckpointType.Snapshot).GetAwaiter().GetResult();
                ClassicAssert.IsTrue(succeeded, "Checkpoint must succeed for this test to mean anything");
                token = checkpointToken;
            }

            // Configured for the original, smaller index: recovery must install the larger checkpointed one.
            store = CreateStore(Threshold, checkpointDir: MethodTestDir);
            _ = store.RecoverAsync(token).GetAwaiter().GetResult();

            ClassicAssert.AreEqual(mainBuckets * 2, store.IndexSize, "Recovery must have installed the checkpointed index");
            ClassicAssert.AreEqual(KVSettings.GetIndexOverflowMaxMemorySize(mainBuckets * 2, Threshold), store.IndexOverflowMaxSizeBytes,
                "The ceiling must be recomputed for the recovered index, not left at the configured size");
        }

        /// <summary>
        /// A grow must guarantee the new generation can hold every bucket the split can need, independently of the
        /// threshold: a split inserts an entry into both halves of the new table when its record is not in memory, so it
        /// can need twice the old generation's entries. Running out mid-split leaves SplitAllBuckets spinning on a chunk
        /// that can never complete.
        /// </summary>
        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void AGrowReservesEnoughOverflowBucketsForTheSplit()
        {
            // At this threshold the doubled generation's threshold-derived ceiling is smaller than the split needs,
            // so the reservation is what supplies the difference rather than the ratio happening to cover it.
            const int Threshold = 150;
            CreateStore(Threshold);

            var mainBuckets = IndexSizeBytes / 64;

            // Fill the current generation past the point where the doubled ratio alone would suffice. The split needs
            // 2 * (main + overflow) buckets, so overflow must exceed main * (Threshold/100 - 1) for that to bind.
            var overflowToAllocate = mainBuckets * 3 / 2 - mainBuckets / 2 + 1;
            for (var i = 0L; i < overflowToAllocate; ++i)
                _ = store.overflowBucketsAllocator.Allocate();

            var requirement = store.GetSplitOverflowBucketRequirement(mainBuckets);
            var ratioCeilingAfterGrow = KVSettings.GetIndexOverflowMaxMemorySize(mainBuckets * 2, Threshold);
            ClassicAssert.Greater(requirement * 64, ratioCeilingAfterGrow,
                "This test is only meaningful if the split needs more than the doubled generation's ratio would allow");

            store.GrowIndexAsync().GetAwaiter().GetResult();
            ClassicAssert.AreEqual(mainBuckets * 2, store.IndexSize, "The index must actually have grown for this test to mean anything");

            ClassicAssert.GreaterOrEqual(store.IndexOverflowMaxSizeBytes, requirement * 64,
                "The new generation must be able to hold every bucket the split can need");
        }

        /// <summary>
        /// A threshold whose share of the main bucket count rounds to nothing must still resolve to the allocator's
        /// smallest page table. The allocator reads a zero budget as "unconfigured" and substitutes its own much larger
        /// default, which would make the tightest thresholds produce the loosest ceilings.
        /// </summary>
        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void AThresholdTooSmallToExpressFloorsAtTheSmallestPageTable()
        {
            var floor = KVSettings.GetIndexOverflowMaxMemorySize(64, 300);

            // 64 buckets at 1% is a fraction of one bucket, so taking the percentage before scaling to bytes truncates
            // it away; a single bucket at 1% is below one byte and rounds to nothing however the arithmetic is ordered.
            foreach (var (buckets, threshold) in new[] { (64L, 1), (1L, 1), (0L, 1) })
            {
                var tiny = KVSettings.GetIndexOverflowMaxMemorySize(buckets, threshold);
                ClassicAssert.AreEqual(floor, tiny,
                    $"A threshold that rounds to nothing ({buckets} buckets at {threshold}%) must floor at the smallest page table, not fall back to the allocator default");
            }

            ClassicAssert.Less(floor, KVSettings.GetIndexOverflowMaxMemorySize(1L << 24, 300),
                "The floor must still be a floor: a real index must produce a larger ceiling than a degenerate one");
        }

        /// <summary>
        /// The ceiling grows with the threshold rather than collapsing at small values, which requires scaling the
        /// bucket count before taking the percentage.
        /// </summary>
        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void TheCeilingIsMonotonicInTheThreshold()
        {
            var buckets = 1L << 18;
            var previous = 0L;
            foreach (var threshold in new[] { 50, 100, 200, 400, 800 })
            {
                var ceiling = KVSettings.GetIndexOverflowMaxMemorySize(buckets, threshold);
                ClassicAssert.GreaterOrEqual(ceiling, previous, $"Ceiling must not shrink as the threshold grows (threshold {threshold})");
                previous = ceiling;
            }
        }

        /// <summary>
        /// The ceiling the allocator can actually express: the requested percentage of the main bucket count, rounded
        /// down to a whole allocator page and floored at the smallest page table the allocator builds.
        /// </summary>
        private static long ExpectedCeiling(long indexSizeBytes, int threshold)
            => KVSettings.GetIndexOverflowMaxMemorySize(indexSizeBytes / 64, threshold);

    }
}