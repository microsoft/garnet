// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.objects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// Exercises the deferred object-load path of snapshot recovery (see <c>RecoverHybridLogFromSnapshotFileAsync</c>):
    /// the hybrid-log phase reads its pages without loading their objects, then after the snapshot pages have also been
    /// read (without their objects), objects are loaded once over the full recovered range honoring the final headAddress.
    /// The recovered range spans both the hybrid-log region (objects in the main object-log) and the snapshot region
    /// (objects in the snapshot object-log), with the device boundary at the page that contains FlushedUntilAddress.
    /// A <see cref="LogSizeTracker{TStoreFunctions, TAllocator}"/> is optionally attached to the recovery store to force
    /// eviction during the deferred load, covering: no eviction (both region loads run over resident pages), partial
    /// eviction (headAddress stays in the hybrid-log region so both loads run), and heavy eviction (headAddress is pushed
    /// into the snapshot region so only the snapshot-region load runs). A non-power-of-2 buffer is also covered.
    /// </summary>
    [TestFixture]
    public class ObjectRecoverySnapshotEvictionTests : TestBase
    {
        const int NumRecords = 6000;

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        // recoveryTargetPageCount: 0 => no size tracker (no eviction); otherwise attach a tracker whose target is that many
        // pages, forcing eviction during recovery. 4 is the minimum (LogSizeTracker.MinTargetPageCount). Small values force
        // the snapshot-only load; larger values leave the head in the hybrid-log region so both region loads run.
        // logMemoryPages: the max allocated page count; 24 is not a power of two, so BufferSize (next power of two = 32)
        // has empty slots that the load loop must skip.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task SnapshotRecoveryDeferredObjectLoad(
            [Values(0, 4, 8, 20, 64)] int recoveryTargetPageCount,
            [Values(32, 24)] int logMemoryPages)
        {
            var logMemorySize = (long)logMemoryPages * MinKvLogPageSize;

            // Write records (spanning many pages so some are flushed to the main log before the checkpoint, creating a
            // hybrid-log region) and take a Snapshot checkpoint capturing the still-mutable region as the snapshot region.
            Prepare(logMemorySize, out var log, out var objlog, out var store);
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var i = 0; i < NumRecords; i++)
                        _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });
                }

                _ = store.TryInitiateHybridLogCheckpoint(out var token, CheckpointType.Snapshot);
                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
                Destroy(log, objlog, store);

                // Recover into a fresh store, optionally under memory pressure so the deferred object load must evict.
                Prepare(logMemorySize, out log, out objlog, out store);
                if (recoveryTargetPageCount > 0)
                {
                    var targetSize = (long)recoveryTargetPageCount * MinKvLogPageSize;
                    var tracker = new LogSizeTracker<ClassStoreFunctions, ClassAllocator>(store.Log, targetSize, targetSize / 8, targetSize / 16, logger: null);
                    store.Log.SetLogSizeTracker(tracker);
                }

                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                // Every record must recover correctly, whether it ended up resident or was evicted (and is read from disk).
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var i = 0; i < NumRecords; i++)
                    {
                        var key = new TestObjectKey { key = i };
                        TestObjectInput input = default;
                        TestObjectOutput output = new();
                        var status = bContext.Read(key, ref input, ref output);
                        if (status.IsPending)
                        {
                            Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                            (status, output) = GetSinglePendingResult(completedOutputs);
                        }

                        ClassicAssert.IsTrue(status.Found, $"key {i} not found (target pages {recoveryTargetPageCount}, mem pages {logMemoryPages})");
                        ClassicAssert.AreEqual(i, output.value.value, $"key {i} wrong value");
                    }
                }

                // With a small memory budget, eviction must have advanced the head above the begin address.
                if (recoveryTargetPageCount is > 0 and <= 8)
                    ClassicAssert.Greater(store.Log.HeadAddress, store.Log.BeginAddress, "expected eviction to advance HeadAddress");
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        // After recovering an object store into a smaller memory budget (so snapshot object pages are evicted and their objects are read back from the
        // main object-log that the recovery flush copied them into), compact the read-only (evicted) region and truncate it, then verify every record is
        // still readable. Compaction reads each live evicted record's objects from the main object-log (validating the copied positions), and Truncate drops
        // the now-stale main-log and object-log segments using each page header's lowest-object-log position (which the recovery flush set to the main object-log).
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task SnapshotRecoveryThenCompactTruncate(
            [Values] CompactionType compactionType,
            [Values(32, 24)] int logMemoryPages)
        {
            var logMemorySize = (long)logMemoryPages * MinKvLogPageSize;

            Prepare(logMemorySize, out var log, out var objlog, out var store);
            try
            {
                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var i = 0; i < NumRecords; i++)
                        _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });
                }

                _ = store.TryInitiateHybridLogCheckpoint(out var token, CheckpointType.Snapshot);
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
                Destroy(log, objlog, store);

                // Recover under memory pressure so snapshot object pages are evicted during recovery (their objects must be read back from the main object-log).
                Prepare(logMemorySize, out log, out objlog, out store);
                var targetSize = 8L * MinKvLogPageSize;
                var tracker = new LogSizeTracker<ClassStoreFunctions, ClassAllocator>(store.Log, targetSize, targetSize / 8, targetSize / 16, logger: null);
                store.Log.SetLogSizeTracker(tracker);

                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                // Recovery has forced eviction of snapshot object pages (their objects were copied into the main object-log). Relax the budget before
                // compaction so the tight recovery target does not starve Compact's allocation (Compact copies live records to the tail); the log still
                // spills to disk via its normal LogMemorySize-driven eviction, so compaction continues to read evicted records' objects from the main object-log.
                tracker.UpdateTargetSize(1L << 30, 1L << 27, 1L << 26);

                using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
                {
                    var bContext = session.BasicContext;

                    // Compact the read-only (evicted) region up to SafeReadOnlyAddress — the resident recovered pages above it are the still-mutable snapshot
                    // region and cannot be compacted — reading each live evicted record's objects from the main object-log, then truncate the stale segments.
                    var compactUntil = session.Compact(store.Log.SafeReadOnlyAddress, compactionType);
                    store.Log.Truncate();
                    ClassicAssert.AreEqual(compactUntil, store.Log.BeginAddress, "BeginAddress should advance to compactUntil after Truncate");

                    // Every record must still be readable after Compact + Truncate.
                    for (var i = 0; i < NumRecords; i++)
                    {
                        var key = new TestObjectKey { key = i };
                        TestObjectInput input = default;
                        TestObjectOutput output = new();
                        var status = bContext.Read(key, ref input, ref output);
                        if (status.IsPending)
                        {
                            Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                            (status, output) = GetSinglePendingResult(completedOutputs);
                        }

                        ClassicAssert.IsTrue(status.Found, $"key {i} not found after compact/truncate (compactionType {compactionType}, mem pages {logMemoryPages})");
                        ClassicAssert.AreEqual(i, output.value.value, $"key {i} wrong value after compact/truncate");
                    }
                }
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        private static void Prepare(long logMemorySize, out IDevice log, out IDevice objlog, out TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "snapevict.log"));
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "snapevict.obj.log"));
            store = new(new()
            {
                IndexSize = 1L << 22,
                LogDevice = log,
                ObjectLogDevice = objlog,
                SegmentSize = 1L << 20,
                LogMemorySize = logMemorySize,
                PageSize = MinKvLogPageSize,
                CheckpointDir = Path.Combine(MethodTestDir, "check-points")
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        private static void Destroy(IDevice log, IDevice objlog, TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            store.Dispose();
            log.Dispose();
            objlog.Dispose();
        }
    }
}