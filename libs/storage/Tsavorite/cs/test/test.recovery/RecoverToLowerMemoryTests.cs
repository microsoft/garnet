// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using LargeObjAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using LargeObjStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;
    using ObjAllocator = ObjectAllocator<StoreFunctions<LongKeyComparer, DefaultRecordTriggers>>;
    using ObjStoreFunctions = StoreFunctions<LongKeyComparer, DefaultRecordTriggers>;

    /// <summary>
    /// Recovers an <see cref="ObjectAllocator{TStoreFunctions}"/> store's Snapshot checkpoint into a smaller memory budget
    /// than was checkpointed, with a <see cref="LogSizeTracker{TStoreFunctions, TAllocator}"/> attached so recovery must
    /// evict snapshot pages to honor the budget. Garnet always uses the ObjectAllocator (even for string values), so this
    /// exercises the same allocator as the Garnet server. Every record — whether it stayed resident or was evicted to disk —
    /// must read back correctly. Regression for issue #1950 (a snapshot page lost during eviction-while-recovering).
    /// </summary>
    [TestFixture]
    public class RecoverToLowerMemoryTests : TestBase
    {
        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        const int NumRecords = 2000;                 // spans enough pages that recovery reads in multiple batches and must evict

        // store1 budget; larger than the recovery budget => a still-mutable snapshot region at checkpoint whose page count
        // exceeds the recovery buffer, so recovery must evict snapshot pages (which is what surfaces #1950).
        const long InitialMemorySize = 63 * 1024;

        // The evict-while-recovering interleaving (and the optional concurrent resizer) is timing-sensitive, so repeat a few
        // times to reliably catch a regression.
        const int RepeatCount = 3;

        sealed class MyFunctions : SimpleLongSimpleFunctions { }

        static TsavoriteKV<ObjStoreFunctions, ObjAllocator> CreateStore(IDevice log, IDevice objlog, string checkpointDir, long memorySize)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 1,
                PageSize = MinKvLogPageSize,
                LogMemorySize = memorySize,
                SegmentSize = 1L << 20,
                CheckpointDir = checkpointDir,
            }, StoreFunctions.Create(LongKeyComparer.Instance, () => new TestObjectValue.Serializer(), DefaultRecordTriggers.Instance),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        // recoveryMemorySize 18k => power-of-two BufferSize; 23k => non-power-of-two (MaxAllocatedPageCount 5, BufferSize 8)
        // so the read batch exceeds the resident set. startResizerDuringReads also runs the background resizer concurrently
        // with the post-recovery reads.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoverSnapshotToLowerMemoryInlineOnly(
            [Values(18 * 1024, 23 * 1024)] long recoveryMemorySize,
            [Values(false, true)] bool startResizerDuringReads)
        {
            var keyArray = new byte[sizeof(long)];

            for (var iter = 0; iter < RepeatCount; iter++)
            {
                var dir = Path.Combine(MethodTestDir, $"it{iter}");
                _ = Directory.CreateDirectory(dir);
                var checkpointDir = Path.Combine(dir, "checkpoints");
                IDevice log = Devices.CreateLogDevice(Path.Combine(dir, "hlog.log"), deleteOnClose: false);
                IDevice objlog = Devices.CreateLogDevice(Path.Combine(dir, "hlog.obj.log"), deleteOnClose: false);
                try
                {
                    // Write all records (inline values) and take a Snapshot checkpoint. store1's own eviction (under
                    // InitialMemorySize) flushes most pages to the main log, leaving a still-mutable snapshot region.
                    Guid token;
                    using (var store1 = CreateStore(log, objlog, checkpointDir, InitialMemorySize))
                    {
                        using (var session = store1.NewSession<TestSpanByteKey, long, long, Empty, MyFunctions>(new MyFunctions()))
                        {
                            var bContext = session.BasicContext;
                            for (long key = 0; key < NumRecords; key++)
                            {
                                var keySpan = new Span<byte>(keyArray);
                                keySpan.AsRef<long>() = key;
                                _ = bContext.Upsert(TestSpanByteKey.FromArray(keyArray), keySpan);
                            }
                        }
                        var (success, checkpointToken) = await store1.TakeFullCheckpointAsync(CheckpointType.Snapshot).ConfigureAwait(false);
                        ClassicAssert.IsTrue(success);
                        token = checkpointToken;
                    }

                    // Recover into a smaller budget with a size tracker attached, so recovery evicts snapshot pages to fit.
                    var store2 = CreateStore(log, objlog, checkpointDir, recoveryMemorySize);
                    var tracker = new LogSizeTracker<ObjStoreFunctions, ObjAllocator>(store2.Log, recoveryMemorySize, recoveryMemorySize / 8, recoveryMemorySize / 16, logger: null);
                    store2.Log.SetLogSizeTracker(tracker);
                    _ = await store2.RecoverAsync(default, token).ConfigureAwait(false);

                    try
                    {
                        if (startResizerDuringReads)
                            tracker.Start(CancellationToken.None);

                        using (var session = store2.NewSession<TestSpanByteKey, long, long, Empty, MyFunctions>(new MyFunctions()))
                        {
                            var bContext = session.BasicContext;
                            for (long key = 0; key < NumRecords; key++)
                            {
                                var keySpan = new Span<byte>(keyArray);
                                keySpan.AsRef<long>() = key;
                                long output = -1;
                                var status = bContext.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                                if (status.IsPending)
                                {
                                    Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                                    (status, output) = GetSinglePendingResult(completedOutputs);
                                }
                                ClassicAssert.IsTrue(status.Found, $"iter {iter}: key {key} not found (recoveryMemorySize {recoveryMemorySize}, startResizer {startResizerDuringReads})");
                                ClassicAssert.AreEqual(key, output, $"iter {iter}: key {key} wrong value {output}");
                            }
                        }
                    }
                    finally
                    {
                        if (startResizerDuringReads)
                            tracker.Stop(wait: true);
                        store2.Dispose();
                    }
                }
                finally
                {
                    log.Dispose();
                    objlog.Dispose();
                    try { Directory.Delete(dir, recursive: true); } catch { }
                }
            }
        }

        static TsavoriteKV<LargeObjStoreFunctions, LargeObjAllocator> CreateObjectStore(IDevice log, IDevice objlog, string checkpointDir, long memorySize)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,               // exercise the logMutableFraction read-only region (and its recovery flush)
                PageSize = MinKvLogPageSize,
                LogMemorySize = memorySize,
                SegmentSize = 1L << 20,
                CheckpointDir = checkpointDir,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordTriggers.Instance),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        /// <summary>
        /// Companion to <see cref="RecoverSnapshotToLowerMemoryInlineOnly"/> that exercises the object path: records hold several-KB heap objects, so
        /// recovery eviction is driven by the object heap (not just page count) — the deferred object-load pass must flush-and-evict snapshot object pages
        /// (copying their objects into the main object-log) and deserialize the resident ones. Every object, resident or evicted-to-disk, must read back.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoverSnapshotToLowerMemoryWithObjects(
            [Values(64 * 1024, 128 * 1024)] long recoveryMemorySize)
        {
            const int objSize = 2000;                // several-KB heap objects
            const int numObjRecords = 1000;

            var dir = MethodTestDir;
            var checkpointDir = Path.Combine(dir, "checkpoints");
            IDevice log = Devices.CreateLogDevice(Path.Combine(dir, "hlog.log"), deleteOnClose: false);
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(dir, "hlog.obj.log"), deleteOnClose: false);
            try
            {
                Guid token;
                using (var store1 = CreateObjectStore(log, objlog, checkpointDir, 128 * 1024))
                {
                    using (var session = store1.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                    {
                        var bContext = session.BasicContext;
                        for (var key = 0; key < numObjRecords; key++)
                        {
                            var value = new TestLargeObjectValue(objSize);
                            value.value[0] = (byte)key;             // make each object's payload identifiable by key
                            value.value[1] = (byte)(key >> 8);
                            _ = bContext.Upsert(new TestObjectKey { key = key }, value);
                        }
                    }
                    var (success, checkpointToken) = await store1.TakeFullCheckpointAsync(CheckpointType.Snapshot).ConfigureAwait(false);
                    ClassicAssert.IsTrue(success);
                    token = checkpointToken;
                }

                // Recover into a smaller budget so the large object heap forces heavy (heap-driven) eviction during recovery.
                var store2 = CreateObjectStore(log, objlog, checkpointDir, recoveryMemorySize);
                var tracker = new LogSizeTracker<LargeObjStoreFunctions, LargeObjAllocator>(store2.Log, recoveryMemorySize, recoveryMemorySize / 8, recoveryMemorySize / 16, logger: null);
                store2.Log.SetLogSizeTracker(tracker);
                _ = await store2.RecoverAsync(default, token).ConfigureAwait(false);

                try
                {
                    using (var session = store2.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                    {
                        var bContext = session.BasicContext;
                        for (var key = 0; key < numObjRecords; key++)
                        {
                            TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
                            TestLargeObjectOutput output = new();
                            var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output);
                            if (status.IsPending)
                            {
                                Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                                (status, output) = GetSinglePendingResult(completedOutputs);
                            }
                            ClassicAssert.IsTrue(status.Found, $"key {key} not found (recoveryMemorySize {recoveryMemorySize})");
                            ClassicAssert.AreEqual(objSize, output.valueObject.value.Length, $"key {key} wrong object size");
                            ClassicAssert.AreEqual((byte)key, output.valueObject.value[0], $"key {key} wrong object payload[0]");
                            ClassicAssert.AreEqual((byte)(key >> 8), output.valueObject.value[1], $"key {key} wrong object payload[1]");
                        }
                    }
                }
                finally
                {
                    store2.Dispose();
                }
            }
            finally
            {
                log.Dispose();
                objlog.Dispose();
            }
        }
    }
}