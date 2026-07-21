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
    }
}
