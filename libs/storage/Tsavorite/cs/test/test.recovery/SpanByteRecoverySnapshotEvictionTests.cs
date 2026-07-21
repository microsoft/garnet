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
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>;

    /// <summary>
    /// Recovers an inline (SpanByte) store's Snapshot checkpoint into a smaller memory budget than was checkpointed,
    /// with a <see cref="LogSizeTracker{TStoreFunctions, TAllocator}"/> attached so recovery must evict snapshot pages
    /// to honor the budget. Every record — whether it stayed resident or was evicted to disk — must read back correctly.
    ///
    /// Regression for issue #1950: <c>RecoverSnapshotPages</c> skipped flushing the last read-batch of snapshot pages
    /// (assuming they stay resident), but the budget-driven recovery eviction floor (<see cref="LogSizeTracker.MinEvictionHeadAddressLag"/>
    /// / <c>MaxAllocatedPageCount</c>) is finer-grained than the read batch, so it could free a page that was never
    /// flushed to the main log. After <c>RecoveryReset</c> marked that page's range flushed, a later read of it did a
    /// 0-byte disk read and returned NOTFOUND (a lost key). Only the inline store was affected (the object-store path
    /// already flushed every page).
    /// </summary>
    [TestFixture]
    public class SpanByteRecoverySnapshotEvictionTests : TestBase
    {
        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        const int NumRecords = 2000;                 // spans enough pages that recovery reads in multiple batches and must evict
        const long InitialMemorySize = 25 * 1024;    // store1 budget; its own eviction flushes most pages before the checkpoint

        // The evict-before-flush interleaving is timing-dependent (a page's recovery flush completing vs. TrimLogPages
        // choosing to evict it), so repeat a few times to reliably catch a regression rather than relying on one recovery.
        const int RepeatCount = 15;

        sealed class MyFunctions : SimpleLongSimpleFunctions { }

        static TsavoriteKV<LongStoreFunctions, LongAllocator> CreateStore(IDevice log, string checkpointDir, long memorySize)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = MinKvLogPageSize,
                LogMemorySize = memorySize,
                CheckpointDir = checkpointDir,
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        // recoveryMemorySize 18k => power-of-two BufferSize; 23k => non-power-of-two (MaxAllocatedPageCount 5, BufferSize 8)
        // so the read batch exceeds the resident set, the layout that surfaced #1950. startResizerDuringReads also runs the
        // background resizer concurrently with the post-recovery reads.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoverSnapshotUnderMemoryBudgetReadsAllRecords(
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
                try
                {
                    // Write all records and take a Snapshot checkpoint. store1's own eviction (under InitialMemorySize)
                    // flushes most pages to the main log, leaving a still-mutable snapshot region captured in the snapshot file.
                    Guid token;
                    using (var store1 = CreateStore(log, checkpointDir, InitialMemorySize))
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
                    var store2 = CreateStore(log, checkpointDir, recoveryMemorySize);
                    var tracker = new LogSizeTracker<LongStoreFunctions, LongAllocator>(store2.Log, recoveryMemorySize, recoveryMemorySize / 8, recoveryMemorySize / 16, logger: null);
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
                    try { Directory.Delete(dir, recursive: true); } catch { }
                }
            }
        }
    }
}