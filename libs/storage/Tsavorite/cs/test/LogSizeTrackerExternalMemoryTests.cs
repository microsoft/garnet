// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.LogSizeTrackerExternalMemory
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>;

    /// <summary>
    /// Memory that lives outside the log but is charged against its budget must make the log shed pages.
    /// </summary>
    /// <remarks>
    /// Garnet's hash index holds one entry per distinct key, including keys whose records live only on disk, and grows
    /// into overflow buckets that no index setting bounds. <see cref="LogSizeTracker{TStoreFunctions, TAllocator}.ExternalMemorySizeProvider"/>
    /// reports that memory to the tracker, which subtracts it from the log's budget so the log sheds pages as the index grows.
    /// </remarks>
    [TestFixture]
    public class LogSizeTrackerExternalMemoryTests : TestBase
    {
        const int PageSize = MinKvLogPageSize;              // 4 KB
        const long LogMemorySize = PageSize * 64;           // 256 KB
        const long TargetSize = LogMemorySize;
        const int NumRecords = 20_000;                      // Many pages' worth, so the log fills its budget

        static readonly TimeSpan CompletionTimeout = TimeSpan.FromSeconds(60);

        IDevice log;
        TsavoriteKV<LongStoreFunctions, LongAllocator> store;
        LogSizeTracker<LongStoreFunctions, LongAllocator> tracker;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            _ = Directory.CreateDirectory(MethodTestDir);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ExternalMemory.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 16,
                LogDevice = log,
                MutableFraction = 0.9,
                PageSize = PageSize,
                LogMemorySize = LogMemorySize,
                SegmentSize = 1L << 22,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
             , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            tracker = new LogSizeTracker<LongStoreFunctions, LongAllocator>(store.Log, TargetSize, TargetSize / 10, TargetSize / 50, logger: null);
            store.Log.SetLogSizeTracker(tracker);
        }

        [TearDown]
        public void TearDown()
        {
            tracker?.Stop(wait: true);
            store?.Dispose();
            store = null;
            tracker = null;
            log?.Dispose();
            log = null;
            OnTearDown();
        }

        void Populate()
        {
            using var session = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions((a, b) => a + b));
            var bContext = session.BasicContext;
            for (long key = 0; key < NumRecords; key++)
                _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), SpanByte.FromPinnedVariable(ref key));
        }

        /// <summary>
        /// External memory reduces the budget the log is held to, so a log that is comfortably within its configured target
        /// is trimmed once memory outside it is charged against the same budget.
        /// </summary>
        [Test, Category("TsavoriteKV"), Category(SmokeTestCategory)]
        public void ExternalMemoryTrimsTheLog()
        {
            tracker.Start(CancellationToken.None);
            Populate();

            var sizeBeforeCharge = WaitForStableSize();
            Assert.That(tracker.ExternalMemorySize, Is.EqualTo(0), "No external memory has been declared yet");
            Assert.That(sizeBeforeCharge, Is.GreaterThan(TargetSize / 2),
                $"Test must fill the log before charging external memory against it, else there is nothing to trim. Tracker: {tracker}");

            // Charge three quarters of the budget to memory outside the log. Without the external-memory term this is
            // invisible to the tracker and the log keeps every page it holds.
            var externalSize = TargetSize * 3 / 4;
            tracker.ExternalMemorySizeProvider = () => externalSize;
            tracker.RefreshExternalMemorySize();

            Assert.That(tracker.ExternalMemorySize, Is.EqualTo(externalSize));

            var sizeAfterCharge = WaitUntil(() => tracker.TotalSize <= TargetSize - externalSize + (TargetSize / 10));
            Assert.That(sizeAfterCharge, Is.LessThan(sizeBeforeCharge),
                $"The log must shed pages once external memory is charged against its budget. Tracker: {tracker}");
            Assert.That(sizeAfterCharge + externalSize, Is.LessThanOrEqualTo(TargetSize + (TargetSize / 10)),
                $"Log plus external memory must stay within the configured budget. Tracker: {tracker}");
        }

        /// <summary>
        /// The reduced budget is floored at <see cref="LogSizeTracker.MinTargetPageCount"/> pages, the same floor an explicitly
        /// configured target is held to. External memory larger than the whole budget must therefore trim the log to that floor
        /// and stop, not drive the eviction-range computation below the resident span.
        /// </summary>
        [Test, Category("TsavoriteKV"), Category(SmokeTestCategory)]
        public void ExternalMemoryLargerThanBudgetTrimsToFloorAndKeepsServing()
        {
            tracker.Start(CancellationToken.None);
            Populate();
            _ = WaitForStableSize();

            tracker.ExternalMemorySizeProvider = () => TargetSize * 4;
            tracker.RefreshExternalMemorySize();

            var floor = (long)PageSize * LogSizeTracker.MinTargetPageCount;
            _ = WaitUntil(() => tracker.TotalSize <= floor * 2);

            // The log is at its floor, but the store still serves reads: recent keys from memory, older ones from disk.
            using var session = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions((a, b) => a + b));
            var bContext = session.BasicContext;
            for (long key = NumRecords - 8; key < NumRecords; key++)
            {
                long output = default;
                var status = bContext.Read(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), ref output);
                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    using (outputs)
                    {
                        Assert.That(outputs.Next(), Is.True);
                        status = outputs.Current.Status;
                        output = outputs.Current.Output;
                    }
                }
                Assert.That(status.Found, Is.True, $"Key {key} must still be readable. Tracker: {tracker}");
                Assert.That(output, Is.EqualTo(key));
            }
        }

        /// <summary>Waits until the log size stops changing, and returns it.</summary>
        long WaitForStableSize()
        {
            var sw = Stopwatch.StartNew();
            var previous = -1L;
            while (sw.Elapsed < CompletionTimeout)
            {
                var current = tracker.TotalSize;
                if (current == previous)
                    return current;
                previous = current;
                Thread.Sleep(100);
            }
            Assert.Fail($"Log size did not stabilize within {CompletionTimeout}. Tracker: {tracker}");
            return 0;
        }

        /// <summary>Waits for <paramref name="predicate"/>, and returns the log size once it holds.</summary>
        long WaitUntil(Func<bool> predicate)
        {
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < CompletionTimeout)
            {
                if (predicate())
                    return tracker.TotalSize;
                Thread.Sleep(50);
            }
            Assert.Fail($"Condition was not met within {CompletionTimeout}. Tracker: {tracker}");
            return 0;
        }
    }
}