// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.Objects
{
    using static TestUtils;

    using ObjAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ObjStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// Sustained over-budget pressure with the resizer running: the case the signal-coalescing in
    /// <see cref="LogSizeTracker{TStoreFunctions, TAllocator}"/> exists for.
    /// </summary>
    /// <remarks>
    /// When the heap stays over budget, two paths ask the resizer to run on every attempt: every object-bearing
    /// record update (<c>InternalUpsert</c> -> <c>IncrementSize</c>), and every page-turn retry
    /// (<c>AllocatorBase.NeedToWaitForClose</c> -> <c>Signal</c>), the latter spinning for as long as a thread is
    /// blocked waiting for eviction. Each of those used to raise a real signal, which allocates a
    /// <see cref="SemaphoreSlim"/>. The information content of all of them is one bit -- "go look at the size" --
    /// so they are coalesced behind a pending flag and only the edge raises a signal.
    /// <para>
    /// Measured on this workload (200,000 records, 8 threads, over budget for ~95% of samples): about 245,000
    /// signal attempts collapse to roughly 1,900 actual signals, a ~125x suppression and about 21 MB less gen-0
    /// garbage. Wall time is unchanged, so this is a GC-pressure and contention result, not a throughput one.
    /// </para>
    /// </remarks>
    [TestFixture]
    internal class LogSizeTrackerSignalCoalescingTests : TestBase
    {
        const int PageSize = 1 << MinKvLogPageSizeBits;             // 4 KB
        const long LogMemorySize = PageSize << 4;                   // MaxAllocatedPageCount == BufferSize == 16
        const long TargetSize = LogMemorySize;
        const int ObjectHeapSize = 4096;                            // large relative to the budget, so eviction cannot keep up
        const int InserterThreads = 8;
        const int RecordsPerThread = 25_000;
        const int TotalRecords = InserterThreads * RecordsPerThread;

        static readonly TimeSpan CompletionTimeout = TimeSpan.FromSeconds(120);

        TsavoriteKV<ObjStoreFunctions, ObjAllocator> store;
        LogSizeTracker<ObjStoreFunctions, ObjAllocator> tracker;
        IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SignalCoalescing.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SignalCoalescing.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                LogMemorySize = LogMemorySize,
                PageSize = PageSize
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new SizedHeapObject.Serializer(), DefaultRecordTriggers.Instance)
             , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            tracker = new LogSizeTracker<ObjStoreFunctions, ObjAllocator>(store.Log, TargetSize, TargetSize / 8, TargetSize / 16, logger: null);
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
            objlog?.Dispose();
            objlog = null;
            OnTearDown();
        }

        [Test, Category(TsavoriteKVTestCategory), Category(ObjectIdMapCategory)]
        public void SustainedOverBudgetPressureCoalescesSignalsAndMakesProgress()
        {
            tracker.Start(CancellationToken.None);

            var overBudgetSamples = 0;
            var totalSamples = 0;
            var stopSampling = false;
            var sampler = new Thread(() =>
            {
                while (!Volatile.Read(ref stopSampling))
                {
                    ++totalSamples;
                    if (tracker.IsOverBudget)
                        ++overBudgetSamples;
                    Thread.Sleep(5);
                }
            })
            {
                // Belt and braces alongside the finally below: a sampler that somehow outlived the test must not
                // keep the test host alive, and must not still be reading tracker once TearDown has cleared it.
                IsBackground = true
            };

            var sw = Stopwatch.StartNew();
            sampler.Start();

            var threads = new List<Thread>();
            Exception failure = null;
            try
            {
                for (var t = 0; t < InserterThreads; t++)
                {
                    var threadId = t;
                    var thread = new Thread(() =>
                    {
                        try
                        {
                            using var session = store.NewSession<TestObjectKey, Empty, Empty, Empty, SizedHeapObjectFunctions>(new SizedHeapObjectFunctions());
                            var bContext = session.BasicContext;
                            for (var i = 0; i < RecordsPerThread; i++)
                                _ = bContext.Upsert(new TestObjectKey { key = (threadId * RecordsPerThread) + i }, new SizedHeapObject(ObjectHeapSize));
                        }
                        catch (Exception e) { _ = Interlocked.CompareExchange(ref failure, e, null); }
                    });
                    thread.Start();
                    threads.Add(thread);
                }

                foreach (var thread in threads)
                    Assert.That(thread.Join(CompletionTimeout), Is.True,
                        $"Inserters did not finish within {CompletionTimeout}; the allocation path is not making progress. Tracker: {tracker}");
            }
            finally
            {
                // A timed-out inserter throws from the assertion above. Without this the sampler keeps running and
                // keeps reading tracker, which TearDown then clears, so the intended timeout failure is replaced by
                // a NullReferenceException on the sampler thread.
                sw.Stop();
                Volatile.Write(ref stopSampling, true);
                _ = sampler.Join(CompletionTimeout);
            }

            if (failure is not null)
                Assert.Fail(failure.ToString());

            var signals = tracker.ResizerSignalCount;
            var overBudgetPercent = totalSamples == 0 ? 0 : 100.0 * overBudgetSamples / totalSamples;
            TestContext.Out.WriteLine($"MEASURE records={TotalRecords} threads={InserterThreads} elapsedMs={sw.Elapsed.TotalMilliseconds:F0} " +
                $"signals={signals} signalsPerRecord={(double)signals / TotalRecords:F4} " +
                $"overBudgetSamples={overBudgetSamples}/{totalSamples} ({overBudgetPercent:F0}%)");

            // The premise: the resizer could not keep up, so the signalling paths really were exercised. If this
            // fails the machine evicted faster than it inserted and the rest of the test proves nothing.
            Assert.That(overBudgetPercent, Is.GreaterThan(50.0),
                $"Test did not sustain over-budget pressure ({overBudgetPercent:F0}% of samples); the coalescing assertion below would be vacuous. Tracker: {tracker}");
            Assert.That(signals, Is.GreaterThan(0), "the resizer must actually have been signalled");

            // Coalescing: signals are bounded by resizer wakeups, not by the number of callers asking for one.
            // Without coalescing this workload raises more than one signal per record (the page-turn retry path
            // adds to the per-record one) and so lands two orders of magnitude above this bound.
            Assert.That(signals, Is.LessThan(TotalRecords / 10),
                $"Expected signals to be coalesced well below one per ten records, but saw {signals} for {TotalRecords} records");
        }
    }
}