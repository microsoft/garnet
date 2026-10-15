// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.Objects
{
    using static TestUtils;

    using ObjAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ObjStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// The eviction range the resizer computes must stay behind TailAddress.
    /// </summary>
    /// <remarks>
    /// The record scan stops at <c>TailAddress - MinEvictionHeadAddressLag</c>, which falls inside the tail page when the page
    /// is larger than that lag. Exhausting the scanned range completes the page, and completing a page advances headAddress to
    /// the start of the next one -- past TailAddress. <c>ShiftHeadAddress</c> caps HeadAddress at FlushedUntilAddress while
    /// <c>ShiftAddressesWithWait</c> waits on the uncapped value, so such a headAddress is waited on forever and the resizer
    /// never runs again. Reaching it needs the heap over budget with most of the overage held by records inside the lag, so
    /// the scan runs out of range before it has trimmed enough.
    /// </remarks>
    [TestFixture]
    internal class LogSizeTrackerEvictionRangeTests : TestBase
    {
        // Larger than MinEvictionHeadAddressLag, so the scan limit lands inside the tail page rather than before it.
        const int PageSize = 1 << 13;                               // 8 KB
        const long LogMemorySize = PageSize << 4;

        // The budget floor, so the heap alone decides whether we are over budget.
        const long TargetSize = PageSize * LogSizeTracker.MinTargetPageCount;

        // Leaves the tail inside the tail page and beyond MinEvictionHeadAddressLag, so the scan limit is above BeginAddress.
        const long FillUntilTailOffset = PageSize - (PageSize / 4);

        static readonly TimeSpan CompletionTimeout = TimeSpan.FromSeconds(60);

        TsavoriteKV<ObjStoreFunctions, ObjAllocator> store;
        LogSizeTracker<ObjStoreFunctions, ObjAllocator> tracker;
        IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "EvictionRange.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "EvictionRange.obj.log"), deleteOnClose: true);

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
            store?.Dispose();
            store = null;
            tracker = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;
            OnTearDown();
        }

        [Test, Category(TsavoriteKVTestCategory), Category(SmokeTestCategory), Category(ObjectIdMapCategory)]
        public void ResizerDoesNotEvictPastTheTailWhenTheHeapIsHeldInsideTheEvictionLag()
        {
            var tailLimit = store.hlogBase.GetLogicalAddressOfStartOfPage(0) + FillUntilTailOffset;

            using (var session = store.NewSession<TestObjectKey, Empty, Empty, Empty, SizedHeapObjectFunctions>(new SizedHeapObjectFunctions()))
            {
                var bContext = session.BasicContext;

                // Records carrying almost no heap, so the scan below MinEvictionHeadAddressLag cannot cover the overage.
                var key = 0;
                while (store.hlogBase.GetTailAddress() < tailLimit)
                    _ = bContext.Upsert(new TestObjectKey { key = key++ }, new SizedHeapObject(8));

                // All of the overage sits in the final record, which is inside the lag and so cannot be evicted.
                _ = bContext.Upsert(new TestObjectKey { key = key }, new SizedHeapObject(TargetSize * 4));
            }

            var tailAddress = store.hlogBase.GetTailAddress();
            ClassicAssert.AreEqual(0, store.hlogBase.GetPage(tailAddress), "The tail must stay on the first page for the scan limit to fall inside it");
            Assert.That(tailAddress, Is.GreaterThan(LogSizeTracker.MinEvictionHeadAddressLag), "The scan limit must be above BeginAddress");
            Assert.That(tracker.IsOverBudget, Is.True, $"Test did not exceed the tracker budget. Tracker: {tracker}");

            var startingReadOnlyAddress = store.hlogBase.ReadOnlyAddress;
            tracker.Start(CancellationToken.None);

            // ShiftAddressesWithWait shifts ReadOnlyAddress before HeadAddress, so this confirms the resizer reached the shift
            // rather than seeing the stop below and exiting its loop first, which would make the test vacuous.
            var deadline = DateTime.UtcNow + CompletionTimeout;
            while (store.hlogBase.ReadOnlyAddress == startingReadOnlyAddress && DateTime.UtcNow < deadline)
                _ = Thread.Yield();
            Assert.That(store.hlogBase.ReadOnlyAddress, Is.GreaterThan(startingReadOnlyAddress),
                $"The resizer did not shift addresses within {CompletionTimeout}; it is waiting on a flush or an eviction for an"
                + $" address past TailAddress {tailAddress}. Tracker: {tracker}");

            // Stop waits for the resizer to reach Stopped, which it cannot do while waiting on an eviction past the tail.
            // Run on a worker so a regression fails the test instead of hanging the test host forever.
            var stop = Task.Run(() => tracker.Stop(wait: true));
            Assert.That(stop.Wait(CompletionTimeout), Is.True,
                $"The resizer did not stop within {CompletionTimeout}; it is waiting on an eviction past TailAddress {tailAddress}. Tracker: {tracker}");
            stop.GetAwaiter().GetResult();

            Assert.That(store.hlogBase.HeadAddress, Is.LessThanOrEqualTo(tailAddress), "HeadAddress must never pass TailAddress");
        }
    }
}