// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.objects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// Covers allocation while a <see cref="LogSizeTracker{TStoreFunctions, TAllocator}"/> is attached but its background resizer
    /// has not been started. The tracker is wired up by <c>SetLogSizeTracker</c> at store construction, but starting its resizer
    /// is separate: Garnet runs checkpoint recovery without it, because recovery owns the log and does its own budget-aware
    /// eviction, and stops it again at shutdown. (AOF replay is not one of these states; <c>StoreWrapper.ReplayAOF</c> starts the
    /// size trackers first, since replay is ordinary store traffic that must stay within the memory budget.) While the resizer is
    /// not running nothing will act on <see cref="LogSizeTracker{TStoreFunctions, TAllocator}.Signal"/>, so the page-turn path must
    /// neither wait on the tracker (which livelocks the allocation retry loop) nor defer the
    /// <see cref="AllocatorBase{TStoreFunctions, TAllocator}.MaxAllocatedPageCount"/> head shift to it (which lets the log grow
    /// past its page cap, up to <c>BufferSize</c>).
    /// </summary>
    [TestFixture]
    public class UnstartedSizeTrackerAllocationTests : TestBase
    {
        // Enough records to turn far more pages than any of the page caps below, so the page-turn path is exercised repeatedly.
        const int NumRecords = 20000;

        // The workload is single-threaded and bounded, and completes in well under a second when the page-turn path makes
        // progress; a much longer wall time means the allocation retry loop is livelocked waiting on the stopped tracker.
        static readonly TimeSpan CompletionTimeout = TimeSpan.FromSeconds(60);

        IDevice log, objlog;
        TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;
            TestUtils.OnTearDown();
        }

        // logMemoryPages is MaxAllocatedPageCount. 5 and 9 are not powers of two, so BufferSize rounds up to 8 and 16 and
        // leaves slots the page cap must keep us out of; 16 is the power-of-two control, where BufferSize ==
        // MaxAllocatedPageCount and the circular-buffer wrap check in NeedToWaitForClose caps the log on its own.
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void StaysWithinPageCapWhenSizeTrackerNotStarted([Values(5, 9, 16)] int logMemoryPages)
        {
            Prepare(logMemoryPages);

            // Attach a tracker whose budget the log exceeds on log memory alone (MinTargetPageCount is 4 pages, and every
            // logMemoryPages above is larger), but never start its resizer. IsOverBudget is therefore true for most of the
            // run while IsRunning stays false -- the state in which the page-turn path must still enforce the page cap itself.
            var targetSize = (long)LogSizeTracker.MinTargetPageCount * MinKvLogPageSize;
            var tracker = new LogSizeTracker<ClassStoreFunctions, ClassAllocator>(store.Log, targetSize, targetSize / 8, targetSize / 16, logger: null);
            store.Log.SetLogSizeTracker(tracker);
            ClassicAssert.IsFalse(tracker.IsRunning, "the resizer must not be running for this test to be meaningful");

            var allocatorBase = store.Log.allocatorBase;
            var maxAllocatedPageCount = store.Log.MaxAllocatedPageCount;
            ClassicAssert.AreEqual(logMemoryPages, maxAllocatedPageCount, "MaxAllocatedPageCount should be the configured page count");

            // Sample the resident page span on every page turn. Checking only at the end would miss a breach that the
            // circular-buffer wrap check later masks, and an assert inside the loop would report the first breach.
            var maxResidentPages = 0L;
            var upserts = Task.Run(() =>
            {
                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
                var bContext = session.BasicContext;
                var lastTailPage = -1L;
                for (var i = 0; i < NumRecords; i++)
                {
                    _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });

                    var tailPage = allocatorBase.GetPage(store.Log.TailAddress);
                    if (tailPage == lastTailPage)
                        continue;
                    lastTailPage = tailPage;

                    var residentPages = tailPage - allocatorBase.GetPage(store.Log.HeadAddress) + 1;
                    if (residentPages > maxResidentPages)
                        maxResidentPages = residentPages;
                }
            });

            // A regression in NeedToWaitForClose livelocks the retry loop here rather than failing, so bound the wait.
            ClassicAssert.IsTrue(upserts.Wait(CompletionTimeout), "allocation did not complete; the retry loop is most likely waiting on the stopped size tracker");
            upserts.GetAwaiter().GetResult();

            // The workload must actually have turned enough pages for the cap to bind, otherwise the assertions below are vacuous.
            ClassicAssert.Greater(allocatorBase.GetPage(store.Log.TailAddress), (long)maxAllocatedPageCount,
                "expected the workload to turn more pages than the page cap");

            // Both bounds allow a single page of slack: the allocator keeps the page after the tail allocated as an
            // allocate-ahead invariant (AllocateCurrentAndNextPage), so one buffer past the cap is resident by design.
            // When the page cap is deferred to a resizer that never runs, the log instead grows until the circular-buffer
            // wrap check stops it, so both measurements land at BufferSize rather than near the cap.
            ClassicAssert.LessOrEqual(maxResidentPages, (long)maxAllocatedPageCount + 1,
                $"resident page span exceeded MaxAllocatedPageCount ({maxAllocatedPageCount}); the page cap was deferred to a resizer that is not running");

            ClassicAssert.LessOrEqual(allocatorBase.HighWaterAllocatedPageCount, maxAllocatedPageCount + 1,
                $"allocated more page buffers than MaxAllocatedPageCount ({maxAllocatedPageCount}); the page cap was deferred to a resizer that is not running");
        }

        private void Prepare(int logMemoryPages)
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "unstarted-tracker.log"));
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "unstarted-tracker.obj.log"));
            store = new(new()
            {
                IndexSize = 1L << 22,
                LogDevice = log,
                ObjectLogDevice = objlog,
                SegmentSize = 1L << 20,
                LogMemorySize = (long)logMemoryPages * MinKvLogPageSize,
                PageSize = MinKvLogPageSize,
                CheckpointDir = Path.Combine(MethodTestDir, "check-points")
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }
    }
}