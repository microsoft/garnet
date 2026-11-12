// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.Objects
{
    using static TestUtils;

    using ObjAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ObjStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// With a <see cref="LogSizeTracker{TStoreFunctions, TAllocator}"/>, a freed ObjectAllocator page enters a pool that
    /// <c>AllocatePage</c> draws from on a page turn. The page must be fully cleared before it enters the pool: a page turn
    /// can take it from the pool immediately, and any clearing still in progress would then erase records written to it.
    /// </summary>
    [TestFixture]
    internal unsafe class ObjectAllocatorPagePoolTests : TestBase
    {
        // Large pages make clearing slow enough that taking the page from the pool overlaps any clearing still in progress.
        const int PageSizeBits = 24;
        const int PageSize = 1 << PageSizeBits;
        const long LogMemorySize = (long)PageSize << 3;

        // Initialize allocates only pages 0 and 1, so these slots are free for the test to allocate and free directly.
        const int FreedPageIndex = 4;
        const int ReusedPageIndex = 5;

        const int StampStride = 4096;
        const byte Stamp = 0xA5;
        const int Iterations = 8;

        static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

        TsavoriteKV<ObjStoreFunctions, ObjAllocator> store;
        IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectAllocatorPagePool.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectAllocatorPagePool.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                LogMemorySize = LogMemorySize,
                PageSize = PageSize
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(), DefaultRecordTriggers.Instance)
             , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            // FreePage returns pages to the pool only when a size tracker is set; it does not need to be started.
            store.Log.SetLogSizeTracker(new LogSizeTracker<ObjStoreFunctions, ObjAllocator>(store.Log, LogMemorySize, LogMemorySize / 8, LogMemorySize / 16, logger: null));
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;
            OnTearDown();
        }

        [Test, Category(TsavoriteKVTestCategory), Category(SmokeTestCategory)]
        public void PageTakenFromPoolKeepsWhatIsWrittenToIt()
        {
            var hlog = store.hlog;
            var hlogBase = store.hlogBase;
            Assert.That(hlogBase.IsAllocated(FreedPageIndex) || hlogBase.IsAllocated(ReusedPageIndex), Is.False);

            for (var iteration = 0; iteration < Iterations; iteration++)
            {
                hlog.AllocatePage(FreedPageIndex);
                Assert.That(hlog.OverflowPageCount, Is.Zero);

                using var ready = new ManualResetEventSlim();
                using var freed = new ManualResetEventSlim();

                // Take the page as soon as it is pooled, as a concurrent page turn would, and write to it from the end backward
                // so the writes land ahead of any clearing still in progress.
                var reuse = Task.Factory.StartNew(() =>
                {
                    ready.Set();
                    while (hlog.OverflowPageCount == 0)
                        Thread.SpinWait(1);
                    hlog.AllocatePage(ReusedPageIndex);

                    var page = (byte*)hlogBase.GetPhysicalAddress(hlogBase.GetLogicalAddressOfStartOfPage(ReusedPageIndex));
                    for (var offset = PageSize - StampStride; offset >= StampStride; offset -= StampStride)
                        page[offset] = Stamp;

                    freed.Wait();
                    var lost = 0;
                    for (var offset = StampStride; offset < PageSize; offset += StampStride)
                    {
                        if (page[offset] != Stamp)
                            lost++;
                    }
                    return lost;
                }, TaskCreationOptions.LongRunning);

                Assert.That(ready.Wait(Timeout), Is.True);
                hlog.FreePage(FreedPageIndex);
                freed.Set();

                Assert.That(reuse.Wait(Timeout), Is.True, "The reusing thread did not complete");
                Assert.That(reuse.Result, Is.Zero, $"Iteration {iteration}: freeing the page erased writes made after it was taken from the pool");

                // Return the reused page so the next iteration starts by taking it back from the pool.
                hlog.FreePage(ReusedPageIndex);
            }
        }
    }
}