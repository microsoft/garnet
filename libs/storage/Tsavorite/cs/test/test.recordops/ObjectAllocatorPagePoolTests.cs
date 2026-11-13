// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
using System;
#endif
using System.IO;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.Objects
{
    using static TestUtils;

    using ObjAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ObjStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// With a <see cref="LogSizeTracker{TStoreFunctions, TAllocator}"/>, a freed ObjectAllocator page enters a pool that
    /// <c>AllocatePage</c> draws from on a page turn. A page turn can take the page as soon as it is added, so the page must be
    /// fully cleared before it is added.
    /// </summary>
    [TestFixture]
    internal unsafe class ObjectAllocatorPagePoolTests : TestBase
    {
        const int PageSizeBits = 16;
        const int PageSize = 1 << PageSizeBits;
        const long LogMemorySize = (long)PageSize << 3;

        // Initialize allocates only pages 0 and 1, so these slots are free for the test to allocate and free directly.
        const int FreedPageIndex = 4;
        const int ReusedPageIndex = 5;

        const byte Stamp = 0xA5;

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
#if !DEBUG
            Assert.Ignore("Requires a test hook that exists only in DEBUG builds");
#else
            var hlog = store.hlog;
            var hlogBase = store.hlogBase;
            var hlogImpl = (ObjectAllocatorImpl<ObjStoreFunctions>)hlogBase;
            Assert.That(hlogBase.IsAllocated(FreedPageIndex) || hlogBase.IsAllocated(ReusedPageIndex), Is.False);

            hlog.AllocatePage(FreedPageIndex);
            Assert.That(hlog.OverflowPageCount, Is.Zero);
            var freedPage = PageStart(FreedPageIndex);

            // Take the page from the pool and write to it at the first point a concurrent page turn can: as soon as it is added,
            // before FreePage returns.
            long reusedPage = 0;
            hlogImpl.testOnPageAddedToPool = () =>
            {
                hlog.AllocatePage(ReusedPageIndex);
                reusedPage = PageStart(ReusedPageIndex);
                PageBody(reusedPage).Fill(Stamp);
            };
            try
            {
                hlog.FreePage(FreedPageIndex);
            }
            finally
            {
                hlogImpl.testOnPageAddedToPool = null;
            }

            Assert.That(reusedPage, Is.EqualTo(freedPage), "The freed page was not taken from the pool");
            Assert.That(PageBody(reusedPage).IndexOfAnyExcept(Stamp), Is.EqualTo(-1), "Freeing the page erased writes made after it was taken from the pool");

            hlog.FreePage(ReusedPageIndex);

            long PageStart(int page) => hlogBase.GetPhysicalAddress(hlogBase.GetLogicalAddressOfStartOfPage(page));

            static Span<byte> PageBody(long pageStart) => new((byte*)pageStart + PageHeader.Size, PageSize - PageHeader.Size);
#endif
        }
    }
}