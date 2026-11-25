// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.spanbyte
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>;

    /// <summary>
    /// Tests that fresh direct-VM log pages are faulted in by one thread before appenders reach them, rather than by the racing appenders.
    /// Residency is read with <c>mincore</c>, so these run only on Linux kernels that support <c>MADV_POPULATE_WRITE</c>.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    internal unsafe class LogPagePrefaultTests : TestBase
    {
        const long PageSize = 4L << 20;
        const int OsPageSize = 4096;

        [DllImport("libc", SetLastError = true)]
        static extern int mincore(nint addr, nuint length, byte* vec);

        [SetUp]
        public void Setup()
        {
            // Support is only known once the kernel has been asked, so probe it with a populate of one page.
            if (OperatingSystem.IsLinux())
            {
                var probe = DirectVirtualMemory.Allocate(OsPageSize, OsPageSize);
                DirectVirtualMemory.Populate(probe.AlignedPtr, OsPageSize);
                DirectVirtualMemory.Free(probe);
            }
            if (!LogPagePrefaulter.IsUseful(PageSize))
                Assert.Ignore("Populating memory requires Linux 5.14 or later");
            DeleteDirectory(MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            _ = NativeAllocatorInitializer.Initialize(false);
            // Native-backed stores free their remaining blocks in a finalizer; drain it so it does not run during a later test.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            DeleteDirectory(MethodTestDir);
        }

        /// <summary>Number of OS pages in [<paramref name="address"/>, +<paramref name="length"/>) that are resident.</summary>
        static int ResidentPages(long address, long length)
        {
            var vec = new byte[length / OsPageSize];
            fixed (byte* v = vec)
                ClassicAssert.AreEqual(0, mincore((nint)address, (nuint)length, v), $"mincore failed: {Marshal.GetLastPInvokeError()}");
            var resident = 0;
            foreach (var b in vec)
                resident += b & 1;
            return resident;
        }

        static void WaitUntil(Func<bool> condition, string message)
        {
            var sw = Stopwatch.StartNew();
            while (!condition())
            {
                if (sw.Elapsed > TimeSpan.FromSeconds(30))
                    Assert.Fail(message);
                Thread.Sleep(1);
            }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void EnsurePopulatedFaultsInAPendingPageAndCancelLeavesItUnmapped()
        {
            var populated = DirectVirtualMemory.Allocate(PageSize, OsPageSize);
            var cancelled = DirectVirtualMemory.Allocate(PageSize, OsPageSize);
            try
            {
                var prefaulter = new LogPagePrefaulter(pageCount: 2, PageSize);
                prefaulter.MarkPending(0, populated.AlignedPtr);
                prefaulter.MarkPending(1, cancelled.AlignedPtr);

                prefaulter.EnsurePopulated(0);
                prefaulter.Cancel(1);

                ClassicAssert.IsFalse(prefaulter.IsPending(0));
                ClassicAssert.IsFalse(prefaulter.IsPending(1));
                ClassicAssert.AreEqual(PageSize / OsPageSize, ResidentPages(populated.AlignedPtr, PageSize), "EnsurePopulated must fault in the whole page");
                ClassicAssert.AreEqual(0, ResidentPages(cancelled.AlignedPtr, PageSize), "A cancelled populate must not run");
            }
            finally
            {
                DirectVirtualMemory.Free(populated);
                DirectVirtualMemory.Free(cancelled);
            }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void WorkerPopulatesAQueuedPage()
        {
            var block = DirectVirtualMemory.Allocate(PageSize, OsPageSize);
            try
            {
                var prefaulter = new LogPagePrefaulter(pageCount: 1, PageSize);
                prefaulter.MarkPending(0, block.AlignedPtr);
                prefaulter.Queue(0);
                WaitUntil(() => !prefaulter.IsPending(0), "The worker did not populate the queued page");
                ClassicAssert.AreEqual(PageSize / OsPageSize, ResidentPages(block.AlignedPtr, PageSize));
            }
            finally
            {
                DirectVirtualMemory.Free(block);
            }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void PagesAreResidentBeforeAppendersFillThem()
        {
            _ = NativeAllocatorInitializer.Initialize(true);
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            using var store = new TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(new()
            {
                IndexSize = 1L << 16,
                LogDevice = log,
                LogMemorySize = PageSize * 8,
                PageSize = PageSize,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;
            var hlog = store.hlogBase;

            Span<int> key = stackalloc int[1];
            Span<int> value = stackalloc int[256];
            var i = 0;
            while (hlog.GetPage(hlog.GetTailAddress()) < 1)
            {
                key[0] = i++;
                _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(key)), MemoryMarshal.Cast<int, byte>(value), Empty.Default);
            }

            // The tail has just turned onto page 1: it must be populated in full even though appenders have written only its first record.
            var page1 = hlog.GetPhysicalAddress(hlog.GetLogicalAddressOfStartOfPage(1));
            ClassicAssert.AreEqual(PageSize / OsPageSize, ResidentPages(page1, PageSize), "The page the tail turns onto must be populated");

            // Page 2 was allocated ahead of the tail, which no appender has touched; the worker populates it.
            var page2 = hlog.GetPhysicalAddress(hlog.GetLogicalAddressOfStartOfPage(2));
            WaitUntil(() => ResidentPages(page2, PageSize) == PageSize / OsPageSize, "The page allocated ahead of the tail was not populated");
        }
    }
}