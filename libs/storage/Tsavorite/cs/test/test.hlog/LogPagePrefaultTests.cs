// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
    /// Tests that each huge page of a fresh direct-VM log page is faulted in by one thread before appenders reach it, rather than by the racing
    /// appenders. Residency is read with <c>mincore</c>, so these run only on Linux kernels that support <c>MADV_POPULATE_WRITE</c>.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    internal unsafe class LogPagePrefaultTests : TestBase
    {
        const long ChunkSize = 2L << 20;
        const long PageSize = 8 * ChunkSize;
        static readonly int OsPageSize = Environment.SystemPageSize;
        static long ChunkPages => ChunkSize / OsPageSize;

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
        public void HelpPopulatesChunksInOrderUntilTheTargetIsClaimed()
        {
            var block = DirectVirtualMemory.Allocate(PageSize, OsPageSize);
            try
            {
                var prefaulter = new LogPagePrefaulter(pageCount: 1, PageSize);
                prefaulter.MarkPending(0, block.AlignedPtr);

                prefaulter.Help(0, target: 3);
                ClassicAssert.AreEqual(3 * ChunkPages, ResidentPages(block.AlignedPtr, 3 * ChunkSize), "The first three chunks must be populated");
                ClassicAssert.AreEqual(0, ResidentPages(block.AlignedPtr + 3 * ChunkSize, PageSize - 3 * ChunkSize), "Only the target must be claimed");
                ClassicAssert.IsTrue(prefaulter.IsPending(0));

                // A target already claimed claims nothing more
                prefaulter.Help(0, target: 2);
                ClassicAssert.AreEqual(3 * ChunkPages, ResidentPages(block.AlignedPtr, PageSize));

                prefaulter.EnsurePopulated(0);
                ClassicAssert.AreEqual(PageSize / OsPageSize, ResidentPages(block.AlignedPtr, PageSize));
                ClassicAssert.IsFalse(prefaulter.IsPending(0));
            }
            finally
            {
                DirectVirtualMemory.Free(block);
            }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void ConcurrentHelpersPopulateEveryChunk()
        {
            const long Length = 64 * ChunkSize;
            var block = DirectVirtualMemory.Allocate(Length, OsPageSize);
            try
            {
                var prefaulter = new LogPagePrefaulter(pageCount: 1, Length);
                prefaulter.MarkPending(0, block.AlignedPtr);

                var threads = new Thread[8];
                for (var t = 0; t < threads.Length; t++)
                {
                    var target = (t + 1) * 8;
                    threads[t] = new Thread(() => prefaulter.Help(0, target));
                    threads[t].Start();
                }
                foreach (var thread in threads)
                    thread.Join();

                ClassicAssert.IsFalse(prefaulter.IsPending(0), "Every chunk must be claimed and finished");
                ClassicAssert.AreEqual(Length / OsPageSize, ResidentPages(block.AlignedPtr, Length));
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
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            using var store = CreateNativeStore(log);
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;
            var hlog = store.hlogBase;

            var i = 0;
            while (hlog.GetPage(hlog.GetTailAddress()) < 1)
                Upsert(bContext, i++);

            // The tail has just turned onto page 1: it must be populated in full even though appenders have written only its first record.
            var page1 = hlog.GetPhysicalAddress(hlog.GetLogicalAddressOfStartOfPage(1));
            ClassicAssert.AreEqual(PageSize / OsPageSize, ResidentPages(page1, PageSize), "The page the tail turns onto must be populated");

            // Page 2 was allocated ahead of the tail; the page turn populated its first chunks, and no appender has touched it.
            var page2 = hlog.GetPhysicalAddress(hlog.GetLogicalAddressOfStartOfPage(2));
            ClassicAssert.AreEqual(LogPagePrefaulter.Lead * ChunkPages, ResidentPages(page2, PageSize), "The page turn must start populating the next page");

            // Each allocation that crosses into another chunk of page 1 populates one more chunk of page 2.
            for (var chunk = 1; chunk <= PageSize / ChunkSize - LogPagePrefaulter.Lead; chunk++)
            {
                while (hlog.GetOffsetOnPage(hlog.GetTailAddress()) < chunk * ChunkSize)
                    Upsert(bContext, i++);
                ClassicAssert.AreEqual((chunk + LogPagePrefaulter.Lead) * ChunkPages, ResidentPages(page2, PageSize), $"after the tail crossed into chunk {chunk}");
            }
            ClassicAssert.AreEqual(PageSize / OsPageSize, ResidentPages(page2, PageSize), "The next page must be populated before the tail reaches the end of the page");
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void ThePageTurnPopulatesTheChunksAppendersLeft()
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            using var store = CreateNativeStore(log);
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;
            var hlog = store.hlogBase;

            // Records of 5.5 MB cross into chunks 2 and 5 of page 0, claiming chunks 0-6 of page 1, and the third does not fit, so no allocation
            // on page 0 claims chunk 7.
            var value = new int[(int)(11 * ChunkSize / 4 / sizeof(int))];
            for (var i = 0; i < 3; i++)
                Upsert(bContext, i, value);
            ClassicAssert.AreEqual(1, hlog.GetPage(hlog.GetTailAddress()));

            // The first record on page 1 writes only its first three chunks.
            var page1 = hlog.GetPhysicalAddress(hlog.GetLogicalAddressOfStartOfPage(1));
            ClassicAssert.AreEqual(PageSize / OsPageSize, ResidentPages(page1, PageSize), "The page turn must populate the chunk no appender claimed");
        }

        static TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> CreateNativeStore(IDevice log)
        {
            _ = NativeAllocatorInitializer.Initialize(true);
            return new(new()
            {
                IndexSize = 1L << 16,
                LogDevice = log,
                LogMemorySize = PageSize * 8,
                PageSize = PageSize,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        static void Upsert(BasicContext<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> bContext, int k)
        {
            Span<int> value = stackalloc int[256];
            Upsert(bContext, k, value);
        }

        static void Upsert(BasicContext<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> bContext, int k, ReadOnlySpan<int> value)
        {
            Span<int> key = [k];
            _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(key)), MemoryMarshal.Cast<int, byte>(value), Empty.Default);
        }
    }
}