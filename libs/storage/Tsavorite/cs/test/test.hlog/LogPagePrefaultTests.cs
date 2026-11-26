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
                prefaulter.MarkPending(0, populated.AlignedPtr, firstChunkWritten: false);
                prefaulter.MarkPending(1, cancelled.AlignedPtr, firstChunkWritten: false);

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
        public void ChunksArePopulatedInOrderOneAtATime()
        {
            var block = DirectVirtualMemory.Allocate(PageSize, OsPageSize);
            try
            {
                var prefaulter = new LogPagePrefaulter(pageCount: 1, PageSize);
                prefaulter.MarkPending(0, block.AlignedPtr, firstChunkWritten: false);

                for (var i = 0; i < 3; i++)
                    ClassicAssert.IsTrue(prefaulter.TryPopulateChunk(0));
                ClassicAssert.AreEqual(3 * ChunkPages, ResidentPages(block.AlignedPtr, 3 * ChunkSize), "The first three chunks must be populated");
                ClassicAssert.AreEqual(0, ResidentPages(block.AlignedPtr + 3 * ChunkSize, PageSize - 3 * ChunkSize), "Only three chunks must be populated");
                ClassicAssert.IsTrue(prefaulter.IsPending(0));

                prefaulter.PopulateUnclaimed(0);
                ClassicAssert.AreEqual(PageSize / OsPageSize, ResidentPages(block.AlignedPtr, PageSize));
                ClassicAssert.IsFalse(prefaulter.IsPending(0));
                ClassicAssert.IsFalse(prefaulter.TryPopulateChunk(0), "No chunk must be left to populate");
            }
            finally
            {
                DirectVirtualMemory.Free(block);
            }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void AFirstChunkTheAllocatorWritesIsLeftToIt()
        {
            var block = DirectVirtualMemory.Allocate(PageSize, OsPageSize);
            try
            {
                var prefaulter = new LogPagePrefaulter(pageCount: 1, PageSize);
                prefaulter.MarkPending(0, block.AlignedPtr, firstChunkWritten: true);

                ClassicAssert.IsTrue(prefaulter.TryPopulateChunk(0));
                ClassicAssert.AreEqual(0, ResidentPages(block.AlignedPtr, ChunkSize), "The first chunk must be left to the allocating thread");
                ClassicAssert.AreEqual(ChunkPages, ResidentPages(block.AlignedPtr + ChunkSize, ChunkSize), "The second chunk must be populated first");

                prefaulter.PopulateUnclaimed(0);
                ClassicAssert.AreEqual(0, ResidentPages(block.AlignedPtr, ChunkSize));
                ClassicAssert.AreEqual(PageSize / OsPageSize - ChunkPages, ResidentPages(block.AlignedPtr + ChunkSize, PageSize - ChunkSize));
                ClassicAssert.IsFalse(prefaulter.IsPending(0));
            }
            finally
            {
                DirectVirtualMemory.Free(block);
            }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void ConcurrentThreadsPopulateEveryChunk()
        {
            const long Length = 64 * ChunkSize;
            var block = DirectVirtualMemory.Allocate(Length, OsPageSize);
            try
            {
                var prefaulter = new LogPagePrefaulter(pageCount: 1, Length);
                prefaulter.MarkPending(0, block.AlignedPtr, firstChunkWritten: false);

                var threads = new Thread[8];
                for (var t = 0; t < threads.Length; t++)
                {
                    threads[t] = new Thread(() => prefaulter.PopulateUnclaimed(0));
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
        public void PagesArePopulatedAtPageTurns()
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            using var store = CreateNativeStore(log);
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;
            var hlog = store.hlogBase;

            // Page 1 was allocated ahead of the tail with the store, and only its header has been written.
            var page1 = hlog.GetPhysicalAddress(hlog.GetLogicalAddressOfStartOfPage(1));
            ClassicAssert.LessOrEqual(ResidentPages(page1, PageSize), ChunkPages);

            var i = 0;
            while (hlog.GetPage(hlog.GetTailAddress()) < 1)
                Upsert(bContext, i++);

            // The tail has just turned onto page 1: it must be populated in full even though appenders have written only its first record.
            ClassicAssert.AreEqual(PageSize / OsPageSize, ResidentPages(page1, PageSize), "The page the tail turns onto must be populated");

            // Page 2 was allocated ahead of the tail at the turn, and the thread that turned the tail populated it.
            var page2 = hlog.GetPhysicalAddress(hlog.GetLogicalAddressOfStartOfPage(2));
            ClassicAssert.AreEqual(PageSize / OsPageSize, ResidentPages(page2, PageSize), "The page turn must populate the next page");
        }

        /// <summary>
        /// A store operation reads the tail before it allocates, so while a page turn is in progress it waits for the tail to settle.
        /// </summary>
        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void AnOperationWaitingForAPageTurnPopulatesAheadOfTheTail()
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            using var store = CreateNativeStore(log);
            var hlog = store.hlogBase;
            var page1 = hlog.GetPhysicalAddress(hlog.GetLogicalAddressOfStartOfPage(1));

            // Hold the tail as if a page turn were in progress, so that an allocation waits for it.
            var tail = hlog.TailPageOffset;
            var turning = tail;
            turning.Offset = (int)PageSize + 1;
            _ = Interlocked.Exchange(ref hlog.TailPageOffset.PageAndOffset, turning.PageAndOffset);

            var upsert = new Thread(() =>
            {
                using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
                Upsert(session.BasicContext, 0);
            });
            upsert.Start();
            try
            {
                var sw = Stopwatch.StartNew();
                while (ResidentPages(page1, PageSize) < PageSize / OsPageSize)
                {
                    ClassicAssert.Less(sw.Elapsed, TimeSpan.FromSeconds(30), "The waiting allocation did not populate the page ahead of the tail");
                    Thread.Sleep(1);
                }
            }
            finally
            {
                _ = Interlocked.Exchange(ref hlog.TailPageOffset.PageAndOffset, tail.PageAndOffset);
                upsert.Join();
            }
        }

        /// <summary>
        /// An allocation that finds a page turn in progress, such as one that reached the end of the page just after the thread turning it,
        /// retries until the turn is done.
        /// </summary>
        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void AnAllocationRetryingDuringAPageTurnPopulatesAheadOfTheTail()
        {
            _ = NativeAllocatorInitializer.Initialize(true);
            using var device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "log.log"), deleteOnClose: true);
            using var log = new TsavoriteLog(new TsavoriteLogSettings
            {
                LogDevice = device,
                PageSize = PageSize,
                MemorySize = PageSize * 8,
                SegmentSizeBits = 30,
                LogCommitDir = MethodTestDir
            });
            var allocator = log.Allocator;
            var page1 = allocator.GetPhysicalAddress(allocator.GetLogicalAddressOfStartOfPage(1));

            // Hold the tail as if a page turn were in progress, so that an allocation retries.
            var tail = allocator.TailPageOffset;
            var turning = tail;
            turning.Offset = (int)PageSize + 1;
            _ = Interlocked.Exchange(ref allocator.TailPageOffset.PageAndOffset, turning.PageAndOffset);

            var append = new Thread(() =>
            {
                allocator.epoch.Resume();
                try
                {
                    ClassicAssert.IsTrue(allocator.TryAllocateRetryNow(64, out _));
                }
                finally
                {
                    allocator.epoch.Suspend();
                }
            });
            append.Start();
            try
            {
                var sw = Stopwatch.StartNew();
                while (ResidentPages(page1, PageSize) < PageSize / OsPageSize)
                {
                    ClassicAssert.Less(sw.Elapsed, TimeSpan.FromSeconds(30), "The retrying allocation did not populate the page ahead of the tail");
                    Thread.Sleep(1);
                }
            }
            finally
            {
                _ = Interlocked.Exchange(ref allocator.TailPageOffset.PageAndOffset, tail.PageAndOffset);
                append.Join();
            }
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
            Span<int> key = [k];
            Span<int> value = stackalloc int[256];
            _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(key)), MemoryMarshal.Cast<int, byte>(value), Empty.Default);
        }
    }
}