// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.InteropServices;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.spanbyte
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>;

    /// <summary>
    /// End-to-end tests for the direct-VM (mmap/VirtualAlloc) hash index — the native-allocator "full" mode
    /// HashIndex surface. Flips the process-global <see cref="NativeAllocatorInitializer.EnabledSurfaces"/>, so
    /// this fixture is <see cref="NonParallelizableAttribute"/> and resets it in teardown.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    internal class NativeHashIndexTests : TestBase
    {
        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            // Drain any finalizer-deferred native frees from a prior test (the NativePageBlockRegistry frees direct-VM
            // blocks in its finalizer) before enabling this surface, so the NativeMemoryTracker baseline captured in
            // the tests below is not corrupted by another fixture's pending frees firing mid-test.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.HashIndex);
        }

        [TearDown]
        public void TearDown()
        {
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.None);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void NativeHashIndexInsertReadRoundTrips()
        {
            var before = NativeMemoryTracker.Bytes;
            var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            var store = new TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(
                new()
                {
                    IndexSize = 1L << 22,   // 4 MB index -> a meaningful direct-VM reservation (> one page)
                    LogDevice = log,
                    LogMemorySize = 1L << 20,
                    PageSize = 1L << 14
                }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            // The index table is now a direct-VM reservation, so tracked native bytes must have grown.
            ClassicAssert.GreaterOrEqual(NativeMemoryTracker.Bytes - before, 1L << 22,
                "hash index should be backed by direct virtual memory");

            var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            Span<int> keySpan = stackalloc int[1];
            Span<int> valueSpan = stackalloc int[4];

            const int n = 20_000;
            for (var i = 0; i < n; i++)
            {
                keySpan[0] = i;
                for (var j = 0; j < 4; j++)
                    valueSpan[j] = i;
                _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(keySpan)),
                    MemoryMarshal.Cast<int, byte>(valueSpan), Empty.Default);
            }

            for (var i = 0; i < n; i++)
            {
                keySpan[0] = i;
                int[] output = null;
                var status = bContext.Read(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(keySpan)), ref output, Empty.Default);
                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                }
                ClassicAssert.IsTrue(status.Found, $"key {i} not found");
                ClassicAssert.AreEqual(4, output.Length);
                ClassicAssert.AreEqual(i, output[0]);
            }

            session.Dispose();
            store.Dispose();
            log.Dispose();

            // Note: the live direct-VM index table is handed to a finalization-owned registry at Dispose (so an
            // in-flight index-checkpoint device write is never unmapped underneath it), matching the managed table
            // lifetime. Its free is therefore GC-timed, not prompt-on-Dispose, so we do not assert release here —
            // the round-trip correctness above and the "index is direct-VM backed" growth check are the invariants.
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void NativeFullModeLogPagesEvictAndRead()
        {
            // Enable both direct-VM singleton surfaces: hash index + log pages.
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.HashIndex | NativeAllocatorSurfaces.LogPages);

            var before = NativeMemoryTracker.Bytes;
            var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            var store = new TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(
                new()
                {
                    IndexSize = 1L << 22,
                    LogDevice = log,
                    LogMemorySize = 1L << 20,   // small 1 MB in-memory log -> forces eviction/flush to disk
                    PageSize = 1L << 13          // 8 KB pages -> many native log pages, exercising alloc/recycle/clear
                }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            // Both the index and (as pages are allocated) the log are direct-VM backed.
            ClassicAssert.Greater(NativeMemoryTracker.Bytes - before, 0, "index + log pages should be direct-VM backed");

            var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            Span<int> keySpan = stackalloc int[1];
            Span<int> valueSpan = stackalloc int[4];

            const int n = 20_000;   // ~2.5 MB of records >> 1 MB log -> heavy eviction/flush through native log pages
            for (var i = 0; i < n; i++)
            {
                keySpan[0] = i;
                for (var j = 0; j < 4; j++)
                    valueSpan[j] = i;
                _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(keySpan)),
                    MemoryMarshal.Cast<int, byte>(valueSpan), Empty.Default);
            }

            // Read back (mostly from disk — records were flushed out of the native log pages). Every key must be found.
            for (var i = 0; i < n; i++)
            {
                keySpan[0] = i;
                int[] output = null;
                var status = bContext.Read(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(keySpan)), ref output, Empty.Default);
                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                }
                ClassicAssert.IsTrue(status.Found, $"key {i} not found");
                ClassicAssert.AreEqual(4, output.Length);
                ClassicAssert.AreEqual(i, output[0]);
            }

            session.Dispose();
            store.Dispose();
            log.Dispose();

            // Note: direct-VM log-page/frame blocks are freed at finalization (to match the managed page lifetime
            // and avoid unmapping a page with an in-flight device IO), so we do not assert prompt free-on-Dispose
            // here — that timing is GC-dependent. Correctness (all records read back through native pages) and the
            // "index + log are direct-VM backed" check above are the meaningful invariants.
        }

        /// <summary>
        /// Stress: sustained heavy eviction churn through native log pages. Samples <see cref="NativeMemoryTracker.Bytes"/>
        /// over many write passes to confirm the direct-VM log-page footprint STABILIZES (bounded by the circular
        /// buffer + overflow pool) rather than growing unbounded as pages are recycled/dropped. Explicit (slow).
        /// </summary>
        [Test]
        [Explicit]
        [Category(TsavoriteKVTestCategory)]
        public void NativeFullModeLogPageMemoryStaysBounded()
        {
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.HashIndex | NativeAllocatorSurfaces.LogPages);

            var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            var store = new TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(
                new()
                {
                    IndexSize = 1L << 22,
                    LogDevice = log,
                    LogMemorySize = 1L << 20,   // 1 MB in-memory log -> constant eviction
                    PageSize = 1L << 13          // 8 KB pages
                }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            Span<int> keySpan = stackalloc int[1];
            Span<int> valueSpan = stackalloc int[4];

            long afterWarmup = 0;
            long peak = 0;
            const int passes = 40;
            const int perPass = 50_000;   // ~6 MB/pass through a 1 MB log -> heavy recycle + occasional pool drop
            for (var pass = 0; pass < passes; pass++)
            {
                for (var i = 0; i < perPass; i++)
                {
                    keySpan[0] = i;
                    for (var j = 0; j < 4; j++)
                        valueSpan[j] = pass;
                    _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(keySpan)),
                        MemoryMarshal.Cast<int, byte>(valueSpan), Empty.Default);
                }

                var now = NativeMemoryTracker.Bytes;
                if (pass == 4)
                    afterWarmup = now;   // baseline after the circular buffer + pool are warm
                if (pass >= 4 && now > peak)
                    peak = now;
                TestContext.Out.WriteLine($"pass {pass}: native bytes = {now:N0}");
            }

            // After warmup the log-page footprint must be bounded: the circular buffer is fixed-size and the overflow
            // pool is small, so the peak must not climb materially above the warm baseline (allow 2 MB slack for the
            // pool + index). A leak from dropped-but-not-freed pages would make this grow ~linearly with passes.
            ClassicAssert.Less(peak - afterWarmup, 2L << 20,
                $"native log-page memory grew unbounded: warm={afterWarmup:N0} peak={peak:N0}");

            session.Dispose();
            store.Dispose();
            log.Dispose();
        }

        /// <summary>
        /// Stress for GPT review finding #4: repeated <c>FlushAndEvict</c> frees many pages at once (more than the
        /// small overflow pool holds), so excess pages are dropped from the pool. This confirms those dropped
        /// direct-VM pages are freed (not retained until finalization), i.e. the native log-page footprint does not
        /// climb across many flush/regrow cycles. Explicit (slow).
        /// </summary>
        [Test]
        [Explicit]
        [Category(TsavoriteKVTestCategory)]
        public void NativeFullModeFlushEvictCyclesStayBounded()
        {
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.HashIndex | NativeAllocatorSurfaces.LogPages);

            var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            var store = new TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(
                new()
                {
                    IndexSize = 1L << 20,
                    LogDevice = log,
                    LogMemorySize = 1L << 21,   // 2 MB in-memory log -> ~256 pages fill it
                    PageSize = 1L << 13          // 8 KB pages
                }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            Span<int> keySpan = stackalloc int[1];
            Span<int> valueSpan = stackalloc int[4];

            long afterWarmup = 0;
            long peak = 0;
            const int cycles = 30;
            const int perCycle = 20_000;   // ~2.5 MB > log -> fills the in-memory log, then evict all of it at once
            for (var c = 0; c < cycles; c++)
            {
                for (var i = 0; i < perCycle; i++)
                {
                    keySpan[0] = i;
                    for (var j = 0; j < 4; j++)
                        valueSpan[j] = c;
                    _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(keySpan)),
                        MemoryMarshal.Cast<int, byte>(valueSpan), Empty.Default);
                }
                store.Log.FlushAndEvict(wait: true);   // bulk-close ~all pages -> overflow pool overflows -> drops

                var now = NativeMemoryTracker.Bytes;
                if (c == 2)
                    afterWarmup = now;
                if (c >= 2 && now > peak)
                    peak = now;
                TestContext.Out.WriteLine($"cycle {c}: native bytes = {now:N0}");
            }

            // A dropped-page leak would grow the footprint by ~(pages-per-flush) each cycle (many MB over 30 cycles).
            // Bounded reuse keeps it near the warm baseline (allow 3 MB slack for index + pool + transient pages).
            ClassicAssert.Less(peak - afterWarmup, 3L << 20,
                $"native log-page memory grew across FlushAndEvict cycles: warm={afterWarmup:N0} peak={peak:N0}");

            session.Dispose();
            store.Dispose();
            log.Dispose();
        }

        /// <summary>
        /// Regression for the native scan/recovery frame path: a long disk scan must REUSE its fixed set of
        /// direct-VM frame slots, not allocate a fresh mapping per page scanned. Scans records far exceeding the
        /// in-memory log (so most are read back from disk through frames) and asserts the peak native footprint
        /// stays bounded (frame slots + index). Frames free deterministically once the iterator drains its IO.
        /// Explicit (slow).
        /// </summary>
        [Test]
        [Explicit]
        [Category(TsavoriteKVTestCategory)]
        public void NativeFullModeScanFrameMemoryStaysBounded()
        {
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.HashIndex | NativeAllocatorSurfaces.LogPages | NativeAllocatorSurfaces.Frames);

            var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            var store = new TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(
                new()
                {
                    IndexSize = 1L << 20,
                    LogDevice = log,
                    LogMemorySize = 1L << 20,   // 1 MB in-memory log
                    PageSize = 1L << 13          // 8 KB pages
                }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            Span<int> keySpan = stackalloc int[1];
            Span<int> valueSpan = stackalloc int[4];

            const int n = 100_000;   // ~12 MB of records >> 1 MB log -> most pages flushed to disk
            for (var i = 0; i < n; i++)
            {
                keySpan[0] = i;
                for (var j = 0; j < 4; j++)
                    valueSpan[j] = i;
                _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(keySpan)),
                    MemoryMarshal.Cast<int, byte>(valueSpan), Empty.Default);
            }
            store.Log.FlushAndEvict(wait: true);

            var beforeScan = NativeMemoryTracker.Bytes;
            long peak = beforeScan;
            using (var iter = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress))
            {
                var count = 0;
                while (iter.GetNext())
                {
                    if ((++count & 0x3FF) == 0)
                    {
                        var now = NativeMemoryTracker.Bytes;
                        if (now > peak)
                            peak = now;
                    }
                }
                ClassicAssert.AreEqual(n, count, "scan must see every record");
            }

            // Frames reuse a fixed set of slots, so scanning ~100k records (12 MB from disk) must not grow native
            // memory by more than a few frame pages. A per-page re-allocation leak would make peak >> beforeScan.
            ClassicAssert.Less(peak - beforeScan, 4L << 20,
                $"native scan-frame memory grew unbounded: beforeScan={beforeScan:N0} peak={peak:N0}");

            session.Dispose();
            store.Dispose();
            log.Dispose();

            // Note: log pages and the index defer their free to finalization, so a post-dispose baseline check is
            // GC-timing dependent (and unreliable in Debug where locals stay rooted); the scan-peak bound above is
            // the meaningful regression guard for the frame-reuse fix. NativeFullModeLogPageMemoryStaysBounded
            // separately proves the log-page footprint is bounded.
        }
    }
}