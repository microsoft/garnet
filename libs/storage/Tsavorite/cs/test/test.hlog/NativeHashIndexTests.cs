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
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.HashIndex);
        }

        [TearDown]
        public void TearDown()
        {
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.None);
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

            // After Dispose, the direct-VM index must be released (tracked native bytes back to baseline).
            ClassicAssert.Less(NativeMemoryTracker.Bytes - before, 1L << 22, "index native memory should be freed on Dispose");
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
    }
}
