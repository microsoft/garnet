// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Tests for the mimalloc-backed <see cref="SectorAlignedBufferPool"/> (native-allocator "buffer-pool" mode).
    /// The pool's native hook is a process-global static (mirroring <c>Disabled</c>/<c>UnpinOnReturn</c>), so this
    /// fixture is <see cref="NonParallelizableAttribute"/> and resets the hook in teardown.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public unsafe class NativeAllocatorTests
    {
        const int SectorSize = 512;

        [TearDown]
        public void TearDown() => SectorAlignedBufferPool.NativeAllocator = null;

        static void RequireMimalloc()
        {
            if (!Mimalloc.TryInitialize())
                Assert.Ignore("mimalloc native library not available for this RID");
        }

        [Test]
        public void MimallocLoads()
        {
            RequireMimalloc();
            ClassicAssert.IsTrue(Mimalloc.Available);
        }

        [Test]
        public void NativePoolGetReturnRoundTrips()
        {
            RequireMimalloc();
            SectorAlignedBufferPool.NativeAllocator = new MimallocPooledAllocator();
            var pool = new SectorAlignedBufferPool(1, SectorSize);

            var page = pool.Get(1000);
            try
            {
                ClassicAssert.AreEqual(0, ((long)page.aligned_pointer) % SectorSize, "aligned_pointer must be sector-aligned");
                ClassicAssert.GreaterOrEqual(page.AlignedTotalCapacity, 1000);
                ClassicAssert.IsNull(page.buffer, "native-backed page must have no managed array");

                // Default clearOnReturn:true maps to mi_zalloc -> zeroed.
                for (var i = 0; i < page.AlignedTotalCapacity; i++)
                    ClassicAssert.AreEqual(0, page.aligned_pointer[i]);

                // Round-trip a pattern through the native buffer.
                for (var i = 0; i < 1000; i++)
                    page.aligned_pointer[i] = (byte)(i & 0xFF);
                for (var i = 0; i < 1000; i++)
                    ClassicAssert.AreEqual((byte)(i & 0xFF), page.aligned_pointer[i]);
            }
            finally
            {
                page.Return();
            }
        }

        [Test]
        public void NativeTrackerReflectsMimallocCommit()
        {
            RequireMimalloc();
            SectorAlignedBufferPool.NativeAllocator = new MimallocPooledAllocator();
            var pool = new SectorAlignedBufferPool(1, SectorSize);

            // Native usage is read on demand from mimalloc's committed stats (not per-op), so hold several
            // large buffers so mimalloc must have committed memory, then assert the tracker reflects it.
            var pages = new System.Collections.Generic.List<SectorAlignedMemory>();
            for (var i = 0; i < 64; i++)
                pages.Add(pool.Get(64 * 1024));
            try
            {
                ClassicAssert.Greater(NativeMemoryTracker.Bytes, 0, "tracker should reflect mimalloc committed bytes");
            }
            finally
            {
                foreach (var p in pages)
                    p.Return();
            }
        }

        [Test]
        public void NativeCrossThreadReturnIsSafe()
        {
            RequireMimalloc();
            SectorAlignedBufferPool.NativeAllocator = new MimallocPooledAllocator();
            var pool = new SectorAlignedBufferPool(1, SectorSize);

            var page = pool.Get(2048);
            page.aligned_pointer[0] = 0x42;

            // Rent on this thread, free on another: exercises mimalloc's cross-thread free path (the scenario
            // PR #2018 hand-rolled with origin-stripe return tracking). Must not throw or corrupt state.
            Exception captured = null;
            var t = new Thread(() =>
            {
                try { page.Return(); }
                catch (Exception e) { captured = e; }
            });
            t.Start();
            t.Join();

            ClassicAssert.IsNull(captured, "cross-thread Return must not throw");

            // Pool remains usable after a cross-thread free.
            var page2 = pool.Get(2048);
            try
            {
                ClassicAssert.AreEqual(0, ((long)page2.aligned_pointer) % SectorSize);
            }
            finally
            {
                page2.Return();
            }
        }

        [Test]
        public void NativeWrapperIsRecycled()
        {
            RequireMimalloc();
            SectorAlignedBufferPool.NativeAllocator = new MimallocPooledAllocator();
            var pool = new SectorAlignedBufferPool(1, SectorSize);

            // Get+Return then Get again on the same thread should recycle the wrapper object (no Gen0 churn),
            // returning a usable, correctly-aligned buffer.
            var p1 = pool.Get(1024);
            p1.Return();
            var p2 = pool.Get(1024);
            try
            {
                ClassicAssert.AreEqual(0, ((long)p2.aligned_pointer) % SectorSize);
                ClassicAssert.GreaterOrEqual(p2.AlignedTotalCapacity, 1024);
            }
            finally
            {
                p2.Return();
            }
        }

        [Test]
        public void NativeReadDestSkipsZeroing()
        {
            RequireMimalloc();
            SectorAlignedBufferPool.NativeAllocator = new MimallocPooledAllocator();
            var pool = new SectorAlignedBufferPool(1, SectorSize);

            // clearOnReturn:false (device-read destination) maps to mi_malloc (no forced zero). We cannot
            // reliably assert non-zero contents, but the buffer must be aligned, sized, and writable.
            var page = pool.Get(1000, clearOnReturn: false);
            try
            {
                ClassicAssert.AreEqual(0, ((long)page.aligned_pointer) % SectorSize);
                ClassicAssert.GreaterOrEqual(page.AlignedTotalCapacity, 1000);
                new Span<byte>(page.aligned_pointer, 1000).Fill(0xAB);
                ClassicAssert.AreEqual(0xAB, page.aligned_pointer[999]);
            }
            finally
            {
                page.Return();
            }
        }
    }
}
