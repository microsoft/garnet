// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Correctness tests for the default origin-return managed <see cref="SectorAlignedBufferPool"/>: cross-thread
    /// return routing, size-class ladder arithmetic, per-pool byte budget /
    /// poolability permits, retirement/seal + <see cref="SectorAlignedBufferPool.Free"/> teardown, dead-thread
    /// permit reclamation, and multi-pool isolation. These run in the normal (non-<c>[Explicit]</c>) suite.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public unsafe class SectorAlignedBufferPoolTests
    {
        const int SectorSize = 512;

        [SetUp]
        public void Setup()
        {
            // Ensure the origin-return default for these fixtures.
            SectorAlignedBufferPool.Disabled = false;
            SectorAlignedBufferPool.UnpinOnReturn = false;
            SectorAlignedBufferPool.UseOriginReturn = true;
            SectorAlignedBufferPool.ManagedBudgetBytes = 1L << 30;
        }

        [TearDown]
        public void TearDown()
        {
            SectorAlignedBufferPool.Disabled = false;
            SectorAlignedBufferPool.UnpinOnReturn = false;
            SectorAlignedBufferPool.UseOriginReturn = true;
            SectorAlignedBufferPool.ManagedBudgetBytes = 1L << 30;
        }

        // ---- Size-class ladder ---------------------------------------------------------------------------------

        [Test]
        public void BucketHeadsAreOnSeparateCacheLines()
        {
            // The owner-hot localHead and producer-hot crossThreadHead must sit on different 64-byte cache lines so
            // a foreign cross-thread push never invalidates the owner's local-stack line (false sharing). A
            // reference-type class ignores LayoutKind.Sequential (ref fields get grouped), so this is enforced with
            // explicit FieldOffsets; guard against a regression that drops the explicit layout.
            var offset = SectorAlignedBufferPool.TestBucketHeadCacheLineOffset();
            ClassicAssert.GreaterOrEqual((long)offset, 64,
                "Bucket.localHead and Bucket.crossThreadHead must be >= 64 bytes apart (separate cache lines)");
        }

        [Test]
        public void LadderIsMonotonicAndBounded()
        {
            var maxSectors = SectorAlignedBufferPool.TestMaxPooledSectors;
            var numClasses = SectorAlignedBufferPool.TestNumClasses;
            var linearTop = SectorAlignedBufferPool.TestLinearTopSectors;
            var stride = SectorAlignedBufferPool.TestLinearStrideSectors;

            var prevClass = -1;
            for (var s = 1; s <= maxSectors; s++)
            {
                var cls = SectorAlignedBufferPool.TestClassOfSectors(s);
                ClassicAssert.GreaterOrEqual(cls, 0, $"sectors={s} within cap must have a class");
                ClassicAssert.Less(cls, numClasses, $"sectors={s} class in range");
                ClassicAssert.GreaterOrEqual(cls, prevClass, $"class must be monotonic non-decreasing at sectors={s}");
                prevClass = cls;

                var cap = SectorAlignedBufferPool.TestClassCapacitySectors(cls);
                ClassicAssert.GreaterOrEqual(cap, s, $"class capacity {cap} must cover request {s} sectors");

                // Linear region: exact tiny classes (waste 0) then stride-rounded classes (waste < one stride);
                // geometric region uses 2 classes per doubling, bounding waste at 1.5x (one class per doubling
                // would be 2x).
                if (s <= linearTop)
                {
                    ClassicAssert.Less(cap - s, stride, "linear rounding waste must be < one stride");
                }
                else
                {
                    ClassicAssert.LessOrEqual(2 * cap, 3 * s, "geometric fragmentation must be bounded at 1.5x");
                }
            }

            // Just above the cap => bypass.
            ClassicAssert.AreEqual(-1, SectorAlignedBufferPool.TestClassOfSectors(maxSectors + 1), "over-cap request must bypass");

            // Exact tiny classes are preserved for the very common small sizes.
            ClassicAssert.AreEqual(1, SectorAlignedBufferPool.TestClassCapacitySectors(SectorAlignedBufferPool.TestClassOfSectors(1)), "512 B request must map to an exact 1-sector class");
            ClassicAssert.AreEqual(2, SectorAlignedBufferPool.TestClassCapacitySectors(SectorAlignedBufferPool.TestClassOfSectors(2)), "1 KB request must map to an exact 2-sector class");

            // A full record built on an ~8 MB inline value (header + key + value ~= 8.1 MB) must be pooled, not bypassed.
            var recordSectors = (8 * 1024 * 1024 + 128 * 1024) / SectorSize;   // ~8.1 MB in 512 B sectors
            ClassicAssert.GreaterOrEqual(SectorAlignedBufferPool.TestClassOfSectors(recordSectors), 0, "an ~8.1 MB record must be pooled, not bypassed");
        }

        [Test]
        public void GetCapacityCoversRequestAcrossSizes()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                foreach (var bytes in new[] { 1, 100, 511, 512, 513, 4096, 4097, 60000, 200000, 2_000_000 })
                {
                    var page = pool.Get(bytes, clearOnReturn: false);
                    try
                    {
                        ClassicAssert.AreEqual(0, ((long)page.aligned_pointer) % SectorSize, "aligned");
                        ClassicAssert.GreaterOrEqual(page.AlignedTotalCapacity, bytes, $"capacity covers {bytes}");
                        // Touch first + last usable byte.
                        page.aligned_pointer[0] = 1;
                        page.aligned_pointer[bytes - 1] = 1;
                    }
                    finally { page.Return(); }
                }
            }
            finally { pool.Free(); }
        }

        // ---- Get/Return basics ---------------------------------------------------------------------------------

        [Test]
        public void GetReturnsZeroedBufferAndReuses()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var p1 = pool.Get(4096);
                for (var i = 0; i < p1.AlignedTotalCapacity; i++)
                    ClassicAssert.AreEqual(0, p1.aligned_pointer[i], "default Get must be zeroed");
                for (var i = 0; i < 4096; i++)
                    p1.aligned_pointer[i] = 0xAB;
                p1.Return();

                // Same thread => local reuse => same object handed back, re-zeroed by default policy.
                var p2 = pool.Get(4096);
                ClassicAssert.AreSame(p1, p2, "same-thread Return then Get should reuse the local buffer");
                for (var i = 0; i < p2.AlignedTotalCapacity; i++)
                    ClassicAssert.AreEqual(0, p2.aligned_pointer[i], "reused buffer must be re-zeroed");
                p2.Return();
            }
            finally { pool.Free(); }
        }

        [Test]
        public void OptOutClearThenDefaultGetIsZeroed()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var p1 = pool.Get(4096, clearOnReturn: false);
                for (var i = 0; i < 4096; i++)
                    p1.aligned_pointer[i] = 0xCD;
                p1.Return();    // opted out => dirty, not cleared on Return

                var p2 = pool.Get(4096);    // default clearOnReturn:true => lazy-clear the dirty tail
                ClassicAssert.AreSame(p1, p2);
                for (var i = 0; i < p2.AlignedTotalCapacity; i++)
                    ClassicAssert.AreEqual(0, p2.aligned_pointer[i], "default Get must lazy-clear a dirty slot");
                p2.Return();
            }
            finally { pool.Free(); }
        }

        // ---- Cross-thread (origin) return routing --------------------------------------------------------------

        [Test]
        public void CrossThreadReturnRoutesBackToOriginAndReuses()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                // Get on this (origin) thread; Return on a different thread. The buffer must route back to the
                // origin thread's shard so a subsequent Get here reuses it (rather than allocating fresh).
                var p1 = pool.Get(4096, clearOnReturn: false);
                var ptr1 = (long)p1.aligned_pointer;

                var t = new Thread(() => p1.Return());
                t.Start();
                t.Join();

                var p2 = pool.Get(4096, clearOnReturn: false);
                ClassicAssert.AreSame(p1, p2, "cross-thread Return must route back to the origin thread and be reused");
                ClassicAssert.AreEqual(ptr1, (long)p2.aligned_pointer, "reused buffer retains its allocation");
                p2.Return();
            }
            finally { pool.Free(); }
        }

        [Test]
        public void LargeClassCrossThreadReturnSharesViaDepot()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                // Large (record/flush) classes are shared globally via the striped depot instead of parking on the
                // origin's cross-thread stack: a big buffer allocated by one thread and returned by a completion
                // thread must be reusable by a *third* (non-origin) thread, so it never strands on an origin that
                // may not re-request that class. (Small classes stay origin-return; see the test above.)
                const int largeSize = 1 << 20; // 1 MB: above the 256 KB large-tier threshold.

                SectorAlignedMemory p1 = null;
                long ptr1 = 0;
                var origin = new Thread(() => { p1 = pool.Get(largeSize, clearOnReturn: false); ptr1 = (long)p1.aligned_pointer; });
                origin.Start();
                origin.Join();

                // A different (completion) thread returns it cross-thread -> handed off to the shared depot.
                var completion = new Thread(() => p1.Return());
                completion.Start();
                completion.Join();

                // A third thread (neither origin nor the returning thread) must reuse it from the depot.
                SectorAlignedMemory p2 = null;
                var reuser = new Thread(() => { p2 = pool.Get(largeSize, clearOnReturn: false); });
                reuser.Start();
                reuser.Join();

                ClassicAssert.AreSame(p1, p2, "large-class cross-thread Return must be shared via the depot and reusable by a non-origin thread");
                ClassicAssert.AreEqual(ptr1, (long)p2.aligned_pointer, "reused large buffer retains its allocation");
                p2.Return();
            }
            finally { pool.Free(); }
        }

        [Test]
        public void LargeClassOwnerReturnSharesViaDepot()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                // Large classes are never parked on the owner-local stack either: a big buffer allocated AND
                // returned by the same thread still hands off to the shared depot, so a different thread can reuse
                // it. This is what keeps the large working set globally shared (legacy-like) under an owner-return
                // workload (e.g. Tsavorite's CompletePending disk-read path) instead of multiplied per thread.
                const int largeSize = 1 << 20; // 1 MB: above the 256 KB large-tier threshold.

                SectorAlignedMemory p1 = null;
                long ptr1 = 0;
                var origin = new Thread(() =>
                {
                    p1 = pool.Get(largeSize, clearOnReturn: false);
                    ptr1 = (long)p1.aligned_pointer;
                    p1.Return(); // owner (same-thread) return -> depot, not the local stack
                });
                origin.Start();
                origin.Join();

                SectorAlignedMemory p2 = null;
                var reuser = new Thread(() => { p2 = pool.Get(largeSize, clearOnReturn: false); });
                reuser.Start();
                reuser.Join();

                ClassicAssert.AreSame(p1, p2, "large-class owner Return must be shared via the depot and reusable by another thread");
                ClassicAssert.AreEqual(ptr1, (long)p2.aligned_pointer, "reused large buffer retains its allocation");
                p2.Return();
            }
            finally { pool.Free(); }
        }

        [Test]
        public void CompletionOnlyThreadCreatesNoShard()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var page = pool.Get(4096, clearOnReturn: false);

                int slotArrayLenOnConsumer = -1;
                var t = new Thread(() =>
                {
                    // This thread only ever Returns; it must not create a shard/slot-array for the pool.
                    page.Return();
                    slotArrayLenOnConsumer = SectorAlignedBufferPool.ThreadShardArrayLength;
                });
                t.Start();
                t.Join();

                ClassicAssert.AreEqual(0, slotArrayLenOnConsumer, "a completion-only thread must not allocate a shard slot array");
            }
            finally { pool.Free(); }
        }

        [Test]
        public void PopFromAnEmptyDepotStripeDoesNotWaitForItsLock()
        {
            // An empty stripe must answer a pop without taking its lock.
            var stripe = new DepotStripe(cap: 1);
            var stripeLock = typeof(DepotStripe).GetField("items", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(stripe);
            using var held = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            var holder = new Thread(() =>
            {
                lock (stripeLock)
                {
                    held.Set();
                    _ = release.Wait(TimeSpan.FromSeconds(10));
                }
            });
            holder.Start();
            try
            {
                ClassicAssert.IsTrue(held.Wait(TimeSpan.FromSeconds(10)));
                var timer = Stopwatch.StartNew();
                ClassicAssert.IsNull(stripe.TryPop());
                ClassicAssert.Less(timer.Elapsed, TimeSpan.FromSeconds(5), "a pop from an empty stripe waited for the stripe's lock");
            }
            finally
            {
                release.Set();
                holder.Join();
            }

            // The check must not hide a page that is present.
            var page = new SectorAlignedMemory();
            ClassicAssert.IsTrue(stripe.TryPush(page));
            ClassicAssert.AreSame(page, stripe.TryPop());
            ClassicAssert.IsNull(stripe.TryPop());
        }

        // ---- Multi-pool isolation ------------------------------------------------------------------------------

        [Test]
        public void MultiplePoolsDifferentSectorSizesNoCorruption()
        {
            var poolA = new SectorAlignedBufferPool(1, 512);
            var poolB = new SectorAlignedBufferPool(1, 4096);
            try
            {
                // Same nominal class index maps to different byte capacities per pool; interleave to ensure a
                // buffer from one pool is never handed out by the other.
                for (var iter = 0; iter < 1000; iter++)
                {
                    var a = poolA.Get(2000, clearOnReturn: false);
                    var b = poolB.Get(2000, clearOnReturn: false);
                    ClassicAssert.AreEqual(0, ((long)a.aligned_pointer) % 512);
                    ClassicAssert.AreEqual(0, ((long)b.aligned_pointer) % 4096);
                    ClassicAssert.GreaterOrEqual(a.AlignedTotalCapacity, 2000);
                    ClassicAssert.GreaterOrEqual(b.AlignedTotalCapacity, 2000);
                    a.aligned_pointer[1999] = 1;
                    b.aligned_pointer[1999] = 1;
                    a.Return();
                    b.Return();
                }
            }
            finally
            {
                poolA.Free();
                poolB.Free();
            }
        }

        // ---- Byte budget / permits -----------------------------------------------------------------------------

        [Test]
        public void BudgetBoundsReusableBytesAndReturnsToZero()
        {
            // Tiny budget: only a handful of 4 KB-class buffers may be cached; the rest are served non-cacheable
            // and dropped on Return. The reserved counter must never exceed the budget and must reach 0 at quiesce.
            SectorAlignedBufferPool.ManagedBudgetBytes = 64 * 1024;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var held = new List<SectorAlignedMemory>();
                for (var i = 0; i < 256; i++)
                    held.Add(pool.Get(4096, clearOnReturn: false));

                ClassicAssert.LessOrEqual(pool.ReservedBytes, SectorAlignedBufferPool.ManagedBudgetBytes,
                    "reserved bytes must never exceed the budget");

                foreach (var p in held)
                    p.Return();

                pool.Free();
                ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must return to zero after Free");
            }
            finally { pool.Free(); }
        }

        [Test]
        public void FreeAfterCrossThreadReturnsIsCleanAndBudgetZero()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            var barrier = new Barrier(9);
            var stop = false;
            var tasks = new List<Task>();

            // 4 producers Get, hand to a shared channel; 4 consumers Return cross-thread. Free() then races them.
            var channel = new ConcurrentQueue<SectorAlignedMemory>();
            for (var i = 0; i < 4; i++)
            {
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    barrier.SignalAndWait();
                    while (!Volatile.Read(ref stop))
                    {
                        var p = pool.Get(4096, clearOnReturn: false);
                        channel.Enqueue(p);
                    }
                }, TaskCreationOptions.LongRunning));
            }
            for (var i = 0; i < 4; i++)
            {
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    barrier.SignalAndWait();
                    while (!Volatile.Read(ref stop) || !channel.IsEmpty)
                    {
                        if (channel.TryDequeue(out var p))
                            p.Return();
                    }
                }, TaskCreationOptions.LongRunning));
            }

            barrier.SignalAndWait();
            Thread.Sleep(300);
            Volatile.Write(ref stop, true);
            Task.WaitAll(tasks.ToArray());

            // Drain any stragglers then Free and assert budget quiesces to zero.
            while (channel.TryDequeue(out var p))
                p.Return();
            pool.Free();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must quiesce to zero after concurrent Free");
        }

        [Test]
        public void SmallBudgetIsolatedFromLargeExhaustion()
        {
            // 8 MB total => small sub-budget 2 MB, large sub-budget 6 MB. A flood of large (2 MB) buffers may
            // exhaust the large sub-budget, but must NOT prevent the isolated small sub-budget from caching the
            // hot small buffers.
            SectorAlignedBufferPool.ManagedBudgetBytes = 8L << 20;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                // Ladder tiering: a 2 MB buffer is "large"; a 4 KB buffer is "small".
                ClassicAssert.GreaterOrEqual(SectorAlignedBufferPool.TestClassOfSectors((2 * 1024 * 1024) / SectorSize), pool.FirstLargeClass, "2 MB must be a large class");
                ClassicAssert.Less(SectorAlignedBufferPool.TestClassOfSectors(4096 / SectorSize), pool.FirstLargeClass, "4 KB must be a small class");

                // Flood the large sub-budget with live 2 MB buffers (each holds its permit while live).
                var large = new List<SectorAlignedMemory>();
                for (var i = 0; i < 32; i++)
                    large.Add(pool.Get(2 * 1024 * 1024, clearOnReturn: false));

                ClassicAssert.LessOrEqual(pool.LargeReservedBytes, 6L << 20, "large reservations must be bounded by the large sub-budget");
                ClassicAssert.AreEqual(0, pool.SmallReservedBytes, "no small buffers cached yet");

                // Despite the large sub-budget being exhausted, small buffers must still cache from their own slice.
                var small = new List<SectorAlignedMemory>();
                for (var i = 0; i < 16; i++)
                    small.Add(pool.Get(4096, clearOnReturn: false));
                foreach (var p in small)
                    p.Return();

                ClassicAssert.Greater(pool.SmallReservedBytes, 0, "small buffers must cache from the isolated small sub-budget even when large is exhausted");
                ClassicAssert.LessOrEqual(pool.SmallReservedBytes, 2L << 20, "small reservations must be bounded by the small sub-budget");

                foreach (var p in large)
                    p.Return();
            }
            finally { pool.Free(); }
            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must return to zero after Free");
        }

        // ---- Per-thread local retention ------------------------------------------------------------------------

        /// <summary>Rent <paramref name="count"/> buffers of <paramref name="size"/> simultaneously, then return them all.</summary>
        private static void RentBurstAndReturn(SectorAlignedBufferPool pool, int size, int count)
        {
            var held = new List<SectorAlignedMemory>(count);
            for (var i = 0; i < count; i++)
                held.Add(pool.Get(size, clearOnReturn: false));
            foreach (var p in held)
                p.Return();
        }

        /// <summary>
        /// Start <paramref name="count"/> threads that each register with <paramref name="pool"/> and then park until
        /// <paramref name="release"/> is set; returns once all have registered.
        /// </summary>
        private static Thread[] RegisterParkedThreads(SectorAlignedBufferPool pool, int count, ManualResetEventSlim release)
        {
            var threads = new Thread[count];
            var registered = new CountdownEvent(count);    // not disposed: a worker may still be inside Signal when Wait returns
            for (var t = 0; t < count; t++)
            {
                threads[t] = new Thread(() =>
                {
                    pool.Get(SectorSize, clearOnReturn: false).Return();
                    registered.Signal();
                    _ = release.Wait(TimeSpan.FromMinutes(1));
                }, maxStackSize: 256 << 10);
                threads[t].Start();
            }
            ClassicAssert.IsTrue(registered.Wait(TimeSpan.FromMinutes(1)), "worker threads did not register");
            return threads;
        }

        /// <summary>Release and join threads started by <see cref="RegisterParkedThreads"/>.</summary>
        private static void ReleaseParkedThreads(Thread[] threads, ManualResetEventSlim release)
        {
            release.Set();
            if (threads is null)
                return;
            foreach (var thread in threads)
                thread.Join();
        }

        [Test]
        public void LocalRetentionIsBoundedByThePerThreadByteCap()
        {
            // A burst far larger than the thread's slice must spill to the shared depot rather than park on the
            // thread: local retention is bounded in bytes, not by a per-class buffer count.
            SectorAlignedBufferPool.ManagedBudgetBytes = 64L << 20;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var cap = pool.ThreadLocalByteCap;
                ClassicAssert.Greater(cap, 0, "pool must publish a per-thread local byte cap");

                RentBurstAndReturn(pool, SectorSize, (int)(cap / (2 * SectorSize)) * 3);

                ClassicAssert.Greater(pool.CallerLocalBytes, 0, "the thread must retain buffers up to its slice");
                ClassicAssert.LessOrEqual(pool.CallerLocalBytes, cap, "a thread must not retain more than its slice");
            }
            finally { pool.Free(); }
            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must return to zero after Free");
        }

        [Test]
        public void LocalByteCapIsSharedFairlyAcrossClasses()
        {
            // The slice is shared across the classes a thread uses: one class alone may hold all of it, but a
            // second active class reclaims down to an equal share instead of being shut out.
            SectorAlignedBufferPool.ManagedBudgetBytes = 64L << 20;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var cap = pool.ThreadLocalByteCap;
                var clsA = SectorAlignedBufferPool.TestClassOfSectors(1);       // SectorSize
                var clsB = SectorAlignedBufferPool.TestClassOfSectors(8);       // 4 KB
                ClassicAssert.AreNotEqual(clsA, clsB, "the two probe sizes must land in different classes");

                // Only class A is active: it may take the whole slice.
                RentBurstAndReturn(pool, SectorSize, (int)(cap / (2 * SectorSize)) + 64);
                ClassicAssert.Greater(pool.CallerLocalBytesForClass(clsA), cap / 2,
                    "a thread using a single size class must be able to hold the whole slice");

                // Class B becomes active against a full slice: it must reclaim toward an equal share.
                RentBurstAndReturn(pool, 4096, (int)(cap / (2 * 9 * SectorSize)) + 64);

                ClassicAssert.LessOrEqual(pool.CallerLocalBytes, cap, "the shared slice must still bound the total");
                ClassicAssert.Greater(pool.CallerLocalBytesForClass(clsB), 0,
                    "a newly active class must obtain a share of the slice");
                ClassicAssert.LessOrEqual(pool.CallerLocalBytesForClass(clsA), cap * 3 / 4,
                    "the incumbent class must give bytes back toward its share");
            }
            finally { pool.Free(); }
            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must return to zero after Free");
        }

        [Test]
        public void MixedClassBurstStaysWithinOneThreadSlice()
        {
            // A thread cycling through many size classes must hold no more than its slice in total. A per-class
            // count cap would admit a full chain per class, so the thread's total would scale with the number of
            // classes it touches rather than being bounded.
            SectorAlignedBufferPool.ManagedBudgetBytes = 64L << 20;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var cap = pool.ThreadLocalByteCap;
                for (var round = 0; round < 4; round++)
                {
                    for (var sectors = 1; sectors <= 16; sectors++)
                        RentBurstAndReturn(pool, sectors * SectorSize, 64);
                }

                ClassicAssert.LessOrEqual(pool.CallerLocalBytes, cap,
                    "a thread's total local retention must be bounded across all size classes it uses");
                ClassicAssert.Greater(pool.CallerLocalBytes, 0, "the thread must still cache within its slice");
            }
            finally { pool.Free(); }
            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must return to zero after Free");
        }

        [Test]
        public void LocalRetentionStaysBoundedWhenMoreThreadsThanExpectedUseThePool()
        {
            // With more threads than expected, the slices of the live threads must sum to at most half the small
            // sub-budget, so a newly arriving thread can still cache.
            var expected = ConcurrencySharding.ExpectedConcurrentThreads;
            var smallBudget = (long)expected << 20;
            SectorAlignedBufferPool.ManagedBudgetBytes = 4 * smallBudget;   // the small sub-budget is a quarter of the total
            const int BufferSize = 8192;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            var threadCount = 4 * expected;
            var threads = new Thread[threadCount];
            using var allRegistered = new CountdownEvent(threadCount);
            using var allParked = new CountdownEvent(threadCount);
            using var release = new ManualResetEventSlim();
            var burstLock = new object();
            var parkedBytes = 0L;
            try
            {
                var expectedSlice = pool.ThreadLocalByteCap;
                for (var t = 0; t < threadCount; t++)
                {
                    threads[t] = new Thread(() =>
                    {
                        pool.Get(BufferSize, clearOnReturn: false).Return();
                        allRegistered.Signal();
                        _ = allRegistered.Wait(TimeSpan.FromMinutes(1));

                        // One burst at a time, each the size of a slice cut for the expected thread count.
                        lock (burstLock)
                        {
                            RentBurstAndReturn(pool, BufferSize, (int)(expectedSlice / BufferSize));
                            parkedBytes += pool.CallerLocalBytes;
                        }
                        allParked.Signal();
                        release.Wait();
                    }, maxStackSize: 256 << 10);
                    threads[t].Start();
                }
                ClassicAssert.IsTrue(allParked.Wait(TimeSpan.FromMinutes(1)), "worker threads did not park");

                ClassicAssert.LessOrEqual(parkedBytes, smallBudget / 2,
                    "the live threads together must not park more than half the small sub-budget");

                var newcomerLocalBytes = 0L;
                var newcomer = new Thread(() =>
                {
                    pool.Get(BufferSize, clearOnReturn: false).Return();
                    newcomerLocalBytes = pool.CallerLocalBytes;
                });
                newcomer.Start();
                newcomer.Join();
                ClassicAssert.Greater(newcomerLocalBytes, 0, "a thread arriving after the others parked must still cache");
            }
            finally
            {
                release.Set();
                foreach (var thread in threads)
                    thread?.Join();
                pool.Free();
            }
            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must return to zero after Free");
        }

        [Test]
        public void AThreadAboveALoweredCeilingSpillsDownOnItsNextReturn()
        {
            // A thread that parked a full slice before more threads arrived must return to the re-cut ceiling at its
            // next Return, and the buffers it spills must stay reusable.
            var expected = ConcurrencySharding.ExpectedConcurrentThreads;
            SectorAlignedBufferPool.ManagedBudgetBytes = 4 * ((long)expected << 20);   // the small sub-budget is a quarter of the total
            const int BufferSize = 8192;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            using var release = new ManualResetEventSlim();
            Thread[] others = null;
            try
            {
                var slice = pool.ThreadLocalByteCap;
                RentBurstAndReturn(pool, BufferSize, (int)(slice / BufferSize));
                var parked = pool.CallerLocalBytes;

                others = RegisterParkedThreads(pool, expected, release);
                var ceiling = pool.ThreadLocalByteCap;
                ClassicAssert.Greater(parked, ceiling, "the thread must hold more than the re-cut ceiling before its next Return");

                pool.Get(BufferSize, clearOnReturn: false).Return();
                ClassicAssert.LessOrEqual(pool.CallerLocalBytes, ceiling, "the next Return must spill down to the ceiling");

                var reservedBefore = pool.SmallReservedBytes;
                var newcomer = new Thread(() => pool.Get(BufferSize, clearOnReturn: false).Return());
                newcomer.Start();
                newcomer.Join();
                ClassicAssert.AreEqual(reservedBefore, pool.SmallReservedBytes, "another thread must reuse a spilled buffer rather than allocate");
            }
            finally
            {
                pool.Free();
                ReleaseParkedThreads(others, release);
            }
            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must return to zero after Free");
        }

        [Test]
        public void TheCeilingRisesAgainOnceTheExtraThreadsExit()
        {
            // The ceiling is cut for the threads using the pool, so it must rise again once the threads that lowered it
            // have exited and their shards are finalized.
            SectorAlignedBufferPool.ManagedBudgetBytes = 64L << 20;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            using var release = new ManualResetEventSlim();
            Thread[] others = null;
            try
            {
                var initial = pool.ThreadLocalByteCap;
                var page = pool.Get(SectorSize, clearOnReturn: false);
                var shard = page.originBucket.owner;
                others = RegisterParkedThreads(pool, ConcurrencySharding.ExpectedConcurrentThreads, release);
                ClassicAssert.Less(shard.localByteCap, initial, "more threads than expected must lower the ceiling");

                ReleaseParkedThreads(others, release);
                others = null;
                for (var attempt = 0; attempt < 40 && shard.localByteCap < initial; attempt++)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
                ClassicAssert.AreEqual(initial, shard.localByteCap, "the ceiling must rise again once the extra threads exit");
                page.Return();
            }
            finally
            {
                pool.Free();
                ReleaseParkedThreads(others, release);
            }
            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must return to zero after Free");
        }

        [Test]
        public void FinalizingAShardThePoolNeverCountedLeavesTheCeilingUnchanged()
        {
            // A shard is constructed before the pool counts it, so one whose construction or registration throws is
            // finalized uncounted; that finalization must not uncount a live thread.
            SectorAlignedBufferPool.ManagedBudgetBytes = 64L << 20;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            using var release = new ManualResetEventSlim();
            Thread[] others = null;
            try
            {
                var page = pool.Get(SectorSize, clearOnReturn: false);
                others = RegisterParkedThreads(pool, ConcurrencySharding.ExpectedConcurrentThreads, release);
                var lowered = pool.ThreadLocalByteCap;

                var uncounted = AbandonUncountedShard(pool);
                for (var attempt = 0; attempt < 40 && uncounted.IsAlive; attempt++)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
                ClassicAssert.IsFalse(uncounted.IsAlive, "the uncounted shard must have been finalized");
                ClassicAssert.AreEqual(lowered, pool.ThreadLocalByteCap, "an uncounted shard's finalizer must not raise the ceiling");
                page.Return();
            }
            finally
            {
                pool.Free();
                ReleaseParkedThreads(others, release);
            }
            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must return to zero after Free");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static WeakReference AbandonUncountedShard(SectorAlignedBufferPool pool)
        {
            var numClasses = SectorAlignedBufferPool.TestNumClasses;
            var shard = new ThreadShard(pool, numClasses, SectorSize, new int[numClasses], pool.ThreadLocalByteCap, bornSealed: false);
            return new WeakReference(shard, trackResurrection: true);
        }

        // ---- Dead-thread permit reclamation --------------------------------------------------------------------

        [Test]
        public void DeadThreadFinalizerReclaimsPermits()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                // A worker thread Gets and same-thread Returns (caching buffers in its local list), then exits
                // WITHOUT the pool being freed. Its shard becomes unreachable; the finalizer must release the
                // held permits back to the budget.
                for (var t = 0; t < 4; t++)
                {
                    var th = new Thread(() =>
                    {
                        for (var i = 0; i < 32; i++)
                        {
                            var p = pool.Get(4096, clearOnReturn: false);
                            p.Return();     // same-thread => cached locally, holds a permit
                        }
                    });
                    th.Start();
                    th.Join();
                }

                ClassicAssert.Greater(pool.ReservedBytes, 0, "cached buffers on live-but-dead-thread shards hold permits");

                for (var attempt = 0; attempt < 10 && pool.ReservedBytes > 0; attempt++)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    Thread.Sleep(50);
                }
                ClassicAssert.AreEqual(0, pool.ReservedBytes, "dead-thread shard finalizers must reclaim all permits");
            }
            finally { pool.Free(); }
        }

        // ---- Double-return guard (CHECK_FREE / DEBUG) ----------------------------------------------------------

        [Test]
        public void DoubleReturnIsCaughtInDebug()
        {
#if !DEBUG
            Assert.Ignore("double-return guard (CHECK_FREE) only compiled in DEBUG");
#endif
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var p = pool.Get(4096, clearOnReturn: false);
                p.Return();
                ClassicAssert.Throws<TsavoriteException>(() => p.Return(), "a double Return must be caught under CHECK_FREE");
            }
            finally { pool.Free(); }
        }

        // ---- Per-level ConcurrentQueue backend (UseOriginReturn = false) ----------------------------------------

        [Test]
        public void LegacyPathStillWorksWhenOriginReturnDisabled()
        {
            SectorAlignedBufferPool.UseOriginReturn = false;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var p1 = pool.Get(4096);
                for (var i = 0; i < p1.AlignedTotalCapacity; i++)
                    ClassicAssert.AreEqual(0, p1.aligned_pointer[i]);
                p1.Return();
                var p2 = pool.Get(4096);
                ClassicAssert.AreEqual(0, ((long)p2.aligned_pointer) % SectorSize);
                p2.Return();
            }
            finally { pool.Free(); }
        }

        [Test]
        public void FreeIsIdempotent()
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            var p = pool.Get(4096, clearOnReturn: false);
            p.Return();
            pool.Free();
            Assert.DoesNotThrow(() => pool.Free(), "Free must be idempotent");
        }

        // ---- Alternate global modes (UnpinOnReturn / Disabled) -------------------------------------------------

        [Test]
        public void UnpinOnReturnModeRepinsAndReusesAcrossThreads()
        {
            // UnpinOnReturn is a documented kill-switch that frees the pin handle on every Return and re-pins on
            // the next Get, so aligned_pointer must be recomputed per rental (the array may have moved while
            // unpinned). Exercise the whole cycle, including a cross-thread Return, under a forced GC.
            SectorAlignedBufferPool.UnpinOnReturn = true;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var p1 = pool.Get(4096);
                ClassicAssert.AreEqual(0, ((long)p1.aligned_pointer) % SectorSize, "aligned while pinned");
                for (var i = 0; i < 4096; i++)
                    p1.aligned_pointer[i] = 0xA5;
                p1.Return();

                // Compact while the buffer is unpinned so a stale pointer would be exposed on re-rent.
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                var p2 = pool.Get(4096);
                ClassicAssert.AreSame(p1, p2, "same-thread Return then Get should reuse the local buffer");
                ClassicAssert.AreEqual(0, ((long)p2.aligned_pointer) % SectorSize, "re-pinned pointer must be re-aligned");
                for (var i = 0; i < p2.AlignedTotalCapacity; i++)
                    ClassicAssert.AreEqual(0, p2.aligned_pointer[i], "reused buffer must be re-zeroed");
                p2.aligned_pointer[4095] = 0x5A;

                // Cross-thread Return must also unpin correctly and route back to this origin thread.
                var t = new Thread(() => p2.Return());
                t.Start();
                t.Join();

                var p3 = pool.Get(4096, clearOnReturn: false);
                ClassicAssert.AreSame(p1, p3, "cross-thread Return must route back to origin under UnpinOnReturn");
                ClassicAssert.AreEqual(0, ((long)p3.aligned_pointer) % SectorSize);
                p3.aligned_pointer[0] = 1;
                p3.Return();
            }
            finally { pool.Free(); }
        }

        [Test]
        public void DisabledPoolServesUncachedBuffersAndHoldsNoBudget()
        {
            // Disabled routes every request to the bypass path: a fresh, correctly-aligned buffer that is dropped
            // (never cached) on Return, so no permit is ever reserved.
            SectorAlignedBufferPool.Disabled = true;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var first = pool.Get(4096);
                ClassicAssert.AreEqual(0, ((long)first.aligned_pointer) % SectorSize, "aligned");
                ClassicAssert.GreaterOrEqual(first.AlignedTotalCapacity, 4096);
                first.aligned_pointer[4095] = 0xEE;
                first.Return();

                var second = pool.Get(4096);
                ClassicAssert.AreNotSame(first, second, "a disabled pool must never recycle a buffer");
                second.Return();

                ClassicAssert.AreEqual(0, pool.ReservedBytes, "a disabled pool must never reserve budget");
            }
            finally { pool.Free(); }
        }

        [Test]
        public void OversizeRequestBypassesCacheAndHoldsNoBudget()
        {
            // A request beyond the pooled ceiling has no size class: it must still be served (allocate-on-Get,
            // free-on-Return) without reserving budget or being recycled.
            var overCapBytes = (SectorAlignedBufferPool.TestMaxPooledSectors + 1) * SectorSize;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var first = pool.Get(overCapBytes, clearOnReturn: false);
                ClassicAssert.AreEqual(0, ((long)first.aligned_pointer) % SectorSize, "aligned");
                ClassicAssert.GreaterOrEqual(first.AlignedTotalCapacity, overCapBytes, "capacity covers an over-cap request");
                first.aligned_pointer[overCapBytes - 1] = 0xEE;   // touch the last usable byte
                first.Return();

                ClassicAssert.AreEqual(0, pool.ReservedBytes, "an over-cap buffer must not consume the byte budget");

                var second = pool.Get(overCapBytes, clearOnReturn: false);
                ClassicAssert.AreNotSame(first, second, "over-cap buffers must not be recycled");
                second.Return();
            }
            finally { pool.Free(); }
        }

        // ---- Teardown corner cases -----------------------------------------------------------------------------

        [Test]
        public void ReturnOfInFlightBufferAfterFreeIsSafe()
        {
            // A pool can be Freed while IO is still in flight; those buffers Return to a closed pool afterwards
            // (from the origin thread and from a foreign thread). Both must be dropped without throwing, and the
            // budget must end at zero.
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            var small = pool.Get(4096, clearOnReturn: false);            // small class => per-thread path
            var large = pool.Get(1 << 20, clearOnReturn: false);         // large class => depot path
            var foreign = pool.Get(4096, clearOnReturn: false);

            pool.Free();

            Assert.DoesNotThrow(() => small.Return(), "origin-thread Return after Free must be safe");
            Assert.DoesNotThrow(() => large.Return(), "large-class Return after Free must be safe");

            Exception foreignError = null;
            var t = new Thread(() =>
            {
                try { foreign.Return(); }
                catch (Exception e) { foreignError = e; }
            });
            t.Start();
            t.Join();
            ClassicAssert.IsNull(foreignError, "foreign Return after Free must be safe");

            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must be zero once in-flight buffers drain after Free");
        }

        [Test]
        public void FreeReleasesBuffersCachedOnOtherThreads()
        {
            // Regression: Free() must not leave buffers cached on a *different* (still-live) thread rooted through
            // that thread's shard slot. Their permits are released, so the byte budget would report zero while the
            // backing arrays stayed resident for the rest of that thread's life.
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            var cached = new List<WeakReference>();
            var populated = new ManualResetEventSlim(false);
            var release = new ManualResetEventSlim(false);

            // A long-lived worker parks buffers on its own shard, then stays alive (as a thread-pool thread would).
            var worker = new Thread(() =>
            {
                PopulateLocalCache(pool, cached);
                populated.Set();
                release.Wait();
            });
            worker.Start();
            populated.Wait();

            // Free from a *different* thread, so the worker's shard takes the non-owner teardown path.
            pool.Free();

            for (var attempt = 0; attempt < 10; attempt++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                if (cached.TrueForAll(w => !w.IsAlive))
                    break;
                Thread.Sleep(20);
            }

            var stillAlive = cached.FindAll(w => w.IsAlive).Count;
            release.Set();
            worker.Join();

            ClassicAssert.AreEqual(0, stillAlive,
                "Free must detach buffers cached on other live threads so they are collectable");
            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must be zero after Free");
        }

        // Kept out of the worker lambda so no stack slot in the still-running thread roots the buffers.
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void PopulateLocalCache(SectorAlignedBufferPool pool, List<WeakReference> cached)
        {
            for (var i = 0; i < 8; i++)
            {
                var p = pool.Get(4096, clearOnReturn: false);
                cached.Add(new WeakReference(p.buffer));
                p.Return();     // parks on this thread's owner-local stack
            }
        }

        [Test]
        public void RecycledSlotIsIsolatedFromStalePoolShard()
        {
            // A freed pool releases its thread-static slot index for reuse. The calling thread still holds the old
            // pool's (now sealed) shard in that slot, so a new pool that recycles the index must reject it via the
            // pool-identity check. If it did not, this thread would be handed the old pool's buffers, sized by the
            // old pool's sector size -> silent undersize / misalignment.
            for (var round = 0; round < 25; round++)
            {
                var oldPool = new SectorAlignedBufferPool(1, 512);
                // Populate this thread's shard for oldPool so a stale entry is left in the slot.
                for (var i = 0; i < 8; i++)
                    oldPool.Get(2000, clearOnReturn: false).Return();
                oldPool.Free();

                // Very likely recycles the slot oldPool just released.
                var newPool = new SectorAlignedBufferPool(1, 4096);
                try
                {
                    for (var i = 0; i < 8; i++)
                    {
                        var p = newPool.Get(2000, clearOnReturn: false);
                        ClassicAssert.AreEqual(0, ((long)p.aligned_pointer) % 4096,
                            "a recycled slot must not serve the previous pool's (512-byte aligned) buffers");
                        ClassicAssert.GreaterOrEqual(p.AlignedTotalCapacity, 2000);
                        p.aligned_pointer[1999] = 1;
                        p.Return();
                    }
                }
                finally { newPool.Free(); }

                ClassicAssert.AreEqual(0, oldPool.ReservedBytes, "freed pool must hold no budget");
            }
        }

        [Test]
        public void CrossThreadDirtyReturnIsLazyClearedOnOwnerReuse()
        {
            // The owner's lazy-clear happens after claiming the cross-thread chain, which is a different code path
            // from the owner-local stack. A buffer dirtied and returned by a foreign thread must still come back
            // zeroed when the origin thread re-Gets it with the default clearOnReturn:true.
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var p1 = pool.Get(4096, clearOnReturn: false);
                for (var i = 0; i < p1.AlignedTotalCapacity; i++)
                    p1.aligned_pointer[i] = 0xAB;

                var t = new Thread(() => p1.Return());   // foreign Return: marks dirty, no clear
                t.Start();
                t.Join();

                var p2 = pool.Get(4096);                 // default clearOnReturn:true
                ClassicAssert.AreSame(p1, p2, "cross-thread Return must route back to the origin thread");
                for (var i = 0; i < p2.AlignedTotalCapacity; i++)
                    ClassicAssert.AreEqual(0, p2.aligned_pointer[i], "cross-thread claim must lazy-clear a dirty buffer");
                p2.Return();

                // Reverse polarity: cleared on a foreign Return, then reused opting out of clearing.
                var p3 = pool.Get(4096, clearOnReturn: true);
                for (var i = 0; i < p3.AlignedTotalCapacity; i++)
                    p3.aligned_pointer[i] = 0xCD;
                t = new Thread(() => p3.Return());       // foreign Return: clears eagerly
                t.Start();
                t.Join();

                var p4 = pool.Get(4096, clearOnReturn: false);
                ClassicAssert.AreSame(p3, p4);
                for (var i = 0; i < p4.AlignedTotalCapacity; i++)
                    ClassicAssert.AreEqual(0, p4.aligned_pointer[i], "an eagerly cleared buffer must stay zeroed");
                p4.Return();
            }
            finally { pool.Free(); }
        }

        [Test]
        public void ConcurrentFreeDuringActiveTrafficIsSafe()
        {
            // Free() racing live Get/Return traffic (a store disposed while IO is in flight) must never throw,
            // corrupt, or leave budget reserved.
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            var errors = new ConcurrentQueue<Exception>();
            var stop = false;
            var started = new Barrier(5);
            var tasks = new List<Task>();

            for (var i = 0; i < 4; i++)
            {
                var seed = i;
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    var rnd = new Random(seed);
                    try
                    {
                        started.SignalAndWait();
                        while (!Volatile.Read(ref stop))
                        {
                            var page = pool.Get(rnd.Next(2) == 0 ? 4096 : 1 << 20, clearOnReturn: false);
                            page.aligned_pointer[0] = 0xEE;
                            page.Return();
                        }
                    }
                    catch (Exception e) { errors.Enqueue(e); }
                }, TaskCreationOptions.LongRunning));
            }

            started.SignalAndWait();
            Thread.Sleep(150);
            pool.Free();            // races the four workers still calling Get/Return
            Thread.Sleep(150);
            Volatile.Write(ref stop, true);
            Task.WaitAll(tasks.ToArray());

            ClassicAssert.IsEmpty(errors, "Free racing live traffic must not throw: " + string.Join("; ", errors.Select(e => e.Message)));

            // A shard owned by another thread cannot have its local chain dropped by Free (that would race the
            // owner's non-atomic pop/push), so Free detaches it and releases its permits instead. A racing owner can
            // resurrect part of that chain after the detach, leaving those buffers' permits reserved. Free therefore
            // hands the repair back to ~ThreadShard by releasing drainedOnce: once the stale shards become
            // unreachable (the owner's next Get replaces the pool-less shard, or the thread exits) finalization
            // releases the resurrected buffers' permits and the budget returns to exactly zero.
            for (var attempt = 0; attempt < 40 && pool.ReservedBytes > 0; attempt++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                Thread.Sleep(25);
            }
            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must quiesce to zero after a raced Free");
        }

        [Test]
        public void AReturnAtTheCeilingThatRacesFreeDoesNotThrow([Values] bool ceilingLowered)
        {
            // Free zeroes a shard's counters without synchronizing with its owner, so an owner Return already past the
            // liveness check can see bytes retained on a chain while the shard counts no class as active.
            SectorAlignedBufferPool.ManagedBudgetBytes = 64L << 20;
            const int BufferSize = 8192;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            using var release = new ManualResetEventSlim();
            Thread[] others = null;
            try
            {
                var page = pool.Get(BufferSize, clearOnReturn: false);
                RentBurstAndReturn(pool, BufferSize, (int)(pool.ThreadLocalByteCap / BufferSize) + 1);
                if (ceilingLowered)
                    others = RegisterParkedThreads(pool, ConcurrencySharding.ExpectedConcurrentThreads, release);
                var shard = page.originBucket.owner;
                ClassicAssert.AreEqual(ceilingLowered, shard.localBytes > pool.ThreadLocalByteCap);
                shard.activeClasses = 0;

                Assert.DoesNotThrow(page.Return);
            }
            finally
            {
                pool.Free();
                ReleaseParkedThreads(others, release);
            }
            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must return to zero after Free");
        }

        [Test]
        public void AReturnAtTheCeilingThatRacesFreeDetachingAChainDoesNotThrow([Values] bool ceilingLowered)
        {
            // Free detaches another thread's owner-local chains before zeroing their counters, so an owner Return
            // already past the liveness check can find a class counted as over its share whose chain is now empty.
            SectorAlignedBufferPool.ManagedBudgetBytes = 64L << 20;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            using var release = new ManualResetEventSlim();
            Thread[] others = null;
            try
            {
                var page = pool.Get(8192, clearOnReturn: false);
                RentBurstAndReturn(pool, 4096, (int)(pool.ThreadLocalByteCap / 4096) + 1);
                if (ceilingLowered)
                    others = RegisterParkedThreads(pool, ConcurrencySharding.ExpectedConcurrentThreads, release);
                var shard = page.originBucket.owner;
                ClassicAssert.AreEqual(ceilingLowered, shard.localBytes > pool.ThreadLocalByteCap);
                var crowded = shard.buckets.Single(b => b.localHead is not null);
                SectorAlignedBufferPool.ReleaseChainPermits(Interlocked.Exchange(ref crowded.localHead, null));

                Assert.DoesNotThrow(page.Return);
            }
            finally
            {
                pool.Free();
                ReleaseParkedThreads(others, release);
            }
            ClassicAssert.AreEqual(0, pool.ReservedBytes, "budget must return to zero after Free");
        }

        [Test]
        public void ManyShortLivedPoolsDoNotAccumulatePerThreadState()
        {
            // A long-lived thread (thread pool, session, or a whole test-suite run) touches many short-lived pools
            // over its life. Slots must be recycled through the free list so the thread-static shard array is bounded
            // by the number of CONCURRENTLY live pools, not the number ever created; otherwise every pool that ever
            // ran would leave the thread permanently wider. Budget permits must likewise fully unwind per pool.
            var baseline = SectorAlignedBufferPool.ThreadShardArrayLength;

            for (var iter = 0; iter < 200; iter++)
            {
                var pool = new SectorAlignedBufferPool(1, SectorSize);
                for (var i = 0; i < 8; i++)
                {
                    var page = pool.Get(4096, clearOnReturn: false);
                    page.aligned_pointer[0] = 0x5A;
                    page.Return();
                }
                ClassicAssert.Greater(pool.ReservedBytes, 0, "cached buffers must hold permits while the pool is live");
                pool.Free();
                ClassicAssert.AreEqual(0, pool.ReservedBytes, $"iteration {iter}: budget must unwind on Free");
            }

            var grown = SectorAlignedBufferPool.ThreadShardArrayLength - baseline;
            ClassicAssert.LessOrEqual(grown, 4,
                $"thread-static shard array grew by {grown} across 200 sequential pool lifetimes; slots are not being recycled");
        }
    }
}