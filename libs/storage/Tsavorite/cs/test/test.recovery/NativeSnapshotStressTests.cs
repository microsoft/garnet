// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>;

    /// <summary>
    /// Stress test targeting a code-review concern for the "full" native mode: a snapshot checkpoint writes
    /// directly from the direct-VM log-page pointers to the snapshot device, and (unlike a main-log flush) that
    /// IO does not gate main-log eviction. This hammers repeated snapshot checkpoints while background writers
    /// drive continuous flush+eviction through the same native pages, to surface any use-after-free (observed as
    /// an AccessViolationException) where an evicted page's block is unmapped while its snapshot write is still
    /// in flight. Explicit (slow) and process-global (flips the native surfaces), so NonParallelizable.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    internal class NativeSnapshotStressTests : TestBase
    {
        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            RecreateDirectory(MethodTestDir);
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.Full);
        }

        [TearDown]
        public void TearDown()
        {
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.None);
            // Drain finalizer-deferred native frees (NativePageBlockRegistry frees direct-VM blocks on the finalizer
            // thread) so a following fixture's NativeMemoryTracker baseline is not perturbed by this test's pending frees.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Explicit]
        [Category(TsavoriteKVTestCategory)]
        public void ConcurrentSnapshotAndEvictionStress()
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            using var store = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                PageSize = 1L << 12,        // 4 KB pages -> many native pages
                LogMemorySize = 1L << 17,   // 128 KB in-memory log -> constant flush + eviction
                MutableFraction = 0.1,      // tiny mutable region -> pages go read-only + flush quickly
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            // Attach a LogSizeTracker so that page eviction goes through ReturnPage -> FreeNativeLogPage (FreePage
            // only calls ReturnPage when a tracker is set). Combined with the tiny in-memory log this makes native
            // log pages be UNMAPPED on eviction concurrently with the snapshot writes below — the exact interleaving
            // under test.
            var tracker = new LogSizeTracker<LongStoreFunctions, LongAllocator>(store.Log, 1L << 16, 1L << 13, 1L << 12, logger: null);
            store.Log.SetLogSizeTracker(tracker);
            tracker.Start(CancellationToken.None);

            using var cts = new CancellationTokenSource();
            Exception writerError = null;
            var freesBefore = Interlocked.Read(ref AllocatorBase<LongStoreFunctions, LongAllocator>.NativeLogPageFreeCount);
            var deferredBefore = Interlocked.Read(ref AllocatorBase<LongStoreFunctions, LongAllocator>.NativeLogPageDeferredCount);
            var reusedBefore = Interlocked.Read(ref AllocatorBase<LongStoreFunctions, LongAllocator>.NativeLogPageReuseCount);

            // Background writers: continuous upserts drive the tail forward, forcing pages read-only -> flushed ->
            // evicted (ReturnPage -> FreeNativeLogPage) concurrently with the snapshot writes below.
            var writers = new Task[8];
            for (var w = 0; w < writers.Length; w++)
            {
                var seed = w;
                writers[w] = Task.Run(() =>
                {
                    try
                    {
                        using var s = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                        var bc = s.BasicContext;
                        var keyArray = new byte[sizeof(long)];
                        long key = seed;
                        while (!cts.IsCancellationRequested)
                        {
                            var keySpan = new Span<byte>(keyArray);
                            keySpan.AsRef<long>() = key;
                            _ = bc.Upsert(TestSpanByteKey.FromArray(keyArray), keySpan);
                            key += writers.Length;
                            if (key > 200_000)
                                key = seed;
                        }
                    }
                    catch (Exception ex)
                    {
                        writerError = ex;
                    }
                });
            }

            // Main: repeated snapshot checkpoints overlapping the eviction churn above.
            const int checkpoints = 200;
            for (var i = 0; i < checkpoints; i++)
            {
                var (success, _) = store.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot).AsTask().GetAwaiter().GetResult();
                ClassicAssert.IsTrue(success, $"snapshot checkpoint {i} failed");
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }

            cts.Cancel();
            Task.WaitAll(writers);
            ClassicAssert.IsNull(writerError, $"writer failed: {writerError}");

            // Prove the test actually exercised the native eviction-unmap path (FreeNativeLogPage) concurrently with
            // the snapshots — otherwise a "no crash" result would be vacuous (pages never unmapped).
            var freed = Interlocked.Read(ref AllocatorBase<LongStoreFunctions, LongAllocator>.NativeLogPageFreeCount) - freesBefore;
            var deferred = Interlocked.Read(ref AllocatorBase<LongStoreFunctions, LongAllocator>.NativeLogPageDeferredCount) - deferredBefore;
            var reused = Interlocked.Read(ref AllocatorBase<LongStoreFunctions, LongAllocator>.NativeLogPageReuseCount) - reusedBefore;
            ClassicAssert.Greater(freed, 0, "expected native log pages to be freed on eviction during the snapshots");
            // And prove the snapshot-deferral path (park-during-snapshot, free-on-completion) was actually hit — this
            // is the interleaving under test: pages evicted while a snapshot write was in flight.
            ClassicAssert.Greater(deferred, 0, "expected some evicted pages to be deferred during in-flight snapshots");
            // And prove the recycle pool is exercised: evicted blocks are pooled and handed back to later allocations
            // instead of munmap+mmap (the free-head/alloc-tail churn win).
            ClassicAssert.Greater(reused, 0, "expected some native log-page allocations to reuse a pooled block");
            TestContext.Out.WriteLine($"native log-page frees: {freed:N0}; deferred during snapshots: {deferred:N0}; reused from pool: {reused:N0}");
        }
    }
}