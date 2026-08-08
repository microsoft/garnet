// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Root-cause profiling for the managed-vs-native <see cref="SectorAlignedBufferPool"/> divergence.
    /// Isolates: (a) shared-ConcurrentQueue contention on the managed path (shared vs per-thread pools) and
    /// (b) the global <see cref="NativeMemoryTracker"/> Interlocked-counter contention on the native path
    /// (tracked vs untracked mimalloc). [Explicit] — run in Release.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    [Explicit]
    public unsafe class NativeAllocatorProfileTests
    {
        const int SectorSize = 512;
        const int BufferSize = 4096;
        const long OpsPerThread = 1_500_000;

        /// <summary>mimalloc allocator with NO tracking (no mi_usable_size, no Interlocked) — isolates tracker cost.</summary>
        sealed class UntrackedMimallocAllocator : INativePinnedAllocator
        {
            public nint Allocate(nuint size, nuint alignment, bool zeroed)
                => zeroed ? Mimalloc.ZallocAligned(size, alignment) : Mimalloc.MallocAligned(size, alignment);
            public void Free(nint ptr, nuint size) => Mimalloc.Free(ptr);
        }

        [TearDown]
        public void TearDown() => SectorAlignedBufferPool.NativeAllocator = null;

        static void Loop(SectorAlignedBufferPool pool)
        {
            for (long i = 0; i < OpsPerThread; i++)
            {
                var page = pool.Get(BufferSize, clearOnReturn: false);
                page.aligned_pointer[0] = 1;
                page.aligned_pointer[BufferSize - 1] = 1;
                page.Return();
            }
        }

        static double RunShared(int threads)
        {
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            return Run(threads, _ => pool);
        }

        static double RunPerThread(int threads)
        {
            var pools = new SectorAlignedBufferPool[threads];
            for (var i = 0; i < threads; i++)
                pools[i] = new SectorAlignedBufferPool(1, SectorSize);
            return Run(threads, tid => pools[tid]);
        }

        static double Run(int threads, System.Func<int, SectorAlignedBufferPool> poolForThread)
        {
            var barrier = new Barrier(threads + 1);
            var tasks = new Task[threads];
            for (var t = 0; t < threads; t++)
            {
                var tid = t;
                tasks[t] = Task.Factory.StartNew(() =>
                {
                    var pool = poolForThread(tid);
                    barrier.SignalAndWait();
                    Loop(pool);
                }, TaskCreationOptions.LongRunning);
            }
            barrier.SignalAndWait();
            var sw = Stopwatch.StartNew();
            Task.WaitAll(tasks);
            sw.Stop();
            return threads * OpsPerThread / sw.Elapsed.TotalSeconds / 1e6;
        }

        [Test]
        public void RootCause()
        {
            if (!Mimalloc.TryInitialize())
                Assert.Ignore("mimalloc native library not available for this RID");

            var threadCounts = new[] { 1, 8, 32 };

            void Report(string name, System.Func<int, double> run)
            {
                var sb = new System.Text.StringBuilder($"{name,-28}");
                foreach (var th in threadCounts)
                {
                    _ = run(th);            // warm up
                    var mops = run(th);
                    sb.Append($" | {mops,8:F2}");
                }
                TestContext.Progress.WriteLine(sb.ToString());
            }

            TestContext.Progress.WriteLine($"{"scenario (Mops/s aggregate)",-28} | {"1 thr",8} | {"8 thr",8} | {"32 thr",8}");
            TestContext.Progress.WriteLine(new string('-', 62));

            Report("managed, SHARED pool", th => { SectorAlignedBufferPool.NativeAllocator = null; return RunShared(th); });
            Report("managed, PER-THREAD pools", th => { SectorAlignedBufferPool.NativeAllocator = null; return RunPerThread(th); });
            Report("mimalloc, tracked (current)", th => { SectorAlignedBufferPool.NativeAllocator = new MimallocPooledAllocator(); return RunShared(th); });
            Report("mimalloc, UNTRACKED", th => { SectorAlignedBufferPool.NativeAllocator = new UntrackedMimallocAllocator(); return RunShared(th); });
            SectorAlignedBufferPool.NativeAllocator = null;
        }
    }
}
