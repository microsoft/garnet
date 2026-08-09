// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.InteropServices;
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
            Report("mimalloc (prod, on-demand stats)", th => { SectorAlignedBufferPool.NativeAllocator = new MimallocPooledAllocator(); return RunShared(th); });
            Report("mimalloc, UNTRACKED", th => { SectorAlignedBufferPool.NativeAllocator = new UntrackedMimallocAllocator(); return RunShared(th); });
            SectorAlignedBufferPool.NativeAllocator = null;
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
        static nint RunRaw(int variant, long n)
        {
            nint sink = 0;
            switch (variant)
            {
                case 0: for (long i = 0; i < n; i++) { var p = Mimalloc.Malloc((nuint)BufferSize); sink ^= p; Mimalloc.Free(p); } break;
                case 1: for (long i = 0; i < n; i++) { var p = Mimalloc.MallocFast((nuint)BufferSize); sink ^= p; Mimalloc.FreeFast(p); } break;
                case 2: for (long i = 0; i < n; i++) { var p = Mimalloc.MallocAligned((nuint)BufferSize, (nuint)SectorSize); sink ^= p; Mimalloc.Free(p); } break;
                case 3: for (long i = 0; i < n; i++) { var p = Mimalloc.MallocAlignedFast((nuint)BufferSize, (nuint)SectorSize); sink ^= p; Mimalloc.FreeFast(p); } break;
            }
            return sink;
        }

        static double RawNs(int variant, long n)
        {
            _ = RunRaw(variant, n / 10);   // warm up
            var sw = Stopwatch.StartNew();
            var sink = RunRaw(variant, n);
            sw.Stop();
            System.GC.KeepAlive(sink);
            return sw.Elapsed.TotalSeconds / n * 1e9;
        }

        [Test]
        public void SingleThreadBreakdown()
        {
            if (!Mimalloc.TryInitialize())
                Assert.Ignore("mimalloc native library not available for this RID");

            // Alignment probe: is plain mi_malloc(4096) already sector/page aligned (would let us skip the
            // aligned slow path)?
            const int nchk = 100_000;
            var ptrs = new nint[nchk];
            int a512 = 0, a4096 = 0;
            for (var i = 0; i < nchk; i++) { var p = Mimalloc.Malloc((nuint)BufferSize); ptrs[i] = p; if ((p & (SectorSize - 1)) == 0) a512++; if ((p & 4095) == 0) a4096++; }
            for (var i = 0; i < nchk; i++) Mimalloc.Free(ptrs[i]);

            const long n = 20_000_000;
            double managedReuseNs = 1000.0 / RunShared(1);   // Mops/s -> ns/op

            double rawMalloc = RawNs(0, n);
            double rawMallocFast = RawNs(1, n);
            double rawAligned = RawNs(2, n);
            double rawAlignedFast = RawNs(3, n);

            SectorAlignedBufferPool.NativeAllocator = new UntrackedMimallocAllocator();
            double poolUntrackedNs = 1000.0 / RunShared(1);
            SectorAlignedBufferPool.NativeAllocator = new MimallocPooledAllocator();
            double poolTrackedNs = 1000.0 / RunShared(1);
            SectorAlignedBufferPool.NativeAllocator = null;

            TestContext.Progress.WriteLine($"plain mi_malloc(4096) alignment: 512-aligned {a512}/{nchk}, 4096-aligned {a4096}/{nchk}");
            TestContext.Progress.WriteLine("");
            TestContext.Progress.WriteLine($"{"single-thread cost (ns/op)",-40} | {"ns/op",8}");
            TestContext.Progress.WriteLine(new string('-', 52));
            TestContext.Progress.WriteLine($"{"managed pool reuse (floor)",-40} | {managedReuseNs,8:F1}");
            TestContext.Progress.WriteLine($"{"raw mi_malloc + mi_free (normal xition)",-40} | {rawMalloc,8:F1}");
            TestContext.Progress.WriteLine($"{"raw mi_malloc + mi_free (SuppressGC)",-40} | {rawMallocFast,8:F1}");
            TestContext.Progress.WriteLine($"{"raw mi_malloc_ALIGNED + free (normal)",-40} | {rawAligned,8:F1}");
            TestContext.Progress.WriteLine($"{"raw mi_malloc_ALIGNED + free (SuppressGC)",-40} | {rawAlignedFast,8:F1}");
            TestContext.Progress.WriteLine($"{"pool, mimalloc UNTRACKED (aligned,normal)",-40} | {poolUntrackedNs,8:F1}");
            TestContext.Progress.WriteLine($"{"pool, mimalloc (prod, on-demand stats)",-40} | {poolTrackedNs,8:F1}");
            TestContext.Progress.WriteLine("");
            TestContext.Progress.WriteLine($"GC-transition cost (2 calls/op): normal-vs-fast plain   = {rawMalloc - rawMallocFast,6:F1} ns/op");
            TestContext.Progress.WriteLine($"GC-transition cost (2 calls/op): normal-vs-fast aligned = {rawAligned - rawAlignedFast,6:F1} ns/op");
            TestContext.Progress.WriteLine($"alignment slow-path cost (normal): aligned - plain      = {rawAligned - rawMalloc,6:F1} ns/op");
            TestContext.Progress.WriteLine($"alignment slow-path cost (fast):   aligned - plain      = {rawAlignedFast - rawMallocFast,6:F1} ns/op");
            TestContext.Progress.WriteLine($"pool + wrapper overhead: poolUntracked - rawAligned     = {poolUntrackedNs - rawAligned,6:F1} ns/op");
            TestContext.Progress.WriteLine($"tracker overhead: poolTracked - poolUntracked           = {poolTrackedNs - poolUntrackedNs,6:F1} ns/op");
        }

        // ---- Physical-memory / page-fault verification ----

        [DllImport("libc", SetLastError = true)]
        static extern int getrusage(int who, byte[] usage);

        // struct rusage on Linux/x86-64: ru_minflt is the 9th long (offset 64), ru_majflt the 10th (offset 72).
        static (long minor, long major) Faults()
        {
            var b = new byte[144];
            if (getrusage(0, b) != 0)
                return (0, 0);
            return (System.BitConverter.ToInt64(b, 64), System.BitConverter.ToInt64(b, 72));
        }

        static void PoolLoop(SectorAlignedBufferPool pool, long n, bool clearOnReturn, int touch)
        {
            for (long i = 0; i < n; i++)
            {
                var page = pool.Get(BufferSize, clearOnReturn);
                if (touch == 2)
                {
                    page.aligned_pointer[0] = 1;
                    page.aligned_pointer[BufferSize - 1] = 1;
                }
                else if (touch < 0)
                {
                    new System.Span<byte>(page.aligned_pointer, BufferSize).Fill(1);   // simulate real IO writing the whole buffer
                }
                page.Return();
            }
        }

        [Test]
        public void PhysicalMemoryCheck()
        {
            if (!Mimalloc.TryInitialize())
                Assert.Ignore("mimalloc native library not available for this RID");

            const long n = 5_000_000;

            (double ns, long minflt, long majflt) Measure(INativePinnedAllocator alloc, bool clearOnReturn, int touch)
            {
                SectorAlignedBufferPool.NativeAllocator = alloc;
                var pool = new SectorAlignedBufferPool(1, SectorSize);
                PoolLoop(pool, n / 20, clearOnReturn, touch);   // warm up (fault in the reused block)
                var (min0, maj0) = Faults();
                var sw = Stopwatch.StartNew();
                PoolLoop(pool, n, clearOnReturn, touch);
                sw.Stop();
                var (min1, maj1) = Faults();
                pool.Free();
                SectorAlignedBufferPool.NativeAllocator = null;
                return (sw.Elapsed.TotalSeconds / n * 1e9, min1 - min0, maj1 - maj0);
            }

            INativePinnedAllocator Managed() => null;
            INativePinnedAllocator Native() => new MimallocPooledAllocator();

            TestContext.Progress.WriteLine($"N = {n:N0} ops/scenario. minflt/majflt = page faults during the measured loop (NOT warmup).");
            TestContext.Progress.WriteLine($"{"scenario",-46} | {"ns/op",7} | {"minflt",8} | {"majflt",7} | flt/op");
            TestContext.Progress.WriteLine(new string('-', 90));

            void Row(string name, INativePinnedAllocator alloc, bool clr, int touch)
            {
                var r = Measure(alloc, clr, touch);
                TestContext.Progress.WriteLine($"{name,-46} | {r.ns,7:F1} | {r.minflt,8:N0} | {r.majflt,7:N0} | {(double)r.minflt / n,6:F4}");
            }

            Row("managed pool, clr=false, touch 2 bytes", Managed(), false, 2);
            Row("mimalloc pool, clr=false, touch 2 bytes", Native(), false, 2);
            Row("managed pool, clr=false, touch FULL 4KB", Managed(), false, -1);
            Row("mimalloc pool, clr=false, touch FULL 4KB", Native(), false, -1);
            Row("managed pool, clr=TRUE (zeroed), touch 2", Managed(), true, 2);
            Row("mimalloc pool, clr=TRUE (mi_zalloc), touch 2", Native(), true, 2);
        }
    }
}