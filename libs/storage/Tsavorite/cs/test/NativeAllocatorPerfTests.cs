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
    /// A/B throughput comparison of the <see cref="SectorAlignedBufferPool"/> Get/Return hot path:
    /// managed per-level <c>ConcurrentQueue</c> recycling vs mimalloc thread-local heaps. This isolates the
    /// cache-line contention on the shared free list (the bottleneck PR #2018 shards) as thread count rises.
    /// [Explicit] — run manually in Release: dotnet test ... --filter FullyQualifiedName~NativeAllocatorPerfTests
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    [Explicit]
    public unsafe class NativeAllocatorPerfTests
    {
        const int SectorSize = 512;
        const int BufferSize = 4096;          // typical IO buffer
        const long OpsPerThread = 3_000_000;  // Get+write+Return per thread

        [TearDown]
        public void TearDown() => SectorAlignedBufferPool.NativeAllocator = null;

        static double RunSameThread(SectorAlignedBufferPool pool, int threads, bool clearOnReturn)
        {
            var barrier = new Barrier(threads + 1);
            var tasks = new Task[threads];
            for (var t = 0; t < threads; t++)
            {
                tasks[t] = Task.Factory.StartNew(() =>
                {
                    barrier.SignalAndWait();
                    for (long i = 0; i < OpsPerThread; i++)
                    {
                        var page = pool.Get(BufferSize, clearOnReturn);
                        page.aligned_pointer[0] = 1;                 // touch first byte
                        page.aligned_pointer[BufferSize - 1] = 1;    // touch last byte
                        page.Return();
                    }
                }, TaskCreationOptions.LongRunning);
            }

            barrier.SignalAndWait();
            var sw = Stopwatch.StartNew();
            Task.WaitAll(tasks);
            sw.Stop();
            return threads * OpsPerThread / sw.Elapsed.TotalSeconds;
        }

        [Test]
        public void SectorPoolContentionAB()
        {
            if (!Mimalloc.TryInitialize())
                Assert.Ignore("mimalloc native library not available for this RID");

            // clearOnReturn:false == device-read destination hot path (managed pool uses its no-memset isDirty
            // optimization; mimalloc uses mi_malloc). This isolates recycling/contention, not zeroing.
            const bool clearOnReturn = false;

            TestContext.Progress.WriteLine($"threads |   managed (Mops/s) |   mimalloc (Mops/s) |  speedup");
            TestContext.Progress.WriteLine($"--------+--------------------+---------------------+---------");
            foreach (var threads in new[] { 1, 2, 4, 8, 16, 32 })
            {
                // Managed
                SectorAlignedBufferPool.NativeAllocator = null;
                var managedPool = new SectorAlignedBufferPool(1, SectorSize);
                _ = RunSameThread(managedPool, threads, clearOnReturn);   // warm up
                var managed = RunSameThread(managedPool, threads, clearOnReturn);
                managedPool.Free();

                // Native (mimalloc)
                SectorAlignedBufferPool.NativeAllocator = new MimallocPooledAllocator();
                var nativePool = new SectorAlignedBufferPool(1, SectorSize);
                _ = RunSameThread(nativePool, threads, clearOnReturn);    // warm up
                var native = RunSameThread(nativePool, threads, clearOnReturn);
                SectorAlignedBufferPool.NativeAllocator = null;

                TestContext.Progress.WriteLine(
                    $"{threads,7} | {managed / 1e6,18:F2} | {native / 1e6,19:F2} | {native / managed,7:F2}x");
            }
        }
    }
}
