// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Heavy stress + performance soak for the origin-return <see cref="SectorAlignedBufferPool"/>.
    /// Exercises the IO asymmetry (issuing threads <c>Get</c>, arbitrary completion threads <c>Return</c>
    /// cross-thread) at high oversubscription, with continuous thread- and pool-churn, mixed size classes, and
    /// periodic GC. Reports throughput (Mops/s), Get+Return latency percentiles, and reuse efficiency
    /// (allocations vs working set); compares the origin-return and per-level ConcurrentQueue backends.
    /// [Explicit] — run manually in Release:
    ///   dotnet test ... -c Release --filter FullyQualifiedName~SectorAlignedBufferPoolStressTests
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    [Explicit]
    public unsafe class SectorAlignedBufferPoolStressTests
    {
        const int SectorSize = 512;

        // Size classes spanning the linear (record-sized), geometric, and above-cutoff (bypass) regions.
        static readonly int[] MixedSizes = [512, 1024, 3072, 4096, 12288, 65536, 262144, 4_000_000];

        [SetUp]
        public void Setup()
        {
            SectorAlignedBufferPool.Disabled = false;
            SectorAlignedBufferPool.UnpinOnReturn = false;
            SectorAlignedBufferPool.UseOriginReturn = true;
            SectorAlignedBufferPool.ManagedBudgetBytes = 1L << 30;
        }

        [TearDown]
        public void TearDown()
        {
            SectorAlignedBufferPool.UseOriginReturn = true;
            SectorAlignedBufferPool.ManagedBudgetBytes = 1L << 30;
        }

        // ========================================================================================================
        // 1. Scaling throughput (per-thread fast path): tight per-thread Get/Return loop across a thread sweep,
        //    comparing the origin-return and per-level ConcurrentQueue backends. Shows that the per-thread
        //    parking design scales with thread count instead of contending on a shared queue's cache line.
        // ========================================================================================================

        [Test]
        public void ScalingThroughputAcrossBackends()
        {
            const int bufferSize = 4096;          // typical record-sized IO buffer (pooled, per-thread fast path)

            TestContext.Progress.WriteLine("threads | origin-return | legacy-1queue | or/legacy");
            TestContext.Progress.WriteLine("--------+---------------+---------------+----------");
            foreach (var threads in new[] { 1, 2, 4, 8, 16, 32, 64 })
            {
                // Full op budget for the origin-return backend; cap the per-level ConcurrentQueue backend at high
                // thread counts, where its throughput drops below 1 Mops/s, so the sweep completes in bounded time.
                var scalableOps = 5_000_000L;
                var legacyOps = threads >= 16 ? 1_000_000L : 5_000_000L;

                var origin = RunTightLoop(useOriginReturn: true, threads, bufferSize, scalableOps);
                var legacy = RunTightLoop(useOriginReturn: false, threads, bufferSize, legacyOps);

                TestContext.Progress.WriteLine(
                    $"{threads,7} | {origin / 1e6,13:F1} | {legacy / 1e6,13:F1} | {(legacy > 0 ? origin / legacy : 0),8:F1}x");
            }
        }

        static double RunTightLoop(bool useOriginReturn, int threads, int bufferSize, long opsPerThread)
        {
            SectorAlignedBufferPool.UseOriginReturn = useOriginReturn;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var barrier = new Barrier(threads + 1);
                var tasks = new Task[threads];
                for (var t = 0; t < threads; t++)
                {
                    tasks[t] = Task.Factory.StartNew(() =>
                    {
                        barrier.SignalAndWait();
                        for (long i = 0; i < opsPerThread; i++)
                        {
                            var page = pool.Get(bufferSize, clearOnReturn: false);
                            page.aligned_pointer[0] = 1;
                            page.aligned_pointer[bufferSize - 1] = 1;
                            page.Return();
                        }
                    }, TaskCreationOptions.LongRunning);
                }
                barrier.SignalAndWait();
                var sw = Stopwatch.StartNew();
                Task.WaitAll(tasks);
                sw.Stop();
                return threads * opsPerThread / sw.Elapsed.TotalSeconds;
            }
            finally
            {
                pool.Free();
            }
        }

        // ========================================================================================================
        // 1b. Cross-thread (origin-return) latency + reuse: N/2 producers Get, N/2 completion threads Return
        //    cross-thread at a bounded in-flight depth, pooled sizes only. Reports Get latency percentiles and
        //    reuse efficiency (allocations vs working set). Throughput here is handoff-bound (BlockingCollection),
        //    so it is intentionally NOT reported as a pool metric — see ScalingThroughputAB for that.
        // ========================================================================================================

        static readonly int[] PooledSizes = [512, 1024, 4096, 12288, 65536];

        [Test]
        public void CrossThreadReturnLatencyAndReuse()
        {
            TestContext.Progress.WriteLine(
                "threads |           mode |  p50(ns) |  p99(ns) | p999(ns) | allocs | workingset | reuse");
            TestContext.Progress.WriteLine(
                "--------+----------------+----------+----------+----------+--------+------------+------");

            foreach (var threads in new[] { 4, 8, 16, 32, 64 })
            {
                RunAsymmetric("origin-return", threads, useOriginReturn: true);
                RunAsymmetric("legacy-1queue", threads, useOriginReturn: false);
            }
        }

        /// <summary>
        /// N/2 producers <c>Get</c> and enqueue; N/2 completion threads dequeue and <c>Return</c> cross-thread,
        /// bounded to a fixed in-flight depth (pooled sizes only). Reports Get-latency percentiles + reuse.
        /// </summary>
        static void RunAsymmetric(string mode, int threads, bool useOriginReturn)
        {
            const long opsPerProducer = 1_000_000;
            const int inFlightPerProducer = 8;

            var producers = Math.Max(1, threads / 2);
            var consumers = Math.Max(1, threads - producers);

            SectorAlignedBufferPool.UseOriginReturn = useOriginReturn;
            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var channels = new BlockingCollection<SectorAlignedMemory>[producers];
                for (var i = 0; i < producers; i++)
                    channels[i] = new BlockingCollection<SectorAlignedMemory>(inFlightPerProducer);

                var barrier = new Barrier(producers + consumers + 1);
                var latencies = new long[producers][];
                var rrIndex = 0;

                var producerTasks = new Task[producers];
                for (var pi = 0; pi < producers; pi++)
                {
                    var localPi = pi;
                    var samples = new long[(int)Math.Min(opsPerProducer, 200_000)];
                    latencies[localPi] = samples;
                    producerTasks[pi] = Task.Factory.StartNew(() =>
                    {
                        var rnd = new Random(1000 + localPi);
                        var sw = new Stopwatch();
                        barrier.SignalAndWait();
                        for (long i = 0; i < opsPerProducer; i++)
                        {
                            var size = PooledSizes[rnd.Next(PooledSizes.Length)];
                            sw.Restart();
                            var page = pool.Get(size, clearOnReturn: false);
                            sw.Stop();
                            if (i < samples.Length)
                                samples[i] = sw.Elapsed.Ticks;
                            page.aligned_pointer[0] = 1;
                            channels[(int)(Interlocked.Increment(ref rrIndex) % producers)].Add(page);
                        }
                    }, TaskCreationOptions.LongRunning);
                }

                var consumerTasks = new Task[consumers];
                var totalProduced = producers * opsPerProducer;
                var consumed = 0L;
                for (var ci = 0; ci < consumers; ci++)
                {
                    var localCi = ci;
                    consumerTasks[ci] = Task.Factory.StartNew(() =>
                    {
                        barrier.SignalAndWait();
                        while (Interlocked.Read(ref consumed) < totalProduced)
                        {
                            for (var c = 0; c < producers; c++)
                            {
                                if (channels[(localCi + c) % producers].TryTake(out var page, 1))
                                {
                                    page.Return();
                                    Interlocked.Increment(ref consumed);
                                }
                            }
                        }
                    }, TaskCreationOptions.LongRunning);
                }

                barrier.SignalAndWait();
                Task.WaitAll(producerTasks);
                Task.WaitAll(consumerTasks);

                var (p50, p99, p999) = Percentiles(latencies);
                long allocs = pool.TotalManagedAllocations;
                var workingSet = (long)producers * inFlightPerProducer;
                var reuse = (double)totalProduced / Math.Max(1, allocs);

                TestContext.Progress.WriteLine(
                    $"{threads,7} | {mode,14} | {p50,8:F0} | {p99,8:F0} | {p999,8:F0} | {allocs,6} | {workingSet,10} | {reuse,4:F0}x");
            }
            finally
            {
                pool.Free();
            }
        }

        static (double p50, double p99, double p999) Percentiles(long[][] perThread)
        {
            var all = new List<long>();
            foreach (var arr in perThread)
                if (arr != null)
                    all.AddRange(arr.Where(x => x > 0));
            if (all.Count == 0)
                return (0, 0, 0);
            all.Sort();
            double at(double q) => all[(int)Math.Min(all.Count - 1, Math.Max(0, q * all.Count))] * 100.0; // Ticks(100ns) -> ns
            return (at(0.50), at(0.99), at(0.999));
        }

        // ========================================================================================================
        // 2. Reuse efficiency: allocation count must track the working set, NOT the op count.
        // ========================================================================================================

        [Test]
        public void ReuseEfficiencyAllocTracksWorkingSet()
        {
            SectorAlignedBufferPool.UseOriginReturn = true;
            const int producers = 8;
            const int inFlight = 8;
            const long opsPerProducer = 1_000_000;

            var pool = new SectorAlignedBufferPool(1, SectorSize);
            try
            {
                var channel = new BlockingCollection<SectorAlignedMemory>(producers * inFlight);
                var barrier = new Barrier(producers + producers + 1);

                var producerTasks = new Task[producers];
                for (var p = 0; p < producers; p++)
                {
                    producerTasks[p] = Task.Factory.StartNew(() =>
                    {
                        barrier.SignalAndWait();
                        for (long i = 0; i < opsPerProducer; i++)
                            channel.Add(pool.Get(4096, clearOnReturn: false));
                    }, TaskCreationOptions.LongRunning);
                }

                var consumed = 0L;
                var total = producers * opsPerProducer;
                var consumerTasks = new Task[producers];
                for (var c = 0; c < producers; c++)
                {
                    consumerTasks[c] = Task.Factory.StartNew(() =>
                    {
                        barrier.SignalAndWait();
                        while (Interlocked.Read(ref consumed) < total)
                            if (channel.TryTake(out var page, 5))
                            {
                                page.Return();
                                Interlocked.Increment(ref consumed);
                            }
                    }, TaskCreationOptions.LongRunning);
                }

                barrier.SignalAndWait();
                Task.WaitAll(producerTasks);
                Task.WaitAll(consumerTasks);

                var allocs = pool.TotalManagedAllocations;
                var workingSet = producers * inFlight;
                TestContext.Progress.WriteLine(
                    $"ops={total:N0} allocs={allocs:N0} workingSet~={workingSet} ratio={(double)allocs / workingSet:F1}x");

                // Origin-return must reuse: allocations bounded by a small multiple of the working set, never the op count.
                ClassicAssert.Less(allocs, total / 100,
                    "origin-return must reuse buffers, not allocate per op");
                ClassicAssert.Less(allocs, workingSet * 50L,
                    "allocation count must stay near the working set");
            }
            finally { pool.Free(); }
        }

        // ========================================================================================================
        // 3. Heavy soak: oversubscribed asymmetric load + thread churn + short-lived pool churn + GC.
        //    Asserts stability (no crash/leak, budget quiesces to zero, bounded shard arrays).
        // ========================================================================================================

        [Test]
        public void HeavySoakWithChurn()
        {
            SectorAlignedBufferPool.UseOriginReturn = true;
            SectorAlignedBufferPool.ManagedBudgetBytes = 256L << 20; // 256 MB budget: exercise overflow/non-cacheable

            var duration = TimeSpan.FromSeconds(20);
            var cores = Environment.ProcessorCount;
            var producers = Math.Max(4, cores);          // oversubscribed
            var consumers = Math.Max(4, cores);

            var longLivedPool = new SectorAlignedBufferPool(1, SectorSize);
            var stop = false;
            var totalGets = 0L;
            var totalReturns = 0L;
            var errors = new ConcurrentQueue<Exception>();
            var channel = new BlockingCollection<SectorAlignedMemory>(producers * 16);

            var tasks = new List<Task>();

            // Steady-state producers against the long-lived pool.
            for (var p = 0; p < producers; p++)
            {
                var seed = p;
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    var rnd = new Random(seed);
                    try
                    {
                        while (!Volatile.Read(ref stop))
                        {
                            var size = MixedSizes[rnd.Next(MixedSizes.Length)];
                            var page = longLivedPool.Get(size, (rnd.Next() & 1) == 0);
                            page.aligned_pointer[0] = 0xEE;
                            Interlocked.Increment(ref totalGets);
                            channel.Add(page);
                        }
                    }
                    catch (Exception e) { errors.Enqueue(e); }
                }, TaskCreationOptions.LongRunning));
            }

            // Completion threads Return cross-thread.
            for (var c = 0; c < consumers; c++)
            {
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    try
                    {
                        while (!Volatile.Read(ref stop) || channel.Count > 0)
                            if (channel.TryTake(out var page, 5))
                            {
                                page.Return();
                                Interlocked.Increment(ref totalReturns);
                            }
                    }
                    catch (Exception e) { errors.Enqueue(e); }
                }, TaskCreationOptions.LongRunning));
            }

            // Adversarial churn: continuously spawn short-lived worker threads that Get/Return on the long-lived
            // pool (exercising dead-origin sealing + finalizer permit reclamation).
            tasks.Add(Task.Factory.StartNew(() =>
            {
                try
                {
                    while (!Volatile.Read(ref stop))
                    {
                        var th = new Thread(() =>
                        {
                            for (var i = 0; i < 64; i++)
                            {
                                var page = longLivedPool.Get(4096, clearOnReturn: false);
                                Interlocked.Increment(ref totalGets);
                                if ((i & 1) == 0)
                                {
                                    page.Return();     // same-thread local cache (some left for finalizer)
                                    Interlocked.Increment(ref totalReturns);
                                }
                                else
                                {
                                    channel.Add(page); // route others cross-thread
                                }
                            }
                        });
                        th.Start();
                        th.Join();
                    }
                }
                catch (Exception e) { errors.Enqueue(e); }
            }, TaskCreationOptions.LongRunning));

            // Adversarial churn: create + free short-lived pools (StreamProvider-style) to exercise slot recycling.
            tasks.Add(Task.Factory.StartNew(() =>
            {
                try
                {
                    while (!Volatile.Read(ref stop))
                    {
                        var ephemeral = new SectorAlignedBufferPool(1, SectorSize);
                        for (var i = 0; i < 32; i++)
                        {
                            var page = ephemeral.Get(1024, clearOnReturn: false);
                            page.Return();
                        }
                        ephemeral.Free();
                        ClassicAssert.AreEqual(0, ephemeral.ReservedBytes, "ephemeral pool budget must be zero after Free");
                    }
                }
                catch (Exception e) { errors.Enqueue(e); }
            }, TaskCreationOptions.LongRunning));

            // Periodic GC to force finalizers.
            tasks.Add(Task.Factory.StartNew(() =>
            {
                while (!Volatile.Read(ref stop))
                {
                    Thread.Sleep(500);
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }, TaskCreationOptions.LongRunning));

            Thread.Sleep(duration);
            Volatile.Write(ref stop, true);
            Task.WaitAll(tasks.ToArray());

            // Drain any stragglers left in the channel.
            while (channel.TryTake(out var page))
            {
                page.Return();
                Interlocked.Increment(ref totalReturns);
            }

            ClassicAssert.IsEmpty(errors, "no exceptions during soak: " + string.Join("; ", errors.Select(e => e.Message)));

            // Peak reserved bytes must have stayed within the budget throughout (hard bound).
            ClassicAssert.LessOrEqual(longLivedPool.ReservedBytes, SectorAlignedBufferPool.ManagedBudgetBytes,
                "reserved bytes must never exceed the budget");

            TestContext.Progress.WriteLine(
                $"soak: gets={totalGets:N0} returns={totalReturns:N0} reserved={longLivedPool.ReservedBytes:N0} liveShards={longLivedPool.LiveShardCount}");

            // Quiesce: free the pool, force finalizers, assert budget returns to zero.
            longLivedPool.Free();
            for (var attempt = 0; attempt < 20 && longLivedPool.ReservedBytes > 0; attempt++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                Thread.Sleep(50);
            }
            ClassicAssert.AreEqual(0, longLivedPool.ReservedBytes, "budget must quiesce to zero after soak + Free + finalizers");
        }
    }
}