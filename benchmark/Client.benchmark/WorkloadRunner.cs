// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using HdrHistogram;
using LightEpoch = Garnet.client.LightEpoch;

namespace Client.benchmark
{
    /// <summary>
    /// Aggregated results of a timed benchmark phase.
    /// </summary>
    internal readonly struct RunResult
    {
        public long TotalOps { get; init; }
        public int Batch { get; init; }
        public double Seconds { get; init; }
        public double MeanMicros { get; init; }
        public double P50Micros { get; init; }
        public double P99Micros { get; init; }
        public double P999Micros { get; init; }
        public double MaxMicros { get; init; }

        /// <summary>Process CPU time (client and in-process server) per counted operation, in microseconds.</summary>
        public double CpuMicrosPerOp { get; init; }

        public double ThroughputOpsPerSec => Seconds > 0 ? TotalOps / Seconds : 0;
    }

    /// <summary>
    /// Closed-loop workload runner: a configurable number of dedicated worker threads each drive
    /// their own client connection and issue the measured operation in a tight loop. Each worker
    /// keeps <c>batch</c> operations in flight (firing them without awaiting, then blocking on
    /// <see cref="Task.WhenAll(Task[])"/>), which lets concurrent async operations share wire
    /// flushes (pipelining). Generic over the concrete <c>TDriver</c> struct so the per-op call is
    /// a constrained, devirtualized invocation.
    /// </summary>
    /// <remarks>
    /// Workers run on dedicated <see cref="Thread"/> instances (not the thread pool), mirroring
    /// Resp.benchmark's online runner. Blocking the pool from the workers would starve the async
    /// response-completion continuations inside <see cref="Garnet.client.GarnetClient"/> and
    /// deadlock the closed loop. The clients are not safe for concurrent use from multiple threads,
    /// so each worker owns a dedicated client connection; all connections of a run share one
    /// <see cref="LightEpoch"/>. When <c>batch</c> &gt; 1 a recorded latency sample measures the
    /// whole group's completion time (matching Resp.benchmark's online <c>--itp</c> accounting),
    /// while throughput counts every individual operation.
    /// </remarks>
    internal static class WorkloadRunner
    {
        static readonly long HistogramUpperBound = TimeStamp.Seconds(100);

        public static RunResult Run<TFactory, TDriver>(TFactory factory, LightEpoch epoch, int threads, int batch, int warmupSeconds, int durationSeconds, bool sharedSocket)
            where TFactory : struct, IDriverFactory<TDriver>
            where TDriver : struct, IOpDriver
        {
            if (batch < 1)
                batch = 1;

            // Per-thread topology: one client/connection per worker (the default). Shared-socket topology: a
            // single client/connection that every worker submits to concurrently, which is the many-producer
            // one-ring workload the duplex send path is designed to coalesce. Each driver is a readonly struct
            // wrapping a reference-typed client, so replicating one driver across the array shares the same
            // underlying client and socket; only the distinct instances are connected and disposed.
            var distinct = sharedSocket ? 1 : threads;
            var pool = new TDriver[distinct];
            var created = 0;
            try
            {
                while (created < pool.Length)
                {
                    pool[created] = factory.Create(epoch);
                    pool[created++].Connect();
                }

                var drivers = new TDriver[threads];
                for (var i = 0; i < threads; i++)
                    drivers[i] = pool[sharedSocket ? 0 : i];

                // Seed the shared key once; the server-side value is visible to every connection.
                pool[0].SeedAsync().GetAwaiter().GetResult();

                if (warmupSeconds > 0)
                    _ = RunPhase(drivers, batch, warmupSeconds, record: false);

                return RunPhase(drivers, batch, durationSeconds, record: true);
            }
            finally
            {
                for (var i = 0; i < created; i++)
                    pool[i].Dispose();
            }
        }

        static RunResult RunPhase<TDriver>(TDriver[] drivers, int batch, int seconds, bool record)
            where TDriver : struct, IOpDriver
        {
            var threads = drivers.Length;
            var histograms = new LongHistogram[threads];
            var counts = new long[threads];
            var startGate = new ManualResetEventSlim(false);
            var workers = new Thread[threads];
            var stop = 0;

            // Only batches that complete inside the measured window count. Workers finish the batch in flight
            // when the window closes, but that drain tail is excluded from both the operation count and the
            // elapsed time.
            var windowTicks = (long)(seconds * (double)Stopwatch.Frequency);
            long deadline = long.MaxValue;

            for (var t = 0; t < threads; t++)
            {
                var id = t;
                histograms[id] = new LongHistogram(1, HistogramUpperBound, 2);
                workers[id] = new Thread(() =>
                {
                    var driver = drivers[id];
                    var hist = histograms[id];
                    var inflight = new Task[batch];
                    startGate.Wait();
                    var end = Volatile.Read(ref deadline);

                    long ops = 0;
                    while (Volatile.Read(ref stop) == 0)
                    {
                        var ts = Stopwatch.GetTimestamp();
                        for (var j = 0; j < batch; j++)
                            inflight[j] = driver.IssueAsync();
                        Task.WhenAll(inflight).GetAwaiter().GetResult();

                        var done = Stopwatch.GetTimestamp();
                        if (done > end)
                            break;

                        if (record)
                        {
                            var elapsed = done - ts;
                            hist.RecordValue(elapsed <= HistogramUpperBound ? elapsed : HistogramUpperBound);
                        }

                        ops += batch;
                    }

                    counts[id] = ops;
                })
                {
                    IsBackground = true,
                    Name = $"bench-worker-{id}",
                };
                workers[id].Start();
            }

            var process = Process.GetCurrentProcess();
            var cpuStart = process.TotalProcessorTime;
            var start = Stopwatch.GetTimestamp();
            Volatile.Write(ref deadline, start + windowTicks);
            startGate.Set();
            Thread.Sleep(TimeSpan.FromSeconds(seconds));
            while (Stopwatch.GetTimestamp() < start + windowTicks)
                Thread.Yield();
            process.Refresh();
            var cpuEnd = process.TotalProcessorTime;
            Volatile.Write(ref stop, 1);
            foreach (var w in workers)
                w.Join();

            long total = 0;
            foreach (var c in counts)
                total += c;

            var scale = OutputScalingFactor.TimeStampToMicroseconds;
            var result = new RunResult
            {
                TotalOps = total,
                Batch = batch,
                Seconds = (double)windowTicks / Stopwatch.Frequency,
                CpuMicrosPerOp = total > 0 ? (cpuEnd - cpuStart).TotalMicroseconds / total : 0,
            };

            if (!record)
                return result;

            var summary = new LongHistogram(1, HistogramUpperBound, 2);
            foreach (var h in histograms)
                summary.Add(h);

            return result with
            {
                MeanMicros = summary.GetMean() / scale,
                P50Micros = summary.GetValueAtPercentile(50) / scale,
                P99Micros = summary.GetValueAtPercentile(99) / scale,
                P999Micros = summary.GetValueAtPercentile(99.9) / scale,
                MaxMicros = summary.GetValueAtPercentile(100) / scale,
            };
        }
    }
}