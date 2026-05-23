// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.kvbench
{
#pragma warning disable IDE0065 // Misplaced using directive
    using KvStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>;
    using KvAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>>;

    /// <summary>Headline result for one phase (load or one run iteration).</summary>
    public sealed class PhaseResult
    {
        public string Phase { get; init; } = "";
        public int Iteration { get; init; }
        public double ElapsedSec { get; init; }
        public long TotalOpsForThroughput { get; init; }
        public long FinalTotalOps { get; init; }
        public long OvershootOps { get; init; }
        public double MaxWorkerExitLagMs { get; init; }
        public long Reads { get; init; }
        public long Writes { get; init; }
        public long Deletes { get; init; }
        public long LogBegin { get; init; }
        public long LogHead { get; init; }
        public long LogReadOnly { get; init; }
        public long LogTail { get; init; }
        public long AllocBytesByWorkerMax { get; init; }
        public int GcGen0Delta { get; init; }
        public int GcGen1Delta { get; init; }
        public int GcGen2Delta { get; init; }
        public bool Interrupted { get; set; }
        public string ErrorMessage { get; set; }

        public double OpsPerSec => ElapsedSec > 0 ? TotalOpsForThroughput / ElapsedSec : 0;
    }

    /// <summary>Per-worker counters returned to the main thread after Join.</summary>
    internal sealed class WorkerStats
    {
        public long LocalOps;
        public long Reads;
        public long Writes;
        public long Deletes;
        public long AllocBytesDelta;
        public long FinalExitTicks; // Stopwatch.GetTimestamp() right before exit
    }

    /// <summary>
    /// KV.benchmark engine. Owns the device, store, scoreboard, and per-phase
    /// worker orchestration. Workers always use <c>BasicContext</c> (safe path).
    /// Implementation is split across partial files:
    /// <list type="bullet">
    ///   <item><c>KvBenchmark.cs</c> — store construction, public phase API (<see cref="Load"/> / <see cref="RunIteration"/> / <see cref="Dispose"/>), worker orchestration.</item>
    ///   <item><c>KvBenchmark.Worker.cs</c> — per-thread worker entrypoint and the RUN hot loop.</item>
    ///   <item><c>KvBenchmark.Validate.cs</c> — optional post-load read-back verification.</item>
    ///   <item><c>KvBenchmark.Setup.cs</c> — data-path / run-dir / stale-cleanup / device construction helpers.</item>
    /// </list>
    /// </summary>
    public sealed partial class KvBenchmark : IDisposable
    {
        public readonly Options Options;
        internal readonly KvNumaPinning Pinning;
        public readonly ZipfConstants ZipfConstants;
        public readonly string DataPath;
        public readonly string RunDir;

        // Hot-path shared state (each on its own cache-line-isolated padded type).
        readonly PaddedLong[] scoreboard;        // length = Threads + 2; [0] and [N+1] are unused sentinels
        readonly PaddedBool[] doneBox = new PaddedBool[1];
        readonly ManualResetEventSlim gate = new(false);
        long startTicks;

        readonly KvSessionFunctions functions = new();
        readonly IDevice device;
        readonly TsavoriteKV<KvStoreFunctions, KvAllocator> store;

        const long kChunkSize = 640;             // ops per chunk in load & run loops

        public KvBenchmark(Options opts)
        {
            Options = opts;
            DataPath = ResolveDataPath(opts);
            Directory.CreateDirectory(DataPath);
            CleanStaleRunDirs(DataPath);
            RunDir = CreateRunDir(DataPath);

            // Pin the SETUP thread BEFORE building the store — first-touch policy
            // puts the hash index + log buffer on the requested NUMA node.
            Pinning = new KvNumaPinning(opts, opts.Threads);
            Pinning.PinSetupOrReporter();

            scoreboard = GC.AllocateArray<PaddedLong>(opts.Threads + 2, pinned: true);
            ZipfConstants = opts.UseZipf ? new ZipfConstants(opts.Keys, opts.ZipfTheta) : null;

            var logPath = opts.ResolvedDeviceType == DeviceType.Null ? null : Path.Combine(RunDir, "hlog");
            device = CreateDevice(opts, logPath);

            // Defaults below match Garnet's defaults.conf for apples-to-apples comparison with the
            // Garnet RESP server. Anything not exposed via CLI stays at the Tsavorite KVSettings
            // default (MutableFraction=0.9, MaxInlineKeySize=128B — both also match Garnet).
            var kvSettings = new KVSettings
            {
                IndexSize = opts.ResolvedIndexRequestedBytes,
                LogDevice = device,
                LogMemorySize = opts.ResolvedLogMemoryBytes,
                PageSize = opts.ResolvedPageSizeBytes,                 // Garnet default: 16 MB
                SegmentSize = opts.ResolvedSegmentSizeBytes,           // Garnet default: 1 GB
                MaxInlineValueSize = (int)opts.ResolvedMaxInlineValueSizeBytes, // Garnet default: 16 KB
                PreallocateLog = opts.PreallocateLog,                  // Garnet default: false (CLI override available)
                // MutableFraction stays at the KVSettings/Garnet default (0.9).
            };

            store = new TsavoriteKV<KvStoreFunctions, KvAllocator>(
                kvSettings,
                StoreFunctions.Create(SpanByteComparer.Instance, new SpanByteRecordTriggers()),
                (allocSettings, sf) => new KvAllocator(allocSettings, sf));
        }

        // ====== Public phase API ======

        /// <summary>Loads <c>Options.Keys</c> records, deterministically partitioned across workers.</summary>
        public PhaseResult Load()
            => RunWorkers(phase: "load", isLoad: true, durationSec: 0, iteration: 0);

        /// <summary>Runs one warmup window (if configured) followed by a measured run window.</summary>
        public PhaseResult RunIteration(int iteration)
        {
            if (Options.WarmupSec > 0)
                RunWorkers(phase: "warmup", isLoad: false, durationSec: Options.WarmupSec, iteration: iteration);
            return RunWorkers(phase: "run", isLoad: false, durationSec: Options.RunSec, iteration: iteration);
        }

        // ====== Worker orchestration ======

        /// <summary>
        /// Spawns Workers, sleeps for the run window (or waits for load to finish),
        /// signals done, joins, and assembles the <see cref="PhaseResult"/>.
        /// </summary>
        PhaseResult RunWorkers(string phase, bool isLoad, int durationSec, int iteration)
        {
            // Reset shared state for this phase.
            doneBox[0].Value = false;
            Volatile.Write(ref globalChunkIdx, 0);
            for (int i = 0; i < scoreboard.Length; i++)
                Volatile.Write(ref scoreboard[i].Value, 0);
            gate.Reset();

            var threads = new Thread[Options.Threads];
            var stats = new WorkerStats[Options.Threads];
            var ready = new CountdownEvent(Options.Threads);

            for (int i = 0; i < Options.Threads; i++)
            {
                int idx = i;
                stats[idx] = new WorkerStats();
                threads[idx] = new Thread(() => WorkerProc(idx, isLoad, stats[idx], ready))
                {
                    IsBackground = false,
                    Name = $"kv-bench-{phase}-{idx}",
                };
                threads[idx].Start();
            }

            // All workers parked on the gate; capture GC + tick baselines just before releasing.
            ready.Wait();
            var gc0 = GC.CollectionCount(0);
            var gc1 = GC.CollectionCount(1);
            var gc2 = GC.CollectionCount(2);
            startTicks = Stopwatch.GetTimestamp();
            gate.Set();

            long doneTicks;
            long totalForThroughput;
            if (isLoad)
            {
                // Load: each partition is bounded; just wait for workers to finish.
                foreach (var t in threads) t.Join();
                doneTicks = Stopwatch.GetTimestamp();
                totalForThroughput = SumScoreboard();
            }
            else if (durationSec <= 0)
            {
                // Degenerate zero-second run: signal done immediately.
                Thread.MemoryBarrier();
                doneBox[0].Value = true;
                doneTicks = Stopwatch.GetTimestamp();
                totalForThroughput = SumScoreboard();
                foreach (var t in threads) t.Join();
            }
            else
            {
                Thread.Sleep(TimeSpan.FromSeconds(durationSec));
                doneBox[0].Value = true;
                doneTicks = Stopwatch.GetTimestamp();
                totalForThroughput = SumScoreboard();
                foreach (var t in threads) t.Join();
            }

            // Aggregate per-worker stats.
            var finalTotal = 0L;
            var maxAlloc = 0L;
            long reads = 0, writes = 0, deletes = 0;
            double maxExitLagMs = 0;
            foreach (var s in stats)
            {
                finalTotal += s.LocalOps;
                reads += s.Reads;
                writes += s.Writes;
                deletes += s.Deletes;
                if (s.AllocBytesDelta > maxAlloc) maxAlloc = s.AllocBytesDelta;
                var lagMs = (s.FinalExitTicks - doneTicks) * 1000.0 / Stopwatch.Frequency;
                if (lagMs > maxExitLagMs) maxExitLagMs = lagMs;
            }

            var elapsedSec = (doneTicks - startTicks) / (double)Stopwatch.Frequency;
            if (isLoad) totalForThroughput = finalTotal;  // load: finalTotal == sum by construction

            return new PhaseResult
            {
                Phase = phase,
                Iteration = iteration,
                ElapsedSec = elapsedSec,
                TotalOpsForThroughput = totalForThroughput,
                FinalTotalOps = finalTotal,
                OvershootOps = Math.Max(0, finalTotal - totalForThroughput),
                MaxWorkerExitLagMs = maxExitLagMs,
                Reads = reads,
                Writes = writes,
                Deletes = deletes,
                LogBegin = store.Log.BeginAddress,
                LogHead = store.Log.HeadAddress,
                LogReadOnly = store.Log.ReadOnlyAddress,
                LogTail = store.Log.TailAddress,
                AllocBytesByWorkerMax = maxAlloc,
                GcGen0Delta = GC.CollectionCount(0) - gc0,
                GcGen1Delta = GC.CollectionCount(1) - gc1,
                GcGen2Delta = GC.CollectionCount(2) - gc2,
            };
        }

        /// <summary>Sums all per-thread scoreboard slots (sentinel slots [0] and [N+1] stay zero).</summary>
        long SumScoreboard()
        {
            long total = 0;
            for (int i = 1; i <= Options.Threads; i++)
                total += Volatile.Read(ref scoreboard[i].Value);
            return total;
        }

        // ====== Cleanup ======

        public void Dispose()
        {
            try { store?.Dispose(); } catch { /* swallow */ }
            try { device?.Dispose(); } catch { /* swallow */ }
            try
            {
                // Only nuke our own run dir (identified by its owner sentinel file).
                if (!string.IsNullOrEmpty(RunDir) && Directory.Exists(RunDir))
                {
                    var sentinel = Path.Combine(RunDir, ".kv-benchmark-owner");
                    if (File.Exists(sentinel))
                        Directory.Delete(RunDir, recursive: true);
                }
            }
            catch { /* best-effort */ }
            gate?.Dispose();
        }
    }
#pragma warning restore IDE0065
}
