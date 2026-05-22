// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Globalization;
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
        public long Hits { get; init; }
        public long Misses { get; init; }
        public long DeletesReinserted { get; init; }
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
        public long Hits;
        public long Misses;
        public long DeletesReinserted;
        public long AllocBytesDelta;
        public long FinalExitTicks; // Stopwatch.GetTimestamp() right before exit
    }

    /// <summary>
    /// KV.benchmark engine. Owns the device, store, scoreboard, and per-phase
    /// worker orchestration. Workers always use <c>BasicContext</c> (safe path).
    /// </summary>
    public sealed class KvBenchmark : IDisposable
    {
        public readonly Options Options;
        internal readonly KvNumaPinning Pinning;
        public readonly ZipfConstants ZipfConstants;
        public readonly string DataPath;
        public readonly string RunDir;

        // Hot-path shared state (each on its own cache-line-isolated padded type).
        readonly PaddedLong[] scoreboard;       // index 0 and N+1 are sentinel padding
        readonly PaddedBool[] doneBox = new PaddedBool[1];
        readonly ManualResetEventSlim gate = new(false);
        long startTicks;

        readonly KvSessionFunctions functions = new();
        readonly IDevice device;
        readonly TsavoriteKV<KvStoreFunctions, KvAllocator> store;

        long chunkCounter; // global chunk id for the run phase (matches YCSB pattern)
        const long kChunkSize = 256;

        public KvBenchmark(Options opts)
        {
            Options = opts;
            DataPath = ResolveDataPath(opts);
            Directory.CreateDirectory(DataPath);
            CleanStaleRunDirs(DataPath);
            RunDir = CreateRunDir(DataPath);

            // Pin the SETUP thread BEFORE building the store; first-touch policy
            // ensures the hash index + log buffer land on the requested NUMA node.
            Pinning = new KvNumaPinning(opts, opts.Threads);
            Pinning.PinSetupOrReporter();

            scoreboard = GC.AllocateArray<PaddedLong>(opts.Threads + 2, pinned: true);

            ZipfConstants = opts.UseZipf ? new ZipfConstants(opts.Keys, opts.ZipfTheta) : null;

            // Create the device.
            var logPath = opts.ResolvedDeviceType == DeviceType.Null ? null : Path.Combine(RunDir, "hlog");
            device = CreateDevice(opts, logPath);

            // Build KVSettings.
            var kvSettings = new KVSettings
            {
                IndexSize = opts.ResolvedIndexRequestedBytes,
                LogDevice = device,
                PreallocateLog = true,
                LogMemorySize = opts.ResolvedLogMemoryBytes,
                PageSize = opts.ResolvedPageSizeBytes,
                SegmentSize = opts.ResolvedSegmentSizeBytes,
                // MutableFraction left at KVSettings default 0.9
            };

            store = new TsavoriteKV<KvStoreFunctions, KvAllocator>(
                kvSettings,
                StoreFunctions.Create(SpanByteComparer.Instance, new SpanByteRecordTriggers()),
                (allocSettings, sf) => new KvAllocator(allocSettings, sf));
        }

        // ====== Public phase API ======

        /// <summary>
        /// Load <c>opts.Keys</c> records, partitioned across worker threads.
        /// Returns the load throughput result.
        /// </summary>
        public PhaseResult Load()
            => RunWorkers(phase: "load", isLoad: true, durationSec: 0, iteration: 0);

        /// <summary>
        /// Run a single run-phase iteration: warmup + measured window.
        /// </summary>
        public PhaseResult RunIteration(int iteration)
        {
            if (Options.WarmupSec > 0)
                RunWorkers(phase: "warmup", isLoad: false, durationSec: Options.WarmupSec, iteration: iteration);
            return RunWorkers(phase: "run", isLoad: false, durationSec: Options.RunSec, iteration: iteration);
        }

        // ====== Worker driver ======

        private PhaseResult RunWorkers(string phase, bool isLoad, int durationSec, int iteration)
        {
            // Reset shared state.
            doneBox[0].Value = false;
            Volatile.Write(ref chunkCounter, 0);
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

            // Wait for all workers to be parked on the gate.
            ready.Wait();

            // GC + timing baselines.
            var gc0 = GC.CollectionCount(0);
            var gc1 = GC.CollectionCount(1);
            var gc2 = GC.CollectionCount(2);

            // Capture startTicks BEFORE releasing the gate.
            startTicks = Stopwatch.GetTimestamp();
            gate.Set();

            long doneTicks;
            long totalForThroughput;
            if (isLoad)
            {
                // Load runs until all workers exit (each partition is bounded).
                foreach (var t in threads) t.Join();
                doneTicks = Stopwatch.GetTimestamp();
                totalForThroughput = SumScoreboard();
            }
            else if (durationSec <= 0)
            {
                // Edge case: 0-second run. Just mark done immediately.
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

            var finalTotal = 0L;
            var maxAlloc = 0L;
            long hits = 0, misses = 0, deletesReinserted = 0;
            double maxExitLagMs = 0;
            foreach (var s in stats)
            {
                finalTotal += s.LocalOps;
                hits += s.Hits;
                misses += s.Misses;
                deletesReinserted += s.DeletesReinserted;
                if (s.AllocBytesDelta > maxAlloc) maxAlloc = s.AllocBytesDelta;
                var lagMs = (s.FinalExitTicks - doneTicks) * 1000.0 / Stopwatch.Frequency;
                if (lagMs > maxExitLagMs) maxExitLagMs = lagMs;
            }

            var elapsedSec = (doneTicks - startTicks) / (double)Stopwatch.Frequency;
            // For the load phase, finalTotal == totalForThroughput by construction.
            if (isLoad) totalForThroughput = finalTotal;

            return new PhaseResult
            {
                Phase = phase,
                Iteration = iteration,
                ElapsedSec = elapsedSec,
                TotalOpsForThroughput = totalForThroughput,
                FinalTotalOps = finalTotal,
                OvershootOps = Math.Max(0, finalTotal - totalForThroughput),
                MaxWorkerExitLagMs = maxExitLagMs,
                Hits = hits,
                Misses = misses,
                DeletesReinserted = deletesReinserted,
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

        long SumScoreboard()
        {
            long total = 0;
            for (int i = 1; i <= Options.Threads; i++)
                total += Volatile.Read(ref scoreboard[i].Value);
            return total;
        }

        // ====== Worker hot loop ======

        unsafe void WorkerProc(int threadIdx, bool isLoad, WorkerStats stats, CountdownEvent ready)
        {
            Pinning.PinWorker(threadIdx);

            // Per-thread RNG seeded distinctly via SplitMix64 mixing.
            ulong basis = Options.Seed * 0x9E3779B97F4A7C15UL + (ulong)(threadIdx + 1);
            var rng = new XoshiroRng(basis);
            var zipf = ZipfConstants != null ? new ZipfGenerator(ZipfConstants) : default;

            // Per-thread buffers. Value-size ≤ 4 KB by validation, so stackalloc is safe.
            byte* valuePtr = stackalloc byte[Options.ValueSize];
            byte* inputPtr = stackalloc byte[Options.ValueSize];
            byte* outputPtr = stackalloc byte[KvSessionFunctions.kReaderCopyBytes];

            // Bake a per-thread "value pattern" into the value buffer so writes
            // are not all zeros (would be a degenerate workload on some allocators).
            for (int i = 0; i < Options.ValueSize; i++)
                valuePtr[i] = (byte)((threadIdx * 31 + i) & 0xFF);
            for (int i = 0; i < Options.ValueSize; i++)
                inputPtr[i] = (byte)((threadIdx * 17 + i + 1) & 0xFF);

            var valueSpan = new Span<byte>(valuePtr, Options.ValueSize);
            var inputSpan = new Span<byte>(inputPtr, Options.ValueSize);
            var outputSpan = new Span<byte>(outputPtr, KvSessionFunctions.kReaderCopyBytes);
            var pinnedInput = PinnedSpanByte.FromPinnedSpan(inputSpan);
            var output = SpanByteAndMemory.FromPinnedSpan(outputSpan);

            // Workload partition for load phase.
            long loadFrom = 0, loadTo = 0;
            if (isLoad)
            {
                loadFrom = (long)((double)Options.Keys * threadIdx / Options.Threads);
                loadTo = (long)((double)Options.Keys * (threadIdx + 1) / Options.Threads);
            }

            using var session = store.NewSession<KvKey, PinnedSpanByte, SpanByteAndMemory, Empty, KvSessionFunctions>(functions);
            var bContext = session.BasicContext;

            ref var mySlot = ref scoreboard[threadIdx + 1];
            ref var doneFlag = ref doneBox[0].Value;

            ready.Signal();
            gate.Wait();

            long localOps = 0;
            long hits = 0, misses = 0, deletesReinserted = 0;
            var allocBefore = GC.GetAllocatedBytesForCurrentThread();

            if (isLoad)
            {
                // Each thread inserts its partition deterministically.
                KvKey key = default;
                for (long k = loadFrom; k < loadTo; k++)
                {
                    key.Value = k;
                    bContext.Upsert(key, valueSpan, Empty.Default);
                    ++localOps;
                    if ((localOps & (kChunkSize - 1)) == 0)
                    {
                        if ((localOps & 65535) == 0)
                            bContext.CompletePending(false);
                        Volatile.Write(ref mySlot.Value, localOps);
                    }
                }
                bContext.CompletePending(true);
            }
            else
            {
                // Dispatch to a small focused loop per workload kind so the JIT
                // inline budget is small enough to inline BasicContext.Read into
                // the hot loop body (rather than emitting an out-of-line call).
                // For pure-read uniform 32-bit workloads (the lean-benchmark
                // baseline), we run the most minimal loop possible.
                var keyCount = Options.Keys;
                var fast32 = keyCount > 0 && keyCount <= uint.MaxValue;
                var readPct = Options.ReadPct;
                var useZipf = Options.UseZipf;
                var deleteReinsert = Options.RumdHasDeletes();
                var isPureReadFast32Uniform = !useZipf && fast32 && readPct == 100;
                if (isPureReadFast32Uniform)
                {
                    localOps = RunReadOnlyUniformFast32(
                        bContext, ref rng, (uint)keyCount, ref pinnedInput, ref output,
                        ref mySlot, ref doneFlag);
                }
                else
                {
                    (localOps, hits, misses, deletesReinserted) = RunGeneralRumd(
                        bContext, ref rng, zipf, valueSpan, ref pinnedInput, ref output,
                        ref mySlot, ref doneFlag, keyCount, fast32,
                        Options.ReadPct, Options.UpsertPctCumulative, Options.RmwPctCumulative,
                        useZipf, deleteReinsert);
                }
            }

            // Final flush of the partial in-flight chunk so post-join sum is complete.
            Volatile.Write(ref mySlot.Value, localOps);

            var allocAfter = GC.GetAllocatedBytesForCurrentThread();
            stats.LocalOps = localOps;
            stats.Hits = hits;
            stats.Misses = misses;
            stats.DeletesReinserted = deletesReinserted;
            stats.AllocBytesDelta = allocAfter - allocBefore;
            stats.FinalExitTicks = Stopwatch.GetTimestamp();
        }

        // ====== Focused per-workload run loops (kept small so BasicContext.Read inlines) ======

        /// <summary>
        /// Hot loop for the canonical case: 100 % reads, uniform keys, key count fits in uint.
        /// Kept deliberately small so the JIT inlines BasicContext.Read into this body.
        /// </summary>
        static long RunReadOnlyUniformFast32(
            Tsavorite.core.BasicContext<KvKey, PinnedSpanByte, SpanByteAndMemory, Empty, KvSessionFunctions, KvStoreFunctions, KvAllocator> bContext,
            ref XoshiroRng rng, uint keyCount32,
            ref PinnedSpanByte pinnedInput, ref SpanByteAndMemory output,
            ref PaddedLong slot, ref bool doneFlag)
        {
            KvKey key = default;
            long localOps = 0;
            while (!Volatile.Read(ref doneFlag))
            {
                for (long i = 0; i < kChunkSize; i++)
                {
                    key.Value = rng.NextUInt32() % keyCount32;
                    bContext.Read(key, ref pinnedInput, ref output, Empty.Default);
                    ++localOps;
                }
                if ((localOps & 511) == 0)
                    bContext.CompletePending(false);
                Volatile.Write(ref slot.Value, localOps);
            }
            bContext.CompletePending(true);
            return localOps;
        }

        /// <summary>
        /// General-purpose RUMD hot loop. Larger than the fast path because it
        /// handles upsert / RMW / delete branches; ContextRead may not inline here.
        /// </summary>
        static (long localOps, long hits, long misses, long deletesReinserted) RunGeneralRumd(
            Tsavorite.core.BasicContext<KvKey, PinnedSpanByte, SpanByteAndMemory, Empty, KvSessionFunctions, KvStoreFunctions, KvAllocator> bContext,
            ref XoshiroRng rng, ZipfGenerator zipf,
            Span<byte> valueSpan, ref PinnedSpanByte pinnedInput, ref SpanByteAndMemory output,
            ref PaddedLong slot, ref bool doneFlag,
            long keyCount, bool fast32,
            int readPct, int upPct, int rmwPct,
            bool useZipf, bool deleteReinsert)
        {
            uint keyCount32 = fast32 ? (uint)keyCount : 0u;
            KvKey key = default;
            long localOps = 0, hits = 0, misses = 0, deletesReinserted = 0;
            while (!Volatile.Read(ref doneFlag))
            {
                for (long i = 0; i < kChunkSize; i++)
                {
                    long k;
                    if (useZipf)
                    {
                        k = zipf.Next(ref rng);
                        if (k >= keyCount) k = keyCount - 1;
                    }
                    else if (fast32)
                    {
                        k = rng.NextUInt32() % keyCount32;
                    }
                    else
                    {
                        k = (long)(rng.NextUInt64() % (ulong)keyCount);
                    }
                    key.Value = k;

                    int r = (int)(rng.NextUInt32() % 100u);
                    if (r < readPct)
                    {
                        bContext.Read(key, ref pinnedInput, ref output, Empty.Default);
                    }
                    else if (r < upPct)
                    {
                        bContext.Upsert(key, valueSpan, Empty.Default);
                    }
                    else if (r < rmwPct)
                    {
                        bContext.RMW(key, ref pinnedInput, Empty.Default);
                    }
                    else
                    {
                        bContext.Delete(key, Empty.Default);
                        if (deleteReinsert)
                        {
                            bContext.Upsert(key, valueSpan, Empty.Default);
                            ++deletesReinserted;
                            ++localOps;
                        }
                    }
                    ++localOps;
                }
                if ((localOps & 511) == 0)
                    bContext.CompletePending(false);
                Volatile.Write(ref slot.Value, localOps);
            }
            bContext.CompletePending(true);
            return (localOps, hits, misses, deletesReinserted);
        }

        // ====== Validation ======

        public unsafe (long mismatches, long readMisses) Validate()
        {
            using var session = store.NewSession<KvKey, PinnedSpanByte, SpanByteAndMemory, Empty, KvSessionFunctions>(functions);
            var bContext = session.BasicContext;

            byte* inputPtr = stackalloc byte[Options.ValueSize];
            byte* outputPtr = stackalloc byte[KvSessionFunctions.kReaderCopyBytes];
            var inputSpan = new Span<byte>(inputPtr, Options.ValueSize);
            var outputSpan = new Span<byte>(outputPtr, KvSessionFunctions.kReaderCopyBytes);
            var pinnedInput = PinnedSpanByte.FromPinnedSpan(inputSpan);
            var output = SpanByteAndMemory.FromPinnedSpan(outputSpan);

            long mismatches = 0;
            long misses = 0;
            KvKey key = default;
            int nCmp = Math.Min(KvSessionFunctions.kReaderCopyBytes, Options.ValueSize);

            for (long k = 0; k < Options.Keys; k++)
            {
                key.Value = k;
                int writerThread = (int)((double)k / Options.Keys * Options.Threads);
                if (writerThread >= Options.Threads) writerThread = Options.Threads - 1;

                var st = bContext.Read(key, ref pinnedInput, ref output, Empty.Default);
                if (!st.Found) { ++misses; continue; }
                for (int i = 0; i < nCmp; i++)
                {
                    var expected = (byte)((writerThread * 31 + i) & 0xFF);
                    if (outputPtr[i] != expected) { ++mismatches; break; }
                }
            }
            return (mismatches, misses);
        }

        // ====== Cleanup / utility ======

        public void Dispose()
        {
            try { store?.Dispose(); } catch { /* swallow */ }
            try { device?.Dispose(); } catch { /* swallow */ }
            try
            {
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

        // ====== Helpers ======

        static string ResolveDataPath(Options opts)
        {
            if (!string.IsNullOrWhiteSpace(opts.DataPath))
                return opts.DataPath;
            return Path.Combine(Path.GetTempPath(), "kv-benchmark");
        }

        static string CreateRunDir(string dataPath)
        {
            var ts = DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ", CultureInfo.InvariantCulture);
            var pid = Environment.ProcessId;
            var dir = Path.Combine(dataPath, $"kv-run-{ts}-{pid}");
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, ".kv-benchmark-owner"),
                $"pid={pid}\nstart_utc={ts}\n");
            return dir;
        }

        static void CleanStaleRunDirs(string dataPath)
        {
            try
            {
                foreach (var dir in Directory.EnumerateDirectories(dataPath, "kv-run-*"))
                {
                    var sentinel = Path.Combine(dir, ".kv-benchmark-owner");
                    if (!File.Exists(sentinel)) continue;
                    int? pid = TryReadOwnerPid(sentinel);
                    if (pid is null) continue;
                    if (IsPidAlive(pid.Value)) continue;
                    try { Directory.Delete(dir, recursive: true); } catch { /* skip */ }
                }
            }
            catch { /* best-effort */ }
        }

        static int? TryReadOwnerPid(string sentinel)
        {
            try
            {
                foreach (var line in File.ReadAllLines(sentinel))
                {
                    if (line.StartsWith("pid=", StringComparison.Ordinal) &&
                        int.TryParse(line.AsSpan(4), out var pid)) return pid;
                }
            }
            catch { /* ignored */ }
            return null;
        }

        static bool IsPidAlive(int pid)
        {
            try { _ = Process.GetProcessById(pid); return true; }
            catch { return false; }
        }

        static IDevice CreateDevice(Options opts, string logPath)
        {
            var devType = opts.ResolvedDeviceType;
            int numCt = opts.DeviceCompletionThreads > 0 ? opts.DeviceCompletionThreads : 1;
            IDevice dev;

            if (devType == DeviceType.Native && OperatingSystem.IsLinux())
            {
                dev = new NativeStorageDevice(logPath,
                    deleteOnClose: true,
                    disableFileBuffering: true,
                    numCompletionThreads: numCt,
                    ioBackend: opts.ResolvedIoBackend);
            }
            else if (devType == DeviceType.Null)
            {
                dev = Devices.CreateLogDevice(null, deviceType: DeviceType.Null);
            }
            else
            {
                dev = Devices.CreateLogDevice(logPath,
                    deviceType: devType,
                    preallocateFile: true,
                    deleteOnClose: true,
                    useIoCompletionPort: true,
                    disableFileBuffering: true);
            }

            if (opts.DeviceThrottle > 0)
                dev.ThrottleLimit = opts.DeviceThrottle;
            return dev;
        }
    }

    internal static class OptionsExtensions
    {
        public static bool RumdHasDeletes(this Options o)
        {
            using var en = o.Rumd.GetEnumerator();
            int idx = 0;
            while (en.MoveNext())
            {
                if (idx == 3) return en.Current > 0;
                idx++;
            }
            return false;
        }
    }
#pragma warning restore IDE0065
}
