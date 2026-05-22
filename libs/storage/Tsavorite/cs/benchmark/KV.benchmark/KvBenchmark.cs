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

        long chunkCounter; // global chunk id for the run phase
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
            Volatile.Write(ref idx_, 0);
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

            using var session = store.NewSession<KvKey, PinnedSpanByte, SpanByteAndMemory, Empty, KvSessionFunctions>(functions);
            var bContext = session.BasicContext;

            ref var mySlot = ref scoreboard[threadIdx + 1];
            ref var doneFlag = ref doneBox[0].Value;

            // Pre-build per-thread txn key array (mirrors YCSB's `txn_keys_[]`).
            // 1M entries × 16B (KvKey size) = 16MB per thread — fits in L3.
            // YCSB's RunYcsbSafeContext cycles through the array by resetting idx_=0 when it hits txnKeysCount.
            // Allocated BEFORE the gate so it's outside the timed region.
            KvKey[] txn_keys = null;
            const int kTxnKeysCount = 1 << 20; // 1M
            if (!isLoad)
            {
                txn_keys = new KvKey[kTxnKeysCount];
                if (Options.UseZipf)
                {
                    var seedZ = new XoshiroRng(Options.Seed * 0x9E3779B97F4A7C15UL + (ulong)(threadIdx + 1));
                    var zipfLocal = new ZipfGenerator(ZipfConstants);
                    for (int j = 0; j < kTxnKeysCount; j++)
                    {
                        var kk = zipfLocal.Next(ref seedZ);
                        if (kk >= Options.Keys) kk = Options.Keys - 1;
                        txn_keys[j].Value = kk;
                    }
                }
                else
                {
                    // YCSB-style xorshift32 seed pattern.
                    uint x = (uint)(Options.Seed) + (uint)(threadIdx + 1);
                    if (x == 0) x = 1;
                    uint y = 362436069u, zz = 521288629u, w = 88675123u;
                    if (Options.Keys <= uint.MaxValue)
                    {
                        uint kc32 = (uint)Options.Keys;
                        for (int j = 0; j < kTxnKeysCount; j++)
                        {
                            uint t = x ^ (x << 11);
                            x = y; y = zz; zz = w;
                            w = (w ^ (w >> 19)) ^ (t ^ (t >> 8));
                            txn_keys[j].Value = w % kc32;
                        }
                    }
                    else
                    {
                        ulong kc64 = (ulong)Options.Keys;
                        for (int j = 0; j < kTxnKeysCount; j++)
                        {
                            uint t1 = x ^ (x << 11);
                            x = y; y = zz; zz = w;
                            ulong rhi = (w = (w ^ (w >> 19)) ^ (t1 ^ (t1 >> 8)));
                            uint t2 = x ^ (x << 11);
                            x = y; y = zz; zz = w;
                            ulong rlo = (w = (w ^ (w >> 19)) ^ (t2 ^ (t2 >> 8)));
                            txn_keys[j].Value = (long)(((rhi << 32) | rlo) % kc64);
                        }
                    }
                }
            }

            // Workload partition for load phase.
            long loadFrom = 0, loadTo = 0;
            if (isLoad)
            {
                loadFrom = (long)((double)Options.Keys * threadIdx / Options.Threads);
                loadTo = (long)((double)Options.Keys * (threadIdx + 1) / Options.Threads);
            }

            ready.Signal();
            gate.Wait();

            long localOps = 0;
            long hits = 0, misses = 0, deletesReinserted = 0;
            var allocBefore = GC.GetAllocatedBytesForCurrentThread();

            if (isLoad)
            {
                // Per-thread load buffer (matches YCSB Setup pattern). Stackalloc inside isLoad branch.
                byte* valuePtr = stackalloc byte[Options.ValueSize];
                for (int i = 0; i < Options.ValueSize; i++)
                    valuePtr[i] = (byte)((threadIdx * 31 + i) & 0xFF);
                var valueSpan = new Span<byte>(valuePtr, Options.ValueSize);

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
                var deleteReinsert = Options.RumdHasDeletes();

                // RunYcsbStyle: literal byte-equivalent of YCSB.benchmark.SpanByteYcsbBenchmark.RunYcsbSafeContext.
                // All hot-path buffers (value/input/output) are declared INSIDE the method so the JIT
                // treats them as stack locals (no extra deref per Read), matching YCSB exactly.
                (localOps, hits, misses, deletesReinserted) = RunYcsbStyle(
                    bContext, ref mySlot, ref doneFlag,
                    txn_keys, kTxnKeysCount, threadIdx,
                    Options.ValueSize,
                    Options.ReadPct, Options.UpsertPctCumulative, Options.RmwPctCumulative,
                    deleteReinsert,
                    seed: (uint)(Options.Seed) + (uint)(threadIdx + 1));
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

        // ====== Per-thread RUN hot loop (mirrors YCSB.benchmark RunYcsbSafeContext) ======

        /// <summary>
        /// Per-thread RUN hot loop that is structurally byte-for-byte equivalent to
        /// YCSB.benchmark's <c>SpanByteYcsbBenchmark.RunYcsbSafeContext</c> — see comments
        /// inline below correlating each block. This mirrored structure is critical:
        /// JIT-emits the same control-flow / branch density / inlining shape as YCSB's
        /// loop, so any single-thread perf delta isolates engine differences from
        /// benchmark harness differences.
        /// </summary>
        long idx_; // global chunk counter for the run phase (matches YCSB)

        unsafe (long localOps, long reads, long writes, long deletes) RunYcsbStyle(
            Tsavorite.core.BasicContext<KvKey, PinnedSpanByte, SpanByteAndMemory, Empty, KvSessionFunctions, KvStoreFunctions, KvAllocator> bContext,
            ref PaddedLong slot, ref bool doneFlag,
            KvKey[] txn_keys, long txnKeysCount, int threadIdx,
            int valueSize,
            int readPercent, int upsertPercent, int rmwPercent,
            bool deleteReinsert,
            uint seed)
        {
            // ===== Per-thread locals (declared INSIDE the method, like YCSB.RunYcsbSafeContext) =====
            // Stackalloc inside the method means the JIT treats input/output as stack slots
            // with no extra indirection per Read — matches YCSB's local-buffer pattern exactly.
            byte* valuePtr = stackalloc byte[valueSize];
            byte* inputPtr = stackalloc byte[valueSize];
            byte* outputPtr = stackalloc byte[KvSessionFunctions.kReaderCopyBytes];

            // Bake per-thread value/input patterns so writes aren't all zeros.
            for (int i = 0; i < valueSize; i++)
                valuePtr[i] = (byte)((threadIdx * 31 + i) & 0xFF);
            for (int i = 0; i < valueSize; i++)
                inputPtr[i] = (byte)((threadIdx * 17 + i + 1) & 0xFF);

            Span<byte> value = new(valuePtr, valueSize);
            Span<byte> input = new(inputPtr, valueSize);
            Span<byte> output_ = new(outputPtr, KvSessionFunctions.kReaderCopyBytes);

            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);
            SpanByteAndMemory _output = SpanByteAndMemory.FromPinnedSpan(output_);

            // Single xorshift32 instance — byte-for-byte identical to YCSB.RandomGenerator.Generate(uint max).
            uint x = seed == 0 ? 1u : seed;
            uint y = 362436069u, z = 521288629u, w = 88675123u;

            long reads_done = 0, writes_done = 0, deletes_done = 0;

            while (!Volatile.Read(ref doneFlag))
            {
                long chunk_idx = Interlocked.Add(ref idx_, kChunkSize) - kChunkSize;
                while (chunk_idx >= txnKeysCount)
                {
                    if (chunk_idx == txnKeysCount)
                        idx_ = 0;
                    chunk_idx = Interlocked.Add(ref idx_, kChunkSize) - kChunkSize;
                }

                for (long idx = chunk_idx; idx < chunk_idx + kChunkSize && !Volatile.Read(ref doneFlag); ++idx)
                {
                    if (idx % 512 == 0)
                    {
                        bContext.CompletePending(false);
                    }

                    var key = txn_keys[idx];

                    uint t = x ^ (x << 11);
                    x = y; y = z; z = w;
                    w = (w ^ (w >> 19)) ^ (t ^ (t >> 8));
                    int r = (int)(w % 100u);

                    if (r < readPercent)
                    {
                        bContext.Read(key, ref pinnedInputSpan, ref _output, Empty.Default);
                        ++reads_done;
                        continue;
                    }
                    if (r < upsertPercent)
                    {
                        bContext.Upsert(key, value, Empty.Default);
                        ++writes_done;
                        continue;
                    }
                    if (r < rmwPercent)
                    {
                        bContext.RMW(key, ref pinnedInputSpan, Empty.Default);
                        ++writes_done;
                        continue;
                    }
                    bContext.Delete(key, Empty.Default);
                    if (deleteReinsert)
                        bContext.Upsert(key, value, Empty.Default);
                    ++deletes_done;
                }

                // Live scoreboard tick per chunk (cheap; supports SumScoreboard in-flight throughput sampler).
                Volatile.Write(ref slot.Value, reads_done + writes_done + deletes_done);
            }
            bContext.CompletePending(true);
            long localOps = reads_done + writes_done + deletes_done + (deleteReinsert ? deletes_done : 0);
            return (localOps, reads_done, writes_done, deletes_done);
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
