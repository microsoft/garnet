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
        const long kChunkSize = 640;

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
            long reads = 0, writes = 0, deletes = 0;
            var allocBefore = GC.GetAllocatedBytesForCurrentThread();

            if (isLoad)
            {
                // Per-thread load buffer. Stackalloc inside isLoad branch.
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
                writes = localOps;
            }
            else
            {
                var deleteReinsert = Options.RumdHasDeletes();

                // Stackalloc OUTSIDE the hot-loop method; pass buffers as ref params (~3.7% faster than stackalloc-inside).
                byte* valuePtrD = stackalloc byte[Options.ValueSize];
                byte* inputPtrD = stackalloc byte[Options.ValueSize];
                byte* outputPtrD = stackalloc byte[KvSessionFunctions.kReaderCopyBytes];
                for (int i = 0; i < Options.ValueSize; i++)
                    valuePtrD[i] = (byte)((threadIdx * 31 + i) & 0xFF);
                for (int i = 0; i < Options.ValueSize; i++)
                    inputPtrD[i] = (byte)((threadIdx * 17 + i + 1) & 0xFF);
                var valueSpanD = new Span<byte>(valuePtrD, Options.ValueSize);
                var inputSpanD = new Span<byte>(inputPtrD, Options.ValueSize);
                var outputSpanD = new Span<byte>(outputPtrD, KvSessionFunctions.kReaderCopyBytes);
                var pinnedInputD = PinnedSpanByte.FromPinnedSpan(inputSpanD);
                var outputD = SpanByteAndMemory.FromPinnedSpan(outputSpanD);

                (localOps, reads, writes, deletes) = RunWorkload(
                    bContext, ref mySlot, ref doneFlag, threadIdx,
                    Options.Keys, Options.UseZipf,
                    valueSpanD, ref pinnedInputD, ref outputD,
                    Options.ReadPct, Options.UpsertPctCumulative, Options.RmwPctCumulative,
                    deleteReinsert,
                    seed: (uint)(Options.Seed) + (uint)(threadIdx + 1));
            }

            // Final flush of the partial in-flight chunk so post-join sum is complete.
            Volatile.Write(ref mySlot.Value, localOps);

            var allocAfter = GC.GetAllocatedBytesForCurrentThread();
            stats.LocalOps = localOps;
            stats.Reads = reads;
            stats.Writes = writes;
            stats.Deletes = deletes;
            stats.AllocBytesDelta = allocAfter - allocBefore;
            stats.FinalExitTicks = Stopwatch.GetTimestamp();
        }

        // ====== Per-thread RUN hot loop ======

        /// <summary>
        /// Per-thread RUN hot loop. Structure:
        /// <list type="bullet">
        ///   <item>Per-op inline xorshift32 key gen (bitmask for pow2 keyCount, Lemire fast-mod otherwise).</item>
        ///   <item>Per-op independent xorshift32 coin toss for op selection (read/upsert/RMW/delete).</item>
        ///   <item>Chained <c>if (r &lt; pct) { ... continue; }</c> branches.</item>
        ///   <item>Per-op done check in inner-loop condition.</item>
        ///   <item><c>CompletePending(false)</c> every 512 ops.</item>
        ///   <item>Per-chunk scoreboard tick for in-flight throughput sampling.</item>
        /// </list>
        /// Two independent RNG states are MANDATORY when distribution=zipf: zipf consumes
        /// its source RNG non-uniformly, so reusing it for op-select would bias the rumd ratio.
        /// </summary>
        long globalChunkIdx;


        // ===== buffers passed in by ref (stackalloc'd outside the method) =====
        unsafe (long localOps, long reads, long writes, long deletes) RunWorkload(
            Tsavorite.core.BasicContext<KvKey, PinnedSpanByte, SpanByteAndMemory, Empty, KvSessionFunctions, KvStoreFunctions, KvAllocator> bContext,
            ref PaddedLong slot, ref bool doneFlag, int threadIdx,
            long keyCount, bool useZipf,
            Span<byte> value, ref PinnedSpanByte pinnedInputSpan, ref SpanByteAndMemory _output,
            int readPercent, int upsertPercent, int rmwPercent,
            bool deleteReinsert,
            uint seed)
        {
            uint xk = seed == 0 ? 1u : seed;
            uint yk = 362436069u, zk = 521288629u, wk = 88675123u;
            uint xr = (seed == 0 ? 1u : seed) ^ 0x9E3779B9u;
            if (xr == 0) xr = 0x9E3779B9u;
            uint yr = 362436069u, zr = 521288629u, wr = 88675123u;

            var rngStruct = new XoshiroRng(((ulong)xk << 32) | wk);
            var zipf = useZipf ? new ZipfGenerator(ZipfConstants) : default;

            bool fast32 = keyCount > 0 && keyCount <= uint.MaxValue;
            uint keyCount32 = fast32 ? (uint)keyCount : 0u;
            bool keysPow2 = fast32 && (keyCount32 & (keyCount32 - 1)) == 0;
            uint keyMask32 = fast32 ? keyCount32 - 1 : 0u;

            // Pre-compute op-select cutoffs in the 32-bit RNG domain, so the per-op coin toss
            // is just `wr < cutoff` (no multiply, no divide). The cutoffs map
            // wr ∈ [0, 2^32) → action with probability pct/100.
            // For pct=100: cutoff = 2^32 (always greater than any uint wr), so check is always true.
            ulong readCutoff = ((ulong)(uint)readPercent << 32) / 100;
            ulong upsertCutoff = ((ulong)(uint)upsertPercent << 32) / 100;
            ulong rmwCutoff = ((ulong)(uint)rmwPercent << 32) / 100;

            long reads_done = 0, writes_done = 0, deletes_done = 0;
            KvKey key = default;  // Hoisted to avoid per-op 16-byte zero-init for the KvKey struct.

            while (!Volatile.Read(ref doneFlag))
            {
                long chunk_idx = Interlocked.Add(ref globalChunkIdx, kChunkSize) - kChunkSize;
                long chunk_end = chunk_idx + kChunkSize;
                // done is checked at chunk boundary only — saves a per-op volatile read + branch.
                // Worst-case stop latency: one chunk (~640 ops ≈ 0.3 ms at 2M ops/sec). Acceptable.
                for (long idx = chunk_idx; idx < chunk_end; ++idx)
                {
                    if (idx % 512 == 0)
                        bContext.CompletePending(false);
                    if (useZipf)
                    {
                        long kk = zipf.Next(ref rngStruct);
                        if (kk >= keyCount) kk = keyCount - 1;
                        key.Value = kk;
                    }
                    else if (fast32)
                    {
                        uint tk = xk ^ (xk << 11);
                        xk = yk; yk = zk; zk = wk;
                        wk = (wk ^ (wk >> 19)) ^ (tk ^ (tk >> 8));
                        key.Value = keysPow2
                            ? (long)(wk & keyMask32)
                            : (long)(uint)(((ulong)wk * keyCount32) >> 32);
                    }
                    else
                    {
                        uint t1 = xk ^ (xk << 11);
                        xk = yk; yk = zk; zk = wk;
                        ulong rhi = (wk = (wk ^ (wk >> 19)) ^ (t1 ^ (t1 >> 8)));
                        uint t2 = xk ^ (xk << 11);
                        xk = yk; yk = zk; zk = wk;
                        ulong rlo = (wk = (wk ^ (wk >> 19)) ^ (t2 ^ (t2 >> 8)));
                        key.Value = (long)(((rhi << 32) | rlo) % (ulong)keyCount);
                    }

                    uint tr = xr ^ (xr << 11);
                    xr = yr; yr = zr; zr = wr;
                    wr = (wr ^ (wr >> 19)) ^ (tr ^ (tr >> 8));
                    // No per-op multiply: compare wr directly against precomputed cutoffs.
                    ulong rcoin = wr;

                    if (rcoin < readCutoff)
                    {
                        bContext.Read(key, ref pinnedInputSpan, ref _output, Empty.Default);
                        ++reads_done;
                        continue;
                    }
                    if (rcoin < upsertCutoff)
                    {
                        bContext.Upsert(key, value, Empty.Default);
                        ++writes_done;
                        continue;
                    }
                    if (rcoin < rmwCutoff)
                    {
                        bContext.RMW(key, ref pinnedInputSpan, Empty.Default);
                        ++writes_done;
                        continue;
                    }
                    bContext.Delete(key, Empty.Default);
                    ++deletes_done;
                    if (deleteReinsert)
                    {
                        bContext.Upsert(key, value, Empty.Default);
                        ++writes_done;
                    }
                }

                // Live scoreboard tick per chunk — now includes reinserts (counted as writes).
                Volatile.Write(ref slot.Value, reads_done + writes_done + deletes_done);
            }
            bContext.CompletePending(true);
            long localOps = reads_done + writes_done + deletes_done;
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
