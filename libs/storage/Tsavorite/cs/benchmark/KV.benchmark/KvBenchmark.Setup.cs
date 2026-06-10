// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using Tsavorite.core;

namespace Tsavorite.kvbench
{
    // Per-process setup helpers: data-path resolution, per-run directory creation,
    // stale-run cleanup (so abandoned runs from killed processes are reclaimed on
    // next start), and device construction (native vs managed vs null).
    public sealed partial class KvBenchmark
    {
        const string OwnerSentinelFileName = ".kv-benchmark-owner";
        const string RunDirPrefix = "kv-run-";

        /// <summary>Resolves --data-path or falls back to the OS temp dir.</summary>
        static string ResolveDataPath(Options opts)
            => !string.IsNullOrWhiteSpace(opts.DataPath)
                ? opts.DataPath
                : Path.Combine(Path.GetTempPath(), "kv-benchmark");

        /// <summary>Creates a unique kv-run-{ts}-{pid} subdir and tags it with an owner sentinel.</summary>
        static string CreateRunDir(string dataPath)
        {
            var ts = DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ", CultureInfo.InvariantCulture);
            var pid = Environment.ProcessId;
            var dir = Path.Combine(dataPath, $"{RunDirPrefix}{ts}-{pid}");
            Directory.CreateDirectory(dir);
            File.WriteAllText(Path.Combine(dir, OwnerSentinelFileName), $"pid={pid}\nstart_utc={ts}\n");
            return dir;
        }

        /// <summary>Deletes kv-run-* directories owned by PIDs that are no longer alive.</summary>
        static void CleanStaleRunDirs(string dataPath)
        {
            try
            {
                foreach (var dir in Directory.EnumerateDirectories(dataPath, $"{RunDirPrefix}*"))
                {
                    var sentinel = Path.Combine(dir, OwnerSentinelFileName);
                    if (!File.Exists(sentinel)) continue;
                    int? pid = TryReadOwnerPid(sentinel);
                    if (pid is null || IsPidAlive(pid.Value)) continue;
                    try { Directory.Delete(dir, recursive: true); } catch { /* skip locked */ }
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

        /// <summary>
        /// Builds the IDevice for the requested backend. Native (libaio) on Linux,
        /// the platform-default managed device elsewhere, or a no-op Null device when
        /// the entire dataset fits in the mutable log window.
        /// </summary>
        static IDevice CreateDevice(Options opts, string logPath)
        {
            var devType = opts.ResolvedDeviceType;
            int numCt = opts.DeviceCompletionThreads > 0 ? opts.DeviceCompletionThreads : 1;
            IDevice dev;

            // LocalMemory: RAM-backed device modelling a syscall-free "fast IO" backend, used to
            // measure the upper bound of Tsavorite throughput. Capacity is sized to fit the
            // expected log tail (Keys * (8B key + ValueSize + 16B header)) rounded up to a
            // multiple of segment size, plus 4 extra segments for in-flight slack.
            if (devType == DeviceType.LocalMemory)
            {
                long segSize = opts.ResolvedSegmentSizeBytes;
                long perRecord = 8 + opts.ValueSize + 16;
                long approxBytes = checked(opts.Keys * perRecord);
                long needSegments = (approxBytes + segSize - 1) / segSize + 4;
                long capacity = needSegments * segSize;

                // Inline completion => parallelism 0 (no completion threads/rings). Otherwise the configured
                // count, or ProcessorCount when unset.
                int parallelism = opts.DeviceInlineCompletion
                    ? 0
                    : (opts.DeviceCompletionThreads > 0 ? opts.DeviceCompletionThreads : System.Environment.ProcessorCount);

                System.Console.WriteLine(
                    $"[localmemory] capacity={capacity / (1024L * 1024 * 1024)}GB segments={needSegments} segSize={segSize / (1024L * 1024)}MB parallelism(=device-completion-threads)={parallelism}{(opts.DeviceInlineCompletion ? " (inline completion)" : "")}");

                // LocalMemoryDevice has no Throttle() override; its per-ring SPSC backpressure (the producer
                // blocks when its ring is full) IS the in-flight bound. So map --device-throttle onto the ring
                // capacity (rounded up to a power of two) — this actually caps in-flight, with no contended
                // device-wide numPending counter. 0 = device default ring.
                int localMemRing = 0;
                if (opts.DeviceThrottle > 0)
                {
                    // Round up to a power of two, overflow-safe. --device-throttle is user-supplied, so a
                    // naive `while (r < throttle) r <<= 1` would overflow to a negative r and spin forever
                    // for values > 2^30; cap at the largest power-of-two int instead.
                    const int MaxRing = 1 << 30;
                    if (opts.DeviceThrottle >= MaxRing)
                        localMemRing = MaxRing;
                    else
                    {
                        localMemRing = 1;
                        while (localMemRing < opts.DeviceThrottle)
                            localMemRing <<= 1;
                    }
                }

                dev = Devices.CreateLogDevice(
                    logPath: null,
                    deviceType: DeviceType.LocalMemory,
                    capacity: capacity,
                    numCompletionThreads: parallelism,
                    localMemorySegmentSize: segSize,
                    localMemoryRingCapacity: localMemRing);
                if (opts.DeviceThrottle > 0) dev.ThrottleLimit = opts.DeviceThrottle;   // reflect intent in reporting (LocalMemory enforces it via the ring)
                return dev;
            }

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
        /// <summary>True when the rumd tuple has a non-zero delete fraction.</summary>
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
}