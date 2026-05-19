// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.IO;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    /// <summary>
    /// Centralizes parsing/wiring of benchmark CLI flags into device + KVSettings + runtime tuning so
    /// individual benchmark drivers (SpanByte/Object/FixedLen) do not duplicate environment-variable
    /// lookups or device-creation logic.
    /// </summary>
    internal static class BenchmarkSetup
    {
        /// <summary>
        /// Parse a size string like "4mb", "64g", "256m", "1024" into bytes. Returns -1 on parse failure.
        /// Recognized suffixes (case-insensitive): k/kb, m/mb, g/gb, t/tb. No suffix means bytes.
        /// </summary>
        internal static long ParseSize(string s)
        {
            if (string.IsNullOrWhiteSpace(s))
                return -1;
            var span = s.Trim().ToLowerInvariant();
            long mult = 1;
            int suffixLen = 0;
            if (span.EndsWith("tb")) { mult = 1L << 40; suffixLen = 2; }
            else if (span.EndsWith("gb")) { mult = 1L << 30; suffixLen = 2; }
            else if (span.EndsWith("mb")) { mult = 1L << 20; suffixLen = 2; }
            else if (span.EndsWith("kb")) { mult = 1L << 10; suffixLen = 2; }
            else if (span.EndsWith('t')) { mult = 1L << 40; suffixLen = 1; }
            else if (span.EndsWith('g')) { mult = 1L << 30; suffixLen = 1; }
            else if (span.EndsWith('m')) { mult = 1L << 20; suffixLen = 1; }
            else if (span.EndsWith('k')) { mult = 1L << 10; suffixLen = 1; }
            var numeric = span.Substring(0, span.Length - suffixLen);
            if (!double.TryParse(numeric, NumberStyles.Float, CultureInfo.InvariantCulture, out var raw) || raw < 0)
                return -1;
            return (long)(raw * mult);
        }

        /// <summary>Format bytes back as a friendly string ("64MB", "32GB", etc.) for config dumps.</summary>
        internal static string FormatSize(long bytes)
        {
            if (bytes <= 0) return bytes.ToString();
            if ((bytes & ((1L << 40) - 1)) == 0) return (bytes >> 40) + "TB";
            if ((bytes & ((1L << 30) - 1)) == 0) return (bytes >> 30) + "GB";
            if ((bytes & ((1L << 20) - 1)) == 0) return (bytes >> 20) + "MB";
            if ((bytes & ((1L << 10) - 1)) == 0) return (bytes >> 10) + "KB";
            return bytes + "B";
        }

        /// <summary>Resolve the YCSB data directory: --data-path flag wins, else hard-coded default.</summary>
        internal static string ResolveDataPath(Options options)
        {
            if (!string.IsNullOrWhiteSpace(options.DataPath))
                return options.DataPath;
            return "D:/data/TsavoriteYcsbBenchmark";
        }

        /// <summary>Parse the device-type flag. Returns DeviceType.Default if unrecognized/empty.</summary>
        internal static DeviceType ResolveDeviceType(Options options)
        {
            if (string.IsNullOrWhiteSpace(options.Device))
                return DeviceType.Default;
            return options.Device.ToLowerInvariant() switch
            {
                "native" => DeviceType.Native,
                "randomaccess" => DeviceType.RandomAccess,
                "filestream" => DeviceType.FileStream,
                "null" => DeviceType.Null,
                "default" => DeviceType.Default,
                _ => DeviceType.Default,
            };
        }

        /// <summary>Parse the --device-io-backend flag. Recognises libaio/uring/default; falls back to Default.</summary>
        internal static NativeStorageDevice.IoBackend ParseIoBackend(string s)
        {
            if (string.IsNullOrWhiteSpace(s))
                return NativeStorageDevice.IoBackend.Default;
            return s.ToLowerInvariant() switch
            {
                "default" => NativeStorageDevice.IoBackend.Default,
                "libaio" => NativeStorageDevice.IoBackend.Libaio,
                "uring" or "io_uring" or "iouring" => NativeStorageDevice.IoBackend.Uring,
                _ => NativeStorageDevice.IoBackend.Default,
            };
        }

        /// <summary>
        /// One-shot pre-run setup: ensure the data directory exists, optionally clean old hlog files,
        /// optionally drop the OS page cache (Linux), and raise ThreadPool min if requested.
        /// Safe to call multiple times.
        /// </summary>
        internal static void PreRun(Options options, string dataPath)
        {
            if (!string.IsNullOrWhiteSpace(dataPath))
            {
                try { Directory.CreateDirectory(dataPath); } catch { /* ignored */ }
                if (options.CleanupDataFiles)
                {
                    try
                    {
                        foreach (var file in Directory.EnumerateFiles(dataPath, "hlog*"))
                        {
                            try { File.Delete(file); } catch { /* ignored */ }
                        }
                    }
                    catch { /* ignored */ }
                }
            }

            if (options.DropPageCache && OperatingSystem.IsLinux())
            {
                try
                {
                    // Linux only; requires root, fails silently otherwise.
                    File.WriteAllText("/proc/sys/vm/drop_caches", "3\n");
                }
                catch { /* ignored - usually means non-root */ }
            }

            if (options.ThreadPoolMin > 0)
            {
                ThreadPool.GetMinThreads(out var minW, out var minC);
                ThreadPool.SetMinThreads(Math.Max(minW, options.ThreadPoolMin), Math.Max(minC, options.ThreadPoolMin));
            }
        }

        /// <summary>
        /// Create the log device based on the --device flag, with throttle/completion-threads overrides
        /// and OS-cache toggle for the managed backends. Replaces the previous YCSB_DEVICE_TYPE /
        /// YCSB_THROTTLE / YCSB_NATIVE_CT env-var path.
        /// </summary>
        internal static IDevice CreateDevice(Options options, string devicePath, bool recoverMode)
        {
            var devType = ResolveDeviceType(options);
            IDevice device;

            if (devType == DeviceType.Native && OperatingSystem.IsLinux())
            {
                int numCT = options.DeviceCompletionThreads > 0 ? options.DeviceCompletionThreads : 1;
                var ioBackend = ParseIoBackend(options.DeviceIoBackend);
                device = new NativeStorageDevice(devicePath,
                    deleteOnClose: !recoverMode,
                    disableFileBuffering: !options.UseOsCache,
                    numCompletionThreads: numCT,
                    ioBackend: ioBackend);
            }
            else if (devType == DeviceType.Default || devType == DeviceType.RandomAccess || devType == DeviceType.FileStream || devType == DeviceType.Native || devType == DeviceType.Null)
            {
                device = Devices.CreateLogDevice(devicePath,
                    deviceType: devType,
                    preallocateFile: true,
                    deleteOnClose: !recoverMode,
                    useIoCompletionPort: true,
                    disableFileBuffering: !options.UseOsCache);
            }
            else
            {
                device = Devices.CreateLogDevice(devicePath, preallocateFile: true, deleteOnClose: !recoverMode, useIoCompletionPort: true);
            }

            if (options.DeviceThrottle > 0)
                device.ThrottleLimit = options.DeviceThrottle;

            return device;
        }

        /// <summary>
        /// Apply storage topology overrides (page-size, log-memory, segment-size, mutable-fraction, preallocate)
        /// onto a KVSettings instance. Caller seeds defaults first; this overlays explicit flag values.
        /// </summary>
        internal static void ApplyKVOverrides(Options options, KVSettings kvSettings)
        {
            if (!options.PreallocateLog)
                kvSettings.PreallocateLog = false;

            var pageBytes = ParseSize(options.PageSize);
            if (pageBytes > 0) kvSettings.PageSize = pageBytes;

            var logBytes = ParseSize(options.LogMemory);
            if (logBytes > 0) kvSettings.LogMemorySize = logBytes;

            var segBytes = ParseSize(options.SegmentSize);
            if (segBytes > 0) kvSettings.SegmentSize = segBytes;

            if (options.MutableFraction >= 0.0 && options.MutableFraction <= 1.0)
                kvSettings.MutableFraction = options.MutableFraction;
        }

        /// <summary>Write a single consolidated config block summarizing all effective settings.</summary>
        internal static void PrintConfig(Options options, string dataPath, IDevice device, long pageSize, long logMemSize, long segmentSize, double mutableFraction, bool preallocateLog)
        {
            if (!options.PrintConfig)
                return;
            Console.WriteLine("=== YCSB benchmark config ===");
            Console.WriteLine($"  threads          : {options.ThreadCount}");
            Console.WriteLine($"  benchmark        : {(BenchmarkType)options.Benchmark}");
            Console.WriteLine($"  allocator        : {(options.UseSBA ? "SpanByte" : "Object")}");
            Console.WriteLine($"  distribution     : {options.DistributionName.ToLower()}");
            Console.WriteLine($"  rumd%            : {string.Join(",", options.RumdPercents)}");
            Console.WriteLine($"  load-keys        : {(options.LoadKeyCount > 0 ? options.LoadKeyCount.ToString() : "default (250M / 4.6M sd)")}");
            Console.WriteLine($"  iterations       : {options.IterationCount}");
            Console.WriteLine($"  phase            : {options.Phase}");
            Console.WriteLine($"  runsec           : {options.RunSeconds}");
            Console.WriteLine($"  data-path        : {dataPath}");
            Console.WriteLine($"  device           : {ResolveDeviceType(options)} ({device?.GetType().Name ?? "n/a"})");
            Console.WriteLine($"  device-throttle  : {options.DeviceThrottle} ({(options.DeviceThrottle > 0 ? "applied" : "device default")})");
            Console.WriteLine($"  device-ct        : {options.DeviceCompletionThreads} (native only)");
            Console.WriteLine($"  device-io-backend: {ParseIoBackend(options.DeviceIoBackend)} (native only)");
            Console.WriteLine($"  use-os-cache     : {options.UseOsCache}");
            Console.WriteLine($"  page-size        : {FormatSize(pageSize)}");
            Console.WriteLine($"  log-memory       : {FormatSize(logMemSize)}");
            Console.WriteLine($"  segment-size     : {FormatSize(segmentSize)}");
            Console.WriteLine($"  mutable-fraction : {mutableFraction}");
            Console.WriteLine($"  preallocate-log  : {preallocateLog}");
            Console.WriteLine($"  threadpool-min   : {options.ThreadPoolMin}");
            Console.WriteLine($"  cleanup-data     : {options.CleanupDataFiles}");
            Console.WriteLine($"  drop-page-cache  : {options.DropPageCache}");
            Console.WriteLine($"  csv-output       : {(string.IsNullOrEmpty(options.CsvOutput) ? "(off)" : options.CsvOutput)}");
            Console.WriteLine("=============================");
        }

        /// <summary>Append a CSV row with the load-phase result.</summary>
        internal static void AppendCsvLoad(Options options, double insPerSec, long tailAddress)
        {
            if (string.IsNullOrEmpty(options.CsvOutput)) return;
            try
            {
                var newFile = !File.Exists(options.CsvOutput);
                using var w = new StreamWriter(options.CsvOutput, append: true);
                if (newFile)
                    w.WriteLine("phase,benchmark,threads,allocator,device,page_size,log_memory,mutable_fraction,ins_per_sec,ops_per_sec,tail_address,timestamp_utc");
                w.WriteLine(string.Join(',',
                    "load",
                    (BenchmarkType)options.Benchmark,
                    options.ThreadCount,
                    (options.UseSBA ? "sba" : "oa"),
                    ResolveDeviceType(options),
                    options.PageSize ?? "",
                    options.LogMemory ?? "",
                    options.MutableFraction.ToString(CultureInfo.InvariantCulture),
                    insPerSec.ToString("F2", CultureInfo.InvariantCulture),
                    "",
                    tailAddress,
                    DateTime.UtcNow.ToString("o")));
            }
            catch { /* best-effort */ }
        }

        /// <summary>Append a CSV row with a run-phase result.</summary>
        internal static void AppendCsvRun(Options options, double opsPerSec, long tailAddress)
        {
            if (string.IsNullOrEmpty(options.CsvOutput)) return;
            try
            {
                var newFile = !File.Exists(options.CsvOutput);
                using var w = new StreamWriter(options.CsvOutput, append: true);
                if (newFile)
                    w.WriteLine("phase,benchmark,threads,allocator,device,page_size,log_memory,mutable_fraction,ins_per_sec,ops_per_sec,tail_address,timestamp_utc");
                w.WriteLine(string.Join(',',
                    "run",
                    (BenchmarkType)options.Benchmark,
                    options.ThreadCount,
                    (options.UseSBA ? "sba" : "oa"),
                    ResolveDeviceType(options),
                    options.PageSize ?? "",
                    options.LogMemory ?? "",
                    options.MutableFraction.ToString(CultureInfo.InvariantCulture),
                    "",
                    opsPerSec.ToString("F2", CultureInfo.InvariantCulture),
                    tailAddress,
                    DateTime.UtcNow.ToString("o")));
            }
            catch { /* best-effort */ }
        }
    }
}