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
        /// Builds the IDevice for the requested backend. Native + io_uring on Linux,
        /// the platform-default managed device elsewhere, or a no-op Null device when
        /// the entire dataset fits in the mutable log window.
        /// </summary>
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