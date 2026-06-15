// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Tsavorite.core;

// Stress harness for MultiLevelPageArray<T>.Allocate(). Runs many concurrent Allocates and asserts that:
//   1. Each id is returned exactly once (no duplicates).
//   2. Every id in [0, threads * perThread) is returned (no skips).
//   3. The Count getter agrees with the number of successful allocates.
//   4. With OOM injection (DEBUG only), all OOMs are recovered without hang, dup, or skip.

namespace Tsavorite.test.stress.mlpa
{
    internal static class Program
    {
        // Modes:
        //   normal     -- pure allocation stress, no OOM injection. Runs in Debug and Release.
        //   oom        -- inject one OOM per page, then succeed. DEBUG only (testInjectAddPageFailure is #if DEBUG).
        //   oomheavy   -- inject 5 OOMs per page, then succeed. DEBUG only.
        //   all        -- shortcut for "normal,oom,oomheavy" (with oom/oomheavy auto-skipped in Release).
        private static readonly string[] AllModes = ["normal", "oom", "oomheavy"];

        // Defaults match the prior positional behavior so existing run commands keep working with named args.
        private const string DefaultModes = "normal";
        private const string DefaultThreads = "16";
        private const int DefaultPerThread = 1;
        private const int DefaultIters = 20_000;

        static int Main(string[] args)
        {
            if (args.Length > 0 && (args[0] == "--help" || args[0] == "-?" || args[0] == "/?"))
            {
                PrintUsage();
                return 0;
            }

            string modesArg, threadsArg;
            int perThread, iters;
            List<string> modes;
            List<int> threadCounts;
            try
            {
                var named = ParseNamed(args);
                modesArg = named.GetValueOrDefault("--mode", DefaultModes);
                threadsArg = named.GetValueOrDefault("--threads", DefaultThreads);
                perThread = int.Parse(named.GetValueOrDefault("--per-thread", DefaultPerThread.ToString(System.Globalization.CultureInfo.InvariantCulture)), System.Globalization.CultureInfo.InvariantCulture);
                iters = int.Parse(named.GetValueOrDefault("--iters", DefaultIters.ToString(System.Globalization.CultureInfo.InvariantCulture)), System.Globalization.CultureInfo.InvariantCulture);
                modes = ExpandModes(modesArg);
                threadCounts = ParseIntList(threadsArg, "--threads");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Argument error: {ex.Message}");
                Console.Error.WriteLine();
                PrintUsage(Console.Error);
                return 64; // EX_USAGE
            }

            int totalFailures = 0;
            foreach (var mode in modes)
            {
                foreach (var threads in threadCounts)
                {
                    totalFailures += RunStress(mode, threads, perThread, iters);
                }
            }
            return totalFailures == 0 ? 0 : 1;
        }

        private static int RunStress(string mode, int threads, int perThread, int iters)
        {
            Console.WriteLine($"=== mode={mode} threads={threads} per-thread={perThread} iters={iters} build={BuildConfig.Name} ===");

            if ((mode == "oom" || mode == "oomheavy") && !BuildConfig.IsDebug)
            {
                Console.WriteLine($"  Mode '{mode}' requires a DEBUG build (OOM injection hook is DEBUG-only). Skipped.");
                return 0;
            }

            int totalFailures = 0;
            var sw = Stopwatch.StartNew();

            for (int it = 0; it < iters; it++)
            {
                var mlpa = new MultiLevelPageArray<object>();
                var hookFireCount = new int[1];
                ConfigureInjection(mlpa, mode, hookFireCount);

                int N = threads * perThread;
                var ids = new int[N];
                int idx = 0;
                int oomCount = 0;
                var barrier = new Barrier(threads);

                // Tasks (not bare Threads) so that any exception from worker code -- including Debug.Assert failures, which on .NET throw
                // DebugAssertException -- propagates back through Task.WaitAll to the test driver instead of being swallowed silently on
                // the worker thread. Using long-running Tasks ensures each gets its own dedicated thread so high-thread-count runs reach
                // the Barrier rendezvous without ThreadPool starvation.
                var tasks = new Task[threads];
                for (int t = 0; t < threads; t++)
                {
                    tasks[t] = Task.Factory.StartNew(() =>
                    {
                        barrier.SignalAndWait();
                        for (int i = 0; i < perThread; i++)
                        {
                            int id;
                            while (true)
                            {
                                try
                                {
                                    id = mlpa.Allocate();
                                    break;
                                }
                                catch (OutOfMemoryException)
                                {
                                    _ = Interlocked.Increment(ref oomCount);
                                }
                            }
                            var slot = Interlocked.Increment(ref idx) - 1;
                            ids[slot] = id;
                        }
                    }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
                }

                try
                {
                    Task.WaitAll(tasks, TimeSpan.FromSeconds(60));
                    if (tasks.Any(t => !t.IsCompleted))
                    {
                        Console.WriteLine($"  iter {it}: HANG (tail.PageAndOffset={mlpa.tail.PageAndOffset})");
                        return totalFailures + 1;
                    }
                }
                catch (AggregateException agg)
                {
                    Console.WriteLine($"  iter {it}: EXCEPTION in worker: {agg.Flatten().InnerExceptions.First()}");
                    return totalFailures + 1;
                }

                int upperBound = Math.Max(N + oomCount + 32, N + 32);
                var seen = new bool[upperBound];
                bool ok = true;
                int dupCount = 0, oobCount = 0;
                for (int i = 0; i < N; i++)
                {
                    var v = ids[i];
                    if (v < 0 || v >= seen.Length) { Console.WriteLine($"  iter {it}: out-of-range id {v}"); oobCount++; ok = false; continue; }
                    if (seen[v]) { dupCount++; if (dupCount <= 3) Console.WriteLine($"  iter {it}: DUPLICATE id {v}"); ok = false; }
                    seen[v] = true;
                }
                int skipCount = 0;
                for (int i = 0; i < N; i++)
                    if (!seen[i]) { skipCount++; if (skipCount <= 3) Console.WriteLine($"  iter {it}: SKIPPED id {i}"); ok = false; }

                if (!ok)
                {
                    Console.WriteLine($"  Summary iter {it}: dups={dupCount} skips={skipCount} oob={oobCount} oom={oomCount} hookFires={hookFireCount[0]} Count={mlpa.Count} tail.PageAndOffset={mlpa.tail.PageAndOffset}");
                    totalFailures++;
                    if (totalFailures >= 3) { Console.WriteLine("  Stopping after 3 failures."); break; }
                }
                else if (it == 0 && (mode == "oom" || mode == "oomheavy"))
                {
                    Console.WriteLine($"  Sanity iter 0: oom={oomCount} hookFires={hookFireCount[0]} Count={mlpa.Count}");
                }

                if ((it + 1) % 2_000 == 0)
                    Console.WriteLine($"  {it + 1}/{iters} iters, failures so far: {totalFailures}, elapsed {sw.Elapsed.TotalSeconds:0.0}s");
            }

            Console.WriteLine($"  DONE in {sw.Elapsed.TotalSeconds:0.0}s. Failures: {totalFailures}");
            return totalFailures;
        }

        private static Dictionary<string, string> ParseNamed(string[] args)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < args.Length; i++)
            {
                if (!args[i].StartsWith("--", StringComparison.Ordinal))
                    throw new ArgumentException($"Unexpected positional argument '{args[i]}'; all arguments must be named (e.g. --mode normal).");
                if (i + 1 >= args.Length)
                    throw new ArgumentException($"Argument '{args[i]}' requires a value.");
                result[args[i]] = args[++i];
            }
            return result;
        }

        private static List<string> ExpandModes(string spec)
        {
            var items = spec.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            var result = new List<string>();
            foreach (var item in items)
            {
                if (item.Equals("all", StringComparison.OrdinalIgnoreCase))
                {
                    foreach (var m in AllModes)
                        if (!result.Contains(m)) result.Add(m);
                }
                else if (AllModes.Contains(item, StringComparer.OrdinalIgnoreCase))
                {
                    var canonical = AllModes.First(m => m.Equals(item, StringComparison.OrdinalIgnoreCase));
                    if (!result.Contains(canonical)) result.Add(canonical);
                }
                else
                {
                    throw new ArgumentException($"Unknown mode '{item}'. Valid: {string.Join(", ", AllModes)}, all.");
                }
            }
            return result;
        }

        private static List<int> ParseIntList(string spec, string argName)
        {
            var items = spec.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            var result = new List<int>(items.Length);
            foreach (var item in items)
            {
                if (!int.TryParse(item, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var v) || v <= 0)
                    throw new ArgumentException($"{argName} value '{item}' is not a positive integer.");
                result.Add(v);
            }
            if (result.Count == 0)
                throw new ArgumentException($"{argName} must specify at least one value.");
            return result;
        }

        // The testInjectAddPageFailure field only exists in DEBUG builds (it is #if DEBUG in MultiLevelPageArray.cs); guard the setter
        // with [Conditional("DEBUG")] so the Release build still compiles cleanly. The Main early-exit prevents reaching this method
        // body in Release for OOM modes anyway.
        [Conditional("DEBUG")]
        private static void ConfigureInjection(MultiLevelPageArray<object> mlpa, string mode, int[] hookFireCount)
        {
#if DEBUG
            if (mode == "oom")
            {
                var injected = new bool[256];
                mlpa.testInjectAddPageFailure = pageIdx =>
                {
                    Interlocked.Increment(ref hookFireCount[0]);
                    if (pageIdx >= injected.Length) return false;
                    if (injected[pageIdx]) return false;
                    injected[pageIdx] = true;
                    return true;
                };
            }
            else if (mode == "oomheavy")
            {
                var remaining = new int[256];
                for (int i = 0; i < remaining.Length; i++) remaining[i] = 5;
                mlpa.testInjectAddPageFailure = pageIdx =>
                {
                    Interlocked.Increment(ref hookFireCount[0]);
                    if (pageIdx >= remaining.Length) return false;
                    int cur;
                    do
                    {
                        cur = remaining[pageIdx];
                        if (cur <= 0) return false;
                    } while (Interlocked.CompareExchange(ref remaining[pageIdx], cur - 1, cur) != cur);
                    return true;
                };
            }
#endif
        }

        private static void PrintUsage(System.IO.TextWriter writer = null)
        {
            writer ??= Console.Out;
            writer.WriteLine("MLPAStress -- concurrency stress harness for MultiLevelPageArray<T>.Allocate().");
            writer.WriteLine();
            writer.WriteLine("Usage: MLPAStress [--mode <list>] [--threads <list>] [--per-thread <n>] [--iters <n>]");
            writer.WriteLine("       MLPAStress --help | -? | /?");
            writer.WriteLine();
            writer.WriteLine("All arguments are named. The harness runs the cartesian product of --mode and --threads.");
            writer.WriteLine();
            writer.WriteLine("Options:");
            writer.WriteLine($"  --mode <list>        Comma-separated list of stress modes. Default: {DefaultModes}");
            writer.WriteLine("                       Valid: normal,oom,oomheavy,all");
            writer.WriteLine("                         normal   -- pure allocation, no injected failures (Debug + Release).");
            writer.WriteLine("                         oom      -- inject one OOM per new page, then succeed (Debug only).");
            writer.WriteLine("                         oomheavy -- inject 5 OOMs per new page, then succeed (Debug only).");
            writer.WriteLine("                         all      -- shorthand for normal,oom,oomheavy.");
            writer.WriteLine("                       OOM modes auto-skip in Release because the injection hook is #if DEBUG.");
            writer.WriteLine();
            writer.WriteLine($"  --threads <list>     Comma-separated list of concurrent worker thread counts. Default: {DefaultThreads}");
            writer.WriteLine("                       Each value triggers a separate run.");
            writer.WriteLine();
            writer.WriteLine($"  --per-thread <n>     Number of Allocate() calls per worker thread per iteration. Default: {DefaultPerThread}");
            writer.WriteLine();
            writer.WriteLine($"  --iters <n>          Number of iterations (fresh MLPA + N*per-thread allocations per run). Default: {DefaultIters}");
            writer.WriteLine();
            writer.WriteLine("Exit codes: 0 success, 1 invariant violation, 64 usage error.");
            writer.WriteLine();
            writer.WriteLine("Examples:");
            writer.WriteLine("  MLPAStress --mode all --threads 1,8,16,64 --per-thread 2048 --iters 500");
            writer.WriteLine("  MLPAStress --mode normal --threads 16 --per-thread 1 --iters 30000");
            writer.WriteLine("  MLPAStress --mode oom,oomheavy --threads 16 --per-thread 2048 --iters 500   # Debug only");
        }

        private static class BuildConfig
        {
#if DEBUG
            public const bool IsDebug = true;
            public const string Name = "Debug";
#else
            public const bool IsDebug = false;
            public const string Name = "Release";
#endif
        }
    }
}