// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using CommandLine;

namespace Tsavorite.kvbench
{
    /// <summary>
    /// Real entry point. Parses options, runs warmup + load + iterations, emits
    /// results, handles SIGINT/SIGTERM idempotently, saves/restores ThreadPool.
    /// </summary>
    internal static class EntryPoint
    {
        static int _shutdownStarted; // for Interlocked.Exchange

        public static int Run(string[] args)
        {
            var parser = new Parser(s => { s.HelpWriter = Console.Out; s.CaseSensitive = false; });
            var parsed = parser.ParseArguments<Options>(args);
            if (parsed.Tag == ParserResultType.NotParsed)
                return 64;

            var opts = parsed.Value;
            var err = opts.Resolve();
            if (err != null)
            {
                Console.Error.WriteLine($"ERROR: {err}");
                return 64;
            }

            int oldMinW = 0, oldMinIO = 0;
            bool tunedThreadPool = false;
            if (!opts.NoThreadPoolTune)
            {
                ThreadPool.GetMinThreads(out oldMinW, out oldMinIO);
                int target = Math.Max(opts.Threads * 2, 256);
                ThreadPool.SetMinThreads(Math.Max(oldMinW, target), Math.Max(oldMinIO, target));
                tunedThreadPool = true;
            }

            KvBenchmark engine = null;
            var iterResults = new List<PhaseResult>();

            // Idempotent shutdown handlers (only fires once; the finally-path call is a no-op
            // unless an interrupt has set the guard first).
            void Shutdown(string reason)
            {
                if (Interlocked.Exchange(ref _shutdownStarted, 1) != 0) return;
                Console.Error.WriteLine($"[interrupt] reason={reason}");
                try { engine?.Dispose(); } catch { /* swallow */ }
                if (tunedThreadPool)
                {
                    try { ThreadPool.SetMinThreads(oldMinW, oldMinIO); } catch { /* swallow */ }
                }
            }

            Console.CancelKeyPress += (_, e) => { e.Cancel = true; Shutdown("sigint"); Environment.Exit(130); };
            AppDomain.CurrentDomain.ProcessExit += (_, _) => Shutdown("sigterm");

            try
            {
                engine = new KvBenchmark(opts);
                var output = new KvOutput(opts, engine.DataPath, args);
                output.EmitConfigHuman(engine.Pinning);

                // ---- Load phase ----
                var loadResult = engine.Load();
                output.EmitPhaseHuman(loadResult);
                output.EmitResultJson(loadResult, engine.Pinning);
                output.EmitResultCsv(loadResult, engine.Pinning);

                // ---- Optional --validate after load ----
                if (opts.Validate)
                {
                    Console.WriteLine("[validate] reading back all keys...");
                    var (mismatches, misses) = engine.Validate();
                    if (mismatches > 0 || misses > 0)
                    {
                        Console.Error.WriteLine($"[validate] FAILED: mismatches={mismatches} misses={misses}");
                        return 2;
                    }
                    Console.WriteLine("[validate] OK");
                }

                // ---- Run iterations ----
                for (int it = 1; it <= opts.Iterations; it++)
                {
                    var r = engine.RunIteration(it);
                    iterResults.Add(r);
                    output.EmitPhaseHuman(r);
                    output.EmitResultJson(r, engine.Pinning);
                    output.EmitResultCsv(r, engine.Pinning);
                }

                if (iterResults.Count > 0)
                {
                    output.EmitAggregateHuman(iterResults);
                    output.EmitAggregateJson(iterResults, engine.Pinning);
                    output.EmitAggregateCsv(iterResults, engine.Pinning);
                }

                return 0;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"FATAL: {ex.GetType().Name}: {ex.Message}");
                Console.Error.WriteLine(ex.StackTrace);
                return 1;
            }
            finally
            {
                // Suppress the ProcessExit-driven Shutdown emission for normal exits.
                Interlocked.Exchange(ref _shutdownStarted, 1);
                try { engine?.Dispose(); } catch { /* swallow */ }
                if (tunedThreadPool)
                {
                    try { ThreadPool.SetMinThreads(oldMinW, oldMinIO); } catch { /* swallow */ }
                }
            }
        }
    }
}
