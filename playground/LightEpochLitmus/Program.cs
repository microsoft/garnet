// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace Tsavorite.epoch.litmus
{
    internal static class Program
    {
        const int ExitPass = 0;
        const int ExitViolation = 1;
        const int ExitInconclusive = 2;
        const int ExitUnsupported = 3;
        const int ExitUsage = 64;

        static int Main(string[] args)
        {
            if (!TryParse(args, out var options, out var parseError))
            {
                if (parseError is not null)
                    Console.Error.WriteLine($"error: {parseError}");

                Usage();
                return parseError is null ? ExitPass : ExitUsage;
            }

            var emulated = Emulation.Detect();
            var epochName = options.Buggy ? "buggy" : "fixed";

            if (!Platform.IsSupported)
            {
                Console.Error.WriteLine($"unsupported: the harness needs Windows or Linux for page allocation and core pinning; this is {RuntimeInformation.OSDescription}");
                return Finish(options, new Report { Emulated = emulated.IsEmulated, EmulationEvidence = emulated.Evidence, Epoch = epochName }, ExitUnsupported, "unsupported-os");
            }

            if (!CoreLayout.TrySelect(options.Disturbers, out var cores))
            {
                Console.Error.WriteLine($"unsupported: the harness needs at least 4 logical processors to separate the reader from the reclaimer; this machine has {Environment.ProcessorCount}");
                return Finish(options, new Report { Emulated = emulated.IsEmulated, EmulationEvidence = emulated.Evidence, Epoch = epochName }, ExitUnsupported, "too-few-processors");
            }

            WriteHeader(options, cores, emulated);

            var report = new Report { Cores = cores.ToString(), Emulated = emulated.IsEmulated, EmulationEvidence = emulated.Evidence, Epoch = epochName };

            if (emulated.IsEmulated)
            {
                Console.Error.WriteLine($"unsupported: this process appears to be running under emulation ({emulated.Evidence}).");
                Console.Error.WriteLine("An emulator does not reproduce the host's memory ordering, so no result here says anything about the architecture being emulated.");
                Console.Error.WriteLine("Run on native hardware.");
                return Finish(options, report, ExitUnsupported, "emulated");
            }

            // The epoch under test is a generic type argument rather than an interface reference so
            // the harness JITs down to direct calls.
            QuarantineLitmusResult Run(int seconds, bool selfTest) => options.Buggy
                ? new QuarantineLitmus<BuggyEpoch>(new BuggyEpoch(), TimeSpan.FromSeconds(seconds), options.Deref, cores, selfTest).Run()
                : new QuarantineLitmus<FixedEpoch>(new FixedEpoch(), TimeSpan.FromSeconds(seconds), options.Deref, cores, selfTest).Run();


            if (!options.NoControl)
            {
                Console.WriteLine($"-- control: forcing the failure for {options.ControlSeconds}s to prove the detector is live");
                var control = Run(options.ControlSeconds, selfTest: true);
                Console.WriteLine($"   {control}");
                report.Control = Summary.From(control);

                if (control.SampledRounds == 0)
                {
                    Console.Error.WriteLine("INCONCLUSIVE: the control never captured a live page pointer, so the race window was never sampled");
                    return Finish(options, report, ExitInconclusive, "control-never-sampled");
                }

                if (control.Violations == 0)
                {
                    Console.Error.WriteLine("INCONCLUSIVE: THE DETECTOR IS BLIND. Pages were recycled under the reader on every round and nothing was reported, so any clean verdict from this build would be worthless.");
                    return Finish(options, report, ExitInconclusive, "detector-blind");
                }

                Console.WriteLine($"   detector is live ({control.Violations} violations observed under forced failure)");
            }

            if (options.SelfTestOnly)
            {
                Console.WriteLine("PASS: control only, main stress run skipped");
                return Finish(options, report, ExitPass, "control-only");
            }

            for (var i = 1; i <= options.Iterations; i++)
            {
                var label = options.Iterations == 1 ? "-- stress" : $"-- stress {i}/{options.Iterations}";
                Console.WriteLine($"{label}: {options.Seconds}s, deref={options.Deref} words");

                var result = Run(options.Seconds, selfTest: false);
                Console.WriteLine($"   {result}");
                report.Runs.Add(Summary.From(result));

                if (result.Violations > 0)
                {
                    Console.Error.WriteLine($"VIOLATION: a protected reader read a recycled page - use-after-free. {result}");
                    return Finish(options, report, ExitViolation, "violation");
                }
            }

            // A clean run only means something if it actually raced and actually reclaimed.
            var totals = report.Totals;
            if (totals.SampledRounds == 0)
            {
                Console.Error.WriteLine("INCONCLUSIVE: the reader never captured a live page pointer, so the race window was never sampled and this run proves nothing");
                return Finish(options, report, ExitInconclusive, "never-sampled");
            }

            if (totals.Quarantines == 0)
            {
                Console.Error.WriteLine("INCONCLUSIVE: the epoch never decided any page was safe to recycle, so this run could not have failed regardless of correctness");
                return Finish(options, report, ExitInconclusive, "never-reclaimed");
            }

            Console.WriteLine($"PASS: no violation in {totals.ElapsedSeconds:F1}s across {totals.SampledRounds:N0} sampled rounds and {totals.Quarantines:N0} reclamations");
            return Finish(options, report, ExitPass, "pass");
        }

        static int Finish(Options options, Report report, int exitCode, string verdict)
        {
            report.Verdict = verdict;
            report.ExitCode = exitCode;

            if (options.JsonPath is null)
                return exitCode;

            var json = JsonSerializer.Serialize(report.ToPayload(), new JsonSerializerOptions { WriteIndented = true });
            if (options.JsonPath == "-")
                Console.WriteLine(json);
            else
                File.WriteAllText(options.JsonPath, json);

            return exitCode;
        }

        static void WriteHeader(Options options, CoreLayout cores, Emulation.Result emulated)
        {
            Console.WriteLine($"LightEpochLitmus  {RuntimeInformation.FrameworkDescription}");
            Console.WriteLine($"  os        {RuntimeInformation.OSDescription}");
            Console.WriteLine($"  arch      {RuntimeInformation.ProcessArchitecture} ({Environment.ProcessorCount} logical processors)");
            Console.WriteLine($"  cores     {cores}");

            var distinctDisturberCores = new HashSet<int>(cores.DisturberCores).Count;
            if (cores.DisturberCores.Length > distinctDisturberCores)
                Console.WriteLine($"  WARNING: {cores.DisturberCores.Length} disturbers over {distinctDisturberCores} cores - oversubscribed, they will share processors");
            Console.WriteLine($"  stress    {options.Seconds}s x {options.Iterations}, deref={options.Deref}");
            Console.WriteLine($"  epoch     {(options.Buggy ? "BuggyLightEpoch (pre-fix - expected to FAIL)" : "LightEpoch (fixed)")}");

            if (emulated.IsEmulated)
                Console.WriteLine($"  EMULATION DETECTED: {emulated.Evidence} - memory-ordering results from this run are not evidence about the emulated architecture");

            Console.WriteLine();
        }

        static bool TryParse(string[] args, out Options options, out string error)
        {
            options = new Options();
            error = null;

            for (var i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "--seconds":
                        if (!TryInt(args, ref i, out options.Seconds, out error)) return false;
                        break;
                    case "--control-seconds":
                        if (!TryInt(args, ref i, out options.ControlSeconds, out error)) return false;
                        break;
                    case "--deref":
                        if (!TryInt(args, ref i, out options.Deref, out error)) return false;
                        break;
                    case "--iterations":
                        if (!TryInt(args, ref i, out options.Iterations, out error)) return false;
                        break;
                    case "--disturbers":
                        if (!TryInt(args, ref i, out options.Disturbers, out error)) return false;
                        break;
                    case "--json":
                        if (++i >= args.Length) { error = "--json needs a path, or - for stdout"; return false; }
                        options.JsonPath = args[i];
                        break;
                    case "--buggy":
                        options.Buggy = true;
                        break;
                    case "--self-test":
                        options.SelfTestOnly = true;
                        break;
                    case "--no-control":
                        options.NoControl = true;
                        break;
                    case "-h":
                    case "--help":
                        return false;
                    default:
                        error = $"unknown argument '{args[i]}'";
                        return false;
                }
            }

            if (options.Seconds <= 0 || options.ControlSeconds <= 0) { error = "durations must be positive"; return false; }
            if (options.Deref <= 0) { error = "--deref must be positive"; return false; }
            if (options.Iterations <= 0) { error = "--iterations must be positive"; return false; }
            if (options.Disturbers < 0) { error = "--disturbers cannot be negative"; return false; }
            if (options.SelfTestOnly && options.NoControl) { error = "--self-test and --no-control cancel each other out"; return false; }

            return true;
        }

        static bool TryInt(string[] args, ref int i, out int value, out string error)
        {
            value = 0;
            error = null;
            var name = args[i];

            if (++i >= args.Length) { error = $"{name} needs a value"; return false; }
            if (!int.TryParse(args[i], NumberStyles.Integer, CultureInfo.InvariantCulture, out value)) { error = $"{name} expects an integer, got '{args[i]}'"; return false; }

            return true;
        }

        static void Usage() => Console.WriteLine(
            """
            Store-Buffer quarantine litmus for LightEpoch.

            A reader announces its epoch and dereferences a page while a reclaimer retires that
            same page. The run asserts the epoch never authorises the free under the live reader.

            usage: LightEpochLitmus [options]

              --seconds N          main stress run duration, per iteration (default 30)
              --iterations N       repeat the stress run N times (default 1)
              --deref N            words the reader walks per protected region (default 20000)
              --disturbers N       threads that keep the epoch table's cache lines shared, which is
                                   what pins an announce in the store buffer long enough to matter
                                   (default 6, 0 to disable)
              --control-seconds N  duration of the forced-failure control (default 5)
              --self-test          run only the control, to check the detector fires here
              --no-control         skip the control (a clean result then proves much less)
              --buggy              run against BuggyLightEpoch (the pre-fix version) instead of the
                                   fixed one, to confirm the harness still catches the bug here
              --json PATH          write a machine-readable summary ('-' for stdout)
              -h, --help           this message

            By default the control runs first: it forces the failure and the run aborts unless the
            detector reports it. Without that, a clean run cannot be distinguished from a run
            that was never capable of failing.

            exit codes:
              0  pass          no violation, and the run demonstrably raced and reclaimed
              1  violation     a protected reader read a recycled page
              2  inconclusive  detector blind, race never sampled, nothing reclaimed, or emulated
              3  unsupported   wrong OS, or fewer than 4 logical processors
              64 usage error
            """);

        sealed class Options
        {
            internal int Seconds = 30;
            internal int ControlSeconds = 5;
            internal int Deref = 20_000;
            internal int Iterations = 1;
            internal int Disturbers = 6;
            internal bool SelfTestOnly;
            internal bool NoControl;
            internal bool Buggy;
            internal string JsonPath;
        }

        sealed class Summary
        {
            internal long Violations { get; init; }
            internal long SampledRounds { get; init; }
            internal long Rounds { get; init; }
            internal long Drains { get; init; }
            internal long Quarantines { get; init; }
            internal double ElapsedSeconds { get; init; }

            internal static Summary From(QuarantineLitmusResult r) => new()
            {
                Violations = r.Violations,
                SampledRounds = r.SampledRounds,
                Rounds = r.Rounds,
                Drains = r.Drains,
                Quarantines = r.Quarantines,
                ElapsedSeconds = r.Elapsed.TotalSeconds
            };

            internal object ToPayload() => new { violations = Violations, sampledRounds = SampledRounds, rounds = Rounds, drains = Drains, quarantines = Quarantines, elapsedSeconds = ElapsedSeconds };
        }

        sealed class Report
        {
            internal string Cores { get; init; }
            internal bool Emulated { get; init; }
            internal string EmulationEvidence { get; init; }
            internal string Epoch { get; init; }
            internal Summary Control { get; set; }
            internal List<Summary> Runs { get; } = [];
            internal string Verdict { get; set; }
            internal int ExitCode { get; set; }

            internal Summary Totals
            {
                get
                {
                    long violations = 0, sampled = 0, rounds = 0, drains = 0, quarantines = 0;
                    var elapsed = 0d;
                    foreach (var run in Runs)
                    {
                        violations += run.Violations;
                        sampled += run.SampledRounds;
                        rounds += run.Rounds;
                        drains += run.Drains;
                        quarantines += run.Quarantines;
                        elapsed += run.ElapsedSeconds;
                    }

                    return new Summary { Violations = violations, SampledRounds = sampled, Rounds = rounds, Drains = drains, Quarantines = quarantines, ElapsedSeconds = elapsed };
                }
            }

            internal object ToPayload() => new
            {
                tool = "LightEpochLitmus",
                epoch = Epoch,
                verdict = Verdict,
                exitCode = ExitCode,
                machine = new
                {
                    os = RuntimeInformation.OSDescription,
                    arch = RuntimeInformation.ProcessArchitecture.ToString(),
                    processors = Environment.ProcessorCount,
                    framework = RuntimeInformation.FrameworkDescription,
                    cores = Cores,
                    emulated = Emulated,
                    emulationEvidence = EmulationEvidence
                },
                control = Control?.ToPayload(),
                runs = Runs.ConvertAll(r => r.ToPayload()),
                totals = Totals.ToPayload()
            };
        }
    }
}