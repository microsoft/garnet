// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.Runtime.InteropServices;

namespace Tsavorite.epoch.litmus
{
    internal static class Program
    {
        const int DefaultSeconds = 180;
        const long DefaultToyRounds = 20_000_000;

        static int Main(string[] args)
        {
            int? seconds = null;
            long? rounds = null;
            var buggy = false;
            var toy = false;
            for (var i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "--buggy":
                        buggy = true;
                        break;
                    case "--toy":
                        toy = true;
                        break;
                    case "--seconds":
                        if (++i >= args.Length || !int.TryParse(args[i], NumberStyles.None, CultureInfo.InvariantCulture, out var s) || s <= 0)
                            return UsageError("--seconds requires a positive integer");
                        seconds = s;
                        break;
                    case "--rounds":
                        if (++i >= args.Length || !RunLimit.TryParseCount(args[i], out var r))
                            return UsageError("--rounds requires a positive integer; '_' separators are allowed (10_000_000)");
                        rounds = r;
                        break;
                    case "-h":
                    case "--help":
                        Usage();
                        return 0;
                    default:
                        return UsageError($"unknown argument '{args[i]}'");
                }
            }
            if (toy && buggy)
                return UsageError("--toy and --buggy cannot be combined; the toy always runs the broken and fixed releases");
            if (seconds.HasValue && rounds.HasValue)
                return UsageError("--seconds and --rounds cannot be combined; choose one stop condition");

            var limit = rounds is { } n ? RunLimit.ForRounds(n)
                : seconds is { } sec ? RunLimit.ForDuration(TimeSpan.FromSeconds(sec))
                : toy ? RunLimit.ForRounds(DefaultToyRounds)
                : RunLimit.ForDuration(TimeSpan.FromSeconds(DefaultSeconds));

            var emulation = Emulation.Detect();
            if (!Platform.IsSupported || !CoreLayout.TrySelect(out var cores))
            {
                Console.Error.WriteLine("UNSUPPORTED: native Windows or Linux with at least four logical processors is required.");
                return 3;
            }
            if (emulation.IsEmulated)
            {
                Console.Error.WriteLine($"UNSUPPORTED: emulated execution does not reproduce native memory ordering ({emulation.Evidence}).");
                return 3;
            }

            Console.WriteLine($"LightEpoch drain litmus  {RuntimeInformation.FrameworkDescription}");
            Console.WriteLine($"  {RuntimeInformation.OSDescription}, {RuntimeInformation.ProcessArchitecture}, {Environment.ProcessorCount} logical processors");
            Console.WriteLine($"  cores: {cores}");
            if (toy)
                return RunToy(limit, cores);
            Console.WriteLine($"  epoch: {(buggy ? "generated volatile-only release (--buggy)" : "checked-out production source")}");

            Console.WriteLine("-- detector control (5s): deliberately read drainCount before clearing the slot");
            var control = new DrainLitmus<ReorderedReleaseEpoch>(new ReorderedReleaseEpoch(), RunLimit.ForDuration(TimeSpan.FromSeconds(5)), cores).Run();
            Console.WriteLine($"   {control}");
            if (control.Stranded == 0)
            {
                Console.Error.WriteLine("INCONCLUSIVE: forced reordering stranded no actions; detector validation failed.");
                return 2;
            }
            Console.WriteLine("   detector validated; control failures are intentional");

            Console.WriteLine($"-- stress ({limit}): no forced reordering");
            var result = buggy
                ? new DrainLitmus<VolatileReleaseEpoch>(new VolatileReleaseEpoch(), limit, cores).Run()
                : new DrainLitmus<ProductionEpoch>(new ProductionEpoch(), limit, cores).Run();
            Console.WriteLine($"   {result}");
            if (result.Stranded > 0)
            {
                Console.Error.WriteLine("VIOLATION: both threads suspended with an action pending; without more epoch activity it remains stranded.");
                return 1;
            }
            if (result.DrainedBySuspender == 0 || result.DrainedByBumper == 0)
            {
                Console.Error.WriteLine("INCONCLUSIVE: callbacks did not execute on both threads; the race was not adequately exercised.");
                return 2;
            }
            Console.WriteLine("PASS: no stranded actions observed. A finite clean run does not prove correctness.");
            return 0;
        }

        static int RunToy(RunLimit limit, CoreLayout cores)
        {
            Console.WriteLine($"-- toy ({limit} per release): suspender clears slot then reads drainCount; bumper increments drainCount then reads slot");
            var broken = new ToyLitmus(ToyRelease.Broken, limit, cores).Run();
            Console.WriteLine($"   {broken}");
            var exchange = new ToyLitmus(ToyRelease.FixedExchange, limit, cores).Run();
            Console.WriteLine($"   {exchange}");
            var barrier = new ToyLitmus(ToyRelease.FixedBarrier, limit, cores).Run();
            Console.WriteLine($"   {barrier}");

            if (exchange.Missed > 0 || barrier.Missed > 0)
            {
                Console.Error.WriteLine("VIOLATION: a fenced release missed a drain.");
                return 1;
            }
            if (broken.Missed == 0)
            {
                Console.Error.WriteLine("INCONCLUSIVE: the volatile-only release missed no drains; rerun longer or on an idle machine.");
                return 2;
            }
            Console.WriteLine("REPRODUCED: only the volatile-only release missed drains; both fences executed one per round.");
            return 0;
        }

        static int UsageError(string message)
        {
            Console.Error.WriteLine($"error: {message}");
            Usage();
            return 64;
        }

        static void Usage() => Console.WriteLine(
            """
            LightEpoch drain-stranding reproduction.

            usage: LightEpochLitmus [--buggy | --toy] [--seconds N | --rounds N] [--help]
              --buggy      use a production-derived copy with Volatile.Write in Release
              --toy        run the race without LightEpoch: the broken Volatile.Write release,
                           then Interlocked.Exchange, then Volatile.Write + Thread.MemoryBarrier
              --seconds N  stop after N seconds (default 180; with --toy, per release)
              --rounds N   stop after N rounds; '_' separators are allowed (10_000_000)
                           (default with --toy: 20_000_000 per release)
              -h, --help   show usage

            --seconds and --rounds cannot be combined. The 5-second forced-reordering
            detector control always runs first, except with --toy, where the broken
            release is the detector.
            The stress run uses the checked-out production epoch unless --buggy is specified.
            Rebuild after changing commits; do not use --no-build.

            Exit codes: 0 clean sampled run (with --toy: reproduced, fences clean),
                        1 stranded action (with --toy: a fenced release missed a drain),
                        2 inconclusive, 3 unsupported platform or emulation, 64 invalid arguments.
            """);
    }
}