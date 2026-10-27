// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.Runtime.InteropServices;

namespace Tsavorite.epoch.litmus
{
    internal static class Program
    {
        static int Main(string[] args)
        {
            var seconds = 180;
            var buggy = false;
            for (var i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "--buggy":
                        buggy = true;
                        break;
                    case "--seconds":
                        if (++i >= args.Length || !int.TryParse(args[i], NumberStyles.Integer, CultureInfo.InvariantCulture, out seconds) || seconds <= 0)
                            return UsageError("--seconds requires a positive integer");
                        break;
                    case "-h":
                    case "--help":
                        Usage();
                        return 0;
                    default:
                        return UsageError($"unknown argument '{args[i]}'");
                }
            }

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
            Console.WriteLine($"  epoch: {(buggy ? "generated volatile-only release (--buggy)" : "checked-out production source")}");

            Console.WriteLine("-- detector control (5s): deliberately read drainCount before clearing the slot");
            var control = new DrainLitmus<ReorderedReleaseEpoch>(new ReorderedReleaseEpoch(), TimeSpan.FromSeconds(5), cores).Run();
            Console.WriteLine($"   {control}");
            if (control.Stranded == 0)
            {
                Console.Error.WriteLine("INCONCLUSIVE: forced reordering stranded no actions; detector validation failed.");
                return 2;
            }
            Console.WriteLine("   detector validated; control failures are intentional");

            Console.WriteLine($"-- stress ({seconds}s): no forced reordering");
            var duration = TimeSpan.FromSeconds(seconds);
            var result = buggy
                ? new DrainLitmus<VolatileReleaseEpoch>(new VolatileReleaseEpoch(), duration, cores).Run()
                : new DrainLitmus<ProductionEpoch>(new ProductionEpoch(), duration, cores).Run();
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

        static int UsageError(string message)
        {
            Console.Error.WriteLine($"error: {message}");
            Usage();
            return 64;
        }

        static void Usage() => Console.WriteLine(
            """
            LightEpoch drain-stranding reproduction.

            usage: LightEpochLitmus [--buggy] [--seconds N] [--help]
              --buggy      use a production-derived copy with Volatile.Write in Release
              --seconds N  stress duration (default 180); the race is probabilistic
              -h, --help   show usage

            The 5-second forced-reordering detector control always runs first.
            The stress run uses the checked-out production epoch unless --buggy is specified.
            Rebuild after changing commits; do not use --no-build.

            Exit codes: 0 clean sampled run, 1 stranded action, 2 inconclusive,
                        3 unsupported platform or emulation, 64 invalid arguments.
            """);
    }
}