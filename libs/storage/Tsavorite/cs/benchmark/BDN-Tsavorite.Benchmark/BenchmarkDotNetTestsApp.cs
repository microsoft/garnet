// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Diagnostics.Windows;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using Perfolizer.Metrology;

namespace BenchmarkDotNetTests
{
    // Driver class for BDN testing
    public class BenchmarkDotNetTestsApp
    {
        public static string TestDirectory => Path.Combine(Path.GetDirectoryName(typeof(BenchmarkDotNetTestsApp).Assembly.Location), "Tests");

        // Number of records to use; can be configurd by cmdline args. It's here because it's common among more than one test.
        public static int NumRecords = 1_000_000;

        const string ExeName = "BDN-Tsavorite.benchmark.exe";
        const string InliningDiag = "inliningDiag";
        const string MemoryDiag = "memoryDiag";
        const string HwCounters = "hwCounters";

        static void Usage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine($"To run BDN, either run 'dotnet run -c Release -f net10.0 --' (that trailing -- is needed) in the project dir, or {ExeName} directly:");
            Console.WriteLine($"    {ExeName} [[<BDN Options (see below)] [Test Options (see below)] --] <BDN options, e.g. --filter *{nameof(EpochTests)}*>");
            Console.WriteLine("  If BDN or Test Options are present the '--' separator is required (and may be the second one if running with dotnet); otherwise it may be omitted");
            Console.WriteLine("To debug a test directly (without waiting for BDN to generate the exes):");
            Console.WriteLine($"    {ExeName} [Test Options (see below)] --debug <Test Name (see below)>");
            Console.WriteLine("For this usage message:");
            Console.WriteLine($"    {ExeName} -? or /? or --help");
            Console.WriteLine("");
            Console.WriteLine("BDN options (these set options on the BDN config; preceded by --):");
            Console.WriteLine($"  --{InliningDiag}: Adds the inlining diagnoser to the run");
            Console.WriteLine($"  --{MemoryDiag}: Adds the memory diagnoser to the run");
            Console.WriteLine($"  --{HwCounters}: Adds the hardware counters to the run");
            Console.WriteLine("Test Options (these set parameters on the individual tests; preceded by --):");
            Console.WriteLine($"  --{nameof(NumRecords)} <num>: Sets the number of records to operate on for {nameof(OperationTests)} or {nameof(IterationTests)} (default: {NumRecords})");
            Console.WriteLine($"  --{nameof(IterationTests.NumRecords)} <num>: Sets whether {nameof(IterationTests)}.{nameof(IterationTests.FlushAndEvict)} is set (default: false");
            Console.WriteLine($"Test Name: run {ExeName} --list tree and then pass the unqualified name of the test you want, e.g. {nameof(EpochTests.ResumeSuspend)}:");

            // Syntax to run BDN:
            //  <exeName> <options for both debugging and release run> <--> <options for BDN>
            // Syntax to debug a test without waiting for BDN to generate the .exes:
            //  <exeName> <options for both debugging and release run> debug <testname>
            // For usage:
            //  <exeName> -? or /? or --help
        }

        public static void Main(string[] args)
        {
            var arg = args?[0].ToLower() ?? null;
            if (arg is null || arg == "-?" || arg == "/?" || arg == "--help")
            {
                Usage();
                return;
            }

            bool inliningDiag = false, memoryDiag = false, hwCounters = false;
            string[] bdnArgs = [];

            // Check for debugging a test
            for (var ii = 0; ii < args.Length; ii++)
            {
                arg = args[ii].ToLower();

                // If we are at the "run BDN" separator, then config options are set; break out and run it.
                if (arg == "--")
                {
                    if (ii + 1 < args.Length)
                        bdnArgs = [.. args.Skip(ii + 1)];
                    break;
                }

                // If we are at the "debug the code" separator, then config options are set; debug it and we're done.
                if (arg == "--debug")
                {
                    ii++;
                    if (ii >= args.Length)
                        throw new ArgumentException($"'{arg}' option must be followed by the name of the test to debug");
                    var testName = args[ii].ToLower();
                    if (testName == nameof(IterationTests.Cursor).ToLower())
                    {
                        var test = new IterationTests();
                        test.SetupPopulatedStore();
                        test.Cursor();
                        test.TearDown();
                        return;
                    }
                    if (testName == nameof(OperationTests.Read).ToLower() || testName == nameof(OperationTests.Upsert).ToLower() || testName == nameof(OperationTests.RMW).ToLower())
                    {
                        var tester = new OperationTests();
                        tester.SetupPopulatedStore();
                        if (testName == nameof(OperationTests.Read).ToLower())
                            tester.Read();
                        else if (testName == nameof(OperationTests.Upsert).ToLower())
                            tester.Upsert();
                        else if (testName == nameof(OperationTests.RMW).ToLower())
                            tester.RMW();
                        else if (testName == "all")
                        {
                            tester.Read();
                            tester.Upsert();
                            tester.RMW();
                        }
                        else
                            throw new ArgumentException($"Unknown {nameof(OperationTests)} test: {args[1]}");
                        return;
                    }
                    throw new ArgumentException($"unknown test name '{testName}");
                }

                // BDN options parsing
                if (arg == $"--{InliningDiag.ToLower()}")
                {
                    inliningDiag = true;
                    continue;
                }
                if (arg == $"--{MemoryDiag.ToLower()}")
                {
                    memoryDiag = true;
                    continue;
                }
                if (arg == $"--{HwCounters.ToLower()}")
                {
                    hwCounters = true;
                    continue;
                }

                // Test options parsing
                if (arg == $"--{nameof(NumRecords).ToLower()}")
                {
                    ii++;
                    if (ii >= args.Length)
                        throw new ArgumentException($"'{arg}' option must be followed by the value");
                    NumRecords = int.Parse(args[ii]);
                    continue;
                }
                if (arg == $"--{nameof(IterationTests.FlushAndEvict).ToLower()}")
                {
                    ii++;
                    if (ii >= args.Length)
                        throw new ArgumentException($"'{arg}' option must be followed by the value");
                    IterationTests.FlushAndEvictConfig = bool.Parse(args[ii]);
                    continue;
                }

                // Assume we should pass this through to BDN itself; otherwise if we don't have test or BDN-config options,
                // we would need to have two -- -- separators which would be ugly.
                if (ii < args.Length)
                    bdnArgs = [.. args.Skip(ii)];
                break;
            }

#if DEBUG
            if (inliningDiag || memoryDiag || hwCounters)
                Console.WriteLine("Warning: Diagnostics options are ignored in debug runs");
            var config = new DebugInProcessConfig();
#else
            var config = new ReleaseConfig(inliningDiag, memoryDiag, hwCounters);
#endif
            BenchmarkSwitcher.FromAssembly(typeof(BenchmarkDotNetTestsApp).Assembly).Run(bdnArgs, config);
        }
    }

    public class ReleaseConfig : ManualConfig
    {
        public ReleaseConfig(bool inliningDiag, bool memoryDiag, bool hardwareCounters)
        {
            _ = AddLogger(ConsoleLogger.Default);
            _ = AddExporter(DefaultExporters.Markdown);
            _ = AddColumnProvider(DefaultColumnProviders.Instance);
            _ = WithSummaryStyle(SummaryStyle.Default.WithSizeUnit(SizeUnit.B));

            var baseJob = Job.Default;

            var net10Job = baseJob
                .WithRuntime(CoreRuntime.Core10_0)
                .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"));

            if (inliningDiag)
            {
                // If there is no inliningDiagnoser output, it may be necessary to turn off TieredCompilation; see https://github.com/dotnet/BenchmarkDotNet/issues/1791
                //net10Job = net10Job.WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredCompilation", "0"));
                _ = AddDiagnoser(new InliningDiagnoser(logFailuresOnly: true, allowedNamespaces: ["Tsavorite.core"]));
            }
            if (memoryDiag)
                _ = AddDiagnoser(MemoryDiagnoser.Default);
            if (hardwareCounters)
                _ = AddHardwareCounters(HardwareCounter.CacheMisses, HardwareCounter.BranchMispredictions);

            _ = AddJob(net10Job.WithId(".NET 10"));
        }
    }
}