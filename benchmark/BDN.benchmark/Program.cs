// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using Perfolizer.Metrology;

class Program
{
    internal static string bdnFramework = string.Empty;
    internal static string bdnOpParam = string.Empty;

    static void Main(string[] args)
    {
        // Extract our custom CLI options before forwarding remaining args to BDN.
        // First check for a usage request, so we don't have to read the .cs file to see the options.
        var passthroughArgs = args;
        if (args.Length > 0)
        {
            var arg1 = args[0].ToLower();
            if (arg1 == "--help" || arg1 == "/?" || arg1 == "-?")
            {
                Console.WriteLine("Garnet: Usage: dotnet run [dotnet options] -- [BDN options] [--frameworks <net8.0|net10.0|all>] [--opparams <none|acl|aof|aad,all>]");
                Console.WriteLine("Garnet: ");
                Console.WriteLine("Garnet: Example: dotnet run -f net10.0 -c Release -- --fw net10.0 --op none -f *RawStringOperations*");
                Console.WriteLine("Garnet: ");
                Console.WriteLine("Garnet: Custom options may appear anywhere in the BDN portion (except 'help' variants which must be first):");
                Console.WriteLine("Garnet:   --frameworks or --fw:  Filter benchmarks to specific framework(s). Comma-separated list of net8.0, net10.0, or all. Default is all.");
                Console.WriteLine("Garnet:   --opparams or --op:    Filter Operations benchmarks by which params to include. Comma-separated list of none, acl, aof, aad, all. Default is all.");
                Console.WriteLine("Garnet:   --help or /? or -?:    (Must be first) Display this help message, followed by BDN help (filter with 'findstr Garnet:' if that is not desired).");

                // Drop through without stripping the help request, so BDN's help will also display. The "Garnet:" prefix makes it possible to findstr on that.
            }
            passthroughArgs = ConsumeCustomArgs(args);
        }

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly)
#if DEBUG
            .Run(passthroughArgs, new DebugInProcessConfig()
                .WithOrderer(new BDN.benchmark.NamespaceTypeOrderer())
                .AddColumn(CategoriesColumn.Default));
#else
            .Run(passthroughArgs, new BaseConfig());
#endif

        string[] ConsumeCustomArgs(string[] args)
        {
            var remaining = new List<string>(args.Length);
            for (var i = 0; i < args.Length; i++)
            {
                var arg = args[i];
                if (arg == "--frameworks" || arg == "--fw")
                {
                    if (i + 1 >= args.Length)
                        throw new ApplicationException($"{arg} requires a value");
                    bdnFramework = args[++i].ToLower();
                    continue;
                }
                if (arg == "--opparams" || arg == "--op")
                {
                    if (i + 1 >= args.Length)
                        throw new ApplicationException($"{arg} requires a value");
                    bdnOpParam = args[++i].ToLower();
                    continue;
                }
                remaining.Add(arg);
            }
            return remaining.ToArray();
        }
    }
}

public class BaseConfig : ManualConfig
{
    public Job Net8BaseJob { get; }
    public Job Net10BaseJob { get; }

    public BaseConfig()
    {
        _ = AddLogger(ConsoleLogger.Default);
        _ = AddExporter(DefaultExporters.Markdown);
        _ = AddColumnProvider(DefaultColumnProviders.Instance);
        _ = WithSummaryStyle(SummaryStyle.Default.WithSizeUnit(SizeUnit.B));

        // Order the results table by namespace, then type, then the normal within-type ordering (categories, etc.).
        // Show the per-benchmark categories so the grouping is visible in the table.
        _ = WithOrderer(new BDN.benchmark.NamespaceTypeOrderer());
        _ = AddColumn(CategoriesColumn.Default);

        var baseJob = Job.Default.WithGcServer(true);

        Net8BaseJob = baseJob
            .WithRuntime(CoreRuntime.Core80)
            .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"));
        Net10BaseJob = baseJob
            .WithRuntime(CoreRuntime.Core10_0)
            .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"));

        bool net8 = true, net10 = true;
        if (!string.IsNullOrEmpty(Program.bdnFramework))
        {
            var fws = Program.bdnFramework.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(p => p.ToLower()).ToArray();
            if (fws.Length == 0)
                throw new ApplicationException("--frameworks must specify at least one value (e.g. net8.0, net10.0, all)");
            net8 = net10 = false;
            foreach (var fw in fws)
            {
                _ = fw switch
                {
                    "net8.0" => net8 = true,
                    "net10.0" => net10 = true,
                    "all" => net8 = net10 = true,
                    _ => throw new ApplicationException($"Unrecognized bdnFramework value: {fw}"),
                };
            }
        }

        _ = (net8, net10) switch
        {
            (true, false) => AddJob(Net8BaseJob.WithId(".NET 8")),
            (false, true) => AddJob(Net10BaseJob.WithId(".NET 10")),
            (true, true) => AddJob(Net8BaseJob.WithId(".NET 8"), Net10BaseJob.WithId(".NET 10")),
            _ => throw new ApplicationException($"Should never encounter a situation where all frameworks are excluded"),
        };

        if (!string.IsNullOrEmpty(Program.bdnOpParam))
        {
            BDN.benchmark.Operations.OperationsBase.SetAllParams(false);
            var ops = Program.bdnOpParam.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(p => p.ToLower()).ToArray();
            if (ops.Length == 0)
                throw new ApplicationException("--opparams must specify at least one value (e.g. none, acl, aof, aad, all)");
            foreach (var op in ops)
            {
                switch (op)
                {
                    case "none":
                        BDN.benchmark.Operations.OperationsBase.ParamsNone = true;
                        break;
                    case "acl":
                        BDN.benchmark.Operations.OperationsBase.ParamsACL = true;
                        break;
                    case "aof":
                        BDN.benchmark.Operations.OperationsBase.ParamsAOF = true;
                        break;
                    case "aad":
                        BDN.benchmark.Operations.OperationsBase.ParamsAAD = true;
                        break;
                    case "all":
                        BDN.benchmark.Operations.OperationsBase.SetAllParams(true);
                        break;
                    default:
                        throw new ApplicationException($"Unrecognized bdnOpParam value: {op}");
                }
            }
        }
    }
}