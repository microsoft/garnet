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
        // Extract our custom CLI options (--runtime / --ops) before forwarding remaining args to BDN.
        var passthroughArgs = ConsumeCustomArgs(args);

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly)
#if DEBUG
            .Run(passthroughArgs, new DebugInProcessConfig());
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

#if DEBUG

#else
#endif

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

        var baseJob = Job.Default.WithGcServer(true);

        Net8BaseJob = baseJob
            .WithRuntime(CoreRuntime.Core80)
            .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"));
        Net10BaseJob = baseJob
            .WithRuntime(CoreRuntime.Core10_0)
            .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"));

        _ = Program.bdnFramework switch
        {
            "net8.0" => AddJob(Net8BaseJob.WithId(".NET 8")),
            "net10.0" => AddJob(Net10BaseJob.WithId(".NET 10")),
            "all" => AddJob(Net8BaseJob.WithId(".NET 8"), Net10BaseJob.WithId(".NET 10")),
            _ when !string.IsNullOrEmpty(Program.bdnFramework) => throw new ApplicationException($"Unrecognized bdnFramework value: {Program.bdnFramework}"),
            _ => AddJob(Net8BaseJob.WithId(".NET 8"), Net10BaseJob.WithId(".NET 10"))
        };

        if (!string.IsNullOrEmpty(Program.bdnOpParam))
        {
            BDN.benchmark.Operations.OperationsBase.SetAllParams(false);
            var ops = Program.bdnOpParam.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(p => p.ToLower()).ToArray();
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