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

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly)
#if DEBUG
    .Run(args, new DebugInProcessConfig());
#else
    .Run(args, new BaseConfig(args));
#endif

public class BaseConfig : ManualConfig
{
    public Job Net8BaseJob { get; }
    public Job Net9BaseJob { get; }

    public BaseConfig(string[] args)
    {
        AddLogger(ConsoleLogger.Default);
        AddExporter(DefaultExporters.Markdown);
        AddColumnProvider(DefaultColumnProviders.Instance);
        WithSummaryStyle(SummaryStyle.Default.WithSizeUnit(SizeUnit.B));

        var baseJob = Job.Default.WithGcServer(true);

        // Bug in BDN: When run with 'dotnet run', all optional BDN parameters are counted as args to program.cs.
        // It also overwrites the last BDN arg with the '--' arg sent to program.cs.
        // Example:
        // dotnet run -c Release -f net9.0 --project C:/GarnetGitHub/benchmark/BDN.benchmark --filter BDN.benchmark.Operations.BasicOperations.* --exporters json -- net9.0
        // BDN fails with "net9.0 is invalid value for '--exporters'."
        // Workaround: Use the last parameter to determine the BDN framework (net8.0 or net9.0).
        // Then set the last arg to the second-to-last arg value (e.g., "json").
        // This ensures BDN runs correctly by using the last arg as the value for the second-to-last arg.

        if (args.Length > 0 && (args[args.Length - 1] == "net8.0" || args[args.Length - 1] == "net9.0"))
        {
            string BDNframework = args[args.Length - 1];
            if (args.Length > 1)
            {
                args[args.Length - 1] = args[args.Length - 2];  // bug in BDN where arg to this app is sent to BDN as the last arg
            }

            switch (BDNframework)
            {
                case "net8.0":
                    AddJob(baseJob.WithRuntime(CoreRuntime.Core80)
                        .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"))
                        .WithId(".NET 8"));
                    break;
                case "net9.0":
                    AddJob(baseJob.WithRuntime(CoreRuntime.Core90)
                        .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"))
                        .WithId(".NET 9"));
                    break;
                default:
                    throw new NotSupportedException($"Unsupported framework: {BDNframework}");
            }
        }
        else
        {
            AddJob(baseJob.WithRuntime(CoreRuntime.Core80)
                .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"))
                .WithId(".NET 8"));
            AddJob(baseJob.WithRuntime(CoreRuntime.Core90)
                .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"))
                .WithId(".NET 9"));
        }
    }
}