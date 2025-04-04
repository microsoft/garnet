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

        if (args.Length > 0)
        {
            var framework = args[0];  
            switch (framework)
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
                    throw new NotSupportedException($"Unsupported runtime: {framework}");
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