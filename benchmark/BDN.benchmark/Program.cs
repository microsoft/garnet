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
    .Run(args, new BaseConfig());
#endif



public class BaseConfig : ManualConfig
{
    public Job Net8BaseJob { get; }
    public Job Net9BaseJob { get; }

    public BaseConfig()
    {
        AddLogger(ConsoleLogger.Default);
        AddExporter(DefaultExporters.Markdown);
        AddColumnProvider(DefaultColumnProviders.Instance);
        WithSummaryStyle(SummaryStyle.Default.WithSizeUnit(SizeUnit.B));

        var baseJob = Job.Default.WithGcServer(true);

        var runtime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription;
        switch (runtime)
        {
            case string r when r.Contains("8.0"):
                AddJob(baseJob.WithRuntime(CoreRuntime.Core80)
                    .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"))
                    .WithId(".NET 8"));
                break;
            case string r when r.Contains("9.0"):
                AddJob(baseJob.WithRuntime(CoreRuntime.Core90)
                    .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"))
                    .WithId(".NET 9"));
                break;
            default:
                throw new NotSupportedException($"Unsupported runtime: {runtime}");
        }
    }
}