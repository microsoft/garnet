// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;

var config = DefaultConfig.Instance
    .AddJob(Job.Default
        .WithRuntime(CoreRuntime.Core60)
        .WithId(".NET 6"))
    .AddJob(Job.Default
        .WithRuntime(CoreRuntime.Core80)
        .WithEnvironmentVariables(new EnvironmentVariable("DOTNET_TieredPGO", "0"))
        .WithId(".NET 8"));

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);