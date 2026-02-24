// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;
using VectorSearchBench;

await Parser.Default
    .ParseArguments<BenchOptions>(args)
    .WithParsedAsync(async opts =>
    {
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        try
        {
            await BenchmarkRunner.RunAsync(opts, cts.Token);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("\n[interrupted]");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[error] {ex.Message}");
            Environment.Exit(1);
        }
    });
