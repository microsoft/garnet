# Benchmarks

This directory contains projects for benchmarking gar

## Resp.benchmark

**Garnet** project contains a Benchmark tool for running RESP benchmarking using different clients, different workloads and different strategies for measuring throughput, performance and latency.

Please visit our website documentation about how to use it in the following link: [The Resp.benchmark tool](https://microsoft.github.io/garnet/docs/benchmarking/resp-bench)

## BenchmarkDotNet.benchmark

The `BenchmarkDotNet.benchmark` command-line tool allows contributors to run precise and reproducible micro-benchmarks by utilizing the [BenchmarkDotNet](https://benchmarkdotnet.org/index.html).

### Usage

You can list all available benchmarks using `--list`, e.g.

```
dotnet run -c Release -- --list flat
```

To run specific benchmarks, you can use `--filter`. For example, to run all RESP-protocol write benchmarks using .NET 6 and 8 runtimes, run:

```
dotnet run -c Release -f net8.0 --runtimes net6.0 net8.0 --filter *RespIntegerWriteBenchmarks*
```

See more command-line options at https://benchmarkdotnet.org/articles/guides/console-args.html

## Privacy

[Microsoft Privacy Statement](https://go.microsoft.com/fwlink/?LinkId=521839)
