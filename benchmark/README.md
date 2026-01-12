# Benchmarks

This directory contains projects for benchmarking Garnet.

## Resp.benchmark

**Garnet** project contains a Benchmark tool for running RESP benchmarking using different clients, different workloads and different strategies for measuring throughput, performance and latency.

Please visit our website documentation about how to use it in the following link: [The Resp.benchmark tool](https://microsoft.github.io/garnet/docs/benchmarking/resp-bench)

## BDN.benchmark

The `BDN.benchmark` command-line tool allows contributors to run precise and reproducible micro-benchmarks by utilizing the [BenchmarkDotNet](https://benchmarkdotnet.org/index.html).

### Usage

You can list all available benchmarks using `--list flat` or `--list tree`, e.g.

```
dotnet run -c Release -f net8.0 --list flat
```

To run specific benchmarks, you can use `--filter`. For example, to run all RESP-protocol write benchmarks using the default configuration, which will run the benchmarks using all .NET target runtimes (with the dynamic PGO disabled):

```
dotnet run -c Release -f net8.0 --filter *RespIntegerWriteBenchmarks*
```

See more command-line options at https://benchmarkdotnet.org/articles/guides/console-args.html

### Writing microbenchmarks

Please see the [Microbenchmark Design Guidelines](https://github.com/dotnet/performance/blob/main/docs/microbenchmark-design-guidelines.md) for the best practices when writing microbenchmarks using BenchmarkDotNet.

## Privacy

[Microsoft Privacy Statement](https://go.microsoft.com/fwlink/?LinkId=521839)
