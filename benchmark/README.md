# Benchmarks

This directory contains projects for benchmarking Garnet.

## Resp.benchmark

**Garnet** project contains a Benchmark tool for running RESP benchmarking using different clients, different workloads and different strategies for measuring throughput, performance and latency.

See [`Resp.benchmark/README.md`](Resp.benchmark/README.md) for a copy-paste cookbook covering the **offline** (throughput) and **online** (latency-histogram) modes, plus a recipe for running an end-to-end disk-bound benchmark that lines up with the lower-layer [KV.benchmark](../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md) and [Device.benchmark](../libs/storage/Tsavorite/cs/benchmark/Device.benchmark/README.md) recipes.

You can also visit the website documentation here: [The Resp.benchmark tool](https://microsoft.github.io/garnet/docs/benchmarking/resp-bench).

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

## Device.benchmark

A low-overhead random-read IOPS benchmark for Tsavorite's `IDevice` implementations
(Native libaio / io_uring on Linux, IOCP on Windows; FileStream; RandomAccess; and
the in-RAM `LocalMemory` device). It is a Tsavorite-engine benchmark and lives
alongside KV/YCSB under `libs/storage/Tsavorite/cs/benchmark/`. See
[`Device.benchmark/README.md`](../libs/storage/Tsavorite/cs/benchmark/Device.benchmark/README.md)
for usage and recipes to saturate ~750K IOPS on a Dell P5600-class NVMe or measure
the ~78M ops/sec LocalMemory ceiling.

The three layers — Device (raw IDevice), [KV](../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md)
(Tsavorite engine), and Resp (full RESP server) — stack as **Resp ≤ KV ≤ Device ≤ fio**,
and each has a matching disk and **LocalMemory (memory)** scenario for comparison.

## Privacy

[Microsoft Privacy Statement](https://go.microsoft.com/fwlink/?LinkId=521839)
