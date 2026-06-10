# Benchmarks

This directory contains projects for benchmarking Garnet.

## Resp.benchmark

**Garnet** project contains a Benchmark tool for running RESP benchmarking using different clients, different workloads and different strategies for measuring throughput, performance and latency.

See [`Resp.benchmark/README.md`](Resp.benchmark/README.md) for a copy-paste cookbook covering the **offline** (throughput) and **online** (latency-histogram) modes, plus a recipe for running an end-to-end disk-bound benchmark that lines up with the lower-layer [KV.benchmark](../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md) and [Device.benchmark](Device.benchmark/README.md) recipes.

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

## StreamBenchmark

A reproducible, publication-grade harness and methodology for benchmarking Garnet's
**Streams** implementation against Redis (and other RESP servers). Covers tooling
selection (memtier_benchmark + the `Resp.benchmark` cross-check), the stream workload
matrix (ingest / range / tail / consumer-group / mixed / trim), configuration parity,
run protocol, and a publication disclosure checklist. See
[`StreamBenchmark/README.md`](StreamBenchmark/README.md).

## Device.benchmark

A low-overhead random-read IOPS benchmark for Tsavorite's `IDevice` implementations
(Native libaio / io_uring on Linux, IOCP on Windows; FileStream; RandomAccess).
Useful for sanity-checking that a backend reaches the raw NVMe ceiling and for
isolating IO-layer performance from upper-layer KV overhead. See
[`Device.benchmark/README.md`](Device.benchmark/README.md) for usage, a
copy-paste recipe to saturate ~750K IOPS on a Dell P5600-class NVMe, and a full
flag reference.

## Privacy

[Microsoft Privacy Statement](https://go.microsoft.com/fwlink/?LinkId=521839)
