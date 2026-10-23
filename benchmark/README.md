# Benchmarks

This directory contains projects for benchmarking Garnet.

## Client.benchmark

`Client.benchmark` compares `GarnetClient` and `GarnetLightClient` against an in-process
Garnet server over loopback TCP. Add `--tls` to enable TLS on the server and the selected
client; plaintext remains the default. Add `--shared-socket` to share one connection
across workers instead of using one connection per worker.

```powershell
dotnet run --project benchmark\Client.benchmark -c Release -f net10.0 -- --client GarnetClient --tls --threads 4 --batch 32 --duration 10
dotnet run --project benchmark\Client.benchmark -c Release -f net10.0 -- --client GarnetLightClient --tls --threads 4 --batch 32 --duration 10 --shared-socket
```

TLS runs generate a temporary self-signed `localhost` certificate and trust only that
certificate, without requiring certificate files or changing the machine's trust store.
Certificate generation, connection setup, and TLS handshakes occur before the timed
workload. The transport mode is printed with the results.

The light client's rings auto-size from `threads * batch`. Use `--max-requests` and
`--max-completions` to override the request-slot floor and completion capacity.

## Resp.benchmark

**Garnet** project contains a Benchmark tool for running RESP benchmarking using different clients, different workloads and different strategies for measuring throughput, performance and latency.

See [`Resp.benchmark/README.md`](Resp.benchmark/README.md) for a copy-paste cookbook covering the **offline** (throughput) and **online** (latency-histogram) modes, plus a recipe for running an end-to-end disk-bound benchmark that lines up with the lower-layer [KV.benchmark](../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md) and [Device.benchmark](../libs/storage/Tsavorite/cs/benchmark/Device.benchmark/README.md) recipes.

You can also visit the website documentation here: [The Resp.benchmark tool](https://microsoft.github.io/garnet/docs/benchmarking/resp-bench).

## BDN.benchmark

The `BDN.benchmark` command-line tool allows contributors to run precise and reproducible micro-benchmarks by utilizing the [BenchmarkDotNet](https://benchmarkdotnet.org/index.html).

### Usage

You can list all available benchmarks using `--list flat` or `--list tree`, e.g.

```
dotnet run -c Release -f net10.0 --list flat
```

To run specific benchmarks, you can use `--filter`. For example, to run all RESP-protocol write benchmarks using the default configuration, which will run the benchmarks using all .NET target runtimes (with the dynamic PGO disabled):

```
dotnet run -c Release -f net10.0 --filter *RespIntegerWriteBenchmarks*
```

See more command-line options at https://benchmarkdotnet.org/articles/guides/console-args.html

### Writing microbenchmarks

Please see the [Microbenchmark Design Guidelines](https://github.com/dotnet/performance/blob/main/docs/microbenchmark-design-guidelines.md) for the best practices when writing microbenchmarks using BenchmarkDotNet.

### LightEpoch costs

`BenchmarkDotNetTests.EpochTests` in the Tsavorite BDN project measures direct
Tsavorite epoch costs. `BDN.benchmark.Client.EpochTests` compiles the same benchmark
source against the client epoch, so neither implementation has an extra adapter
or delegate on the measured path. `ResumeSuspend` measures acquire/release pairs;
`ProtectAndDrain` is a no-pending-drain control; `BumpAndSuspend` includes an empty
drain callback and a release on every operation. Metadata warmup and assembly
SHA256 reporting happen in setup. Results are normalized per operation.

Run direct costs separately from the `EpochContentionTests` classes:

```powershell
$env:GARNET_EPOCH_BENCHMARK_CPUS = '0:2'
dotnet run --project libs\storage\Tsavorite\cs\benchmark\BDN-Tsavorite.Benchmark -c Release -f net10.0 -- --filter '*EpochTests.*' --launchCount 1 --warmupCount 5 --iterationCount 12 --iterationTime 1000 --exporters json
dotnet run --project benchmark\BDN.benchmark -c Release -f net10.0 -- --filter '*Client.EpochTests.*' --launchCount 1 --warmupCount 5 --iterationCount 12 --iterationTime 1000 --exporters json
```

`GARNET_EPOCH_BENCHMARK_CPUS` is an optional Windows-only comma-separated list of
`processor-group:logical-CPU` entries. Choose CPUs from the machine's actual core
and NUMA topology; CPU numbering alone does not identify physical cores. Pinning
is applied to the benchmark thread (or individual workers), not to the process,
so it does not shrink `Environment.ProcessorCount` and change the epoch table size.
Invalid or duplicate entries and affinity failures fail explicitly. Thread
affinity is restored during cleanup. Leave the variable unset for scheduler-default
placement or platforms without this affinity helper.

For contention, supply at least eight distinct CPUs, for example
`0:0,0:2,0:4,0:6,0:8,0:10,0:12,0:14`, and filter `*EpochContentionTests.*`.
Workers are persistent and warmed before measurement; synchronization occurs only
at batch boundaries. `BumpAndSuspend` releases every operation, while
`BumpWhileProtected` holds protection across its batch. These exercise different
interleavings and are not interchangeable fence-cost tests. Multi-thread results
are aggregate elapsed time per operation, not per-thread latency or CPU time.
Use separate same-NUMA and cross-NUMA CPU lists rather than pooling their results.

For before/after comparisons, use the **identical updated benchmark sources** on
both revisions. Isolate the Tsavorite fence change from the client's whole
implementation replacement. Run independent launches in repeated ABBA order,
where A is the parent and B is the candidate, holding runtime, GC, CPU placement,
and JIT settings fixed within each comparison. The existing drivers disable
dynamic PGO; also evaluate production JIT defaults separately. Check binary hashes,
case coverage, and successful measurements, not only the process exit code.
Inspect disassembly in a separate run with `--disasm --disasmDepth 3`.

Predeclare an acceptable added-cost limit, such as 1 ns per acquire/release pair.
Analyze paired **launch-level** differences and require the upper confidence bound
to fall below that limit before calling the cost negligible. Retain negative and
multimodal results; overlapping BDN confidence intervals alone do not establish
equivalence. These microbenchmarks do not establish end-to-end throughput impact
or memory-model correctness, and x64 measurements do not establish ARM64 cost.

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
