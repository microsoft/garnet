# Resp.benchmark

End-to-end throughput / latency benchmark for the Garnet RESP server. Generates
synthetic GET/SET/INCR/PFADD/ZADD/... workloads against any RESP server (Garnet,
Redis, KeyDB, Dragonfly, …) over TCP — or against an in-process embedded Garnet
server — using one of four pluggable clients:

| Client (`--client`) | Pipelining | Notes |
| --- | --- | --- |
| `LightClient` | manual batching (offline only) | Custom zero-alloc RESP pipeliner. Used by all offline runs by default. |
| `GarnetClientSession` | async pipelined | Native Garnet client, supports `--itp` (intra-thread parallelism). Best for online throughput. |
| `GarnetClient` | async pipelined | Higher-level Garnet client with per-call callbacks; supports `--sync` and `--itp`. |
| `SERedis` | StackExchange.Redis pipelined | Redis-compatible client. Use for apples-to-apples comparisons with Redis. |
| `InProc` | direct dispatch | Embedded `EmbeddedRespServer` — no TCP, no networking. Measures server-side CPU only. |

The tool measures two distinct things depending on mode:

* **Offline** (`--op X`) — pre-builds RESP request batches of size `-b N`, then
  N worker threads loop `Send(batch) → CompletePendingRequests()` for `--runtime`
  seconds. Reports **throughput in ops/sec**. This is what you want for
  saturating the server.
* **Online** (`--online`) — N worker threads each issue **one in-flight request
  at a time** (or `--itp K` in-flight) and record per-op latency in an
  HdrHistogram, printing min / p5 / p50 / mean / p95 / p99 / p99.9 / iter_ops /
  Kops/sec every 2 s. This is what you want for **latency curves**.

For a website-style overview see
[microsoft.github.io/garnet/docs/benchmarking/resp-bench](https://microsoft.github.io/garnet/docs/benchmarking/resp-bench).
This README focuses on **reproducible recipes**.

---

## Quick start

```bash
# Build (run once)
dotnet build benchmark/Resp.benchmark/Resp.benchmark.csproj -c Release -f net10.0
dotnet build main/GarnetServer/GarnetServer.csproj -c Release -f net10.0

# Start a Garnet server in the background (default port 6379)
dotnet main/GarnetServer/bin/Release/net10.0/GarnetServer.dll --port 6379 &

# Offline throughput: 1M-key DB, GET, 8 threads, batch=512, 15 s
dotnet benchmark/Resp.benchmark/bin/Release/net10.0/Resp.benchmark.dll \
    --op GET --dbsize 1000000 -t 8 -b 512 --runtime 15

# Online latency: 8 connections, 50/50 GET/SET, no batching
dotnet benchmark/Resp.benchmark/bin/Release/net10.0/Resp.benchmark.dll \
    --online --op-workload GET,SET --op-percent 50,50 \
    --dbsize 1000000 -t 8 -b 1 --runtime 30 --client GarnetClientSession
```

Performance numbers should always come from a **Release** build. Debug builds
are correctness-only. All examples below assume the binary path is in `$RB`:

```bash
export RB=benchmark/Resp.benchmark/bin/Release/net10.0/Resp.benchmark.dll
export GS=main/GarnetServer/bin/Release/net10.0/GarnetServer.dll
```

---

## Offline vs online — what each one is for

Offline and online are two completely separate code paths. They are **not**
interchangeable: the workload they generate, the metric they emit, and the
clients they support all differ. Pick based on the question you're asking.

| Use offline (`--op X`) when you want to … | Use online (`--online`) when you want to … |
| --- | --- |
| Saturate the server with max throughput | Measure end-to-end latency percentiles |
| Sweep `-t 1,2,4,…,32` and observe scaling | Hold offered-load constant and observe response time |
| Compare GET vs MGET vs INCR throughput | Compare GET vs SET tail latency at a fixed concurrency |
| Run against an in-process server (`--client InProc`) | Compare clients (LightClient vs SERedis vs GarnetClientSession) at the same load |

### Offline mode (`--op X`)

* All requests for the run are pre-generated into pinned byte buffers and
  reused. Per-op CPU on the client side is essentially zero.
* Each thread pipelines `-b` requests per network round trip; default `-b 4096`.
* Default client is `LightClient` — a zero-alloc custom RESP pipeliner.
* `--client InProc` swaps the TCP path for direct in-process dispatch to an
  embedded `RespServerSession`, isolating server-side CPU from networking.
* Outputs: `[Total time]: <ms> for <total_ops_done> ops` and
  `[Throughput]: <ops/sec>`.

### Online mode (`--online`)

* One outstanding op per worker thread (or `--itp K` outstanding when paired
  with `GarnetClient` / `GarnetClientSession` / `SERedis`).
* Latency of every op is recorded in a per-thread `HdrHistogram` and printed
  every 2 s. Each row covers one 2 s polling window (`iter_tops`) and is also
  rolled into a `total_ops` running sum.
* `-b` is **forced to 1** in online mode (the tool will warn and override). Use
  `--itp` instead to increase concurrency per thread.
* Workload mix is set with `--op-workload GET,SET,DEL --op-percent 50,30,20`
  (must sum to 100). Default `--op GET` with single-op workload is fine.

```
min (us);      5th (us);      median (us);   avg (us);      95th (us);     99th (us);     99.9th (us);   total_ops;     iter_tops;     tpt (Kops/sec)
0.86           1.14           1.61           1.71           2.81           4.85           14.79          318,464        318,464        159.23
```

---

## Common knobs

| Flag | Default | What it controls |
| --- | --- | --- |
| `--op` | `GET` | Op to benchmark (offline). See full list below. |
| `--dbsize` | `1024` | Number of distinct keys in the keyspace. Pre-loaded at startup unless `-s`. |
| `--keylength` | `1` (auto-pads up to `log10(dbsize)`) | Minimum key bytes. `0` = pad to DbSize's max-digit width. |
| `--valuelength` | `8` | Value bytes per record. Use `100` for parity with KV.benchmark. |
| `-t, --threads` | `1,2,4,8,16,32` | Worker thread counts to sweep (comma-separated; offline only). |
| `-b, --batchsize` | `4096` | RESP requests per pipeline (offline). Online forces `-b 1`. |
| `--runtime` | `15` (sec) | Per-thread-count cell duration. |
| `--totalops` | `1<<25` | Caps total ops per cell (whichever hits first: `--runtime` or `--totalops`). |
| `-s, --skipload` | `false` | Skip the load phase. **Use this to run against a pre-loaded server.** |
| `--client` | `LightClient` (offline), `GarnetClientSession` (online) | See client table at top. |
| `--burst` | `false` | `GarnetClientSession` only: don't wait for response (fire-and-forget). |
| `--online` | `false` | Switch to online (latency-histogram) mode. |
| `--itp` | `1` | Online only: in-flight requests per thread. Increase for more offered load. |
| `--sync` | `false` | Online + `GarnetClient` only: synchronous wait-per-op. |
| `--zipf` | `false` | Skew key distribution with θ = 0.99 instead of uniform. |
| `--op-workload`, `--op-percent` | `GET,SET,DEL` / `60,30,10` | Online workload mix; percents must sum to 100. |
| `-h, --host`, `--port` | `127.0.0.1`, `6379` | Server endpoint. |
| `--client-hist` | `false` | Record client-side latency histogram (Garnet clients only). |
| `--tls`, `--tlshost`, `--cert-file-name`, `--cert-password` | off | Enable TLS to the server. |
| `--save-freq` | `0` | Issue `SAVE` every N seconds during the run. |
| `--logger-level` | `Information` | Console verbosity. |

Run `dotnet $RB --help` for the full list (incl. AOF-bench, transactions,
scripts, cluster, and in-proc embedded-server flags).

---

## Cookbook — offline (throughput)

All recipes assume `$RB` is set (see Quick start) and a Garnet server listens on
`127.0.0.1:6379`. To run against a different server (Redis / KeyDB / Dragonfly /
remote Garnet), add `-h <ip> --port <port>` and, for parity, `--client SERedis`
(if the server doesn't speak Garnet's binary protocol extensions, which it
shouldn't need for these ops anyway).

| Scenario | Command |
| --- | --- |
| **GET, in-memory, default DB (1024 keys)** — quick smoke test | `dotnet $RB --op GET -t 1,2,4,8,16,32 -b 1024` |
| **GET, 16 M keys × 100 B values, single thread** | `dotnet $RB --op GET --dbsize 16777216 --valuelength 100 -t 1 -b 1024 --runtime 15` |
| **SET write throughput, 16 M keys × 100 B** | `dotnet $RB --op SET --dbsize 16777216 --valuelength 100 -t 16 -b 1024 --runtime 15` |
| **MGET, 4-way pipelined within each RESP request** (heavy server CPU per round trip) | `dotnet $RB --op MGET --dbsize 16777216 --valuelength 8 -t 16 -b 512` |
| **INCR throughput (8-byte numeric values)** | `dotnet $RB --op INCR --dbsize 1024 --valuelength 0 -t 16 -b 1024` |
| **Large-value bitmap throughput (1 MiB values)** | `dotnet $RB --op BITCOUNT --dbsize 256 --valuelength 1048576 -t 1 -b 1` |
| **Zipfian skew on GET** | `dotnet $RB --op GET --dbsize 16777216 --valuelength 100 -t 16 -b 1024 --zipf` |
| **Compare with StackExchange.Redis client (apples-to-apples vs Redis)** | `dotnet $RB --op GET --dbsize 1000000 --valuelength 100 -t 16 -b 256 --client SERedis` |
| **Server-side CPU only (no TCP)** | `dotnet $RB --op GET --dbsize 1000000 --valuelength 100 -t 16 -b 1024 --client InProc` |
| **Pre-load only, then exit** (DB seeding for separate runs) | `dotnet $RB --op MSET --dbsize 16777216 --valuelength 100 --runtime 0` |
| **Run GET against an already-loaded server** | `dotnet $RB -s --op GET --dbsize 16777216 --valuelength 100 -t 1,2,4,8,16,32 -b 1024 --runtime 15` |

Tips:
* `-b` (batch size) is the dominant throughput knob. `b=1024` is a good
  default — large enough to amortize network and per-batch CPU, small enough
  to keep per-thread memory reasonable.
* Thread sweep `-t 1,2,4,8,16,32` produces one cell per thread-count, each
  `--runtime` seconds long. Total wall clock ≈ `<#cells> × --runtime + load`.
* Load wall-clock for 16 M × 100 B is ~5-10 s; for 100 M × 100 B is ~30-60 s.
  Run with `-s` (skipload) once the DB is warm and you're iterating on run
  parameters.

---

## Cookbook — online (latency)

Online mode always uses **one outstanding op per worker thread** (or `--itp K`
when you want more offered load per thread). `-b` is forced to 1. The reported
`tpt (Kops/sec)` is computed as `BatchSize × iter_tops / elapsedSecs`, so for
`--itp K` it already accounts for the K-way parallelism.

| Scenario | Command |
| --- | --- |
| **Single-client GET latency** (cleanest latency reading) | `dotnet $RB --online --op-workload GET --op-percent 100 --dbsize 1000000 -t 1 -b 1 --runtime 30` |
| **50/50 GET/SET tail latency under load** (16 client connections) | `dotnet $RB --online --op-workload GET,SET --op-percent 50,50 --dbsize 1000000 -t 16 -b 1 --runtime 60 --client GarnetClientSession` |
| **Hold a fixed offered load** (8 conns × 64 in-flight = 512 outstanding) | `dotnet $RB --online --op-workload GET --op-percent 100 --dbsize 1000000 -t 8 --itp 64 --runtime 60 --client GarnetClientSession` |
| **SERedis online comparison** (pooled connections) | `dotnet $RB --online --op-workload GET --op-percent 100 --dbsize 1000000 -t 16 --itp 16 --runtime 60 --client SERedis --pool` |
| **ZADD-heavy online mix** | `dotnet $RB --online --op-workload ZADD,ZCARD --op-percent 80,20 --dbsize 100000 -t 1 -b 1 --client SERedis` |
| **Sustain forever** (manual stop via Ctrl-C, useful for live perf-watching) | `dotnet $RB --online --op-workload GET --op-percent 100 --dbsize 1000000 -t 16 -b 1 --runtime -1` |

`-i 0` for `--runtime` is **not** valid (the loop checks `iteration == runDuration` 
and `runtime=0` exits immediately). Use a small positive value or `-1` for an
infinite run.

Output columns (printed every 2 s):

```
min (us); 5th; median; avg; 95th; 99th; 99.9th; total_ops; iter_tops; tpt (Kops/sec)
```

* `iter_tops` = ops completed in the most-recent 2 s polling window.
* `total_ops` = running sum since last `--resetInterval` (30 iters = 60 s by
  default; rolled internally).
* `tpt` = `BatchSize × iter_tops / window_secs`. For online with `--itp K` the
  tool multiplies `iter_tops` by `K` so the throughput line is the actual
  per-second op rate, not the per-second sampled-latency-record rate.

---

## Disk-bound benchmark (mostly-on-disk read workload)

Garnet's "mostly on disk" RESP workload — the end-to-end analogue of
[KV.benchmark's `--log-memory 16m`](../../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md#cookbook-copy-paste-matrix-distribution--dataset-size--log-size)
disk-IO sweep and
[Device.benchmark's raw-IOPS test](../Device.benchmark/README.md#saturating-nvme-at-750k-iops).
Use this when you want to know how much of the device IOPS the **full RESP
stack** (network parse → hash bucket walk → pending IO → device read → RESP
serialize → network send) is delivering.

The hierarchy of "what's limiting" goes:

```
fio (raw kernel)         e.g., 750 K IOPS  (NVMe ceiling, 8 jobs × QD=64)
  ↓
Device.benchmark         e.g., 750 K IOPS  (Tsavorite IDevice direct, 16 t × throttle=512)
  ↓
KV.benchmark             e.g., 565 K IOPS  (Tsavorite pending-read path, 8 t × throttle=512, 100 M)
  ↓
Resp.benchmark (this)    e.g., ??? IOPS    (full Garnet RESP stack)
```

All four layers must use the **same dataset size, throttle, log shape,
and backend** to be directly comparable. The numbers above are
illustrative — actual values depend on the device. A **100 M × 100 B**
dataset is the minimum at which a fast NVMe reaches its random-read
ceiling: smaller datasets fill only the first ~1 GB of LBA range and
land on a subset of NAND dies, raising per-IO service time at the device
even though queue depth and IO size are unchanged. Always benchmark
disk-bound recipes with at least 100 M records.

If `Resp.benchmark` drops well below `KV.benchmark` on the same hardware, the
gap is in the Garnet server path (RESP parse, command dispatch, pending-IO
plumbing inside `RespServerSession`, response write). If `KV.benchmark` itself
drops well below `Device.benchmark`, the gap is in the Tsavorite per-op CPU /
GC pressure on the pending-read path.

### Recipe — 100 M × 100 B records, 16 MB log, libaio

This forces ≈ 99.875 % of the dataset to disk so every GET is a 4 KB random
fetch through Tsavorite's pending-read path. Wall clock per recipe (one load +
sweep) is ≈ 35 s + 6 × 15 s ≈ 2 min.

**Step 1 — start the Garnet server with disk tiering and a small memory log**:

```bash
# Pick a fast NVMe mount for --logdir
DATA=/mnt/nvme/garnet
mkdir -p $DATA

numactl --cpunodebind=0 --membind=0 dotnet $GS \
    --port 6379 \
    --memory 16m --page 4m --segment 1g \
    --index 4g \
    --storage-tier --logdir $DATA \
    --device-type Native --device-io-backend libaio \
    --device-throttle-limit 512 \
    --sg-get true \
    --logger-level Warning &
SERVER_PID=$!
```

Notes:
* **`--sg-get true` is required for disk-bound GET throughput.** It enables
  scatter-gather libaio submissions, batching contiguous pending GETs from a
  pipelined RESP connection into a single vectored IO. Without it, the
  server processes pending GETs one at a time per connection and the device
  queue depth stays below 10 even with multiple client threads — the disk
  reports 100 % busy at a small fraction of its IOPS ceiling. Enabling the
  flag typically yields a several-fold throughput increase at low thread
  counts and 3–4× at higher thread counts.
* `--memory 16m --page 4m` → 4 pages, just enough that records spill into
  read-only and then to disk. (KV.benchmark uses the same shape.)
* `--index 4g` → 4 GB hash index = 64 M buckets = 8 entries/bucket × 64 M ÷
  100 M keys ≈ load factor 2, safely below chain-extension territory. Default
  `--index 128m` is **too small** for 100 M keys (3-4× slowdown from overflow
  chains).
* `--device-type Native --device-io-backend libaio` is the fastest backend on
  Linux for this workload. Swap `libaio` → `uring` to compare; `--device-type
  Default` falls back to `RandomAccess` on Linux (≈ 120 K IOPS BCL-async cap;
  much slower).
* `--device-throttle-limit 512` matches the Device.benchmark default. On a
  fast NVMe (≥ 500 K IOPS), Little's Law keeps actual kernel in-flight ≈ 50
  even at this throttle, well below the 128-slot libaio ring depth. Lower it
  to 128 on slower (SATA) devices if you see `errorCode != 0` callbacks in
  the server log.
* `numactl --cpunodebind=0 --membind=0` keeps the server pinned to one NUMA
  node. The same wrapper on the client side is recommended (next steps).

**Step 2 — load 100 M × 100 B records once** (a few minutes wall-clock; the
client spends most of it generating request batches, with the actual
server-side load running at multi-MOPS):

```bash
numactl --cpunodebind=0 --membind=0 dotnet $RB \
    --op MSET --dbsize 100000000 --valuelength 100 \
    -t 8 -b 1024 --runtime 0
```

Loading uses 8 threads × MSET batches of `dbsize/threads` keys (capped at
`-b`). After load, the server has ≈ 12.8 GB on disk and ≈ 16 MB in the
mutable region — i.e., ≈ 0.125 % in-memory hit rate. Verify with
`redis-cli DBSIZE` (it should report 100000000).

**Step 3 — run the GET sweep against the preloaded server**:

```bash
numactl --cpunodebind=0 --membind=0 dotnet $RB -s \
    --op GET --dbsize 100000000 --valuelength 100 \
    -t 1,2,4,8,16,32 -b 1024 --runtime 15
```

`-s` (skipload) is essential — the server already has the data. Per-cell
output looks like:

```
[Total time]: 15,001ms for 4,830,000 ops
[Throughput]: 322,000.00 ops/sec
```

Sweep `-t 1,2,4,8,16,32` to see how the RESP path scales with disk-IO
concurrency. Throughput should plateau at the lower of (a) the server's
network/RESP processing ceiling and (b) the Tsavorite pending-read path
ceiling measured by KV.benchmark with the equivalent disk-bound recipe.

**Batch-size tuning**: `-b 1024` is a safe default and reaches near-peak
disk-bound throughput at 8 threads; `-b 2048` typically gives a small
additional gain. Higher values (`-b 4096`+) regress because client-side
batch generation dominates wall time and server-side response buffers
fragment more. Resp.benchmark has no built-in warmup flag, so the first
per-cell measurement of each sweep includes ramp-up — re-run a second
time and use the steady-state numbers.

**Step 4 — compare to the layers below**:

Run the matching recipes from the three benchmarks back-to-back on the same
machine:

```bash
# Layer 1: raw device (Tsavorite IDevice direct)
dotnet benchmark/Device.benchmark/bin/Release/net10.0/Device.benchmark.dll \
    --file-name $DATA/devbench.dat --file-size 17179869184 \
    --device-type Native --io-backend libaio \
    --sector-size 4096 --batch-size 4096 \
    --threads 16 --throttle-limit 512 --runtime 8

# Layer 2: full KV (Tsavorite pending-read).
# Throttle, log shape, backend, and thread sweep match Step 1/3 so the
# three layers are directly comparable.
KV=libs/storage/Tsavorite/cs/benchmark/KV.benchmark/bin/Release/net10.0/KV.benchmark.dll
numactl --cpunodebind=0 --membind=0 dotnet $KV \
    -n 100000000 -v 100 --device native --device-io-backend libaio \
    --device-throttle 512 \
    --log-memory 16m --page-size 4m --segment-size 1g \
    --rumd 100,0,0,0 --load-threads 8 --run-threads-sweep 1,2,4,8,16,32 \
    --runsec 15 --warmup-sec 5 --data-path $DATA/kv

# Layer 3: full RESP (this benchmark, steps 2+3 above)
```

The numbers should roughly stack: **Resp ≤ KV ≤ Device ≤ fio**. The gap
between adjacent layers tells you which subsystem is consuming the missing
IOPS.

**Step 5 — clean up**:

```bash
kill $SERVER_PID
rm -rf $DATA
```

### Why these settings

The KV.benchmark and Device.benchmark READMEs cover the
[device-side settings](../Device.benchmark/README.md#why-these-settings) and
the [mostly-on-disk log shape](../../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md#cookbook-copy-paste-matrix-distribution--dataset-size--log-size).
The Garnet-server-specific knobs:

* `--memory 16m --page 4m` — forces records to spill to disk while still
  allowing one or two pages in the mutable region (so writes during load are
  not blocked on flush). Matches the KV.benchmark `--log-memory 16m
  --page-size 4m` shape.
* `--index 4g` — at 100 M keys with the default 128 MB index, average chain
  depth balloons and reads pay an extra ~2 µs per chain link. 4 GB keeps
  average chain length ≤ 1.
* `--storage-tier` — required to enable disk tiering at all; without it the
  server keeps the full log in memory and crashes when the `--memory` cap is
  hit.
* `--device-throttle-limit 512` — Garnet's analogue of Device.benchmark's
  `--throttle-limit`. On fast NVMe ≥ 128 is normally safe.
* `--logger-level Warning` — keeps stdout quiet so it doesn't perturb the run.
  Bump to `Information` or `Debug` only when investigating.

---

## Other modes

These exist but are out of scope for this README — see the source.

* `--txn` — transactional procedure micro-benchmark. See `TxnPerfBench.cs`.
* `--aof-bench`, `--aof-bench-type` — replica-side AOF replay / append
  microbench. See `OfflineBench/AOFBench/`.
* `--client InProc` + `--cluster` — embed an in-proc cluster node for
  cluster-mode RESP CPU profiling.

---

## Output reference

### Offline
```
Operation type: GET
Num threads: 16
...
[Total time]: 15,000.00ms for 75,000,000 ops
[Throughput]: 5,000,000.00 ops/sec
```
`Throughput = total_ops_done / runtime_seconds`. `total_ops_done` is an
atomic counter incremented per `Send → CompletePendingRequests` round trip,
multiplied by `BatchSize`.

### Online
```
Using OpRunnerGarnetClientSession...
min (us); 5th; median; avg; 95th; 99th; 99.9th; total_ops; iter_tops; tpt (Kops/sec)
0.86      1.14  1.61    1.71  2.81   4.85   14.79         318,464   318,464   159.23
```
* All latencies in microseconds, derived from per-thread `HdrHistogram` with
  resolution = 2 significant digits, lower bound 1 µs, upper bound 100 s.
* `total_ops` resets every `--resetInterval` (default 30) iterations × 2 s =
  every 60 s.
* Out-of-range latency samples (≥ 100 s) are recorded as the upper bound
  rather than discarded.

---

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `Skipload not supported with --online` | online mode pre-loads on its own client | drop `-s` or use offline mode |
| `Batch size parameter should be one entry of size 1, for the online benchmark` | `-b N>1` passed to `--online` | tool auto-overrides to `-b 1` — use `--itp` for per-thread concurrency |
| `Pooling of LightClient is not supported` | `--pool` with `--client LightClient` | use `GarnetClientSession`, `GarnetClient`, or `SERedis` for `--pool` |
| Disk-bound throughput far below KV.benchmark | server-side bottleneck — RESP parse / dispatch / response write | profile with `dotnet-trace collect -p <pid> --providers Microsoft-DotNETCore-SampleProfiler` while the sweep is running |
| Disk-bound throughput far below Device.benchmark, KV.benchmark matches Resp.benchmark | Tsavorite pending-read path is the bottleneck | see KV.benchmark README's [cookbook section](../../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md#cookbook-copy-paste-matrix-distribution--dataset-size--log-size) and [Linux native I/O backends](../../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md#linux-native-io-backends---device-io-backend) |
| `--keylength` lower than `log10(--dbsize)` | tool silently pads to `max(NumDigits(dbsize), --keylength)` | not a bug; pass `--keylength N` ≥ `NumDigits(dbsize)` to make the padding explicit |
| `--dbsize % loadDbThreads != 0` | load phase requires divisibility | round `--dbsize` to a multiple of the loader's thread count (default 8) |

---

## Related

* [Device.benchmark](../Device.benchmark/README.md) — raw `IDevice` IOPS
  benchmark (layer 1 in the disk-bound hierarchy).
* [KV.benchmark](../../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md) —
  Tsavorite KV throughput benchmark (layer 2 in the disk-bound hierarchy).
* [BDN.benchmark](../BDN.benchmark) — BenchmarkDotNet-driven RESP
  microbenchmarks (single-RESP-command CPU / allocation profiling; runs in
  CI).
* [Garnet benchmarking docs](https://microsoft.github.io/garnet/docs/benchmarking/resp-bench)
  — full website tutorial including TLS, cluster, and replication.
