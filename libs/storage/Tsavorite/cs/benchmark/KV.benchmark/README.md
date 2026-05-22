# KV.benchmark

A lean throughput benchmark for the Tsavorite key-value store. Measures load
(insert) and run (RUMD = reads / upserts / RMWs / deletes) throughput on a
synthetic 8-byte-key + fixed-length-value dataset, using ObjectAllocator and
the safe `BasicContext` session path.

Designed to **reflect underlying Tsavorite engine performance** without
benchmark-side noise: zero per-op allocations on the hot path, no shared
counters on the inner loop, NUMA-pinned worker threads, scoreboard layout that
prevents false sharing across worker cores, central tick timing that excludes
worker-join lag from the measured duration, and rich per-iteration metadata in
human-readable, JSON, and CSV streams.

## Quick start

```bash
# Build
dotnet build libs/storage/Tsavorite/cs/benchmark/KV.benchmark/KV.benchmark.csproj \
    -c Release -f net10.0

# Minimal in-memory smoke test (1 thread, null device, 5 s read run)
dotnet libs/storage/Tsavorite/cs/benchmark/KV.benchmark/bin/Release/net10.0/KV.benchmark.dll \
    -t 1 -n 1000000 -v 100 --device null --rumd 100,0,0,0 --runsec 5
```

```powershell
# Windows (PowerShell)
dotnet libs\storage\Tsavorite\cs\benchmark\KV.benchmark\bin\Release\net10.0\KV.benchmark.dll `
    -t 1 -n 1000000 -v 100 --device null --rumd 100,0,0,0 --runsec 5
```

Expected output (excerpt):
```
=== KV.benchmark config ===
  threads          : 1  (pinned: 0)
  keys             : 1,000,000
  value-size       : 100 bytes (reader copies first 64 B)
  rumd%            : 100,0,0,0  (deletes auto-reinsert: n/a)
  ...
===========================
[load]   1,000,000 ops in 0.5 s  (~2,000,000 ops/sec)  ...
[run 1]  ~5,000,000 ops in 5.000 s  (~1,000,000 ops/sec)  ...
[aggregate] iterations=1 mean=... stdev=0.0 (0.0%) ...
```

## Building

```bash
dotnet build libs/storage/Tsavorite/cs/benchmark/KV.benchmark/KV.benchmark.csproj -c Release -f net10.0
```

Performance numbers should always be taken from a **Release** build on
**net10.0**. The project enables Server GC (`<ServerGarbageCollection>true</…>`)
and concurrent GC for steady-state behaviour. Debug builds are fine for
correctness checks but not for measurement.

## Worked examples

### 1. In-memory throughput ceiling (100 % read)

`--device null` + log auto-sized to fit the dataset in the mutable region.

```bash
numactl --membind=0 --cpunodebind=0 \
  dotnet KV.benchmark.dll -t 64 -n 50000000 -v 100 \
    --device null --rumd 100,0,0,0 \
    --runsec 5 --warmup-sec 2 --report-interval-sec 0 -i 3
```

Sample result on a 2 × 80-core / 2-NUMA host:
```
[run 1] 566,958,848 ops in 5.000 s  (113,386,218 ops/sec)  hits=566,974,720 misses=0 gc=0/0/0 alloc/wkr=4360B
[run 2] 567,287,296 ops in 5.000 s  (113,451,754 ops/sec)  hits=567,303,424 misses=0 gc=0/0/0 alloc/wkr=4328B
[run 3] 567,573,760 ops in 5.000 s  (113,509,656 ops/sec)  hits=567,589,888 misses=0 gc=0/0/0 alloc/wkr=4328B
[aggregate] iterations=3 mean=113,449,209 ops/sec stdev=50,425.7 (0.0%) min=113,386,218 max=113,509,656 trimmed=113,451,754
```

### 2. In-memory throughput with skewed reads (zipf)

```bash
numactl --membind=0 --cpunodebind=0 \
  dotnet KV.benchmark.dll -t 64 -n 50000000 -v 100 \
    --device null --rumd 100,0,0,0 \
    --distribution zipf --zipf-theta 0.99 \
    --runsec 5 --warmup-sec 2 --report-interval-sec 0 -i 3
```

Zipf reads concentrate on the same hot keys, so cache-line reuse goes up;
typically 1.5–2× higher than uniform.

### 3. Mixed RUMD throughput

```bash
numactl --membind=0 --cpunodebind=0 \
  dotnet KV.benchmark.dll -t 64 -n 50000000 -v 100 \
    --device null --rumd 50,40,5,5 \
    --runsec 5 --warmup-sec 2 --report-interval-sec 0 -i 3
```

When `d % > 0`, every Delete is **immediately followed by a re-Upsert of the
same key** (counted as a separate op). This keeps the dataset stable across
the run; otherwise a delete-heavy workload would silently degrade into a
read-miss workload.

### 4. NVMe write throughput (load only is write-bound)

```bash
numactl --membind=0 --cpunodebind=0 \
  dotnet KV.benchmark.dll -t 64 -n 50000000 -v 100 \
    --device randomaccess \
    --rumd 100,0,0,0 --runsec 1 --warmup-sec 0 \
    --data-path /mnt/nvme/kv
```

Load throughput is reported by the `[load]` line. The run phase here is
short — we just want the load number.

### 5. NVMe read-only after warm load (in-memory reads, large log)

```bash
numactl --membind=0 --cpunodebind=0 \
  dotnet KV.benchmark.dll -t 64 -n 50000000 -v 100 \
    --device randomaccess --rumd 100,0,0,0 \
    --runsec 5 --warmup-sec 2 --report-interval-sec 0 -i 3 \
    --data-path /mnt/nvme/kv
```

With `--log-memory` auto-sized to fit the dataset, reads stay in memory and
the NVMe device is mostly idle. Sample result: `115.83 M ops/sec trimmed
mean, 0.1 % stdev`.

### 6. Multi-iteration stability run

```bash
dotnet KV.benchmark.dll -t 64 -n 50000000 -v 100 \
    --device null --rumd 100,0,0,0 \
    --runsec 5 --warmup-sec 2 --report-interval-sec 0 -i 7
```

The final `[aggregate]` line shows `mean`, `stdev`, `stdev%`, `min`, `max`,
and (when `-i ≥ 3`) a `trimmed` mean that drops the single highest and
lowest samples. Use `trimmed` for stability-focused reports.

### 7. Data integrity check after load

```bash
dotnet KV.benchmark.dll -t 4 -n 1000000 -v 100 \
    --device null --validate \
    --runsec 0 --warmup-sec 0
```

`--validate` runs a single-threaded scan after the load phase, reading back
every key and asserting the value bytes match what was written. Exits with
code `2` on mismatch.

### 8. Device backend sweep

```bash
for dev in null randomaccess filestream; do
  rm -rf /mnt/nvme/kv-sweep/*
  numactl --membind=0 --cpunodebind=0 \
    dotnet KV.benchmark.dll -t 64 -n 50000000 -v 100 \
      --device $dev --rumd 100,0,0,0 \
      --runsec 5 --warmup-sec 2 --report-interval-sec 0 -i 3 \
      --data-path /mnt/nvme/kv-sweep \
      --csv-output /tmp/kv-sweep.csv --quiet
done
column -t -s , /tmp/kv-sweep.csv | head
```

## All flags

### Workload (8)

| Flag | Default | Meaning |
| --- | --- | --- |
| `-t / --threads` | `1` | Worker thread count. Default 1 = single-thread baseline; pass `nodeCpus` to saturate the pinned NUMA node. |
| `-n / --keys` | `100_000_000` | Number of unique keys. |
| `-v / --value-size` | `100` | Value length in bytes. Range: **8 ≤ value-size ≤ 4096** (inline-value path only). |
| `--rumd` | `100,0,0,0` | Percent of [reads, upserts, RMWs, deletes] (sum=100). When `d% > 0`, deletes auto-reinsert. |
| `-d / --distribution` | `uniform` | `uniform` or `zipf`. |
| `--zipf-theta` | `0.99` | Zipf skew parameter. |
| `--runsec` | `30` | Run-phase duration in seconds (excludes warmup). |
| `--warmup-sec` | `5` | Warmup duration in seconds, discarded from results. `0` disables. |

### Reproducibility (2)

| Flag | Default | Meaning |
| --- | --- | --- |
| `-s / --seed` | `211` | Base RNG seed. Per-thread seeds = `SplitMix64(seed, threadIdx)`. Same seeds every iteration → workload is bit-deterministic across iterations (for read-only RUMD). |
| `-i / --iterations` | `1` | Run-phase iterations. Load runs once; warmup runs once per iteration. |

### Sizing (4)

| Flag | Default | Meaning |
| --- | --- | --- |
| `--hashpack` | `2.0` | Hash packing factor. `index_size_requested = (long)(keys / hashpack) << 6`. Note the engine rounds **down** to the nearest power of 2, so the effective hashpack is typically higher than configured — both `requested` and `applied` are emitted in metadata. |
| `--log-memory` | auto | Total in-memory log window (e.g. `16GB`). Auto-default = `NextPow2(keys × recordSize / 0.9)`, capped at 70 % of host MemAvailable. Explicit values bypass the RAM cap. |
| `--page-size` | `16MB` | Page size (e.g. `16MB`, `4MB`). |
| `--segment-size` | `1GB` | On-disk segment size. |

### Device (5)

| Flag | Default | Meaning |
| --- | --- | --- |
| `--device` | `default` | `native`, `randomaccess`, `filestream`, `null`, `default`. |
| `--device-throttle` | `0` | Max in-flight IOs. `0` = device default (`120` for every Tsavorite device). |
| `--device-io-backend` | `default` | Linux native backend: `libaio`, `uring`, `default` (→ libaio). |
| `--device-completion-threads` | `0` | Native completion thread count. `0` → 1. |
| `--data-path` | OS temp | Where hlog files live. A unique `<data-path>/kv-run-<ts>-<pid>/` child directory is created per run and removed on exit. |

### Host tuning (3)

| Flag | Default | Meaning |
| --- | --- | --- |
| `--no-numa-pin` | off | Disable in-process NUMA pinning. |
| `--numa-node` | `0` | Which NUMA node to pin to. |
| `--no-threadpool-tune` | off | Disable auto `ThreadPool.SetMinThreads(max(t*2, 256))` (and the matching restore-on-exit). |

### Output / hygiene (5)

| Flag | Default | Meaning |
| --- | --- | --- |
| `--report-interval-sec` | `1` | Live throughput tick (seconds). `0` disables — **recommended for canonical numbers**. |
| `--validate` | off | After load: single-threaded readback of every key. Aborts (exit 2) on mismatch. |
| `--json-output FILE` | none | Append JSON summary rows to FILE. |
| `--csv-output FILE` | none | Append CSV rows to FILE. |
| `--quiet` | off | Suppress human-readable progress/config; final results still print. |

### Hard-coded (no flag)

- `MutableFraction = 0.9` (KVSettings default).
- `PreallocateLog = true` (in-memory log pages physically committed at startup).
- `O_DIRECT` / `FILE_FLAG_NO_BUFFERING` on managed devices.
- Run-dir scoped cleanup (no recursive delete of `--data-path`).
- Session context: **`BasicContext` only** (safe path — per-op epoch resume/suspend is included in every measurement).
- Synthetic data only.
- `ObjectAllocator` only.

### Hot-loop architecture (why throughput stays close to YCSB.benchmark)

At startup the engine **dispatches once** to one of several focused per-workload worker methods, each small enough that the JIT inlines `BasicContext.Read` into the hot-loop body rather than emitting an out-of-line call. Currently:

- `RunReadOnlyUniformFast32` — pure-read uniform 32-bit keys (the canonical reference baseline). Minimal body; `BasicContext.Read` is fully inlined (verified via `DOTNET_JitDisasm`).
- `RunGeneralRumd` — general RUMD path (mixed reads/upserts/RMWs/deletes, zipf, 64-bit key counts).

This matters because at single-thread on a 100 M-key workload, the difference between an inlined `BasicContext.Read` and an out-of-line call is roughly **30–40 % throughput** — much more than the per-op work in the call itself, because the inlined version lets the JIT keep arguments in registers and skip a full call-prologue/epilogue.


## Output schema

### Human-readable

The startup **config block** echoes every resolved flag plus NUMA placement.

Per-phase line shape:
```
[<phase>][optionally <iter>] <ops> ops in <sec> s  (<ops/sec>)  hits=N misses=N reinserts=N overshoot=N exit-lag=Xms gc=g0/g1/g2 alloc/wkr=Nbytes
```

After the last iteration, an `[aggregate]` line with `mean`, `stdev`, `stdev%`,
`min`, `max`, and (when `-i ≥ 3`) `trimmed`.

### JSON (`KV-RESULT-JSON: …`)

One self-contained JSON object per phase (load / each run iteration) on stdout
(also appended to `--json-output FILE` if set). After the last iteration, an
aggregate row with `phase: "aggregate"`. Schema is `schema_version: "1"`.

Top-level fields include: `phase`, `iteration`, `ops_per_sec`, `elapsed_sec`,
`total_ops_for_throughput`, `final_total_ops`, `overshoot_ops`,
`max_worker_exit_lag_ms`, `hits`, `misses`, `deletes_reinserted`, `interrupted`,
`error`, `log.{begin,head,readonly,tail}_address`, `gc_delta.{gen0,gen1,gen2,alloc_bytes_by_worker_max}`,
`config.*` (every flag's resolved value), `host.*` (hostname, OS, dotnet version,
git sha, NUMA node, worker CPU mask, Server GC, GC latency mode, tiered compilation,
THP mode, data path, RAM), and `argv` (the exact command-line that produced the row).

### CSV (`--csv-output FILE`)

Same fields, wide schema, one row per phase per iteration plus an aggregate row.
Header is written automatically the first time the file is created.

### Headline throughput calculation

`ops/sec` is computed from a **scoreboard snapshot taken immediately after
`done = true` is published**, divided by `doneTicks - startTicks`. The
post-join sum (`final_total_ops`) may be slightly larger because workers
flush one last `localOps` after observing `done` at their next chunk boundary —
that delta is reported as `overshoot_ops` for diagnostics, never as part of
the headline number. This excludes worker-join lag from the measured duration.

## Determinism

With `--rumd 100,0,0,0` (pure reads), `--device null`, the same `--seed`,
the same `--threads`, and the same `--keys`, every iteration touches exactly
the same set of keys in the same order against an unchanging store. Any
delta in `ops_per_sec` between iterations is OS scheduling noise on top of
an otherwise identical workload — this is the "stable mean ± stdev" target
and typically produces stdev% well under 1 %.

For mixed RUMD, the same key/op stream is replayed each iteration, but
writes from prior iterations remain visible.

## NUMA + ThreadPool

On Linux, `KvNumaPinning` reads `/sys/devices/system/node/node<N>/cpulist`,
intersects with `sched_getaffinity` (so cgroup CPU quotas are honored),
deduplicates hyperthread siblings, and pins each worker thread to one
physical core on the chosen node via `sched_setaffinity`. The setup/reporter
thread pins to the first **un-pinned** CPU on the same node, so it never
competes with a worker.

On Windows the helper uses `SetThreadGroupAffinity` with the same exclusion
semantics; best-effort on multi-processor-group hosts.

`--threads` is the only knob that controls reservation: when
`--threads < nodeCpus`, the remaining `nodeCpus - threads` CPUs are
naturally un-pinned and absorb Tsavorite IO completion threads, the
reporter, and runtime GC threads. The default `--threads 1` reserves
essentially the entire node — a single-thread baseline with massive
headroom.

**Always run with `numactl --membind=$N --cpunodebind=$N`** on multi-NUMA
Linux hosts for full memory locality. Without it, even with our CPU
pinning, the .NET allocator may place pages on the wrong node and you can
see ~2× variance between runs. Worked example:

```bash
# Two runs of the same config — first without numactl, second with.
dotnet KV.benchmark.dll -t 64 -n 50000000 --device null --runsec 5 -i 5
numactl --membind=0 --cpunodebind=0 \
  dotnet KV.benchmark.dll -t 64 -n 50000000 --device null --runsec 5 -i 5
```

`ThreadPool.SetMinThreads(max(threads * 2, 256))` is applied at startup and
**restored to the previous values on exit** (so the benchmark plays nicely
with larger test harnesses that keep the process alive afterward).
`--no-threadpool-tune` skips both.

## Device backends

| `--device` | Linux | Windows | Notes |
| --- | --- | --- | --- |
| `default` | `RandomAccessLocalStorageDevice` | `LocalStorageDevice` (IOCP) | Platform default. |
| `native` | `NativeStorageDevice` (libaio / io_uring) | `LocalStorageDevice` | Requires `liburing.so.2` to be installed on Linux even when `--device-io-backend libaio` is selected (the shipped `libnative_device.so` is linked against it). |
| `randomaccess` | `RandomAccessLocalStorageDevice` | same | Pure .NET `RandomAccess` API; no native deps. |
| `filestream` | `ManagedLocalStorageDevice` | same | Pure .NET `FileStream`; lowest performance, no native deps. |
| `null` | `NullDevice` | `NullDevice` | No I/O; for measuring engine-only throughput. |

`--device-throttle 0` resolves to the device default of 120 in-flight IOs.
`--device-completion-threads 0` resolves to 1.

## Sizing cheatsheet

| `--keys` × `--value-size` | Auto `--log-memory` | `--hashpack 2.0` → applied index |
| --- | --- | --- |
| 10 M × 100 B | 2 GB | 256 MB (4 M buckets, effective hashpack ≈ 2.50) |
| 100 M × 100 B | 16 GB | 2 GB (32 M buckets, effective hashpack ≈ 2.98) |
| 250 M × 100 B | 32 GB | 8 GB (128 M buckets, effective hashpack ≈ 1.95) |
| 1 G × 100 B | 128 GB (or clamped) | 16 GB (256 M buckets, effective hashpack ≈ 3.91) |
| 1 M × 16 B | 64 MB (floored at 2 × page) | 32 MB (512 K buckets) |

The auto-default `--log-memory` is capped at 70 % of `MemAvailable` on
Linux. Explicit `--log-memory` values are never clamped — let the OOM
killer be the final authority for "did you really mean that?".

## Troubleshooting

- **"Throughput much lower than expected on Linux"** → check `numa` line in
  the config block to verify pinning; check `worker_cpu_mask` in metadata;
  make sure you're running under `numactl --membind=$N` on multi-NUMA hosts;
  check `--device` is right for your goal (use `null` for engine-only).
- **"Process killed by OOM"** → reduce `--keys` or `--log-memory`. The auto-
  clamp only fires for auto-derived `--log-memory`, not for explicit values.
- **`--validate` reports mismatches** → likely a session-functions
  regression. The exit code is `2` and the message reports the first
  mismatch.
- **"Numbers vary wildly between runs"** → check `stdev%` in the
  `[aggregate]` line. Increase `-i`. Check whether the host is under other
  load (`mpstat`, `top`). For canonical numbers use
  `--report-interval-sec 0`.
- **"uring symbol not found"** → install `liburing2` (`sudo apt-get install
  liburing2`) or fall back to `--device randomaccess`. The shipped
  `libnative_device.so` has a load-time dependency on `liburing.so.2`
  regardless of `--device-io-backend`.
- **"Can't access `/proc/sys/vm/drop_caches`"** → this benchmark doesn't
  drop caches; it relies on `--warmup-sec` to stabilize hot state. Use
  `numactl --membind=0` + an external `echo 3 > /proc/sys/vm/drop_caches`
  between invocations if cold-start measurement is needed.

## Reproducing the numbers in this README

Numbers above were collected on `git rev-parse --short HEAD =
f0652b638` on Linux 6.8 / Ubuntu 24.04 / .NET 10, dual-socket
80-physical-core / 2-NUMA-node host with 540 GB RAM and a /DATA2
NVMe SSD (`ext4`). Each row used the exact command shown next to it,
preceded by `numactl --membind=0 --cpunodebind=0`.

## Related

- Tsavorite onboarding: <https://microsoft.github.io/garnet/docs/dev/onboarding>
- Garnet benchmark scripts: `benchmark/` at the repo root.
