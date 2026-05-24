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
  value-size       : 100 bytes (reader copies first 32 B)
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
**net10.0**. The project uses Workstation GC by default (the .NET runtime
default). Set `DOTNET_gcServer=1` in the environment when running with high
thread counts to switch to Server GC, which scales better past ~8 threads.
Debug builds are fine for correctness checks but not for measurement.

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
[run 1] 566,958,848 ops in 5.000 s  (113,386,218 ops/sec)  reads=566,974,720 writes=0 deletes=0 gc=0/0/0 alloc/wkr=4360B
[run 2] 567,287,296 ops in 5.000 s  (113,451,754 ops/sec)  reads=567,303,424 writes=0 deletes=0 gc=0/0/0 alloc/wkr=4328B
[run 3] 567,573,760 ops in 5.000 s  (113,509,656 ops/sec)  reads=567,589,888 writes=0 deletes=0 gc=0/0/0 alloc/wkr=4328B
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

### 9. Run-thread scalability sweep (single load → multiple thread counts)

```bash
numactl --membind=0 --cpunodebind=0 \
  dotnet KV.benchmark.dll -n 100000000 -v 96 \
    --load-threads 32 --run-threads-sweep 1,2,4,8,16,32 \
    --device null --rumd 100,0,0,0 \
    --runsec 15 --warmup-sec 5 -i 3
```

Loads the 100 M-key dataset ONCE using 32 threads, then runs the full
`--iterations` loop at each thread count in the sweep. The final summary
prints a compact table with trimmed mean, stdev%, and speedup vs the
smallest thread count:

```
  Run sweep (3 iterations per thread count):
    threads |        trimmed |           mean |  stdev% | speedup
    --------+----------------+----------------+---------+--------
          1 | 2.08 M ops/sec | 2.05 M ops/sec |    2.8% |   1.00×
          2 | 4.22 M ops/sec | 4.22 M ops/sec |    0.9% |   2.03×
          4 | 8.29 M ops/sec | 8.32 M ops/sec |    0.8% |   3.99×
          8 |16.53 M ops/sec |16.75 M ops/sec |    2.7% |   7.95×
         16 |33.94 M ops/sec |34.02 M ops/sec |    1.9% |  16.32×
         32 |69.94 M ops/sec |69.99 M ops/sec |    0.2% |  33.64×
```

## All flags

### Workload (10)

| Flag | Default | Meaning |
| --- | --- | --- |
| `-t / --threads` | `1` | Default run-phase worker count (also used for load if `--load-threads` is unspecified). |
| `--load-threads` | `0` | Threads to use for the LOAD phase. `0` = same as `--threads`. Useful when you want a fast parallel load followed by single-thread or sweep runs on the same dataset. |
| `--run-threads-sweep` | none | Comma-separated list of run-phase thread counts (e.g. `1,2,4,8,16,32`). When set, the engine loads ONCE and then runs the full `--iterations` loop for each thread count. Overrides `--threads` for the run phase. |
| `-n / --keys` | `100_000_000` | Number of unique keys. |
| `-v / --value-size` | `100` | Value length in bytes. Range: **32 ≤ value-size ≤ `--max-inline-value-size`** (inline-value path only). |
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
| `--page-size` | `16MB` | Page size (e.g. `16MB`, `4MB`). **Matches Garnet `defaults.conf` PageSize=16m**. |
| `--segment-size` | `1GB` | On-disk segment size. **Matches Garnet SegmentSize=1g**. |
| `--max-inline-value-size` | `16KB` | `KVSettings.MaxInlineValueSize` — values larger than this overflow to a separate heap object. **Matches Garnet `ValueOverflowThreshold=16k`**. |
| `--preallocate-log` | `false` | When `true`, pre-touches every log page at startup so first-touch faults don't bias the timed window. **Default `false` matches Garnet**; enable for the most stable single-thread benchmark numbers (cost: 6 s per 16 GB of log at setup). |

### Device (5)

| Flag | Default | Meaning |
| --- | --- | --- |
| `--device` | `default` | `native`, `randomaccess`, `filestream`, `null`, `default`. |
| `--device-throttle` | `0` | Max in-flight IOs. `0` = device default (`120` for every Tsavorite device). |
| `--device-io-backend` | `default` | Linux native backend: `libaio`, `default` (→ libaio). |
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
| `--json-output FILE` | none | Append **pretty-printed** JSON summary rows to FILE (one row per phase). |
| `--json-stdout` | off | Also emit single-line `KV-RESULT-JSON: {…}` blobs to stdout for log scraping. |
| `--csv-output FILE` | none | Append CSV rows to FILE. |
| `--quiet` | off | Suppress human-readable progress/config; final summary still prints. |

### Hard-coded (no flag)

- `MutableFraction = 0.9` (KVSettings default; matches Garnet `MutablePercent=90`).
- `MaxInlineKeySize = 128 B` (KVSettings default; matches Garnet).
- `O_DIRECT` / `FILE_FLAG_NO_BUFFERING` on managed devices.
- Run-dir scoped cleanup (no recursive delete of `--data-path`).
- Session context: **`BasicContext` only** (safe path — per-op epoch resume/suspend is included in every measurement).
- Synthetic data only.
- `ObjectAllocator` only.

### Hot-loop architecture

`RunWorkload` is a single per-thread method:

- **Per-op inline xorshift32 key gen** — uniform: `wk & (N-1)` if `N` is a power of two (1 cycle), otherwise Lemire's fast modulo `((ulong)wk * N) >> 32` (~5 cycles). 64-bit keyCount paths use plain `% N`.
- **Per-op independent xorshift32 coin toss** for op selection (read/upsert/RMW/delete). Two independent RNG states are MANDATORY when distribution=zipf: zipf consumes its source RNG non-uniformly, so reusing it for op-select would bias the rumd ratio.
- **Pre-computed op cutoffs** in the 32-bit RNG domain so the coin toss is `wr < cutoff` with no per-op multiply.
- **Chunk-boundary done check** — saves a per-op `Volatile.Read(ref doneFlag)`. Stop latency is bounded at one chunk (~0.3 ms at 2 M ops/sec).
- **`CompletePending(false)` every 512 ops** — JIT folds the literal `% 512` to a bitmask.
- **`Interlocked.Add(ref globalChunkIdx, kChunkSize)`** for chunk scheduling — at single thread it's no slower than a thread-local counter and at multi-thread it gives even chunk distribution across workers.
- **Hot-path buffers (`value`, `input`, `output`) are stackalloc'd in `WorkerProc`** and passed into `RunWorkload` by `ref`. This is measurably (~3.7 %) faster than declaring them inside `RunWorkload`: a smaller method body lets the JIT inline `BasicContext.Read` into the hot loop rather than emitting an out-of-line `call ContextRead`.
- **All three buffers are 32-byte aligned** via overallocate-then-round-up. Reader's `Slice(0, 32).CopyTo(...)` JITs to a single `vmovdqu ymm0, [src]` + `vmovdqu [dst], ymm0` (32-byte AVX2). The SOURCE (value bytes in the Tsavorite log) is always 5-byte unaligned within an 8-byte-aligned record (header is `RecordInfo(8) + NumIndicatorBytes(3) + KeyLen(1) + RecLen(1) + Key(8) = 21`), and with 120 B records the value start cycles `21,13,5,61,53,45,37,29` mod 64 — half of reads already cross a cache line on the source side, which can't be fixed without changing the log layout. Aligning the DESTINATION (output) and INPUT buffers to 32 B removes the dest-side cross-line penalty; measured ~+5 % at memory-bound scale (100 M keys, BasicContext, in-memory log).
- **`KvSessionFunctions.Reader` copies a constant 32 bytes** (one cache line). The `32` is a `const int`, not a `static readonly int`, so the JIT const-folds the `Slice(0, 32).CopyTo(...)` into a single SSE memcpy (≈15 % single-thread improvement over a non-const length).

At single thread on a 100M-key SpanByte workload (96-byte values, BasicContext, in-memory log), the difference between an inlined `BasicContext.Read` and an out-of-line call is roughly **30–40 % throughput** — much more than the per-op work in the call itself, because the inlined version lets the JIT keep arguments in registers and skip a full call-prologue/epilogue.


## Output schema

### Human-readable

The startup **config block** echoes every resolved flag plus NUMA placement.

Per-phase line shape:
```
[<phase>][optionally <iter>] <ops> ops in <sec> s  (<ops/sec>)  reads=N writes=N deletes=N overshoot=N exit-lag=Xms gc=g0/g1/g2 alloc/wkr=Nbytes
```

After the last iteration, an `[aggregate]` line with `mean`, `stdev`, `stdev%`,
`min`, `max`, and (when `-i ≥ 3`) `trimmed`, followed by a multi-line
`KV.benchmark — final summary` block that recaps config, load, and run perf
in a readable form.

### JSON

By default, NO JSON is written to stdout — the per-phase blob is huge and
clutters the terminal. Two opt-ins:

- `--json-output FILE` — appends a **pretty-printed** JSON object per phase
  (load / each run iter / final aggregate) to FILE. Good for archiving or
  diffing across runs.
- `--json-stdout` — also emits a single-line `KV-RESULT-JSON: {…}` blob per
  phase to stdout. Useful for log-scraping pipelines.

Schema is `schema_version: "1"`.

Top-level fields include: `phase`, `iteration`, `ops_per_sec`, `elapsed_sec`,
`total_ops_for_throughput`, `final_total_ops`, `overshoot_ops`,
`max_worker_exit_lag_ms`, `reads`, `writes`, `deletes`, `interrupted`,
`error`, `log.{begin,head,readonly,tail}_address`, `gc_delta.{gen0,gen1,gen2,alloc_bytes_by_worker_max}`,
`config.*` (every flag's resolved value), `host.*` (hostname, OS, dotnet version,
git sha, NUMA node, worker CPU mask, Server GC, GC latency mode, tiered compilation,
THP mode, data path, RAM), and `argv` (the exact command-line that produced the row).

The `reads` / `writes` / `deletes` counters are per-RESP-op (not per-record):
when `--rumd ...,d=N` is non-zero, every delete is immediately followed by a
re-insert (Upsert) which is counted under `writes`.

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

**GC quiescence**: between iterations the benchmark forces a full
Gen2 collection + finalizer pass + Gen2 collection again *after* all
workers have parked on the start gate but *before* the timed window
opens. This guarantees the per-iteration `gc_delta` reflects only
collections that actually happened during the measured window (almost
always `0/0/0` since the hot loop is allocation-free).

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
| `native` | `NativeStorageDevice` (libaio) | `LocalStorageDevice` | Linux uses libaio via the shipped `libnative_device.so`. |
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
