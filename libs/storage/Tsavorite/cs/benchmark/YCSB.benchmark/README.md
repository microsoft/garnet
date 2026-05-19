# Tsavorite YCSB Benchmark

A YCSB-style workload generator for measuring Tsavorite's insert (load) and read/update/RMW/delete (run) throughput
under different device backends, log topologies, and threading configurations.

## Quick start

```bash
# Build
dotnet build libs/storage/Tsavorite/cs/benchmark/YCSB.benchmark/YCSB.benchmark.csproj -c Release -f net10.0

# Smallest in-memory test (synthetic data, 4 threads, 5-second run, null device, ~100k keys)
dotnet libs/storage/Tsavorite/cs/benchmark/YCSB.benchmark/bin/Release/net10.0/YCSB.benchmark.dll \
    -b 1 -t 4 --synth --sm --runsec 5 \
    --rumd 50,50,0,0 --load-keys 100000 \
    --device null --data-path /tmp/ycsb
```

Output lines (most actionable):
* `##00; ins/sec: ...` — load (insert) throughput from the setup phase
* `##03; ops/sec: ...` — run (RUMD) throughput from the experiment phase
* `##10; ins/sec: ...; stdev: ...` — final averaged load throughput across iterations
* `##11; ops/sec: ...; stdev: ...` — final averaged run throughput
* `##12; TailAddress (abs): ...` — final log tail address (bytes written)

## Common scenarios

### 1. Pure in-memory benchmark (no disk I/O ever)

Use `--device null`, large `--log-memory`, and `--mutable-fraction 1.0` (so the log never flushes).

```bash
dotnet ./bin/Release/net10.0/YCSB.benchmark.dll \
    -b 1 -t 64 --synth \
    --rumd 100,0,0,0 --runsec 10 \
    --device null --log-memory 64GB --page-size 32MB --mutable-fraction 1.0 \
    --threadpool-min 512
```

### 2. Small-memory disk write benchmark (insert-bound)

`--sm` sets page-size=4MB and log-memory=64MB so the log is constantly flushing and the inserter is gated on disk
throughput. This is the canonical "is my write path saturating the device?" test.

Linux (libaio):
```bash
dotnet ./bin/Release/net10.0/YCSB.benchmark.dll \
    -b 1 -t 64 -i 3 --synth --runsec 5 \
    --rumd 100,0,0,0 --sm \
    --device native --device-throttle 120 --device-completion-threads 1 \
    --threadpool-min 512 \
    --data-path /mnt/nvme/ycsb --cleanup-data-files
```

Windows (Win32 IOCP):
```powershell
dotnet .\bin\Release\net10.0\YCSB.benchmark.dll `
    -b 1 -t 64 -i 3 --synth --runsec 5 `
    --rumd 100,0,0,0 --sm `
    --device native --device-throttle 120 `
    --data-path D:\ycsb --cleanup-data-files
```

### 3. Compare device backends

Run the same workload across all backends and dump rows into one CSV:

```bash
for dev in native randomaccess filestream null; do
    dotnet ./bin/Release/net10.0/YCSB.benchmark.dll \
        -b 1 -t 64 -i 3 --synth --runsec 5 \
        --rumd 100,0,0,0 --sm \
        --device $dev --device-throttle 120 \
        --threadpool-min 512 \
        --data-path /mnt/nvme/ycsb --cleanup-data-files \
        --csv-output /tmp/ycsb-devices.csv \
        --print-config false
done
```

CSV columns: `phase, benchmark, threads, allocator, device, page_size, log_memory, mutable_fraction, ins_per_sec, ops_per_sec, tail_address, timestamp_utc`.

### 4. Load once, run many (recover mode)

Useful for tuning read-path or RUMD parameters without paying the load cost each time. The store is checkpointed
under `<data-path>/<distribution>_<synthetic|ycsb>_<key-count>` after the first load.

```bash
# First run: load + checkpoint
dotnet ./bin/Release/net10.0/YCSB.benchmark.dll \
    -b 1 -t 64 --synth --runsec 10 \
    --rumd 50,50,0,0 \
    --device native --data-path /mnt/nvme/ycsb \
    --cleanup-data-files -k

# Subsequent runs: skip load, only run RUMD
dotnet ./bin/Release/net10.0/YCSB.benchmark.dll \
    -b 1 -t 64 -i 5 --synth --runsec 10 \
    --rumd 50,50,0,0 \
    --device native --data-path /mnt/nvme/ycsb \
    -k --phase run
```

### 5. Reproducibility hygiene for repeat runs

Cold-start every iteration by clearing the OS page cache and prior hlog files:

```bash
dotnet ./bin/Release/net10.0/YCSB.benchmark.dll \
    -b 1 -t 64 -i 5 --synth --runsec 5 \
    --rumd 100,0,0,0 --sm \
    --device native --data-path /mnt/nvme/ycsb \
    --cleanup-data-files --drop-page-cache
```

Note: `--drop-page-cache` is Linux-only and requires root (silently no-ops otherwise — try `sudo`).

### 6. Allocator + value-shape comparison

```bash
# SpanByteAllocator (variable-length values, raw bytes)
dotnet ./bin/Release/net10.0/YCSB.benchmark.dll -b 1 -t 32 --synth --sm --sba

# ObjectAllocator (default; supports overflow + object values)
dotnet ./bin/Release/net10.0/YCSB.benchmark.dll -b 1 -t 32 --synth --sm

# Object benchmark (longer values, exercises overflow path)
dotnet ./bin/Release/net10.0/YCSB.benchmark.dll -b 2 -t 32 --synth --sm --ovf
```

### 7. Stable throughput measurement on multi-NUMA hosts

On a multi-socket / multi-NUMA-node Linux box (check with `numactl --hardware`), Tsavorite throughput can vary
**~2×** between runs depending on which NUMA node the OS happens to allocate memory and schedule threads on.
Cross-NUMA loads pay ~2× memory latency (e.g. 20 vs 10 cycles per the `node distances` matrix).

Pin both CPU and memory to a single node with `numactl` for repeatable measurements. The commands below are
the exact recipes that produced the numbers in the result table further down.

```bash
DATA=/path/to/nvme/data          # any local NVMe with >32GB free
BENCH=./bin/Release/net10.0/YCSB.benchmark.dll

# (a) Pure in-memory ceiling, NUMA-pinned (no disk, large mutable log)
rm -f $DATA/hlog*
numactl --cpunodebind=0 --membind=0 \
    dotnet $BENCH -b 1 -t 64 -i 7 --synth --rumd 100,0,0,0 --runsec 1 \
        --device null --log-memory 16GB --page-size 4MB --mutable-fraction 0.9 \
        --threadpool-min 512 --data-path $DATA --cleanup-data-files \
        --load-keys 50000000 --print-config false

# (b) Disk-stressed --sm, NUMA-pinned (libaio, 64MB log → constant flushing)
rm -f $DATA/hlog*
numactl --cpunodebind=0 --membind=0 \
    dotnet $BENCH -b 1 -t 64 -i 7 --synth --rumd 100,0,0,0 --runsec 1 --sm \
        --device native --device-throttle 120 --device-completion-threads 1 \
        --threadpool-min 512 --data-path $DATA --cleanup-data-files \
        --load-keys 50000000 --print-config false

# (c) Cross-NUMA baseline (no pinning) — shows the variance you avoid by pinning
rm -f $DATA/hlog*
dotnet $BENCH -b 1 -t 64 -i 7 --synth --rumd 100,0,0,0 --runsec 1 \
    --device null --log-memory 16GB --page-size 4MB --mutable-fraction 0.9 \
    --threadpool-min 512 --data-path $DATA --cleanup-data-files \
    --load-keys 50000000 --print-config false

# (d) Sweep LogMemorySize to find the cache-bandwidth knee
#     (smaller logs keep the active page set in L3 → faster inserts/sec)
for logmem in 64MB 256MB 1GB 4GB 16GB 64GB; do
    rm -f $DATA/hlog*
    numactl --cpunodebind=0 --membind=0 \
        dotnet $BENCH -b 1 -t 64 -i 1 --synth --rumd 100,0,0,0 --runsec 1 \
            --device null --log-memory $logmem --page-size 4MB --mutable-fraction 0.9 \
            --threadpool-min 512 --data-path $DATA --cleanup-data-files \
            --load-keys 50000000 --print-config false 2>&1 | grep "^##00;"
done
```

Read the **trimmed mean** (`##20; ins/sec:`) from the `-i 7` runs — it averages the middle 5 of 7 iterations
and is far more stable than any individual `##00; ins/sec` line.

Example results from one Linux box (2 × 80-core sockets, 2 NUMA nodes, NVMe SSD, default `--mutable-fraction 0.9`):

| Experiment | Trimmed mean (`##20`) | stdev % | Notes |
| --- | --- | --- | --- |
| (a) In-memory, NUMA-pinned (warm steady state) | **7.5M ins/sec** | low | Last 2 iters of a 7-iter run |
| (a) In-memory, NUMA-pinned (all 7 iters) | 6.4M ins/sec | 15% | Includes cold first iters |
| (b) `--sm` libaio, NUMA-pinned | **3.9M ins/sec** | **1.5%** | Disk-bound, very stable |
| (c) In-memory, **no** NUMA pinning | 2.95M ins/sec | 16% | Cross-socket memory traffic |
| (c) `--sm`, no NUMA pinning | 3.47M ins/sec | 10% | Cross-socket memory traffic |

Two non-obvious findings worth keeping in mind when tuning:

1. **Always pin** on a multi-NUMA box, or expect 2× variance between runs of identical config.
2. **Larger log memory is not always faster.** With a 16GB log, every insert lands on a fresh cache line over
   a 6.4GB working set → every insert misses L3. A small `--sm` log (64MB) keeps the active pages hot in cache
   even though it triggers continuous flushes. The ceiling is achieved either way; the *median* shape depends
   on workload size relative to L3.

## Flag reference

### Workload shape

| Flag | Default | Description |
| --- | --- | --- |
| `-b, --benchmark` | `0` | 0 = FixedLen, 1 = SpanByte, 2 = Object, 3 = ConcurrentDictionary |
| `-t, --threads` | `8` | Worker thread count for load + run phases |
| `-i, --iterations` | `1` | Iterations of the experiment (results averaged in `##10/##11`) |
| `-d, --distribution` | `uniform` | `uniform` or `zipf` |
| `-s, --seed` | `211` | RNG seed for synthetic data |
| `--rumd a,b,c,d` | `50,50,0,0` | Percent of [reads, upserts, RMWs, deletes] in the run phase |
| `--synth` | off | Use synthetic data instead of YCSB file (`./ycsb_files/`) |
| `--sd` | off | Small-data: 4.6M init / 10M txn keys (default 250M / 1B) |
| `--load-keys N` | `0` (= default) | Override the load-phase key count (rounded to 640-key chunks) |
| `--runsec N` | `30` | Run-phase duration in seconds |
| `--phase` | `both` | `load` (insert only), `run` (RUMD only; requires `-k`), `both` |
| `--di` | off | Delete + immediate re-insert |

### Storage topology

| Flag | Default | Description |
| --- | --- | --- |
| `--page-size SIZE` | (allocator default) | e.g. `4MB`, `32m`, `8192`. Overrides default and `--sm`. |
| `--log-memory SIZE` | (allocator default) | Total in-memory log window, e.g. `64GB`, `256m` |
| `--segment-size SIZE` | `max(page,log)` | On-disk segment size, e.g. `1GB` |
| `--mutable-fraction F` | `0.9` | Fraction of log memory kept mutable. `1.0` = no flushing |
| `--preallocate-log` | `true` | Pre-allocate all log pages on startup |
| `--hashpack F` | `2.0` | #keys / F = hash bucket count (default packing) |
| `--sm` | off | Shorthand: small-memory log (page=4MB, log-memory=64MB) |
| `--ovf` / `--obj` | off | Force overflow- or object-valued records (Object benchmark only) |

### Device backend

| Flag | Default | Description |
| --- | --- | --- |
| `--device TYPE` | `default` (= platform pick) | `native` (libaio/IOCP), `randomaccess`, `filestream`, `null`, `default` |
| `--device-throttle N` | `0` (= device default) | Max concurrent IOs in flight |
| `--device-io-backend B` | `default` | Native backend on Linux: `default` (= libaio), `libaio`, or `uring`. Ignored when `--device` is not `native`. |
| `--device-completion-threads N` | `0` (= 1) | Native completion thread count (Linux libaio + uring) |
| `--use-os-cache` | off | Allow OS page cache on managed devices (off = `O_DIRECT` / `FILE_FLAG_NO_BUFFERING`) |
| `--data-path DIR` | `D:/data/TsavoriteYcsbBenchmark` | Where hlog files and checkpoints live |

Notes:
* `native` on Linux uses `NativeStorageDevice` (libaio with P/Invoke); on Windows uses `LocalStorageDevice` (Win32
  overlapped I/O on IOCP). Both bypass the OS page cache by default.
* `randomaccess` is the default on Linux when `--device default`. `native` is the default on Windows.
* `--device-throttle` is the highest-impact tuning knob for write-bound runs; start at `8 × #threads` and tune up.

### Runtime

| Flag | Default | Description |
| --- | --- | --- |
| `--threadpool-min N` | `0` (no override) | Raise `ThreadPool.SetMinThreads` to N (worker + completion). Useful with `-t 64` |
| `-n, --numa` | `0` | NUMA sharding (Windows only). `1` = shard threads across sockets |

### Reproducibility / hygiene

| Flag | Default | Description |
| --- | --- | --- |
| `--cleanup-data-files` | off | Delete any pre-existing `hlog*` files in `--data-path` before the run |
| `--drop-page-cache` | off | Linux-only: write `3` to `/proc/sys/vm/drop_caches` before the run (requires root) |
| `-k, --recover` | off | Recover from checkpoint if present, else load + checkpoint |
| `--chkptms N` / `--chkptsnap` | off | Periodic checkpointing during the run phase |

### Output

| Flag | Default | Description |
| --- | --- | --- |
| `--print-config` | `true` | Print a single consolidated config block at start (`--print-config false` to silence) |
| `--csv-output FILE` | none | Append `phase,benchmark,threads,allocator,device,page_size,log_memory,mutable_fraction,ins_per_sec,ops_per_sec,tail_address,timestamp_utc` rows to FILE (creates with header if missing) |
| `--dumpdist` | off | Dump hash-bucket distribution after load |
| `--safectx` | off | Use safe context (slower, per-op epoch control) — for diagnostics only |

## Tuning cheatsheet

| Goal | Recommended starting flags |
| --- | --- |
| **Saturate NVMe writes (Linux)** | `-t 64 --sm --device native --device-throttle 120 --device-completion-threads 1 --threadpool-min 512` |
| **Pure-RAM read throughput** | `-t 64 --device null --log-memory 64GB --mutable-fraction 1.0 --rumd 100,0,0,0 --runsec 30 --threadpool-min 512` |
| **Hot working set (cache-friendly)** | `--sm` (forces 64MB memory, ~16 pages) |
| **Cold working set (re-load from disk)** | omit `--sm`, large `--log-memory`, `--cleanup-data-files --drop-page-cache` |
| **Stable median across iterations** | `-i 5` + `--csv-output runs.csv`, read `##10`/`##11` lines |
| **Stable measurement on multi-NUMA Linux** | Prefix with `numactl --cpunodebind=N --membind=N` (see scenario 7) |

## Data files

If you have YCSB data files (`load_uniform_250M_raw.dat`, `run_uniform_250M_1000M_raw.dat`, `load_zipf_*`,
`run_zipf_*`), drop them under `C:/ycsb_files`, `D:/ycsb_files`, or `E:/ycsb_files` and omit `--synth`. Otherwise
the loader falls back to synthetic uniform/zipf streams.

## Related

* `scripts/run_benchmark.ps1` — driver script for sweep runs (Windows)
* `scripts/compare_runs.ps1` — pivot CSV outputs into comparison tables
