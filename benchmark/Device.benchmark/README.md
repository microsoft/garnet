# Device.benchmark

A low-overhead random-read IOPS benchmark for Tsavorite's `IDevice` implementations
(Native, FileStream, RandomAccess). Useful for sanity-checking that a backend
reaches the raw NVMe ceiling, isolating IO-layer performance from upper-layer KV
overhead (cf. [KV.benchmark](../../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md)),
and comparing libaio vs io_uring on Linux.

The benchmark fills the configured file with a sector-aligned test pattern, then
spawns N worker threads. Each worker pulls a buffer from a per-thread pool,
generates a random sector offset, issues `device.ReadAsync(...)`, and recycles
the buffer in the completion callback. Workers start and end on a coordinated
event so the sample is taken on a steady-state in-flight window.

## Quick start

```bash
cd benchmark/Device.benchmark
dotnet build -c Release -f net10.0

# Linux: libaio against a real NVMe file, with sane defaults
numactl --membind=0 --cpunodebind=0 \
  dotnet bin/Release/net10.0/Device.benchmark.dll \
    --file-name /mnt/nvme/devbench.dat \
    --device-type Native --io-backend libaio \
    --file-size 17179869184 --sector-size 4096 \
    --batch-size 4096 --threads 8 --runtime 8 \
    --throttle-limit 128
```

Output:

```
Benchmark finished: 5103142 ok, 0 err, 5103142 submitted in 8.00 s, throughput: 637811.53 ops/sec
```

Throughput is computed from **successful completions only**. Errored ops are
reported separately with a per-`errorCode` histogram (see `--throttle-limit`
help text for the most common cause).

## Saturating NVMe at ~750K IOPS

Both Linux Native backends can reach the hardware ceiling on modern NVMe (the
test below was on a Dell P5600; `fio` 4K random-read reference for that device
is 749K IOPS). Recipe:

```bash
# Common setup: 16 GB file, 4 KB sectors, batch=4096, throttle 512 (a
# user-side back-pressure cap, not a kernel limit — see "Why these settings"
# below for the safety argument; tl;dr Little's Law keeps actual kernel
# in-flight ~45 on a 750K-IOPS NVMe with ~60 µs latency, well below the
# 128-slot per-context/per-ring depth, so 0 errors at 512).
FILE=/mnt/nvme/devbench.dat
COMMON="--file-name $FILE --file-size 17179869184 --sector-size 4096 \
        --segment-size 1073741824 --batch-size 4096 --runtime 8 \
        --throttle-limit 512 --device-type Native"

# libaio: a single kernel io_context is enough. The --completion-threads hint
# is honoured by uring but ignored by libaio in this build (sharding the libaio
# io_context was tested and empirically did nothing — kernel mutex is efficient
# at all tested loads), so pass 1 explicitly for clarity. 16 worker threads is
# the sweet spot; 32 is noise on this hardware.
numactl --membind=0 --cpunodebind=0 dotnet bin/Release/net10.0/Device.benchmark.dll \
  $COMMON --io-backend libaio --completion-threads 1 --threads 16
# → ~750K ops/sec

# io_uring: needs sharded rings (each completion thread gets its own io_uring).
# CT=1 caps at ~340K (user-space SpinLock around io_uring_get_sqe + submit);
# CT=4 already saturates the device; CT=8 is the peak. With CT=8 the total
# device-wide submission capacity is 8 × 128 = 1024 slots, so throt=512 is
# trivially safe even before Little's Law.
numactl --membind=0 --cpunodebind=0 dotnet bin/Release/net10.0/Device.benchmark.dll \
  $COMMON --io-backend uring --completion-threads 8 --threads 16
# → ~758K ops/sec
```

Wall clock per recipe: ~30 s (fill phase + one 8 s run + cleanup).

### Cookbook (copy-paste matrix: backend × completion threads × workers)

Measured on the Dell P5600 reference setup above; reproduces in ±2 % across runs.

| backend | --completion-threads | --threads | --throttle-limit | ops/sec |
|---|---|---|---|---|
| Native libaio | 1 | 8  | 128 | 638 K |
| Native libaio | 1 | 16 | 512 | **750 K** |
| Native libaio | 1 | 32 | 512 | 755 K |
| Native uring  | 1 | 16 | 512 | 342 K (single-ring SpinLock cap) |
| Native uring  | 4 | 16 | 512 | 745 K |
| Native uring  | 8 | 16 | 512 | **758 K** |
| Native uring  | 8 | 32 | 512 | 748 K |

### Why these settings

* `--throttle-limit 512` — a **user-side back-pressure cap**, NOT a kernel
  limit. The benchmark spins on `Throttle()` (returns true when in-flight
  exceeds this value) before each submission. On this NVMe at ~750 K IOPS
  with ~60 µs average latency, Little's Law says steady-state actual kernel
  in-flight ≈ 750000 × 60e-6 ≈ 45 — comfortably below the 128-slot per-libaio
  -io_context / per-io_uring-SQ ring depth (`kMaxEvents` in `file_linux.h`).
  Setting `--throttle-limit 512` is therefore safe (verified: 0 errors at
  throt = 128, 256, 512, 1024, t = 16/32) and high enough that back-pressure
  never gates the submit side. It is NOT a guarantee for slower devices: if
  device latency × IOPS rises near the ring depth (e.g., a slow SATA SSD at
  10 K IOPS × 5 ms = 50 in-flight on the bleeding edge of 128), drop the
  throttle. Setting it too high (~2048+ on this NVMe) lets burst submission
  overflow the 128-slot ring and produces `Status::IOError=4` (EAGAIN) errors
  — the benchmark catches these in the `code4=NNN` line. Setting it to 0
  (no throttle) is a guaranteed flood on any device.
* `--threads 16` — saturates the submit side without contending on the device.
  Going to 32 helps libaio marginally; going past 32 starts to lose throughput
  to scheduler overhead.
* `--completion-threads 8` for uring — each completion thread is bound 1:1 to
  its own io_uring (this is the only way to escape the per-ring user-space
  SpinLock around `io_uring_get_sqe + io_uring_prep_* + io_uring_submit`).
  CT=4 already saturates; CT=8 is the safe peak. With CT=8, total device-wide
  submission capacity is 8 × 128 = 1024 slots, so `--throttle-limit 512` is
  trivially safe even before the Little's Law argument.
* `--completion-threads 1` for libaio — the libaio path in this build always
  uses one io_context regardless of the hint (sharding it was tested and
  empirically did nothing because the kernel mutex is already efficient at all
  tested loads), so any value collapses to a single drainer at runtime. Pass 1
  explicitly so the intent is visible in scripts.
* `--batch-size 4096` — per-thread buffer pool depth. Smaller values
  prematurely starve workers in the `TryDequeue` spin loop; larger values
  inflate startup wall-clock for buffer pre-allocation with no throughput gain.
* `numactl --membind=0 --cpunodebind=0` — keeps the buffer pool, P/Invoke
  call sites, and worker stacks on the same NUMA node as the IO threads.
  Cross-node references drop throughput by 10-15 % on this hardware.

## Comparing IO layer vs full KV path

If KV.benchmark on the same hardware caps lower than what this benchmark shows,
the gap lives in the upper-layer Tsavorite pending-read path (xxHash, hash
bucket walk, `PendingContext` allocation, P/Invoke marshalling, completion
delegate dispatch), not in the device backend. On the reference setup,
KV.benchmark with the same backend tops out around 525 K ops/sec (~30 % below
the IO ceiling); the gap is `pending-read` per-op CPU + GC pressure.

## All flags

| flag | default | purpose |
|---|---|---|
| `--file-name` | `c:/data/test.dat` | Device-backing file path |
| `--file-size` | 1 GB | File size in bytes (`long`; use values like `17179869184` for 16 GB). **Must be a multiple of `1024 × --sector-size`** — the fill phase uses a 1024-sector temp buffer and throws `InvalidOperationException` at startup otherwise. |
| `--sector-size` | 512 | IO size for each read (also the alignment requirement under O_DIRECT) |
| `--segment-size` | 1 GB | Tsavorite segment size in bytes |
| `--device-type` | `Native` | `Native`, `FileStream`, `RandomAccess` |
| `--io-backend` | `default` | Linux Native only: `libaio`, `uring`, `default`. Unknown values are rejected at startup |
| `--throttle-limit` | 0 | Max in-flight ops (0 = no throttle; floods kernel ring under high QD — see help text) |
| `--completion-threads` | 0 | Background drainer count (0 = processor count on Windows, 1 on Linux) |
| `--batch-size` | 1024 | Per-thread buffer pool depth (comma-separated for sweep) |
| `--threads` | 1,2,4,8,16,32 | Worker thread counts to sweep (comma-separated) |
| `--runtime` | 15 | Seconds per (backend, threads, batch) point |

## Output schema

```
Benchmark finished: <ok> ok, <err> err, <submitted> submitted in <T> s, throughput: <X> ops/sec
  error breakdown: code<N>=<count> ...        # only printed when err > 0
```

* `ok` — completion callbacks fired with `errorCode == 0`. This is the
  throughput-source-of-truth.
* `err` — completion callbacks fired with non-zero `errorCode`. The most common
  one on Linux Native is `code4` (`Status::IOError = 4`), the generic
  Tsavorite Native error for "submission rejected by the kernel": libaio's
  `io_submit` returning != 1 (typically EAGAIN when the 128-slot io_context
  ring is full), or io_uring's `io_uring_get_sqe` returning null / submit
  failing (typically when the 128-slot SQ is full). Fix by tightening
  `--throttle-limit` — on a fast NVMe like the reference Dell P5600,
  Little's Law keeps steady-state in-flight ≈ 45 even with `--throttle-limit
  512`, so this is rarely hit there; on slower devices or with `--throttle-limit
  0` (no throttle) it's easy to flood the ring. If errors appear, halve
  `--throttle-limit` and re-measure.
* `submitted` — total `ReadAsync` calls issued. Equals `ok + err` for clean
  runs.
* `throughput` — `ok / T`. **Errored ops do not count.**

## Troubleshooting

| symptom | cause | fix |
|---|---|---|
| `error breakdown: code4=...` and throughput < expected | Native `Status::IOError` — libaio io_context full (`io_submit` EAGAIN) or io_uring SQ full (`get_sqe` null / submit failure) | Tighten `--throttle-limit`. On fast NVMe ≥ 128 (e.g., 512) is normally safe; halve until errors disappear, then re-measure |
| Unknown `--io-backend 'foo'` | typo | use one of `default`, `libaio`, `uring` |
| `--file-size` rejected with `must be a perfect multiple of the temporary buffer size (1024 sectors)` | fill phase only handles 1024-sector-multiple files | round `--file-size` to a multiple of `1024 × --sector-size` (e.g., 4 MB-aligned for 4K sectors) |
| Throughput half of fio reference | cross-NUMA references | wrap with `numactl --membind=0 --cpunodebind=0` |
| io_uring saturates around 340 K | per-ring SpinLock cap | `--completion-threads 4` or `8` (sharded rings) |

## Related

* [KV.benchmark](../../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md) — full Tsavorite KV throughput benchmark.
* [Tsavorite Native device backends](../../libs/storage/Tsavorite/cs/src/core/Device/NativeStorageDevice.cs) — Linux libaio / io_uring implementations.
