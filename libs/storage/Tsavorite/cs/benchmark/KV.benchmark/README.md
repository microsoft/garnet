# KV.benchmark

Throughput benchmark for the Tsavorite key-value store. Measures **load** (insert)
and **run** (RUMD = reads / upserts / RMWs / deletes) throughput on an 8-byte-key +
fixed-value dataset via the safe `BasicContext` path. Built to reflect engine
performance with minimal benchmark noise (zero per-op alloc, NUMA-pinned workers,
false-sharing-free scoreboard, central tick timing).

It sits one layer above [Device.benchmark](../Device.benchmark/README.md)
(raw IDevice IOPS) and below [Resp.benchmark](../../../../../../benchmark/Resp.benchmark/README.md)
(full RESP server): each layer adds per-op work on top of the one below it. The
three benchmarks use different datasets and configurations, so their absolute
numbers are not directly comparable.

## Build & run

```bash
dotnet build libs/storage/Tsavorite/cs/benchmark/KV.benchmark/KV.benchmark.csproj -c Release -f net10.0
KV=libs/storage/Tsavorite/cs/benchmark/KV.benchmark/bin/Release/net10.0/KV.benchmark.dll

# In-memory smoke test (null device = pure engine ceiling)
dotnet $KV -t 1 -n 1000000 -v 100 --device null --rumd 100,0,0,0 --runsec 5
```

Measure on a **Release** / net10.0 build. Set `DOTNET_gcServer=1` for high thread
counts (Server GC scales past ~8 threads). Run `dotnet $KV --help` for all flags.

## The three scenarios

Same dataset (100 M × 100 B), three setups distinguished by **where reads land**.
All NUMA-pin (`numactl --cpunodebind=0 --membind=0`). Pinning matters most where
reads are served from RAM (scenarios 1 and 3), since remote-DRAM latency then gates
throughput directly; in the disk-bound scenario NVMe latency dominates and pinning
is within run-to-run noise. Common tail: `-v 100 --rumd 100,0,0,0 -i 3` (`-i 3` = 3
iterations; use the `trimmed` mean). Scenarios 1 and 3 measure with `--runsec 15
--warmup-sec 5`; the disk-bound scenario needs a longer window (`--runsec 25
--warmup-sec 5`) to reach steady state — at 12 s it reads ~10% low.

### 1. Memory-bound — pure engine ceiling, no IO

`--device null` and a log auto-sized to hold the dataset, so every read is served
from RAM. This is the Tsavorite upper bound.

```bash
numactl --cpunodebind=0 --membind=0 dotnet $KV -t 32 -n 100000000 \
  --device null -v 100 --rumd 100,0,0,0 --runsec 15 --warmup-sec 5 -i 3
```

### 2. NVMe storage-bound — reads hit real disk

A small `--log-memory 16m` keeps ~0.125% of the dataset in RAM, so every read is a
random NVMe fetch through the pending-read path. On a fast array set
`--device-completion-threads 8` (the default is 1). The Native throttle already
defaults to 4096, sized for a fast NVMe queue; the managed devices
(`randomaccess`/`filestream`) default to 120 and need `--device-throttle-limit 512`
to spin up. Reference host: **8×NVMe RAID-0** (`/raid`, `fio` random-read ceiling
≈ **8.24 M IOPS at 4 K** / **8.20 M at 512 B** — the array is IOPS-bound, so block
size barely moves it); KV peaks at **~7.7 M** (≈ 94% of `fio`), close to the
raw-device ceiling measured in
[Device.benchmark](../Device.benchmark/README.md#nvme-storage-bound).

```bash
# libaio:
numactl --cpunodebind=0 --membind=0 dotnet $KV -n 100000000 -v 100 \
  --device native --device-io-backend libaio --device-throttle-limit 4096 \
  --device-completion-threads 8 --log-memory 16m --page-size 4m --segment-size 1g \
  --rumd 100,0,0,0 --load-threads 8 --run-threads-sweep 8,32,64 \
  --runsec 25 --warmup-sec 5 -i 3 --data-path /raid/kv

# uring: swap in --device-io-backend uring. No extra flag needed — the smart
# default sizes rings to min(2×cores, 64), covering these run-thread counts (see
# Device README); the uring rows below use it.
```

Trimmed means of 3 iterations:

| backend | pin | t=8 | t=32 | t=64 |
|---|---|---|---|---|
| libaio | node-0 | 2.37 M | 6.86 M | **7.71 M** |
| libaio | none | 2.39 M | 6.90 M | **7.65 M** |
| uring | node-0 | 2.30 M | **7.62 M** | 7.23 M |
| uring | none | 2.27 M | **7.72 M** | 7.37 M |

libaio scales through **t=64** and peaks there (~7.7 M); uring peaks at **t=32**
(~7.6–7.7 M) and eases ~5% by t=64. Pinned and unpinned rows differ by at most ~2%,
inside run-to-run noise. Swap
`--device native` → `randomaccess` (BCL async, slower) / `filestream` (slowest) to
compare backends. Compare to the device's `fio` ceiling (`--rw=randread --bs=4k
--direct=1 --ioengine=libaio --iodepth=64 --numjobs=8`).

> Confirm it's truly device-bound: on a big-RAM host the 12.8 GB dataset fits in the
> page cache, but the device opens with `O_DIRECT`, so reads bypass it. During the run
> `iostat -x 1` should show `nvme r/s ≈ ops/sec` and `aqu-sz ≈ --device-throttle-limit`; on a
> shared box use per-process `/proc/<pid>/io` `read_bytes` (excludes other tenants).

### 3. Memory-device-bound — reads hit the in-RAM device

Same as (2) but `--device localmemory`, a syscall-free RAM-backed `IDevice`. Reads
still go through the full pending-read path (hash walk, `OperationState`, completion
dispatch) but with **zero disk latency**, isolating engine per-op CPU/GC. It stays
below the
[Device.benchmark LocalMemory ceiling](../Device.benchmark/README.md#memory-device-bound-localmemory)
(which excludes the KV path).

The device copies the record on the completion thread, so a drainer pool smaller than
the run-thread count gates throughput: with `--device-completion-threads 8` this peaks
at ~3.4 M, below scenario 2. `--device-inline-completion` completes on the issuing
thread and removes that bottleneck (~19.6 M at t=32) — that is the configuration that
measures the engine's pending-read path rather than the drainer pool.

```bash
numactl --cpunodebind=0 --membind=0 dotnet $KV -n 100000000 -v 100 \
  --device localmemory --device-inline-completion \
  --log-memory 16m --page-size 4m --segment-size 1g \
  --rumd 100,0,0,0 --load-threads 8 --run-threads-sweep 1,2,4,8,16,32 \
  --runsec 15 --warmup-sec 5
```

**Variations** (any scenario): `-d zipf --zipf-theta 0.99` (skew), `--rumd 50,40,5,5`
(mixed reads/upserts/RMWs/deletes), `--load-threads N --run-threads-sweep 1,2,4,...`
(one load → many run thread-counts). Use **100 M+** keys for disk runs — smaller
datasets touch few NAND dies and understate IOPS.

## Key knobs

- **`--device`** — `null` (no IO, pure engine), `localmemory` (RAM device),
  `randomaccess` / `native` (real disk; native = libaio/uring via
  `--device-io-backend`), `filestream` (slowest).
- **`--log-memory`** — in-memory log window. Auto-sized to fit the dataset (reads
  stay in memory). Set small (`16m`) to force disk/device spill. Units: `512m`,`16g`.
- **`--device-throttle-limit`** — max in-flight IOs. Native defaults to 4096 (sized for a
  fast array); the managed devices (`randomaccess`/`filestream`) default to 120, which
  leaves a fast device idle — raise to **512** on a single NVMe or **4096** on a
  multi-drive array to reach peak IOPS.
- **`--device-completion-threads`** — native/localmemory drainer count (**8** on a
  fast array; localmemory: one SPSC ring per thread).
- **`--device-io-contexts`** — kernel io_contexts / io_uring rings (native, decoupled
  from drainers). Leave default for libaio. For **uring** the smart default sizes rings
  to `min(2 × cores, 64)`, enough for ≤ 64 submitters, so leave it unset in the common
  case; set it **>= run threads** only beyond 64 (or to pin an exact count) — rings below
  the submitter count cap uring well below libaio (see
  [Device README](../Device.benchmark/README.md#nvme-storage-bound)).
- **`-b` / `--batch-size`** — run-phase batch depth (ops issued per chunk before an
  opportunistic non-blocking drain). Default 1024. It sets the **per-thread buffer-rent
  burst**: a thread rents one read buffer per op in the chunk before returning any, so
  the batch size is what the buffer pool's per-thread reuse must cover. In-flight is
  still bounded by `--device-throttle-limit`.
- **`-n` keys / `-v` value-size / `--rumd` mix / `-t` threads / `-d` distribution.**

## Output

Human-readable `[load]` / `[run N]` / `[aggregate]` lines (throughput, mean/stdev,
and a `trimmed` mean for `-i ≥ 3`). Add `--json-output FILE` / `--csv-output FILE`
for machine-readable rows, `--report-interval-sec 0` for clean canonical numbers,
`--validate` for a post-load key readback (exit 2 on mismatch).

## Related

- [Device.benchmark](../Device.benchmark/README.md) — raw IDevice IOPS (layer below).
- [Resp.benchmark](../../../../../../benchmark/Resp.benchmark/README.md) — full Garnet RESP server (layer above).
