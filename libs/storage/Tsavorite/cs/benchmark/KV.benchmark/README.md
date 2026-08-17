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
is within run-to-run noise. Common tail: `-v 100 --rumd 100,0,0,0 --runsec 15
--warmup-sec 5 -i 3` (`-i 3` = 3 iterations; use the `trimmed` mean).

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
size barely moves it); KV peaks at **~6.3 M** (≈ 77% of `fio`). The gap is
Tsavorite managed per-op CPU (hash lookup, pending context, completion dispatch),
not the device, which reaches `fio` parity in
[Device.benchmark](../Device.benchmark/README.md#nvme-storage-bound).

```bash
# libaio:
numactl --cpunodebind=0 --membind=0 dotnet $KV -n 100000000 -v 100 \
  --device native --device-io-backend libaio --device-throttle-limit 4096 \
  --device-completion-threads 8 --log-memory 16m --page-size 4m --segment-size 1g \
  --rumd 100,0,0,0 --load-threads 8 --run-threads-sweep 8,32,64 \
  --runsec 12 --warmup-sec 4 -i 3 --data-path /raid/kv

# uring: swap in --device-io-backend uring. No extra flag needed — the smart
# default sizes rings to min(2×cores, 64), covering these run-thread counts (see
# Device README); the uring rows below use it.
```

Trimmed means of 3 iterations:

| backend | pin | t=8 | t=32 | t=64 |
|---|---|---|---|---|
| libaio | node-0 | 2.24 M | 5.57 M | **6.34 M** |
| libaio | none | 2.23 M | 5.67 M | **6.20 M** |
| uring | node-0 | 2.17 M | 5.98 M | **6.21 M** |
| uring | none | 2.17 M | 6.01 M | **6.21 M** |

Both backends scale through **t=64**, landing at ~6.2–6.3 M; uring leads at t=32
(~6.0 M vs ~5.6 M). Pinned and unpinned rows differ by at most ~2%, inside
run-to-run noise. Swap
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
dispatch) but with **zero disk latency**, isolating engine per-op CPU/GC. Sits
between (1) and (2), and below the
[Device.benchmark LocalMemory ceiling](../Device.benchmark/README.md#memory-device-bound-localmemory)
(which excludes the KV path).

```bash
numactl --cpunodebind=0 --membind=0 dotnet $KV -n 100000000 -v 100 \
  --device localmemory --device-completion-threads 8 \
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
  opportunistic non-blocking drain). Default 1024. In-flight is bounded by
  `--device-throttle-limit`, not by this, so it is largely throughput-neutral.
- **`-n` keys / `-v` value-size / `--rumd` mix / `-t` threads / `-d` distribution.**

## Output

Human-readable `[load]` / `[run N]` / `[aggregate]` lines (throughput, mean/stdev,
and a `trimmed` mean for `-i ≥ 3`). Add `--json-output FILE` / `--csv-output FILE`
for machine-readable rows, `--report-interval-sec 0` for clean canonical numbers,
`--validate` for a post-load key readback (exit 2 on mismatch).

## Related

- [Device.benchmark](../Device.benchmark/README.md) — raw IDevice IOPS (layer below).
- [Resp.benchmark](../../../../../../benchmark/Resp.benchmark/README.md) — full Garnet RESP server (layer above).
