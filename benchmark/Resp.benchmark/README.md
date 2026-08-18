# Resp.benchmark

End-to-end throughput / latency benchmark for the Garnet RESP server. Drives
GET/SET/INCR/MGET/... workloads against any RESP server (Garnet, Redis, KeyDB,
Dragonfly) over TCP, or against an in-process embedded Garnet server. It is the
top of the stack: **Resp ≤ [KV](../../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md)
≤ [Device](../../libs/storage/Tsavorite/cs/benchmark/Device.benchmark/README.md) ≤ fio**.

Two modes:
- **Offline** (`--op X`): pre-built request batches of size `-b`, N threads loop
  `Send → CompletePending`. Reports **throughput (ops/sec)**. Use to saturate.
- **Online** (`--online`): one in-flight op per thread (`--itp K` for more),
  per-op latency in an HdrHistogram printed every 2 s. Use for **latency curves**.

Clients (`--client`): `LightClient` (default offline, zero-alloc pipeliner),
`GarnetClientSession` (default online, async pipelined, `--itp`), `GarnetClient`,
`SERedis` (for apples-to-apples vs Redis), `InProc` (embedded, no TCP — server CPU only).

## Build & run

```bash
dotnet build benchmark/Resp.benchmark/Resp.benchmark.csproj -c Release -f net10.0
dotnet build main/GarnetServer/GarnetServer.csproj -c Release -f net10.0
RB=benchmark/Resp.benchmark/bin/Release/net10.0/Resp.benchmark.dll
GS=main/GarnetServer/bin/Release/net10.0/GarnetServer.dll

dotnet $GS --port 6379 &                                        # start server
dotnet $RB --op GET --dbsize 1000000 -t 8 -b 512 --runtime 15   # offline throughput
```

Always measure on a **Release** build. `dotnet $RB --help` lists all flags.

## Key knobs

| Flag | Default | Controls |
|---|---|---|
| `--op` | `GET` | Op to benchmark (offline): GET, MGET, INCR, SET, ZADD, ... |
| `--dbsize` | `1024` | Distinct keys (pre-loaded unless `-s`). |
| `--valuelength` | `8` | Value bytes (use `--keylength 16 --valuelength 96` = 128 B record for KV/Device parity). |
| `-t` | `1,2,4,8,16,32` | Thread-count sweep (offline). |
| `-b` | `4096` | Requests per pipeline (offline; dominant throughput knob, `1024` is a good default). Online forces `1`. |
| `--runtime` | `15` | Seconds per cell. `0` = load only (no run). |
| `-s` | `false` | Skip load — run against a pre-loaded server. |
| `--itp` | `1` | Online: in-flight ops per thread. |
| `--zipf` | `false` | Skew keys (θ=0.99) instead of uniform, including within each `--cluster-bench` shard. |

## The three scenarios

Three server setups distinguished by **where reads land**, swept over threads for
offline throughput. Scatter-gather GET (`--sg-get`, on by default) batches contiguous
pending GETs into one vectored IO — essential for the device-backed scenarios.

### 1. Memory-bound — data in RAM

Default server; the dataset fits in the in-memory log, so reads never touch a device.

```bash
dotnet $GS --port 6379 &
dotnet $RB --op GET --dbsize 16777216 --valuelength 100 --runtime 0   # load 16 M × 100 B
dotnet $RB -s --op GET --dbsize 16777216 --valuelength 100 -t 1,2,4,8,16,32 -b 1024
```

### 2. NVMe storage-bound — reads hit real disk

Tier the store with a tiny memory log so ~99.9% of a 100 M dataset is on NVMe and
every GET is a random device fetch. Use **100 M × 128 B** records (`--keylength 16
--valuelength 96`, matching the KV/Device benchmarks — 128 B records read over the
array's 512 B sectors). Reference host: **8×NVMe RAID-0** (`/raid`, `fio` random-read
ceiling ≈ **8.24 M IOPS at 4 K** / **8.20 M at 512 B** — the array is IOPS-bound, so
block size barely moves it); Garnet sustains **~7.3 M** end-to-end (≈ 89% of `fio`).

```bash
DATA=/raid/garnet; mkdir -p $DATA
# Server pinned to NUMA node 0, client driven from node 1:
numactl --cpunodebind=0 --membind=0 dotnet $GS --port 6379 --bind 127.0.0.1 \
  --memory 16m --page 4m --segment 1g --index 8g --storage-tier --logdir $DATA \
  --device-type Native --device-io-backend Libaio --logger-level Warning &
numactl --cpunodebind=1 --membind=1 dotnet $RB --op MSET --dbsize 100000000 \
  --keylength 16 --valuelength 96 --client LightClient --load-threads 32 -b 4096 --runtime 0
numactl --cpunodebind=1 --membind=1 dotnet $RB -s --op GET --dbsize 100000000 \
  --keylength 16 --valuelength 96 --client LightClient -t 8,32,48,64 -b 1024 --runtime 12
```

**Check the load before trusting the GET numbers.** A key that was never written is
answered from memory without touching the device, and a miss is ~30× cheaper than a
disk read, so a partial load silently inflates the result — an unloaded store reports
>150 M ops/sec on this host. Confirm the `MSET` step reports ~100 M ops in its
`[Total time]` line; the [generator script](scripts/nvme-raid0-matrix.sh) enforces this.

| backend | NUMA | t=8 | t=32 | t=48 | t=64 |
|---|---|---|---|---|---|
| Libaio | srv node-0 / cli node-1 | 1.82 M | 5.70 M | 6.97 M | **7.06 M** |
| Libaio | no pin | 1.38 M | 5.09 M | 5.72 M | 5.57 M |

> `Libaio` (the Linux default) is shown here for a quick look. For the full
> backend × pin matrix — including `Uring` on out-of-box defaults, which reaches a
> slightly higher peak — see [Sample results](#sample-results--8-nvme-ssd-raid-0) below.

- `--index 8g` for 100 M keys (default 128 m → 3–4× slowdown from hash chains).
- Peak is in the **t=48–64** band: the RESP server's pipelined client connections drive
  in-flight depth through the server's own network + completion threads, so throughput
  keeps climbing well past the raw device's t=32 peak before queueing costs take over.
- **NUMA pinning matters most here** (stateful server): pinning the server to node 0
  and the client to node 1 lifts t=48 from 5.72 → 6.97 M and t=64 from 5.57 → 7.06 M.
- **`Libaio`** is the Linux default and needs no ring tuning. **`Uring`** now auto-sizes
  its ring count to `min(2 × cores, 64)` — decoupled from `--device-completion-threads` —
  so it is competitive with libaio out of the box; use **`--device-io-contexts N`** to set
  the ring count explicitly (at or above your connection count) for very high concurrency
  (see [Device Tuning](https://microsoft.github.io/garnet/docs/dev/device-tuning) and the
  [Device README](../../libs/storage/Tsavorite/cs/benchmark/Device.benchmark/README.md#nvme-storage-bound)).
- Capacity knobs are left at their defaults above; add `--device-completion-threads 8
  --device-throttle-limit 4096` to hand-tune. `--device-throttle-limit 4096` suits this
  array; lower to 512/128 on a single/SATA disk.

### 3. Memory-device-bound — reads hit the in-RAM device

Same tiered server, but the syscall-free `LocalMemory` device: the full RESP +
pending path with **zero disk latency** (the software ceiling; matches the
[KV](../../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md#the-three-scenarios)
and [Device](../../libs/storage/Tsavorite/cs/benchmark/Device.benchmark/README.md#memory-device-bound-localmemory)
LocalMemory runs). Replace the device flags in (2) with:

```bash
  ... --device-type LocalMemory --device-completion-threads 4 --device-throttle-limit 512 ...
```

Reference (10 M × 100 B, t=16): **~2.7 M ops/sec** at `-b 1024`, **~3.7 M** at `-b 256`.

## Sample results — 8× NVMe SSD RAID-0

Full **scenario 2** GET throughput matrix on **out-of-box device defaults** — only `--storage-tier`
and `--device-io-backend` are set; `--device-completion-threads`, `--device-throttle-limit`, and
`--device-io-contexts` are left at their server defaults, so this is what an operator gets with zero
device tuning. Median of 3 passes per cell.

**Host** — 2× Intel Xeon Platinum 8480CL (56 cores × 2 threads/socket, 224 logical CPUs, 2 NUMA nodes),
~2 TB DDR5; **8× Kioxia KCM6DRUL3T84** 3.84 TB PCIe-Gen4 NVMe in Linux `md` RAID-0 (`/dev/md1`, 512 KB
chunks, ext4, ≈28 TB); Ubuntu 24.04.4 LTS, kernel 6.8.0-136, .NET 10.0.302; `fs.aio-max-nr` = 4194304.
`fio` random-read ceiling on this array: **8.24 M IOPS at 4 K** and **8.20 M IOPS at 512 B**
(32 jobs × QD64, io_uring, `O_DIRECT`, 8 files) — the array is IOPS-bound at these sizes, so the
ceiling is effectively block-size independent.

**Workload** — 100 M × 128 B records (`--keylength 16 --valuelength 96`) tiered onto the array
(`--memory 16m --page 4m --segment 1g --index 8g`); 100% random GET; client `-b 1024`; 12 s per cell.
The generator verifies that the load actually wrote ~100 M keys before it measures, so every cell
below is storage-bound rather than partly served as in-memory misses.

| backend | NUMA | t=8 | t=32 | t=48 | t=64 |
|---|---|---|---|---|---|
| Libaio | srv node-0 / cli node-1 | 1.82 M | 5.70 M | 6.97 M | 7.06 M |
| Libaio | no pin | 1.38 M | 5.09 M | 5.72 M | 5.57 M |
| Uring | srv node-0 / cli node-1 | 1.82 M | 6.13 M | **7.32 M** | 6.89 M |
| Uring | no pin | 1.48 M | 4.60 M | 5.55 M | 6.29 M |

- **Peak ≈ 7.3 M ops/sec** (uring, pinned, t=48) — **~89% of the `fio` ceiling** for random reads
  of the same shape (128 B records fetched over the array's 512 B sectors), driven end-to-end
  through the RESP protocol and the Tsavorite pending-read path (not raw device IO). The remaining
  gap to `fio` is the RESP + pending-read path: the raw device layer reaches ~8.4 M on this array.
- **The two backends peak at different thread counts.** Uring peaks at t=48 and eases off at t=64,
  where added client concurrency costs more in queueing than it recovers in in-flight depth; libaio
  is still climbing at t=64. Sweep the t=48–64 band rather than assuming a single best value.
- **Defaults reach the tuned peak.** Uring's smart ring-count default (`min(2 × cores, 64)` rings,
  decoupled from the 4 completion threads) sizes rings to the hardware with no flags; libaio
  needs no ring tuning. Explicit tuning (`--device-completion-threads 8 --device-throttle-limit 4096`,
  uring `--device-io-contexts 96`) moves each cell by only a few percent on this host.
- **NUMA pinning is the largest single factor** on this dual-socket box (stateful server): e.g. uring
  t=48 rises 5.55 → 7.32 M when the server is pinned to node 0 and the client to node 1. On a
  single-socket host the pin / no-pin rows converge.
- Both backends reach the same ~7 M plateau once each has its required ring config — uring leads by
  5–7% at t=32–48, libaio by 2% at t=64 — so pick either and tune the thread count (see
  [Device Tuning](https://microsoft.github.io/garnet/docs/dev/device-tuning)).

Reproduce with the checked-in generator (needs Release builds of `GarnetServer` + `Resp.benchmark`,
`numactl`, and an NVMe / O_DIRECT mount):

```bash
DATA=/mnt/nvme/garnet benchmark/Resp.benchmark/scripts/nvme-raid0-matrix.sh
```

It sweeps both backends × pin/no-pin × threads, takes the median of `PASSES` (default 3) per cell, and
prints the Markdown table above. Override `DATA`, `THREADS`, `PASSES`, `RUNTIME`, or set
`CT` / `THROTTLE` / `URING_IOCTX` to run the explicitly tuned configuration instead of the defaults.
Generator: [`scripts/nvme-raid0-matrix.sh`](scripts/nvme-raid0-matrix.sh).

## Offline variations

```bash
dotnet $RB --op MGET --dbsize 16777216 --valuelength 8 -t 16 -b 512         # MGET (scatter-gather)
dotnet $RB --op GET  --dbsize 1000000  -v 100 -t 16 -b 1024 --client InProc # server CPU only, no TCP
dotnet $RB --op GET  --dbsize 1000000  -v 100 -t 16 -b 256  --client SERedis # apples-to-apples vs Redis
dotnet $RB --op GET  --dbsize 16777216 -v 100 -t 16 -b 1024 --zipf          # skewed keys (θ=0.99)
dotnet $RB --cluster-bench --op GET --dbsize 16777216 -t 16 -b 1024 --zipf  # skewed keys within each shard
```

## Online (latency)

```bash
# Single-client GET latency (cleanest reading)
dotnet $RB --online --op-workload GET --op-percent 100 --dbsize 1000000 -t 1 -b 1 --runtime 30
# 50/50 GET/SET tail latency, 16 connections
dotnet $RB --online --op-workload GET,SET --op-percent 50,50 --dbsize 1000000 -t 16 --runtime 60 --client GarnetClientSession
# Fixed offered load: 8 conns × 64 in-flight
dotnet $RB --online --op-workload GET --op-percent 100 --dbsize 1000000 -t 8 --itp 64 --client GarnetClientSession
```

`--runtime -1` runs until interrupted; `0` is invalid for online.

### Methodology (read before trusting numbers)

- **Pure load** = `--op GET --runtime 0` (seeds the keyspace, no run phase). Then
  `-s` for read phases.
- **Verify the load is on disk**: `redis-cli INFO store` — `Log.TailAddress` should
  match the dataset size and `Log.HeadAddress ≈ TailAddress` (data evicted from the
  small memory region to the device).
- **Confirm reads actually hit the device.** On a big-RAM host the whole dataset fits
  in the page cache; the storage device opens with `O_DIRECT`, so reads bypass it.
  During the read phase `iostat -x 1` should show the NVMe at `r/s ≈ ops/sec` and
  `aqu-sz ≈ --device-throttle-limit`. On a shared box, read per-process
  `/proc/<GarnetServer pid>/io` `read_bytes` instead — it excludes other tenants' IO.
- **Run a few read phases** and take the steady-state — the first is warm-up.
- **A/B fairly**: build/load/run each variant separately. To beat CPU clock drift,
  run both servers on different ports and **interleave** the runs. Stop a server by
  its real `GarnetServer.dll` PID — stopping the `dotnet` launcher leaves the runtime
  child alive, and leaked spinning servers cause large variance.

## Output

- Offline: `[Total time]: <ms> for <ops>` and `[Throughput]: <ops/sec>`
  (`= ops_done × batch / runtime`).
- Online: `min; 5th; median; avg; 95th; 99th; 99.9th; total_ops; iter_tops; tpt(Kops/s)`
  every 2 s (µs; per-thread HdrHistogram).

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Skipload not supported with --online` | drop `-s` or use offline |
| `-b N>1` warning in online | tool forces `-b 1`; use `--itp` for concurrency |
| `--pool` with `LightClient` unsupported | use GarnetClientSession / GarnetClient / SERedis |
| Disk-bound throughput far below KV.benchmark | server-side bottleneck — profile with `dotnet-trace` |
| `--dbsize % loadThreads != 0` | round `--dbsize` to a multiple of the loader thread count |

## Related

- [Device.benchmark](../../libs/storage/Tsavorite/cs/benchmark/Device.benchmark/README.md) — raw IDevice IOPS (layer 1).
- [KV.benchmark](../../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md) — Tsavorite KV throughput (layer 2).
- [BDN.benchmark](../BDN.benchmark) — per-command CPU/alloc microbenchmarks (CI).
- [Garnet benchmarking docs](https://microsoft.github.io/garnet/docs/benchmarking/resp-bench)
