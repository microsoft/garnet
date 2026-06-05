# Resp.benchmark

End-to-end throughput / latency benchmark for the Garnet RESP server. Drives
GET/SET/INCR/MGET/... workloads against any RESP server (Garnet, Redis, KeyDB,
Dragonfly) over TCP, or against an in-process embedded Garnet server. It is the
top of the stack: **Resp ≤ [KV](../../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md)
≤ [Device](../Device.benchmark/README.md) ≤ fio**.

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
| `--valuelength` | `8` | Value bytes (use `100` for KV.benchmark parity). |
| `-t` | `1,2,4,8,16,32` | Thread-count sweep (offline). |
| `-b` | `4096` | Requests per pipeline (offline; dominant throughput knob, `1024` is a good default). Online forces `1`. |
| `--runtime` | `15` | Seconds per cell. `0` = load only (no run). |
| `-s` | `false` | Skip load — run against a pre-loaded server. |
| `--itp` | `1` | Online: in-flight ops per thread. |
| `--zipf` | `false` | Skew keys (θ=0.99) instead of uniform. |

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
every GET is a 4 KB random fetch. Use **100 M × 100 B** (smaller datasets touch few
NAND dies and understate device IOPS).

```bash
DATA=/mnt/nvme/garnet; mkdir -p $DATA
numactl --cpunodebind=0 --membind=0 dotnet $GS --port 6379 \
  --memory 16m --page 4m --segment 1g --index 4g --storage-tier --logdir $DATA \
  --device-type Native --device-io-backend libaio --device-throttle-limit 512 \
  --logger-level Warning &
numactl --cpunodebind=0 --membind=0 dotnet $RB --op MSET --dbsize 100000000 --valuelength 100 -t 8 -b 1024 --runtime 0
numactl --cpunodebind=0 --membind=0 dotnet $RB -s --op GET --dbsize 100000000 --valuelength 100 -t 1,2,4,8,16,32 -b 1024 --runtime 15
```

- `--index 4g` for 100 M keys (default 128 m → 3–4× slowdown from hash chains).
- `libaio` is fastest on Linux (`uring` to compare; `Default` → RandomAccess, slower).
  `--device-throttle-limit 512` is safe on fast NVMe; lower to 128 on SATA.

### 3. Memory-device-bound — reads hit the in-RAM device

Same tiered server, but the syscall-free `LocalMemory` device: the full RESP +
pending path with **zero disk latency** (the software ceiling; matches the
[KV](../../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md#the-three-scenarios)
and [Device](../Device.benchmark/README.md#memory-device-bound-localmemory)
LocalMemory runs). Replace the device flags in (2) with:

```bash
  ... --device-type LocalMemory --device-completion-threads 4 --device-throttle-limit 512 ...
```

Reference (10 M × 100 B, t=16): **~2.7 M ops/sec** at `-b 1024`, **~3.7 M** at `-b 256`.

## Offline variations

```bash
dotnet $RB --op MGET --dbsize 16777216 --valuelength 8 -t 16 -b 512         # MGET (scatter-gather)
dotnet $RB --op GET  --dbsize 1000000  -v 100 -t 16 -b 1024 --client InProc # server CPU only, no TCP
dotnet $RB --op GET  --dbsize 1000000  -v 100 -t 16 -b 256  --client SERedis # apples-to-apples vs Redis
dotnet $RB --op GET  --dbsize 16777216 -v 100 -t 16 -b 1024 --zipf          # skewed keys (θ=0.99)
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

- [Device.benchmark](../Device.benchmark/README.md) — raw IDevice IOPS (layer 1).
- [KV.benchmark](../../libs/storage/Tsavorite/cs/benchmark/KV.benchmark/README.md) — Tsavorite KV throughput (layer 2).
- [BDN.benchmark](../BDN.benchmark) — per-command CPU/alloc microbenchmarks (CI).
- [Garnet benchmarking docs](https://microsoft.github.io/garnet/docs/benchmarking/resp-bench)
