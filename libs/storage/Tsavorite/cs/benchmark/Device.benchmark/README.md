# Device.benchmark

Random-read IOPS benchmark for Tsavorite's `IDevice` backends. Use it to check a
backend reaches the raw NVMe ceiling (isolating IO from upper-layer KV overhead,
cf. [KV.benchmark](../KV.benchmark/README.md)),
to compare libaio vs io_uring on Linux, or — with `LocalMemory` — to measure the
IO-submission/completion machinery itself with no real device latency.

It fills the backing file with a sector-aligned pattern, then runs N workers that
each issue `device.ReadAsync` at random offsets and recycle buffers in the
completion callback. Throughput counts **successful completions only**.

## Build & run

```bash
cd benchmark/Device.benchmark
dotnet build -c Release -f net10.0
DB=bin/Release/net10.0/Device.benchmark.dll

# Linux NVMe, libaio
numactl --membind=0 --cpunodebind=0 dotnet $DB \
  --file-name /mnt/nvme/devbench.dat --device-type Native --io-backend libaio \
  --file-size 17179869184 --sector-size 4096 \
  --batch-size 4096 --threads 16 --throttle-limit 512 --runtime 8
# → Benchmark finished: ... throughput: ~750000 ops/sec
```

Always measure on a **Release** build. Run `dotnet $DB --help` for all flags.

## Scenarios

This is a device benchmark, so it has two of the suite's three scenarios — both
issue real IO. (The *memory-bound*/no-IO scenario lives in
[KV](../KV.benchmark/README.md#the-three-scenarios)
and [Resp](../../../../../../benchmark/Resp.benchmark/README.md#the-three-scenarios), which can serve reads
from RAM with no device.)

### NVMe storage-bound

Measured on a Dell P5600 NVMe (`fio` 4K randread ceiling ≈ 749K IOPS); reproduces
within ±2%. Common flags: `--file-size 17179869184 --sector-size 4096
--segment-size 1073741824 --batch-size 4096 --throttle-limit 512 --runtime 8`.

```bash
numactl --membind=0 --cpunodebind=0 dotnet $DB \
  --file-name /mnt/nvme/devbench.dat --device-type Native --io-backend libaio \
  --completion-threads 1 --threads 16 \
  --file-size 17179869184 --sector-size 4096 --batch-size 4096 --throttle-limit 512 --runtime 8
```

| backend | --completion-threads | --threads | ops/sec |
|---|---|---|---|
| Native libaio | 1 | 16 | **750 K** |
| Native libaio | 1 | 32 | 755 K |
| Native uring  | 1 | 16 | 342 K (single-ring SpinLock cap) |
| Native uring  | 8 | 16 | **758 K** |

### Memory-device-bound (`LocalMemory`)

`LocalMemory` is an in-RAM `IDevice`: reads are a `memcpy` served by per-thread
SPSC rings drained by `--completion-threads` worker threads. With no real device
latency, this measures the **submission/completion path ceiling** (ring routing,
wakeups, callback dispatch) — the upper bound for `KV`/`resp` LocalMemory runs and
a regression test for the ring code.

```bash
# Sweep; set --completion-threads == --threads (one SPSC ring per submitter).
for T in 8 16 32 40; do
  numactl --cpunodebind=0 --membind=0 dotnet $DB \
    --device-type LocalMemory --completion-threads $T --threads $T \
    --file-size 1073741824 --segment-size 1073741824 --sector-size 512 \
    -b 1024 --throttle-limit 8192 --runtime 6
done
```

| --threads (= --completion-threads) | 8 | 16 | 32 | 40 |
|---|---|---|---|---|
| MIOps/s | 34 | 57 | **78** | 74 |

Peaks near the physical core count, then falls off. Use a large `--throttle-limit`
(8192) — there is no kernel ring to overflow, so back-pressure should not gate.

## Key knobs

- **`--throttle-limit`** — user-side in-flight cap (not a kernel limit). On fast
  NVMe, 512 is safe (Little's Law keeps actual kernel in-flight ≈ 45, below the
  128-slot libaio/io_uring ring). `0` floods the ring → `code4` (EAGAIN) errors;
  halve until errors disappear. For `LocalMemory`, use a large value (8192).
- **`--completion-threads`** — libaio: 1 (kernel mutex already efficient; sharding
  is a no-op). io_uring: 4–8 sharded rings to escape the per-ring SpinLock.
  LocalMemory: match `--threads`.
- **`--threads`** — 16 is the NVMe sweet spot; LocalMemory peaks near core count.
- **`numactl --membind=0 --cpunodebind=0`** — required; cross-NUMA costs 10–15%.
- **`--file-size`** must be a multiple of `1024 × --sector-size`.

## Output

```
Benchmark finished: <ok> ok, <err> err, <submitted> submitted in <T> s, throughput: <ok/T> ops/sec
  error breakdown: code<N>=<count> ...   # only when err > 0
```

`code4` (`Status::IOError`) = kernel ring full (libaio `io_submit` EAGAIN /
io_uring SQ full). Fix by lowering `--throttle-limit`.

## Related

- [KV.benchmark](../KV.benchmark/README.md) — full Tsavorite KV throughput.
- [Native device backends](../../src/core/Device/NativeStorageDevice.cs) — libaio / io_uring.
