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

# Linux NVMe RAID-0, libaio
numactl --membind=0 --cpunodebind=0 dotnet $DB \
  --file-name /raid/devbench.dat --device-type Native --device-io-backend libaio \
  --file-size 12801015808 --sector-size 512 \
  --batch-size 4096 --threads 32 --device-completion-threads 8 --device-throttle-limit 4096 --runtime 8
# → Benchmark finished: ... throughput: ~8.2M ops/sec (8×NVMe RAID-0)
```

Always measure on a **Release** build. Run `dotnet $DB --help` for all flags.

## Scenarios

This is a device benchmark, so it has two of the suite's three scenarios — both
issue real IO. (The *memory-bound*/no-IO scenario lives in
[KV](../KV.benchmark/README.md#the-three-scenarios)
and [Resp](../../../../../../benchmark/Resp.benchmark/README.md#the-three-scenarios), which can serve reads
from RAM with no device.)

### NVMe storage-bound

Reference host: **8×NVMe SSD RAID-0** (Linux `md`, mounted `/raid`); `fio` 4K
randread ceiling ≈ **8.24 M IOPS**. The Device benchmark reaches this at the raw
`IDevice` level with **either** backend. The config is kept **compatible with the
KV/RESP benchmarks**: `--sector-size 512` (the array's logical block size — Garnet
reads the sector-aligned window covering a 128 B record) and `--file-size
12801015808` (12.8 GB = 100 M × 128 B). Common flags: `--segment-size 1073741824
--batch-size 4096 --device-completion-threads 8 --device-throttle-limit 4096 --runtime 8`.

```bash
# libaio — a few kernel io_contexts suffice (default ct rings):
numactl --membind=0 --cpunodebind=0 dotnet $DB \
  --file-name /raid/devbench.dat --device-type Native --device-io-backend libaio \
  --device-completion-threads 8 --threads 32 \
  --file-size 12801015808 --sector-size 512 --segment-size 1073741824 \
  --batch-size 4096 --device-throttle-limit 4096 --runtime 8

# uring — needs one ring per submitter: set --device-io-contexts >= --threads:
numactl --membind=0 --cpunodebind=0 dotnet $DB \
  --file-name /raid/devbench.dat --device-type Native --device-io-backend uring \
  --device-completion-threads 8 --device-io-contexts 32 --threads 32 \
  --file-size 12801015808 --sector-size 512 --segment-size 1073741824 \
  --batch-size 4096 --device-throttle-limit 4096 --runtime 8
```

| backend | rings | --threads | ops/sec |
|---|---|---|---|
| Native libaio | ct=8 (8 io_contexts) | 32 | **8.23 M** |
| Native libaio | ct=8 | 64 | 7.7 M |
| Native uring | ct=8, `--device-io-contexts 8` | 32 | 2.9 M (per-ring SpinLock cap) |
| Native uring | ct=8, `--device-io-contexts 32` | 32 | **8.00 M** |

Both backends hit the `fio` ceiling, but **libaio needs only ~8 kernel io_contexts**
(its io_context mutex is cheap) whereas **io_uring needs one ring per submitter**
(`--device-io-contexts 32`) to escape the managed per-ring `SpinLock` — with the default
8 rings it caps at ~2.9 M. NUMA pinning is ~neutral at the raw device layer (node-0
pin vs no pin within ±2%); it matters far more up the stack (KV/RESP). Peak is at
`--threads 32` (32 submit + 8 drain ≈ node-0's physical cores); `--threads 64`
oversubscribes and falls to ~7.5 M.

### Memory-device-bound (`LocalMemory`)

`LocalMemory` is an in-RAM `IDevice`: reads are a `memcpy` served by per-thread
SPSC rings drained by `--device-completion-threads` worker threads. With no real device
latency, this measures the **submission/completion path ceiling** (ring routing,
wakeups, callback dispatch) — the upper bound for `KV`/`resp` LocalMemory runs and
a regression test for the ring code.

```bash
# Sweep; set --device-completion-threads == --threads (one SPSC ring per submitter).
for T in 8 16 32 40; do
  numactl --cpunodebind=0 --membind=0 dotnet $DB \
    --device-type LocalMemory --device-completion-threads $T --threads $T \
    --file-size 1073741824 --segment-size 1073741824 --sector-size 512 \
    -b 1024 --device-throttle-limit 8192 --runtime 6
done
```

| --threads (= --device-completion-threads) | 8 | 16 | 32 | 40 |
|---|---|---|---|---|
| MIOps/s | 34 | 57 | **78** | 74 |

Peaks near the physical core count, then falls off. Use a large `--device-throttle-limit`
(8192) — there is no kernel ring to overflow, so back-pressure should not gate.

## Key knobs

- **`--device-throttle-limit`** — user-side in-flight cap (not a kernel limit). On a fast
  multi-drive array use **4096**; on a single NVMe **512** is enough (Little's Law
  keeps actual kernel in-flight well below the ring depth). `0` floods the ring →
  `code4` (EAGAIN) errors; halve until errors disappear. For `LocalMemory`, use a
  large value (8192).
- **`--device-completion-threads`** — background drainer count. **8** is a good default for
  a fast array (both backends); 1 suffices for a single NVMe. LocalMemory: match
  `--threads`.
- **`--device-io-contexts`** — kernel io_contexts / io_uring rings, decoupled from drainers.
  **libaio**: leave at default (= ct rings) — its io_context mutex is cheap, more
  rings are a no-op. **io_uring**: set **>= submitter `--threads`** (e.g.
  `--device-io-contexts 32` for 32 threads) so each submitter gets its own ring and escapes
  the managed per-ring `SpinLock`; otherwise uring caps at ~a third of libaio.
- **`--threads`** — 32 is the NVMe-array sweet spot (submit + drain ≈ node-0 cores);
  >32 oversubscribes and falls off. LocalMemory peaks near core count.
- **`numactl --membind=0 --cpunodebind=0`** — near-neutral at the raw device layer,
  but keeps memory local; matters much more up the stack (KV/RESP cross-NUMA costs
  10–30%).
- **`--file-size`** must be a multiple of `1024 × --sector-size`.

## Completion model & high-latency (cloud) devices

The completion path is **block-on-signal**, not busy-spin. A read that misses
memory goes pending; the waiting thread suspends its epoch and parks on the
session's `readyResponses` semaphore (`SemaphoreSlim`, via `WaitPending`). The
background drainer parks in the kernel — `io_getevents(min_nr=1, timeout)`
(libaio) or `io_uring_wait_cqe_timeout` (uring; rings are created with flags `0`,
so **no SQPOLL** kernel poller) — and releases the semaphore when a completion
lands. A waiting reader therefore burns no CPU during the device-latency window;
this is the steady state on a **high-latency (cloud) device** (Azure/EBS-class,
~0.5–2 ms), where throughput is latency×concurrency-bound, as it must be.

Two poll levers exist only to reach the local-NVMe ceiling; neither is a hot
idle-spin, and both fall through to the block-on-signal path when completions are
not immediately ready:

- **Inline affine drain** (always on): before parking, the reader does **one**
  non-blocking peek of its own ring (`TryCompleteMine`). On a saturated fast array
  the completion is usually already there, so the reader never parks (poll-driven →
  peak IOPS). On a cloud device the peek usually misses and the thread parks on the
  semaphore, so the one extra peek is a negligible cost there.
- **Submit-side backpressure**: `AsyncGetFromDisk` spins **only** while a thread's
  in-flight exceeds its per-thread `--device-throttle-limit` share, and it drains
  completions on each turn. A request/response reader holds ≤1 in-flight, so it
  never hits this; it engages only for bulk multi-issue callers (recovery/scan).
  Size `--device-throttle-limit` to the device's bandwidth-delay product (deep queues are
  how you hide cloud latency) and the spin stays at the ceiling only.

## Output

```
Benchmark finished: <ok> ok, <err> err, <submitted> submitted in <T> s, throughput: <ok/T> ops/sec
  error breakdown: code<N>=<count> ...   # only when err > 0
```

`code4` (`Status::IOError`) = kernel ring full (libaio `io_submit` EAGAIN /
io_uring SQ full). Fix by lowering `--device-throttle-limit`.

## Related

- [KV.benchmark](../KV.benchmark/README.md) — full Tsavorite KV throughput.
- [Native device backends](../../src/core/Device/NativeStorageDevice.cs) — libaio / io_uring.
