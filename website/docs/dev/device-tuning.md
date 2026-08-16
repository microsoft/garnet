---
id: device-tuning
sidebar_label: Device Tuning
title: Native Storage Device Tuning
---

This page documents the tuning surface of Garnet's **Native storage device** — the
Linux `io_uring` / `libaio` `IDevice` implementation used when the hybrid log is tiered to
an NVMe/SSD (`--storage-tier`). It covers every `--device-*` knob, its default, the exact
formulas the device uses to derive its internal parameters, the internal constants that
bound those formulas, and the definitions of **headroom**, **floor**, **cap**, and
**ceiling** as the code uses them.

The Native device is the default on x64 Linux and is what lets a disk-served workload
approach raw-device (`fio`) IOPS. On Windows it uses the IOCP thread-pool backend and most
Linux-only knobs below are ignored.

## Mental model: three orthogonal dimensions + software backpressure

A kernel async-IO device has exactly two physical capacity dimensions plus one software
policy. Garnet exposes each as its own knob so that no single number silently controls two
things:

| Dimension | Knob | What it sizes | Analogy (fio) |
|---|---|---|---|
| **Ring count** `N` | `--device-io-contexts` | number of independent kernel submission queues (io_uring rings / libaio `io_context`s) | `numjobs` / parallel submission queues |
| **Ring depth** `D` | `--device-queue-depth` | per-ring kernel submission depth (`maxEvents` passed to `io_uring_queue_init` / `io_setup`) | per-queue `iodepth` |
| **Aggregate in-flight** `T` | `--device-throttle-limit` | max reads the allocator keeps in flight before applying backpressure | total effective `iodepth` |

The only relationship between the three is a single correctness invariant:

```
aggregate in-flight  T  ≤  kernel capacity  N × D
```

`T` is **software backpressure** enforced by the Tsavorite allocator — it bounds how much
pending-read work (and thus how much pinned read-buffer memory) accumulates. `N` and `D`
size actual kernel structures. Keeping them as separate knobs lets ring count and ring
depth be sized independently.

## The tuning knobs

All knobs have a command-line form (`--device-…`), a config-file form (the PascalCase name),
and a default. Defaults live in `libs/host/defaults.conf`; the option definitions in
`libs/host/Configuration/Options.cs`.

| CLI flag | Config key | Default | Applies to | Meaning |
|---|---|---|---|---|
| `--device-type` | `DeviceType` | `Default` | all | `Default` (Native on x64 Linux/Windows, else RandomAccess), `Native`, `RandomAccess`, `FileStream`, `AzureStorage`, `Null`. |
| `--device-io-backend` | `DeviceIoBackend` | `Default` | Native, Linux | `Default` (= `Libaio`), `Libaio`, or `Uring` (io_uring). |
| `--device-completion-threads` | `DeviceCompletionThreads` | `4` (max 64) | Native, Linux | Number of background IO **completion drain** threads. |
| `--device-io-contexts` | `DeviceIoContexts` | `0` (→ smart default) | Native, Linux | Ring **count** `N` (see [smart default](#derived-smart-io-context-default)). |
| `--device-queue-depth` | `DeviceQueueDepth` | `0` (→ 4096) | Native, Linux | Per-ring **depth** `D`. |
| `--device-throttle-limit` | `DeviceThrottleLimit` | `0` (→ 4096 Native / 120 managed) | all devices | Aggregate **in-flight** `T` (`IDevice.ThrottleLimit`). |
| `--device-aio-max-devices` | `DeviceAioMaxDevices` | `32` | Native, Linux, **libaio only** | Target number of libaio devices to fit within the machine-global `fs.aio-max-nr` budget. |

### `--device-io-backend`

Selects the Linux kernel async-IO API for the Native device.

* **`Libaio`** (also `Default`) — classic Linux AIO (`io_submit`/`io_getevents`). Each ring is
  a kernel `io_context`. libaio is **ring-count-neutral**: its kernel `io_context` mutex is
  cheap, so a handful of rings already saturates it. **Caveat:** `io_setup` permanently
  reserves `N × D` events from the *machine-global* `fs.aio-max-nr` budget (see
  [libaio reservation](#derived-libaio-reservation-depth)).
* **`Uring`** — io_uring. Each ring is an `io_uring` instance with its own SQ/CQ mmap memory
  (no global budget). io_uring is **ring-count-sensitive**: when rings `<` submitter
  concurrency, submitters serialize on a per-ring submit lock and throughput can drop ~3×.
  Always set `--device-io-contexts` at or above your concurrency (the [smart default](#derived-smart-io-context-default)
  does this for you).

The shipped `libnative_device.so` is built with `-DUSE_URING=ON`, so `liburing.so.2` must be
present at load time for **all** backends; a `-DUSE_URING=OFF` rebuild only needs `libaio`.

### `--device-completion-threads`

Number of background threads that drain completions from the rings. Under high concurrent
pending-read load a single drainer convoys on the completion-signal path and collapses
throughput; the default of `4` removes that on both backends. io_uring scales further and
more CPU-efficiently with more drainers; libaio benefits little beyond a few. Drainers
range-drain contiguous slices of the rings, so `N` is always clamped up to at least the
drainer count (every drainer owns ≥ 1 ring).

Inline draining (a submitter thread reaping its own ring's completions before waiting) is
always on and is the primary completion mechanism at the serving peak; the background
drainers are a backstop for rings whose owning thread is idle.

### `--device-io-contexts` (ring count `N`)

The single most important knob for **io_uring**. It is the ring count, decoupled from the
drainer count. Set it at or above your submitter concurrency (roughly your connection count)
so each submitter owns a ring and `io_submit` is contention-free. Too few rings serialize
submitters on the per-ring lock (~3× slower). **libaio is
largely indifferent** to it. `0` selects the [smart default](#derived-smart-io-context-default).

### `--device-queue-depth` (ring depth `D`)

Per-ring kernel submission depth. Orthogonal to `N` and `T`. `0` selects the default of
`4096`, [capped](#floor-cap-ceiling-headroom) at the io_uring hard limit of `32768`. For
libaio the effective `io_setup` reservation is sized down from this default (see
[below](#derived-libaio-reservation-depth)); set it explicitly to reserve a specific depth.

### `--device-throttle-limit` (aggregate in-flight `T`)

`IDevice.ThrottleLimit` — the max reads kept in flight before the allocator applies
backpressure and drains instead of issuing more. `0` uses the device's built-in default:
**4096** for the Native device (deep NVMe / io_uring queues), **120** for the managed in-box
devices. It is a pure software / memory limit (its footprint is `T ×` sector-aligned read
size of pinned read buffers); in the split model it sizes nothing in the kernel. It is
[capped](#derived-effective-throttle) at the kernel capacity `N × D`.

### `--device-aio-max-devices` (libaio budget divisor)

libaio only. `fs.aio-max-nr` is a *machine-global* event budget shared by every device in
every process. `io_setup` permanently draws `N × D` events from it per device. This knob is
the **target number of Native libaio devices to fit** within that budget: the default per-device
reservation is hard-capped at `fs.aio-max-nr / this` (default 32), keeping at least this many
devices creatable regardless of `--device-completion-threads` / `--device-throttle-limit`. The
cap is best-effort — see [the reservation derivation](#derived-libaio-reservation-depth) for the
two cases it cannot cover. Raise `fs.aio-max-nr` (e.g. `sysctl -w fs.aio-max-nr=1048576`) or
lower this value to give each serving device a deeper reservation. Ignored for io_uring (no
global budget) and non-Linux.

### `--device-uring-sqpoll` (io_uring submission polling)

io_uring only. Enables `IORING_SETUP_SQPOLL` so a **kernel thread** polls the submission
queue and user-side submissions become syscall-free (no `io_uring_enter` per submit). Each
ring gets its **own** poll thread (no `IORING_SETUP_ATTACH_WQ`), so submission stays parallel
across rings; one poll thread shared across rings would serialize it.
`--device-uring-sqpoll-idle-ms` sets `sq_thread_idle` (how long a poll thread spins after the
last submit before parking; `0` = 10s native default). **Off by default (opt-in).** Ignored
for libaio / on Windows.

With one poll thread per ring, SQPOLL **matches or slightly beats** the default per-submit
path on the 8×NVMe RAID-0 target (uring, 512B random reads), peaking at fio parity:

| config (io-contexts / threads) | SQPOLL off | SQPOLL on |
|--------------------------------|-----------:|----------:|
| 8 / 16                         |     3.10M  |  **3.44M**|
| 16 / 32                        |     5.18M  |  **5.83M**|
| 32 / 32 (peak)                 |     8.12M  |  **8.39M**|
| 32 / 64                        |     6.98M  |  **7.18M**|

:::tip Let the kernel place the poll threads
The poll threads are left unpinned so the scheduler can spread them across node 0's mostly-idle
cores; pinning them onto the submitter / RESP cores costs throughput. They are busy-polling
kernel threads and consume CPU, so give them cores to run on; on core-starved hosts SQPOLL can
lose to the default path.
:::

## Derived parameters

The device computes several internal parameters at creation (first IO) from the knobs above
and the host environment. These are **not** directly settable; understanding them is the key
to tuning.

### Derived: smart io-context default {#derived-smart-io-context-default}

When `--device-io-contexts` is left at `0`:

```
uring : N = max(completion-threads, min(2 × ProcessorCount, 64))
libaio: N = completion-threads
```

The value is then clamped up to `completion-threads` so every drainer owns at least one ring.
io_uring is ring-starved below submitter concurrency, so it defaults to a hardware-aware ring
count (`2 × cores`, **capped** at 64 to bound ring memory at ~400 KB/ring → ≤ ~25 MB). libaio
is ring-count-neutral and its `N × D` draws from the global budget, so it keeps the
conservative `rings = drainers` default.

### Derived: queue depth `D` {#derived-queue-depth}

```
D = (--device-queue-depth > 0) ? --device-queue-depth : 4096      // DefaultQueueDepth
D = min(D, 32768)                                                  // MaxQueueDepth ceiling (io_uring hard limit)
```

`D = 4096` (a **ceiling**, not pre-allocated work) satisfies "per-ring depth ≥ per-ring
in-flight" for any `N` as long as `T ≤ 4096 × N` (the default `T = 4096` always fits), so no
ring ever stalls full. Deeper-than-needed rings are harmless; the only cost is bounded pinned
ring memory.

### Derived: libaio reservation depth {#derived-libaio-reservation-depth}

For **libaio with the default queue depth**, `D = 4096` would over-reserve from the global
`fs.aio-max-nr` budget (a libaio ring never actually holds more than the aggregate throttle
spread across the rings). So the reservation is sized to that throttle share instead
(`ResolveLibaioReservationDepth`):

```
share   = ceil(T / N)                                 // this ring's share of aggregate in-flight
depth   = NextPow2(share × Headroom)                  // Headroom = 2   (over-provision factor)
depth   = max(depth, 128)                             // Floor  = LibaioReservationFloor
depth   = min(depth, 2048)                            // Cap    = LibaioReservationCap
depth   = min(depth, D)                               // Ceiling = the resolved queue depth

// Then a HARD per-device AIO-budget ceiling, independent of N and T:
perDeviceBudget = fs.aio-max-nr / --device-aio-max-devices     // default fs.aio-max-nr / 32
while (depth > 1 && N × depth > perDeviceBudget)  depth >>= 1  // halve (stay pow2) until it fits
```

The caller then caps `effectiveThrottleLimit` at `N × depth` so aggregate in-flight tracks the
(possibly reduced) reservation. Consequences:

* **Multi-ring serving devices** (`N ≥ 4`) keep `N × depth ≥ T` from the share math alone, so
  the full aggregate throttle stays usable — **no IOPS cost** — while the per-device
  global-budget footprint drops.
* **Low-ring-count auxiliary devices** (e.g. cluster AOF / checkpoint logs created with the
  raw `Devices.CreateLogDevice` single-ring defaults) drop to `≈ N × cap`, so the
  `--device-aio-max-devices` target of them coexists within a stock 65536 budget.
* The hard per-device ceiling keeps at least `--device-aio-max-devices` devices fitting the
  budget: on a stock 65536 budget it bounds each device to 2048 events; a host that sizes
  `fs.aio-max-nr` for its workload keeps serving devices at full depth (e.g. `4194304 / 32 =
  131072` per device, which never binds). It is best-effort in two respects: `depth` cannot fall
  below one event per ring, so an `N` above the per-device share still exceeds it (warned at
  creation); and the budget is the machine total, not what remains after other processes.
* **When the budget ceiling binds, it overrides the "no IOPS cost" property above**, because
  `effectiveThrottleLimit` is capped at `N × depth ≤ perDeviceBudget`. On a stock 65536 budget
  that bound is 2048, so the default `T = 4096` is halved at every ring count — worth ~9% on a
  libaio disk-serving workload. Size `fs.aio-max-nr` for the host (`sysctl -w
  fs.aio-max-nr=…`, persisted under `/etc/sysctl.d/`) so `fs.aio-max-nr /
  --device-aio-max-devices ≥ T`; a serving host wanting the default `T = 4096` across 32
  devices needs `fs.aio-max-nr ≥ 131072`.

io_uring skips all of this — it uses `D` directly (per-ring mmap memory, no global budget).

### Derived: effective throttle {#derived-effective-throttle}

```
requestedThrottle   = (ThrottleLimit > 0) ? ThrottleLimit : 4096   // DefaultThrottleLimit
kernelCapacity      = N × D
effectiveThrottle   = min(requestedThrottle, kernelCapacity)       // cap at kernel capacity (warns if it binds)
```

Capping `T` at `N × D` enforces the "in-flight ≤ kernel capacity" invariant that prevents the
ring-full submit spin. This cap is **decoupled** from the depth cap, so a high-connection
deployment can raise `T` as long as `N × D` is large enough. The per-shard clamp described
next imposes a second, independent ceiling: raising `T` above
`NumShards × MaxPerThreadInFlight` (**4096** on any host with ≥ 16 logical processors) has no
effect, because the per-thread budget saturates at `MaxPerThreadInFlight`.

### Derived: per-thread in-flight (sharding) {#derived-sharding}

In-flight is tracked **per submitter thread** (sharded) rather than as one global counter, to
avoid cache-line contention at high IOPS. Each submitter is assigned a shard round-robin
(`AssignShard`, reduced modulo `NumShards` as `uint` so a long-running thread-churning server
never wraps to a negative index), and throttles on its own shard:

```
global     = effectiveThrottle
active     = activeShards                              // occupied shards (exact live count)
perThread  = clamp(global / active, 1, 128)            // 128 = MaxPerThreadInFlight
Throttle() = (this shard's in-flight) > perThread
```

`activeShards` is the number of currently *occupied* shards (shards with at least one in-flight
IO). It is maintained **exactly** by `SubmitToShard` / `CompleteShard`, which increment it on a
shard's `0→1` in-flight transition and decrement it on the `1→0` transition — so the divisor
always reflects the live set of concurrently-submitting shards, with no global in-flight counter
and no periodic reconciliation. Once more submitter threads than `NumShards` are active they
collide on the fixed shard set, so this counts occupied shards rather than distinct threads; the
shard count is sized so that stays a close proxy for concurrent submitter count.

Because `perThread` saturates at `MaxPerThreadInFlight`, device-wide in-flight is bounded by
`NumShards × MaxPerThreadInFlight` = **4096** (on hosts with ≥ 16 logical processors)
independently of `T`. `T` therefore controls in-flight only in the `0 < T ≤ 4096` range; the
default `T = 4096` already sits at that ceiling. Raising the ceiling would mean growing
`SlotsPerShard` (and with it `MaxResults`), not raising `--device-throttle-limit`.

`T` is a **coarse** bound, not an exact global cap. A shard admits against whatever divisor was
in effect at the time, and keeps that budget until it drains, so shards that filled while few
were occupied hold more than the final `T / active` share. Worst case is shards becoming
occupied one at a time with no completions in between: for `T = 120` across 32 shards the
aggregate settles near `Σ(⌊120/k⌋ + 1) ≈ 510`, roughly 4× the configured value. Exact
kernel-queue safety does not depend on this — it is enforced downstream by the native ring-full
retry (a submit that finds the ring full unwinds to `Pending` and retries after a completion).

## Internal constants

These are compile-time constants in `NativeStorageDevice.cs` (and `kMaxEvents` in
`file_linux.h`). They bound the formulas above. They are **not** knobs — each has a single
correct regime — but they define the tuning envelope.

| Constant | Value | Role |
|---|---|---|
| `DefaultQueueDepth` | `4096` (1&lt;&lt;12) | default per-ring depth `D`. |
| `MaxQueueDepth` | `32768` (1&lt;&lt;15) | io_uring hard **ceiling** for `D`. |
| `DefaultThrottleLimit` | `4096` (1&lt;&lt;12) | default aggregate in-flight `T` for the Native device. |
| `kMaxEvents` (native) | `128` | **floor** for a libaio ring's native depth when the caller passes no explicit positive depth. |
| `LibaioReservationHeadroom` | `2` | over-provision multiplier on the libaio throttle-share depth. |
| `LibaioReservationFloor` | `128` (1&lt;&lt;7) | **floor** for the default libaio reservation depth. |
| `LibaioReservationCap` | `2048` (1&lt;&lt;11) | **cap** on a single libaio ring's reservation. |
| `DefaultAioMaxDevices` | `32` (1&lt;&lt;5) | default value of `AioMaxDevices` (the `--device-aio-max-devices` divisor). |
| `AioMaxDevices` | `32` | **process-wide** `public static int`. `fs.aio-max-nr` is machine-global, so "how many devices to fit within it" is a process policy, not per-device config — a static means devices created off the raw factory (cluster aux logs) honor the budget without plumbing. |
| `NumShards` | `Math.Min(2 × ProcessorCount, 32)` | per-submitter-thread shard count for in-flight de-contention. Two shards per core, capped at 32; the knee tracks peak concurrent submitters and is throttle-bounded past that. |
| `SlotsPerShard` | `256` | completion-slot free-list size per shard. |
| `MaxPerThreadInFlight` | `128` (`SlotsPerShard / 2`) | per-thread in-flight clamp; half of `SlotsPerShard` so a shard's free-list keeps 2× headroom and never empties under the throttle. |
| `ShardCounter` | `128 B` | cache-line-pair-padded per-shard in-flight counter struct (each shard's counter owns its own line, preventing false sharing). |
| `MaxResults` | `NumShards × 256` | size of the completion-context slot table (pure managed memory). |

## Floor, cap, ceiling, headroom {#floor-cap-ceiling-headroom}

The reservation math uses four distinct bounding concepts:

* **Headroom** — a **multiplicative over-provision factor applied *above* a computed
  need**. `LibaioReservationHeadroom = 2` sizes each libaio ring to *twice* its expected
  steady-state in-flight (`share`). It is not a limit; it is slack so a transient burst of
  reads does not momentarily fill the ring (which costs a ~2% ring-full IOPS dip).

* **Floor** — a **lower bound**: the value is not sized *below* it by the share math.
  `LibaioReservationFloor = 128` (and the native `kMaxEvents = 128`) keeps a ring able to hold
  a minimum useful burst even when the throttle-share math computes something tiny (e.g. a high
  ring count dividing a modest throttle), which would otherwise produce rings shallow enough to
  stall constantly. A ceiling still overrides it: the per-device budget loop runs last and
  halves below the floor when the reservation does not fit (e.g. 32 rings on a stock 65536
  budget resolve to `64`), because exceeding the budget fails device creation outright while a
  shallow ring only costs throughput.

* **Cap** — an **upper bound that is a self-imposed *policy* choice**. `LibaioReservationCap =
  2048` says a *single* libaio ring never reserves the full deep queue (`4096`) from the
  global budget, because no single ring needs that much in-flight and the remainder stays
  available to other coexisting devices. Exceeding it breaks nothing physically.

* **Ceiling** — an **upper bound imposed by a *hard external constraint*** (kernel limit,
  hardware, or a shared global budget), not a policy preference. Violating it is a hard
  failure. Three appear in the math:
  * `ceilingDepth` = the resolved `--device-queue-depth` — never reserve more than the ring is
    actually sized for.
  * `MaxQueueDepth = 32768` — io_uring's hard kernel maximum entries per ring.
  * `perDeviceBudget = fs.aio-max-nr / AioMaxDevices` — the machine-global libaio budget
    divided by the device target; exceeding it makes `io_setup` fail with `EAGAIN`.

`ResolveLibaioReservationDepth` applies them in order: compute `share`, multiply by
**headroom**, raise to the **floor**, lower to the policy **cap**, lower to the queue-depth
**ceiling**, then lower again to the hard global-budget **ceiling**.

## Tuning recipes

Start from the defaults — on x64 Linux they reach peak out of the box: the smart io-context
default sizes rings to the hardware, and the Native throttle default of 4096 keeps NVMe
queues full. Only reach for the knobs below for a specific reason.

* **io_uring, high connection count.** Set `--device-io-contexts` ≥ your peak concurrent
  connections (e.g. `96` or `128`). The default caps at 64 rings; more connections than that
  want more rings to stay 1:1 and contention-free.
* **libaio.** Leave `--device-io-contexts` at the default (ring-count-neutral). On a host left
  at the stock `fs.aio-max-nr` of 65536 the per-device budget ceiling caps each device at 2048
  events, halving the default 4096 throttle — worth ~9% on a disk-serving workload, and silent
  (no error). Size the budget so `fs.aio-max-nr / --device-aio-max-devices ≥` your throttle
  (`sysctl -w fs.aio-max-nr=1048576`, persisted under `/etc/sysctl.d/` so it survives reboot).
  If you run **many** Native devices in one process (cluster with many shards/AOF/checkpoint
  logs) and hit `io_setup` `EAGAIN`, either raise `fs.aio-max-nr` or raise
  `--device-aio-max-devices` so each device reserves a smaller slice of the budget
  (`perDeviceBudget = fs.aio-max-nr / device-aio-max-devices`). The reservation guard logs an
  actionable warning when `N × D` exceeds `fs.aio-max-nr`, and when a device's own reservation
  cannot be brought within its per-device share (one event per ring is the floor).
* **Memory-constrained host.** Lower `--device-queue-depth` (e.g. `1024`) to cut io_uring ring
  memory ~4× (uring ring memory ≈ `N × D × ~100 B`), and/or lower `--device-throttle-limit` to
  cut pinned read-buffer memory (`T ×` read size). Both reduce a ceiling, not steady-state work.
* **Latency-sensitive over throughput.** Fewer, shallower rings and a lower throttle reduce
  queueing depth at the cost of peak IOPS.

## Diagnostics

* Watch `iostat -x 1` on the tiered mount: at the serving peak the device queue (`aqu-sz`)
  should be deep and `%util` ~100%. A shallow queue with idle device indicates the throttle or
  ring depth is too low (or, for io_uring, too few rings serializing submitters).
* A startup `TsavoriteException` / `DllNotFoundException` naming `fs.aio-max-nr`,
  `io_uring_disabled`, seccomp, or an old kernel is the native init surfacing an actionable
  cause; a missing `liburing.so.2` is the most common load failure.

## Related

* [Configuration](configuration.md) — how all Garnet settings are parsed and applied.
* [Storage layer (Tsavorite)](tsavorite/intro.md) — the hybrid-log allocator that drives the device.
