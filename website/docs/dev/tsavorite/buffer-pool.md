---
id: buffer-pool
sidebar_label: Sector-Aligned Buffer Pool
title: Sector-Aligned Buffer Pool
---

# Sector-Aligned Buffer Pool

This document describes, from first principles, the design of Garnet/Tsavorite's default
`SectorAlignedBufferPool` — the origin-return, per-thread buffer pool used for direct disk I/O. The
implementation lives in `libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs` (with the
public buffer type and dispatch in `BufferPool.cs`, and the fallback pool in `BufferPool.Legacy.cs`).

The user-facing switches to select the legacy pool (`--use-legacy-buffer-pool`) and to size its byte budget
(`--buffer-pool-memory-budget`) are documented in
[Managing memory usage](../../getting-started/memory.md#sector-aligned-buffer-pool).

## 1. What problem does the pool solve?

Tsavorite performs direct, unbuffered disk I/O (`O_DIRECT` on Linux, `FILE_FLAG_NO_BUFFERING` on Windows). The
operating system requires those I/O buffers to be **sector-aligned**: the start address must be a multiple of the
device sector size (512 B or 4 KB), and lengths are rounded to a sector multiple. A plain `new byte[n]` guarantees
neither, and allocating one buffer per I/O would flood the GC and fragment the Pinned Object Heap.

So we need a **pool of reusable, sector-aligned byte buffers**. A "buffer" is a `SectorAlignedMemory` object: it
wraps a `byte[] buffer`, exposes an `aligned_pointer` rounded up to the sector boundary, and is handed out by
`Get(size)` and handed back by `Return()`. The external contract is just: `Get` → use → `Return`, with buffers
recycled instead of collected.

## 2. Why the legacy design collapses under load

The legacy pool (`BufferPool.Legacy.cs`) keeps one shared `ConcurrentQueue` per size level, used by all threads.
The problem is Garnet's actual access pattern: **the thread that allocates a buffer is almost never the thread
that frees it**. An issuing thread calls `Get`, but the buffer is `Return`ed later on a random
**I/O-completion thread**.

With a single shared queue, every thread contends on the same queue head/tail — cache-line ping-pong — and
throughput *inverts* with core count. In a tight Get/Return microbenchmark the legacy pool goes from ~35 Mops/s
at 1 thread to under 1 Mops/s at 64 threads. That collapse is what this design fixes.

## 3. The core insight: origin return

Instead of one shared queue, give **each thread its own private cache**. But the cross-thread-free pattern above
creates a hazard: if a completion thread caches every buffer it frees into *its own* cache, then issuing threads
(which only allocate) keep allocating fresh while completion threads hoard buffers forever — zero reuse and
unbounded memory growth.

The fix is **origin return**: a returned buffer routes back to the thread that originally allocated it (its
*origin*), not to the thread doing the freeing. Freeing threads never keep buffers; issuing threads get their own
buffers back. Every buffer therefore remembers, for its current rental, which thread's cache it belongs to — the
`originBucket` field on `SectorAlignedMemory`.

## 4. Identity: the `(pool, thread, size-class)` coordinate

A buffer's home is uniquely identified by three coordinates:

* **pool** — which `SectorAlignedBufferPool` instance. There are many (different logs/devices, different sector
  sizes); pools never share buffers.
* **thread** — the origin thread.
* **size-class** — which size bucket on the ladder (a 4 KB buffer and a 4 MB buffer live in separate caches).

Three data structures map onto these coordinates:

| Term | Type | Scope | Holds |
|------|------|-------|-------|
| **Shard** | `ThreadShard` | one per **(pool, thread)** | an array of `NumClasses` buckets |
| **Bucket** | `Bucket` | one per **(pool, thread, size-class)** | this thread's cache for one size |
| **Depot** | `DepotStripe[]` | one per **(pool)**, subdivided per class | the shared overflow (see §6) |

A **shard** is "one thread's private set of caches within one pool"; a **bucket** is one of those caches, for one
size. A thread that touches a pool for the first time lazily creates a shard
(`GetOrCreateShard` → `CreateShardSlow`).

To find *its* shard for *this* pool with no lock, each pool is assigned a small integer `slotIndex` at
construction, and each thread has a `[ThreadStatic] ThreadShard[] t_shards` indexed by that slot. So
`t_shards[pool.slotIndex]` is an O(1) lookup. Slots are recycled when pools are freed, so a stale entry is
validated with `ReferenceEquals(shard.pool, this)` before use.

## 5. Anatomy of a bucket: two lists on two cache lines

Each `Bucket` holds **two** intrusive singly-linked stacks. They are linked through the buffer's own `next`
field — a buffer is in exactly one list at a time, so a single link serves every list it may join.

1. **`localHead`** — the **owner-only local stack**. Only the owning thread touches it, so there are **no atomics
   and no locks**. This is the hot path.
2. **`crossThreadHead`** — the **cross-thread return stack**, where *foreign* threads push buffers back to this
   bucket's origin. It is lock-free.

The two lists are deliberately placed on **separate 64-byte cache lines**. This is done with an explicit
`[StructLayout(LayoutKind.Explicit)]` on the `Bucket`: the owner-hot fields (`localHead` and the immutable
identity fields) sit at offsets `[0, 64)` and `crossThreadHead` is pinned to offset `64` with a trailing pad
reserving the rest of its line. Explicit offsets are required here — a reference-type class *ignores*
`LayoutKind.Sequential` and groups its reference fields together, so merely interleaving padding fields would
**not** separate them. Without the guaranteed separation, a foreign thread pushing to `crossThreadHead` would
invalidate the owner's cache line holding `localHead` — false sharing, the exact pathology we are escaping.

The cross-thread stack is **multi-producer / single-consumer (MPSC)**:

* Many foreign threads **push** (`TryPushCrossThread`): CAS the new node in as the new head.
* The single owner **bulk-claims the entire chain at once** (`ClaimCrossThread`): one CAS swaps `crossThreadHead`
  from its current value to `null`, taking the whole linked list. The owner keeps one buffer and splices the rest
  onto its local stack.

Claiming the *whole chain* to `null` (rather than popping one node at a time) is what sidesteps the **ABA
problem**: there is no "read head, then read head.next" window in which the head could be freed and reused
underneath the reader.

## 6. The depot: a shared, per-class, lock-striped overflow pool

The **depot** is the third tier — a shared fallback owned by the pool. Decoding the term:

* **pool-owned** — it is a single array field (`depot`) on the `SectorAlignedBufferPool`, shared by all threads
  (unlike shards/buckets, which are per-thread).
* **per-class** — it is logically one depot *per size-class*. The backing array is laid out as
  `depot[cls * DepotStripes + stripe]`, so buffers of different sizes never mix.
* **lock-striped** — within each class the depot is split into `DepotStripes` independent sub-stacks. A thread
  picks its stripe by `ThreadId & (DepotStripes - 1)`. This is classic **lock striping** (`DepotStripes` locks
  instead of 1), so several threads can push/pop concurrently without colliding. The count is
  `ConcurrencySharding.DepotStripeCount` — `2 × ProcessorCount` rounded up to a power of two, floored at 8 and
  capped at 64 — so the number of locks scales with the number of threads that can contend for them.
* **locked** — each stripe (`DepotStripe`) is a plain `Stack<SectorAlignedMemory>` guarded by a `lock` (Monitor).
  A lock is acceptable here because the depot is the **cold path** — only reached when a thread's own local *and*
  cross-thread lists are both empty (on `Get`), or when a thread's local cache is over its cap (on `Return`). A
  lock is simpler than lock-free and avoids ABA entirely.

  A `ConcurrentStack` would not work here, for three reasons:
  1. **Atomic close.** `Close()` sets the `closed` flag *and* drains the stripe under the one lock, so a push can
     never land in a stripe that has already been drained. Lock-free, such a push would strand that buffer's byte
     permit for the life of the pool — nothing ever revisits a closed stripe (§10).
  2. **Bounded capacity.** `ConcurrentStack` has no bounded form, so `DepotStripeCap` would need a separate
     interlocked counter that can drift from the actual contents. That cap is what bounds how much memory threads
     may park cross-thread, so it must be enforced atomically with the push.
  3. **No per-push allocation.** `ConcurrentStack.Push` allocates a link node per item, which is exactly what this
     pool exists to avoid; `Stack<T>` pushes into a pre-grown array.

  Contention is bounded by striping: the depot is spread `DepotStripes` ways, sized from the machine's processor
  count, each critical section is O(1), and a lock-free CAS on a single shared head would reintroduce the
  cross-core cache-line ping-pong this design removes.
* **overflow pool** — it catches buffers that cannot stay in per-thread caches, and redistributes them:
  * On **Return**, if the origin thread is at its local byte ceiling (§9) or the origin shard has retired (its
    thread died), the buffer overflows into the depot (`DepotPush`) instead of being dropped.
  * On **Get**, after checking its own local and cross-thread lists, a thread pulls from the depot (`DepotPop`)
    before allocating fresh. `DepotPop` scans every stripe of the class, starting at the thread's own — a cheap
    form of **work-stealing** so buffers parked by a now-idle thread get reused by an active one.

### Large classes are depot-only

The per-thread local/cross-thread tiers are ideal for the **small, hot** record-sized buffers a thread reuses
back-to-back. They are the *wrong* place for **large** buffers (record/flush reads above `LargeTierMinBytes`,
256 KB): under a wide value-size mix a thread rarely re-requests the same large class twice in a row, so a large
buffer parked on its origin thread mostly sits idle — and with many threads × many large classes this **strands**
big buffers and multiplies the working set, inflating peak RSS and Gen2 GC even though the byte budget is honored.

So on `Return`, **large-class buffers skip the per-thread tiers entirely and go straight to the shared depot**, on
*both* the owner-return and foreign-return paths (`page.Level >= firstLargeClass` in `ReturnOriginReturn`). Any
thread can then reuse them from the depot under the same `largeBudget`, which recovers legacy-like sharing for big
buffers while small classes keep the atomic-free per-thread fast path. Because large-buffer operations are
low-rate and the disk workload is bandwidth-bound, the striped-depot lock handoff costs effectively nothing.
Routing large owner-returns through the pool-owned depot also sidesteps the origin-shard finalize race by
construction. Empirically, at the default 1 GiB budget under a 100 B–10 MB disk read mix at 64 threads, this
raised large-class reuse from ~12–50 % to ~54–96 %, cut new allocations and Gen2 collections by roughly half, and
lowered peak RSS — with no throughput change.

## 7. The size-class ladder

Before any of the above, `Get(bytes)` must choose *which* class. Sizes are measured in **sectors**. The ladder
(`ClassOfSectors`) has three regions; at a 512 B sector it spans 512 B … 16 MB across `NumClasses = 28` classes:

| Region | Classes | Sizes (512 B sector) | Granularity |
|--------|---------|----------------------|-------------|
| exact tiny | 2 | 512 B, 1 KB | 1 sector (zero waste) |
| coarse linear | 4 | 2, 4, 6, 8 KB | 4-sector stride |
| geometric | 22 | 12 KB … 16 MB | 2 classes per doubling (≤ 1.5× waste) |

Requests above `MaxPooledSectors` (16 MB at a 512 B sector) **bypass** the cache: `AllocateUncached` returns an
exact-fit buffer that is never pooled and goes straight back to the GC on `Return`. `ClassCapacitySectors` is the
inverse (class → capacity). Both functions are pure O(1) integer math with no table scans.

The geometric region emits **two** classes per doubling — a midpoint at `1.5 × 2^octave` and a top at
`2^(octave+1)` — which is why `NumClasses` carries a `2 * GeometricDoublings` term. The second class per doubling
is what bounds worst-case over-allocation at 1.5× rather than the 2× a single class per doubling would give.

The 16 MB ceiling is chosen so that a record built on a multi-MB inline value up to nearly Garnet's ~16 MB
maximum is pooled rather than bypassed, and so the 4 MB object-log flush buffer lands exactly on a class with
zero waste. A record at the absolute inline-value cap (`0xFFFFFE` bytes) plus its key and header rounds just
past 16 MB and takes the bypass path — intentional, since such maximum-size records are rare and not worth
parking a buffer larger than 16 MB per thread-class.

## 8. The `Get` path

Given a size → class `cls` and this thread's `bucket = shard.buckets[cls]`, `GetOriginReturn` tries four tiers,
fastest first:

1. **Local stack** — pop `bucket.localHead`. No atomics. The overwhelmingly common case of a thread reusing its
   own buffers.
2. **Cross-thread stack** — if local is empty, bulk-claim `crossThreadHead` with one CAS, keep one buffer, splice
   the rest into local. This is where buffers that other threads returned to me come home.
3. **Depot** — if both are empty, `DepotPop(cls)` across the class's stripes. For **large** classes the first two
   tiers are always empty (large buffers are returned straight to the depot — see §6), so this is their normal source.
4. **Allocate** — `AllocateForBucket`: allocate a new `byte[]`, compute alignment, and try to reserve a budget
   **permit** (see §9). If the reservation succeeds, the buffer is marked `cacheable`; otherwise it is still
   served to the caller but marked non-cacheable and dropped on `Return`.

## 9. The byte budget and permits

Reuse without a ceiling means unbounded memory growth, so each pool enforces a hard **byte budget**
(`ManagedBudgetBytes`, default 1 GiB, set by `--buffer-pool-memory-budget`). To be cacheable, a buffer must
reserve a **permit** equal to its byte size, via `BudgetState.TryReserve` (a lock-free CAS loop on a single
counter). Setting the budget to `0` is an explicit opt-out that instead sets `Disabled`, short-circuiting `Get`
to `AllocateUncached` so no shard, bucket, or permit work is done at all.

The crucial subtlety: a permit is taken **once at the buffer's birth** (first allocation) and released **once at
its death** (`DropBuffer` → `ReleasePermit`, guarded by a CAS flag for exactly-once semantics). It is **not**
touched as the buffer moves between the local, cross-thread, and depot tiers. So the budget counter measures
"total live cacheable bytes this pool is responsible for," cheaply, without being touched on the hot recycling
paths.

Each buffer records which `BudgetState` it drew from (`page.budget`), so its permit can be released correctly even
after the pool has closed — for example, from a dead thread's finalizer.

### Small/large budget split

With the ladder reaching 16 MB, a burst of very large buffers could reserve the entire budget and force every
small `Get` to become non-cacheable — destroying reuse on the hot small-buffer path. To prevent this, the budget
is partitioned into **two independent `BudgetState` instances**:

* `smallBudget` — a fixed `1 / SmallBudgetDivisor` slice (default one quarter) of the total.
* `largeBudget` — the remainder.

`BudgetFor(cls)` selects the sub-budget by class at the `LargeTierMinBytes` boundary (256 KB): classes whose
capacity exceeds it draw from `largeBudget`; everything else draws from `smallBudget`. A flood of large record or
flush buffers can, at worst, exhaust the large slice; the small slice is reserved for the hot path.

### Per-thread local retention

Local retention is bounded in **bytes per thread**, not in buffers per class: one class's buffer is up to 512×
another's, and the buffer count a thread needs is its in-flight I/O pipeline depth — a property of the caller
rather than of the pool. Each thread's ceiling (`ThreadShard.localByteCap`) is an equal slice of the sub-budget
that local caching draws from:

```
threadLocalByteCap = max(smallBudget / ExpectedConcurrentThreads, MinThreadLocalBytes)
```

`ExpectedConcurrentThreads` is `2 × ProcessorCount` capped at 64, and `MinThreadLocalBytes` (1 MB) is a floor so
a small configured budget still caches. At the 1 GiB default on a 32-core or larger box that is
256 MB / 64 = **4 MB** per thread. The slices of that many threads sum to the sub-budget, so no thread can retain
enough to starve the others. Only small classes reach this path; large classes go straight to the depot (§6).

The ceiling is shared across all of a thread's size classes and is enforced only once the thread reaches it, so a
thread whose traffic is a single class keeps the whole slice. At the ceiling, admission is **max-min fair** across
the classes that thread actually uses (`TryMakeRoom`): a class holding less than an equal share
(`localByteCap / activeClasses`) is admitted, and room is made by spilling the class furthest *above* its share
(`WorstOverShare` → `SpillOneLocal`). A class whose chain is empty still counts itself active, so a starved class
is not shut out by the classes crowding it.

A spill is a **relocation, not an eviction**: the buffer moves to the shared depot, where any thread can still
reuse it, and its permit travels unchanged. The cap therefore selects *where* a buffer is cached, never *whether* —
only budget exhaustion makes a buffer uncacheable.

## 10. Lifecycle and correctness

Two events make this hard: **threads die** and **pools close** — both possibly while foreign threads are mid-`Return`.

* **Seal sentinel.** When a shard retires, each bucket's `crossThreadHead` is atomically swapped to a shared
  `Sealed` sentinel object. A foreign producer that observes `Sealed` **reroutes to the depot** instead of pushing
  into a dead thread's list that no one will ever drain; an owner-side claim that observes `Sealed` aborts. The
  sentinel is never swapped back out — it is a permanent "this list is closed" marker.
* **Dead-thread reclamation.** There is no thread-exit callback, so `ThreadShard` is **finalizable**. If a thread
  dies with buffers still cached, the finalizer walks its buckets and releases their permits back to the budget so
  it does not leak. A `drainedOnce` CAS arbitrates between the finalizer and an explicit pool `Free` so buffers are
  drained **at most once**; `Free` deliberately releases that flag again for shards it did not own (see below).
* **Pool `Free`.** `FreeOriginReturn` flips `poolState` to `Closing` (stopping new reservations and making any new
  shard born-sealed), seals and drains all registered live shards, closes every depot stripe, marks `Closed`, and
  releases the pool's slot. It is idempotent. Draining is asymmetric by ownership: the freeing thread's own shard
  is quiescent, so its local stacks are dropped outright, but a shard still owned by another **live** thread has
  its local stack **detached** (atomically) and its permits released *without* dropping the buffers — a buffer
  that owner is concurrently popping stays valid. Detaching is required rather than merely tidy, because that
  thread's thread-static slot roots `shard → buckets → localHead → buffers`; leaving the chain in place would pin
  a disposed pool's buffers for the remaining life of a long-lived (e.g. thread-pool) thread, and invisibly, since
  the permits backing them have already been returned to the budget.
* **Resurrection after detach.** A racing owner's local push and pop are both non-atomic read-then-write pairs on
  `localHead`, so either can straddle that detaching exchange and put buffers *back* into the bucket afterwards.
  Those buffers stay valid, but their permits would otherwise stay reserved for the life of the process — the
  budget of a pool freed under concurrent traffic would read permanently short. `Free` therefore **releases
  `drainedOnce` back to 0** for shards it did not own, handing repair to `~ThreadShard`: once the pool-less shard
  becomes unreachable (the owner's next `Get` fails the pool-identity check and installs a fresh shard, or the
  thread exits) the finalizer sweeps whatever was resurrected. This is safe because each buffer's permit release
  is CAS-guarded for exactly-once — so a second sweep over already-released buffers is a no-op — and no finalizer
  can run concurrently with the drain, since `Free` still holds the shard reachable via its live-shard list.
* **Pinning policy.** Optionally (`unpinOnReturn`), buffers are GC-unpinned while parked and re-pinned on rent, so
  the pool does not hold thousands of pins long-term. This policy is captured **per pool at construction**, so a
  buffer is always returned under the same pin policy it was allocated under.

## 11. Summary

A `SectorAlignedBufferPool` hands out reusable, sector-aligned I/O buffers. To scale across many threads where the
freeing thread is not the allocating thread, each **(pool, thread)** gets a private **shard** of per-size-class
**buckets**; each bucket has an atomic-free **local stack** (owner reuse) and a lock-free MPSC **cross-thread
stack** (buffers routed home to their *origin* thread). **Small** classes use those per-thread tiers as the hot
path; **large** classes (> 256 KB) bypass them and share globally through the depot. Overflow and cross-thread
redistribution go through a shared, per-class, lock-striped **depot** (8–64 stripes, sized from the processor
count). A per-pool **byte budget**, split into isolated small and large slices, caps total retained bytes via one
permit per buffer taken at birth and released at death; within the small slice each thread retains up to an equal
per-thread byte share, spilling the surplus to the depot rather than dropping it. Seal sentinels, finalizers, and
a closing state machine make thread death and pool teardown race-safe.
