---
id: memory
sidebar_label: Memory
title: Managing memory usage of Garnet
---

# Memory Usage

For large-scale production scenarios, Garnet's memory usage needs to be tuned to make optimal use
of available memory on a machine. Here, we discuss the components of memory, and how to tune them.
Configuration parameters are listed [here](configuration).

Garnet stores all data in a single unified Tsavorite store, accessed through three contexts (String, Object, and Unified). A record's `ValueIsObject` bit distinguishes values held as inline bytes (raw strings) from values held as references to heap objects (data-structure objects such as Hash, List, Set, SortedSet). If you use only raw strings (e.g., `GET`, `SET` and their variants), `HYPERLOGLOG`, and `BITMAP` commands, you can disable support for data-structure objects using the `DisableObjects` (`--no-obj`) parameter. This avoids reserving memory for object bookkeeping.

The store's memory usage is the sum of three components:
* Index
* Hybrid log
* Overflow buckets

## Index

The index size is configured using the `IndexMemorySize` (`-i` or `--index`) parameter. It specifies the total size in bytes
that the index occupies in main memory. The index is organized as hash buckets, where each bucket is 64 bytes long, i.e.,
the size of a cache line. The bucket holds 7 entries and a pointer to an overflow bucket, described [below](#overflow-buckets).

The rule of thumb for sizing the index is: if you expect the cache-store to hold K keys, set the size to `K * 16` bytes. The
reasoning for this is:
* We want buckets to be half full on average, so around 4 keys per bucket
* Therefore, with K keys, we want `K / 4` buckets
* Each bucket takes up 64 bytes
* So, the total size is `64 * (K / 4) = K * 16` bytes

### Overflow buckets

Each hash bucket has 7 entries (slots) that store the root of a chain of records stored in the log. If the hash bucket for
a given key is full, we overflow into extra buckets called overflow buckets that are allocated dynamically. While these
cannot be controlled or bounded, they are typically very small and can be ignored. In case your index was sized too small, 
they can take up more space, and to combat this, you can dynamically grow the index as described [below](#auto-resizing-index).

### Auto-Resizing Index

You can configure Garnet to automatically grow the index (doubling each time) as it fills up. This is done by
configuring `IndexResizeFrequencySecs` (`--index-resize-freq`) to specify how frequently to trigger the
resizing check. Index growth is triggered if the number of overflow buckets exceeds a specified percentage
of the total number of hash buckets. This threshold is specified using `IndexResizeThreshold` (`--index-resize-threshold`).

We also support `IndexMaxMemorySize` (`--index-max-size`) which identifies the maximum size until which the index
will grow in size. We do not support index size shrinking at this point.

## Hybrid Log

The index described above does not hold keys or values. Instead, both keys and values are stored in a separate structure 
called the hybrid log. The memory occupied by the log is configured using `LogMemorySize` (`-m` or `--memory`). This single
parameter covers the entire main-log memory budget, including both the inline bytes of raw-string records and the heap
memory referenced by data-structure objects.

Memory is organized as a circular buffer of pages, where each page has size configured using `PageSize` (`-p` or `--page`). The page
size controls the maximum key or value size you can store inline, as a record needs to fit entirely within a page.

Every record begins with an 8-byte header, called `RecordInfo`, which holds metadata (including the `ValueIsObject` bit) and
the logical address of the previous entry in a record chain, followed by the key. The value layout depends on the record type:

* For a raw-string record (`ValueIsObject` is clear), the value is stored inline as bytes within the log page, alongside the key.
* For a data-structure-object record (`ValueIsObject` is set), the value is a reference to an `IGarnetObject` instance (such as
  SortedSet, Hash, or Set) allocated on the .NET heap. The inline record holds only the reference; the object's contents live on
  the heap and are charged against the same `LogMemorySize` budget through per-object heap accounting.

Because `LogMemorySize` accounts for both inline bytes and referenced heap memory, it bounds the total main-log memory regardless
of the mix of raw strings and data-structure objects in the store.

## Read Cache

Read cache helps bring in records from disk to memory in a separate read cache region without growing the main log. This helps avoid additional I/O when reading records that are already on the disk. More details on the internals of read cache are available [here](../dev/tsavorite/locking.md#readcache).

Use the `--readcache` option to enable the read cache. The following configuration options control its memory utilization:
* `--readcache-page` controls the size of each read cache page.
* `--readcache-memory` controls the total read cache memory, covering both inline bytes and referenced heap memory.
## Sector-aligned buffer pool

Direct (unbuffered) disk I/O requires **sector-aligned** buffers. Garnet serves these from a
`SectorAlignedBufferPool`, one instance per log/device, that recycles buffers instead of allocating a fresh
pinned array for every read/flush. By default this is a **scalable, per-thread, origin-return** pool: a returned
buffer is routed back to the thread that originally allocated it, so it scales with the many I/O-completion
threads that free buffers concurrently, under a per-pool byte budget that bounds retained memory. The internals
are described in the developer guide under
[Sector-Aligned Buffer Pool](../dev/tsavorite/buffer-pool.md).

The `--use-legacy-buffer-pool` (`UseLegacyBufferPool`) switch selects the older per-size-level
`ConcurrentQueue` pool instead. It is a boolean switch (default **off**):

* **off** (default) — the origin-return per-thread pool. Recommended; it scales across concurrent
  I/O-completion threads and caps retained bytes with a per-pool byte budget.
* **on** — the legacy pool: one shared `ConcurrentQueue` per size level with no per-pool byte budget. Provided
  as a fallback; it does not scale with core count under cross-thread frees.

This switch is decided once per pool at construction and should be set at program entry (like the other pool
policies). It is independent of `--use-native-allocator`: the buffer pool always uses managed (Pinned Object
Heap) buffers in this release, regardless of the native-allocator setting.

### Buffer pool memory budget

The `--buffer-pool-memory-budget` (`BufferPoolMemoryBudget`) setting bounds how many buffer bytes the
origin-return pool keeps cached for reuse **per pool**, across all I/O-completion threads. It is a
memory-size string (default **`1g`**; e.g. `512m`, `1g`, `8g`):

* The budget is split **25% to small size-classes / 75% to large**, so a burst of large record/flush buffers
  cannot evict the hot small-buffer cache (and vice-versa).
* A buffer that would push cached bytes past the budget — or a request above the pooled size ceiling — is
  still served, but is allocated on demand and freed on return instead of being cached. This caps retained
  memory at the cost of lower reuse for that buffer.
* Raise the budget for workloads with a large working set of big values paged to/from disk (fewer
  large-object-heap allocations and less GC churn); lower it to cap the pool's steady-state footprint.
* The setting is **ignored when `--use-legacy-buffer-pool` is set** (the legacy pool has no per-pool byte
  budget), and, like the pool-selection switch, is applied once at program entry before any pool is created.
* Setting the budget to **`0` disables buffer caching entirely**: every I/O buffer is allocated on demand and
  reclaimed by the GC on return. This trades reuse for the smallest possible steady-state pool footprint, and
  is useful when you would rather give that memory to the store and let the GC absorb the buffer churn.
  Unlike a positive budget, `0` applies to **both** pool backends (it short-circuits the pool entirely rather
  than bounding it), so it is also honored under `--use-legacy-buffer-pool`.

## Native (off-heap) allocator

By default, Garnet's large, long-lived buffers — the hash index, hybrid-log pages, and recovery frames —
are allocated as pinned arrays on the managed .NET heap (the Pinned Object Heap). The optional
`--use-native-allocator` setting moves these allocations *off* the managed heap, into native memory, which reduces
GC pause times (the collector has far less to scan/compact) and removes POH fragmentation. It is **off by
default** and opt-in. (The `SectorAlignedBufferPool` IO/flush buffers always remain on the managed Pinned Object
Heap regardless of this setting.)

`--use-native-allocator` is a boolean switch (default **off**):

* **off** (default) — all allocations use the managed heap.
* **on** — routes the hash index, hybrid-log pages, and recovery frames to a direct OS
  virtual-memory allocator (`mmap`/`VirtualAlloc`). These give demand-zero, first-touch-placed pages that match
  the managed allocator's behavior, but off the GC heap — so a large index/log does not inflate the managed
  heap. On Linux these regions are 2&#160;MB-aligned and hinted for transparent huge pages, which lowers dTLB
  misses on random index/log access (measured ~8% higher in-memory throughput vs the managed heap). No native
  library is required — the direct-VM surfaces call the OS virtual-memory APIs directly. (Network buffers and
  the sector-aligned IO/flush buffer pool remain managed in this release.)

### Sizing and the GC when native memory is enabled

Native memory lives **outside** the managed GC heap, so:

* Size `GCHeapHardLimit`/`GCHeapHardLimitPercent` to leave headroom for the native pools; the GC's own limit
  does not account for native memory.
* Set `DOTNET_GCDynamicAdaptationMode=0` — DATAS (default in .NET 9+) resizes the managed heap by throughput and
  is blind to native memory, so it can grow the heap into a container OOM.
* Monitor `native_allocator_bytes` in `INFO memory` alongside `gc_heap_bytes`. For example, with a large index and
  the native allocator enabled you will see the index bytes move from `gc_heap_bytes` into `native_allocator_bytes`,
  with the managed heap becoming dramatically smaller.

Native memory is not reported to the GC via `GC.AddMemoryPressure` (which would only trigger unproductive Gen2
collections that cannot reclaim it); use the hard-limit + telemetry approach above instead.

### Platform support

The direct-VM surfaces (hash index, log pages, frames) use `mmap`/`VirtualAlloc` and need no shipped
native binary, so they work on any platform. Transparent huge pages for those regions are a Linux optimization
(`madvise(MADV_HUGEPAGE)`); on Windows they use regular pages (large-page support there needs the privileged
`SeLockMemoryPrivilege`), so the native allocator is functionally identical on Windows, just without the huge-page
throughput bonus.
