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
## Native (off-heap) allocator

By default, Garnet's large, long-lived buffers — the hash index, hybrid-log pages, IO/flush buffers, and
recovery frames — are allocated as pinned arrays on the managed .NET heap (the Pinned Object Heap). The optional
`--native-allocator` setting moves these allocations *off* the managed heap, into native memory, which reduces
GC pause times (the collector has far less to scan/compact) and removes POH fragmentation. It is **off by
default** and opt-in.

Modes (`--native-allocator <mode>`):

* `off` (default) — all allocations use the managed heap. Behavior-identical to prior releases.
* `buffer-pool` — the sector-aligned IO/flush buffer pool (`SectorAlignedBufferPool`) is backed by
  [mimalloc](https://github.com/microsoft/mimalloc). Its thread-local heaps eliminate the shared free-list
  contention that caps this pool under concurrent flush/read on fast devices. This is the primary throughput
  win and the safe first increment (a single, localized surface). This mode **requires** the mimalloc native
  library: if it cannot be loaded for the platform/RID, **startup fails** rather than silently degrading to the
  managed pool (so a missing native binary surfaces as a loud config error, not a quiet slowdown). Use `off` to
  run fully managed.
* `full` — additionally routes the hash index, hybrid-log pages, and recovery frames to a direct OS
  virtual-memory allocator (`mmap`/`VirtualAlloc`). These give demand-zero, first-touch-placed pages that match
  the managed allocator's behavior, but off the GC heap — so a large index/log no longer inflates the managed
  heap. On Linux these regions are 2&#160;MB-aligned and hinted for transparent huge pages, which lowers dTLB
  misses on random index/log access (measured ~8% higher in-memory throughput vs the managed heap). `full`
  includes `buffer-pool`, so it likewise requires mimalloc and fails fast if it is unavailable. (Network buffers
  remain managed in this release.)

### Sizing and the GC when native memory is enabled

Native memory lives **outside** the managed GC heap, so:

* Size `GCHeapHardLimit`/`GCHeapHardLimitPercent` to leave headroom for the native pools; the GC's own limit
  does not account for native memory.
* Set `DOTNET_GCDynamicAdaptationMode=0` — DATAS (default in .NET 9+) resizes the managed heap by throughput and
  is blind to native memory, so it can grow the heap into a container OOM.
* Monitor `native_allocator_bytes` in `INFO memory` alongside `gc_heap_bytes`. For example, with a large index in
  `full` mode you will see the index bytes move from `gc_heap_bytes` into `native_allocator_bytes`, with the
  managed heap becoming dramatically smaller.

Native memory is not reported to the GC via `GC.AddMemoryPressure` (which would only trigger unproductive Gen2
collections that cannot reclaim it); use the hard-limit + telemetry approach above instead.
