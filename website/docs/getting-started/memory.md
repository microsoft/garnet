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