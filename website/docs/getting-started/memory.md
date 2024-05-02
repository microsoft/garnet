---
id: memory
sidebar_label: Memory
title: Managing memory usage of Garnet
---

# Memory Usage

For large-scale production scenarios, Garnet's memory usage needs to be tuned to make optimal use
of available memory on a machine. Here, we discuss the components of memory, and how to tune them.
Configuration parameters are listed [here](configuration).

Garnet is designed as two storage instances of Tsavorite: the main store and the object store. Each one is independently configured for memory. If you use only raw strings (e.g., `GET`, `SET` and their variants), `HYPERLOGLOG`, and `BITMAP` commands, you should disable the object store using the `DisableObjects` (`--no-obj`) parameter. This will avoid wasting memory for the object store.

## Main Store

Memory used by the main store consists of the sum of three components:
* Index size
* Log size
* Overflow buckets

### Index Size

The index size is configured using the `IndexSize` (`-i` or `--index`) parameter. It specifies the total size in bytes
that the index occupies in main memory. The index is organized as hash buckets, where each bucket is 64 bytes long, i.e.,
the size of a cache line. The bucket holds 7 entries and a pointer to an overflow bucket, described [below](#overflow-buckets).

The rule of thumb for sizing the index is: if you expect the cache-store to hold K keys, set the size to `K * 16` bytes. The
reasoning for this is:
* We want buckets to be half full on average, so around 4 keys per bucket
* Therefore, with K keys, we want `K / 4` buckets
* Each bucket takes up 64 bytes
* So, the total size is `64 * (K / 4) = K * 16` bytes

### Log Size

The index described above does not hold keys or values. Instead, both keys and values are stored in a separate structure 
called the hybrid log. The memory occupied by the log is configured using `MemorySize` (`-m` or `--memory`).

Memory is organized as a circular buffer of pages, where each page has size cofigured using `PageSize` (`-page`). The page
size controls the maximum key or value size you can store, as a record needs to fit entirely within a page.

A record in the Garnet main store consists of:
* An 8-byte header, called `RecordInfo`, which holds metadata and the logical address of the previous entry in a record chain.
* The key, which consists of an 8-byte header followed the the key bytes (`SpanByte`)
* The value, which consists of an 8-byte header followed the the value bytes (`SpanByte`)


### Overflow buckets

Each hash bucket has 7 entries (slots) that store the root of a chain of records stored in the log. If the hash bucket for
a given key is full, we overflow into extra buckets called overflow buckets that are allocated dynamically. While these
cannot be controlled or bounded, they are typically very small and can be ignored. In case, your index was sized too small, 
they can take up more space, and to combat this, you can dynamically grow the index as described [below](#auto-resizing-index).


### Auto-Resizing Index

You can configure Garnet to automatically grow the index (doubling each time) as it fills up. This is done by
configuring `IndexResizeFrequencySecs` (`--index-resize-freq`) to specify how frequently to trigger the
resizing check. Index growth is triggered if the number of overflow buckets exceeds a specified percentage
of the total number of main hash buckets. This threshold is specified using `IndexResizeThreshold` (`--index-resize-threshold`).

We also support `IndexMaxSize` (`--index-max-size`) which identifies the maximum size until which the index
will grow in size. We do not support index size shrinking at this point.


## Object Store

The index and overflow buckets are managed similarly to the main store, and the corresponding parameters
are:
* `ObjectStoreIndexSize` (`--obj-index`)
* `ObjectStoreIndexMaxSize` (`--obj-index-max-size`)

However, the log memory is handled differently, as described below.

### Log Size (Object Store)

In case of the object store, the hybrid log holds _references_ to keys and values (which are objects), rather 
than the actual keys and values themselves. The memory occupied by the object store log is configured using 
`ObjectStoreLogMemorySize` (`--obj-memory`). However, this parameter only controls the number of records
in the object store, where each record consists of:
* An 8-byte header, called `RecordInfo`, which holds metadata and the logical address of the previous entry in a record chain.
* An 8-byte reference to the key object, which is a byte array on heap (byte[])
* An 8-byte reference to the value object, which is an `IGarnetObject` instance, which could be different object types such as SortedSet, Hash, Set, etc.

In other words, when you set `ObjectStoreLogMemorySize`, you are only controlling the number of records in
memory, and not the total memory usage of the objects. Specifically, since each record is 24 bytes long,
setting `ObjectStoreLogMemorySize` to S merely implies that you can hold at most `S / 24` records in main
memory.

This means, of course, that we need to track the total memory using a different mechanism. For this, Garnet
exposes a configuration called `ObjectStoreTotalMemorySize` (`--obj-total-memory`) which represents total object 
store log memory used, including the hybrid log and the heap memory in bytes. You can use this parameter
to control the total memory used by the object store.

To summarize, the total space occupied by the object store is the sum of:
* Object store index size (and overflow buckets), as before
* `ObjectStoreTotalMemorySize`

with `ObjectStoreLogMemorySize` used to control the maximum _number_ of records in memory.

