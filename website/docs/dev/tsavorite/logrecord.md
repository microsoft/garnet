---
id: logrecord
sidebar_label: LogRecord
title: LogRecord
---

# LogRecord

The `LogRecord` struct is a major revision in the Tsavorite `ISessionFunctions` design. It replaces individual `ref key` and `ref value` parameters in the `ISessionFunctions` methods with a single LogRecord, which may be either `LogRecord<TValue>` for in-memory log records, or `DiskLogRecord<TValue>` for on-disk records. There are a number of additional changes in this design as well, as shown in the following sections.

## All Keys are now `SpanByte`

Originally, Tsavorite was templated on the key type. In this revision, all keys are now `SpanByte`. Any key structure must be converted to a stream of bytes and a SpanByte created over this. This can be a stack variable (which is not subject to GC) or a pinned pointer. 

This has simplified the signature and internal implementation of TsavoriteKV itself, the sessions, allocators, ISessionFunctions, Iterators, and so on. And we now have only two allocators, `SpanByteAllocator` and the new `ObjectAllocator`.

## Removal of `BlittableAllocator`

As part of the migration to `SpanByte`-only keys, `BlittableAllocator` has been removed. Tsavorite Unit Tests such as `BasicTests` and the YCSB benchmark's fixed-length test illustrate simple ways to use stack-based 'long' keys and values with `SpanByte`. This does incur some log record space overhead for the key's or value's length prefix.

## Replace `GenericAllocator` with `ObjectAllocator`

With the move to `SpanByte`-only keys we also created a new `ObjectAllocator` for a store that uses an object value type. `GenericAllocator` is not able to take SpanByte keys, and it also uses a separate object log file. `ObjectAllocator` uses a single log file; see the separate `ObjectAllocator` documentation for more details.

## Overflow Keys and Values

To keep the size of the main log record tractable, we provide an option to have large SpanByte keys and values "overflow"; rather than being stored inline in the log record, they are allocated separately by the `OverflowAllocator`, and a pointer is stored in the log record (with the key or value length being `sizeof(IntPtr)`). This does incur some performance overhead. The initial reason for this was to keep `ObjectAllocator` pages small enough that the page-level object size tracking would be sufficiently granular; if those pages are large enough to support large keys, then it is possible there are a large number of records with small keys and large objects, making it impossible to control object space budgets with sufficient granularity. By providing this for SpanByte values as well, it allows similar control of the number of records per `SpanByteAllocator` page.

### OverflowAllocator

The `OverflowAllocator` provides native-memory pointers to support overflow keys and values. It has two sub-allocators, both of which use a `NativePageAllocator` to provide the actual memory:
    - `FixedSizePages` for keys and values up to `FixesSizePages.MaxExternalBlockSize`. This sub-divides a native allocation into smaller blocks via pointer-advancement, with additional pages allocated as needed. The size of the page is configurable. It has a list of `freeBins` that are linked lists of pointers in power-of-2 sizes from `FixedSizePages.MinInternalBlockSize` to `FixedSizePages.MaxInternalBlockSize`. All allocations in this allocator are done in power-of-2 sizes, similar to a buddy allocator, but bounded by size to minimize wasted space.
      - The difference between `MaxExternalBlockSize` and `MaxInternalBlockSize` is the size of the block header, which is used to track the allocations sizes and to link blocks together in the free list.
    - `OversizePages` for keys that are larger than `FixesSizePages.MaxExternalBlockSize`

### BlockHeader
### MultiLevelPageArray
### SimpleConcurrentStack
### NativePageAllocator
### FixedSizePages
### OversizePages

## LogRecord<TValue>

## DiskLogRecord<TValue>
