---
id: object-allocator
sidebar_label: ObjectAllocator
title: ObjectAllocator
---

# ObjectAllocator

`ObjectAllocator` stores inline byte fields, overflow byte arrays, and heap objects in one Tsavorite hybrid log.
Garnet uses it as its unified store allocator, so raw strings and collection objects share the same
`TsavoriteKV` instance. `SpanByteAllocator` remains available for Tsavorite configurations that need only inline
byte records, while `TsavoriteLogAllocator` backs `TsavoriteLog`.

The allocator keeps hybrid-log records compact:

- inline keys and values are stored directly in the record;
- an out-of-line key or value occupies a 4-byte objectId slot;
- a record with any out-of-line component has an 8-byte object-log position; and
- the page's `ObjectIdMap` resolves each in-memory objectId to an `OverflowByteArray` or `IHeapObject`.

`MaxInlineKeySize` and `MaxInlineValueSize` control when `ObjectAllocator` moves byte fields out of line. Smaller
hybrid-log pages remain practical because large payloads occupy only objectId slots in those pages, which also gives
object-memory tracking and recovery eviction finer granularity.

For the exact persisted field encoding, chunk framing, direct IO, and recovery rules, see
[Object-log serialization](objectlog-serialization.md).

## Hybrid log and object log

The hybrid log stores record headers, inline data, objectId slots, optionals, and object-log positions. The separate
object log stores overflow bytes and serialized heap-object bytes.

Object-log IO uses circular pools of sector-aligned 4 MiB buffers. The read and write buffer counts are configurable
through `LogSettings.NumberOfDeserializationBuffers` and `LogSettings.NumberOfFlushBuffers`; both default to four.
Writers can flush one buffer while filling another, and readers issue bounded read-ahead while consuming earlier
buffers. Large overflow payloads use aligned direct IO for their interior instead of copying all bytes through the
ring.

## Adaptive record fields

An objectId slot has two simultaneous roles in a flushed current-format record:

- its low 23 bits remain the `ObjectIdMap` index; and
- its high 9 bits carry an initial object-log read hint.

The high four bits of the object-log position word are format flags, while the low 60 bits hold the segment and
offset. Exact out-of-line components of at most 511 bytes are headerless and carry their byte length in the objectId
hint. Larger components use the hint only to size initial IO; `ChunkHeader` framing in the object log carries
authoritative lengths.

Every in-memory objectId lookup masks the hint bits through `ObjectIdMap.GetIndex()`. Stamping disk-read metadata
therefore does not change the object resolved by a live record and does not change the record's physical size.

## Flush flow

`ObjectAllocatorImpl.AsyncFlushPagesForReadOnly()` creates one `CircularDiskWriteBuffer` for the issued range. For
each page, `ObjectLogWriter` serializes out-of-line components in record order and stamps their resulting positions
and hints into the resident page. The object-log writes are issued before the hybrid-log page write, and a shared
completion batch prevents `FlushedUntilAddress` from advancing until both are durable.

Current-format ReadOnly, Snapshot, and recovery flushes write the resident page directly rather than allocating and
copying a full page image. A partial write may use one pooled trailing-sector buffer to preserve bytes through the
logical endpoint and zero the non-durable suffix. ReadOnly releases epoch protection while object serialization and
device IO run.

Object-log Snapshot writes use a fixed-size completion window, normally the object-log flush-buffer count. Several
pages may be in flight, but `SnapshotFlushCoordination` advances the ReadOnly page limit only through the contiguous
completed prefix. A ReadOnly flush therefore waits only when it catches an unfinished Snapshot page; completed pages
remain available for main-object-log restamping and eviction. Real-device allocators without an object log retain
uncoordinated parallel Snapshot issuance.

The no-copy path requires a record's flush-critical bytes to remain readable until its asynchronous page write
completes. `OnDispose` implementations must copy off cleanup state and defer clearing those bytes until that write
finishes.

## Read and recovery flow

Point reads, scans, and recovery use `CircularDiskReadBuffer` plus `ObjectLogReader`. A page scan establishes bounded
same-address-space read-ahead; each record's hints and framing then tighten or extend demand. Large overflow reads can
read directly into their final `OverflowByteArray`, while heap-object deserializers receive a dense logical stream
with chunk headers and alignment padding removed.

Snapshot recovery first restores the durable main-log prefix and overlays only the Snapshot suffix. If an evicted
Snapshot page must become durable in the main log, its object bytes are copied verbatim to the main object log and
the live record positions are repointed before the page is written.

## Transient objectId remapping

Single-record disk reads and iterators use a transient `ObjectIdMap`. `DiskLogRecord` remaps recovered objectIds into
that map so the source page can be evicted and its page-local map reused without invalidating the temporary record.
Recovery Pass 2 instead repopulates the resident page's own `ObjectIdMap`.
