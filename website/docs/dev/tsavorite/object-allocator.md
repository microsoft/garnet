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

For the exact persisted field encoding, extent framing, and recovery rules, see
[Object-log serialization](objectlog-serialization.md).

## Hybrid log and object log

The hybrid log stores record headers, inline data, objectId slots, optionals, and object-log positions. The separate
object log stores overflow bytes and serialized heap-object bytes.

The object log is an `ObjectLog`: a log-structured store composed over `TsavoriteLogAllocatorImpl`, with its own page
ring of eight 8 MiB pages by default. `KVSettings.ObjectLogPageSize`, `ObjectLogMemorySize`, and `ObjectLogSegmentSize`
configure it, for the main object log and each Snapshot checkpoint's alike (see
[Object-log serialization](objectlog-serialization.md), section 12.6). A page flush writes its records' components into
that ring, and the ring's own flush makes them durable; reads take recent bytes from the ring and older ones from the
device.
`LogSettings.NumberOfDeserializationBuffers` (4) sets how many 4 MiB windows a walk over a page's records reads ahead
through, during recovery, scans, and the reading of a version 7 object log.

## Adaptive record fields

An objectId slot has two simultaneous roles in a flushed current-format record:

- its low 23 bits remain the `ObjectIdMap` index; and
- its high 9 bits carry an initial object-log read hint.

The high four bits of the object-log position word are format flags, while the low 60 bits hold a flat object-log
address. The position addresses one extent: the record's only out-of-line component, or a directory that lists its
chunks. An extent of at most 511 bytes is headerless and carries its byte length in the hint; a larger one uses the hint
only to size its first read, and a 4-byte length prefix carries its exact length.

Every in-memory objectId lookup masks the hint bits through `ObjectIdMap.GetIndex()`. Stamping disk-read metadata
therefore does not change the object resolved by a live record and does not change the record's physical size.

## Flush flow

For each page, `ObjectLogWriter` writes out-of-line components to the main `ObjectLog` in record order and stamps their
resulting positions and hints into the resident page. The page then requests the object log's flush and issues its own
device writes, and it completes only once both are durable, so `FlushedUntilAddress` never covers a record whose objects
are not on the device.

Current-format ReadOnly, Snapshot, and recovery flushes write the resident page directly rather than allocating and
copying a full page image. A write covers whole sectors, `[alignedStartOffset, RoundUp(endOffset, sectorSize))`; bytes
above the logical endpoint reach disk verbatim, and recovery never parses them because it bounds its record walk by the
unrounded endpoint. A ReadOnly or Snapshot page whose `objectIdMap` is empty has no out-of-line data, so it writes that
span directly without walking its records. ReadOnly releases epoch protection while object serialization and device IO
run.

Object-allocator Snapshot writes use a fixed-size completion window of up to 64 pages. Several pages may be in flight,
but `SnapshotFlushCoordination` advances the ReadOnly page limit only through the contiguous completed prefix. A
ReadOnly flush therefore waits only when it catches an unfinished Snapshot page; completed pages remain available for
main-object-log restamping and eviction. Real-device allocators without an object log retain uncoordinated parallel
Snapshot issuance.

The no-copy path requires a record's flush-critical bytes to remain readable until its asynchronous page write
completes. `OnDispose` implementations must copy off cleanup state and defer clearing those bytes until that write
finishes.

## Read and recovery flow

Point reads, scans, and recovery use `ObjectLogReader`. One read fetches the extent a record's position addresses,
sized by its hint; a directory then gives the exact address and length of every chunk, so an overflow array is
allocated once at its final size and a heap-object deserializer reads a dense stream bounded by its own byte count. A
version 7 object log is read instead through `LegacyObjectLogReader` and its `CircularDiskReadBuffer` read-ahead ring.

Snapshot recovery first restores the durable main-log prefix and overlays only the Snapshot suffix. If an evicted
Snapshot page must become durable in the main log, its records' components are copied from the checkpoint's object log
into the main object log, and the live records are restamped with their new positions before the page is written.

## Transient objectId remapping

Single-record disk reads and iterators use a transient `ObjectIdMap`. `DiskLogRecord` remaps recovered objectIds into
that map so the source page can be evicted and its page-local map reused without invalidating the temporary record.
Recovery Pass 2 instead repopulates the resident page's own `ObjectIdMap`.
