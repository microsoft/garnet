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

For each page, `ObjectLogWriter` writes out-of-line components to the flush's `ObjectLog` in record order and stamps
their resulting positions and hints into the resident page: the main object log for a ReadOnly flush, the checkpoint's
own for a Snapshot flush, and the destination of a version 7 up-conversion during recovery. The page then requests the
object log's flush and issues its own device writes, and it completes only once both are durable, so
`FlushedUntilAddress` never covers a record whose objects are not on the device.

Current-format ReadOnly, Snapshot, and recovery flushes write the resident page directly rather than allocating and
copying a full page image. A write covers whole sectors, `[alignedStartOffset, RoundUp(endOffset, sectorSize))`; bytes
above the logical endpoint reach disk verbatim, and recovery never parses them because it bounds its record walk by the
unrounded endpoint. A ReadOnly flush that starts mid-sector is the exception for its first sector: it writes that
sector from a pooled buffer holding the sector's durable bytes followed by its own, and issues its writes only after
the earlier write of that sector completes (see [Object-log serialization](objectlog-serialization.md), section 5.6).
A ReadOnly or Snapshot page whose `objectIdMap` is empty has no out-of-line data, so it writes that
span directly without walking its records. ReadOnly releases epoch protection while object serialization and device IO
run.

Object-allocator Snapshot writes use a fixed-size completion window of up to 64 pages, with or without an object log.
Several pages may be in flight, but `SnapshotFlushCoordination` advances the ReadOnly page limit only through the
contiguous completed prefix. A ReadOnly flush therefore waits only when it catches an unfinished Snapshot page;
completed pages remain available for main-object-log restamping and eviction. A real-device allocator other than
`ObjectAllocator` installs no coordination and keeps parallel Snapshot issuance.

The no-copy path requires a record's flush-critical bytes to remain readable until its asynchronous page write
completes. Tsavorite's releases of a superseded, deleted or elided record change none of its bytes (see
[Releasing a record's out-of-line value](#releasing-a-records-out-of-line-value)). `OnDispose` implementations must copy
off cleanup state and defer clearing those bytes until that write finishes.

## Read and recovery flow

Point reads, scans, and recovery use `ObjectLogReader`. One read, sized by its hint, fetches the extent a record's
position addresses; a value-sized extent longer than the 4 MiB first read takes a second. A directory then gives the
exact address and length of every chunk, so an overflow array is
allocated once at its final size and a heap-object deserializer reads a dense stream bounded by its own byte count. A
version 7 object log is read instead through `LegacyObjectLogReader` and its `CircularDiskReadBuffer` read-ahead ring.

Snapshot recovery first restores the durable main-log prefix and overlays only the Snapshot suffix. If an evicted
Snapshot page must become durable in the main log, its records' components are copied from the checkpoint's object log
into the main object log, and the live records are restamped with their new positions before the page is written.

## Transient objectId remapping

Single-record disk reads and iterators use a transient `ObjectIdMap`. `DiskLogRecord` remaps recovered objectIds into
that map so the source page can be evicted and its page-local map reused without invalidating the temporary record.
Recovery Pass 2 instead repopulates the resident page's own `ObjectIdMap`.

## Releasing a record's out-of-line value

A record's out-of-line value, an overflow array or a heap object, is released once nothing needs it: when a CopyUpdate,
a Delete or an expiration supersedes the record, or when an elision removes the record from its hash chain. A release
takes one of two forms, depending on whether anything else may still read the record.

### Terminal releases

A *terminal release* releases the value of a record that is superseded, deleted or elided, and that is never rewritten
or reused while its page is resident, while something that does not hold the key's lock may still read it:

- a scan copies a record on a page entirely below `SafeReadOnlyAddress` without a lock, and a pull scan
  (`LogAccessor.Scan`) never locks;
- a flush writes the live page; and
- the deferred-disposal drain holds no lock.

So a terminal release changes only the page's `ObjectIdMap`, never the record's bytes:

1. It credits the size tracker with what it drops (see [Accounting](#accounting)).
2. For a deletion or an elision, it runs the application's dispose trigger (`IRecordTriggers.OnDispose`, with
   `DisposeReason.Deleted` or `DisposeReason.Elided`), which sees the value.
3. It points the value's slot at a [placeholder](#placeholders). It does not dispose the object, except a CopyUpdate's
   source, whose new record has taken the value; an application releases a deleted value's external resources in the
   trigger.

The key, the optionals (ETag and expiration), the object-log position, the filler and the header stay as they are, and
the slot is not freed (see [Retained slots](#retained-slots)). Debug builds assert that `ReleaseInPlace` changes none
of the record's bytes after its `RecordInfo`.

| Release | Where |
|---|---|
| A CopyUpdate's main-log source; the object is disposed | `ReleaseCopyUpdatedSourceInPlace` |
| A Snapshot capture's overflow source; the slot gets the checkpoint's sentinel | `CompleteOverflowCapture` |
| A source deleted by a Delete or an expiration | `ReleaseSupersededSource`, or later the drain or eviction (`RunDeferredDispose`), through `ReleaseInPlace(Deleted)` |
| A source elided below the revivifiable range | `DetachBypassedSource`, through `ReleaseInPlace(Elided)` |
| An expired source that an RMW elides in place, with no new record, below `ReadOnlyAddress` | `InternalRMW` (`RMWAction.ExpireAndStop`), through `ReleaseInPlace(Deleted)` |

### Exclusive resets

An *exclusive reset* changes a record's layout when nothing else can read the record: it frees the slot of each
out-of-line field it clears, writes `InvalidObjectId` into the field, and publishes an inline header
(`LogField.ClearObjectIdAndConvertToInline`). A reset through `OnDispose` also credits the size tracker, runs the
application's trigger with its `DisposeReason`, and clears the optionals; it keeps a deleted record's key, which the
chain needs, and clears any other record's key too. The exclusive resets are:

- a record being reused: a free-list transfer, then revivification; the record is sealed and invalidated first;
- a record that was never published: a failed compare-and-swap, or an abandoned insert;
- a record in the mutable region, under the key's lock: an in-place Delete, an in-place expiration, the reinitialization
  of an expired record, and the value of a new record that becomes a tombstone. A tombstone stays in the chain, and a
  later insert of the key may revivify it there, which needs an inline value and no optionals. An expired source that an
  RMW elides in place at or above `ReadOnlyAddress` is converted for the same reason;
- transient and disk records, whose maps are private; and
- a read-cache source's value: nothing flushes or scans the read cache, and every reader of a read-cache value holds the
  key's bucket lock.

A reset holds the key's lock, and reaches only records at or above `ReadOnlyAddress` and new records its operation has
not inserted, none of which can be on a page entirely below `SafeReadOnlyAddress` before the operation ends. A scan that
has the store locks every valid or sealed record on any other page while it copies it, so only a pull scan can copy a
record that a reset is changing; see [ObjectScanIterator](logrecord.md#objectscaniterator).

### Placeholders

| Slot | Placeholder | Reads as |
|---|---|---|
| Object value | `SupersededObjectValue.Released`, or a Snapshot checkpoint's `ObjectSentinel` for a value that checkpoint captured | null, from `LogRecord.ValueObject` |
| Overflow value | `OverflowByteArray.Released`, or a Snapshot checkpoint's `OverflowSentinel` | an empty value |

Each overflow placeholder is a shared, header-only array marked as a sentinel. Pointing a slot that already holds a
placeholder at a placeholder changes nothing, and a placeholder counts as no heap, so a value is dropped and credited
only once. A flush writes a record whose slot holds a
placeholder with a null or empty value, unless the placeholder names the flush's own checkpoint as the one that captured
the value ([Object-log serialization](objectlog-serialization.md), section 13.4). A resident record's object slot is
never null: an object value with no bytes in the object log loads as `SupersededObjectValue.Released`.

Keys are not released by a terminal release: an elided record keeps its key until its page is evicted. A scan or a
traceback that reached the record before the elision still finds a well-formed, sealed record, whose key it can compare.
Overflow keys are rare, so the memory they hold until eviction is small.

### Retained slots

A terminal release never returns the value's slot to the page's free list. The record keeps the slot's id, so the slot
cannot be given to another record while anything can still read this one: a scan's copy, a flush, the drain or
eviction. A reader that read the id before the release resolves it afterwards to the placeholder, never to another
record's value. A retained slot is freed when its page is evicted (`FreePage` clears the page's map), or by an exclusive
reset that reuses the record, which frees whatever id each field holds, placeholder or not.

**What retention costs.** A freed slot is reused only by a later allocation from the same page's map, and only records at
or above `ReadOnlyAddress` allocate: new records at the tail, in-place updates that give a value a slot, and
revivification. Almost every terminal release is of a record below `ReadOnlyAddress`, on a page that no longer
allocates, where a freed slot would never be reused anyway:

- the drain and eviction release records below `FlushedUntilAddress`;
- an elision is a terminal release only below the revivifiable range, which starts at `ReadOnlyAddress` with the default
  `RevivifiableFraction` of 1.0, whether or not revivification is enabled; and
- an RMW's in-place expiration is a terminal release only below `ReadOnlyAddress`.

Retention costs something only on a page that still allocates after the release:

- **The page a FoldOver checkpoint leaves straddling `ReadOnlyAddress`.** The checkpoint shifts `ReadOnlyAddress` to the
  tail, mid-page. Deletions that land below the tail during the checkpoint's flush are marked, and drained after it,
  while new records fill the rest of the page. That is the page where reuse would be a hazard (below). There is one such
  page per FoldOver checkpoint, and only the deletions made during its flush are affected.
- **A deleted source at or above `ReadOnlyAddress`,** released by the operation that superseded it. This is rare: a
  Delete or an expiration there is normally done in place, as an exclusive reset. It happens when an in-place update
  fails and an RMW then expires the key through a new record.
- **Elisions above `ReadOnlyAddress`,** only when `RevivifiableFraction` is below 1.0.

**Bound.** Retention never allocates; it only prevents reuse. So a page's map never holds more entries than it would if
none of its records had been released: one per out-of-line field of each record on the page, which a page of live
records needs anyway.

**Size.** A map entry is one object reference, 8 bytes; the released value, which is the large part, becomes collectable
at once. The map grows in blocks of 1024 entries (8 KB), and clearing it keeps the first 16 blocks for the next page that
uses it, so a few hundred retained entries on one page usually cost no new block.

**Why there is no shared placeholder slot.** An alternative gives each page one placeholder slot, writes that slot's id
into a released record, and frees the record's own slot for reuse. It gains only where it is unsafe. A freed slot is
reused only on a page that still allocates, and there a reader that read the old id before the swap resolves it
afterwards: a scan that locked the key while the drain, which takes no lock, released the record, or any pull scan. If
another record on the page has been given the slot meanwhile, the scan reports that record's value, and a flush walking
the record publishes the other record's object at this record's address, setting that object's owner and phase as if
this record had published it. Reuse is safe only once every reader that may hold the old id has passed an epoch
barrier. The swap also writes the record's id field, and it needs a placeholder slot per active Snapshot checkpoint,
because a captured source points at its own checkpoint's placeholder. It would recover at most 8 bytes per released
value, on the few pages listed above.

If a measurement ever shows that reuse matters, the safe form writes the shared placeholder slot's id into the record,
accepting that one field write, and puts the old id on a per-page retire list tagged with the current epoch, from which
`Allocate` draws only once `SafeToReclaimEpoch` has passed the tag. That needs no epoch bump per release, but adds the
record write, the retire list and the per-checkpoint slots.

`ObjectDeleteDisposeTests.ObjDeletionsFrozenForFlushKeepTheirSlotsWhenTheDrainReleasesThemTest` checks the bound on a
FoldOver checkpoint's straddling page, and that a scan's copy of a record the drain releases resolves to that record's
value or to nothing.

### Accounting

A placeholder counts as no heap: `CalculateHeapMemorySize` and `GetValueHeapMemorySize` skip it, since an object
placeholder reports a size of 0 and an overflow one is recognized by `IsSentinel`. So every release credits the full
size of what it drops (`CreditDisposedHeap`):

- a deletion credits the value; the key stays, and is credited at eviction;
- an elision or a free-list transfer credits the key and the value, or only the key of a tombstone, whose value was
  credited when it was deleted; and
- a failed compare-and-swap or an abandoned insert credits nothing, because a new record's key and value are charged
  only once its compare-and-swap succeeds.

A record that becomes invalid while it still holds an overflow key, as when an elision follows an in-place Delete or
expiration, has the key credited then, because eviction skips invalid records.

Counting a placeholder as zero gives one rule. Keeping back each sentinel's nominal share for eviction to credit instead
would break for tombstones and invalid records, which eviction does not count, so every release would have to know the
record's future; and a shared sentinel is not per-record memory.

### Timing

A terminal release changes no bytes, but it drops the value and, for a deletion, runs the delete trigger. So the release
of a deleted source waits while something still needs either:

- **a ReadOnly flush** may be writing the record (`IsFrozenForFlush`, from `FlushedUntilAddress` to `ReadOnlyAddress`):
  the release waits so that the flush writes the value, and the delete trigger runs after the flush's OnFlush trigger
  and read-only observer;
- **a checkpoint** still needs the record's `(v)` image, which includes its value and any external state the delete
  trigger would remove; or
- **a streaming snapshot's scan** may still push the record.

Such a release is marked `DeferredDispose` on the record and run by the drain or by eviction; see
[The same shape, without a checkpoint](state-machine.md#the-same-shape-without-a-checkpoint). An elision is declined
for such a record, because it invalidates the record. A CopyUpdate's release never waits: its new record holds the key's
value, and no trigger runs.

### What a scan sees

A scan reports a superseded record whose value was released as having no value: an object value reads as null, and an
overflow value as empty. A copy of such a record is released too: `LogRecord.TryCopyFrom` gives it the released
placeholder, or an empty value. Scan compaction and `Iterate` copy every record they scan into a temporary store, so they
make such copies.

A scan that reports only each key's latest version (`ScanCursor`, `IterateLookup`) copies a record before it looks for a
newer version of the key. A value is released only once the record that supersedes it is in the chain, so an unbounded
scan that copied a released value finds the newer version and skips the record. A bounded scan (one with a `maxAddress`,
such as `IterateLookupSnapshot`, or `Iterate`, which ends at the tail it started from) looks only below its bound. So it
can report a key's last version within its bound after an operation above the bound superseded that version and released
its value: the key, with no value. Only a streaming snapshot's scan holds such releases back (see [Timing](#timing)).
