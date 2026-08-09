# Object-log serialization

This document describes the v2.2 on-disk format used by `ObjectAllocator` for overflow keys, overflow values, and
serialized object values. Inline key/value bytes remain in the hybrid-log record and are not covered here.

## Record metadata

`RecordDataHeader.KeyLength` and `RecordDataHeader.ValueLength` describe only the physical inline fields. They are
always exact: an inline field stores its byte length, while an out-of-line field stores `ObjectIdMap.ObjectIdSize`.
Object-log serialization never stores a length or read hint in these fields.

The object-log start position is stored after the value slot. Its low 60 bits are the segment and offset; its high bits
are:

| Bit | Meaning |
|---|---|
| 60 | `ValueIsExactSize` |
| 61 | `KeyIsExactSize` |
| 62 | Reserved |
| 63 | Legacy v2.1 `ReuseObjectIdForSize` discriminator |

Each out-of-line key/value slot is an objectId. Bits 0-22 retain the object-map index and bits 23-31 contain a 9-bit
initial-read hint:

- Exact-size flag set: the hint is the exact headerless byte length, from 0 through 511.
- Exact-size flag clear and hint 1-510: the hint is a count of 4 KB pages covering the initial framing.
- Exact-size flag clear and hint 511: read one 4 MB discovery window.

The hint is not an authoritative component length. Headered overflow lengths and object chunk lengths come from
`ChunkHeader`; a same-space successor position may provide additional safe read-ahead.

## `ChunkHeader`

`ChunkHeader` is 8 bytes:

| Field | Meaning |
|---|---|
| `currentLength` | Data bytes in this chunk; the high bit is `ContinuationFlag` |
| second word | Alignment padding for overflow, reserved for object chunks |

Headers and object chunks are self-framing. There is no forward `nextLength`.

## Overflow layout

An overflow key or value of at most 511 bytes is headerless:

```text
[payload]
```

A larger overflow has one header:

```text
[ChunkHeader(full payload length, alignment padding)]
[alignment padding]
[payload]
```

The writer may direct-write payloads larger than `MaxCopySpanLen` (128 KB). The header records any leading alignment
padding, and the physical write is sector-rounded.

The reader obtains the header through the circular read buffers, allocates an `OverflowByteArray` of the exact payload
length, copies the already-buffered initial bytes, and direct-reads the remaining sector-aligned range into the pinned
array. Direct reads split at object-log segment boundaries. The pin remains held until every direct read completes, and
the circular buffers are then repositioned at the logical payload end.

## Object layout

Objects of at most 511 serialized bytes are headerless:

```text
[serialized data]
```

Larger objects retain a 511-byte headerless prefix and frame all remaining data:

```text
[first 511 data bytes]
[padding to absolute 8-byte alignment]
[ChunkHeader][chunk data]
[ChunkHeader][chunk data]
...
```

The writer stamps an initial-read hint covering the prefix, alignment, first header, and first chunk. A continuation
header causes the reader to request the next 4 MB discovery window. Each parsed header replaces that discovery
requirement with the chunk's absolute required endpoint. A zero-length continuation chunk is valid when a header fills
the remaining write-buffer space.

The deserializer sees only serialized data; `ObjectLogReader` strips prefix alignment, headers, and continuation
framing.

## Read accounting and successor bounds

The circular reader tracks absolute endpoints rather than additive unread lengths:

- `baseRequiredEndAddress` covers the caller's initial one-or-more-record range.
- `dynamicRequiredEndAddress` covers the currently parsed component or object chunk.
- `nextFileReadPosition` is the sector-rounded issued high-water.

The effective requirement is the maximum base/dynamic endpoint. Tightening a dynamic endpoint cannot cancel already
issued IO; speculative reads are reused by following records or drained before buffer disposal.

Within one object-log address space, a following object-bearing record's position safely bounds read-ahead for the
current record. Snapshot and main object-log positions must never be compared or subtracted. Without a same-space
successor, framing remains authoritative and extends reads as needed.

## Packing and segment boundaries

- Components are densely packed; there is no per-record padding beyond header alignment and direct-IO padding.
- `ObjectLogFilePositionInfo.Advance` must be used for arithmetic that can cross segments.
- Overflow direct reads and writes split at segment boundaries.
- Snapshot verbatim relocation preserves the source object-log start modulo 8 so the first object header remains
  aligned at the same relative point.
- A final record whose initial hint under-counts its extent is copied during recovery by following its headers to the
  exact end.

## Recovery and versioning

The current format is checkpoint version 8 (v2.2). Version 7 (v2.1) stores exact out-of-line lengths using the historical
split RDH/objectId encoding and has no `ChunkHeader` framing. Bit 63 selects that legacy per-record decoder.

Snapshot-region recovery may copy object-log bytes verbatim and repoint only the position while preserving all format
flags. Current-format records keep their objectId hints. A guarded v2.1 reposition can convert only components that
remain byte-identical and headerless; a large v2.1 component that would require inserting v2.2 headers fails fast until
a validated v2.1 checkpoint fixture and header-insertion conversion exist.

## No-copy flush constraint

Stamping the position and objectId hints is non-destructive to normal readers because `ObjectIdMap.GetIndex` masks the
hint bits. This removes metadata mutation as a blocker to writing a live page directly, but it does not make a live-page
asynchronous flush inherently safe: record disposal can still mutate flush-visible bytes while device IO is in flight.
The private-buffer copy path remains the unconditional safety baseline.

## Network serialization

Migration and replication use independent stream framing; see
[Migration / Replication record layout](./migration-replication-record-layout.md). They do not define the object-log
file format described here.
