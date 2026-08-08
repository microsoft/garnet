# Object-log serialization (Flush / file-IO format)

This document describes the **on-disk byte layout** the `ObjectAllocator` writes to the **object-log device** for a
record's out-of-line pieces — an **overflow key**, an **overflow value**, or a streamed **object value** — during a
**Flush**, and how the reader fetches them back **precisely** (reading only the bytes needed) with **fetch-ahead**.

> This is the **file-IO** context. Migration / replication serialize the same pieces over the **network** with a
> different, simpler framing (the network feeds a whole buffer at a time, so it has no fetch-ahead concern); see
> [Migration / Replication record layout](./migration-replication-record-layout.md). Inline keys/values (of any length)
> are written to the **main** log as part of the record and are not covered here.

> **Keep this document in sync with the code.** If any of the following change, update the layouts below:
> - `libs/storage/Tsavorite/cs/src/core/Allocator/ObjectSerialization/ObjectLogWriter.cs` / `ObjectLogReader.cs`
> - `libs/storage/Tsavorite/cs/src/core/Allocator/ObjectSerialization/CircularDiskWriteBuffer.cs` / `CircularDiskReadBuffer.cs`
> - `libs/storage/Tsavorite/cs/src/core/Allocator/ObjectSerialization/DiskWriteBuffer.cs` / `DiskReadBuffer.cs`
> - `libs/storage/Tsavorite/cs/src/core/Allocator/ObjectSerialization/ChunkHeader.cs`, `ChunkedRecordConstants.cs`
> - `libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs` — RDH `KeyLength`/`ValueLength` encodings.
> - `libs/storage/Tsavorite/cs/src/core/Allocator/ObjectSerialization/ObjectLogFilePositionInfo.cs` — position word + flags.
> - `libs/storage/Tsavorite/cs/src/core/Allocator/LogRecord.cs` — `SetObjectLogPositionAndLengthHints`,
>   `GetObjectLogRecordStartPositionAndLengths`, `RepointObjectLogPosition`, `SetRecoveredObjectLogRecordStartPosition`,
>   `*_v21` decoders (`LogRecord_v21.cs`).
> - `libs/storage/Tsavorite/cs/src/core/Allocator/ObjectAllocatorImpl.cs` — flush, single-record read, scan/recovery.

---

## 1. Design goals

1. **Precise reads** — IO only the bytes a record needs (never "read 4 MB to get 4 KB").
2. **Fetch-ahead** — learn a record's total object-log extent *before* touching the object log, so the whole read-ahead
   ring fills immediately (no serial "read a header, then issue the real read").
3. **Zero overhead for small objects** — a small object/overflow (e.g. a 5-int list) pays **no** per-chunk header.
4. **No-copy-ready format** — the format lets flush make only **non-destructive** edits to the live record, so the page
   *could* be written to the device directly from live memory (the remaining blocker is concurrent record mutation, not the
   format; see §7).
5. **Maximal object-log address space** — spend as few `ObjectLogPosition` flag bits as possible.

Mechanism: encode a record's out-of-line **extent in the RDH length fields** (already in the reader's hand) → drives
fetch-ahead + precise reads with no forward-length look-ahead and no destructive edit.

Constants: `BufferSize = 4 MB` (bits=22); `MaxCopySpanLen = 128 KB`; key sentinel `= 1023`; `ChunkHeader.TotalSize = 8`.

---

## 2. RDH length-field encodings (Flush context)

Inline fields = **just the byte length**. The encodings below apply only to **non-inline** fields, Flush context.

### 2.1 `ValueLength` (24-bit field; low 12 bits used) — non-inline value

Object **and** overflow values share the same 12-bit out-of-line encoding, `EncodeFlushOutOfLineValue(dataLength, totalOnDiskExtent)`
(decoded to the initial read-ahead extent by `DecodeFlushValueInitialReadExtent`; `SetObjectLogLengthHints` writes it). Only the
low 12 bits of the physical 24-bit field are used; the `ValueLength` **property** still returns `ObjectIdSize` for a non-inline
value, so the encoding is read via `GetValueLengthRaw()`.

| bit 11 `isExactSize` | bit 10 `hasHeader` | bits `[9:0]` | Meaning |
|---|---|---|---|
| 1 | 0 | exact byte length `0..1023` | **Headerless** value ≤ 1023 bytes; no `ChunkHeader` precedes it. |
| 0 | 1 | 4 KB-page count of the on-disk extent | **Headered** value > 1023 bytes; a leading `ChunkHeader` precedes it. `1023` = **sentinel**: extent ≥ 1023×4 KB (~4 MB) — read in 4 MB blocks and learn the exact length(s) from the `ChunkHeader`(s). |

The page count spans the value's **whole on-disk extent** (leading `ChunkHeader` + any DMA/8-align padding + data), so a
below-sentinel headered value's first read already covers all of its framing.

- **Object** (> 1023 data bytes) on-disk layout: `[1023-byte headerless prefix][hdr₁][chunk₁]…[hdr_N][chunk_N]`. The prefix keeps
  small objects header-free; beyond it a **chunk == one write buffer's worth of data**, each preceded by an 8-byte `ChunkHeader`
  (`currentLength | ContinuationFlag`). The deserializer self-terminates. See §3, §5.
- **Overflow** (> 1023 data bytes): a single leading `ChunkHeader` carries the full length + O_DIRECT `alignmentPadding`; the data
  follows (one chunk, no continuation).

> The cutoff is 1023 because that is the largest exact byte length the 10-bit payload holds. Retired: the old bit-23-chunked /
> bit-22-overflow-header / 24-bit-exact object scheme (`EncodeFlushObjectValue`) and `DecodeFlushValueExtent`.
> `ChunkHeader.currentLength` is a 32-bit int with the top bit as `ContinuationFlag`, so a chunked value is capped at `1<<30`
> per chunk. Size-tracker reconciles count→bytes against the real heap size after deserialize.

**Invariant note (useLivePage):** a no-copy (`useLivePage`) flush stamps this encoding into the **live** record's RDH. That is safe
for readers (the `ValueLength` property masks the raw field to `ObjectIdSize`), but record disposal converts the field to inline
(`LogField.ClearObjectIdAndConvertToInline`), after which the property returns the raw field — so that method sets the length
explicitly to `ObjectIdSize` rather than trusting the stamped value.

### 2.2 `KeyLength` (10 bits) — non-inline (overflow) key

| `KeyLength` | Meaning | read |
|---|---|---|
| `< 1023` | headerless overflow key, exact length | read exactly. |
| `== 1023` | **has header** (rare large key); full len from first-buffer header; normal header + padding processing. | `RoundUpToSector(one page)`, then read exactly. |

---

## 3. `ChunkHeader` (8 bytes)

Precedes a piece only when §2 says so. **No forward `nextLength`** — fetch-ahead is RDH-driven, so the writer never
holds/back-patches.

| Piece | `currentLength` (off 0) | second word (off 4) |
|---|---|---|
| **Object chunk** | this chunk's data length + `ContinuationFlag` iff more follows. | unused/reserved. |
| **Overflow** | full overflow length (one chunk, no continuation). | `alignmentPadding` (O_DIRECT). |

- Object: read the 1023-byte headerless prefix, 8-align to the first header, then follow `currentLength` to each next chunk
  (may be < 4 MB, §5); stop when the continuation flag clears (handle zero-length continuation chunks). A sentinel object extends
  read-ahead per-chunk. Each header is 8-byte-aligned and **back-filled** when its buffer fills (see §5).
- Overflow: header in the **first** buffer; read it, learn full length, allocate `OverflowByteArray`, stream the rest.

---

## 4. Has-header signaling / position-word budget

Derived from the RDH — **zero** new position-word bits: value = `ValueLength` bit 10 (`FlushValueHasHeader`); key = `KeyLength==1023`.
`ObjectLogFilePositionInfo.word`: bits 0–59 segment+offset (~1 EB), bit 63 downlevel flag, **bit 62 reserved** (future
DMA-padded short key only), bits 60–61 reserved.

---

## 5. Packing & boundary rules

- **Dense packing, no partial-buffer padding** (avoids extra writes for many small objects). The reader advances to each next
  object chunk by following the header's `currentLength` (may be < 4 MB), not by assuming buffer alignment.
- **Object ChunkHeaders are 8-byte-aligned and appended, never slide-inserted.** `Debug.Assert(ChunkHeader.TotalSize == 8)`. The
  writer serializes the 1023-byte headerless prefix, then pads the object-log position to the next 8-boundary and pokes a
  placeholder header; its `currentLength` (| `ContinuationFlag`) is **back-filled when the buffer fills or the object ends**, while
  still in the unflushed buffer. Because a buffer (4 MB) and segment (1 GB) are 8-multiples, an 8-aligned 8-byte header never
  straddles a buffer/segment boundary. The recorded object extent is the monotonic distance from the object's start position.
- **Overflow headers never straddle a buffer/segment**: within ~128 bytes of the end → advance the start to the next
  buffer/segment; that advanced start is the recorded `ObjectLogPosition`, so the reader lands correctly.
- **Zero-length-chunk edge** (a header landing exactly at `buffer_end − 8`): only the first post-prefix header can hit it (a
  fresh buffer always leaves ≥ one sector for a header plus data). The 8-byte header fills the buffer with no data room, so it is
  **back-filled as a zero-length continuation chunk** (`currentLength = 0 | ContinuationFlag`) and the object data resumes in the
  next buffer. The reader (§6) skips zero-length continuation chunks. Deterministically exercised by
  `ObjectChunkZeroLengthFirstChunkTest`, which packs dense overflow fillers so the object starts at `buffer_end − (1023 + 8)`.

---

## 6. Writer flow

1. Overflow key: `==1023` → header (full len + padding) then bytes; else headerless. Honor §5.
2. Value: overflow → single leading header (`hasHeader`, full len + padding) or headerless (`isExactSize`, ≤ 1023); object
   > 1023 bytes → 1023-byte headerless prefix, then an 8-aligned per-buffer `ChunkHeader` whose `currentLength |
   ContinuationFlag` is **back-filled** when the buffer fills or the object ends (§5); object ≤ 1023 bytes → headerless
   (`isExactSize`).
3. Stamp non-destructively (§7): `ObjectLogPosition` (post boundary-advance) + RDH `KeyLength`/`ValueLength` per §2.

---

## 7. No-copy flush

**Status: enabled, relying on the OnDispose "readable-during-flush" contract.** A plain **ReadOnly, full-page,
sector-aligned** flush writes the live main-log page to the device directly (stamping the length hints + `ObjectLogPosition`
into the live records in place), skipping the sector-aligned `srcBuffer` copy (`ObjectAllocatorImpl.WriteAsync`, gated by
`useLivePage = flushRequestState == ReadOnly && !partial && startPadding == 0`; Snapshot/Recovery/partial/unaligned still
copy). The stamping itself is **non-destructive** to in-memory readers:
- **RDH raw `KeyLength`/`ValueLength`** — the property returns `ObjectIdSize` for non-inline regardless of raw bits, so
  `AllocatedSize`/`GetValueFieldInfo`/the `ObjectLogPosition` slot address are unchanged; the raw bits are the on-disk read hint.
  The one exception is record **disposal**, which converts the field to inline (after which the property returns the raw bits);
  `LogField.ClearObjectIdAndConvertToInline` therefore sets the converted length to `ObjectIdSize` rather than trusting the
  stamped value (see §2.1).
- **`ObjectLogPosition`** — read only by the flush/recovery disk-image paths, never by a normal in-memory read/upsert.
- **objectId slots** — left live and untouched; meaningless on disk; the reader re-allocates.

**Why it is unsafe as a direct swap:** the copy path isolates the asynchronous device write from *post-submission
mutation of the flushed records*. Records in the read-only region (down to `HeadAddress`) are **not content-immutable**:
any `Upsert`/`RMW`/`Delete` that supersedes such a record seals it in place (`RecordInfo.Seal()`), and `Delete`
additionally disposes it (`OnDispose` → `ClearHeapFields` + `ClearOptionals`) — see `InternalDelete` (searches to
`HeadAddress`; `OnDispose` + `Seal` the main-log source) and `ObjectAllocatorImpl.OnDispose`. A ReadOnly flush does not
hold the epoch during the device write, so with no-copy the device can observe these torn / half-cleared bytes mid-write
and the completion marks the page durable even though the superseding tail record (tombstone / new value) may not be
durable — a crash can then recover a cleared/malformed old record. The synchronous `srcBuffer` copy captures a
point-in-time image and the async write reads that isolated buffer, avoiding the hazard.

**Why "invalidate before clear" is not enough.** An attempt to make no-copy safe by making every mutator flip the record
to **Invalid atomically before** clearing its fields (`SealAndInvalidate()` then `OnDispose`, matching the elide path in
`CreateNewRecordUpsert`/RMW, and changing `CreateNewRecordDelete` to do the same) was implemented and **reverted**: it does
not close the race. The async device write reads the live page *over the whole I/O duration*, so it can read the old
**Valid** `RecordInfo` at one instant and the concurrently-cleared body at a later instant — persisting a torn *Valid*
record even though the flip to Invalid happened in between. Invalidate-before-clear only helps an in-memory reader (which
sees one atomic word); it cannot help a byte-by-byte device read of a mutating buffer. (Separately, not only Delete mutates
Valid read-only sources: RMW expiration and object CopyUpdate also clear/dispose the source before sealing.) The copy path
is safe precisely because the device reads a private `srcBuffer` snapshot that no concurrent operation touches.

**The OnDispose contract that makes it safe.** No-copy is correct **iff** a record stays byte-consistent (readable)
throughout a flush of its page. This is a **contract on `OnDispose` implementations**: rather than tearing a record's
flush-critical bytes (`ObjectLogPosition`, the record header/layout) in place while an async device write may be reading
them, an `OnDispose` **copies off** whatever it needs for cleanup (the heap object to dispose, the app's external-resource
handle) and leaves the record's on-disk image intact until the record is evicted (i.e. after the flush is durable). Given
that contract, the async device write always observes a consistent record even if a concurrent `Upsert`/`RMW`/`Delete`
supersedes it (the only other in-place mutation, `Seal()`, is a single atomic `RecordInfo` word write). Output is
byte-identical to the copy path (perf only).

> **Caveat (current impl):** `ObjectAllocatorImpl.OnDispose` today calls `ClearHeapFields`, which for an inline-key object
> record removes `ObjectLogPosition` and rewrites the header/filler **non-atomically**, so it does not yet strictly honor
> the contract. To make the contract hold for every store, that clearing must be made flush-safe (copy off + defer the
> layout change to eviction). The `srcBuffer` copy path is unconditionally safe regardless, because the device reads a
> private snapshot that no concurrent operation touches.

---

## 8. Reader flow

1. Decode RDH extent (§2) → size read-ahead (exact size / page-count×4 KB / one 4 MB block for the sentinel); multi-record
   scans size from successive absolute-position differences. Fill the ring.
2. Overflow: parse the leading header **before allocating**; allocate exact `currentLength`; skip header (+padding);
   read exactly.
3. Object: read the 1023-byte headerless prefix, 8-align to the first `ChunkHeader`, then walk chunks — parse header, feed
   `currentLength` (mask continuation) to the deserializer, follow to the next (skip zero-length continuation chunks), stop
   when continuation clears. Deserializer self-terminates; ≤ 4 KB final-page rounding harmless.
4. `ReadExactly(8)` for headers (may span boundaries); deserializer never sees headers/padding/next-piece/over-read.

**Read-accounting:** absolute endpoints (`componentCursor`, extendable `requiredLogicalEnd`, `issuedAlignedEnd`, hard
bound = successor position / object-log tail), not a single additive delta.

---

## 9. Recovery & positions

- **Snapshot-region verbatim copy** (`ObjectAllocatorImpl` snapshot-recovery flush): each record's bytes are copied from the
  snapshot object-log to the main object-log. A **headerless** record is sized exactly by the RDH `KeyLength+ValueLength`
  hints. A record with a **leading `ChunkHeader`** or **headered object** (`FlushValueHasHeader` for a ≥-sentinel overflow key,
  or an overflow/object value with the has-header bit) is sized by the **successor object record's snapshot position minus
  this record's** — exactly this record's raw key+value+header(s)+padding extent, copied verbatim (`successor.pos ≥ this.pos +
  extent`, so it never under-copies; trailing over-copy is ignored on read-back, which re-frames from the header). Validated by
  `RecoverSnapshotHeaderedOverflowValue` and the multi-segment `LargeObjectTest`.
  - **Last object record on a page:** no successor to bound it, and its size hint under-counts a sentinel-sized value, so the copy
    **follows the record's `ChunkHeader` framing to the exact on-disk extent** (`CopyRecoveredObjectBytesFollowingFraming` →
    `ObjectLogReader.CopyRecordObjectsFollowingFraming`): the framing walk decodes each chunk header, self-extends the snapshot
    read-ahead as chunks are consumed, and tees every raw byte into the main object-log — so a value spanning multiple 4 MB
    read-ahead buffers is copied whole rather than truncated. In copy-to-end mode `ReadObjectData` self-terminates after the final
    (non-continuing) data chunk (a data chunk with the continuation flag clear is only ever back-filled at serialize completion, so
    it is provably the last), rather than relying on the deserializer as the normal read path does. Validated by the 5 MB point of
    `RecoverObjectValueLowMemBoundaries` / `RecoverOverflowValueLowMemBoundaries`.
- **Flag bits:** `RepointObjectLogPosition` and `SetObjectLogPositionAndLengthHints` preserve the reserved position-word flag
  bits; `RepointObjectLogPosition` additionally preserves **bit 63** (a verbatim-copied downlevel record stays downlevel).
  `SetRecoveredObjectLogRecordStartPosition` intentionally **clears bit 63** — it converts the record to v2.2.
- **v2.1 reposition guard:** because that conversion does not insert a `ChunkHeader`, if a downlevel source would convert to
  a headered overflow key (≥ 1023) or a headered overflow/object value (> 1023) it **throws (fail-fast)** rather than silently
  corrupt (see §10). No-op for v2.2 sources (bit 63 never set).
- **Validate:** overflow length within limits; cumulative object ≤ `IHeapObject.MaxSerializedObjectSize`.

---

## 10. Versioning & downlevel

- **Checkpoint versions (resolved):** the current chunk-framing format is **v2.2 = checkpoint version 8**
  (`HybridLogRecoveryInfo.CheckpointVersion`); the downlevel split/objectId-slot encoding is **v2.1 = version 7**
  (`MinRecoverableCheckpointVersion`). This build recovers v7 and v8. A v2.1 binary (which only accepts v7) rejects a v8
  checkpoint, so the checkpoint version is the file-level v2.1-vs-v2.2 discriminator — pre-flag binaries **reject** rather
  than misread.
- **Per-record discriminator:** `ObjectLogPosition` **bit 63** (`ReuseObjectIdForSize`). Set = v2.1 (split length = RDH low
  bits + objectId-slot high 32 bits; dense object-log stream, no `ChunkHeader`s), read via the `*_v21` decoders
  (`LogRecord_v21.cs`; previous `_v20` renamed to `_v21`). Clear = the current hint format. New records are only ever
  written in the current format.
- **v2.1 → v2.2 recovery:** v2.1 object-log bytes are byte-identical to the current **headerless** (small overflow) and
  **chunked-object** (dense, no per-chunk header) encodings, so those records convert by simply repointing. The paths:
  - *FoldOver* recovery does **not** re-flush the object log (only Snapshot-region pages are flushed on eviction); records
    are read via `*_v21` and upgrade lazily on the next normal eviction.
  - *Snapshot* fuzzy-region **verbatim copy** (`RepointObjectLogPosition`) **preserves** bit 63, so those records stay v2.1.
  - *Snapshot* stable-boundary **reposition** (`SetRecoveredObjectLogRecordStartPosition`) is the only path that clears the
    flag and converts to v2.2. A **large overflow key (≥ 1023) / value (> 1023)** would there need a leading `ChunkHeader`
    that the dense v2.1 bytes lack; inserting it on recovery (which grows the object log and shifts following positions) is
    **not yet implemented**, so that narrow case **throws (fail-fast)** rather than silently corrupting. `SetDeserializedValueObject`
    preserves bit 63 across the deserialized-length store so an object-value source is still detectable there.
- **Chunk length cap (forward-looking):** any future `ChunkHeader`-based split must cap each chunk at **`1<<30`** bytes —
  `ChunkHeader.currentLength` is a 32-bit int whose top bit is `ContinuationFlag`. Objects are headerless/dense today (they
  never hit this); it bites only once objects become headered.
- **Untestable today:** new records are never written in v2.1, so full v2.1→v2.2 header-insertion conversion cannot be
  validated without a v2.1 checkpoint fixture/writer — do not ship it unvalidated.

---

## 11. Migration / Replication (network) context

Same pieces, receiver fed a buffer at a time → simpler framing (sentinel + in-stream length prefix), RDH usage defined
independently of the Flush encoding; see [Migration / Replication record layout](./migration-replication-record-layout.md).
Inline fields stay pure length in both.
