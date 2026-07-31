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

### 2.1 `ValueLength` (24 bits) — non-inline value

Decoded by `RecordDataHeader.DecodeFlushValueExtent` (branches on `ValueIsObject`); encoded by `SetObjectLogLengthHints`.

**Object value:**

| `C` (bit 23) | Meaning | low bits |
|---|---|---|
| 1 | **Chunked object** (serialized length ≥ one buffer). Dense stream, **no** per-chunk framing; the count is the read-ahead extent and the deserializer self-terminates. | `[11:0]` full-buffer count (→16 GB); `[21:12]` final-buffer 4 KB-page count. |
| 0 | **Headerless object** (serialized length < one buffer). | `[21:0]` **exact** serialized length (< 4 MB). |

**Overflow value** (length known up front, full 24-bit exact range):

| `ValueLength` | Meaning |
|---|---|
| `< sentinel` (`< 2^24-1`) | exact byte length. |
| `== sentinel` (`2^24-1`) | length ≥ the field maximum; full length carried in a leading `ChunkHeader` (symmetric with a ≥-sentinel overflow **key**). Reader reads the header and extends the read-ahead (`ReadOverflowHeaderLengthAndExtend`). |

> Bit 22 (`kFlushOverflowHeaderBit` / `EncodeFlushOverflowHeader`) is **reserved** for a future precise first-read hint for a
> headered overflow value; it is not written or read today (the sentinel path reads one buffer up front, then extends).
> Objects ≥ 16 GB (buffer count > 4095) throw; per-buffer continuation headers for that saturation case are not implemented.
> Size-tracker converts count→bytes; reconcile against real heap size after deserialize.

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

- Object: follow `currentLength` to the next chunk (may be < 4 MB, §5); stop when continuation clears; RDH count is the
  read-ahead size (on saturation > 4 K buffers, read full buffers until continuation clears).
- Overflow: header in the **first** buffer; read it, learn full length, allocate `OverflowByteArray`, stream the rest.

---

## 4. Has-header signaling / position-word budget

Derived from the RDH — **zero** new position-word bits now: value = `ValueLength` bit 23 or 22; key = `KeyLength==1023`.
`ObjectLogFilePositionInfo.word`: bits 0–59 segment+offset (~1 EB), bit 63 downlevel flag, **bit 62 reserved** (future
DMA-padded short key only), bits 60–61 reserved.

---

## 5. Packing & boundary rules

- **Dense packing, no partial-buffer padding** (avoids extra writes for many small objects). Reader advances to each
  next chunk by following the header's `currentLength` (may be < 4 MB), not by assuming buffer alignment.
- **Headers never straddle a buffer/segment boundary**: within ~128 bytes of the end → advance the start to the next
  buffer/segment; that advanced start is the recorded `ObjectLogPosition`, so the reader lands correctly.

---

## 6. Writer flow

1. Overflow key: `==1023` → header (full len + padding) then bytes; else headerless. Honor §5.
2. Value: overflow → single header (`01`) or headerless (`00`); object → per-buffer header with continuation set when a
   buffer flushes while incomplete (known immediately — **no back-patch**), clear on last; single-buffer object (<
   `MaxCopySpanLen`) → headerless (`00`).
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

## 9. Recovery & positions

---

## 8. Reader flow

1. Decode RDH extent (§2) → size read-ahead (exact / first-buffer-hint+header / buffer+page count); multi-record scans
   size from successive absolute-position differences. Fill the ring.
2. Overflow: parse the leading header **before allocating**; allocate exact `currentLength`; skip header (+padding);
   read exactly.
3. Object: walk chunks — parse header, feed `currentLength` (mask continuation) to the deserializer, follow to next,
   stop when continuation clears. Deserializer self-terminates; ≤ 4 KB final-page rounding harmless.
4. `ReadExactly(8)` for headers (may span boundaries); deserializer never sees headers/padding/next-piece/over-read.

**Read-accounting:** absolute endpoints (`componentCursor`, extendable `requiredLogicalEnd`, `issuedAlignedEnd`, hard
bound = successor position / object-log tail), not a single additive delta.

---

## 9. Recovery & positions

- **Snapshot-region verbatim copy** (`ObjectAllocatorImpl` snapshot-recovery flush): each record's bytes are copied from the
  snapshot object-log to the main object-log sized by its RDH `KeyLength+ValueLength` hints. That equals the true on-disk
  extent for a **headerless** record, and safely **over-copies** a **chunked object** (the reader repositions per record via
  `OnBeginRecord` and the deserializer self-terminates, so trailing bytes are ignored on read-back — validated by the 40 MB
  `MultiListObjectTest`). A record with a **leading `ChunkHeader`** (≥-sentinel overflow key or overflow value) would be
  **under-copied** and truncated, so that case currently **throws** (fail-fast) pending the exact-extent fix below.
  - **TODO:** size the copy by the **raw on-disk extent** (successor object-log position difference, bounded by the snapshot
    tail for the last record on a page) so headered records copy correctly; then remove the guard.
- **Preserve all flag bits** (`0xF << 60`) in `RepointObjectLogPosition`, `SetObjectLogPositionAndLengthHints`,
  `SetRecoveredObjectLogRecordStartPosition`.
- **Validate:** overflow length within limits; cumulative object ≤ `IHeapObject.MaxSerializedObjectSize`.

---

## 10. Versioning & downlevel

- New format written going forward; **v2.1** headerless hint format stays **readable**, never written, via `*_v21`
  decoders (`LogRecord_v21.cs`; previous `_v20` renamed to `_v21`), selected by the position bit-63 flag.
- **Open (confirm):** exact v2.1-vs-new discriminator (checkpoint/file version bump vs per-record bit-63) and the
  version mapping of the renamed decoders. Pre-flag downlevel binaries must **reject**, not misread, the new format.

---

## 11. Migration / Replication (network) context

Same pieces, receiver fed a buffer at a time → simpler framing (sentinel + in-stream length prefix), RDH usage defined
independently of the Flush encoding; see [Migration / Replication record layout](./migration-replication-record-layout.md).
Inline fields stay pure length in both.
