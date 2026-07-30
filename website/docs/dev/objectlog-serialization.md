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
4. **No-copy flush** — the main-log page is written to the device directly from live memory, so flush may make only
   **non-destructive** edits to the live record (§7).
5. **Maximal object-log address space** — spend as few `ObjectLogPosition` flag bits as possible.

Mechanism: encode a record's out-of-line **extent in the RDH length fields** (already in the reader's hand) → drives
fetch-ahead + precise reads with no forward-length look-ahead and no destructive edit.

Constants: `BufferSize = 4 MB` (bits=22); `MaxCopySpanLen = 128 KB`; key sentinel `= 1023`; `ChunkHeader.TotalSize = 8`.

---

## 2. RDH length-field encodings (Flush context)

Inline fields = **just the byte length**. The encodings below apply only to **non-inline** fields, Flush context.

### 2.1 `ValueLength` (24 bits) — non-inline value

| `C` (bit 23) | `H` (bit 22) | Meaning | low 22 bits |
|---|---|---|---|
| 1 | — | **Chunked object** (multi-buffer; per-buffer continuation headers). | `[11:0]` full-buffer count (→16 GB); `[21:12]` final-buffer 4 KB-page count. |
| 0 | 1 | **Overflow with one header** (len ≥ sentinel and/or DMA padding). One chunk, no continuation; full len in header. | first-buffer read hint (≤ 4 MB). |
| 0 | 0 | **Headerless** small object/overflow (< `MaxCopySpanLen`, copied, no DMA). | **exact** byte length (< 4 MB). |

Read hint/exact length capped at 22 bits (4 MB, the buffer single-read limit). Size-tracker converts count→bytes
(expands budget to ~16 GB); reconcile against real heap size after deserialize.

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

Only **non-destructive** edits to the live record:
- **RDH raw `KeyLength`/`ValueLength`** — property returns `ObjectIdSize` for non-inline regardless of raw bits; record
  immutable in read-only region during flush; raw bits captured on disk.
- **`ObjectLogPosition`** — in-memory-unused; safe to stamp.
- **objectId slots** — left live; meaningless on disk; reader re-allocates.

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

- **Verbatim copy** by **raw on-disk extent** (successor-position diff, bounded by checkpoint tail), not
  `keyHint+valueHint`; copy headers/padding intact; advance dest by bytes actually copied.
- **Preserve all flag bits** (`0xF << 60`) in `RepointObjectLogPosition`, `SetObjectLogPositionAndLengthHints`,
  `SetRecoveredObjectLogRecordStartPosition`.
- **Validate:** overflow length within limits; chunk `currentLength` nonzero when continuation set and ≤ capacity;
  cumulative object ≤ `IHeapObject.MaxSerializedObjectSize`.

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
