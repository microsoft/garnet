# Migration / Replication record layout

This document describes the on-the-wire byte layout of the records that cluster **key migration** and **replication**
ship in the send buffer, in both the **non-chunked** (whole record in one buffer) and **chunked** (record too large for
one buffer) forms.

> **Keep this document in sync with the code.** If any of the following change, update the layouts and diagrams below:
> - `libs/storage/Tsavorite/cs/src/core/Allocator/DiskLogRecord.cs` — `Serialize` / `Deserialize`, the chunked helpers
>   (`GetChunkedRecordInlineSize`, `CompleteDeserializeChunkedRecord`), and `SerializeInlinePortionForMigration`.
> - `libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs` — the record data header (indicator word).
> - `libs/client/ClientSession/GarnetClientSessionIncremental.cs` — `MigrationRecordSpanType`, `TryWriteRecordSpan`,
>   `TryWriteChunkedRecordSpan`, and the send-buffer framing.
> - `libs/cluster/Session/ChunkedRecordReassembler.cs` — the receive-side reassembler.
> - `libs/cluster/Session/RespClusterMigrateCommands.cs` / `RespClusterReplicationCommands.cs` — the receive paths
>   (`NetworkClusterMigrate` / `NetworkClusterSync`, `CompleteChunkedRecordReassembly`).
> - `libs/server/MigrationChunkWriterAccumulator.cs` / `libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs` — migration's
>   in-epoch capture (`HandleMigrate` fills the accumulator + inline portion).
> - `libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs` — migration send (per-key `Read_UnifiedStore`, assemble the
>   captured pieces, `WriteOrSend*RecordAsync`).
> - `libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSnapshotIterator.cs` — replication send (`StoreSnapshotIterator.Reader` / `WriteRecord` / fan-out).
> - `libs/storage/Tsavorite/cs/src/core/Allocator/ObjectSerialization/ChunkedRecordConstants.cs` — the continuation flag.

---

## 1. Overview

Both migration and replication ship store records over the network as serialized `DiskLogRecord` images in a **send
buffer** that is flushed to the target, which deserializes each record back into the store. The two mechanisms differ in
how records are produced and applied (see [§5](#5-send-and-receive-flows)): **migration** retrieves the specific keys
being moved (a read per key) and the target applies them conditionally as commands; **replication** streams the whole
store through a snapshot iterator and the replica inserts every record.

- A record whose serialized size fits within `NetworkBufferSettings.maxSendBufferContentSize` is written whole, as a
  single **non-chunked** `LogRecord` entry ([§3](#3-non-chunked-record-logrecord-tag-1)).
- A larger record is split into a sequence of **chunk** entries (`ChunkedLogRecord`) that the receiver reassembles
  before deserializing ([§4](#4-chunked-record-chunkedlogrecord-tag-5)). Because the chunks are reassembled as a `ReadOnlySequence<byte>` (not one contiguous
  array), an object value may exceed 2 GB.

> This is a serialized `DiskLogRecord` **record image**, distinct from the AOF's operation image. For the AOF format
> see the companion doc, [AOF / TsavoriteLog record layout](./aof-record-layout.md). The two paths share only the chunk
> **continuation flag** (`ChunkedRecordConstants.ContinuationFlag`).

---

## 2. Send-buffer framing

The migrate/replica iteration buffer is a single RESP bulk string whose payload is a count-prefixed run of entries
(`GarnetClientSession.SendAndResetIterationBuffer`):

```
+------------------+----------------------+-----------------------------+------+
| $<len>\r\n       | recordCount (i32)    | entry, entry, entry, ...    | \r\n |
+------------------+----------------------+-----------------------------+------+
```

Each **entry** begins with a one-byte `MigrationRecordSpanType` tag:

| Tag | Value | Payload |
|---|---|---|
| `Invalid` | 0 | — |
| `LogRecord` | 1 | A whole serialized `DiskLogRecord` ([§3](#3-non-chunked-record-logrecord-tag-1)) |
| `VectorSetElement` | 2 | Bespoke vector-set element encoding (out of scope here) |
| `VectorSetIndex` | 3 | Bespoke vector-set index encoding (out of scope here) |
| `SerializedRangeIndexStream` | 4 | Chunked RangeIndex stream (out of scope here) |
| `ChunkedLogRecord` | 5 | One chunk of a serialized `DiskLogRecord` too large for one buffer ([§4](#4-chunked-record-chunkedlogrecord-tag-5)) |

This doc covers `LogRecord` (1) and `ChunkedLogRecord` (5). Vector-set and RangeIndex encodings are handled separately.

---

## 3. Non-chunked record: `LogRecord` (tag 1)

`GarnetClientSession.TryWriteRecordSpan` writes the whole record:

```
+--------+---------------------------------------------+
| type=1 |  serialized DiskLogRecord (§3.1)            |
+--------+---------------------------------------------+
```

### 3.1 Serialized `DiskLogRecord` layout

The whole record is one contiguous image: an **inline portion** followed by any **overflow key**, then any **overflow
value** or **object value** (`DiskLogRecord.Serialize` / `Deserialize`). The overflow key/value lengths are carried in the
inline portion's **objectId slots** — the 4-byte slot at the key/value position is meaningless on the wire (the receiver
assigns its own objectIds), so it holds the component's full `int` length instead (see `LogRecord.SetWireOutOfLineLengths`).
The RDH `KeyLength`/`ValueLength` raw fields are **left exactly as the source record had them** (`ObjectIdSize`) — the wire
format never writes them, reserving them for the future hybrid-value inline length. `Deserialize` reads the slots to size
and locate the overflow bytes, and computes the overflow offset as `RoundUp(ActualSize)`. An **object value** is the tail
of the image; its exact length also rides in the value objectId slot (continuation flag clear when it fits an `int`, else
first-chunk length with the flag set), but is derived on read as `recordSpan.Length - objectValueStart`, so a small object
deserializes directly from the buffer. A record is sent whole only when it fits one send buffer; a larger record (including
a large object) takes the chunked path ([§4](#4-chunked-record-chunkedlogrecord-tag-5)).

```
+=====================================================================+
| INLINE PORTION                                                      |
+----------------+----------------+----------------+------------------+
| RecordInfo     | RecordDataHdr  | ExtendedNamesp.| inline Key       |
| (8 B)          | (8 B, §3.2)    | (0..N B)       | (0..N B)*        |
+----------------+----------------+----------------+------------------+
| inline Value   | optionals: ETag (8 B if present), Expiration       |
| (0..N B)*      | (8 B if present), ObjectLogPosition, ...           |
+----------------+----------------------------------------------------+
| OVERFLOW KEY data   (present only if the key is Overflow)           |
+---------------------------------------------------------------------+
| OVERFLOW VALUE data (if Overflow) -- or -- OBJECT VALUE bytes       |
+=====================================================================+

* For an overflow key/value or object value, the inline objectId slot carries the component's length on the wire
  (the receiver overwrites it with a real objectId on read); the actual bytes live in the overflow-key /
  overflow-value / object tail. The RDH KeyLength/ValueLength raw fields are never rewritten on the wire.
```

`FixedHeaderSize = RecordInfo.Size (8) + RecordDataHeader.Size (8) = 16`. The inline size is
`FixedHeaderSize + ExtendedNamespaceLength + KeyLength + ValueLength + OptionalSize`, rounded up to
`kRecordAlignment`.

### 3.2 `RecordDataHeader` (indicator word) — 8 B

A single 8-byte word (atomically published) that fully defines the record's layout (`RecordDataHeader.cs`):

```
bit(s)   field
 0       KeyIsInline
 1       ValueIsInline
 2       ValueIsObject
 3       HasExpiration
 4       HasETag
 5       (reserved)
 6..13   FillerWords          (count of 8-byte filler words after alignment padding)
 14..23  KeyLength            (raw inline key length; = objectId size for an overflow key — never rewritten on the wire)
 24..47  ValueLength          (raw inline value length; = objectId size for overflow/object — never rewritten on the wire)
 48..55  RecordType           (byte, caller-interpreted)
 56..63  Namespace            (byte; encodes whether extra namespace bytes precede the key data)
```

The receiver reads these bits to know whether the key/value are inline, overflow, or (for the value) an object, and
therefore how to interpret the tail.

---

## 4. Chunked record: `ChunkedLogRecord` (tag 5)

A record too large for one send buffer — a large inline record, an overflow key/value, or a large object value — is streamed
as a sequence of chunk entries. Replication drives this through `DiskLogRecord.Serialize` (a reused network-mode
`ChunkedObjectSerializer`); migration assembles the captured pieces and sends them via
`MigrateSession.WriteOrSendAccumulatedRecordAsync`. Either way each chunk is framed by
`GarnetClientSession.TryWriteChunkedRecordSpan`:

```
each chunk entry:
+--------+------------------------------+---------------------+
| type=5 | i32 chunkLength | cont-flag  |  chunk bytes        |
+--------+------------------------------+---------------------+
          bit 31 = ChunkedRecordConstants.ContinuationFlag
                   (1 = more chunks of this record follow; clear on the last chunk)
          bits 30..0 = this chunk's data length
```

The chunk boundaries are arbitrary send-buffer cut points (not record-structure boundaries), and the chunks of one
record may span multiple send buffers / commands. The **concatenation of all chunk bytes** of a record is the serialized
stream:

```
+=====================+==============+==================================================+
| INLINE PORTION      | overflow key | overflow value -- or -- object value (streamed)  |
| (RDH, §3.2)         | (keyLen B)   | (valueLen B)              (no length prefix)      |
+=====================+==============+==================================================+
   always present        only if key is Overflow      only if value is Overflow / Object
```

- The inline portion is the same [§3.1](#31-serialized-disklogrecord-layout) inline bytes (RDH indicator bits tell the
  receiver which out-of-line components follow, and the objectId slots carry the overflow key/value lengths — see §3.1).
- There are **no per-component length prefixes**: each overflow key/value length rides in the inline portion's objectId
  slot, so the receiver reads the slot to allocate the overflow buffer up front and populate it directly. Overflow
  key/value are `byte[]`-bounded (≤ 2 GB), so an `int` slot suffices.
- An **object value** has **no length prefix** — it is streamed and its length is derived on receive (it may exceed
  2 GB, the max length of one `byte[]`); its first-chunk length (with the continuation flag) also rides in the value
  objectId slot for forward compatibility.

### 4.1 Object value length

An object value's serialized length is not known when the inline portion is emitted, so its value objectId slot is first
written with the streaming sentinel (`ContinuationFlag`) and patched once the object is serialized: the sender writes the
exact length when it fits an `int` (continuation clear) or the first-chunk length with the continuation flag set. The
receiver derives the total length as the **sum of the accumulated object-value chunk lengths**
(`ChunkedRecordReassembler.objectValueLength`); the slot value is forward-compatible metadata (not required to reassemble).

### 4.2 Reassembly and &gt;2 GB support

`ChunkedRecordReassembler` routes the incoming chunk bytes by component using a small state machine. The inline size comes
from `DiskLogRecord.GetChunkedRecordInlineSize`; the component kinds are read on demand from the record's data header (the
`RecordDataHeader` at the start of the accumulated inline buffer):

- **Inline portion:** accumulated contiguously into `ChunkedRecordReassembler.inlineBuffer`; once the fixed header
  (`FixedHeaderSize` = 16 B) is present, the layout gives the inline size and which components follow.
- **Overflow key / value:** the length is read from the inline portion's objectId slot, a single `OverflowByteArray` is
  allocated up front, and each incoming chunk is copied **straight into it** (no intermediate list) — populating
  `ChunkedRecordReassembler.keyOverflow` / `valueOverflow`.
- **Object value:** accumulated as a chunk list (`ChunkedRecordReassembler.objectValueChunks`, `List<byte[]>`) and
  exposed as a `ReadOnlySequence<byte>` for streaming deserialize — so it may exceed 2 GB.
- **Fully-inline record:** the whole record is the contiguous inline buffer.

On completion:

- **Fully-inline record** → `DiskLogRecord.Deserialize(inlineBuffer)`.
- **Any out-of-line component** → deserialize the object value (if any) from its sequence, then
  `DiskLogRecord.CompleteDeserializeChunkedRecord(header, keyOverflow, valueOverflow, valueObject)`, which **assigns** the
  pre-populated overflow key/value directly (no re-allocation or copy). The RDH length fields are left untouched — the
  out-of-line lengths rode in the wire objectId slots, which the receiver already consumed.

## 5. Send and receive flows

Both mechanisms use the record layout above (a whole `LogRecord`, or a stream of `ChunkedLogRecord` chunks), but they
**produce** and **apply** records differently: **migration** retrieves the individual keys being moved and applies them
conditionally as commands; **replication** streams the whole store through a snapshot iterator and inserts every record.

### 5.1 Migration

The source retrieves each key being migrated and captures its pieces **in-epoch** (`HandleMigrate` → a
`MigrationChunkWriterAccumulator`): the inline portion is copied into `output.SpanByteAndMemory` and its overflow key/value
lengths written into the objectId slots (`SerializeInlinePortion` → `SetWireOutOfLineLengths`), while the overflow key (a
**shallow reference** — store keys are immutable), the overflow value (a **deep copy** — the store value may be mutated
once the epoch is released), or an object value (serialized into a **chunk list**, so it may exceed 2 GB) go into the
accumulator. Out of epoch the caller patches the inline portion's value objectId slot with the object length (if any) and
assembles `[inline][overflow key][overflow value | object chunks]`, sending it under a `CLUSTER MIGRATE` command — whole
(`LogRecord`) if the record fits a send buffer, else as `ChunkedLogRecord` chunks. The receiver reads the overflow lengths
from the objectId slots and derives the object length from the record span (whole records) or the accumulated chunks
(chunked), so a small object is sent whole and deserialized directly from the buffer, while a large object chunks. This
in-epoch capture / out-of-epoch send is required because migration sends **asynchronously** and the store epoch must never
be held across an `await` (unlike replication, which sends synchronously via `BlockingWait` and can stream a record to the
network in-epoch). The target applies each record only if its slot is importing and the key may be written (`replace` set,
or the key is absent).

```mermaid
flowchart TB
  subgraph MSend["Migration send (source)"]
    direction LR
    MA[MIGRATE keys / migrating-slot keys] --> MB[per key: Read_UnifiedStore<br/>capture pieces in-epoch:<br/>inline + accumulator]
    MB --> MC{"larger than send buffer?"}
    MC -->|no| MD[assemble whole<br/>TryWriteRecordSpan type=1 LogRecord]
    MC -->|yes| ME[send pieces as chunks<br/>TryWriteChunkedRecordSpan type=5]
    MD --> MF[(CLUSTER MIGRATE)]
    ME --> MF
  end
  subgraph MRecv["Migration receive (target)"]
    direction LR
    MG[NetworkClusterMigrate:<br/>per record, deserialize] --> MH{"slot importing and replace-or-absent?"}
    MH -->|yes| MI[basicGarnetApi.SET]
    MH -->|no| MJ[skip]
  end
  MF --> MG
```

Migration also carries `VectorSetElement` / `VectorSetIndex` / `SerializedRangeIndexStream` record kinds (out of scope
here); this doc covers only the `LogRecord` / `ChunkedLogRecord` kinds.

### 5.2 Replication

Diskless replica sync takes a streaming-snapshot checkpoint whose `StoreSnapshotIterator.Reader` pushes every store
record to `WriteRecord`, which fans each record out to all attached replica sessions in lockstep under `CLUSTER SYNC`.
The replica inserts every record unconditionally. A record that fits one send buffer is sent whole (`LogRecord`): a
fully-inline record directly, and a **prefix-free object record** (inline key + object value) via the chunker's `Consume`
when it drains in one piece (the chunker's ring buffers up to one send buffer, so an object that completes in the first
drain fit) — so small objects deserialize directly on the replica. Anything larger streams as `ChunkedLogRecord` chunks.

```mermaid
flowchart TB
  subgraph RSend["Replication send (primary)"]
    direction LR
    RA[streaming-snapshot checkpoint] --> RB[StoreSnapshotIterator.Reader<br/>pushes each record]
    RB --> RC{"inline and fits buffer?"}
    RC -->|yes| RD[FanOutRecordSpan<br/>type=1 LogRecord]
    RC -->|no| RE[DiskLogRecord.Serialize<br/>drain through chunker]
    RE --> RG2{"fit one drain and prefix-free object?"}
    RG2 -->|yes| RD
    RG2 -->|no| REchunk[FanOutChunk type=5]
    RD --> RF[(CLUSTER SYNC to all replicas)]
    REchunk --> RF
  end
  subgraph RRecv["Replication receive (replica)"]
    direction LR
    RG[NetworkClusterSync:<br/>per record, deserialize] --> RH[basicGarnetApi.SET<br/>every record]
  end
  RF --> RG
```

### 5.3 Chunked transfer over the wire (shared)

The chunk framing and reassembly are identical for both mechanisms (only the final apply differs — conditional SET for
migration, unconditional SET for replication). A record too large for one send buffer is streamed as `ChunkedLogRecord`
chunks and routed by component in `ChunkedRecordReassembler` before deserialize:

```mermaid
sequenceDiagram
    participant P as Sender
    participant N as Send buffer
    participant T as Receiver
    participant A as Reassembler
    P->>N: chunk 0, type 5, length with continuation flag, bytes
    P->>N: chunk 1, type 5, length with continuation flag, bytes
    P->>N: chunk M, type 5, length, bytes, continuation flag clear
    N->>T: read entries
    T->>A: Append chunk 0, moreFollow true
    A->>A: route by component: inline, then overflow key/value (into OverflowByteArray), then object chunks
    T->>A: Append chunk M, moreFollow false, complete
    T->>T: CompleteChunkedRecordReassembly: deserialize object value if any
    T->>T: DiskLogRecord Deserialize (fully-inline) or CompleteDeserializeChunkedRecord (assign overflow/object)
    T->>T: SET record into store
```

---

## 6. Call sequence (code paths)

Indentation = call depth; a multi-step flow may sit on one line (`a → b → c`), and *italic* sub-items are terse notes.
Migration and replication differ on write; they share the receive path. The record's components — **inline portion**,
**overflow key**, **overflow value**, **object value** — are called out throughout.

**Migration write** — retrieve each key from the store and send it (in-epoch capture → out-of-epoch send):

- `MigrateSession.TransmitKeysAsync()`
  - *`MigrateOperation.cs`; iterate the keys being migrated*
  - `WriteOrSendRecordAsync(key)`
    - *`MigrateSessionCommonUtils.cs`*
    - `BasicGarnetApi.Read_UnifiedStore(key)` → `HandleMigrate(srcLogRecord)`
      - *`UnifiedStore/ReadMethods.cs`; capture the record's pieces in-epoch (a migrating key is not locked, so its value may change concurrently)*
      - `DiskLogRecord.SerializeInlinePortionForMigration()`
        - *inline portion → `output.SpanByteAndMemory`; copy + write the overflow key/value lengths into the objectId slots (RDH lengths left untouched)*
      - `acc.SetKeyOverflow(KeyOverflow)`
        - *overflow key → shallow ref (store keys are immutable); populates `MigrationChunkWriterAccumulator.keyOverflow`*
      - `acc.SetValueOverflowDeepCopy(ValueOverflow)`
        - *overflow value → deep copy (the value may change once the epoch is released); populates `MigrationChunkWriterAccumulator.valueOverflow`*
      - `acc.SerializeObjectValue(ValueObject)` → `ChunkedObjectSerializer` → `acc.Consume(first, second)`
        - *object value → serialized chunks; populates `MigrationChunkWriterAccumulator.objectValueChunks` (`List<byte[]>`, supports values over 2 GB). The serializer is reused across records, so its ring is allocated once per migration, not per object*
    - `WriteOrSendAccumulatedRecordAsync(inline, acc)`
      - *out of epoch: assemble the captured pieces and send*
      - `gcs.TryWriteRecordSpan(record, LogRecord)`
        - *fits one buffer → whole record (type 1); assemble `[inline][overflow key][overflow value | object bytes]` contiguously (no length prefixes; overflow/object lengths carried in the inline objectId slots, an object length also derived on read from the record span)*
      - `gcs.TryWriteChunkedRecordSpan(chunk, moreFollow)`
        - *too large for one buffer → chunks (type 5): stream `[inline][overflow key][overflow value | object chunks]` (overflow lengths in the inline objectId slots, no per-component prefixes), continuation until the last byte → **send buffer** → `CLUSTER MIGRATE`*

**Replication write** — stream a store snapshot to the replica(s) (in-epoch, synchronous send):

- `ReplicationSyncManager` → `StoreSnapshotIterator.Reader(srcLogRecord)` → `SnapshotIteratorManager.WriteRecord(srcLogRecord)`
  - *`ReplicationSnapshotIterator.cs`; a streaming-snapshot checkpoint pushes each record*
  - `DiskLogRecord.DirectCopyInlinePortionOfRecord()` → `FanOutRecordSpan(LogRecord)`
    - *inline record → whole (type 1), fanned to all replicas in lockstep*
  - `DiskLogRecord.Serialize(srcLogRecord, serializer, chunker)` → `SnapshotIteratorManager.Consume(...)`
    - *`DiskLogRecord.cs`; non-inline → stream through the chunker (epoch held; replication flushes synchronously via `BlockingWait`, so it may stream in-epoch)*
    - *writes the inline portion (overflow key/value lengths in its objectId slots), then the **overflow key** and **overflow value** — or streams the **object value** — into the chunk stream (no per-component length prefixes); the carrier is the streamed netbuffer chunk (no accumulator)*
    - `Consume`: `first` drain is `isComplete` and prefix-free object → `FanOutRecordSpan(LogRecord)`
      - *small object (inline key + object value) that fit one drain → whole record (type 1), deserialized directly on the replica*
    - `Consume`: otherwise → `FanOutChunk(chunk, moreFollow)`
      - *too large for one buffer → chunks (type 5), continuation until the last byte → **send buffer** → `CLUSTER SYNC`*

**Receive** (migration + replication) — read records from the buffer and write them to the store:

- `NetworkClusterMigrate()` / `NetworkClusterSync()`
  - *`RespClusterMigrateCommands.cs` / `RespClusterReplicationCommands.cs`; per record in the payload*
  - `type == LogRecord` → `DiskLogRecord.Deserialize(recordSpan)`
    - *whole record: inline + any overflow key/value (lengths from the inline objectId slots) or a small object value (length derived from the record span), restored/deserialized directly from the contiguous span*
  - `type == ChunkedLogRecord` → `ChunkedRecordReassembler.Append(chunk, moreFollow)`
    - *route bytes by component (may span commands): inline → `inlineBuffer`; overflow key/value → a single `OverflowByteArray` (`keyOverflow`/`valueOverflow`) populated directly from the chunks; object value → `objectValueChunks` (`List<byte[]>`)*
    - `CompleteChunkedRecordReassembly(headerPtr)`
      - *component kinds read from the record header in `inlineBuffer`; a fully-inline record → `DiskLogRecord.Deserialize(inlineBuffer)`*
      - `GarnetObjectSerializer.Deserialize(ObjectValueSequence())` → `DiskLogRecord.CompleteDeserializeChunkedRecord(header, keyOverflow, valueOverflow, valueObject)`
        - *assign the pre-populated overflow key/value directly + the deserialized (over-2 GB-capable) object value; the RDH lengths are left untouched (the wire objectId-slot lengths were already consumed)*
  - `basicGarnetApi.SET(diskLogRecord)`
    - *→ **store** (migrate: only if the slot is importing and `replace`-or-absent; replication: unconditional)*
