# RESP-Style Remote API for Bf-Tree as a RangeIndex Data Type

## Summary

Proposal for a Redis-compatible RESP protocol API that exposes Bf-Tree as a `RangeIndex` data type, analogous to how Redis exposes SortedSets via `Z*` commands. The server hosts a key-value cache where keys are RangeIndex names (e.g., `r1`, `r2`) and values are `BfTree` instances.

All commands follow the `RI.*` prefix convention (short for **R**ange**I**ndex).

---

### 1. Lifecycle / Management Commands

| Command | Syntax | Description | Maps to |
|---|---|---|---|
| **RI.CREATE** | `RI.CREATE key [options...]` | Create a new RangeIndex. Options allow tuning the underlying BfTree config | `BfTree::new()` / `BfTree::with_config()` |
| **RI.DROP** | `RI.DROP key` | Destroy a RangeIndex and free its resources | `drop(BfTree)` |
| **RI.EXISTS** | `RI.EXISTS key` | Check if a RangeIndex exists. Returns `1` or `0` | Cache lookup |
| **RI.CONFIG** | `RI.CONFIG key` | Return current config of the RangeIndex as key-value pairs | `BfTree::config()` |
| **RI.METRICS** | `RI.METRICS key` | Return buffer/tree metrics (JSON) | `BfTree::get_buffer_metrics()` / `get_metrics()` |

Snapshot and restore are handled automatically by the cache checkpointing mechanism.

#### `RI.CREATE` options

```
RI.CREATE myindex
    [MEMORY | DISK path | CACHE]
    [CACHESIZE bytes]
    [MINRECORD bytes]
    [MAXRECORD bytes]
    [MAXKEYLEN bytes]
    [PAGESIZE bytes]
    [WAL path]
```

**Examples:**
```
RI.CREATE r1
RI.CREATE r1 MEMORY CACHESIZE 33554432
RI.CREATE r1 DISK /data/r1.bftree CACHESIZE 67108864 MAXKEYLEN 64 WAL /data/r1.wal
RI.CREATE r1 CACHE CACHESIZE 16777216 MINRECORD 8 MAXRECORD 4096
```

**Reply:** `+OK` or `-ERR <config error message>`

---

### 2. Write Commands

| Command | Syntax | Description | Maps to |
|---|---|---|---|
| **RI.SET** | `RI.SET key field value` | Insert or update a key-value entry in the RangeIndex | `BfTree::insert(key, value)` |
| **RI.DEL** | `RI.DEL key field` | Delete an entry from the RangeIndex | `BfTree::delete(key)` |
| **RI.MSET** | `RI.MSET key field1 value1 [field2 value2 ...]` | Batch insert multiple entries | Multiple `BfTree::insert()` |
| **RI.MDEL** | `RI.MDEL key field1 [field2 ...]` | Batch delete multiple entries | Multiple `BfTree::delete()` |

**Terminology:** `key` is the RangeIndex name; `field` is the entry key within the BfTree; `value` is the entry value.

#### `RI.SET`
```
RI.SET r1 "user:1001" "Alice"
```
**Reply:** `+OK` on success, `-ERR <reason>` on `InvalidKV`

#### `RI.DEL`
```
RI.DEL r1 "user:1001"
```
**Reply:** `:1` (deleted) or `:0` (not found)

#### `RI.MSET`
```
RI.MSET r1 "user:1001" "Alice" "user:1002" "Bob" "user:1003" "Charlie"
```
**Reply:** `:3` (number of entries inserted)

#### `RI.MDEL`
```
RI.MDEL r1 "user:1001" "user:1002"
```
**Reply:** `:2` (number of entries deleted)

---

### 3. Read Commands

| Command | Syntax | Description | Maps to |
|---|---|---|---|
| **RI.GET** | `RI.GET key field` | Read a single entry | `BfTree::read(key, buffer)` |
| **RI.MGET** | `RI.MGET key field1 [field2 ...]` | Read multiple entries | Multiple `BfTree::read()` |

#### `RI.GET`
```
RI.GET r1 "user:1001"
```
**Reply:** `$5\r\nAlice\r\n` (bulk string) or `$-1` (nil, if not found/deleted)

#### `RI.MGET`
```
RI.MGET r1 "user:1001" "user:1002" "user:9999"
```
**Reply:** Array of bulk strings (nil for missing entries):
```
*3\r\n$5\r\nAlice\r\n$3\r\nBob\r\n$-1\r\n
```

---

### 4. Scan / Range Query Commands

These are the core differentiating commands that leverage Bf-Tree's range scan capability.

| Command | Syntax | Description | Maps to |
|---|---|---|---|
| **RI.SCAN** | `RI.SCAN key start COUNT n [FIELDS KEY\|VALUE\|BOTH]` | Scan `n` entries starting from `start` key | `BfTree::scan_with_count()` |
| **RI.RANGE** | `RI.RANGE key start end [FIELDS KEY\|VALUE\|BOTH]` | Scan entries in `[start, end]` range | `BfTree::scan_with_end_key()` |

#### `RI.SCAN`

Scans `n` entries starting at `start` key (inclusive), ordered by key bytes.

```
RI.SCAN r1 "user:1000" COUNT 10
RI.SCAN r1 "user:1000" COUNT 10 FIELDS KEY
RI.SCAN r1 "user:1000" COUNT 10 FIELDS VALUE
RI.SCAN r1 "user:1000" COUNT 10 FIELDS BOTH
```

**Default `FIELDS`:** `BOTH` (returns key-value pairs)

**Reply** (with `FIELDS BOTH`, default):
```
*4
*2
$9
user:1000
$5
Alice
*2
$9
user:1001
$3
Bob
...
```
Each element is a 2-element array `[key, value]`.

**Reply** (with `FIELDS KEY`):
Array of bulk strings (keys only).

**Reply** (with `FIELDS VALUE`):
Array of bulk strings (values only).

**Errors:**
- `-ERR invalid count` if count is 0
- `-ERR invalid start key` if key is empty or too long
- `-ERR cache-only mode does not support scan` if the index is cache-only

#### `RI.RANGE`

Scans all entries in the closed range `[start, end]`.

```
RI.RANGE r1 "user:1000" "user:2000"
RI.RANGE r1 "user:1000" "user:2000" FIELDS KEY
RI.RANGE r1 "a" "z" FIELDS BOTH
```

Same reply format as `RI.SCAN`.

**Errors:**
- `-ERR invalid start key`
- `-ERR invalid end key`
- `-ERR start key must be <= end key`

---

### 5. Utility Commands

| Command | Syntax | Description |
|---|---|---|
| **RI.KEYS** | `RI.KEYS [pattern]` | List all RangeIndex names (optionally filtered by glob pattern) |
| **RI.PING** | `RI.PING [message]` | Health check, returns `PONG` or echoes message |

---

## RESP Protocol Details

All commands follow [RESP3](https://redis.io/docs/reference/protocol-spec/) conventions:

- **Simple Strings:** `+OK\r\n`
- **Errors:** `-ERR message\r\n`
- **Integers:** `:N\r\n`
- **Bulk Strings:** `$len\r\ndata\r\n` (nil = `$-1\r\n`)
- **Arrays:** `*count\r\n...`

Keys and values are transmitted as raw bytes (bulk strings), matching Bf-Tree's `&[u8]` interface.

---

## Mapping Summary: Bf-Tree API → RESP Commands

| Bf-Tree Method | RESP Command |
|---|---|
| `BfTree::new()` | `RI.CREATE` |
| `BfTree::with_config()` | `RI.CREATE ... [options]` |
| `drop(BfTree)` | `RI.DROP` |
| `BfTree::insert(key, value)` | `RI.SET` |
| `BfTree::read(key, buf)` | `RI.GET` |
| `BfTree::delete(key)` | `RI.DEL` |
| `BfTree::scan_with_count()` | `RI.SCAN` |
| `BfTree::scan_with_end_key()` | `RI.RANGE` |
| `BfTree::config()` | `RI.CONFIG` |
| `BfTree::get_buffer_metrics()` | `RI.METRICS` |

---

## Example Session

```
> RI.CREATE r1 MEMORY CACHESIZE 33554432
+OK

> RI.SET r1 "emp:001" "Alice,Engineering,L5"
+OK

> RI.SET r1 "emp:002" "Bob,Sales,L3"
+OK

> RI.SET r1 "emp:010" "Charlie,Engineering,L7"
+OK

> RI.GET r1 "emp:002"
$14
Bob,Sales,L3

> RI.SCAN r1 "emp:001" COUNT 2 FIELDS BOTH
*2
*2
$7
emp:001
$20
Alice,Engineering,L5
*2
$7
emp:002
$14
Bob,Sales,L3

> RI.RANGE r1 "emp:001" "emp:010" FIELDS KEY
*3
$7
emp:001
$7
emp:002
$7
emp:010

> RI.DEL r1 "emp:002"
:1

> RI.DROP r1
+OK
```

---

# Implementation Plan: RangeIndex in Garnet

This plan is designed to be self-contained: it includes enough context, file paths, code
patterns, and reference pointers so that an implementer can work through each step without
needing to rediscover the Garnet architecture.

## Overview

RangeIndex is implemented as a **built-in Garnet type stored in the unified store**,
accessed via the **string context**. A small fixed-size struct ("stub") is stored as a
raw-byte value (not a heap object) in Tsavorite's unified store. The stub contains BfTree
configuration metadata, a native pointer (`nint`) to the live BfTree instance, and a
`Guid` process-instance-id for stale-pointer detection after restart. A
`RangeIndexManager` (partial class) owns the BfTree lifecycle outside of Tsavorite.

This follows the same "stub-in-store with external data manager" pattern used by
**VectorManager** on the
[`vectorApiPoC-storeV2`](https://github.com/microsoft/garnet/tree/vectorApiPoC-storeV2)
prototype branch. Implementers should cross-reference that branch for working examples
of each pattern described below. The key difference: VectorSet stores element data inside
Tsavorite (via a separate `VectorSessionFunctions` context), while BfTree manages **all**
its data externally (circular buffer + disk files), so RangeIndex only needs the string
context for the stub — no additional Tsavorite context type is required.

**Why the string context and not the object context?**
- The Bf-Tree manages its own memory (circular buffer, leaf pages) and disk storage — it
  cannot be inlined into Tsavorite's log the way a `HashObject` or `SortedSetObject` can.
- The stub pattern cleanly separates metadata persistence (Tsavorite checkpoint) from
  index operations (Bf-Tree).
- A 50-byte fixed-size stub is a natural fit for the string context's inline byte values,
  avoiding the overhead of `GarnetObjectBase` serialization.
- The `RecordInfo.ValueIsObject` bit remains `false` for RangeIndex records, distinguishing
  them from collection objects.

---

## Proposed File Layout

The RangeIndex implementation is organized as a partial class (`RangeIndexManager`) split
across multiple files, plus supporting files for RESP handlers, storage session wrappers,
and native interop. Each file mirrors a corresponding VectorManager file on the
[`vectorApiPoC-storeV2`](https://github.com/microsoft/garnet/tree/vectorApiPoC-storeV2)
prototype branch.

| # | New File | Prototype reference (`vectorApiPoC-storeV2`) | Role |
|---|---|---|---|
| 1 | `libs/server/Resp/RangeIndex/RangeIndexManager.cs` | [`VectorManager.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Resp/Vector/VectorManager.cs) | Main class: constants, `processInstanceId`, `IsEnabled`, initialization, `TryInsert`/`TryRead`/`TryScan`/`TryRange` methods, `ResumePostRecovery()` |
| 2 | `libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs` | [`VectorManager.Index.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Resp/Vector/VectorManager.Index.cs) | Stub struct definition, `CreateIndex()`, `ReadIndex()`, `RecreateIndex()`, `DropIndex()` |
| 3 | `libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs` | [`VectorManager.Locking.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Resp/Vector/VectorManager.Locking.cs) | `ReadRangeIndexLock` ref struct, `ReadRangeIndex()`, `ReadOrCreateRangeIndex()` — shared/exclusive lock management via `ReadOptimizedLock` |
| 4 | `libs/server/Resp/RangeIndex/RangeIndexManager.Cleanup.cs` | [`VectorManager.Cleanup.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Resp/Vector/VectorManager.Cleanup.cs) | Post-drop async cleanup: background task that scans and removes orphaned data |
| 5 | `libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs` | [`VectorManager.Migration.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Resp/Vector/VectorManager.Migration.cs) | *(future)* Replication/migration support |
| 6 | `libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs` | [`VectorManager.Replication.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Resp/Vector/VectorManager.Replication.cs) | *(future)* Primary→replica replication |
| 7 | `libs/server/Resp/RangeIndex/BfTreeService.cs` | [`DiskANNService.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Resp/Vector/DiskANNService.cs) | Wraps native BfTree library via P/Invoke |
| 8 | `libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs` | [`RespServerSessionVectors.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Resp/Vector/RespServerSessionVectors.cs) | RESP command handlers (`NetworkRISET`, `NetworkRIGET`, etc.) |
| 9 | `libs/server/Storage/Session/MainStore/RangeIndexOps.cs` | *(inline in VectorManager methods)* | Storage session wrappers that acquire locks via manager and call `Try*` methods |

Additionally, several existing files are modified (see [Complete File Inventory](#complete-file-inventory)).

---

## Architecture (data flow)

```
RESP Client ("RI.SET r1 mykey myval")
  │
  ▼
┌───────────────────────────────────────────────────────────────────────┐
│ 1. RESP Parser (Resp/Parser/RespCommand.cs)                           │
│    Tokenizes "RI.SET" → RespCommand.RISET enum value                  │
│    Read/write classification by enum position (reads < APPEND)        │
├───────────────────────────────────────────────────────────────────────┤
│ 2. Command Dispatch (Resp/RespServerSession.cs)                       │
│    Switch on RespCommand → calls NetworkRISET<TGarnetApi>(ref api)    │
├───────────────────────────────────────────────────────────────────────┤
│ 3. RESP Handler (Resp/RangeIndex/RespServerSessionRangeIndex.cs)      │
│    Parses args from parseState, calls storageApi.RangeIndexSet(...)   │
├───────────────────────────────────────────────────────────────────────┤
│ 4. IGarnetApi / GarnetApi (API/IGarnetApi.cs, API/GarnetApi.cs)       │
│    Thin delegation: PinnedSpanByte → ReadOnlySpan<byte>, forwards to  │
│    storageSession.RangeIndexSet(...)                                  │
├───────────────────────────────────────────────────────────────────────┤
│ 5. Storage Session (Storage/Session/MainStore/RangeIndexOps.cs)       │
│    Acquires index lock via rangeIndexManager.ReadOrCreateRangeIndex() │
│    Calls rangeIndexManager.TryInsert(indexSpan, field, value)         │
│    Replicates on success via rangeIndexManager.Replicate*(...)        │
├───────────────────────────────────────────────────────────────────────┤
│ 6. RangeIndexManager (Resp/RangeIndex/RangeIndexManager.cs)           │
│    TryInsert: ReadIndex(stub) → extract TreePtr → BfTreeService.Insert│
├───────────────────────────────────────────────────────────────────────┤
│ 7. BfTreeService (Resp/RangeIndex/BfTreeService.cs)                   │
│    P/Invoke call: bftree_insert(treePtr, key, keyLen, val, valLen)    │
├───────────────────────────────────────────────────────────────────────┤
│ 8. Bf-Tree Rust library (bftree.dll / libbftree.so)                   │
│    BfTree::insert(key, value) → LeafInsertResult::Success             │
└───────────────────────────────────────────────────────────────────────┘
```

**On first write (key doesn't exist yet):**
- Step 5 calls `ReadOrCreateRangeIndex()` which fails to find the key
- It promotes the shared lock to exclusive, creates a new BfTree via
  `BfTreeService.CreateIndex()`, and issues an RMW with the new stub
- The RMW hits `InitialUpdater` in `RMWMethods.cs` which writes the stub and
  sets the `RecordType` to `RangeIndexManager.RangeIndexRecordType` on the record
- Lock is released, then re-acquired as shared for the actual operation

---

## The Stub (RangeIndexManager.Index.cs)

> **Prototype reference:** [`VectorManager.Index.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Resp/Vector/VectorManager.Index.cs) —
> the 50-byte `Index` struct, `CreateIndex()`, `ReadIndex()`, `RecreateIndex()`, `DropIndex()`.

A fixed-size struct stored as a raw-byte (non-object) value in the unified store, accessed
via the string context. Since `RecordInfo.ValueIsObject` is `false` for these records, the
string context's `MainSessionFunctions` handles the RMW/Read/Delete callbacks.

```csharp
[StructLayout(LayoutKind.Explicit, Size = 50)]
private struct RangeIndexStub
{
    [FieldOffset(0)]  public nint TreePtr;              // Pointer to live BfTree instance
    [FieldOffset(8)]  public ulong CacheSize;           // BfTree cb_size_byte
    [FieldOffset(16)] public uint MinRecordSize;        // BfTree cb_min_record_size
    [FieldOffset(20)] public uint MaxRecordSize;        // BfTree cb_max_record_size
    [FieldOffset(24)] public uint MaxKeyLen;            // BfTree cb_max_key_len
    [FieldOffset(28)] public uint LeafPageSize;         // BfTree leaf_page_size
    [FieldOffset(32)] public byte StorageBackend;       // 0=Memory, 1=Disk, 2=Cache
    [FieldOffset(33)] public byte Flags;                // bit 0: WAL enabled
    [FieldOffset(34)] public Guid ProcessInstanceId;    // Detects stale pointers after restart/eviction
}
```

**Key fields explained:**

- `TreePtr` — Native pointer to a live `BfTree` instance. Becomes invalid after process
  restart. On recovery, `ProcessInstanceId` mismatch triggers `RecreateIndex()` which
  restores from snapshot and updates this field.
- `ProcessInstanceId` — Each `RangeIndexManager` instance generates a unique `Guid` at
  startup. When reading a stub, if `stub.ProcessInstanceId != this.processInstanceId`,
  the pointer is stale and the BfTree must be recreated from snapshot.
- Config fields (`CacheSize`, `MinRecordSize`, etc.) — Persisted so recovery can
  reconstruct the BfTree with identical configuration.

---

## Step-by-Step Implementation

### Step 1: Type discrimination for RangeIndex records

> **Reference:** `libs/storage/Tsavorite/cs/src/core/Allocator/LogRecord.cs` —
> `RecordType` is a `byte` field in the `RecordDataHeader` (offset 2 in the header),
> accessible via `LogRecord.RecordType` and `srcLogRecord.RecordType` in session function
> callbacks. Currently `LogRecord.InitializeRecord()` hardcodes `recordType: 0` (with
> a TODO to pass in the actual type).
>
> See also: `RecordDataHeader.cs` — `RecordTypeOffsetInHeader = 2`,
> `ISourceLogRecord.RecordType` property.

RangeIndex records need to be distinguishable from regular string records so that:
- **ReadMethods.cs** — RI commands are rejected on non-RI keys, and non-RI commands
  are rejected on RI keys (type safety)
- **DeleteMethods.cs** — deletion is blocked while stub is non-zero (BfTree still alive)
- **RMWMethods.cs** — no guard needed; the command itself determines behavior

**Approach:** Use the `RecordType` byte on each `LogRecord`. Define a constant
`RangeIndexRecordType` (e.g., `2`, reserving `1` for VectorSet). This byte is set when
the stub record is first created and checked in `Reader`/`Deleter` callbacks for type
safety.

```csharp
// In RangeIndexManager.cs:
internal const byte RangeIndexRecordType = 2;
```

**Tsavorite plumbing required:** The `RecordDataHeader.Initialize()` method and
`LogRecord.InitializeRecord()` currently hardcode `recordType: 0`. These need to be
updated to accept and propagate a `recordType` parameter from the session function
callbacks (e.g., `InitialUpdater` sets the record type when creating a new record).
This is a one-time infrastructure change that also unblocks VectorSet.

Also add a helper in `RespCommand` extensions:

```csharp
// In RespCommandExtensions.cs:
public static bool IsLegalOnRangeIndex(this RespCommand cmd)
    => cmd is RespCommand.RISET or RespCommand.RIDEL or RespCommand.RIGET
       or RespCommand.RIMSET or RespCommand.RIMDEL or RespCommand.RIMGET
       or RespCommand.RISCAN or RespCommand.RIRANGE
       or RespCommand.RICREATE or RespCommand.RIDROP
       or RespCommand.RIEXISTS or RespCommand.RICONFIG or RespCommand.RIMETRICS;
```

---

### Step 2: Add RESP commands to the parser

> **Reference:** `libs/server/Resp/Parser/RespCommand.cs`
> - Read commands are defined before `APPEND` (the last read command is just before `APPEND`)
> - Write commands are defined starting at `APPEND`
> - Read/write classification uses enum ordering: `cmd <= LastReadCommand` ⟹ read-only
> - Fast parsing: `FastParseArrayCommand()` uses `ulong` pointer comparisons for short
>   fixed-length commands
> - Longer/unusual commands fall through to `SlowParseCommand()`

**Add enum values:**

```csharp
// --- Read commands (insert before APPEND) ---
RIGET,        // RI.GET key field
RIMGET,       // RI.MGET key field1 [field2 ...]
RISCAN,       // RI.SCAN key start COUNT n [FIELDS KEY|VALUE|BOTH]
RIRANGE,      // RI.RANGE key start end [FIELDS KEY|VALUE|BOTH]
RIEXISTS,     // RI.EXISTS key
RICONFIG,     // RI.CONFIG key
RIMETRICS,    // RI.METRICS key
RIKEYS,       // RI.KEYS [pattern]

// --- Write commands (insert after APPEND, before boundary) ---
RISET,        // RI.SET key field value
RIDEL,        // RI.DEL key field
RIMSET,       // RI.MSET key f1 v1 [f2 v2 ...]
RIMDEL,       // RI.MDEL key f1 [f2 ...]
RICREATE,     // RI.CREATE key [options]
RIDROP,       // RI.DROP key
```

**Parsing:** RI commands are dot-prefixed (`RI.SET`), so they won't fit the 4-char fast
path. Add a branch in `SlowParseCommand()` or a dedicated `ParseRangeIndexCommand()`:

```csharp
// In SlowParseCommand or equivalent:
if (length >= 4 && ptr[0] == 'R' && ptr[1] == 'I' && ptr[2] == '.')
{
    return ParseRangeIndexCommand(ptr + 3, length - 3);
}

private static RespCommand ParseRangeIndexCommand(byte* ptr, int length)
{
    // Match remaining bytes: "SET", "GET", "DEL", "SCAN", "RANGE", etc.
    return length switch
    {
        3 when *(ushort*)ptr == MemoryMarshal.Read<ushort>("SE"u8)
            && ptr[2] == (byte)'T' => RespCommand.RISET,
        3 when *(ushort*)ptr == MemoryMarshal.Read<ushort>("GE"u8)
            && ptr[2] == (byte)'T' => RespCommand.RIGET,
        3 when *(ushort*)ptr == MemoryMarshal.Read<ushort>("DE"u8)
            && ptr[2] == (byte)'L' => RespCommand.RIDEL,
        4 when *(uint*)ptr == MemoryMarshal.Read<uint>("SCAN"u8) => RespCommand.RISCAN,
        4 when *(uint*)ptr == MemoryMarshal.Read<uint>("MSET"u8) => RespCommand.RIMSET,
        4 when *(uint*)ptr == MemoryMarshal.Read<uint>("MDEL"u8) => RespCommand.RIMDEL,
        4 when *(uint*)ptr == MemoryMarshal.Read<uint>("MGET"u8) => RespCommand.RIMGET,
        4 when *(uint*)ptr == MemoryMarshal.Read<uint>("DROP"u8) => RespCommand.RIDROP,
        4 when *(uint*)ptr == MemoryMarshal.Read<uint>("KEYS"u8) => RespCommand.RIKEYS,
        5 when *(uint*)ptr == MemoryMarshal.Read<uint>("RANG"u8)
            && ptr[4] == (byte)'E' => RespCommand.RIRANGE,
        6 when *(uint*)ptr == MemoryMarshal.Read<uint>("CREA"u8) => RespCommand.RICREATE,
        6 when *(uint*)ptr == MemoryMarshal.Read<uint>("CONF"u8) => RespCommand.RICONFIG,
        6 when *(uint*)ptr == MemoryMarshal.Read<uint>("EXIS"u8) => RespCommand.RIEXISTS,
        7 when *(uint*)ptr == MemoryMarshal.Read<uint>("METR"u8) => RespCommand.RIMETRICS,
        _ => RespCommand.NONE,
    };
}
```

---

### Step 3: Add command dispatch in `RespServerSession`

> **Reference:** `libs/server/Resp/RespServerSession.cs`
> Commands are dispatched via switch expressions in `ProcessBasicCommands` and
> `ProcessArrayCommands`. Each maps a `RespCommand` enum to a handler method.
> Example: `RespCommand.GET => NetworkGET(ref storageApi),`

Add RangeIndex dispatch entries:

```csharp
// In the command dispatch switch:
RespCommand.RISET    => NetworkRISET(ref storageApi),
RespCommand.RIDEL    => NetworkRIDEL(ref storageApi),
RespCommand.RIGET    => NetworkRIGET(ref storageApi),
RespCommand.RISCAN   => NetworkRISCAN(ref storageApi),
RespCommand.RIRANGE  => NetworkRIRANGE(ref storageApi),
RespCommand.RIMSET   => NetworkRIMSET(ref storageApi),
RespCommand.RIMDEL   => NetworkRIMDEL(ref storageApi),
RespCommand.RIMGET   => NetworkRIMGET(ref storageApi),
RespCommand.RICREATE => NetworkRICREATE(ref storageApi),
RespCommand.RIDROP   => NetworkRIDROP(ref storageApi),
RespCommand.RIEXISTS => NetworkRIEXISTS(ref storageApi),
RespCommand.RICONFIG => NetworkRICONFIG(ref storageApi),
RespCommand.RIMETRICS => NetworkRIMETRICS(ref storageApi),
RespCommand.RIKEYS   => NetworkRIKEYS(ref storageApi),
```

---

### Step 4: Implement RESP command handlers

> **Reference:** RESP handler pattern used throughout `libs/server/Resp/` (e.g.,
> `BasicCommands.cs`, `Objects/HashCommands.cs`).
> **Prototype reference:** [`RespServerSessionVectors.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Resp/Vector/RespServerSessionVectors.cs) —
> vector RESP handlers (`NetworkVADD`, `NetworkVGET`, etc.) follow the same pattern.
> Pattern: `private bool NetworkXXX<TGarnetApi>(ref TGarnetApi storageApi) where TGarnetApi : IGarnetApi`
> - Parse arguments from `parseState.GetArgSliceByRef(idx)`
> - Call `storageApi.RangeIndex*(...)` with parsed args
> - Write RESP response via `RespWriteUtils.TryWrite*(..., ref dcurr, dend)`
> - Handle `GarnetStatus.OK`, `NOTFOUND`, `WRONGTYPE`

**File:** `libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs` (new)

<details>
<summary>RESP command handler implementations (click to expand)</summary>

```csharp
internal sealed unsafe partial class RespServerSession
{
    /// RI.SET key field value
    /// Inserts or updates a field in the RangeIndex. Auto-creates the index if needed.
    private bool NetworkRISET<TGarnetApi>(ref TGarnetApi storageApi)
        where TGarnetApi : IGarnetApi
    {
        // Expect 3 args: key, field, value
        if (parseState.Count != 3)
        {
            return AbortWithWrongNumberOfArguments(nameof(RespCommand.RISET));
        }

        var key = parseState.GetArgSliceByRef(0);
        var field = parseState.GetArgSliceByRef(1);
        var value = parseState.GetArgSliceByRef(2);

        var res = storageApi.RangeIndexSet(key, field, value,
            out var result, out var errorMsg);

        if (res == GarnetStatus.WRONGTYPE)
            // Key exists but is not a RangeIndex
            return AbortRangeIndexWrongType();

        if (result == RangeIndexResult.OK)
        {
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
        }
        else
        {
            while (!RespWriteUtils.WriteError(errorMsg, ref dcurr, dend))
                SendAndReset();
        }
        return true;
    }

    /// RI.GET key field
    /// Returns the value for a field, or nil if not found.
    private bool NetworkRIGET<TGarnetApi>(ref TGarnetApi storageApi)
        where TGarnetApi : IGarnetApi
    {
        if (parseState.Count != 2)
            return AbortWithWrongNumberOfArguments(nameof(RespCommand.RIGET));

        var key = parseState.GetArgSliceByRef(0);
        var field = parseState.GetArgSliceByRef(1);
        var output = new SpanByteAndMemory();

        var res = storageApi.RangeIndexGet(key, field, ref output, out var result);

        if (res == GarnetStatus.WRONGTYPE)
            return AbortRangeIndexWrongType();
        if (res == GarnetStatus.NOTFOUND || result == RangeIndexResult.NotFound)
        {
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                SendAndReset();
        }
        else
        {
            // Write bulk string from output
            // ... (use output.SpanByte or output.Memory)
        }
        return true;
    }

    /// RI.DEL key field
    private bool NetworkRIDEL<TGarnetApi>(ref TGarnetApi storageApi)
        where TGarnetApi : IGarnetApi
    {
        if (parseState.Count != 2)
            return AbortWithWrongNumberOfArguments(nameof(RespCommand.RIDEL));

        var key = parseState.GetArgSliceByRef(0);
        var field = parseState.GetArgSliceByRef(1);

        var res = storageApi.RangeIndexDel(key, field);

        if (res == GarnetStatus.WRONGTYPE)
            return AbortRangeIndexWrongType();

        while (!RespWriteUtils.TryWriteInt32(res == GarnetStatus.OK ? 1 : 0,
            ref dcurr, dend))
            SendAndReset();
        return true;
    }

    /// RI.SCAN key start COUNT n [FIELDS KEY|VALUE|BOTH]
    private bool NetworkRISCAN<TGarnetApi>(ref TGarnetApi storageApi)
        where TGarnetApi : IGarnetApi
    {
        // Minimum 4 args: key, start, "COUNT", n
        // Optional: "FIELDS", KEY|VALUE|BOTH
        if (parseState.Count < 4)
            return AbortWithWrongNumberOfArguments(nameof(RespCommand.RISCAN));

        var key = parseState.GetArgSliceByRef(0);
        var startKey = parseState.GetArgSliceByRef(1);

        // Parse COUNT
        // parseState[2] must be "COUNT"
        var countArg = parseState.GetArgSliceByRef(3);
        if (!NumUtils.TryParse(countArg.ReadOnlySpan, out int count) || count <= 0)
        {
            while (!RespWriteUtils.WriteError("ERR invalid count"u8, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        // Parse optional FIELDS
        byte returnField = 2; // 0=Key, 1=Value, 2=KeyAndValue (default BOTH)
        if (parseState.Count >= 6)
        {
            // parseState[4] = "FIELDS", parseState[5] = KEY|VALUE|BOTH
            var fieldsVal = parseState.GetArgSliceByRef(5).ReadOnlySpan;
            if (fieldsVal.SequenceEqual("KEY"u8)) returnField = 0;
            else if (fieldsVal.SequenceEqual("VALUE"u8)) returnField = 1;
            else returnField = 2;
        }

        var output = new SpanByteAndMemory();
        var res = storageApi.RangeIndexScan(key, startKey, count, returnField,
            ref output, out var resultCount);

        // Write RESP array from output
        // ...
        return true;
    }

    /// RI.RANGE key start end [FIELDS KEY|VALUE|BOTH]
    private bool NetworkRIRANGE<TGarnetApi>(ref TGarnetApi storageApi)
        where TGarnetApi : IGarnetApi
    {
        if (parseState.Count < 3)
            return AbortWithWrongNumberOfArguments(nameof(RespCommand.RIRANGE));

        var key = parseState.GetArgSliceByRef(0);
        var startKey = parseState.GetArgSliceByRef(1);
        var endKey = parseState.GetArgSliceByRef(2);

        byte returnField = 2; // default BOTH
        if (parseState.Count >= 5)
        {
            var fieldsVal = parseState.GetArgSliceByRef(4).ReadOnlySpan;
            if (fieldsVal.SequenceEqual("KEY"u8)) returnField = 0;
            else if (fieldsVal.SequenceEqual("VALUE"u8)) returnField = 1;
            else returnField = 2;
        }

        var output = new SpanByteAndMemory();
        var res = storageApi.RangeIndexRange(key, startKey, endKey, returnField,
            ref output, out var resultCount);

        // Write RESP array from output
        // ...
        return true;
    }

    // NetworkRICREATE, NetworkRIDROP, NetworkRIMSET, NetworkRIMDEL,
    // NetworkRIMGET, NetworkRIEXISTS, NetworkRICONFIG, NetworkRIMETRICS,
    // NetworkRIKEYS follow the same pattern.
}
```

</details>

---

### Step 5: Add `IGarnetApi` and `GarnetApi` interface methods

> **Reference:** `libs/server/API/IGarnetApi.cs` — declares storage API methods.
> `libs/server/API/GarnetApi.cs` — delegation: `PinnedSpanByte` → `.ReadOnlySpan`
> then forward to `storageSession.RangeIndex*()`.

**IGarnetApi.cs — add:**

<details>
<summary>IGarnetApi method declarations (click to expand)</summary>

```csharp
// --- RangeIndex operations ---

GarnetStatus RangeIndexSet(PinnedSpanByte key, PinnedSpanByte field, PinnedSpanByte value,
    out RangeIndexResult result, out ReadOnlySpan<byte> errorMsg);

GarnetStatus RangeIndexDel(PinnedSpanByte key, PinnedSpanByte field);

GarnetStatus RangeIndexGet(PinnedSpanByte key, PinnedSpanByte field,
    ref StringOutput output, out RangeIndexResult result);

GarnetStatus RangeIndexScan(PinnedSpanByte key, PinnedSpanByte startKey,
    int count, byte returnField,
    ref StringOutput output, out int resultCount);

GarnetStatus RangeIndexRange(PinnedSpanByte key, PinnedSpanByte startKey, PinnedSpanByte endKey,
    byte returnField,
    ref StringOutput output, out int resultCount);

GarnetStatus RangeIndexCreate(PinnedSpanByte key,
    ulong cacheSize, uint minRecord, uint maxRecord,
    uint maxKeyLen, uint leafPageSize, byte storageBackend,
    out RangeIndexResult result, out ReadOnlySpan<byte> errorMsg);

GarnetStatus RangeIndexDrop(PinnedSpanByte key);

GarnetStatus RangeIndexExists(PinnedSpanByte key, out bool exists);

GarnetStatus RangeIndexConfig(PinnedSpanByte key,
    ref StringOutput output);

GarnetStatus RangeIndexMetrics(PinnedSpanByte key,
    ref StringOutput output);
```

</details>

**GarnetApi.cs — add delegation** (expression-bodied):

<details>
<summary>GarnetApi delegation methods (click to expand)</summary>

```csharp
public GarnetStatus RangeIndexSet(PinnedSpanByte key, PinnedSpanByte field, PinnedSpanByte value,
    out RangeIndexResult result, out ReadOnlySpan<byte> errorMsg)
    => storageSession.RangeIndexSet(
        key.ReadOnlySpan,
        field, value, out result, out errorMsg);

public GarnetStatus RangeIndexGet(PinnedSpanByte key, PinnedSpanByte field,
    ref StringOutput output, out RangeIndexResult result)
    => storageSession.RangeIndexGet(
        key.ReadOnlySpan,
        field, ref output, out result);

// ... same pattern for all methods
```

</details>

---

### Step 6: Implement Storage Session layer (`RangeIndexOps.cs`)

> **Reference:** `libs/server/Storage/Session/MainStore/` — contains string-context
> storage operations (e.g., `MainStoreOps.cs`). RangeIndex follows the same pattern
> with a new file `RangeIndexOps.cs`.
> - Write ops: marshal args → `ReadOrCreateRangeIndex()` → `TryInsert()` → replicate
> - Read ops: `ReadRangeIndex()` → `TryRead()` / `TryScan()` / `TryRange()`
> - Lock pattern: `using (rangeIndexManager.ReadOrCreateRangeIndex(this, ...))` — the
>   `using` block holds a shared lock via `ReadRangeIndexLock` ref struct
> - On first access, `ReadOrCreateRangeIndex` promotes to exclusive lock, creates BfTree
>   via `BfTreeService.CreateIndex()`, issues RMW to persist stub, releases exclusive,
>   re-acquires shared lock, then returns

**File:** `libs/server/Storage/Session/MainStore/RangeIndexOps.cs` (new)

<details>
<summary>StorageSession RangeIndexOps implementation (click to expand)</summary>

```csharp
internal sealed unsafe partial class StorageSession
{
    /// RI.SET — insert/update a field in the range index
    public GarnetStatus RangeIndexSet(ReadOnlySpan<byte> key, PinnedSpanByte field, PinnedSpanByte value,
        out RangeIndexResult result, out ReadOnlySpan<byte> errorMsg)
    {
        errorMsg = default;

        // Marshal field + value into parseState for replication log
        parseState.InitializeWithArguments(field, value);
        var input = new StringInput(RespCommand.RISET, ref parseState);

        // Acquire lock + create-if-needed
        Span<byte> indexSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];
        using (rangeIndexManager.ReadOrCreateRangeIndex(
            this, key, ref input, indexSpan, out var status))
        {
            if (status != GarnetStatus.OK)
            {
                result = RangeIndexResult.Error;
                return status;
            }

            // Dispatch to BfTree while holding shared lock
            result = rangeIndexManager.TryInsert(
                indexSpan, field.ReadOnlySpan, value.ReadOnlySpan,
                out errorMsg);

            if (result == RangeIndexResult.OK)
                rangeIndexManager.ReplicateRangeIndexSet(
                    key, ref input, ref stringBasicContext);

            return GarnetStatus.OK;
        }
    }

    /// RI.GET — point read
    public GarnetStatus RangeIndexGet(ReadOnlySpan<byte> key, PinnedSpanByte field,
        ref StringOutput output, out RangeIndexResult result)
    {
        parseState.InitializeWithArgument(field);
        var input = new StringInput(RespCommand.RIGET, ref parseState);

        Span<byte> indexSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];
        using (rangeIndexManager.ReadRangeIndex(
            this, key, ref input, indexSpan, out var status))
        {
            if (status != GarnetStatus.OK)
            {
                result = RangeIndexResult.NotFound;
                return status;
            }

            result = rangeIndexManager.TryRead(
                indexSpan, field.ReadOnlySpan, ref output);
            return GarnetStatus.OK;
        }
    }

    /// RI.DEL — delete a field
    public GarnetStatus RangeIndexDel(ReadOnlySpan<byte> key, PinnedSpanByte field)
    {
        parseState.InitializeWithArgument(field);
        var input = new StringInput(RespCommand.RIDEL, ref parseState);

        Span<byte> indexSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];
        using (rangeIndexManager.ReadRangeIndex(
            this, key, ref input, indexSpan, out var status))
        {
            if (status != GarnetStatus.OK) return status;

            rangeIndexManager.TryDelete(indexSpan, field.ReadOnlySpan);
            rangeIndexManager.ReplicateRangeIndexDel(
                key, ref input, ref stringBasicContext);
            return GarnetStatus.OK;
        }
    }

    /// RI.SCAN — range scan with count
    public GarnetStatus RangeIndexScan(ReadOnlySpan<byte> key, PinnedSpanByte startKey,
        int count, byte returnField,
        ref StringOutput output, out int resultCount)
    {
        // ... same lock pattern, then:
        // rangeIndexManager.TryScanWithCount(indexSpan, startKey, count, returnField, ref output, out resultCount)
    }

    /// RI.RANGE — range scan with end key
    public GarnetStatus RangeIndexRange(ReadOnlySpan<byte> key, PinnedSpanByte startKey,
        PinnedSpanByte endKey, byte returnField,
        ref StringOutput output, out int resultCount)
    {
        // ... same lock pattern, then:
        // rangeIndexManager.TryScanWithEndKey(indexSpan, startKey, endKey, returnField, ref output, out resultCount)
    }
}
```

</details>

---

### Step 7: Implement `RangeIndexManager` (partial class)

> `RangeIndexManager` is a new partial class split across multiple files.
> We start with 4 core files; migration and replication are deferred.

#### 7a. `RangeIndexManager.cs` — Main class

> **Prototype reference:** [`VectorManager.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Resp/Vector/VectorManager.cs) —
> constants, `processInstanceId`, `TryAdd`/`TryRemove`, `Initialize()`, `ResumePostRecovery()`, `Dispose()`.

<details>
<summary>RangeIndexManager main class (click to expand)</summary>

```csharp
public sealed partial class RangeIndexManager : IDisposable
{
    // --- Constants ---
    internal const int IndexSizeBytes = 50; // sizeof(RangeIndexStub)
    internal const long RISetAppendLogArg = long.MinValue;
    internal const long DeleteAfterDropArg = RISetAppendLogArg + 1;
    internal const long RecreateIndexArg = DeleteAfterDropArg + 1;
    internal const long RIDelAppendLogArg = RecreateIndexArg + 1;

    // --- Fields ---
    private readonly Guid processInstanceId = Guid.NewGuid();
    private readonly BfTreeService service = new();
    public bool IsEnabled { get; }
    private readonly ILogger logger;

    // --- Core operation methods ---

    /// Insert a field-value pair into the BfTree identified by the stub
    internal RangeIndexResult TryInsert(ReadOnlySpan<byte> indexValue,
        ReadOnlySpan<byte> field, ReadOnlySpan<byte> value,
        out ReadOnlySpan<byte> errorMsg)
    {
        errorMsg = default;
        ReadIndex(indexValue, out var treePtr, out _, out _, out _,
            out _, out _, out _, out _, out _);
        var insertResult = service.Insert(treePtr, field, value);
        if (insertResult == BfTreeInsertResult.InvalidKV)
        {
            errorMsg = "ERR invalid key or value size"u8;
            return RangeIndexResult.Error;
        }
        return RangeIndexResult.OK;
    }

    /// Read a single field from the BfTree
    internal RangeIndexResult TryRead(ReadOnlySpan<byte> indexValue,
        ReadOnlySpan<byte> field, ref SpanByteAndMemory output)
    {
        ReadIndex(indexValue, out var treePtr, out _, out var maxRecordSize,
            out _, out _, out _, out _, out _, out _);
        var result = service.Read(treePtr, field, maxRecordSize, ref output);
        return result;
    }

    /// Delete a field from the BfTree
    internal void TryDelete(ReadOnlySpan<byte> indexValue,
        ReadOnlySpan<byte> field)
    {
        ReadIndex(indexValue, out var treePtr, out _, out _, out _,
            out _, out _, out _, out _, out _);
        service.Delete(treePtr, field);
    }

    /// Scan with count
    internal RangeIndexResult TryScanWithCount(ReadOnlySpan<byte> indexValue,
        ReadOnlySpan<byte> startKey, int count, byte returnField,
        ref SpanByteAndMemory output, out int resultCount)
    {
        ReadIndex(indexValue, out var treePtr, out _, out _, out _,
            out _, out _, out _, out _, out _);
        return service.ScanWithCount(treePtr, startKey, count, returnField,
            ref output, out resultCount);
    }

    /// Scan with end key
    internal RangeIndexResult TryScanWithEndKey(ReadOnlySpan<byte> indexValue,
        ReadOnlySpan<byte> startKey, ReadOnlySpan<byte> endKey,
        byte returnField,
        ref SpanByteAndMemory output, out int resultCount)
    {
        ReadIndex(indexValue, out var treePtr, out _, out _, out _,
            out _, out _, out _, out _, out _);
        return service.ScanWithEndKey(treePtr, startKey, endKey, returnField,
            ref output, out resultCount);
    }

    /// Recovery is lazy — no proactive scan needed. ReadRangeIndex detects stale stubs.
    internal void ResumePostRecovery() { /* no-op; lazy recovery via ReadRangeIndex */ }

    public void Dispose() { service.Dispose(); }
}
```

</details>

#### 7b. `RangeIndexManager.Index.cs` — Stub struct + serialization

> **Prototype reference:** [`VectorManager.Index.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Resp/Vector/VectorManager.Index.cs) —
> 50-byte `Index` struct with `CreateIndex()`, `ReadIndex()`, `RecreateIndex()`, `DropIndex()`, `SetContextForMigration()`.

<details>
<summary>RangeIndexStub struct and serialization methods (click to expand)</summary>

```csharp
public sealed partial class RangeIndexManager
{
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct RangeIndexStub
    {
        internal const int Size = 50;

        [FieldOffset(0)]  public nint TreePtr;
        [FieldOffset(8)]  public ulong CacheSize;
        [FieldOffset(16)] public uint MinRecordSize;
        [FieldOffset(20)] public uint MaxRecordSize;
        [FieldOffset(24)] public uint MaxKeyLen;
        [FieldOffset(28)] public uint LeafPageSize;
        [FieldOffset(32)] public byte StorageBackend;
        [FieldOffset(33)] public byte Flags;
        [FieldOffset(34)] public ushort Reserved;
        [FieldOffset(36)] public uint Reserved2;
        [FieldOffset(40)] public Guid ProcessInstanceId;
    }

    /// Write a new stub into the value span of a LogRecord.
    /// Called from InitialUpdater via RMWMethods.cs.
    internal void CreateIndex(uint cacheSize, uint minRecordSize,
        uint maxRecordSize, uint maxKeyLen, uint leafPageSize,
        byte storageBackend, nint treePtr, Span<byte> valueSpan)
    {
        Debug.Assert(valueSpan.Length >= RangeIndexStub.Size);
        ref var stub = ref Unsafe.As<byte, RangeIndexStub>(
            ref MemoryMarshal.GetReference(valueSpan));
        stub.TreePtr = treePtr;
        stub.CacheSize = cacheSize;
        stub.MinRecordSize = minRecordSize;
        stub.MaxRecordSize = maxRecordSize;
        stub.MaxKeyLen = maxKeyLen;
        stub.LeafPageSize = leafPageSize;
        stub.StorageBackend = storageBackend;
        stub.ProcessInstanceId = processInstanceId;
    }

    /// Deserialize stub from main store value.
    internal static void ReadIndex(ReadOnlySpan<byte> value,
        out nint treePtr, out ulong cacheSize,
        out uint minRecordSize, out uint maxRecordSize,
        out uint maxKeyLen, out uint leafPageSize,
        out byte storageBackend, out byte flags, out Guid pid)
    {
        ref readonly var stub = ref Unsafe.As<byte, RangeIndexStub>(
            ref MemoryMarshal.GetReference(value));
        treePtr = stub.TreePtr;
        cacheSize = stub.CacheSize;
        minRecordSize = stub.MinRecordSize;
        maxRecordSize = stub.MaxRecordSize;
        maxKeyLen = stub.MaxKeyLen;
        leafPageSize = stub.LeafPageSize;
        storageBackend = stub.StorageBackend;
        flags = stub.Flags;
        pid = stub.ProcessInstanceId;
    }

    /// Update TreePtr after recovery (old pointer is stale).
    internal void RecreateIndex(nint newTreePtr, Span<byte> valueSpan)
    {
        ReadIndex(valueSpan, out _, out _, out _, out _,
            out _, out _, out _, out _, out var indexPid);
        Debug.Assert(processInstanceId != indexPid,
            "Shouldn't recreate an index from the same process instance");
        ref var stub = ref Unsafe.As<byte, RangeIndexStub>(
            ref MemoryMarshal.GetReference(valueSpan));
        stub.TreePtr = newTreePtr;
        stub.ProcessInstanceId = processInstanceId;
    }

    /// Zero-check for safe deletion.
    internal void DropIndex(ReadOnlySpan<byte> indexValue)
    {
        ReadIndex(indexValue, out var treePtr, out _, out _, out _,
            out _, out _, out _, out _, out var pid);
        if (pid != processInstanceId)
            return; // Never spun up this index, nothing to drop
        service.Drop(treePtr);
    }
}
```

</details>

#### 7c. `RangeIndexManager.Locking.cs` — Lock management

> The locking pattern uses a striped `ReadOptimizedLock` for concurrent access, with
> stripe count based on `Environment.ProcessorCount`. Key hash (via
> `unifiedBasicContext.GetKeyHash(key)`) selects the stripe.
> `ReadRangeIndex()` acquires a shared lock; `ReadOrCreateRangeIndex()` promotes to
> exclusive if the key doesn't exist, creates the BfTree, then downgrades to shared.
>
> **Note:** `ReadOptimizedLock` does not yet exist on the dev branch. It needs to be
> ported from the VectorSet prototype branch
> [`vectorApiPoC-storeV2`](https://github.com/microsoft/garnet/tree/vectorApiPoC-storeV2):
> - **Implementation:** [`libs/server/Resp/Vector/VectorManager.Locking.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Resp/Vector/VectorManager.Locking.cs)
>   (the `ReadOptimizedLock` struct and `ReadVectorLock`/`ExclusiveVectorLock` ref structs)
> - **Tests:** [`test/Garnet.test/ReadOptimizedLockTests.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/test/Garnet.test/ReadOptimizedLockTests.cs)
>
> It provides shared/exclusive locking with `AcquireSharedLock(keyHash, out token)`,
> `TryPromoteSharedLock(keyHash, sharedToken, out exclusiveToken)`,
> `ReleaseSharedLock(token)`, and `ReleaseExclusiveLock(token)` methods.

<details>
<summary>ReadRangeIndexLock and locking methods (click to expand)</summary>

```csharp
public sealed partial class RangeIndexManager
{
    private readonly ReadOptimizedLock rangeIndexLocks; // Striped lock (ProcessorCount stripes)

    /// RAII lock holder — disposed at end of `using` block
    internal readonly ref struct ReadRangeIndexLock : IDisposable
    {
        private readonly ref readonly ReadOptimizedLock lockRef;
        private readonly int lockToken;

        internal ReadRangeIndexLock(in ReadOptimizedLock lockRef, int token)
        {
            this.lockRef = ref lockRef;
            this.lockToken = token;
        }
        public void Dispose() => lockRef.ReleaseSharedLock(lockToken);
    }

    /// Acquire shared lock on an EXISTING range index.
    /// Returns NOTFOUND if key doesn't exist.
    /// If ProcessInstanceId mismatches (evicted index whose stub was cleared by the
    /// deserialization observer, or stale pointer after process restart), promotes to
    /// exclusive, restores BfTree from snapshot, releases exclusive, re-acquires shared.
    internal ReadRangeIndexLock ReadRangeIndex(
        StorageSession session, PinnedSpanByte key, ref StringInput input,
        Span<byte> indexSpan, out GarnetStatus status)
    {
        // 1. var keyHash = session.unifiedBasicContext.GetKeyHash(key);
        // 2. rangeIndexLocks.AcquireSharedLock(keyHash, out var sharedLockToken);
        // 3. var indexOutput = StringOutput.FromPinnedSpan(indexSpan);
        //    session.Read_MainStore(key.ReadOnlySpan, ref input, ref indexOutput, ref session.stringBasicContext);
        // 4. If not found: status = NOTFOUND, return default lock
        // 5. ReadIndex → extract TreePtr, ProcessInstanceId
        // 6. If ProcessInstanceId != this.processInstanceId:
        //      TryPromoteSharedLock → restore from snapshot → RMW update → release exclusive, retry
        // 7. Return ReadRangeIndexLock holding shared lock
    }

    /// Acquire shared lock, CREATE the range index if key doesn't exist.
    /// Used by RI.SET and RI.CREATE.
    internal ReadRangeIndexLock ReadOrCreateRangeIndex(
        StorageSession session, PinnedSpanByte key, ref StringInput input,
        Span<byte> indexSpan, out GarnetStatus status)
    {
        // 1. Same as ReadRangeIndex
        // 2. If not found: TryPromoteSharedLock to exclusive
        // 3. Create BfTree: var treePtr = service.CreateIndex(config...)
        // 4. Inject treePtr into input.parseState (arg slot for InitialUpdater)
        // 5. Issue RMW_MainStore → hits InitialUpdater which writes stub to logRecord
        // 6. Release exclusive lock, re-acquire shared
        // 7. Read the stub into indexSpan
        // 8. Return ReadRangeIndexLock holding shared lock
    }
}
```

</details>

---

### Step 8: Wire into Main Store RMW callbacks

> **Reference:** `libs/server/Storage/Functions/MainStore/RMWMethods.cs`
> - `InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo)`
> - `InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo)`
> - `CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo)`
> Add new cases for `RespCommand.RICREATE`, `RISET`, and `RIDEL` in each method.
> Access command via `input.header.cmd`. Access/modify value via `logRecord.ValueSpan` / `logRecord.TrySetValueSpan(...)`.

#### InitialUpdater

<details>
<summary>InitialUpdater case (click to expand)</summary>

```csharp
case RespCommand.RICREATE:
case RespCommand.RISET:
{
    if (input.arg1 is RangeIndexManager.RISetAppendLogArg)
    {
        break; // Synthetic replication op, do nothing
    }

    // Extract config + treePtr from parseState
    // (injected by ReadOrCreateRangeIndex before issuing RMW)
    var cacheSize = MemoryMarshal.Read<uint>(
        input.parseState.GetArgSliceByRef(0).ReadOnlySpan);
    var minRecordSize = MemoryMarshal.Read<uint>(
        input.parseState.GetArgSliceByRef(1).ReadOnlySpan);
    var maxRecordSize = MemoryMarshal.Read<uint>(
        input.parseState.GetArgSliceByRef(2).ReadOnlySpan);
    var maxKeyLen = MemoryMarshal.Read<uint>(
        input.parseState.GetArgSliceByRef(3).ReadOnlySpan);
    var leafPageSize = MemoryMarshal.Read<uint>(
        input.parseState.GetArgSliceByRef(4).ReadOnlySpan);
    var storageBackend = input.parseState.GetArgSliceByRef(5).ReadOnlySpan[0];
    var treePtr = MemoryMarshal.Read<nint>(
        input.parseState.GetArgSliceByRef(6).ReadOnlySpan);

    // Set RecordType to identify this as a RangeIndex stub
    // logRecord.RecordType = RangeIndexManager.RangeIndexRecordType;
    functionsState.rangeIndexManager.CreateIndex(
        cacheSize, minRecordSize, maxRecordSize,
        maxKeyLen, leafPageSize, storageBackend,
        treePtr, logRecord.ValueSpan);
}
break;
```

</details>

#### InPlaceUpdater

<details>
<summary>InPlaceUpdater case (click to expand)</summary>

```csharp
case RespCommand.RISET:
case RespCommand.RIDEL:
case RespCommand.RICREATE:
    if (input.arg1 == RangeIndexManager.DeleteAfterDropArg)
    {
        logRecord.ValueSpan.Clear();
    }
    else if (input.arg1 == RangeIndexManager.RecreateIndexArg)
    {
        var newTreePtr = MemoryMarshal.Read<nint>(
            input.parseState.GetArgSliceByRef(6).ReadOnlySpan);
        functionsState.rangeIndexManager.RecreateIndex(
            newTreePtr, logRecord.ValueSpan);
    }
    // All other operations (insert/delete/scan) are handled OUTSIDE
    // of Tsavorite's RMW — they happen in the StorageSession layer
    // while holding the shared lock. The RMW path here is only for
    // stub lifecycle (create/recreate/drop).
    return true;
```

</details>

#### CopyUpdater

<details>
<summary>CopyUpdater case (click to expand)</summary>

```csharp
case RespCommand.RISET:
case RespCommand.RIDEL:
case RespCommand.RICREATE:
    if (input.arg1 == RangeIndexManager.DeleteAfterDropArg)
    {
        dstLogRecord.ValueSpan.Clear();
    }
    else if (input.arg1 == RangeIndexManager.RecreateIndexArg)
    {
        var newTreePtr = MemoryMarshal.Read<nint>(
            input.parseState.GetArgSliceByRef(6).ReadOnlySpan);
        srcLogRecord.ValueSpan.CopyTo(dstLogRecord.ValueSpan);
        functionsState.rangeIndexManager.RecreateIndex(
            newTreePtr, dstLogRecord.ValueSpan);
    }
    else
    {
        srcLogRecord.ValueSpan.CopyTo(dstLogRecord.ValueSpan);
    }
    break;
```

</details>

---

### Step 9: Wire into Main Store Read Methods

> **Reference:** `libs/server/Storage/Functions/MainStore/ReadMethods.cs`
> - `SingleReader()` — validates record type before allowing reads
> - `ConcurrentReader()` — same pattern for concurrent access
> Currently, `ReadMethods.cs` checks `ValueIsObject` to reject string commands on
> object records. Add analogous guards using the `RecordType` byte.

Add type-safety guards in both `SingleReader` and `ConcurrentReader`:

```csharp
// Add RangeIndex type-safety checks:
if (srcLogRecord.RecordType == RangeIndexManager.RangeIndexRecordType && !cmd.IsLegalOnRangeIndex())
{
    readInfo.Action = ReadAction.CancelOperation;
    return false;
}
else if (srcLogRecord.RecordType != RangeIndexManager.RangeIndexRecordType && cmd.IsLegalOnRangeIndex())
{
    readInfo.Action = ReadAction.CancelOperation;
    return false;
}
```

**Note:** Actual read operations (RI.GET, RI.SCAN, RI.RANGE) do NOT go through
Tsavorite's `Read()` path for data access. The storage session reads the stub via
`Read_MainStore()` only to get the `TreePtr`, then calls BfTree directly via
`RangeIndexManager.TryRead()` etc. The type-safety guards above prevent misuse
(e.g., `GET` on a RangeIndex key).

---

### Step 10: Delete/Drop handling

> **Reference:** `libs/server/Storage/Functions/MainStore/DeleteMethods.cs`
> Add a guard to prevent deletion of RangeIndex keys while the stub is non-zero
> (BfTree still alive). The `RI.DROP` command explicitly zeroes the stub before deleting.

```csharp
// In SingleDeleter and ConcurrentDeleter:
if (logRecord.RecordType == RangeIndexManager.RangeIndexRecordType
    && value.AsReadOnlySpan().ContainsAnyExcept((byte)0))
{
    // Stub not yet zeroed — BfTree still alive, block deletion
    deleteInfo.Action = DeleteAction.CancelOperation;
    return false;
}
```

**RI.DROP command flow:**

1. RESP handler `NetworkRIDROP` calls `storageApi.RangeIndexDrop(key)`
2. `StorageSession.RangeIndexDrop()`:
   a. `ReadRangeIndex()` → acquire shared lock, read stub
   b. `rangeIndexManager.DropIndex(indexSpan)` → calls `BfTreeService.Drop(treePtr)` to
      free the BfTree
   c. Issue RMW with `arg1 = DeleteAfterDropArg` → `InPlaceUpdater` zeroes the stub
   d. Issue `DEL` on the key → now succeeds because stub is all zeros
3. *(Future)* `CleanupDroppedIndex()` for any async cleanup if needed

---

### Step 11: Implement `BfTreeService` (native interop)

> **Prototype reference:** [`DiskANNService.cs`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Resp/Vector/DiskANNService.cs) —
> wraps the unmanaged DiskANN library. BfTreeService follows the same pattern with
> P/Invoke to a Rust shared library instead.

**File:** `libs/server/Resp/RangeIndex/BfTreeService.cs` (new)

<details>
<summary>BfTreeService implementation (click to expand)</summary>

```csharp
/// Wraps the native Bf-Tree library (bftree.dll / libbftree.so).
/// Provides managed C# interface for BfTree lifecycle and operations.
internal sealed class BfTreeService : IDisposable
{
    /// Create a new BfTree instance. Returns native pointer.
    internal nint CreateIndex(ulong cacheSize, uint minRecordSize,
        uint maxRecordSize, uint maxKeyLen, uint leafPageSize,
        byte storageBackend, string filePath)
    {
        fixed (byte* pathPtr = Encoding.UTF8.GetBytes(filePath))
            return NativeBfTreeMethods.bftree_create(
                cacheSize, minRecordSize, maxRecordSize,
                maxKeyLen, leafPageSize, storageBackend,
                pathPtr, filePath.Length);
    }

    /// Insert a key-value pair. Returns result code.
    internal BfTreeInsertResult Insert(nint tree,
        ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        fixed (byte* kp = key, vp = value)
            return (BfTreeInsertResult)NativeBfTreeMethods.bftree_insert(
                tree, kp, key.Length, vp, value.Length);
    }

    /// Point read. Writes value into output.
    internal RangeIndexResult Read(nint tree,
        ReadOnlySpan<byte> key, uint maxRecordSize,
        ref SpanByteAndMemory output)
    {
        Span<byte> buffer = stackalloc byte[(int)maxRecordSize];
        fixed (byte* kp = key, bp = buffer)
        {
            var result = NativeBfTreeMethods.bftree_read(
                tree, kp, key.Length, bp, buffer.Length);
            if (result < 0) return RangeIndexResult.NotFound;
            // Copy result into output SpanByteAndMemory
            // ...
            return RangeIndexResult.OK;
        }
    }

    /// Delete a key.
    internal void Delete(nint tree, ReadOnlySpan<byte> key)
    {
        fixed (byte* kp = key)
            NativeBfTreeMethods.bftree_delete(tree, kp, key.Length);
    }

    /// Scan with count. Iterates and writes RESP-formatted results into output.
    internal RangeIndexResult ScanWithCount(nint tree,
        ReadOnlySpan<byte> startKey, int count, byte returnField,
        ref SpanByteAndMemory output, out int resultCount)
    {
        // 1. Call bftree_scan_with_count → get iterator pointer
        // 2. Loop: bftree_scan_next → append to output
        // 3. bftree_scan_drop → free iterator
        // returnField: 0=Key, 1=Value, 2=KeyAndValue (maps to ScanReturnField enum)
    }

    /// Scan with end key. Same pattern as ScanWithCount.
    internal RangeIndexResult ScanWithEndKey(nint tree,
        ReadOnlySpan<byte> startKey, ReadOnlySpan<byte> endKey,
        byte returnField,
        ref SpanByteAndMemory output, out int resultCount)
    { /* same iterator pattern */ }

    /// Take a snapshot. Returns snapshot file path.
    internal string Snapshot(nint tree)
    {
        var pathPtr = NativeBfTreeMethods.bftree_snapshot(tree);
        // Convert native string to managed
    }

    /// Restore from snapshot. Returns new tree pointer.
    internal nint RestoreFromSnapshot(string snapshotPath,
        ulong cacheSize, uint minRecordSize, uint maxRecordSize,
        uint maxKeyLen, uint leafPageSize)
    {
        fixed (byte* pp = Encoding.UTF8.GetBytes(snapshotPath))
            return NativeBfTreeMethods.bftree_new_from_snapshot(
                pp, snapshotPath.Length,
                cacheSize, minRecordSize, maxRecordSize,
                maxKeyLen, leafPageSize);
    }

    /// Drop/free a BfTree instance.
    internal void Drop(nint tree)
        => NativeBfTreeMethods.bftree_drop(tree);

    public void Dispose() { /* cleanup any global state */ }
}
```

</details>

<details>
<summary>P/Invoke declarations (click to expand)</summary>

```csharp
/// P/Invoke declarations for the native Bf-Tree library.
internal static class NativeBfTreeMethods
{
    private const string LibName = "bftree";

    [DllImport(LibName)] internal static extern nint bftree_create(
        ulong cacheSize, uint minRecord, uint maxRecord,
        uint maxKeyLen, uint leafPageSize, byte storageBackend,
        byte* filePath, int filePathLen);

    [DllImport(LibName)] internal static extern int bftree_insert(
        nint tree, byte* key, int keyLen, byte* value, int valueLen);

    [DllImport(LibName)] internal static extern int bftree_read(
        nint tree, byte* key, int keyLen, byte* outBuffer, int outBufferLen);

    [DllImport(LibName)] internal static extern void bftree_delete(
        nint tree, byte* key, int keyLen);

    [DllImport(LibName)] internal static extern nint bftree_scan_with_count(
        nint tree, byte* startKey, int startKeyLen, int count, byte returnField);

    [DllImport(LibName)] internal static extern nint bftree_scan_with_end_key(
        nint tree, byte* startKey, int startKeyLen,
        byte* endKey, int endKeyLen, byte returnField);

    [DllImport(LibName)] internal static extern int bftree_scan_next(
        nint iter, byte* outBuffer, int outBufferLen,
        out int keyLen, out int valueLen);

    [DllImport(LibName)] internal static extern void bftree_scan_drop(nint iter);

    [DllImport(LibName)] internal static extern nint bftree_snapshot(nint tree);

    [DllImport(LibName)] internal static extern nint bftree_new_from_snapshot(
        byte* configPath, int configPathLen,
        ulong cacheSize, uint minRecord, uint maxRecord,
        uint maxKeyLen, uint leafPageSize);

    [DllImport(LibName)] internal static extern void bftree_drop(nint tree);
}

/// Result codes from BfTree insert operations
internal enum BfTreeInsertResult
{
    Success = 0,
    InvalidKV = 1,
}
```

</details>

**Rust FFI side** (`bf-tree/src/ffi.rs`, new file in the bf-tree crate):

<details>
<summary>Rust FFI exports (click to expand)</summary>

```rust
use crate::{BfTree, Config, LeafInsertResult, LeafReadResult, ScanReturnField, ScanIter};
use std::ffi::c_char;
use std::slice;

#[no_mangle]
pub extern "C" fn bftree_create(
    cache_size: u64, min_record: u32, max_record: u32,
    max_key_len: u32, leaf_page_size: u32, storage_backend: u8,
    file_path: *const u8, file_path_len: i32,
) -> *mut BfTree {
    let mut config = Config::default();
    config.cb_size_byte(cache_size as usize);
    config.cb_min_record_size(min_record as usize);
    config.cb_max_record_size(max_record as usize);
    config.cb_max_key_len(max_key_len as usize);
    config.leaf_page_size(leaf_page_size as usize);
    // ... set storage backend, file path
    let tree = BfTree::with_config(config, None).unwrap();
    Box::into_raw(Box::new(tree))
}

#[no_mangle]
pub extern "C" fn bftree_insert(
    tree: *mut BfTree, key: *const u8, key_len: i32,
    value: *const u8, value_len: i32,
) -> i32 {
    let tree = unsafe { &*tree };
    let key = unsafe { slice::from_raw_parts(key, key_len as usize) };
    let value = unsafe { slice::from_raw_parts(value, value_len as usize) };
    match tree.insert(key, value) {
        LeafInsertResult::Success => 0,
        LeafInsertResult::InvalidKV(_) => 1,
    }
}

#[no_mangle]
pub extern "C" fn bftree_read(
    tree: *mut BfTree, key: *const u8, key_len: i32,
    out_buffer: *mut u8, out_buffer_len: i32,
) -> i32 {
    let tree = unsafe { &*tree };
    let key = unsafe { slice::from_raw_parts(key, key_len as usize) };
    let buffer = unsafe { slice::from_raw_parts_mut(out_buffer, out_buffer_len as usize) };
    match tree.read(key, buffer) {
        LeafReadResult::Found(n) => n as i32,
        LeafReadResult::NotFound => -1,
        LeafReadResult::Deleted => -2,
        LeafReadResult::InvalidKey => -3,
    }
}

#[no_mangle]
pub extern "C" fn bftree_delete(
    tree: *mut BfTree, key: *const u8, key_len: i32,
) {
    let tree = unsafe { &*tree };
    let key = unsafe { slice::from_raw_parts(key, key_len as usize) };
    tree.delete(key);
}

#[no_mangle]
pub extern "C" fn bftree_drop(tree: *mut BfTree) {
    unsafe { drop(Box::from_raw(tree)); }
}

// ... scan_with_count, scan_with_end_key, scan_next, scan_drop, snapshot,
// new_from_snapshot follow similar patterns
```

</details>

---

### Step 12: Add `RangeIndexManager` to `FunctionsState`

> **Reference:** `libs/server/Storage/Functions/FunctionsState.cs`
> This class holds shared state accessible from Tsavorite session function callbacks
> (e.g., `appendOnlyFile`, `watchVersionMap`, `objectStoreSizeTracker`).
>
> **Prototype reference:** On `vectorApiPoC-storeV2`, `FunctionsState` has a
> [`vectorManager` field](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Storage/Functions/FunctionsState.cs)
> and [`StorageSession`](https://github.com/microsoft/garnet/blob/vectorApiPoC-storeV2/libs/server/Storage/Session/StorageSession.cs)
> has a matching `vectorManager` field — follow the same pattern for `rangeIndexManager`.

```csharp
// Add field:
internal readonly RangeIndexManager rangeIndexManager;

// Initialize in constructor:
this.rangeIndexManager = new RangeIndexManager(logger);
```

Also add `rangeIndexManager` to `StorageSession` so that `RangeIndexOps.cs` can access it:

> **Reference:** `libs/server/Storage/Session/StorageSession.cs`
> The session holds context fields (`stringBasicContext`, `objectBasicContext`,
> `unifiedBasicContext`) and shared state. Add the manager as a field.

```csharp
internal readonly RangeIndexManager rangeIndexManager;
```

---

### Step 13: Checkpoint & Recovery

> The stub in the store is automatically serialized by Tsavorite during checkpoint
> (as part of the hybrid log). On recovery, BfTrees are **not** proactively recreated.
> Instead, `RangeIndexManager` generates a fresh `processInstanceId` at startup, and
> `ReadRangeIndex()` lazily detects stale stubs (via `ProcessInstanceId` mismatch) on
> first access, restoring from snapshot at that point. This matches how VectorSet
> handles recovery on the prototype branch.

**Checkpoint:** Before Garnet checkpoint, call `RangeIndexManager.PrepareCheckpoint()`:

```csharp
internal void PrepareCheckpoint()
{
    // Snapshot all active BfTrees to disk.
    // The stub's config fields + deterministic path allow reconstruction.
    // TreePtr becomes stale but ProcessInstanceId enables detection.
}
```

**Recovery:** No proactive store scan needed. After Tsavorite recovery, the stubs contain
`ProcessInstanceId` values from the prior process. Since `RangeIndexManager` generates a
fresh `Guid` at startup, every stub will mismatch on first access. The existing
`ReadRangeIndex()` flow handles this lazily:

1. First `RI.*` command on a recovered key → `Read_MainStore` returns stub from store
2. `ProcessInstanceId != this.processInstanceId` → stale
3. Promote to exclusive lock → restore BfTree from snapshot → update stub via RMW
4. Proceed with the requested operation

This avoids a potentially expensive full-store scan at startup, and only pays the
restore cost for indexes that are actually accessed.

---

## Result Enum

```csharp
public enum RangeIndexResult
{
    OK,
    NotFound,
    Deleted,
    InvalidKey,
    Error,
}
```

---

## Complete File Inventory

### New Files (10 files)

| # | File Path | Purpose | Lines (est.) |
|---|---|---|---|
| 1 | `libs/server/Resp/RangeIndex/RangeIndexManager.cs` | Main class: constants, Try* methods, recovery | ~200 |
| 2 | `libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs` | Stub struct, Create/Read/Recreate/DropIndex | ~120 |
| 3 | `libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs` | ReadRangeIndexLock, ReadRangeIndex, ReadOrCreateRangeIndex | ~250 |
| 4 | `libs/server/Resp/RangeIndex/RangeIndexManager.Cleanup.cs` | Post-drop async cleanup | ~100 |
| 5 | `libs/server/Resp/RangeIndex/BfTreeService.cs` | High-level BfTree operations wrapper | ~150 |
| 6 | `libs/server/Resp/RangeIndex/NativeBfTreeMethods.cs` | P/Invoke declarations | ~60 |
| 7 | `libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs` | RESP command handlers | ~400 |
| 8 | `libs/server/Storage/Session/MainStore/RangeIndexOps.cs` | Storage session wrappers | ~200 |
| 9 | `bf-tree/src/ffi.rs` | Rust C FFI exports | ~200 |
| 10 | `test/Garnet.test/RangeIndexTests.cs` | Integration tests | ~300 |

### Modified Files (9 files)

| # | File Path | Change | Scope |
|---|---|---|---|
| 1 | `libs/storage/Tsavorite/.../LogRecord.cs` | Wire `RecordType` propagation through `InitializeRecord()` | ~15 lines |
| 2 | `libs/server/Resp/Parser/RespCommand.cs` | Add RI* enum values + `ParseRangeIndexCommand()` | ~50 lines |
| 3 | `libs/server/Resp/RespServerSession.cs` | Add RI* command dispatch entries | ~15 lines |
| 4 | `libs/server/Storage/Functions/MainStore/RMWMethods.cs` | Add RICREATE/RISET/RIDEL cases in InitialUpdater, InPlaceUpdater, CopyUpdater | ~60 lines |
| 5 | `libs/server/Storage/Functions/MainStore/ReadMethods.cs` | Add `RecordType` type guards in Reader | ~20 lines |
| 6 | `libs/server/Storage/Functions/MainStore/DeleteMethods.cs` | Add `RecordType` deletion guard | ~10 lines |
| 7 | `libs/server/Storage/Functions/FunctionsState.cs` | Add `rangeIndexManager` field | ~5 lines |
| 8 | `libs/server/Storage/Session/StorageSession.cs` | Add `rangeIndexManager` field | ~5 lines |
| 9 | `libs/server/API/IGarnetApi.cs` + `GarnetApi.cs` | Add RangeIndex* method declarations + delegation | ~60 lines |

---

## Persistence, Checkpoint, Migration, and Replication

This section covers how BfTree data survives page flush, checkpoint, recovery, replica
sync, and key migration. The **critical design consideration** is that BfTree stores its
data entirely **outside** Tsavorite (in its own circular buffer + disk files). The stub in
Tsavorite is just a pointer + config — the actual data must be managed separately at every
persistence boundary. This is fundamentally different from built-in object types (Hash,
List, Set, SortedSet) which serialize their data directly into Tsavorite's log and benefit
from automatic checkpoint/migration handling.

### Background: How Tsavorite Persists Records

> **Reference:** `libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs`

- Tsavorite writes hybrid log pages **verbatim to disk** — raw memory bytes, no per-record
  transformation or callback.
- `RecordInfo` flags and `RecordType` byte are part of the record header
  and survive flush as-is.
- The `TreePtr` field in the stub is a raw `nint` — it is written to disk as a raw
  64-bit value. After the BfTree is evicted and freed (see Page Flush below), this
  pointer becomes stale. On re-read from disk, `ProcessInstanceId` mismatch (set to a
  sentinel during eviction) triggers `RecreateIndex()` to restore the BfTree from its
  snapshot.
- There is an **observer pattern** via `store.Log.SubscribeEvictions()` that fires per-page,
  which we use to snapshot and free BfTrees whose stubs are being evicted.

### The Four Persistence Boundaries

| Boundary | What happens | RangeIndex action required |
|---|---|---|
| **Page flush** | Tsavorite evicts a hybrid log page from memory to disk. Stub is written as raw bytes. | Snapshot the BfTree to disk, free the native instance, and update `ProcessInstanceId` to a sentinel so the stale `TreePtr` is detected on re-read. Without this, cold BfTree instances leak native memory indefinitely. |
| **Checkpoint** | Tsavorite takes a full or incremental checkpoint of the hybrid log. All stubs are included. | Snapshot ALL active BfTrees before checkpoint begins. |
| **Recovery** | Tsavorite recovers from checkpoint. Stubs are loaded with stale `TreePtr` values. | Scan for `RangeIndexRecordType` records, restore each BfTree from its snapshot, update `TreePtr`. |
| **Key migration** | Individual keys are transferred to another node during cluster slot migration. | Serialize the BfTree (full snapshot bytes) alongside the stub when transmitting. Receiver recreates the BfTree from the serialized bytes. |
| **Replica sync** | Full checkpoint is sent to a replica. | Send BfTree snapshot files alongside the Tsavorite checkpoint files. Replica recovery restores BfTrees from those files. |

### Design: Snapshot File Management

Each BfTree instance has a **deterministic snapshot file path** derived from its Garnet
key name and a checkpoint-specific directory:

```
{garnet_checkpoint_dir}/rangeindex/{key_hash}/{checkpoint_token}.bftree
```

The `RangeIndexManager` maintains a registry mapping live `TreePtr` values to their key
names and snapshot paths:

```csharp
// In RangeIndexManager.cs
private readonly ConcurrentDictionary<nint, RangeIndexEntry> activeIndexes = new();

private struct RangeIndexEntry
{
    public byte[] KeyBytes;        // Garnet key name
    public string SnapshotPath;    // Last snapshot file path
    public ulong CacheSize;        // Config for restoration
    public uint MinRecordSize;
    public uint MaxRecordSize;
    public uint MaxKeyLen;
    public uint LeafPageSize;
    public byte StorageBackend;
}
```

When a BfTree is created (in `CreateIndex`), it is registered. When dropped, it is
unregistered.

### Design: Deterministic Snapshot Path Derivation

Recovery locates the correct BfTree snapshot file for a given stub using a **deterministic
path convention** — the same function is used at checkpoint time (to write the file) and at
recovery time (to locate it). No explicit file path is stored in the stub.

The three inputs to the derivation are:

1. **Garnet key bytes** — always available from the deserialized Tsavorite KV record.
2. **Checkpoint directory** — known from Garnet's server configuration.
3. **Checkpoint token** (`Guid`) — known from Tsavorite's recovery metadata
   (`store.RecoveredToken` after `RecoverCheckpoint()`).

The key bytes are hashed to a 128-bit value (formatted as a `Guid`) to produce a
fixed-length, filesystem-safe folder name. A 128-bit hash has negligible collision
probability (~50% at 2⁶⁴ keys), far beyond any realistic number of RangeIndexes.

```csharp
// In RangeIndexManager.Persistence.cs
internal static string DeriveSnapshotPath(
    ReadOnlySpan<byte> keyBytes, string checkpointDir, Guid checkpointToken)
{
    // Hash key bytes to a 128-bit Guid for a fixed-length, collision-resistant folder name.
    var keyHash = HashKeyToGuid(keyBytes);
    return Path.Combine(checkpointDir, "rangeindex", keyHash.ToString("N"),
        $"{checkpointToken}.bftree");
}

// Overload for page-flush eviction (uses a persistent snapshot directory, no token)
internal static string DeriveSnapshotPath(
    ReadOnlySpan<byte> keyBytes, string baseSnapshotDir)
{
    var keyHash = HashKeyToGuid(keyBytes);
    return Path.Combine(baseSnapshotDir, "rangeindex", keyHash.ToString("N"),
        "latest.bftree");
}

// 128-bit hash of key bytes, formatted as a Guid.
private static Guid HashKeyToGuid(ReadOnlySpan<byte> keyBytes)
{
    Span<byte> hash = stackalloc byte[16];
    System.IO.Hashing.XxHash128.Hash(keyBytes, hash);
    return new Guid(hash);
}
```

---

### A. Page Flush (Hybrid Log Eviction)

> **Reference:**
> - `libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs` — `EvictPage()`
>   (line 1262) and `OnPagesClosedWorker()` (line 1330).
> - `libs/storage/Tsavorite/cs/src/core/Index/StoreFunctions/IRecordDisposer.cs` —
>   `DisposeOnPageEviction` property and `DisposeValueObject(IHeapObject, DisposeReason)`.
> - `libs/storage/Tsavorite/cs/src/core/Index/StoreFunctions/DisposeReason.cs` —
>   `DisposeReason.PageEviction` already defined.
> - `libs/storage/Tsavorite/cs/src/core/Allocator/ObjectAllocatorImpl.cs` —
>   `DisposeRecord(ref LogRecord, DisposeReason)` (line 240) handles per-record disposal.
> - `ClearBitsForDiskImages()` in `RecordInfo.cs` — called when records are loaded from
>   disk (recovery, delta log apply, scan). See `Recovery.cs:1252`,
>   `AllocatorScan.cs:172`, `AllocatorBase.cs:473`.

**Problem:** When Tsavorite evicts a page containing a RangeIndex stub to disk, the
BfTree instance remains in native memory. If the index is cold and never accessed again,
this native memory is leaked indefinitely. We must reclaim it.

**Solution — `DisposeRecord` on eviction + `OnLoadFromDisk` on deserialization:**

**1. Eviction via `DisposeRecord` with `DisposeReason.PageEviction`:** Tsavorite already
has `DisposeRecord(ref LogRecord, DisposeReason)` and `DisposeReason.PageEviction`.
Currently, `EvictPage()` defers to `OnEvictionObserver` instead of calling
`DisposeRecord` per-record (see the TODO at `AllocatorBase.cs:1270`). The fix is to
wire `DisposeRecord` into the eviction path so it is called per-record with
`DisposeReason.PageEviction`.

The existing `DisposeValueObject(IHeapObject, DisposeReason)` on `IRecordDisposer`
only handles heap objects. For RangeIndex (which stores inline bytes, not heap objects),
`DisposeRecord(ref LogRecord, DisposeReason)` is the right hook — it provides access
to the full record including `RecordType` and inline value bytes.

In the Garnet-level `DisposeRecord` implementation (or a new `IRecordDisposer` that
replaces `DefaultRecordDisposer`), check `RecordType` and clean up the BfTree:

<details>
<summary>Eviction observer implementation (click to expand)</summary>

```csharp
// In Garnet's record disposer (handling DisposeReason.PageEviction):
public void DisposeRecord(ref LogRecord logRecord, DisposeReason reason)
{
    if (reason != DisposeReason.PageEviction)
        return;
    if (logRecord.RecordType != RangeIndexManager.RangeIndexRecordType)
        return;

    ReadIndex(logRecord.ValueSpan, out var treePtr, ...);
    if (treePtr == nint.Zero) return;

    if (rangeIndexManager.activeIndexes.TryRemove(treePtr, out var entry))
    {
        // Snapshot BfTree to disk, then free native instance
        var snapshotPath = DeriveSnapshotPath(entry.KeyBytes,
            rangeIndexManager.latestSnapshotDir);
        rangeIndexManager.service.Snapshot(treePtr, snapshotPath);
        rangeIndexManager.service.Drop(treePtr);
    }
}
```

</details>

> **Tsavorite change required:** Wire `DisposeRecord(ref LogRecord, DisposeReason.PageEviction)`
> into the eviction path (`EvictPage()` / `OnPagesClosedWorker()`), iterating records on
> the evicted page and calling `DisposeRecord` per-record. This resolves the existing
> TODO at `AllocatorBase.cs:1270`.

**2. `OnLoadFromDisk` callback** (new `ISessionFunctions` method): Invoked per-record
when a record is loaded from disk into memory, at the same call site as
`ClearBitsForDiskImages()`. Gives session functions an opportunity to inspect and modify
the record value.

For RangeIndex, `MainSessionFunctions.OnLoadFromDisk` clears the `ProcessInstanceId`
so that `ReadRangeIndex()` detects the stub as stale and triggers restoration:

<details>
<summary>OnLoadFromDisk callback (click to expand)</summary>

```csharp
// In MainSessionFunctions (new ISessionFunctions callback):
public void OnLoadFromDisk(ref LogRecord logRecord)
{
    if (logRecord.RecordType != RangeIndexManager.RangeIndexRecordType)
        return;

    // Clear ProcessInstanceId so ReadRangeIndex detects it as stale
    ref var stub = ref Unsafe.As<byte, RangeIndexStub>(
        ref MemoryMarshal.GetReference(logRecord.ValueSpan));
    stub.ProcessInstanceId = Guid.Empty;
    stub.TreePtr = nint.Zero;
}
```

</details>

> **Tsavorite change required:** Add `OnLoadFromDisk(ref LogRecord logRecord)` to
> `ISessionFunctions` and call it per-record alongside `ClearBitsForDiskImages()` in
> `Recovery.cs`, `AllocatorScan.cs`, and `AllocatorBase.cs` (delta log apply).
> Default implementation is a no-op.

**Hot-path cost:** For active indexes (the common case), the `ProcessInstanceId` matches
`this.processInstanceId` immediately — a single 16-byte comparison. No registry lookup.
The callbacks only fire on cold paths (page eviction / page load from disk).

**On cold read (ReadRangeIndex flow):**
1. Tsavorite loads record from disk → `OnLoadFromDisk` clears `ProcessInstanceId`
2. `ReadRangeIndex()` reads stub → `ProcessInstanceId == Guid.Empty` ≠ `this.processInstanceId`
3. Promote to exclusive lock
4. Derive snapshot path from key bytes + latest snapshot directory
5. Restore BfTree: `newTreePtr = service.RestoreFromSnapshot(snapshotPath, config...)`
6. Register `newTreePtr` in `activeIndexes`
7. Issue RMW with `RecreateIndexArg` to update the stub's `TreePtr` and `ProcessInstanceId`
8. Release exclusive lock, re-acquire shared, return

---

### B. Checkpoint

> **Reference:** `libs/server/Databases/DatabaseManagerBase.cs` —
> `InitiateCheckpointAsync()` orchestrates the checkpoint state machine.
> The `RangeIndexManager.PrepareCheckpoint()` hook should be called before the
> state machine starts, to snapshot all active BfTrees.

**Pre-checkpoint hook:** Before Tsavorite begins the checkpoint state machine, snapshot
all active BfTrees:

<details>
<summary>PrepareCheckpoint implementation (click to expand)</summary>

```csharp
// Hook into IClusterProvider.OnCheckpointInitiated()
// or into DatabaseManagerBase.InitiateCheckpointAsync() before the state machine runs.

// In RangeIndexManager.cs:
internal void PrepareCheckpoint(Guid checkpointToken, string checkpointDir)
{
    foreach (var (treePtr, entry) in activeIndexes)
    {
        // Derive path: {checkpointDir}/rangeindex/{keyHash}/{checkpointToken}.bftree
        var snapshotPath = DeriveSnapshotPath(entry.KeyBytes, checkpointDir, checkpointToken);
        service.Snapshot(treePtr, snapshotPath);
        entry.SnapshotPath = snapshotPath;
    }
    // The snapshot files now sit alongside the Tsavorite checkpoint files.
    // When checkpoint completes, these files are part of the checkpoint "bundle."
}
```

</details>

**Where to hook this in:**

```csharp
// In DatabaseManagerBase.cs, inside InitiateCheckpointAsync():
// BEFORE the state machine runs (line ~670):
rangeIndexManager.PrepareCheckpoint(checkpointToken, checkpointDir);
// ... then start the state machine:
db.StateMachineDriver.RunAsync();
```

**After checkpoint:** Optionally clean up old snapshot files from previous checkpoints.

---

### C. Recovery

> **Reference:** `libs/server/Databases/SingleDatabaseManager.cs` — `RecoverCheckpoint()`
> restores the store from a checkpoint.

**No proactive store scan.** After Tsavorite recovery, all stubs are loaded with stale
`ProcessInstanceId` values (from the prior process). `RangeIndexManager` generates a fresh
`processInstanceId = Guid.NewGuid()` at construction, so every stub will mismatch.

BfTrees are restored **lazily** on first access via the existing `ReadRangeIndex()` flow:

1. First `RI.*` command on a recovered key → `Read_MainStore` returns the stub
2. `stub.ProcessInstanceId != this.processInstanceId` → stale pointer detected
3. Promote shared lock to exclusive
4. Derive snapshot path from key bytes + checkpoint directory + checkpoint token
5. `newTreePtr = service.RestoreFromSnapshot(snapshotPath, config...)`
6. Register in `activeIndexes`
7. Issue RMW with `RecreateIndexArg` → updates `TreePtr` and `ProcessInstanceId` in stub
8. Release exclusive, re-acquire shared, proceed with operation

This avoids a full-store scan at startup. Only indexes that are actually accessed pay
the restore cost. This is the same approach used by VectorManager on the prototype branch
(where `ResumePostRecovery()` is a no-op TODO).

---

### D. Replica Sync (Sending Checkpoint to Replica)

> **Reference:** `libs/cluster/Server/Replication/PrimaryOps/ReplicaSyncSession.cs` —
> `SendCheckpoint()` sends checkpoint files categorized by `CheckpointFileType` enum.
> `libs/cluster/Server/Replication/CheckpointFileType.cs` — currently defines types for
> `STORE_HLOG`, `STORE_HLOG_OBJ`, `STORE_DLOG`, `STORE_INDEX`, `STORE_SNAPSHOT`,
> `STORE_SNAPSHOT_OBJ`. A new `RANGEINDEX_SNAPSHOT` type must be added.

**Problem:** The Tsavorite checkpoint files contain stubs with stale `TreePtr` values. The
actual BfTree data is in separate snapshot files that are not part of Tsavorite's file set.
When sending a checkpoint to a replica, we must send these additional files.

**Solution:**

#### 1. Add a new `CheckpointFileType`

```csharp
// In CheckpointFileType.cs, add:
RANGEINDEX_SNAPSHOT = 11,  // BfTree snapshot file
```

#### 2. Extend `SendCheckpoint()` to send BfTree snapshots

```csharp
// In ReplicaSyncSession.cs, inside SendCheckpoint(), after sending store files:

// Send RangeIndex snapshot files
var riSnapshotDir = Path.Combine(checkpointDir, "rangeindex");
if (Directory.Exists(riSnapshotDir))
{
    foreach (var snapshotFile in Directory.EnumerateFiles(
        riSnapshotDir, "*.bftree", SearchOption.AllDirectories))
    {
        // Derive a unique segment ID from the file path
        var relativePath = Path.GetRelativePath(checkpointDir, snapshotFile);
        SendFileByPath(snapshotFile, CheckpointFileType.RANGEINDEX_SNAPSHOT,
            relativePath, fileToken);
    }
}
```

#### 3. Extend the replica receiver to handle BfTree snapshots

> **Reference:** `libs/cluster/Session/RespClusterMigrateCommands.cs` — processes
> incoming checkpoint file segments.

```csharp
// On the replica side, when receiving RANGEINDEX_SNAPSHOT file type:
case CheckpointFileType.RANGEINDEX_SNAPSHOT:
    // Write snapshot file to local checkpoint directory
    var localPath = Path.Combine(localCheckpointDir, "rangeindex", relativePath);
    Directory.CreateDirectory(Path.GetDirectoryName(localPath));
    WriteSegmentToFile(localPath, segment);
    break;
```

After all files are received and Tsavorite recovery completes, the replica's
`RangeIndexManager` has a fresh `processInstanceId`. BfTrees are restored lazily on
first access — `ReadRangeIndex()` detects the `ProcessInstanceId` mismatch in each stub
and restores from the snapshot files at the expected paths.

---

### E. Key Migration (Cluster Slot Migration)

> **Reference:** `libs/cluster/Server/Migration/MigrateSessionKeys.cs` —
> migrates individual keys during cluster slot migration via `MigrateKeysFromStore()`.
> Key scanning: `MigrateScanFunctions.cs` — iterates store records for slot matching.
> Receiver: `libs/cluster/Session/RespClusterMigrateCommands.cs`.
> Currently, migration handles only standard string and object records. RangeIndex
> keys require special 2-phase migration since BfTree data lives outside Tsavorite.

**Problem:** During slot migration, individual keys are transferred to the target node.
For a RangeIndex key, we can't just send the 50-byte stub — we must also send the
entire BfTree data (all entries in the index). The target node must recreate the BfTree
from this data.

**Solution: 2-Phase Migration**

#### Phase 1: Serialize BfTree data

When the migration scanner encounters a RangeIndex record (identified by `RecordType`):

```csharp
// In MigrateScanFunctions.cs (add RangeIndex detection):
if (srcLogRecord.RecordType == RangeIndexManager.RangeIndexRecordType)
{
    mss.EncounteredRangeIndex(ref key, ref value);
    return; // Don't transmit the stub yet — Phase 2
}
```

```csharp
// In MigrateSessionKeys.cs (new method):
internal void EncounteredRangeIndex(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
{
    // Save key + value for Phase 2
    rangeIndexKeysToMigrate.Add((key.ToByteArray(), value.ToByteArray()));
}
```

#### Phase 2: Transmit BfTree snapshot + stub

```csharp
// In MigrateSessionKeys.cs, TransmitRangeIndexKeys() (new method):
internal bool TransmitRangeIndexKeys()
{
    foreach (var (keyBytes, stubBytes) in rangeIndexKeysToMigrate)
    {
        // 1. Read stub to get TreePtr
        RangeIndexManager.ReadIndex(stubBytes, out var treePtr, out var cacheSize,
            out var minRecordSize, out var maxRecordSize,
            out var maxKeyLen, out var leafPageSize,
            out var storageBackend, ...);

        // 2. Snapshot BfTree to a temporary file
        var tempSnapshotPath = Path.GetTempFileName();
        rangeIndexManager.SnapshotToPath(treePtr, tempSnapshotPath);

        // 3. Read snapshot file bytes
        var snapshotBytes = File.ReadAllBytes(tempSnapshotPath);

        // 4. Send to target: [stub (50 bytes)] + [snapshot_length (4 bytes)]
        //                   + [snapshot_bytes (N bytes)]
        var payload = new byte[stubBytes.Length + 4 + snapshotBytes.Length];
        Buffer.BlockCopy(stubBytes, 0, payload, 0, stubBytes.Length);
        BitConverter.TryWriteBytes(payload.AsSpan(stubBytes.Length), snapshotBytes.Length);
        Buffer.BlockCopy(snapshotBytes, 0, payload, stubBytes.Length + 4,
            snapshotBytes.Length);

        // 5. Transmit as a special store type "RISTORE"
        gcs.TryWriteRangeIndexMigration(keyBytes, payload);

        // 6. Cleanup temp file
        File.Delete(tempSnapshotPath);

        // 7. Delete local key (if not COPY)
        if (!isCopy) DeleteLocalKey(keyBytes);
    }
    return true;
}
```

#### Receiver side

```csharp
// In RespClusterMigrateCommands.cs, add case for RISTORE:
case "RISTORE":
    // 1. Parse payload: stub + snapshot bytes
    var stub = payload.AsSpan(0, RangeIndexManager.IndexSizeBytes);
    var snapshotLen = BitConverter.ToInt32(
        payload.AsSpan(RangeIndexManager.IndexSizeBytes));
    var snapshotBytes = payload.AsSpan(
        RangeIndexManager.IndexSizeBytes + 4, snapshotLen);

    // 2. Write snapshot to local file
    var localSnapshotPath = DeriveSnapshotPath(key);
    File.WriteAllBytes(localSnapshotPath, snapshotBytes.ToArray());

    // 3. Restore BfTree from snapshot
    RangeIndexManager.ReadIndex(stub, out _, out var cacheSize, ...);
    var newTreePtr = rangeIndexManager.RestoreFromSnapshot(
        localSnapshotPath, cacheSize, minRecordSize, maxRecordSize,
        maxKeyLen, leafPageSize);

    // 4. Build new stub with updated TreePtr + ProcessInstanceId
    var newStubBytes = new byte[RangeIndexManager.IndexSizeBytes];
    stub.CopyTo(newStubBytes);
    rangeIndexManager.UpdateStubPointer(newStubBytes, newTreePtr);

    // 5. Insert into local main store with RangeIndex RecordType
    InsertRangeIndexKey(keyBytes, newStubBytes);
    break;
```

---

### F. Summary: New FFI Functions Required

The BfTree Rust FFI layer needs additional functions for persistence:

```rust
/// Snapshot to a specific file path (not the default path).
#[no_mangle]
pub extern "C" fn bftree_snapshot_to_path(
    tree: *mut BfTree,
    path: *const u8, path_len: i32,
) -> i32;  // 0 = success, nonzero = error

/// Restore from a specific snapshot file path.
#[no_mangle]
pub extern "C" fn bftree_new_from_snapshot_path(
    path: *const u8, path_len: i32,
    cache_size: u64, min_record: u32, max_record: u32,
    max_key_len: u32, leaf_page_size: u32, storage_backend: u8,
) -> *mut BfTree;  // null on failure

/// Serialize BfTree to a byte buffer (for migration without temp files).
/// Returns the number of bytes written, or -1 if buffer too small.
#[no_mangle]
pub extern "C" fn bftree_serialize_to_buffer(
    tree: *mut BfTree,
    buffer: *mut u8, buffer_len: i32,
) -> i64;

/// Get the serialized size of a BfTree snapshot (for pre-allocating buffer).
#[no_mangle]
pub extern "C" fn bftree_serialized_size(tree: *mut BfTree) -> i64;

/// Restore from a byte buffer (received from migration).
#[no_mangle]
pub extern "C" fn bftree_deserialize_from_buffer(
    buffer: *const u8, buffer_len: i32,
    cache_size: u64, min_record: u32, max_record: u32,
    max_key_len: u32, leaf_page_size: u32, storage_backend: u8,
) -> *mut BfTree;  // null on failure
```

These buffer-based variants are useful for key migration where we want to avoid writing
temp files — the BfTree data can be serialized directly into the migration payload buffer.

---

### G. Updated File Inventory (Persistence-Related Additions)

| # | File Path | Purpose |
|---|---|---|
| NEW | `libs/server/Resp/RangeIndex/RangeIndexManager.Persistence.cs` | `DisposeRecord` handler, `PrepareCheckpoint()`, snapshot path derivation |
| NEW | `libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs` | `HandleMigratedRangeIndexKey()`, migration serialization/deserialization |
| MOD | `libs/server/Databases/DatabaseManagerBase.cs` | Call `rangeIndexManager.PrepareCheckpoint()` before checkpoint state machine |
| MOD | `libs/server/Databases/SingleDatabaseManager.cs` | Call `rangeIndexManager.PrepareCheckpoint()` before checkpoint |
| MOD | `libs/cluster/Server/Replication/CheckpointFileType.cs` | Add `RANGEINDEX_SNAPSHOT` enum value |
| MOD | `libs/cluster/Server/Replication/PrimaryOps/ReplicaSyncSession.cs` | Send BfTree snapshot files during replica sync |
| MOD | `libs/cluster/Session/RespClusterMigrateCommands.cs` | Handle `RISTORE` type during key migration |
| MOD | `libs/cluster/Server/Migration/MigrateSessionKeys.cs` | Detect RangeIndex `RecordType`, serialize BfTree, 2-phase transmit |
| MOD | `libs/cluster/Server/Migration/MigrateScanFunctions.cs` | Check `RecordType` during slot scan |
| MOD | `bf-tree/src/ffi.rs` | Add `bftree_snapshot_to_path`, `bftree_serialize_to_buffer`, `bftree_deserialize_from_buffer` |

---

### H. Persistence Testing Plan

1. **Page flush round-trip** — Insert data, force page eviction → BfTree freed; read cold key back → BfTree restored from snapshot, data intact
2. **Checkpoint + recovery** — Insert data, checkpoint, restart process, verify all RI.GET/RI.SCAN return correct results
3. **Multiple checkpoints** — Take multiple checkpoints, verify only latest snapshot files are used
4. **Replica sync** — Primary creates RangeIndex, inserts data, replica connects and receives checkpoint, verify RI.GET on replica returns correct data
5. **Key migration** — Create RangeIndex in slot N, migrate slot N to another node, verify RI.GET on target returns correct data, verify source no longer has the key
6. **Migration + concurrent writes** — Migrate a slot while writes are happening, verify no data loss
7. **Recovery with missing snapshot** — Delete snapshot file, attempt recovery, verify graceful error handling
8. **Large BfTree migration** — Create RangeIndex with 100K entries, migrate, verify correctness and timing

---

## Testing Plan

### Unit Tests

1. **Stub round-trip** — `CreateIndex` → `ReadIndex` → verify all fields
2. **RecreateIndex** — verify `TreePtr` and `ProcessInstanceId` update
3. **BfTreeService** — insert/read/delete/scan via P/Invoke against live BfTree
4. **Result codes** — verify `NotFound`, `Deleted`, `InvalidKey` mapping

### Integration Tests (RESP client → Garnet server)

1. **RI.CREATE + RI.SET + RI.GET** — basic CRUD
2. **RI.SET auto-create** — `RI.SET` on non-existent key creates the index
3. **RI.MSET + RI.MGET** — batch operations
4. **RI.SCAN** — scan with count, verify ordering and count limit
5. **RI.RANGE** — scan with end key, verify inclusive bounds
6. **RI.SCAN FIELDS KEY/VALUE/BOTH** — verify ScanReturnField behavior
7. **RI.DEL + RI.GET** — delete then read returns nil
8. **RI.DROP** — drop index, verify key is gone
9. **RI.EXISTS** — returns 1/0 correctly
10. **RI.CONFIG / RI.METRICS** — return valid data
11. **WRONGTYPE** — `GET` on RI key returns WRONGTYPE; `RI.SET` on string key returns WRONGTYPE
12. **Large scan** — insert 10K entries, scan all, verify ordering

### Checkpoint/Recovery Tests

1. Create index → insert data → checkpoint → restart → RI.GET verifies data
2. Multiple indexes → checkpoint → restart → all indexes restored
3. RI.SCAN after recovery returns same results as before checkpoint

### Concurrency Tests

1. Multiple threads doing RI.SET/RI.GET simultaneously (BfTree is thread-safe)
2. RI.SCAN concurrent with RI.SET (scan consistency)
3. RI.DROP during concurrent RI.SET (proper locking)
