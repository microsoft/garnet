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

RangeIndex is implemented as a **built-in Garnet type stored in the main store**, following
the identical "stub-in-main-store" architecture used by **Vector Indexes** (`VectorManager`).

A small fixed-size struct ("stub") is stored as the `SpanByte` value in Tsavorite's main
store. The stub contains BfTree configuration metadata, a native pointer (`nint`) to the
live BfTree instance, and a `Guid` process-instance-id for stale-pointer detection after
restart. A `RangeIndexManager` (partial class, analogous to `VectorManager`) owns the
BfTree lifecycle outside of Tsavorite.

**Why the main store and not the object store?**
- The Bf-Tree manages its own memory (circular buffer, leaf pages) and disk storage — it
  cannot be inlined into Tsavorite's log the way a `SortedSetObject` can.
- The stub pattern cleanly separates metadata persistence (Tsavorite checkpoint) from
  index operations (Bf-Tree).
- This is the same trade-off made for DiskANN vector indexes.

---

## Reference: VectorManager Architecture (the template)

The entire RangeIndex implementation mirrors VectorManager file-by-file. The table below
maps each VectorManager file to its RangeIndex counterpart and explains the role:

| VectorManager File | RangeIndex Counterpart | Role |
|---|---|---|
| `libs/server/Resp/Vector/VectorManager.cs` | `libs/server/Resp/RangeIndex/RangeIndexManager.cs` | Main class: constants, `processInstanceId`, `IsEnabled`, initialization, `TryAdd`/`TryRemove`/`TryRead`/`TryScan`/`TryRange` methods, `ResumePostRecovery()` |
| `libs/server/Resp/Vector/VectorManager.Index.cs` | `libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs` | Stub struct definition, `CreateIndex()`, `ReadIndex()`, `RecreateIndex()`, `DropIndex()` |
| `libs/server/Resp/Vector/VectorManager.Locking.cs` | `libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs` | `ReadRangeIndexLock` ref struct, `ReadRangeIndex()`, `ReadOrCreateRangeIndex()` — shared/exclusive lock management |
| `libs/server/Resp/Vector/VectorManager.Cleanup.cs` | `libs/server/Resp/RangeIndex/RangeIndexManager.Cleanup.cs` | Post-drop async cleanup: background task that scans and removes orphaned data |
| `libs/server/Resp/Vector/VectorManager.Callbacks.cs` | *(not needed initially)* | DiskANN uses callbacks so it can call back into Garnet storage. BfTree does not need this — all operations are initiated from Garnet's side. |
| `libs/server/Resp/Vector/VectorManager.Migration.cs` | `libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs` | *(future)* Replication/migration support |
| `libs/server/Resp/Vector/VectorManager.Replication.cs` | `libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs` | *(future)* Primary→replica replication |
| `libs/server/Resp/Vector/DiskANNService.cs` | `libs/server/Resp/RangeIndex/BfTreeService.cs` | Wraps native BfTree library via P/Invoke |
| `libs/server/Resp/Vector/RespServerSessionVectors.cs` | `libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs` | RESP command handlers (`NetworkRISET`, `NetworkRIGET`, etc.) |
| `libs/server/Storage/Session/MainStore/VectorStoreOps.cs` | `libs/server/Storage/Session/MainStore/RangeIndexOps.cs` | Storage session wrappers that acquire locks via manager and call `Try*` methods |
| `libs/server/API/IGarnetApi.cs` (vector methods) | `libs/server/API/IGarnetApi.cs` (range index methods) | Interface declarations |
| `libs/server/API/GarnetApi.cs` (vector delegation) | `libs/server/API/GarnetApi.cs` (range index delegation) | `ArgSlice` → `SpanByte` delegation to `StorageSession` |

---

## Architecture (data flow)

```
RESP Client ("RI.SET r1 mykey myval")
  │
  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. RESP Parser (Resp/Parser/RespCommand.cs)                            │
│    Tokenizes "RI.SET" → RespCommand.RISET enum value                  │
│    Read/write classification by enum position (reads < APPEND)         │
├─────────────────────────────────────────────────────────────────────────┤
│ 2. Command Dispatch (Resp/RespServerSession.cs)                        │
│    Switch on RespCommand → calls NetworkRISET<TGarnetApi>(ref api)     │
├─────────────────────────────────────────────────────────────────────────┤
│ 3. RESP Handler (Resp/RangeIndex/RespServerSessionRangeIndex.cs)       │
│    Parses args from parseState, calls storageApi.RangeIndexSet(...)    │
├─────────────────────────────────────────────────────────────────────────┤
│ 4. IGarnetApi / GarnetApi (API/IGarnetApi.cs, API/GarnetApi.cs)        │
│    Thin delegation: converts ArgSlice → SpanByte, forwards to          │
│    storageSession.RangeIndexSet(...)                                   │
├─────────────────────────────────────────────────────────────────────────┤
│ 5. Storage Session (Storage/Session/MainStore/RangeIndexOps.cs)        │
│    Acquires index lock via rangeIndexManager.ReadOrCreateRangeIndex()   │
│    Calls rangeIndexManager.TryInsert(indexSpan, field, value)          │
│    Replicates on success via rangeIndexManager.Replicate*(...)         │
├─────────────────────────────────────────────────────────────────────────┤
│ 6. RangeIndexManager (Resp/RangeIndex/RangeIndexManager.cs)            │
│    TryInsert: ReadIndex(stub) → extract TreePtr → BfTreeService.Insert │
├─────────────────────────────────────────────────────────────────────────┤
│ 7. BfTreeService / BfTreeInterop (Resp/RangeIndex/BfTreeService.cs)    │
│    P/Invoke call: bftree_insert(treePtr, key, keyLen, val, valLen)     │
├─────────────────────────────────────────────────────────────────────────┤
│ 8. Bf-Tree Rust library (bftree.dll / libbftree.so)                    │
│    BfTree::insert(key, value) → LeafInsertResult::Success              │
└─────────────────────────────────────────────────────────────────────────┘
```

**On first write (key doesn't exist yet):**
- Step 5 calls `ReadOrCreateRangeIndex()` which fails to find the key
- It promotes the shared lock to exclusive, creates a new BfTree via
  `BfTreeService.CreateIndex()`, and issues an RMW with the new stub
- The RMW hits `InitialUpdater` in `RMWMethods.cs` which writes the stub and
  sets `recordInfo.RangeIndexSet = true`
- Lock is released, then re-acquired as shared for the actual operation

---

## The Stub (RangeIndexManager.Index.cs)

A fixed-size struct stored as the SpanByte value in the main store.

> **Reference:** `VectorManager.Index` struct is defined in
> `libs/server/Resp/Vector/VectorManager.Index.cs` lines 22-45 (56 bytes,
> `[StructLayout(LayoutKind.Explicit, Size = 56)]`).

```csharp
[StructLayout(LayoutKind.Explicit, Size = 56)]
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
    [FieldOffset(34)] public ushort Reserved;
    [FieldOffset(36)] public uint Reserved2;             // Reserved (was SnapshotPathHash)
    [FieldOffset(40)] public Guid ProcessInstanceId;    // Detects stale pointers after restart
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

### Step 1: Add `RecordInfo.RangeIndexSet` bit flag

> **Reference:** `RecordInfo.VectorSet` property in
> `libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs`.
> Search for `VectorSet` to find the bit position and property pattern.

Add a new single-bit flag property. This enables:
- **ReadMethods.cs** — type safety: RI commands rejected on non-RI keys, non-RI commands
  rejected on RI keys (same pattern as `VectorSet` checks at lines 28-45 and 240-257)
- **DeleteMethods.cs** — blocking deletion while stub is non-zero (same as VectorSet
  guard at line 17-31)
- **RMWMethods.cs** — no guard needed; the command itself determines behavior

```csharp
// In RecordInfo.cs — add alongside VectorSet:
public bool RangeIndexSet
{
    readonly get => IsSet(RANGE_INDEX_SET_BIT);
    set => Set(RANGE_INDEX_SET_BIT, value);
}
```

Also add a helper in `RespCommand` extensions:

```csharp
// In RespCommandExtensions.cs — analogous to IsLegalOnVectorSet():
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
> - Read commands are defined before `APPEND` (lines ~96-117)
> - Write commands are defined after `APPEND` (lines ~210-228)
> - Read/write classification uses enum ordering: `cmd <= LastReadCommand` ⟹ read-only
> - Fast parsing: `FastParseArrayCommand()` (lines ~1006-1019) uses `ulong` pointer
>   comparisons for 4-char commands like `ZADD`
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
> Vector commands are dispatched in a switch expression like:
> `RespCommand.VADD => NetworkVADD(ref storageApi),`

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

> **Reference:** `libs/server/Resp/Vector/RespServerSessionVectors.cs`
> Pattern: `private bool NetworkVADD<TGarnetApi>(ref TGarnetApi storageApi) where TGarnetApi : IGarnetApi`
> - Parse arguments from `parseState.GetArgSliceByRef(idx)`
> - Call `storageApi.RangeIndex*(...)` with parsed args
> - Write RESP response via `RespWriteUtils.TryWrite*(..., ref dcurr, dend)`
> - Handle `GarnetStatus.OK`, `NOTFOUND`, `WRONGTYPE`

**File:** `libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs` (new)

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

---

### Step 5: Add `IGarnetApi` and `GarnetApi` interface methods

> **Reference:** `libs/server/API/IGarnetApi.cs` — vector methods declared here.
> `libs/server/API/GarnetApi.cs` — delegation: `ArgSlice` → `SpanByte.FromPinnedPointer()`
> then forward to `storageSession.RangeIndex*()`.

**IGarnetApi.cs — add:**

```csharp
// --- RangeIndex operations ---

GarnetStatus RangeIndexSet(ArgSlice key, ArgSlice field, ArgSlice value,
    out RangeIndexResult result, out ReadOnlySpan<byte> errorMsg);

GarnetStatus RangeIndexDel(ArgSlice key, ArgSlice field);

GarnetStatus RangeIndexGet(ArgSlice key, ArgSlice field,
    ref SpanByteAndMemory output, out RangeIndexResult result);

GarnetStatus RangeIndexScan(ArgSlice key, ArgSlice startKey,
    int count, byte returnField,
    ref SpanByteAndMemory output, out int resultCount);

GarnetStatus RangeIndexRange(ArgSlice key, ArgSlice startKey, ArgSlice endKey,
    byte returnField,
    ref SpanByteAndMemory output, out int resultCount);

GarnetStatus RangeIndexCreate(ArgSlice key,
    ulong cacheSize, uint minRecord, uint maxRecord,
    uint maxKeyLen, uint leafPageSize, byte storageBackend,
    out RangeIndexResult result, out ReadOnlySpan<byte> errorMsg);

GarnetStatus RangeIndexDrop(ArgSlice key);

GarnetStatus RangeIndexExists(ArgSlice key, out bool exists);

GarnetStatus RangeIndexConfig(ArgSlice key,
    ref SpanByteAndMemory output);

GarnetStatus RangeIndexMetrics(ArgSlice key,
    ref SpanByteAndMemory output);
```

**GarnetApi.cs — add delegation** (expression-bodied, like vector methods):

```csharp
public unsafe GarnetStatus RangeIndexSet(ArgSlice key, ArgSlice field, ArgSlice value,
    out RangeIndexResult result, out ReadOnlySpan<byte> errorMsg)
    => storageSession.RangeIndexSet(
        SpanByte.FromPinnedPointer(key.ptr, key.length),
        field, value, out result, out errorMsg);

public unsafe GarnetStatus RangeIndexGet(ArgSlice key, ArgSlice field,
    ref SpanByteAndMemory output, out RangeIndexResult result)
    => storageSession.RangeIndexGet(
        SpanByte.FromPinnedPointer(key.ptr, key.length),
        field, ref output, out result);

// ... same pattern for all methods
```

---

### Step 6: Implement Storage Session layer (`RangeIndexOps.cs`)

> **Reference:** `libs/server/Storage/Session/MainStore/VectorStoreOps.cs`
> - Write ops: marshal args → `ReadOrCreateRangeIndex()` → `TryInsert()` → replicate
> - Read ops: `ReadRangeIndex()` → `TryRead()` / `TryScan()` / `TryRange()`
> - Lock pattern: `using (rangeIndexManager.ReadOrCreateRangeIndex(this, ...))` — the
>   `using` block holds a shared lock via `ReadRangeIndexLock` ref struct
> - On first access, `ReadOrCreateRangeIndex` promotes to exclusive lock, creates BfTree
>   via `BfTreeService.CreateIndex()`, issues RMW to persist stub, releases exclusive,
>   re-acquires shared lock, then returns

**File:** `libs/server/Storage/Session/MainStore/RangeIndexOps.cs` (new)

```csharp
internal sealed unsafe partial class StorageSession
{
    /// RI.SET — insert/update a field in the range index
    public GarnetStatus RangeIndexSet(SpanByte key, ArgSlice field, ArgSlice value,
        out RangeIndexResult result, out ReadOnlySpan<byte> errorMsg)
    {
        errorMsg = default;

        // Marshal field + value into parseState for replication log
        parseState.InitializeWithArguments([
            ArgSlice.FromPinnedSpan(field.ReadOnlySpan),
            ArgSlice.FromPinnedSpan(value.ReadOnlySpan)
        ]);
        var input = new RawStringInput(RespCommand.RISET, ref parseState);

        // Acquire lock + create-if-needed
        Span<byte> indexSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];
        using (rangeIndexManager.ReadOrCreateRangeIndex(
            this, ref key, ref input, indexSpan, out var status))
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
                    ref key, ref input, ref basicContext);

            return GarnetStatus.OK;
        }
    }

    /// RI.GET — point read
    public GarnetStatus RangeIndexGet(SpanByte key, ArgSlice field,
        ref SpanByteAndMemory output, out RangeIndexResult result)
    {
        parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));
        var input = new RawStringInput(RespCommand.RIGET, ref parseState);

        Span<byte> indexSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];
        using (rangeIndexManager.ReadRangeIndex(
            this, ref key, ref input, indexSpan, out var status))
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
    public GarnetStatus RangeIndexDel(SpanByte key, ArgSlice field)
    {
        parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(field.ReadOnlySpan));
        var input = new RawStringInput(RespCommand.RIDEL, ref parseState);

        Span<byte> indexSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];
        using (rangeIndexManager.ReadRangeIndex(
            this, ref key, ref input, indexSpan, out var status))
        {
            if (status != GarnetStatus.OK) return status;

            rangeIndexManager.TryDelete(indexSpan, field.ReadOnlySpan);
            rangeIndexManager.ReplicateRangeIndexDel(
                ref key, ref input, ref basicContext);
            return GarnetStatus.OK;
        }
    }

    /// RI.SCAN — range scan with count
    public GarnetStatus RangeIndexScan(SpanByte key, ArgSlice startKey,
        int count, byte returnField,
        ref SpanByteAndMemory output, out int resultCount)
    {
        // ... same lock pattern, then:
        // rangeIndexManager.TryScanWithCount(indexSpan, startKey, count, returnField, ref output, out resultCount)
    }

    /// RI.RANGE — range scan with end key
    public GarnetStatus RangeIndexRange(SpanByte key, ArgSlice startKey,
        ArgSlice endKey, byte returnField,
        ref SpanByteAndMemory output, out int resultCount)
    {
        // ... same lock pattern, then:
        // rangeIndexManager.TryScanWithEndKey(indexSpan, startKey, endKey, returnField, ref output, out resultCount)
    }
}
```

---

### Step 7: Implement `RangeIndexManager` (partial class)

> **Reference:** `VectorManager` is split across 8 files. We start with 4 core files;
> migration and replication are deferred.

#### 7a. `RangeIndexManager.cs` — Main class

```csharp
public sealed partial class RangeIndexManager : IDisposable
{
    // --- Constants (mirrors VectorManager constants) ---
    internal const int IndexSizeBytes = 56; // sizeof(RangeIndexStub)
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

    /// Recovery entry point (analogous to VectorManager.ResumePostRecovery)
    internal void ResumePostRecovery() { /* scan for stale stubs, RecreateIndex */ }

    public void Dispose() { service.Dispose(); }
}
```

#### 7b. `RangeIndexManager.Index.cs` — Stub struct + serialization

```csharp
public sealed partial class RangeIndexManager
{
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct RangeIndexStub
    {
        internal const int Size = 56;

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

    /// Write a new stub into the main store value.
    /// Called from InitialUpdater via RMWMethods.cs.
    internal void CreateIndex(uint cacheSize, uint minRecordSize,
        uint maxRecordSize, uint maxKeyLen, uint leafPageSize,
        byte storageBackend, nint treePtr, ref SpanByte value)
    {
        value.ShrinkSerializedLength(RangeIndexStub.Size);
        ref var stub = ref Unsafe.As<byte, RangeIndexStub>(
            ref MemoryMarshal.GetReference(value.AsSpan()));
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
    internal void RecreateIndex(nint newTreePtr, ref SpanByte value)
    {
        ReadIndex(value.AsReadOnlySpan(), out _, out _, out _, out _,
            out _, out _, out _, out _, out var indexPid);
        Debug.Assert(processInstanceId != indexPid,
            "Shouldn't recreate an index from the same process instance");
        ref var stub = ref Unsafe.As<byte, RangeIndexStub>(
            ref MemoryMarshal.GetReference(value.AsSpan()));
        stub.TreePtr = newTreePtr;
        stub.ProcessInstanceId = processInstanceId;
    }

    /// Zero-check for safe deletion (analogous to VectorManager pattern).
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

#### 7c. `RangeIndexManager.Locking.cs` — Lock management

> **Reference:** `VectorManager.Locking.cs` lines 25-49 (`ReadVectorLock`),
> lines 103-235 (`ReadVectorIndex`), lines 242-431 (`ReadOrCreateVectorIndex`)

```csharp
public sealed partial class RangeIndexManager
{
    private ReadOptimizedLock[] rangeIndexLocks; // Striped lock array

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
    /// If stub has stale ProcessInstanceId, promotes to exclusive, recreates BfTree,
    /// releases exclusive, re-acquires shared.
    internal ReadRangeIndexLock ReadRangeIndex(
        StorageSession session, ref SpanByte key, ref RawStringInput input,
        Span<byte> indexSpan, out GarnetStatus status)
    {
        // 1. Hash key → pick lock stripe
        // 2. AcquireSharedLock
        // 3. session.Read_MainStore(ref key) → copy value into indexSpan
        // 4. If not found: status = NOTFOUND, return default lock
        // 5. ReadIndex → check ProcessInstanceId
        // 6. If stale: promote to exclusive, BfTreeService.Recreate(), RMW update, release exclusive, retry
        // 7. Return ReadRangeIndexLock holding shared lock
    }

    /// Acquire shared lock, CREATE the range index if key doesn't exist.
    /// Used by RI.SET and RI.CREATE.
    internal ReadRangeIndexLock ReadOrCreateRangeIndex(
        StorageSession session, ref SpanByte key, ref RawStringInput input,
        Span<byte> indexSpan, out GarnetStatus status)
    {
        // 1. Same as ReadRangeIndex
        // 2. If not found: promote to exclusive
        // 3. Create BfTree: var treePtr = service.CreateIndex(config...)
        // 4. Inject treePtr into input.parseState (arg slot for InitialUpdater)
        // 5. Issue RMW → hits InitialUpdater which calls CreateIndex(ref value)
        // 6. Release exclusive lock, re-acquire shared
        // 7. Read the stub into indexSpan
        // 8. Return ReadRangeIndexLock holding shared lock
    }
}
```

---

### Step 8: Wire into Main Store RMW callbacks

> **Reference:** `libs/server/Storage/Functions/MainStore/RMWMethods.cs`
> - `InitialUpdater()` at line 79 (VADD case: lines 247-275)
> - `InPlaceUpdaterWorker()` at line 360 (VADD case: lines 831-854)
> - `CopyUpdater()` (VADD case: lines 1406-1420)

#### InitialUpdater

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
        input.parseState.GetArgSliceByRef(0).Span);
    var minRecordSize = MemoryMarshal.Read<uint>(
        input.parseState.GetArgSliceByRef(1).Span);
    var maxRecordSize = MemoryMarshal.Read<uint>(
        input.parseState.GetArgSliceByRef(2).Span);
    var maxKeyLen = MemoryMarshal.Read<uint>(
        input.parseState.GetArgSliceByRef(3).Span);
    var leafPageSize = MemoryMarshal.Read<uint>(
        input.parseState.GetArgSliceByRef(4).Span);
    var storageBackend = input.parseState.GetArgSliceByRef(5).Span[0];
    var treePtr = MemoryMarshal.Read<nint>(
        input.parseState.GetArgSliceByRef(6).Span);

    recordInfo.RangeIndexSet = true;
    functionsState.rangeIndexManager.CreateIndex(
        cacheSize, minRecordSize, maxRecordSize,
        maxKeyLen, leafPageSize, storageBackend,
        treePtr, ref value);
}
break;
```

#### InPlaceUpdater

```csharp
case RespCommand.RISET:
case RespCommand.RIDEL:
case RespCommand.RICREATE:
    if (input.arg1 == RangeIndexManager.DeleteAfterDropArg)
    {
        value.AsSpan().Clear();
    }
    else if (input.arg1 == RangeIndexManager.RecreateIndexArg)
    {
        var newTreePtr = MemoryMarshal.Read<nint>(
            input.parseState.GetArgSliceByRef(6).Span);
        functionsState.rangeIndexManager.RecreateIndex(
            newTreePtr, ref value);
    }
    // All other operations (insert/delete/scan) are handled OUTSIDE
    // of Tsavorite's RMW — they happen in the StorageSession layer
    // while holding the shared lock. The RMW path here is only for
    // stub lifecycle (create/recreate/drop).
    return IPUResult.Succeeded;
```

#### CopyUpdater

```csharp
case RespCommand.RISET:
case RespCommand.RIDEL:
case RespCommand.RICREATE:
    if (input.arg1 == RangeIndexManager.DeleteAfterDropArg)
    {
        newValue.AsSpan().Clear();
    }
    else if (input.arg1 == RangeIndexManager.RecreateIndexArg)
    {
        var newTreePtr = MemoryMarshal.Read<nint>(
            input.parseState.GetArgSliceByRef(6).Span);
        oldValue.CopyTo(ref newValue);
        functionsState.rangeIndexManager.RecreateIndex(
            newTreePtr, ref newValue);
    }
    else
    {
        oldValue.CopyTo(ref newValue);
    }
    break;
```

---

### Step 9: Wire into Main Store Read Methods

> **Reference:** `libs/server/Storage/Functions/MainStore/ReadMethods.cs`
> - `SingleReader()` lines 28-45: VectorSet type checking
> - `ConcurrentReader()` lines 240-257: same pattern

Add type-safety guards in both `SingleReader` and `ConcurrentReader`:

```csharp
// After the existing VectorSet checks:
if (readInfo.RecordInfo.RangeIndexSet && !cmd.IsLegalOnRangeIndex())
{
    readInfo.Action = ReadAction.CancelOperation;
    return false;
}
else if (!readInfo.RecordInfo.RangeIndexSet && cmd.IsLegalOnRangeIndex())
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

> **Reference:** `libs/server/Storage/Functions/MainStore/DeleteMethods.cs` lines 17-31

```csharp
// In SingleDeleter and ConcurrentDeleter:
if (recordInfo.RangeIndexSet
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

> **Reference:** `libs/server/Resp/Vector/DiskANNService.cs` — wraps unmanaged DiskANN
> library. RangeIndex follows the same pattern with P/Invoke to a Rust shared library.

**File:** `libs/server/Resp/RangeIndex/BfTreeService.cs` (new)

```csharp
/// Wraps the native Bf-Tree library (bftree.dll / libbftree.so).
/// Analogous to DiskANNService for Vector Indexes.
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

**Rust FFI side** (`bf-tree/src/ffi.rs`, new file in the bf-tree crate):

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

---

### Step 12: Add `RangeIndexManager` to `FunctionsState`

> **Reference:** `libs/server/Storage/Functions/FunctionsState.cs`
> `vectorManager` is declared and initialized here.

```csharp
// Add field:
internal readonly RangeIndexManager rangeIndexManager;

// Initialize in constructor alongside vectorManager:
this.rangeIndexManager = new RangeIndexManager(logger);
```

Also add `rangeIndexManager` to `StorageSession` so that `RangeIndexOps.cs` can access it:

> **Reference:** `libs/server/Storage/Session/StorageSession.cs`
> `vectorManager` is passed to the session and stored as a field.

```csharp
internal readonly RangeIndexManager rangeIndexManager;
```

---

### Step 13: Checkpoint & Recovery

> **Reference:** VectorManager doesn't use explicit checkpoint callbacks. Instead:
> - The stub in the main store is automatically serialized by Tsavorite during checkpoint
> - On recovery, `ResumePostRecovery()` scans for stale stubs and recreates BfTrees

**Checkpoint:** Before Garnet checkpoint, call `RangeIndexManager.PrepareCheckpoint()`:

```csharp
internal void PrepareCheckpoint()
{
    // Snapshot all active BfTrees to disk.
    // The stub's config fields + deterministic path allow reconstruction.
    // TreePtr becomes stale but ProcessInstanceId enables detection.
}
```

**Recovery:** After Tsavorite recovery, call `RangeIndexManager.ResumePostRecovery()`:

```csharp
internal void ResumePostRecovery()
{
    // Iterate main store for all records with RangeIndexSet == true.
    // For each:
    //   1. ReadIndex → extract config + ProcessInstanceId
    //   2. If ProcessInstanceId != this.processInstanceId → stale
    //   3. Derive snapshot path from config
    //   4. newTreePtr = service.RestoreFromSnapshot(path, config...)
    //   5. Issue RMW with RecreateIndexArg to update stub
}
```

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

### Modified Files (8 files)

| # | File Path | Change | Scope |
|---|---|---|---|
| 1 | `libs/storage/Tsavorite/.../RecordInfo.cs` | Add `RangeIndexSet` bit flag | ~5 lines |
| 2 | `libs/server/Resp/Parser/RespCommand.cs` | Add RI* enum values + `ParseRangeIndexCommand()` | ~50 lines |
| 3 | `libs/server/Resp/RespServerSession.cs` | Add RI* command dispatch entries | ~15 lines |
| 4 | `libs/server/Storage/Functions/MainStore/RMWMethods.cs` | Add RICREATE/RISET/RIDEL cases in InitialUpdater, InPlaceUpdater, CopyUpdater | ~60 lines |
| 5 | `libs/server/Storage/Functions/MainStore/ReadMethods.cs` | Add RangeIndexSet type guards in SingleReader + ConcurrentReader | ~20 lines |
| 6 | `libs/server/Storage/Functions/MainStore/DeleteMethods.cs` | Add RangeIndexSet deletion guard | ~10 lines |
| 7 | `libs/server/Storage/Functions/FunctionsState.cs` | Add `rangeIndexManager` field | ~5 lines |
| 8 | `libs/server/API/IGarnetApi.cs` + `GarnetApi.cs` | Add RangeIndex* method declarations + delegation | ~60 lines |

---

## Persistence, Checkpoint, Migration, and Replication

This section covers how BfTree data survives page flush, checkpoint, recovery, replica
sync, and key migration. This is the **critical difference from VectorSet**: VectorSet
stores all element data inside Tsavorite (in a separate namespace), so Tsavorite's own
checkpoint/migration machinery handles it automatically. BfTree stores its data entirely
**outside** Tsavorite (in its own circular buffer + disk files). The stub in Tsavorite is
just a pointer + config — the actual data must be managed separately at every persistence
boundary.

### Background: How Tsavorite Persists SpanByte Records

> **Reference:** `libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs` lines 479-500;
> `libs/storage/Tsavorite/cs/src/core/Allocator/SpanByteAllocatorImpl.cs` lines 252-272

- Tsavorite writes hybrid log pages **verbatim to disk** — raw memory bytes, no per-record
  transformation or callback.
- `RecordInfo` flags (including our `RangeIndexSet` bit) are part of the 8-byte record
  header and survive flush as-is.
- The `TreePtr` field in the stub is a raw `nint` — it is written to disk as a meaningless
  64-bit value. On reload, it is stale and must be fixed up via `RecreateIndex()`.
- There is **no per-record callback** during page flush (`DisposeOnPageEviction` is `false`
  for `SpanByteRecordDisposer`). However, there is an **observer pattern** via
  `store.Log.SubscribeEvictions()` that fires per-page.

### The Four Persistence Boundaries

| Boundary | What happens | RangeIndex action required |
|---|---|---|
| **Page flush** | Tsavorite evicts a hybrid log page from memory to disk. Stub is written as raw bytes; `TreePtr` becomes stale on disk. | Snapshot the BfTree so its data is on disk. |
| **Checkpoint** | Tsavorite takes a full or incremental checkpoint of the hybrid log. All stubs are included. | Snapshot ALL active BfTrees before checkpoint begins. |
| **Recovery** | Tsavorite recovers from checkpoint. Stubs are loaded with stale `TreePtr` values. | Scan for `RangeIndexSet` records, restore each BfTree from its snapshot, update `TreePtr`. |
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

```csharp
// In RangeIndexManager.Persistence.cs
internal static string DeriveSnapshotPath(
    ReadOnlySpan<byte> keyBytes, string checkpointDir, Guid checkpointToken)
{
    // Hex-encode the key bytes for a collision-free, filesystem-safe directory name.
    // RangeIndex keys are typically short (e.g., "r1"), so path length is not a concern.
    var keyHex = Convert.ToHexString(keyBytes);
    return Path.Combine(checkpointDir, "rangeindex", keyHex,
        $"{checkpointToken}.bftree");
}

// Overload for page-flush (uses latest checkpoint directory, no token subdirectory)
internal static string DeriveSnapshotPath(
    ReadOnlySpan<byte> keyBytes, string baseSnapshotDir)
{
    var keyHex = Convert.ToHexString(keyBytes);
    return Path.Combine(baseSnapshotDir, "rangeindex", keyHex, "latest.bftree");
}
```

> **Why hex-encoding instead of hashing?** A 32-bit hash (e.g., FNV-1a) is susceptible to
> collisions via the birthday paradox (~50% at ~77K keys). Two RangeIndex keys that collide
> would map to the same snapshot file, causing silent data loss. Hex-encoding the key bytes
> is deterministic, collision-free, and produces short paths for typical key names.

### Design: Updated Stub Layout

Since the snapshot path is now derived collision-free from the hex-encoded key bytes, the
`SnapshotPathHash` field in the stub is **no longer needed for verification**. It can be
repurposed as a reserved field or removed. The path derivation is fully deterministic from
inputs already available at recovery time (key bytes, checkpoint dir, checkpoint token).

---

### A. Page Flush (Hybrid Log Eviction)

> **Reference:** `libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs`
> Eviction observer: `store.Log.SubscribeEvictions(observer)` —
> see `libs/server/Storage/SizeTracker/CacheSizeTracker.cs` lines 88, 97

**Problem:** When Tsavorite flushes a page containing a RangeIndex stub to disk, the
`TreePtr` is written as a raw pointer. If the page is later loaded from disk (e.g., during
a read for a cold key), the `TreePtr` is stale. But the BfTree instance may still be alive
in memory — we just can't reach it from the deserialized stub.

**Solution:** Use the `SubscribeEvictions()` observer to intercept page evictions and
snapshot any RangeIndex stubs being evicted:

```csharp
// In RangeIndexManager.cs
internal void SubscribeToEvictions(TsavoriteKV<SpanByte, SpanByte, ...> store)
{
    store.Log.SubscribeEvictions(new RangeIndexEvictionObserver(this));
}

private class RangeIndexEvictionObserver :
    IObserver<ITsavoriteScanIterator<SpanByte, SpanByte>>
{
    private readonly RangeIndexManager manager;

    public void OnNext(ITsavoriteScanIterator<SpanByte, SpanByte> iter)
    {
        // Scan evicted records for RangeIndexSet flag
        while (iter.GetNext(out var recordInfo))
        {
            if (!recordInfo.RangeIndexSet) continue;

            ref var key = ref iter.GetKey();
            ref var value = ref iter.GetValue();

            ReadIndex(value.AsReadOnlySpan(), out var treePtr, ...);
            if (treePtr == nint.Zero) continue;

            // Snapshot this BfTree to disk before it becomes unreachable
            if (activeIndexes.TryGetValue(treePtr, out var entry))
            {
                var snapshotPath = DeriveSnapshotPath(entry.KeyBytes);
                service.Snapshot(treePtr, snapshotPath);
            }
        }
    }
}
```

**On page reload:** When Tsavorite reads a page back from disk and a read/RMW targets a
RangeIndex key, the `ReadRangeIndex()` locking method detects the stale `ProcessInstanceId`
and triggers `RecreateIndex()` — restoring the BfTree from its snapshot file. This is the
same flow as recovery.

---

### B. Checkpoint

> **Reference:** `libs/server/Databases/DatabaseManagerBase.cs` line 634:
> `IClusterProvider.OnCheckpointInitiated()` fires before checkpoint state machine starts.
> `libs/server/Databases/SingleDatabaseManager.cs` line 112: recovery calls
> `VectorManager.Initialize()` post-recovery.

**Pre-checkpoint hook:** Before Tsavorite begins the checkpoint state machine, snapshot
all active BfTrees:

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

> **Reference:** `libs/server/Databases/SingleDatabaseManager.cs` `RecoverCheckpoint()`
> lines 57-113. `VectorManager.Initialize()` + `ResumePostRecovery()` called post-recovery.

**Post-recovery scan:** After Tsavorite recovery loads all records from the checkpoint,
scan the main store for all records with `RangeIndexSet == true` and recreate their
BfTrees:

```csharp
// In RangeIndexManager.cs:
internal void ResumePostRecovery(
    TsavoriteKV<SpanByte, SpanByte, ...> store,
    string checkpointDir, Guid checkpointToken)
{
    // 1. Iterate main store for RangeIndexSet records
    using var iter = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress);
    while (iter.GetNext(out var recordInfo))
    {
        if (!recordInfo.RangeIndexSet) continue;
        ref var key = ref iter.GetKey();
        ref var value = ref iter.GetValue();

        ReadIndex(value.AsReadOnlySpan(), out var treePtr,
            out var cacheSize, out var minRecordSize, out var maxRecordSize,
            out var maxKeyLen, out var leafPageSize, out var storageBackend,
            out var flags, out var pid);

        // 2. ProcessInstanceId mismatch → stale pointer
        if (pid == processInstanceId) continue; // Already live (shouldn't happen post-recovery)

        // 3. Derive snapshot path deterministically from key + checkpoint info
        //    Uses the same DeriveSnapshotPath() that PrepareCheckpoint() used to write the file.
        //    Inputs: key bytes (from Tsavorite record), checkpointDir (from config),
        //            checkpointToken (from store.RecoveredToken).
        var snapshotPath = DeriveSnapshotPath(
            key.AsReadOnlySpan(), checkpointDir, checkpointToken);

        if (!File.Exists(snapshotPath))
        {
            logger.LogError("RangeIndex snapshot not found: {path}", snapshotPath);
            continue; // Data loss — snapshot file missing
        }

        // 4. Restore BfTree from snapshot
        var newTreePtr = service.RestoreFromSnapshot(
            snapshotPath, cacheSize, minRecordSize, maxRecordSize,
            maxKeyLen, leafPageSize);

        // 5. Register in active indexes
        activeIndexes[newTreePtr] = new RangeIndexEntry
        {
            KeyBytes = key.AsReadOnlySpan().ToArray(),
            SnapshotPath = snapshotPath,
            CacheSize = cacheSize,
            // ... other config
        };

        // 6. Issue RMW with RecreateIndexArg to update stub in Tsavorite
        // This updates TreePtr and ProcessInstanceId in the main store record
        IssueRecreateRMW(ref key, newTreePtr, store);
    }
}

private void IssueRecreateRMW(ref SpanByte key, nint newTreePtr,
    TsavoriteKV<...> store)
{
    // Build parseState with newTreePtr at arg slot 6
    // Set input.arg1 = RecreateIndexArg
    // Call basicContext.RMW(ref key, ref input)
    // This triggers InPlaceUpdater/CopyUpdater which calls RecreateIndex()
}
```

**Call site:**

```csharp
// In SingleDatabaseManager.RecoverCheckpoint(), after line 112:
defaultDatabase.RangeIndexManager.Initialize();
defaultDatabase.RangeIndexManager.ResumePostRecovery(
    MainStore, checkpointDir, recoveredToken);
```

---

### D. Replica Sync (Sending Checkpoint to Replica)

> **Reference:** `libs/cluster/Server/Replication/PrimaryOps/ReplicaSyncSession.cs`
> `SendCheckpoint()` lines 101-346. Sends files by `CheckpointFileType` enum.
> `libs/cluster/Server/Replication/CheckpointFileType.cs` lines 12-58.

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
// In ReplicaSyncSession.cs, inside SendCheckpoint(), after sending object store files:

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

After all files are received and Tsavorite recovery completes, the replica calls
`RangeIndexManager.ResumePostRecovery()` which finds the snapshot files at the expected
paths and recreates the BfTrees.

---

### E. Key Migration (Cluster Slot Migration)

> **Reference:** `libs/cluster/Server/Migration/MigrateSessionKeys.cs` lines 23-147.
> VectorSet 2-phase migration: `VectorManager.Migration.cs`.
> Key scanning: `MigrateScanFunctions.cs` line 54 checks `RecordInfo.VectorSet`.
> Receiver: `libs/cluster/Session/RespClusterMigrateCommands.cs`.

**Problem:** During slot migration, individual keys are transferred to the target node.
For a RangeIndex key, we can't just send the 56-byte stub — we must also send the
entire BfTree data (all entries in the index). The target node must recreate the BfTree
from this data.

**Solution: 2-Phase Migration (analogous to VectorSet)**

#### Phase 1: Serialize BfTree data

When the migration scanner encounters a `RangeIndexSet` record:

```csharp
// In MigrateScanFunctions.cs (add alongside VectorSet detection):
if (recordMetadata.RecordInfo.RangeIndexSet)
{
    mss.EncounteredRangeIndex(ref key, ref value);
    return; // Don't transmit the stub yet — Phase 2
}
```

```csharp
// In MigrateSessionKeys.cs (new method):
internal void EncounteredRangeIndex(ref SpanByte key, ref SpanByte value)
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

        // 4. Send to target: [stub (56 bytes)] + [snapshot_length (4 bytes)]
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

    // 5. Insert into local main store with RangeIndexSet flag
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
| NEW | `libs/server/Resp/RangeIndex/RangeIndexManager.Persistence.cs` | Eviction observer, `PrepareCheckpoint()`, `ResumePostRecovery()`, snapshot path derivation |
| NEW | `libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs` | `HandleMigratedRangeIndexKey()`, migration serialization/deserialization |
| MOD | `libs/server/Databases/DatabaseManagerBase.cs` | Call `rangeIndexManager.PrepareCheckpoint()` before checkpoint state machine |
| MOD | `libs/server/Databases/SingleDatabaseManager.cs` | Call `rangeIndexManager.ResumePostRecovery()` after Tsavorite recovery |
| MOD | `libs/cluster/Server/Replication/CheckpointFileType.cs` | Add `RANGEINDEX_SNAPSHOT` enum value |
| MOD | `libs/cluster/Server/Replication/PrimaryOps/ReplicaSyncSession.cs` | Send BfTree snapshot files during replica sync |
| MOD | `libs/cluster/Session/RespClusterMigrateCommands.cs` | Handle `RISTORE` type during key migration |
| MOD | `libs/cluster/Server/Migration/MigrateSessionKeys.cs` | Detect `RangeIndexSet`, serialize BfTree, 2-phase transmit |
| MOD | `libs/cluster/Server/Migration/MigrateScanFunctions.cs` | Check `RangeIndexSet` flag during slot scan |
| MOD | `bf-tree/src/ffi.rs` | Add `bftree_snapshot_to_path`, `bftree_serialize_to_buffer`, `bftree_deserialize_from_buffer` |

---

### H. Persistence Testing Plan

1. **Page flush round-trip** — Insert data, force page eviction, read cold key back → BfTree restored from snapshot
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
