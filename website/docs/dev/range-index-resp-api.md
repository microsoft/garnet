# Integrating Range Index (Bf-Tree) as a Garnet Data Type

## Summary

Proposal for a RESP protocol API that exposes [Bf-Tree](https://github.com/microsoft/bf-tree) as a `RangeIndex` data type, analogous to how Garnet exposes SortedSets via `Z*` commands. The server hosts a key-value cache where keys are RangeIndex names (e.g., `r1`, `r2`) and values are `BfTree` instances.

All commands follow the `RI.*` prefix convention (short for **R**ange**I**ndex).

---

## Implementation Status

> **Note:** The implementation plan sections below were written before implementation and contain
> some stale details (e.g., 51-byte stubs with `ProcessInstanceId`, `ResumePostRecovery()`).
> This section documents the **actual** implemented design. Refer to the RESP API specification
> (Section 1–5) and the code for authoritative details.

### Stub Design (actual: 35 bytes)

The `ProcessInstanceId` Guid was removed — stale pointers are handled by `OnDiskRead` zeroing
the `TreeHandle`. The stub is 35 bytes:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | `TreeHandle` | Native pointer to live BfTreeService (zeroed on disk read) |
| 8 | 8 | `CacheSize` | Circular buffer size |
| 16 | 4 | `MinRecordSize` | Min record size |
| 20 | 4 | `MaxRecordSize` | Max record size |
| 24 | 4 | `MaxKeyLen` | Max key length |
| 28 | 4 | `LeafPageSize` | Leaf page size |
| 32 | 1 | `StorageBackend` | 0=Disk, 1=Memory |
| 33 | 1 | `Flags` | bit 0: Flushed, bit 1: Recovered |
| 34 | 1 | `SerializationPhase` | Reserved |

### IRecordTriggers Lifecycle (implemented)

All lifecycle callbacks go through the `IRecordTriggers` interface in Tsavorite, implemented
by `GarnetRecordTriggers`:

| Trigger | When | What it does for RangeIndex |
|---------|------|---------------------------|
| `OnFlush` | Page moves to read-only | Snapshot BfTree to `flush.bftree` under exclusive lock; set `FlagFlushed` |
| `OnEvict` | Page evicted past HeadAddress | Free BfTree under exclusive lock via `DisposeTreeUnderLock`; data files preserved for lazy restore |
| `OnDiskRead` | Record loaded from disk | Zero `TreeHandle` (native pointer is stale) |
| `OnCheckpoint(VersionShift)` | PREPARE→IN_PROGRESS | Set checkpoint barrier; mark all live trees `SnapshotPending=1` |
| `OnCheckpoint(FlushBegin)` | WAIT_FLUSH | Snapshot trees with `SnapshotPending=1` under exclusive lock; clear barrier |
| `OnCheckpoint(CheckpointCompleted)` | REST | Purge old checkpoint snapshot files (if `removeOutdated`) |
| `OnRecovery(Guid)` | Before snapshot file recovery | Store recovered checkpoint token |
| `OnRecoverySnapshotRead` | Per record from snapshot file | Set `FlagRecovered` on RI stubs |
| `OnDispose(Deleted)` | DEL/UNLINK | Free BfTree under exclusive lock; delete data files (`data.bftree`, `flush.bftree`, snapshots, key directory) |

### Lazy Promote (Flush → Tail)

When `ReadRangeIndex` detects `IsFlushed`:
1. Release shared lock
2. Issue `RIPROMOTE` RMW — `CopyUpdater` copies stub to tail, clears `FlagFlushed`
3. `PostCopyUpdater` clears source `TreeHandle` (after CAS success, not before)
4. Retry — stub is now in mutable region

### Lazy Restore (Eviction/Recovery → Live Tree)

When `ReadRangeIndex` detects `TreeHandle == 0`:
1. Release shared lock
2. Acquire **exclusive** lock (prevents concurrent restores)
3. Re-read stub — if another thread already restored, return
4. Determine snapshot path: `FlagRecovered` → checkpoint file, else → `flush.bftree`
5. Copy snapshot to `data.bftree` (working path), open via `RecoverFromSnapshot`
6. Register in `liveIndexes`, issue `RIRESTORE` RMW to set new `TreeHandle` and clear `FlagRecovered`
7. Release exclusive lock, retry

> **Note:** `RecreateIndex()` clears `FlagRecovered` when setting the new `TreeHandle`.
> This ensures that subsequent eviction cycles restore from `flush.bftree` (which
> reflects post-recovery writes) rather than the stale checkpoint snapshot.

### Checkpoint Consistency

- Trees with `SnapshotPending=1` (marked at barrier time) are snapshotted — their data
  reflects version v (v+1 RI ops are blocked by the per-tree barrier).
- Trees restored/created in v+1 have `SnapshotPending=0` — they are **skipped** by
  `SnapshotAllTreesForCheckpoint`. On recovery they fall back to `flush.bftree` (v-state).
- Each BfTree snapshot is stored at `{dataDir}/rangeindex/{hash}/snapshot.{token}.bftree`.
- Old checkpoint snapshots are purged at `CheckpointCompleted` when `removeOutdated=true`
  (non-cluster mode), matching Tsavorite's checkpoint cleanup behavior.

### AOF Logging

RI.SET and RI.DEL operate on the native BfTree outside Tsavorite's RMW path. After each
successful operation, a synthetic no-op RMW is injected to trigger AOF logging. On AOF replay,
`AofProcessor.StoreRMW` detects RISET/RIDEL commands and routes them to
`RangeIndexManager.HandleRangeIndexSetReplay/DelReplay`, which re-executes the BfTree operation.

### Type Safety (WRONGTYPE)

- `ReadMethods`: rejects non-RI commands on RI keys and RI commands on non-RI keys
- `RMWMethods`: same bidirectional checks in `InPlaceUpdater`
- `UpsertMethods`: `InPlaceWriter` rejects SET on RI/Vector stubs (`UpsertAction.WrongType`)
- `TYPE` command returns `"rangeindex"` for RI keys

### Implemented Commands

| Command | Status |
|---------|--------|
| RI.CREATE | ✅ Implemented |
| RI.SET | ✅ Implemented + AOF |
| RI.GET | ✅ Implemented |
| RI.DEL | ✅ Implemented + AOF |
| RI.SCAN | ✅ Implemented |
| RI.RANGE | ✅ Implemented |
| RI.EXISTS | ✅ Implemented |
| RI.CONFIG | ✅ Implemented |
| RI.METRICS | ✅ Implemented |
| DEL/UNLINK | ✅ Works via OnDispose |
| TYPE | ✅ Returns "rangeindex" |
| RI.MSET / RI.MGET / RI.MDEL | ❌ Not yet implemented |
| RI.KEYS | ❌ Not yet implemented |
| Cluster replication | ❌ Future work |
| Key migration | ❌ Future work |

---

### 1. Lifecycle / Management Commands

| Command | Syntax | Description | Maps to |
|---|---|---|---|
| **RI.CREATE** | `RI.CREATE key [options...]` | Create a new RangeIndex. Options allow tuning the underlying BfTree config | `BfTree::new()` / `BfTree::with_config()` |
| **RI.EXISTS** | `RI.EXISTS key` | Check if a RangeIndex exists. Returns `1` or `0` | Cache lookup |
| **RI.CONFIG** | `RI.CONFIG key` | Return current config of the RangeIndex as key-value pairs | `BfTree::config()` |
| **RI.METRICS** | `RI.METRICS key` | Return buffer/tree metrics (JSON) | `BfTree::get_buffer_metrics()` / `get_metrics()` |

Deletion of RangeIndex keys uses the standard `DEL` / `UNLINK` commands. The store's `DisposeRecord` callback detects the RangeIndex `RecordType`, snapshots the BfTree pointer from the stub, and frees it — no special drop command is needed.

Snapshot and restore are handled automatically by the cache checkpointing mechanism.

#### `RI.CREATE` options

```
RI.CREATE myindex
    [DISK | MEMORY]
    [CACHESIZE bytes]
    [MINRECORD bytes]
    [MAXRECORD bytes]
    [MAXKEYLEN bytes]
    [PAGESIZE bytes]
```

**Storage backends:**

- **`DISK`** (default) — Disk-backed tree. Base pages are stored in a data file on
  disk at a **deterministic path** derived from the key bytes:
  `{dataDir}/rangeindex/{XxHash128(key)}/data.bftree`. The circular buffer (`CACHESIZE`)
  acts as a hot-data cache. No data loss on eviction. Total capacity is limited by disk
  space. Supports all operations including scan. Snapshot and recovery use the tree's
  own data file.
- **`MEMORY`** — Memory-only tree (maps to bf-tree's `cache_only` mode). All data
  lives in the circular buffer. Total capacity is bounded by `CACHESIZE`. Scan
  operations are **not supported**. Snapshot and recovery will be supported in a
  future bf-tree release; Garnet will snapshot/recover memory-only trees the same way
  as disk-backed trees once bf-tree adds this capability.

**Examples:**
```
RI.CREATE r1 DISK
RI.CREATE r1 DISK CACHESIZE 67108864 MAXKEYLEN 64
RI.CREATE r1 MEMORY CACHESIZE 16777216 MINRECORD 8 MAXRECORD 4096
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
- `-ERR memory-only mode does not support scan` if the index is memory-only

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
| `drop(BfTree)` | `DEL` / `UNLINK` |
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
> RI.CREATE r1 DISK CACHESIZE 33554432
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

> DEL r1
:1
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

RangeIndex supports two **storage backends**, configurable per index via `RI.CREATE`:

- **Disk-backed** (default) — Bf-Tree stores leaf pages in a data file on disk, with a
  circular buffer in memory as a hot-data cache. This is the primary mode for production
  use. No data loss on eviction — evicted pages are written to disk. Total capacity is
  limited by disk space. Snapshot uses `BfTree::snapshot()` which drains the circular
  buffer and writes the index structure to the tree's own data file. Recovery uses
  `BfTree::new_from_snapshot(config)` which opens the existing data file and resumes.
- **Memory-only** — Bf-Tree uses a bounded in-memory circular buffer (bf-tree's
  `cache_only` mode). Evicted pages are nullified. Total capacity is bounded by
  `CACHESIZE`. Scan operations are not supported. Snapshot and recovery are planned
  for a future bf-tree release; Garnet will treat memory-only trees identically to
  disk-backed trees for persistence once bf-tree adds this capability. Until then,
  snapshot/recovery calls throw `NotSupportedException` at the FFI boundary.

This follows the same "stub-in-store with external data manager" pattern used by
**VectorManager** on the
[`dev`](https://github.com/microsoft/garnet/tree/dev)
dev branch. Implementers should cross-reference that branch for working examples
of each pattern described below. The key difference: VectorSet stores element data inside
Tsavorite (via a separate `VectorSessionFunctions` context), while BfTree manages **all**
its data externally (circular buffer + disk files), so RangeIndex only needs the string
context for the stub — no additional Tsavorite context type is required.

**Why the string context and not the object context?**
- The Bf-Tree manages its own memory (circular buffer, leaf pages) and disk storage — it
  cannot be inlined into Tsavorite's log the way a `HashObject` or `SortedSetObject` can.
- The stub pattern cleanly separates metadata persistence (Tsavorite checkpoint) from
  index operations (Bf-Tree).
- A 51-byte fixed-size stub is a natural fit for the string context's inline byte values,
  avoiding the overhead of `GarnetObjectBase` serialization.
- The `RecordInfo.ValueIsObject` bit remains `false` for RangeIndex records, distinguishing
  them from collection objects.

---

## Proposed File Layout

The RangeIndex implementation is organized as a partial class (`RangeIndexManager`) split
across multiple files, plus supporting files for RESP handlers, storage session wrappers,
and native interop. Each file mirrors a corresponding VectorManager file on the
[`dev`](https://github.com/microsoft/garnet/tree/dev)
dev branch.

| # | New File | Reference (`dev`) | Role |
|---|---|---|---|
| 1 | `libs/server/Resp/RangeIndex/RangeIndexManager.cs` | [`VectorManager.cs`](https://github.com/microsoft/garnet/blob/dev/libs/server/Resp/Vector/VectorManager.cs) | Main class: constants, `processInstanceId`, `IsEnabled`, initialization, `TryInsert`/`TryRead`/`TryScan`/`TryRange` methods, `ResumePostRecovery()` |
| 2 | `libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs` | [`VectorManager.Index.cs`](https://github.com/microsoft/garnet/blob/dev/libs/server/Resp/Vector/VectorManager.Index.cs) | Stub struct definition, `CreateIndex()`, `ReadIndex()`, `RecreateIndex()` |
| 3 | `libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs` | [`VectorManager.Locking.cs`](https://github.com/microsoft/garnet/blob/dev/libs/server/Resp/Vector/VectorManager.Locking.cs) | `ReadRangeIndexLock` ref struct, `ReadRangeIndex()`, `ReadOrCreateRangeIndex()` — shared/exclusive lock management via `ReadOptimizedLock` |
| 4 | `libs/server/Resp/RangeIndex/RangeIndexManager.Cleanup.cs` | [`VectorManager.Cleanup.cs`](https://github.com/microsoft/garnet/blob/dev/libs/server/Resp/Vector/VectorManager.Cleanup.cs) | Post-drop async cleanup: background task that scans and removes orphaned data |
| 5 | `libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs` | [`VectorManager.Migration.cs`](https://github.com/microsoft/garnet/blob/dev/libs/server/Resp/Vector/VectorManager.Migration.cs) | *(future)* Replication/migration support |
| 6 | `libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs` | [`VectorManager.Replication.cs`](https://github.com/microsoft/garnet/blob/dev/libs/server/Resp/Vector/VectorManager.Replication.cs) | *(future)* Primary→replica replication |
| 7 | `libs/native/bftree-garnet/BfTreeInterop.csproj` | — | C# interop project: MSBuild cargo target + `ContentWithTargetPath` for native libs |
| 8 | `libs/native/bftree-garnet/BfTreeService.cs` | [`DiskANNService.cs`](https://github.com/microsoft/garnet/blob/dev/libs/server/Resp/Vector/DiskANNService.cs) | High-level managed wrapper for native BfTree library |
| 9 | `libs/native/bftree-garnet/NativeBfTreeMethods.cs` | — | `[LibraryImport]` P/Invoke declarations for `bftree_garnet` native library |
| 10 | `libs/native/bftree-garnet/Cargo.toml` + `src/lib.rs` | [`diskann-garnet`](https://github.com/microsoft/DiskANN/tree/main/diskann-garnet) | Rust FFI wrapper crate: `#[no_mangle] extern "C"` exports over `bf-tree` crate |
| 11 | `libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs` | [`RespServerSessionVectors.cs`](https://github.com/microsoft/garnet/blob/dev/libs/server/Resp/Vector/RespServerSessionVectors.cs) | RESP command handlers (`NetworkRISET`, `NetworkRIGET`, etc.) |
| 12 | `libs/server/Storage/Session/MainStore/RangeIndexOps.cs` | *(inline in VectorManager methods)* | Storage session wrappers that acquire locks via manager and call `Try*` methods |

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

> **Reference:** [`VectorManager.Index.cs`](https://github.com/microsoft/garnet/blob/dev/libs/server/Resp/Vector/VectorManager.Index.cs) —
> the `Index` struct, `CreateIndex()`, `ReadIndex()`, `RecreateIndex()`.

A fixed-size struct stored as a raw-byte (non-object) value in the unified store, accessed
via the string context. Since `RecordInfo.ValueIsObject` is `false` for these records, the
string context's `MainSessionFunctions` handles the RMW/Read/Delete callbacks.

```csharp
[StructLayout(LayoutKind.Explicit, Size = 35)]
private struct RangeIndexStub
{
    [FieldOffset(0)]  public nint TreeHandle;            // Pointer to live BfTree instance
    [FieldOffset(8)]  public ulong CacheSize;            // BfTree cb_size_byte
    [FieldOffset(16)] public uint MinRecordSize;         // BfTree cb_min_record_size
    [FieldOffset(20)] public uint MaxRecordSize;         // BfTree cb_max_record_size
    [FieldOffset(24)] public uint MaxKeyLen;             // BfTree cb_max_key_len
    [FieldOffset(28)] public uint LeafPageSize;          // BfTree leaf_page_size
    [FieldOffset(32)] public byte StorageBackend;        // 0=Disk, 1=Memory
    [FieldOffset(33)] public byte Flags;                 // bit 0: Flushed (needs promote to tail)
    [FieldOffset(34)] public byte SerializationPhase;    // checkpoint coordination (future)
}
```

**Key fields explained:**

- `TreeHandle` — Native pointer to a live `BfTree` instance. Zeroed by `OnDiskReadRecord`
  when the record is loaded from disk (recovery, pending read, etc.). A zero TreeHandle
  signals "needs lazy restore" — the first subsequent operation restores the BfTree from
  the flush snapshot file via `RecoverFromSnapshot` and updates this field via RIRESTORE RMW.
- `Flags` — Bit 0 (`FlagFlushed`): Set by `OnFlushRecord` when the page moves to read-only.
  The next RI operation detects this flag, issues RIPROMOTE RMW to copy the stub to the
  mutable region (tail), and clears the flag. This ensures the stub will be re-flushed
  (with up-to-date BfTree snapshot) on the next checkpoint or ReadOnly transition.
- `SerializationPhase` — Reserved for checkpoint coordination (future work).
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
       or RespCommand.RICREATE
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
RespCommand.RIEXISTS => NetworkRIEXISTS(ref storageApi),
RespCommand.RICONFIG => NetworkRICONFIG(ref storageApi),
RespCommand.RIMETRICS => NetworkRIMETRICS(ref storageApi),
RespCommand.RIKEYS   => NetworkRIKEYS(ref storageApi),
```

---

### Step 4: Implement RESP command handlers

> **Reference:** RESP handler pattern used throughout `libs/server/Resp/` (e.g.,
> `BasicCommands.cs`, `Objects/HashCommands.cs`).
> **Reference:** [`RespServerSessionVectors.cs`](https://github.com/microsoft/garnet/blob/dev/libs/server/Resp/Vector/RespServerSessionVectors.cs) —
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

    // NetworkRICREATE, NetworkRIMSET, NetworkRIMDEL,
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

> **Reference:** [`VectorManager.cs`](https://github.com/microsoft/garnet/blob/dev/libs/server/Resp/Vector/VectorManager.cs) —
> constants, `processInstanceId`, `TryAdd`/`TryRemove`, `Initialize()`, `ResumePostRecovery()`, `Dispose()`.

<details>
<summary>RangeIndexManager main class (click to expand)</summary>

```csharp
public sealed partial class RangeIndexManager : IDisposable
{
    // --- Constants ---
    internal const int IndexSizeBytes = 51; // sizeof(RangeIndexStub)
    internal const long RISetAppendLogArg = long.MinValue;
    internal const long RecreateIndexArg = RISetAppendLogArg + 1;
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

> **Reference:** [`VectorManager.Index.cs`](https://github.com/microsoft/garnet/blob/dev/libs/server/Resp/Vector/VectorManager.Index.cs) —
> 51-byte `Index` struct with `CreateIndex()`, `ReadIndex()`, `RecreateIndex()`, `SetContextForMigration()`.

<details>
<summary>RangeIndexStub struct and serialization methods (click to expand)</summary>

```csharp
public sealed partial class RangeIndexManager
{
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct RangeIndexStub
    {
        internal const int Size = 51;

        [FieldOffset(0)]  public nint TreePtr;
        [FieldOffset(8)]  public ulong CacheSize;
        [FieldOffset(16)] public uint MinRecordSize;
        [FieldOffset(20)] public uint MaxRecordSize;
        [FieldOffset(24)] public uint MaxKeyLen;
        [FieldOffset(28)] public uint LeafPageSize;
        [FieldOffset(32)] public byte StorageBackend;
        [FieldOffset(33)] public byte Flags;
        [FieldOffset(34)] public byte SerializationPhase;
        [FieldOffset(35)] public Guid ProcessInstanceId;
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

}
```

</details>

#### 7c. `RangeIndexManager.Locking.cs` — Lock management

> The locking pattern uses a striped `ReadOptimizedLock` (from `Garnet.common`) for
> concurrent access, with stripe count based on `Environment.ProcessorCount`. Key hash
> (via `unifiedBasicContext.GetKeyHash(key)`) selects the stripe.
> `ReadRangeIndex()` acquires a shared lock; `ReadOrCreateRangeIndex()` promotes to
> exclusive if the key doesn't exist, creates the BfTree, then downgrades to shared.
>
> `ReadOptimizedLock` is defined in `libs/common/Synchronization/ReadOptimizedLock.cs`
> (shared with VectorSet). It provides shared/exclusive locking with
> `AcquireSharedLock(keyHash, out token)`,
> `TryPromoteSharedLock(keyHash, sharedToken, out exclusiveToken)`,
> `ReleaseSharedLock(token)`, and `ReleaseExclusiveLock(token)` methods.
> Tests: `test/Garnet.test/ReadOptimizedLockTests.cs`.

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
    if (input.arg1 == RangeIndexManager.RecreateIndexArg)
    {
        var newTreePtr = MemoryMarshal.Read<nint>(
            input.parseState.GetArgSliceByRef(6).ReadOnlySpan);
        functionsState.rangeIndexManager.RecreateIndex(
            newTreePtr, logRecord.ValueSpan);
    }
    // All other operations (insert/delete/scan) are handled OUTSIDE
    // of Tsavorite's RMW — they happen in the StorageSession layer
    // while holding the shared lock. The RMW path here is only for
    // stub lifecycle (create/recreate).
    return true;
```

</details>

#### CopyUpdater

After copying the stub to the new record, we must handle the old record's BfTree
carefully. This is the inline-bytes analogue of `CacheSerializedObjectData` in
`HeapObjectBase.cs`, which uses a `SerializationPhase` state machine (`REST` →
`SERIALIZING` → `SERIALIZED`) to coordinate between CopyUpdater and concurrent
snapshot flush.

**The problem:** During a checkpoint, the snapshot flush callback needs a consistent
BfTree snapshot. But once CopyUpdater completes and the new record becomes visible,
concurrent operations will modify the BfTree through the new record — racing with the
snapshot flush trying to read the same BfTree for the old (checkpoint-version) record.

**The solution:** The CopyUpdater itself snapshots the BfTree (under the exclusive RMW
lock, before the new record is visible), producing a stable file. The snapshot flush
callback then uses the already-written file instead of the live BfTree.

A `SerializationPhase` state machine on the RangeIndexManager (per-index, keyed by
`TreePtr`) coordinates this:
- **`REST`** — no snapshot in progress
- **`SERIALIZING`** — CopyUpdater or snapshot flush is writing the BfTree to disk
- **`SERIALIZED`** — a stable snapshot file exists for this checkpoint version

**Two cases in CopyUpdater:**

1. **No checkpoint in progress** (`!srcLogRecord.Info.IsInNewVersion`):
   Zero `TreePtr` in the old stub. Eviction sees `TreePtr == 0` and skips.

2. **Checkpoint in progress** (`srcLogRecord.Info.IsInNewVersion`):
   The old record is part of the checkpoint. Snapshot the BfTree now (transition
   `REST` → `SERIALIZING` → `SERIALIZED`), then zero `TreePtr` in the old stub.
   The snapshot flush callback sees `SERIALIZED` and uses the file, or sees
   `SERIALIZING` and waits (spin-yield), matching the `CacheSerializedObjectData`
   pattern.

<details>
<summary>CopyUpdater case (click to expand)</summary>

```csharp
case RespCommand.RISET:
case RespCommand.RIDEL:
case RespCommand.RICREATE:
    if (input.arg1 == RangeIndexManager.RecreateIndexArg)
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

    // Handle old record's BfTree — analogous to CacheSerializedObjectData.
    ReadIndex(srcLogRecord.ValueSpan, out var treePtr, ...);
    if (treePtr != nint.Zero)
    {
        if (srcLogRecord.Info.IsInNewVersion)
        {
            // Checkpoint in progress: snapshot the BfTree NOW, before the new
            // record becomes visible and concurrent ops modify the tree.
            // Uses SerializationPhase state machine to coordinate with flush callback.
            functionsState.rangeIndexManager.SnapshotForCheckpoint(
                treePtr, srcLogRecord.Key);
        }

        // Zero TreePtr in old record — safe now because either:
        // (a) no checkpoint → eviction will skip, or
        // (b) checkpoint → snapshot file already written above
        ref var oldStub = ref Unsafe.As<byte, RangeIndexStub>(
            ref MemoryMarshal.GetReference(srcLogRecord.ValueSpan));
        oldStub.TreePtr = nint.Zero;
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

### Step 10: Delete handling via `DisposeRecord`

> **Reference:** `libs/server/Storage/Functions/MainStore/DisposeMethods.cs`
> The `DisposeRecord` callback is invoked by Tsavorite when a record is deleted (`DEL` / `UNLINK`)
> or evicted from the log. It handles freeing the BfTree for RangeIndex keys.

No special `RI.DROP` command is needed. Standard `DEL` / `UNLINK` commands delete
RangeIndex keys. The store's `DisposeRecord(DisposeReason.PageEviction)` and delete
path handles BfTree cleanup:

```csharp
// In DisposeRecord:
if (logRecord.RecordType == RangeIndexManager.RangeIndexRecordType)
{
    var indexSpan = logRecord.ValueSpan;
    functionsState.rangeIndexManager.ReadIndex(indexSpan,
        out var treePtr, out _, out _, out _,
        out _, out _, out _, out _, out var pid);
    if (pid == functionsState.rangeIndexManager.ProcessInstanceId && treePtr != 0)
    {
        // Snapshot the BfTree pointer and free it
        functionsState.rangeIndexManager.Service.Drop(treePtr);
    }
}
```

This approach is simpler and safer than a dedicated drop command:
- No two-phase zeroing+delete dance is needed
- `DisposeRecord` is guaranteed to be called for every deleted or evicted record
- Works for both explicit `DEL`/`UNLINK` and page eviction scenarios

---

### Step 11: Build and integrate the native Bf-Tree library

The Bf-Tree is a Rust library published on crates.io as
[`bf-tree`](https://crates.io/crates/bf-tree) (source:
[`microsoft/bf-tree`](https://github.com/microsoft/bf-tree)). It has no C FFI layer —
that is provided by a thin **wrapper crate** in the Garnet repo. Unlike the
[`diskann-garnet`](https://github.com/microsoft/DiskANN/tree/main/diskann-garnet)
approach (which publishes a separate NuGet from the DiskANN repo), the `bftree-garnet`
crate and its C# interop wrapper live **inside the Garnet repo** and the native binaries
ship inside the existing `Microsoft.Garnet` NuGet package. This avoids the need for a
separate signing pipeline in the `bf-tree` repo, keeps versioning unified with Garnet,
and follows the same pattern used by `native_device` (Tsavorite's native storage driver).

#### 11a. Project structure

**Location:** `libs/native/bftree-garnet/` in the Garnet repo.

```
libs/native/bftree-garnet/
├── Cargo.toml                    # Rust cdylib crate, depends on bf-tree from crates.io
├── src/
│   └── lib.rs                    # #[no_mangle] extern "C" fn FFI exports
└── BfTreeInterop.csproj          # C# project with MSBuild cargo target + interop code
    ├── NativeBfTreeMethods.cs    #   [LibraryImport] P/Invoke declarations
    └── BfTreeService.cs          #   High-level managed wrapper
```

**`Cargo.toml`:**
```toml
[package]
name = "bftree-garnet"
version = "0.1.0"
edition = "2021"
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
bf-tree = "0.4"
```

**`src/lib.rs`** — contains all `#[no_mangle] pub extern "C" fn` exports that wrap
bf-tree's Rust API for C/P/Invoke consumption. See the Rust FFI code below.

**`BfTreeInterop.csproj`** — a C# class library that:
1. Contains the managed interop code (`NativeBfTreeMethods.cs`, `BfTreeService.cs`)
2. Has an MSBuild `<Exec>` target that runs `cargo build --release` for local development
3. Copies the native library to `$(OutDir)` for the current platform
4. Declares `ContentWithTargetPath` items to include the native library under
   `runtimes/{rid}/native/` for NuGet packaging

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <AllowUnsafeBlocks>true</AllowUnsafeBlocks>
    <RootNamespace>Garnet.server.BfTreeInterop</RootNamespace>
  </PropertyGroup>

  <!-- Local dev: build Rust crate for current platform -->
  <Target Name="BuildRustCdylib" BeforeTargets="Build"
          Condition="'$(CI)' != 'true' AND '$(ContinuousIntegrationBuild)' != 'true'">
    <Exec Command="cargo build --release --manifest-path=$(MSBuildThisFileDirectory)Cargo.toml"
          IgnoreExitCode="false" />
  </Target>

  <!-- Include the native library so it propagates to consuming projects
       via ProjectReference. Platform-conditional Content items with
       CopyToOutputDirectory automatically copy to the output of any
       project that references BfTreeInterop. -->
  <ItemGroup Condition="$([MSBuild]::IsOSPlatform('Linux'))">
    <Content Include="$(MSBuildThisFileDirectory)target/release/libbftree_garnet.so"
             CopyToOutputDirectory="PreserveNewest" Link="libbftree_garnet.so"
             Condition="Exists('$(MSBuildThisFileDirectory)target/release/libbftree_garnet.so')" />
  </ItemGroup>
  <ItemGroup Condition="$([MSBuild]::IsOSPlatform('Windows'))">
    <Content Include="$(MSBuildThisFileDirectory)target\release\bftree_garnet.dll"
             CopyToOutputDirectory="PreserveNewest" Link="bftree_garnet.dll"
             Condition="Exists('$(MSBuildThisFileDirectory)target\release\bftree_garnet.dll')" />
  </ItemGroup>
  <ItemGroup Condition="$([MSBuild]::IsOSPlatform('OSX'))">
    <Content Include="$(MSBuildThisFileDirectory)target/release/libbftree_garnet.dylib"
             CopyToOutputDirectory="PreserveNewest" Link="libbftree_garnet.dylib"
             Condition="Exists('$(MSBuildThisFileDirectory)target/release/libbftree_garnet.dylib')" />
  </ItemGroup>

  <!-- NuGet packaging: include pre-built native binaries for all platforms.
       In CI, native binaries are placed here by the pipeline before dotnet build. -->
  <ItemGroup>
    <ContentWithTargetPath Include="runtimes/linux-x64/native/libbftree_garnet.so"
                           TargetPath="runtimes/linux-x64/native/libbftree_garnet.so"
                           CopyToOutputDirectory="PreserveNewest"
                           Condition="Exists('runtimes/linux-x64/native/libbftree_garnet.so')" />
    <ContentWithTargetPath Include="runtimes/win-x64/native/bftree_garnet.dll"
                           TargetPath="runtimes/win-x64/native/bftree_garnet.dll"
                           CopyToOutputDirectory="PreserveNewest"
                           Condition="Exists('runtimes/win-x64/native/bftree_garnet.dll')" />
    <ContentWithTargetPath Include="runtimes/osx-x64/native/libbftree_garnet.dylib"
                           TargetPath="runtimes/osx-x64/native/libbftree_garnet.dylib"
                           CopyToOutputDirectory="PreserveNewest"
                           Condition="Exists('runtimes/osx-x64/native/libbftree_garnet.dylib')" />
  </ItemGroup>

  <!-- Exclude Rust build artifacts from the C# project -->
  <ItemGroup>
    <None Remove="target/**" />
    <None Remove="src/**" />
  </ItemGroup>
</Project>
```

**`Garnet.server.csproj`** references this project:
```xml
<ProjectReference Include="../../native/bftree-garnet/BfTreeInterop.csproj" />
```

#### 11b. Pipeline build and signing plan

The native library is built as part of Garnet's existing release pipeline
(`azure-pipelines-external-release.yml`). A new **Stage 1** builds the Rust crate on
each target platform, then the existing .NET build/sign/pack stages consume the outputs.

**Stage 1: Build Native (new, matrix job)**

Two parallel agents build the Rust crate for their respective platforms:

| Agent | Rust target | Output |
|---|---|---|
| Linux (ubuntu) | `x86_64-unknown-linux-gnu` | `libbftree_garnet.so` |
| Windows | `x86_64-pc-windows-msvc` | `bftree_garnet.dll` |

Each agent:
1. Installs the Rust toolchain (e.g., via `rustup`)
2. Runs `cargo build --release --manifest-path libs/native/bftree-garnet/Cargo.toml`
3. Uploads the native library as a pipeline artifact

```yaml
# Pseudocode for azure-pipelines-external-release.yml additions:
- stage: BuildNative
  jobs:
  - job: BuildNativeLinux
    pool:
      vmImage: 'ubuntu-latest'
    steps:
    - script: |
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        source $HOME/.cargo/env
        cargo build --release --manifest-path libs/native/bftree-garnet/Cargo.toml
    - publish: libs/native/bftree-garnet/target/release/libbftree_garnet.so
      artifact: native-linux-x64

  - job: BuildNativeWindows
    pool:
      vmImage: 'windows-latest'
    steps:
    - script: |
        rustup default stable
        cargo build --release --manifest-path libs/native/bftree-garnet/Cargo.toml
    - publish: libs/native/bftree-garnet/target/release/bftree_garnet.dll
      artifact: native-win-x64
```

**Stage 2: Build .NET + Sign + Pack (existing stage, modified)**

Before the existing `dotnet build` step, download the native artifacts from Stage 1 and
place them in the expected `runtimes/` layout:

```yaml
# Download and place native binaries
- download: current
  artifact: native-linux-x64
- download: current
  artifact: native-win-x64
- script: |
    mkdir -p libs/native/bftree-garnet/runtimes/linux-x64/native
    mkdir -p libs/native/bftree-garnet/runtimes/win-x64/native
    cp $(Pipeline.Workspace)/native-linux-x64/libbftree_garnet.so \
       libs/native/bftree-garnet/runtimes/linux-x64/native/
    cp $(Pipeline.Workspace)/native-win-x64/bftree_garnet.dll \
       libs/native/bftree-garnet/runtimes/win-x64/native/
```

The rest of the existing pipeline continues unchanged, with two modifications:

1. **ESRP binary signing** — extend the file pattern to include the new native DLL:
   ```
   Pattern: Garnet*.dll,Tsavorite*.dll,Garnet*.exe,HdrHistogram.dll,native_device.dll,bftree_garnet.dll,*Lua.dll
   ```

2. **`dotnet pack`** — no changes needed. The `ContentWithTargetPath` items in
   `BfTreeInterop.csproj` automatically include the native binaries in the NuGet.

**Signing summary:**

| Artifact | Signed? | Method |
|---|---|---|
| `bftree_garnet.dll` (Windows) | ✅ Yes | ESRP Authenticode signing (`CP-230012`), added to existing binary signing glob |
| `libbftree_garnet.so` (Linux) | ❌ No | Linux shared libraries are not Authenticode-signed (same as `libnative_device.so`) |
| `Microsoft.Garnet.*.nupkg` | ✅ Yes | Existing ESRP NuGet signing step (`CP-401405`), no changes needed |
| `garnet-server.*.nupkg` | ✅ Yes | Same existing step, no changes needed |

**Local development experience:**

- `dotnet build` triggers `cargo build --release` via the MSBuild target → copies the
  current-platform native lib to the output directory → everything works out of the box
- The `Condition="'$(CI)' != 'true'"` guard on the MSBuild target prevents the local
  cargo build from running in CI (where pre-built binaries are provided by Stage 1)
- No Rust toolchain is needed if you're not modifying FFI code — NuGet restore from
  nuget.org brings pre-built binaries for all platforms in the published package

**Version management:**

- The `bf-tree` crate version is pinned in `libs/native/bftree-garnet/Cargo.toml`
  (e.g., `bf-tree = "0.4"`)
- To update: bump the version in `Cargo.toml`; the next pipeline run picks it up
- No separate NuGet versioning — the native lib ships inside the existing
  `Microsoft.Garnet` NuGet, versioned together with Garnet via `Version.props`

#### 11c. Implement `BfTreeService` (C# interop wrapper)

> **Reference:** [`DiskANNService.cs`](https://github.com/microsoft/garnet/blob/dev/libs/server/Resp/Vector/DiskANNService.cs) —
> wraps the unmanaged DiskANN library. BfTreeService follows the same pattern with
> P/Invoke to the Rust shared library.

**File:** `libs/native/bftree-garnet/BfTreeService.cs` (new, inside BfTreeInterop project)

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

    /// Scan all entries in the tree, ordered by key.
    /// Useful for streaming the full tree state to a replica.
    /// Internally calls ScanWithCount with start_key=\x00 and count=int.MaxValue.
    /// Only supported for disk-backed trees (memory-only do not support scan).
    internal List<ScanRecord> ScanAll(ScanReturnField returnField = ScanReturnField.KeyAndValue)
        => ScanWithCount(new byte[] { 0 }, int.MaxValue, returnField);

    /// Snapshot a disk-backed BfTree in place.
    internal void Snapshot(nint tree)
    {
        int result = NativeBfTreeMethods.bftree_snapshot(tree);
        if (result != 0)
            throw new InvalidOperationException("Failed to snapshot BfTree.");
    }

    /// Recover a disk-backed BfTree from its data file.
    internal nint RecoverFromSnapshot(string filePath,
        ulong cacheSize, uint minRecordSize, uint maxRecordSize,
        uint maxKeyLen, uint leafPageSize)
    {
        var pathBytes = Encoding.UTF8.GetBytes(filePath);
        fixed (byte* pp = pathBytes)
            return NativeBfTreeMethods.bftree_new_from_snapshot(
                pp, pathBytes.Length,
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
/// Uses LibraryImport (source-generated, zero-overhead) matching the DiskANN pattern.
internal static unsafe partial class NativeBfTreeMethods
{
    private const string LibName = "bftree_garnet";

    [LibraryImport(LibName)] internal static partial nint bftree_create(
        ulong cacheSize, uint minRecord, uint maxRecord,
        uint maxKeyLen, uint leafPageSize, byte storageBackend,
        byte* filePath, int filePathLen);

    [LibraryImport(LibName)] internal static partial int bftree_insert(
        nint tree, byte* key, int keyLen, byte* value, int valueLen);

    [LibraryImport(LibName)] internal static partial int bftree_read(
        nint tree, byte* key, int keyLen, byte* outBuffer, int outBufferLen,
        int* outValueLen);

    [LibraryImport(LibName)] internal static partial void bftree_delete(
        nint tree, byte* key, int keyLen);

    [LibraryImport(LibName)] internal static partial nint bftree_scan_with_count(
        nint tree, byte* startKey, int startKeyLen, int count, byte returnField);

    [LibraryImport(LibName)] internal static partial nint bftree_scan_with_end_key(
        nint tree, byte* startKey, int startKeyLen,
        byte* endKey, int endKeyLen, byte returnField);

    [LibraryImport(LibName)] internal static partial int bftree_scan_next(
        nint iter, byte* outBuffer, int outBufferLen,
        out int keyLen, out int valueLen);

    [LibraryImport(LibName)] internal static partial void bftree_scan_drop(nint iter);

    [LibraryImport(LibName)] internal static partial int bftree_snapshot(nint tree);

    [LibraryImport(LibName)] internal static partial nint bftree_new_from_snapshot(
        byte* filePath, int filePathLen,
        ulong cacheSize, uint minRecord, uint maxRecord,
        uint maxKeyLen, uint leafPageSize);

    [LibraryImport(LibName)] internal static partial void bftree_drop(nint tree);
}

/// Result codes from BfTree insert operations
internal enum BfTreeInsertResult
{
    Success = 0,
    InvalidKV = 1,
}
```

</details>

**Rust FFI side** (`libs/native/bftree-garnet/src/lib.rs`):

<details>
<summary>Rust FFI exports (click to expand)</summary>

```rust
use bf_tree::{BfTree, Config, LeafInsertResult, LeafReadResult, ScanReturnField,
              ScanIter, StorageBackend};
use std::path::Path;
use std::slice;

// Storage backend constants (matches C# StorageBackendType enum)
const STORAGE_MEMORY: u8 = 1;

#[no_mangle]
pub unsafe extern "C" fn bftree_create(
    cb_size_byte: u64, cb_min_record_size: u32, cb_max_record_size: u32,
    cb_max_key_len: u32, leaf_page_size: u32,
    storage_backend: u8, file_path: *const u8, file_path_len: i32,
) -> *mut BfTree {
    let mut config = Config::default();
    // ... apply non-zero config fields ...
    if storage_backend == STORAGE_MEMORY {
        config.cache_only(true);
    } else {
        // Disk-backed (default)
        let path_str = /* UTF-8 from file_path */;
        config.storage_backend(StorageBackend::Std);
        config.file_path(Path::new(path_str));
    }
    match BfTree::with_config(config, None) {
        Ok(tree) => Box::into_raw(Box::new(tree)),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn bftree_insert(
    tree: *mut BfTree, key: *const u8, key_len: i32,
    value: *const u8, value_len: i32,
) -> i32 { /* ... 0=Success, 1=InvalidKV */ }

#[no_mangle]
pub unsafe extern "C" fn bftree_read(
    tree: *mut BfTree, key: *const u8, key_len: i32,
    out_buffer: *mut u8, out_buffer_len: i32, out_value_len: *mut i32,
) -> i32 { /* ... 0=Found, -1=NotFound, -2=Deleted, -3=InvalidKey */ }

#[no_mangle]
pub unsafe extern "C" fn bftree_delete(
    tree: *mut BfTree, key: *const u8, key_len: i32,
) { /* ... */ }

#[no_mangle]
pub unsafe extern "C" fn bftree_drop(tree: *mut BfTree) {
    if !tree.is_null() { drop(Box::from_raw(tree)); }
}

/// Snapshot a disk-backed BfTree in place. Returns 0 on success, -1 on failure.
#[no_mangle]
pub unsafe extern "C" fn bftree_snapshot(tree: *mut BfTree) -> i32 { /* ... */ }

/// Recover a disk-backed BfTree from its data file. Returns tree ptr or null.
#[no_mangle]
pub unsafe extern "C" fn bftree_new_from_snapshot(
    file_path: *const u8, file_path_len: i32,
    cb_size_byte: u64, cb_min_record_size: u32, cb_max_record_size: u32,
    cb_max_key_len: u32, leaf_page_size: u32,
) -> *mut BfTree { /* ... BfTree::new_from_snapshot(config, None) */ }

/// STUB: Snapshot a memory-only (cache_only) BfTree to disk. Returns -1 (not yet implemented).
#[no_mangle]
pub unsafe extern "C" fn bftree_snapshot_memory(
    _tree: *mut BfTree, _path: *const u8, _path_len: i32,
) -> i32 { -1 /* TODO: implement when bf-tree adds cache_only snapshot */ }

/// STUB: Recover a memory-only (cache_only) BfTree from disk. Returns null (not yet implemented).
#[no_mangle]
pub unsafe extern "C" fn bftree_recover_memory(
    _path: *const u8, _path_len: i32,
    _cb_size_byte: u64, _cb_min_record_size: u32, _cb_max_record_size: u32,
    _cb_max_key_len: u32, _leaf_page_size: u32,
) -> *mut BfTree { std::ptr::null_mut() /* TODO: implement when bf-tree adds cache_only recovery */ }

// scan_with_count, scan_with_end_key, scan_next, scan_drop follow similar patterns.
// See libs/native/bftree-garnet/src/lib.rs for the full implementation.
```

</details>

---

### Step 12: Add `RangeIndexManager` to `FunctionsState`

> **Reference:** `libs/server/Storage/Functions/FunctionsState.cs`
> This class holds shared state accessible from Tsavorite session function callbacks
> (e.g., `appendOnlyFile`, `watchVersionMap`, `cacheSizeTracker`).
>
> **Reference:** On `dev`, `FunctionsState` has a
> [`vectorManager` field](https://github.com/microsoft/garnet/blob/dev/libs/server/Storage/Functions/FunctionsState.cs)
> and [`StorageSession`](https://github.com/microsoft/garnet/blob/dev/libs/server/Storage/Session/StorageSession.cs)
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
> handles recovery on the dev branch.

**Checkpoint:** No separate pre-checkpoint scan. BfTrees are serialized per-record
during the snapshot page flush via the `OnSnapshotRecord` callback (see section B below).

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

### New Files (12 files)

| # | File Path | Purpose | Lines (est.) |
|---|---|---|---|
| 1 | `libs/server/Resp/RangeIndex/RangeIndexManager.cs` | Main class: constants, Try* methods, recovery | ~200 |
| 2 | `libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs` | Stub struct, Create/Read/RecreateIndex | ~120 |
| 3 | `libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs` | ReadRangeIndexLock, ReadRangeIndex, ReadOrCreateRangeIndex | ~250 |
| 4 | `libs/server/Resp/RangeIndex/RangeIndexManager.Cleanup.cs` | Post-drop async cleanup | ~100 |
| 5 | `libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs` | RESP command handlers | ~400 |
| 6 | `libs/server/Storage/Session/MainStore/RangeIndexOps.cs` | Storage session wrappers | ~200 |
| 7 | `libs/native/bftree-garnet/BfTreeInterop.csproj` | C# interop project with MSBuild cargo target | ~50 |
| 8 | `libs/native/bftree-garnet/NativeBfTreeMethods.cs` | [LibraryImport] P/Invoke declarations | ~60 |
| 9 | `libs/native/bftree-garnet/BfTreeService.cs` | High-level managed BfTree operations wrapper | ~150 |
| 10 | `libs/native/bftree-garnet/Cargo.toml` | Rust crate config, depends on bf-tree from crates.io | ~15 |
| 11 | `libs/native/bftree-garnet/src/lib.rs` | Rust #[no_mangle] extern "C" FFI exports | ~200 |
| 12 | `test/Garnet.test/RangeIndexTests.cs` | Integration tests | ~300 |

### Modified Files (10 files)

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
| 10 | `libs/server/Garnet.server.csproj` | Add `<ProjectReference>` to `BfTreeInterop.csproj` | ~2 lines |

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

Both storage backends support snapshot and recovery at the Garnet level. For
**disk-backed** trees, snapshot and recovery are fully implemented via `BfTree::snapshot()`
and `BfTree::new_from_snapshot(config)`. For **memory-only** trees, bf-tree's `cache_only`
mode does not yet implement snapshot/recovery — Garnet will throw `NotSupportedException`
at the FFI boundary until bf-tree adds this capability, at which point memory-only trees
will be snapshotted/recovered identically to disk-backed trees.

For disk-backed trees:
- **Snapshot**: `BfTree::snapshot()` drains the circular buffer and writes the index
  structure to the tree's own data file.
- **Recovery**: `BfTree::new_from_snapshot(config)` reopens the existing data file and
  resumes operations.

The `StorageBackend` byte in the stub determines the behavior.

| Boundary | What happens | RangeIndex action required |
|---|---|---|
| **Page flush** | Tsavorite moves pages to read-only, then flushes to disk. | `OnFlushRecord` snapshots BfTree + sets Flushed flag. Next access promotes stub to tail via RIPROMOTE. `DisposeRecord(PageEviction)` frees native instance. `OnDiskReadRecord` zeros TreeHandle on disk reads. Lazy restore via `RestoreTreeFromFlush`. |
| **Checkpoint** | Tsavorite takes a full checkpoint of the hybrid log. All stubs are included. | Stubs in mutable region are flushed (triggering `OnFlushRecord`). Promoted stubs capture latest BfTree state. |
| **Recovery** | Tsavorite recovers from checkpoint. Stubs loaded from disk. | `OnDiskReadRecord` zeros TreeHandle. First access promotes (IsFlushed) → restores BfTree from flush file via `RestoreTreeFromFlush`. |
| **Key migration** | Individual keys are transferred to another node during cluster slot migration. | For disk-backed trees: serialize the BfTree snapshot alongside the stub. Memory-only trees: same approach once snapshot is supported. |
| **Replica sync** | Full checkpoint is sent to a replica. | For disk-backed trees: send BfTree data files alongside checkpoint, or use `ScanAll()` to stream the full tree state record-by-record. Memory-only trees: send snapshot once supported. |

### Design: Snapshot File Management

Each BfTree instance has a **deterministic file path** derived from its Garnet key bytes
and the server's data directory.

**Working copy** (where the live BfTree stores its data):
```
{dataDir}/rangeindex/{key_hash}/data.bftree
```

**Flush snapshot** (point-in-time copy for cold-read recovery after page eviction):
```
{dataDir}/rangeindex/{key_hash}/flush.bftree
```

`key_hash` is the XxHash128 of the key bytes, formatted as a 32-character hex string
(`Guid.ToString("N")`). All paths are derived deterministically — no in-memory registry
or per-stub file paths are needed. The user does not specify any paths; `RI.CREATE DISK`
derives them automatically.

### Design: Deterministic Path Derivation (Implemented)

The `RangeIndexManager` derives paths using `XxHash128` hashing of the key bytes:

```csharp
// In RangeIndexManager.cs
internal string DeriveWorkingPath(ReadOnlySpan<byte> keyBytes)
    => Path.Combine(dataDir, "rangeindex", HashKeyToDirectoryName(keyBytes), "data.bftree");

internal string DeriveFlushPath(ReadOnlySpan<byte> keyBytes)
    => Path.Combine(dataDir, "rangeindex", HashKeyToDirectoryName(keyBytes), "flush.bftree");

internal static string HashKeyToDirectoryName(ReadOnlySpan<byte> keyBytes)
{
    var hash = XxHash128.Hash(keyBytes);
    return new Guid(hash).ToString("N");
}
```

---

### A. Page Flush (Hybrid Log Eviction) — Implemented

> **Reference:**
> - `libs/storage/Tsavorite/cs/src/core/Allocator/ObjectAllocatorImpl.cs` —
>   `OnFlushRecordsInRange()` calls `storeFunctions.OnFlushRecord(ref logRecord)` per
>   valid record on original in-memory pages before flush, gated by `CallOnFlush`.
> - `libs/storage/Tsavorite/cs/src/core/Index/StoreFunctions/IRecordTrigger.cs` —
>   `OnFlushRecord`, `OnDiskReadRecord`, `DisposeRecord` callbacks.
> - `libs/server/Storage/Functions/GarnetRecordTrigger.cs` — Garnet implementation.
> - `libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs` — Lazy promote + restore.

**Three-phase lifecycle for page eviction:**

1. **`OnFlushRecord` — snapshot + set flushed flag:** Called per valid record in
   `OnPagesMarkedReadOnlyWorker()` on the **original in-memory records** (not a copy),
   before the page is flushed to disk. For RangeIndex records:
   - Calls `BfTreeService.SnapshotToFile(flushPath)`: disk-backed trees do in-place
     `snapshot()` then `File.Copy` to `{dataDir}/rangeindex/{key_hash}/flush.bftree`.
   - Sets the `Flushed` flag on the in-memory stub so the next operation promotes it.

2. **Lazy promote on next access:** `ReadRangeIndex()` detects `IsFlushed` → releases
   shared lock → issues `RIPROMOTE` RMW → `CopyUpdater` copies stub to tail (mutable
   region), clears flushed flag, transfers tree ownership by clearing source TreeHandle.
   This ensures the stub will be re-flushed (with latest BfTree state) on the next
   checkpoint or ReadOnly transition.

3. **`DisposeRecord(PageEviction)` — free native instance:** Called when the page is
   evicted past HeadAddress. `GarnetRecordTrigger` calls `DisposeTreeIfOwned()` which
   unregisters and frees the native BfTree. The source TreeHandle was already cleared
   by RIPROMOTE's CopyUpdater (if the stub was promoted), so this is a no-op for
   promoted records.

**`OnDiskReadRecord` — invalidate stale handles:** Called per record loaded from disk
(recovery, pending reads, push scans). Zeros `TreeHandle` on RangeIndex
stubs so operations detect the stub as "needs lazy restore."

**On cold read (after eviction past HeadAddress):**
1. Tsavorite issues pending read from disk → `OnDiskReadRecord` zeros TreeHandle
2. `ReadRangeIndex()` detects `IsFlushed` → promotes stub to tail via RIPROMOTE RMW
3. CopyUpdater from disk source: stub already has TreeHandle=0 (from OnDiskReadRecord)
4. After promote, `ReadRangeIndex()` detects `TreeHandle == 0` → calls `RestoreTreeFromFlush`
5. Derives flush path from key bytes → `RecoverFromSnapshot(flushPath, config...)`
6. Registers restored BfTree → issues RIRESTORE RMW to set TreeHandle on mutable stub
7. Re-reads stub → TreeHandle is valid → returns data from restored BfTree

**Internal RMW commands:**
- `RIPROMOTE` — Copies stub to tail, clears flushed flag, transfers tree ownership.
  IPU: asserts flushed flag is never set on mutable records. NeedCopyUpdate: always true.
- `RIRESTORE` — Sets TreeHandle on a mutable stub after lazy restore from flush file.
  IPU: sets TreeHandle from `input.arg1`. CopyUpdater: copies stub + sets TreeHandle.

---

### B. Checkpoint

**No separate pre-checkpoint scan needed.** During a snapshot checkpoint, Tsavorite
flushes pages to the snapshot file. A per-record callback is invoked for each record
being written to the snapshot. For RangeIndex stubs, this callback ensures a stable
BfTree snapshot file exists for the checkpoint.

**Important: snapshot consistency.** The bf-tree `snapshot()` method flushes state to
the **same SSD backend file** the tree uses for operations. It does not create a separate
point-in-time copy. Subsequent writes to the BfTree modify the same file, overwriting the
serialized state. For checkpoint correctness, we need a point-in-time copy.

**Approach: reflink (copy-on-write) file clone.** After calling `snapshot()` to flush the
BfTree state, create a **reflink copy** of the backend file to the checkpoint directory.
On filesystems that support it (btrfs, XFS, bcachefs, ZFS), this is an instant O(1)
operation that shares data blocks via copy-on-write — subsequent BfTree writes allocate
new blocks without affecting the checkpoint copy. On filesystems without reflink support
(ext4, NTFS), this falls back to a regular file copy.

```csharp
private static void ReflinkCopy(string source, string destination)
{
    Directory.CreateDirectory(Path.GetDirectoryName(destination));
    // Try reflink first (Linux: ioctl FICLONE), fall back to regular copy
    if (!TryReflinkClone(source, destination))
        File.Copy(source, destination, overwrite: true);
}
```

**Coordination with CopyUpdater via `SerializationPhase` state machine:**

The CopyUpdater (Step 8) may have already serialized the BfTree for this checkpoint
version (when `IsInNewVersion` was true). The `SerializationPhase` state machine
(`REST` → `SERIALIZING` → `SERIALIZED`) coordinates this:

- If `SERIALIZED`: CopyUpdater already wrote the file — the snapshot callback just
  reflink-copies it to the checkpoint directory. No need to touch the live BfTree.
- If `SERIALIZING`: Another thread is writing — spin-yield until `SERIALIZED`.
- If `REST`: No CopyUpdate happened — the snapshot callback snapshots the BfTree itself
  (under exclusive lock) and reflink-copies.

> **Tsavorite change required:** Invoke a per-record callback during snapshot page flush,
> analogous to `DisposeRecord` on eviction. This could be a new `DisposeReason` value
> (e.g., `DisposeReason.SnapshotCheckpoint`) or a separate `OnSnapshotToDisk` callback.
> The implementation snapshots the BfTree but does **not** free it (unlike eviction) —
> the BfTree remains live in memory.

```csharp
// In Garnet's record disposer / callback:
// Called per-record during snapshot page flush
public void OnSnapshotRecord(ref LogRecord logRecord, Guid checkpointToken, string checkpointDir)
{
    if (logRecord.RecordType != RangeIndexManager.RangeIndexRecordType)
        return;

    ReadIndex(logRecord.ValueSpan, out var treePtr, ...);
    if (treePtr == nint.Zero) return;

    var keyBytes = logRecord.Key;
    var backendPath = DeriveBackendPath(keyBytes, rangeIndexManager.dataDir);
    var checkpointPath = DeriveSnapshotPath(keyBytes, checkpointDir, checkpointToken);

    // Check SerializationPhase — CopyUpdater may have already serialized
    while (true)
    {
        var phase = rangeIndexManager.GetSerializationPhase(treePtr);
        if (phase == SerializationPhase.SERIALIZED)
        {
            // CopyUpdater already wrote the snapshot — just reflink-copy
            ReflinkCopy(backendPath, checkpointPath);
            return;
        }
        if (phase == SerializationPhase.SERIALIZING)
        {
            // Another thread is writing — wait
            Thread.Yield();
            continue;
        }

        // REST — we need to snapshot the BfTree ourselves
        if (rangeIndexManager.TryTransitionSerializationPhase(treePtr,
                SerializationPhase.REST, SerializationPhase.SERIALIZING))
            break;
    }

    // Acquire exclusive lock, snapshot, reflink-copy
    var keyHash = HashKeyToGuid(keyBytes);
    rangeIndexManager.rangeIndexLocks.AcquireExclusiveLock(keyHash, out var lockToken);
    try
    {
        rangeIndexManager.service.Snapshot(treePtr);
        rangeIndexManager.SetSerializationPhase(treePtr, SerializationPhase.SERIALIZED);
        ReflinkCopy(backendPath, checkpointPath);
    }
    finally
    {
        rangeIndexManager.rangeIndexLocks.ReleaseExclusiveLock(lockToken);
    }
}
```

**After checkpoint:** Reset all `SerializationPhase` states to `REST`. Optionally clean up
old snapshot files from previous checkpoints.

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
4. Derive checkpoint snapshot path from key bytes + checkpoint directory + checkpoint token
5. `newTreePtr = service.RestoreFromSnapshot(snapshotPath, config...)`
6. Issue RMW with `RecreateIndexArg` → updates `TreePtr` and `ProcessInstanceId` in stub
7. Release exclusive, re-acquire shared, proceed with operation

This avoids a full-store scan at startup. Only indexes that are actually accessed pay
the restore cost. This is the same approach used by VectorManager on the dev branch
(where `ResumePostRecovery()` is a no-op TODO).

---

### D. Replica Sync (Sending Checkpoint to Replica)

> **Reference:** `libs/cluster/Server/Replication/PrimaryOps/ReplicaSyncSession.cs` —
> `SendCheckpoint()` sends checkpoint files categorized by `CheckpointFileType` enum.
> `libs/cluster/Server/Replication/CheckpointFileType.cs` — currently defines types for
> `STORE_HLOG`, `STORE_HLOG_OBJ`, `STORE_INDEX`, `STORE_SNAPSHOT`,
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
For a RangeIndex key, we can't just send the 51-byte stub — we must also send the
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

        // 4. Send to target: [stub (51 bytes)] + [snapshot_length (4 bytes)]
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

### F. Summary: FFI Functions for Persistence

The core FFI functions for disk-backed tree persistence are implemented in
`libs/native/bftree-garnet/src/lib.rs`:

- **`bftree_snapshot(tree)`** — Snapshots a disk-backed tree in place (drains circular
  buffer, writes index to data file). Returns 0 on success.
- **`bftree_new_from_snapshot(file_path, ...config)`** — Recovers a disk-backed tree
  from its data file. Returns tree pointer or null.
- **`bftree_create(..., storage_backend, file_path, ...)`** — Creates a new tree with
  the specified backend (0=Disk, 1=Memory).
- **`bftree_drop(tree)`** — Frees a tree instance.

**Tsavorite `IRecordTrigger` callbacks (implemented):**

| Callback | Gating Property | When Called | RangeIndex Action |
|---|---|---|---|
| `OnFlushRecord(ref LogRecord)` | `CallOnFlush` | Per valid record on original in-memory page in `OnPagesMarkedReadOnlyWorker`, before flush | Snapshot BfTree to flush file, set Flushed flag |
| `OnDiskReadRecord(ref LogRecord)` | `CallOnDiskRead` | Per record loaded from disk (recovery, pending reads, push scans) | Zero TreeHandle (invalidate stale pointer) |
| `DisposeRecord(ref LogRecord, reason)` | `DisposeOnPageEviction` | Per record on page eviction, delete | Free native BfTree via `DisposeTreeIfOwned` |

**Memory-only trees:** bf-tree's `snapshot_memory_to_disk` panics for `cache_only`
trees (scan not supported). Future fix: use `StorageBackend::Memory` with
`cache_only=false` instead, which supports scan and snapshot.

**Future:** For key migration, additional buffer-based serialization functions may be
needed to avoid temp files:

```rust
/// Serialize BfTree to a byte buffer (for migration without temp files).
#[no_mangle]
pub extern "C" fn bftree_serialize_to_buffer(
    tree: *mut BfTree,
    buffer: *mut u8, buffer_len: i32,
) -> i64;  // bytes written, or -1 if buffer too small

/// Get the serialized size of a BfTree snapshot (for pre-allocating buffer).
#[no_mangle]
pub extern "C" fn bftree_serialized_size(tree: *mut BfTree) -> i64;

/// Restore from a byte buffer (received from migration).
#[no_mangle]
pub extern "C" fn bftree_deserialize_from_buffer(
    buffer: *const u8, buffer_len: i32,
    cb_size_byte: u64, cb_min_record_size: u32, cb_max_record_size: u32,
    cb_max_key_len: u32, leaf_page_size: u32,
) -> *mut BfTree;  // null on failure
```

These are not yet implemented and will be added when migration support is built.

---

### G. Updated File Inventory (Persistence-Related Additions)

| # | File Path | Purpose |
|---|---|---|
| NEW | `libs/server/Resp/RangeIndex/RangeIndexManager.Persistence.cs` | `DisposeRecord` handler, `OnSnapshotRecord` handler, snapshot path derivation |
| NEW | `libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs` | `HandleMigratedRangeIndexKey()`, migration serialization/deserialization |
| MOD | `libs/server/Databases/DatabaseManagerBase.cs` | *(no RangeIndex-specific changes needed — checkpoint handled via per-record callback)* |
| MOD | `libs/server/Databases/SingleDatabaseManager.cs` | *(no RangeIndex-specific changes needed — recovery is lazy)* |
| MOD | `libs/cluster/Server/Replication/CheckpointFileType.cs` | Add `RANGEINDEX_SNAPSHOT` enum value |
| MOD | `libs/cluster/Server/Replication/PrimaryOps/ReplicaSyncSession.cs` | Send BfTree snapshot files during replica sync |
| MOD | `libs/cluster/Session/RespClusterMigrateCommands.cs` | Handle `RISTORE` type during key migration |
| MOD | `libs/cluster/Server/Migration/MigrateSessionKeys.cs` | Detect RangeIndex `RecordType`, serialize BfTree, 2-phase transmit |
| MOD | `libs/cluster/Server/Migration/MigrateScanFunctions.cs` | Check `RecordType` during slot scan |
| MOD | `libs/native/bftree-garnet/src/lib.rs` | *(future)* Add `bftree_serialize_to_buffer`, `bftree_deserialize_from_buffer` for migration |

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
8. **DEL/UNLINK** — delete RangeIndex key, verify BfTree freed
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
3. DEL during concurrent RI.SET (proper locking)
