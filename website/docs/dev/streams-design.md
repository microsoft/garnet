---
id: streams-design
sidebar_label: Streams
title: Streams
---

# Garnet Streams — Design Document

:::caution Experimental

Stream support in Garnet is **experimental**. APIs, on-disk layout, and recovery semantics are subject to change. Consumer group state is currently in-memory only and is not preserved across restarts.

:::

## Overview

Garnet Streams is a Redis-compatible implementation of the [Redis Streams](https://redis.io/docs/data-types/streams/) data type. Each stream is an append-only log of timestamped entries, indexed by a BTree for efficient range lookups. Consumer groups provide delivery tracking, acknowledgement, and fan-out semantics on top of the raw stream.

### Key design choices

| Decision | Rationale |
|----------|-----------|
| **TsavoriteLog** for entry storage | Append-only, page-aligned, supports recovery, tiered storage, and zero-copy reads via `Span<byte>`. |
| **BTree** for ID → address index | O(log n) range scans, ordered iteration, in-place tombstoning, and prefix trimming — all needed for `XRANGE`/`XREVRANGE`/`XTRIM`. |
| **In-memory consumer groups** on `StreamObject` | Matches Redis semantics (groups are part of the stream object). Durability piggybacks on the stream's own log recovery + future serialization, avoiding split-brain with two persistence systems. |
| **Per-stream TsavoriteLog** (not the main KV store) | Stream entries are variable-length append-only records that don't fit the fixed-size RMW model of the main store. A dedicated log per stream avoids contention and allows independent compaction/trim. |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        RESP Client                                  │
└────────────┬────────────────────────────────────────────────────────┘
             │  XADD / XRANGE / XREADGROUP / XACK / ...
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  RespServerSession  (libs/server/Resp/StreamCommands.cs)            │
│  • Parses arguments from network buffer                             │
│  • Calls StreamManager methods                                      │
│  • Formats RESP responses via RespMemoryWriter                      │
└────────────┬────────────────────────────────────────────────────────┘
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  StreamManager  (libs/server/Stream/StreamManager.cs)               │
│  • Dictionary<byte[], StreamObject> — one entry per stream key      │
│  • SingleWriterMultiReaderLock for dictionary mutations              │
│  • Creates StreamObject on first XADD (unless NOMKSTREAM)           │
│  • Recovery: enumerates hex-named subdirectories, replays each log  │
│  • Forwards all operations to the appropriate StreamObject           │
└────────────┬────────────────────────────────────────────────────────┘
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  StreamObject  (libs/server/Stream/Stream.cs)                       │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────────┐   │
│  │ TsavoriteLog │  │   BTree      │  │   Consumer Groups       │   │
│  │ (append-only │  │ (StreamID →  │  │   (in-memory, per-group │   │
│  │  entry log)  │  │  log address)│  │    PEL + consumers)     │   │
│  └──────────────┘  └──────────────┘  └─────────────────────────┘   │
│  • SingleWriterMultiReaderLock guards all mutations                  │
│  • waitForCommit: sync flush after every write when AOF enabled     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## On-Disk Layout

When `--stream-log-dir` is set, each stream gets its own subdirectory named by the hex-encoded key bytes:

```
<stream-log-dir>/
  ├── 6D7973747265616D/          # hex("mystream")
  │   ├── streamLog.0            # TsavoriteLog segment 0
  │   ├── streamLog.1            # TsavoriteLog segment 1
  │   └── streamLog.commit       # commit metadata
  ├── 6F74686572/                # hex("other")
  │   └── ...
```

When `--stream-log-dir` is not set, all streams use `NullDevice` (in-memory only, no durability).

### Log record format

Every record in the TsavoriteLog starts with a fixed 20-byte header:

```
StreamLogEntryHeader (20 bytes, Pack=1)
┌────────────────────────────────────────────┐
│  StreamID id       (16 bytes)              │  ← 8-byte ms timestamp + 8-byte sequence
│  int      numPairs (4 bytes)               │  ← ≥0 = data record; <0 = control record
└────────────────────────────────────────────┘
```

**Data records** (`numPairs ≥ 0`): header is followed by the raw field-value pairs as a contiguous byte span (the original RESP payload bytes).

**Control records** (`numPairs < 0`):
- `Tombstone (-1)`: marks a deleted entry. The `id` field identifies which entry was deleted. No payload follows the header.

---

## Core Operations

### XADD — Add an entry

```
XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] *|id field value [field value ...]
```

1. **ID resolution**: The `parseIDString` method handles four formats:
   - `*` — auto-generate `{currentTimeMs}-{seq}`, incrementing seq if ms matches `lastId`
   - `{ms}-*` — auto-generate sequence for given timestamp
   - `{ms}` — shorthand for `{ms}-0`
   - `{ms}-{seq}` — explicit full ID
2. **Monotonicity check**: New ID must be strictly greater than `lastId`.
3. **Log append**: `log.Enqueue<StreamLogEntryHeader>(header, rawFieldValuePairs, out logicalAddress)`
4. **Index insert**: `index.Insert(id, Value(logicalAddress))`
5. **Commit**: If `waitForCommit`, calls `log.Commit(spinWait: true)`.
6. **Response**: Returns the generated ID as a bulk string (e.g., `"1526919030474-55"`).

### XRANGE / XREVRANGE — Range scan

```
XRANGE key start end [COUNT count]
XREVRANGE key end start [COUNT count]
```

1. **Parse bounds**: `-` = minimum, `+` = maximum, or explicit `ms-seq` IDs.
2. **BTree range query**: `index.Get(startKey, endKey, out startAddr, out endAddr, out tombstones, limit, reverse)` returns log addresses and a tombstone set for deleted entries.
3. **Log scan**: `log.Scan(clampedStart, endAddr)` iterates records within the address window.
4. **Filter**: Skip control records (negative `numPairs`) and tombstoned IDs.
5. **RESP output**: Each entry is `*2 $id *{2*numPairs} $field $value ...`. The outer array holds all matching entries.

### XDEL — Delete entries

```
XDEL key id [id ...]
```

1. **BTree delete**: `index.Delete(id)` — marks the leaf entry as a tombstone (sets `Value.valid = 0`).
2. **Log tombstone**: Enqueues a `StreamLogEntryHeader` with `numPairs = ControlRecordKind.Tombstone` so recovery replays the delete.
3. **Commit**: If `waitForCommit`, flushes synchronously.

### XTRIM — Trim the stream

```
XTRIM key MAXLEN|MINID [=|~] threshold
```

1. **BTree trim**: `TrimByLength` or `TrimByID` — walks leaves from head, tombstoning entries until the threshold is met. Returns the new head address.
2. **Log truncation**: `log.TruncateUntil(newHeadAddress)` — discards all pages before the new head. This is the persistence of the trim — recovery simply won't see the truncated records.
3. **Commit**: If `waitForCommit`, flushes synchronously.

### XLEN — Stream length

Returns `index.Count` (number of non-tombstoned entries in the BTree).

### XLAST — Last entry

Returns the most recent entry by looking up `lastId` in the BTree and reading from the log.

---

## BTree Index

The BTree (`libs/server/BTreeIndex/`) is a B+Tree implemented over fixed-size 4KB pages allocated via `NativeMemory.AlignedAlloc`. It provides ordered access to stream entries.

### Key/Value layout

| Field | Size | Description |
|-------|------|-------------|
| **Key** | 16 bytes | `StreamID` — 8-byte ms timestamp + 8-byte sequence, stored in big-endian for correct lexicographic ordering |
| **Value** | 9 bytes | `byte valid` (1=live, 0=tombstoned) + `ulong address` (TsavoriteLog logical address) |

### Capacities (4KB page)

- **Leaf nodes**: `(4096 - header) / (16 + 9)` entries per leaf, linked for sequential scan
- **Internal nodes**: `(4096 - header) / (16 + 8)` child pointers per node

### Operations

| Operation | Complexity | Description |
|-----------|-----------|-------------|
| `Insert(key, value)` | O(log n) | Append-optimized fast path when key > all existing keys (common case for streams) |
| `Delete(key)` | O(log n) | In-place tombstone (sets `valid = 0`), no structural removal |
| `Get(start, end, ...)` | O(log n + k) | Range scan returning addresses + tombstone set; supports forward/reverse and count limit |
| `TrimByLength(maxLen)` | O(t) | Walk leaves from head, tombstone entries exceeding the length limit |
| `TrimByID(minId)` | O(t) | Walk leaves from head, tombstone entries with ID < minId |
| `FirstAlive()` | O(k) | Scan from head to find the first non-tombstoned entry |

---

## Consumer Groups

Consumer groups enable multiple consumers to cooperatively process a stream with at-least-once delivery semantics. Each group tracks:
- **Which entries have been delivered** (`LastDeliveredId`)
- **Which entries are pending acknowledgement** (the PEL — Pending Entries List)
- **Which consumer owns each pending entry**

### Data Model

```
StreamObject
  └── consumerGroups: Dictionary<string, ConsumerGroup>

ConsumerGroup
  ├── Name: string
  ├── LastDeliveredId: StreamID        — high-water mark for delivery
  ├── EntriesRead: long                — total entries ever delivered
  ├── PEL: SortedList<StreamID, PendingEntry>  — all pending entries
  └── Consumers: Dictionary<string, StreamConsumer>
        └── StreamConsumer
              ├── Name: string
              ├── SeenTime: long       — last interaction timestamp (ms)
              └── PendingIds: SortedSet<StreamID>  — this consumer's pending IDs

PendingEntry
  ├── Id: StreamID
  ├── ConsumerName: string
  ├── DeliveryTime: long               — when first/last delivered (ms)
  └── DeliveryCount: int               — re-delivery counter
```

All consumer group state is in-memory, protected by the owning `StreamObject`'s `SingleWriterMultiReaderLock`. Thread safety is achieved by requiring callers to hold the stream lock before any group mutation.

### XGROUP — Group management

```
XGROUP CREATE key group id-or-$ [MKSTREAM]
XGROUP SETID key group id-or-$ [ENTRIESREAD n]
XGROUP DESTROY key group
XGROUP CREATECONSUMER key group consumer
XGROUP DELCONSUMER key group consumer
```

- **CREATE**: Initializes a new `ConsumerGroup` with `LastDeliveredId` set to the given ID (or the stream's current `lastId` if `$`). `MKSTREAM` creates the stream if it doesn't exist.
- **SETID**: Resets the group's delivery cursor. Optional `ENTRIESREAD` sets the counter for lag calculation.
- **DESTROY**: Removes the group and all its state.
- **CREATECONSUMER / DELCONSUMER**: Explicitly manage consumers within a group.

### XREADGROUP — Consume entries

```
XREADGROUP GROUP group consumer [COUNT count] [NOACK] STREAMS key [key ...] id [id ...]
```

Two modes based on the ID argument:

| ID | Behavior |
|----|----------|
| `>` | **New entries**: Delivers entries after `LastDeliveredId`. Advances the cursor. Adds entries to the consumer's PEL (unless `NOACK`). |
| `0` or specific ID | **Pending replay**: Returns entries from the consumer's own PEL starting at the given ID. Does not advance the cursor. |

**New entry delivery flow**:
1. Look up entries in BTree after `LastDeliveredId`, limited by `COUNT`.
2. For each entry, read from the log and format as RESP.
3. Unless `NOACK`: create a `PendingEntry`, add to group PEL and consumer's `PendingIds`.
4. Update `LastDeliveredId` to the last delivered entry's ID.
5. Increment `EntriesRead`.

**Response format** (always multi-stream):
```
*1                          ← number of streams
  *2
    $8 mystream             ← stream key
    *N                      ← number of entries
      *2
        $15 1526919030474-0 ← entry ID
        *4 $f1 $v1 $f2 $v2 ← field-value pairs
```

### XACK — Acknowledge entries

```
XACK key group id [id ...]
```

Removes entries from the group PEL and the owning consumer's `PendingIds`. Returns the count of successfully acknowledged IDs.

### XPENDING — Inspect pending entries

```
XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
```

**Summary form** (no start/end):
```
*4
  :pending-count
  $first-pending-id
  $last-pending-id
  *N [*2 $consumer :count]   ← per-consumer breakdown
```

**Detail form** (with start/end/count): Returns individual pending entries, optionally filtered by `IDLE` time and consumer name.

### XCLAIM — Transfer ownership

```
XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT n] [FORCE] [JUSTID]
```

Transfers pending entries from one consumer to another. Only claims entries idle for ≥ `min-idle-time` (unless `FORCE`). Options:
- `IDLE`: Override the idle time in the PEL entry
- `TIME`: Set the last delivery time
- `RETRYCOUNT`: Set the delivery counter
- `FORCE`: Claim even if the entry isn't in the PEL (creates a new PEL entry)
- `JUSTID`: Return only IDs, not full entry data

### XAUTOCLAIM — Automatic idle entry reclaim

```
XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
```

Scans the PEL from `start`, claims entries idle for ≥ `min-idle-time`, and returns:
```
*3
  $next-cursor-id    ← resume point for next call (0-0 if done)
  *N [entries...]    ← claimed entries (or just IDs if JUSTID)
  *M [deleted-ids]   ← IDs that were in PEL but no longer in the stream
```

### XINFO — Introspection

```
XINFO STREAM key
XINFO GROUPS key
XINFO CONSUMERS key group
```

Returns metadata about the stream, its groups, or a group's consumers.

### XREAD — Multi-stream read (without groups)

```
XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
```

Reads new entries from one or more streams starting after the given IDs. `BLOCK` is parsed but not implemented (returns immediately). Response format matches `XREADGROUP`.

---

## Durability

Stream durability has three tiers depending on server configuration:

### Tier 1: In-memory only (default)

When `--stream-log-dir` is not set, streams use `NullDevice`. All data lives in memory. No persistence.

### Tier 2: Periodic flush

When `--stream-log-dir` is set and `--enable-aof` is on:
- Each stream's TsavoriteLog writes to real disk segments.
- The periodic `CommitTask` (configured via `--aof-commit-freq`) commits both the main AOF and all stream logs at the configured interval.
- Checkpoints also flush stream logs (via `streamManager.CommitAsync()`).
- **Possible data loss window**: entries written between the last commit and a crash.

### Tier 3: Wait-for-commit (strong consistency)

When `--enable-aof` and `--aof-commit-wait` are both set:
- Every write operation (`XADD`, `XDEL`, `XTRIM`) calls `log.Commit(spinWait: true)` after the `Enqueue`, synchronously flushing to disk and waiting for completion before returning a response.
- **No data loss window**, but higher latency per operation.

### Configuration flow

```
GarnetServerOptions.EnableAOF && WaitForCommit
    ↓
StoreWrapper constructor
    ↓
StreamManager(waitForCommit: true)
    ↓
StreamObject(waitForCommit: true)
    ↓
log.Commit(spinWait: true) after each Enqueue
```

### Recovery

On server restart with `--stream-log-dir` set:

1. `StreamManager.Recover()` enumerates hex-named subdirectories.
2. For each directory, opens a `StreamObject` with `recover: true`.
3. `StreamObject.RebuildIndexFromLog()` scans the TsavoriteLog from `BeginAddress` to `TailAddress`:
   - **Data records** (`numPairs ≥ 0`): inserted into the BTree with `index.Insert(id, address)`.
   - **Tombstone records** (`numPairs == -1`): replayed as `index.Delete(id)`.
   - Updates `lastId` and `totalEntriesAdded` to match the recovered state.
4. Consumer group state is **not** recovered from the log (in-memory only). Groups must be recreated after restart.

---

## Command Reference

| Command | Status | Handler | Description |
|---------|--------|---------|-------------|
| `XADD` | ✅ | `StreamAdd` | Append entry |
| `XLEN` | ✅ | `StreamLength` | Entry count |
| `XRANGE` | ✅ | `StreamRange` | Forward range scan |
| `XREVRANGE` | ✅ | `StreamRange(isReverse)` | Reverse range scan |
| `XDEL` | ✅ | `StreamDelete` | Delete entries by ID |
| `XTRIM` | ✅ | `StreamTrim` | Trim by MAXLEN or MINID |
| `XLAST` | ✅ | `StreamLast` | Last entry (Garnet extension) |
| `XGROUP` | ✅ | `StreamGroup` | CREATE/SETID/DESTROY/CREATECONSUMER/DELCONSUMER |
| `XREADGROUP` | ✅ | `StreamReadGroup` | Consumer group read |
| `XACK` | ✅ | `StreamAck` | Acknowledge entries |
| `XPENDING` | ✅ | `StreamPending` | Inspect pending entries |
| `XCLAIM` | ✅ | `StreamClaim` | Transfer entry ownership |
| `XAUTOCLAIM` | ✅ | `StreamAutoClaim` | Auto-reclaim idle entries |
| `XINFO` | ✅ | `StreamInfoCmd` | STREAM/GROUPS/CONSUMERS introspection |
| `XREAD` | ✅ | `StreamRead` | Multi-stream read (no groups) |
| `BLOCK` | ⏳ | — | Parsed but ignored (returns immediately) |

---

## File Map

| Path | Purpose |
|------|---------|
| `libs/server/Stream/Stream.cs` | `StreamObject` — core stream logic, entry operations, consumer group operations |
| `libs/server/Stream/StreamManager.cs` | `StreamManager` — stream lifecycle, dictionary, forwarding, recovery |
| `libs/server/Stream/ConsumerGroup.cs` | `ConsumerGroup`, `StreamConsumer`, `PendingEntry` data model |
| `libs/server/BTreeIndex/BTree.cs` | BTree root — create, insert dispatch |
| `libs/server/BTreeIndex/BTreeInsert.cs` | Insert with leaf/node splitting |
| `libs/server/BTreeIndex/BTreeDelete.cs` | In-place tombstone delete |
| `libs/server/BTreeIndex/BTreeLookup.cs` | Range query, FirstAlive |
| `libs/server/BTreeIndex/BTreeTrim.cs` | TrimByLength, TrimByID |
| `libs/server/BTreeIndex/BTreeInternals.cs` | Page layout, Key/Value structs, node structures |
| `libs/server/Resp/Parser/RespCommand.cs` | `RespCommand` enum + fast parse entries for stream commands |
| `libs/server/Resp/StreamCommands.cs` | All RESP handlers for stream commands |
| `libs/server/Resp/RespServerSession.cs` | Dispatch switch (`ProcessArrayCommands`) |
| `libs/server/StoreWrapper.cs` | StreamManager creation, commit task integration, checkpoint hooks |
| `libs/server/Servers/GarnetServerOptions.cs` | Stream configuration (log dir, page size, memory size) |
| `test/Garnet.test/RespStreamTests.cs` | Stream unit tests |
| `playground/StreamBench/Program.cs` | Stream benchmark tool |
