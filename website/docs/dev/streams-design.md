---
id: streams-design
sidebar_label: Streams
title: Streams
---

# Garnet Streams — Design Document

:::caution Experimental

Stream support in Garnet is **experimental**. APIs, on-disk layout, and recovery semantics are subject to change. Streams are modeled as a first-class object type (`GarnetObjectType.Stream`) in the unified store, so consumer-group state is now captured by the object checkpoint and **preserved across restarts**.

**Streams are not supported in cluster mode.** All stream commands (`XADD`, `XREAD`, `XREADGROUP`, `XGROUP`, etc.) are implemented only for the standalone (single-node) configuration. Slot routing, key migration, and replication of stream entries are not yet wired up — running stream commands against a node started with `--cluster` is not supported.

:::

## Overview

Garnet Streams is a Redis-compatible implementation of the [Redis Streams](https://redis.io/docs/data-types/streams/) data type. Each stream is an append-only log of timestamped entries, indexed by a BTree for efficient range lookups. Consumer groups provide delivery tracking, acknowledgement, and fan-out semantics on top of the raw stream.

### Key design choices

| Decision | Rationale |
|----------|-----------|
| **Stream is an `IGarnetObject`** (`GarnetObjectType.Stream`) in the unified store | A stream key is a first-class object like Hash/List/Set/SortedSet, so it inherits the store's keyspace semantics (TYPE, DEL/UNLINK, EXPIRE, FLUSHDB/FLUSHALL), checkpoint/recovery, and per-record locking for free. |
| **Operate-dispatch for all commands** | `XADD`/`XRANGE`/`XREADGROUP`/`XACK`/… build an `ObjectInput` (header = `GarnetObjectType.Stream` + a `StreamOperation`) and run through the object store RMW/Read path into `StreamObject.Operate`, so the store mediates liveness/eviction/locking (no detached references). |
| **TsavoriteLog** for entry storage (internal to the object) | Append-only, page-aligned, supports recovery, tiered storage, and zero-copy reads via `Span<byte>`. The `StreamObject` owns a per-stream log; the log is committed during the object's checkpoint serialization. |
| **BTree** for ID → address index (internal to the object) | O(log n) range scans, ordered iteration, in-place tombstoning, and prefix trimming — all needed for `XRANGE`/`XREVRANGE`/`XTRIM`. Rebuilt from the per-stream log when the object is deserialized. |
| **Consumer groups serialized into the object blob** | `DoSerialize`/`(BinaryReader)` round-trip the groups, PEL, consumers, last-delivered-id, and entries-read, so the checkpoint/AOF path makes consumer-group state durable across restarts. |
| **Default database (DB 0) only** | Streams are stored in the default database's unified store; they are not per-database and remain unsupported in cluster mode. |

> **Note on durability coordination:** stream *entries* live in the per-stream `TsavoriteLog` and are committed when the object is serialized during a store checkpoint (`StreamObject.DoSerialize` calls `log.Commit`). On recovery the object is deserialized — it re-opens its per-stream log and rebuilds the BTree from it. `DEL`/`UNLINK`/`EXPIRE` route through the store and `GarnetRecordTriggers.OnDispose` (never fired on eviction) closes the per-stream log and removes its on-disk directory; `FLUSHDB`/`FLUSHALL` close all live stream log handles before sweeping the per-stream directories.

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
│  • Builds ObjectInput (header = GarnetObjectType.Stream + StreamOp) │
│  • Calls the DB 0 object store API (StorageSession.StreamObject*)   │
│  • Formats RESP responses via RespMemoryWriter                      │
└────────────┬────────────────────────────────────────────────────────┘
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Unified object store (DB 0)  —  ObjectSessionFunctions RMW/Read    │
│  • Stream key is a record whose value is a StreamObject             │
│  • InitialUpdater creates the StreamObject from the record key      │
│    (StreamObject.CreateForKey) on first XADD / XGROUP CREATE MKSTREAM│
│  • InPlaceUpdater/Reader dispatch into StreamObject.Operate          │
│  • Checkpoint/AOF persist the object; OnDispose handles DEL cleanup  │
└────────────┬────────────────────────────────────────────────────────┘
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  StreamObject : GarnetObjectBase  (libs/server/Stream/Stream.cs)    │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────────┐   │
│  │ TsavoriteLog │  │   BTree      │  │   Consumer Groups       │   │
│  │ (append-only │  │ (StreamID →  │  │   (PEL + consumers;      │   │
│  │  entry log)  │  │  log address)│  │    serialized to blob)   │   │
│  └──────────────┘  └──────────────┘  └─────────────────────────┘   │
│  • Operate(StreamOperation) executes every command under the store's    │
│    per-record lock                                                       │
│  • DoSerialize/(BinaryReader): metadata + consumer groups (+ log    │
│    commit); deserialize re-opens the log and rebuilds the BTree      │
└─────────────────────────────────────────────────────────────────────┘

StreamObjectConfig (a static holder in libs/server/Stream/Stream.cs) publishes
the server-global stream config (log dir, page/memory size) for the
deserialization factory, tracks live stream instances, and performs
FLUSHDB/FLUSHALL directory cleanup.
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
5. **Durability**: The mutation is logged to the main AOF as an `ObjectStoreRMW` entry (waited before responding in `--aof-commit-wait` mode); the per-stream log is committed to disk at the next object-store checkpoint (see Persistence below). There is no per-write `log.Commit`.
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
3. **Durability**: As with XADD, the delete is captured by the main AOF and persisted to the per-stream log at the next object-store checkpoint.

### XTRIM — Trim the stream

```
XTRIM key MAXLEN|MINID [=|~] threshold
```

1. **BTree trim**: `TrimByLength` or `TrimByID` — walks leaves from head, tombstoning entries until the threshold is met. Returns the new head address.
2. **Log truncation**: `log.TruncateUntil(newHeadAddress)` — discards all pages before the new head. This is the persistence of the trim — recovery simply won't see the truncated records.
3. **Durability**: As with XADD, the trim is captured by the main AOF and persisted to the per-stream log at the next object-store checkpoint.

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

All consumer group state is in-memory and serialized into the object blob at checkpoint. Thread safety comes from the unified object store: each command runs through the store's RMW/Read path, which holds the per-record lock (and epoch protection) for the duration of `StreamObject.Operate`. The object therefore needs no internal lock of its own — like every other Garnet object type (Hash/List/Set/SortedSet).

The per-consumer `PendingIds` set is an *index* into the group PEL, not an independent source of truth. Every PEL mutation (insert, claim, delete) must keep both views consistent — that invariant is enforced by funnelling all mutations through the same `ConsumerGroup` methods rather than mutating the structures directly.

### Delivery Semantics

Consumer groups implement **at-least-once** delivery: once a message is handed to a consumer it stays in the group's PEL — and remains eligible for re-delivery — until something explicitly removes it (`XACK`, `XGROUP DESTROY`, `XGROUP DELCONSUMER`, or stream-side deletion via `XDEL`/`XTRIM` reconciled through `XAUTOCLAIM`). Application processors must therefore be idempotent or otherwise tolerate duplicates.

#### Message state machine

A single stream entry, from the perspective of one consumer group, moves through these states:

```
                  XREADGROUP ... > (id=">")
                  ─────────────────────────────►
   ┌──────────────┐                              ┌──────────────────┐
   │ undelivered  │                              │  pending         │
   │ (in stream,  │                              │  (owner = C1,    │
   │  ID > group  │                              │   delivered at T,│
   │  last-ID)    │                              │   count = 1)     │
   └──────────────┘                              └────────┬─────────┘
                                                          │
                  XACK key group id                       │
                  ◄───────────────────────────────────────┤
                                                          │
                                                          │ XCLAIM / XAUTOCLAIM
                                                          │ (idle ≥ min-idle-time)
                                                          ▼
                                                ┌──────────────────┐
                                                │  pending         │
                                                │  (owner = C2,    │
                                                │   delivered at T',│
                                                │   count = 2)     │
                                                └──────────────────┘
```

A pending entry is always owned by *exactly one* consumer. Ownership transfers happen atomically while the store holds the per-record lock during `Operate`.

#### Three PEL transitions

| Transition | Trigger | Effect on PEL |
|------------|---------|---------------|
| **Insert** | `XREADGROUP ... STREAMS key >` (and not `NOACK`) | New `PendingEntry(id, consumer, now, 1)` added to group PEL + consumer's `PendingIds`. Group `LastDeliveredId` and `EntriesRead` advance. |
| **Remove** | `XACK key group id` | Entry removed from group PEL and from the owning consumer's `PendingIds`. Returns the count of IDs actually removed (unknown IDs are silently ignored). |
| **Transfer** | `XCLAIM` / `XAUTOCLAIM` | Entry's `ConsumerName` updated; entry moves between per-consumer `PendingIds` sets; `DeliveryTime` touched; `DeliveryCount` incremented (skipped under `JUSTID`). |

`XGROUP DELCONSUMER` is a fourth, coarser PEL mutation: it removes the consumer entirely and drops every PEL entry it owned. The return value is the number of pending entries that were discarded, which is what callers use to decide whether the consumer's outstanding work needs to be claimed elsewhere *before* deletion.

#### The two modes of XREADGROUP

The ID argument to `XREADGROUP` selects between two completely different code paths:

| ID | Mode | Cursor advances? | PEL entries created? |
|----|------|------------------|----------------------|
| `>` | **New delivery** — fetch entries with ID > `LastDeliveredId` from the stream's BTree | Yes | Yes (one per delivered entry, unless `NOACK`) |
| `0` or specific ID | **Pending replay** — return the *calling consumer's* own PEL entries with ID ≥ the given ID | No | No |

Replay mode is the recovery primitive: a consumer that just restarted does `XREADGROUP GROUP g me COUNT N STREAMS key 0` to re-fetch the payloads of everything it still owes an ack on. Replay never crosses consumer boundaries — to see another consumer's pending work, you have to `XPENDING` (read-only) or `XCLAIM` (transfer ownership first, then replay).

`NOACK` on `>` mode opts out of the PEL insert step entirely — fire-and-forget delivery with no at-least-once guarantee. Useful only when the application has its own external tracker.

#### Why claim is decoupled from delivery

Claim and delivery are deliberately orthogonal operations:

- **Delivery** (`XREADGROUP >`) is driven by the *stream's* contents and the *group's* read cursor. It produces PEL entries as a side effect.
- **Claim** (`XCLAIM` / `XAUTOCLAIM`) is driven by *external* knowledge that a consumer is dead or stalled. It only touches the PEL — it doesn't move `LastDeliveredId`, doesn't read from the stream's BTree (it just looks up payloads for already-known IDs), and doesn't require the original owner's cooperation.

The `min-idle-time` parameter on claim is a *coordination convention* between consumers, not a liveness check: Garnet doesn't track consumer health, so two consumers must agree on "if a message has been idle ≥ N ms, it's fair game to claim." Set it conservatively — too short, and healthy slow processors get their work stolen.

`FORCE` and `JUSTID` are the two options worth special attention:

- `FORCE` lets you manufacture a PEL entry for a stream ID that was never actually delivered to this group (the entry must still exist in the stream). Used for reconciling external state with the PEL — uncommon in normal operation.
- `JUSTID` skips the `DeliveryCount` increment in addition to omitting the payload. This is the "claim without blame" semantic: useful for periodic rebalancing where you don't want to make legitimate messages look like poison pills.

#### The typical reliable-consumer loop

The four operations compose into a standard consumer pattern:

```text
on startup:
    # Recover anything I left pending last time I was alive.
    while (entries := XREADGROUP GROUP g me COUNT N STREAMS key 0):
        process(entries); XACK key g <ids>

main loop:
    entries := XREADGROUP GROUP g me COUNT N STREAMS key >
    process(entries); XACK key g <ids>

recovery thread (periodic, e.g. every 30 s):
    cursor := 0-0
    repeat:
        (next, claimed, evicted) := XAUTOCLAIM key g me 60000 cursor COUNT 100
        process(claimed); XACK key g <claimed-ids>
        cursor := next
    until next == 0-0
```

The recovery thread is the failover mechanism. `XAUTOCLAIM`'s third return value — the list of IDs that were in the PEL but no longer exist in the stream (deleted by `XDEL` or trimmed) — is *automatically* evicted from the PEL as a side effect of the scan. This is the only thing that keeps the PEL consistent with the underlying stream over time, so even healthy systems benefit from running `XAUTOCLAIM` periodically.

The `DeliveryCount` field is the standard signal for poison-pill handling: when it crosses an application-defined threshold, divert to a dead-letter handler (`XADD dlq ...` then `XACK` the original) instead of re-processing.

#### Implementation notes specific to Garnet

- **Consumer group state is persisted via the object-store checkpoint.** Groups, consumers, PELs, `LastDeliveredId`, and `EntriesRead` are serialized into the `StreamObject` blob by `DoSerialize` and restored by the `(BinaryReader)` constructor on recovery, alongside the stream entries (which recover from the on-disk TsavoriteLog). A server restart no longer loses consumer-group state.
- **The store's per-record lock is held for the entire duration of an `XREADGROUP >` call**, including the BTree range scan and log reads. This keeps the PEL insert and `LastDeliveredId` advance atomic with the delivery decision, at the cost of serializing concurrent commands against the same stream key. For workloads with many consumers on one stream this is the dominant contention point and may need to be revisited.
- **`XPENDING` does not take an idle filter into the summary form** — the `IDLE` clause is only honoured in the detail form (with `start`/`end`/`count`). This matches Redis behaviour but is easy to overlook.
- **`XAUTOCLAIM`'s pagination cursor is a stream ID, not an opaque token.** Callers must pass `0-0` to start and the returned next-id to continue; a value of `0-0` in the response means the scan is complete.
- **Per-consumer `PendingIds` is a `SortedSet<StreamID>`** for fast ordered iteration during replay. Mutations are O(log n); access is serialized by the store's per-record lock.

#### Pitfalls

- **Trimmed-but-pending entries**: `XTRIM` and `XDEL` do not consult any group's PEL — they're stream-level operations. The PEL will keep a reference to a deleted ID until either `XACK` or `XAUTOCLAIM` (which evicts via its third return slot) removes it. The payload is gone, but the ID lingers; queries against the consumer's replay (`XREADGROUP ... 0`) will skip those entries silently.
- **Silent `XACK`**: acknowledging an unknown ID returns 0 in the count but does not error. A typo in the group name doesn't fail — it just acks nothing. Validate at the application layer if this matters.
- **`JUSTID` and the delivery counter**: forgetting that `JUSTID` skips the counter increment means rebalance operations can permanently mask poison messages.
- **`XGROUP DELCONSUMER` is destructive**: the count it returns is the number of pending entries *lost*, not transferred. Always claim a consumer's pending work to another consumer before deleting it, unless those messages really are abandonable.

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
- Stream mutations (`XADD`, `XDEL`, …) run as object-store RMW operations, so they are also recorded in the main AOF and replayed on recovery (re-applied after the checkpoint-restored state).
- Checkpoints flush each live stream's log as part of serializing its `StreamObject` (`StreamObject.DoSerialize` calls `log.Commit`).
- **Possible data loss window**: entries written between the last commit/checkpoint and a crash (when AOF is off).

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

## Limitations

The current implementation is feature-complete enough to run real workloads on a standalone server, but a number of known gaps remain. They are listed here so the call sites are easy to find when picking up the work.

### Cluster mode is unsupported

Stream commands are only supported on a node started in **standalone** mode. Running them against a node started with `--cluster` is not currently supported because:

- The stream-command path does not consult the cluster's slot map. There is no slot routing, `MOVED`/`ASK` redirection, or cross-shard error handling for stream keys.
- The per-stream TsavoriteLog directories live under a single `--stream-log-dir` and are not partitioned by slot, so a slot migration would have no way to move a stream's on-disk state with it.
- The replication AOF replay path does not currently understand stream records — secondaries will not see `XADD`/`XDEL`/`XTRIM` mutations propagated from the primary.
- Consumer group state is not propagated over the replication stream, so even if entries were replicated, group cursors, PELs, and consumer membership would diverge between primary and replica.

Wiring up streams for cluster mode requires, at minimum: slot-aware key routing for stream keys, slot-tagged subdirectories in the on-disk layout, an AOF entry type for stream mutations, and a migration handshake to move a stream's log + BTree + consumer group state atomically between nodes. None of this is in place today.

### Consumer group state is not replicated to replicas

Consumer group state (groups, consumers, PELs, `LastDeliveredId`, `EntriesRead`) **is persisted across restarts** — it is serialized into the stream object's checkpoint blob and restored on recovery (see Persistence). What is not yet handled is propagating that state to replicas over the replication stream; see the cluster limitation above.

### `BLOCK` is parsed but not implemented

`XREAD` and `XREADGROUP` accept the `BLOCK milliseconds` option for compatibility but ignore it: the call returns immediately whether or not entries are available. Implementing blocking requires hooking streams into the `CollectionItemBroker` (the same mechanism used by `BLPOP`/`BRPOP`).

### `XTRIM` near-exact trimming (`~`) and `LIMIT` are not supported

Only exact trimming is implemented. The `~` modifier (approximate trimming, "trim at least this much, but trim more if it's cheap") and the `LIMIT count` cap are parsed but treated as exact / unlimited respectively.

### `XSETID` and the `*-HELP` sub-commands are not implemented

`XSETID` (override a stream's `lastId` and entry counters) is currently absent; the equivalent control on a group is available via `XGROUP SETID`. The `XGROUP HELP` and `XINFO HELP` sub-commands return an error instead of a help table.

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
