---
id: checkpoint-recovery
sidebar_label: Checkpoint and Recovery
title: Checkpoint and Recovery
---

# Checkpoint and Recovery

Recovery makes three decisions, in order: **which store's devices to open, which hybrid-log checkpoint
to recover, and which index checkpoint can accelerate that recovery**. A Snapshot checkpoint's files
belong to its hybrid-log checkpoint token; there is no separate search for a matching snapshot or a
chain of snapshots to replay.

The flow below describes the unified store, with raw strings and objects in the same hybrid log.
The [metadata format](#file-layout) follows the recovery flow.

## Files and directories

Garnet keeps working logs separately from checkpoint artifacts. With local storage and the default
naming scheme, the relevant layout for storage slot 0 is:

```text
<LogDir>/Store/
├── hlog.<segment>                  shared working hybrid log
└── hlog_objs.<segment>             shared working object log

<CheckpointDir ?? LogDir ?? relative root>/Store/checkpoints/
├── index-checkpoints/
│   └── <index-token>/
│       ├── info.dat.0             IndexRecoveryInfo
│       └── ht.dat.0               hash table and overflow buckets
└── cpr-checkpoints/
    └── <log-token>/
        ├── info.dat.0             HybridLogRecoveryInfo
        ├── snapshot.dat.<segment>     Snapshot hybrid-log image
        └── snapshot.obj.dat.<segment> Snapshot object log, including superseded values
```

`LogDir` defaults to the current directory when the storage tier is enabled. Without that tier,
Garnet uses a `NullDevice` for the working hybrid log; a Snapshot supplies the persisted record images.
`CheckpointBaseDirectory` falls back from `CheckpointDir` to `LogDir`, then to an empty relative root.
The checkpoint manager uses an `INamedDeviceFactory` and an `ICheckpointNamingScheme`, so custom/cloud
devices can represent the same logical names without this local filesystem layout.

Each token is a `Guid`, not a version number. `DefaultCheckpointNamingScheme` maps a token directly
to its directory. Logical device names such as `info.dat` receive a device segment suffix; metadata
uses segment 0, while log and snapshot devices can span segments. Snapshot hybrid-log offsets are
relative to the page containing `snapshotFileLogicalStartAddress`, not to hybrid-log address zero.
Snapshot object-log positions are in that checkpoint's own object-log address space.

For storage slot *n* other than 0, the working devices are `hlog_n` and `hlog_objs_n`, and the checkpoint
directory is `Store/checkpoints_n`. A slot is a stable on-disk identity, not necessarily the current
logical database id after `SWAPDB`; see [database mapping](#databasemapping-and-swapepoch).

A **full checkpoint** writes index and hybrid-log artifacts with the same token. An **index-only**
checkpoint writes an index token; a **log-only** checkpoint writes a hybrid-log token and needs no
matching index token. `CheckpointType.FoldOver` persists records to the working logs and requires no
snapshot files. `CheckpointType.Snapshot` adds the snapshot devices shown above, with object data
when applicable.

AOF files are separate: under `CheckpointDir` (or a relative root), Garnet uses `AOF`, `AOF_n`, and
their log/commit artifacts. AOF recovery and replay follow checkpoint recovery; AOF commit numbers
do not select a hybrid-log checkpoint.

## Recovery entry points

| Request | Selection |
| --- | --- |
| `RecoverAsync()` | Discover the latest readable hybrid-log checkpoint, then a compatible index checkpoint. |
| `RecoverAsync(fullCheckpointToken)` | Open that token directly for both hybrid log and index; no discovery scan. |
| `RecoverAsync(indexCheckpointToken, hybridLogCheckpointToken)` | Open the supplied pair directly. `Guid.Empty` for the index token requests rebuilding the index from the log. |
| `GetLatestCheckpointTokens(...)`, `GetLatestCheckpointVersion()` | Discover the latest checkpoint without loading records. These helpers report its `nextVersion`; `RecoverAsync` reports its `version`. |

**There is no production recovery-by-version-number path in this branch.** To recover a particular
retained checkpoint, use its token. There is no `RecoverAsync(recoverTo: ...)` overload, and its integer
parameter is `numPagesToPreload`, not a version. Store versions (`version`/`nextVersion`), checkpoint
tokens, and the metadata format version (`hybridLogRecoveryVersion`, currently 8) are separate concepts.

At Garnet startup, `GarnetServer.Start` completes recovery before starting network listeners.
For standalone `Recover` startup, `StoreWrapper` recovers checkpoints, reconciles vector state,
recovers the AOF, checks completeness, and replays the AOF. Multi-database recovery discovers storage
slots from `Store/checkpoints*`, recovers each store under its slot, then applies the authoritative
persisted database mapping. AOF directory discovery can add AOF-only databases.

Cluster startup delegates to `ReplicationManager`: a primary recovers local checkpoint/AOF state;
a replica does so only when `ClusterReplicaResumeWithData` is enabled. Disk-based replica sync can
instead supply the transferred index/log tokens explicitly through `RecoverCheckpointAsync` with
`recoverFromToken` set.

`StreamingSnapshot` is a different interface: the host receives a stream of key-value pairs and
restores it by inserting them into an empty store. It is not a chain of `snapshot.dat` files loaded
by the token-based recovery flow described here.

## Discovery and selection

### 1. Open the store's working devices

Garnet constructs each store with its configured `LogDevice` and `ObjectLogDevice`, using the storage
slot's names above. It constructs the checkpoint manager with that slot's checkpoint directory.
**It does not choose a working `hlog` file by timestamp or by snapshot token.** The device maps logical
addresses to its segments; checkpoint metadata subsequently determines which address range is valid.
All checkpoints of this store refer to the same working devices, not private copies of those logs.

### 2. Choose the hybrid-log checkpoint

`FindRecoveryInfo` calls `GetClosestHybridLogCheckpointInfo`. The checkpoint manager's
`GetLogCheckpointTokens` enumerates `cpr-checkpoints` through the named-device factory and extracts
the directory tokens. For each candidate, `HybridLogCheckpointInfo.Recover` reads its `info.dat`
and deserializes `HybridLogRecoveryInfo`, including the format-version and checksum checks.

- **Latest (`requestedVersion == -1`):** examine all candidates and select the greatest metadata
  `version`. The local factory enumerates directories by descending last-write time, but timestamps
  can tie and are not the authority for choosing the latest hybrid-log checkpoint.
- **Explicit token:** bypass enumeration and version ranking; load that token's metadata directly.

The selector also has a **version-targeted branch**, but no production caller uses it:
`GetRecoverVersion(recoverTo)` is a probe called only by a regression test, not by Garnet recovery.
That branch rejects candidates above the requested version, stops at a candidate satisfying
`version <= requestedVersion < nextVersion`, or otherwise selects the greatest `version` not exceeding
the request. The probe returns the selected `version` (0 if selection throws), without loading records.
This code is not an additional recovery sequence or reconstruction of intermediate write history.

During discovery, an unreadable candidate is logged and skipped, allowing an older readable candidate
to win. No eligible candidate causes `TsavoriteNoHybridLogException`, which includes candidate and
unreadable counts. Garnet distinguishes a fresh/AOF-only start from existing but unreadable checkpoints
when checking recovery completeness. Explicit hybrid-log tokens do not fall back to another token.
Nor does discovery promise to retry an older checkpoint if the selected checkpoint's subsequent
geometry validation or data loading fails.

### 3. Choose an index checkpoint

After selecting the hybrid-log checkpoint, `GetClosestIndexCheckpointInfo` enumerates
`index-checkpoints`, skips unreadable metadata, and accepts the **first compatible** candidate:

```text
index.finalLogicalAddress <= hybridLog.recoveredTailAddress
```

Despite the helper's name, this is not a minimum-distance search across index versions. With the local
factory, the first compatible candidate follows the directory timestamp ordering. Index and log
tokens need not match. If no compatible index exists, recovery rebuilds it from the log beginning at
`beginAddress`; an index checkpoint can move the index-repair starting point to its `startLogicalAddress`.
Loading resident pages can still require reading below that point.

For an explicit pair, missing/unreadable index metadata also falls back to rebuilding the index.
A readable but incompatible supplied index causes an error instead. The hybrid-log checkpoint is
required; the index checkpoint is an optimization.

### 4. Open that checkpoint's snapshots

There is **no further closest-match calculation**. `useSnapshotFile` in the selected hybrid-log metadata
determines the recovery mode. For Snapshot, recovery passes that metadata's `guid` to
`GetSnapshotLogDevice` and `GetSnapshotObjectLogDevice`, opening `snapshot.dat` and `snapshot.obj.dat`
under `cpr-checkpoints/<selected-log-token>`. The index token does not affect these paths.
For FoldOver, recovery reads the working logs instead.

Retention matters: the default manager, with `removeOutdated` enabled as in standalone Garnet, keeps
one log checkpoint and two index checkpoints in its cleanup history. After successful recovery,
`OnRecovery` deletes unselected log/index checkpoint directories. Recovering an older token requires
retaining both its checkpoint artifacts and the working-log ranges it references; token APIs are not
an archival-history guarantee.

## Applying the selected checkpoint

The selected metadata describes both the record sources and the restored address boundaries:

| Symbol | Metadata field | Role |
| --- | --- | --- |
| `B` | `beginAddress` | First valid hybrid-log address. |
| `H` | `headAddress` | Initial resident-region boundary; recovery may raise it to fit memory. |
| `S` | `snapshotFileLogicalStartAddress` | Snapshot file's logical base, captured when Snapshot flushing starts. |
| `L` | `snapshotObjectLogStartAddress` | Lowest main-log record image that can reference this checkpoint's snapshot object log; `M` if none. This is a hybrid-log address, not an object-log position. |
| `M` | `mainLogRecoveryEndAddress` | Exclusive end of main-log recovery; Snapshot overlay starts here. |
| `T` | `recoveredTailAddress` | Exclusive recovered tail. |
| `F` | `fuzzyRegionStartAddress` | Start of the region where recovery checks for next-version records. |

For a Snapshot with a nonempty snapshot range, its file represents `[S, T)`, but concurrent ReadOnly
flushes can make `[S, M)` durable on the main log before checkpoint metadata is committed. Recovery
therefore uses **the main log below `M` and the selected snapshot at/above `M`**, not the whole snapshot
file. `M` is capped at `T`, including when reading older checkpoints that did not enforce that cap.
If there is no snapshot suffix, no snapshot record overlay is needed. With a `NullDevice`, `M` stays
at the captured `S` (capped at `T`): a later head movement is not evidence of durable main-log bytes.

`InternalRecoverAsync` validates log geometry and establishes index-repair, scan, and resident ranges.
The scan start accounts for both index repair and resident-page loading; `numPagesToPreload` and the
memory budget affect residency, not which checkpoint is selected. On an already populated store,
recovery resets the store first and requires the caller to quiesce it.

For a current-format Snapshot, the sequence is:

1. Open the main object log for read-only recovery, using `objectLogBeginAddress` and the recorded
   `hlogEndObjectLogTail` as the original bounds. Recovery can inspect durable references beyond
   that tail but cannot append until it has discovered the safe append tail.
2. Read the required main-log pages below `M` and repair/rebuild the index. Include `[L, M)` even
   when index-assisted replay or residency would otherwise skip it. A marked reference in that
   range belongs to the selected snapshot; obsolete marked images outside it are invalidated before
   reading their keys. Full object loading is deferred. With the default `undoNextVersion: true`,
   fuzzy-region next-version records are invalidated and their index entries repaired.
3. During that same walk, retain the highest **main-object-log position**, with its hints, among
   accepted main-owned references. It need not occur in the last hybrid-log record. Establish its
   exact extent end from an exact hint or a length-prefix read, without deserializing its value.
   Resume appends at the greater of this end and the metadata tail. Snapshot-owned positions do not
   participate in this comparison.
4. Repair marked main-log images in `[L, M)`, including disk-only records below the final head.
   Copy their serialized components from the selected snapshot object log to the main object log,
   flush the copies, then persist the new positions/hints and cleared markers. Streamed-out pages
   can require rereading for this repair; there is no separate tail-discovery scan.
5. Overlay only `[M, T)` from the selected snapshot. The snapshot file's page-zero mapping comes from
   the page containing `S`, even though the overlay starts at `M`. If `M` is mid-page, preserve the
   durable main prefix and merge only the snapshot suffix, including when both share an IO sector.
   Snapshot next-version records in the fuzzy region are always invalidated because their objects
   were not included in the checkpoint.
6. Load objects across the **whole final resident range**, including main-log pages below `M`.
   References below `M` address the main object log; those at/above it address the selected snapshot
   object log unless recovery has already copied and repointed them. The boundary page is split at
   the exact address `M`, not merely at its page boundary. Snapshot-region pages evicted during recovery
   are first made durable on the main logs, copying their object data and rewriting their references.
7. Restore the recovered addresses, with tail `T`, the final memory-budget-adjusted head, and the
   read-only/flushed boundary based on `M` and any required recovery flushes. Notify the checkpoint
   manager via `OnRecovery`.

The checkpoint metadata is not rewritten. On a second recovery, already-repaired main-log references
raise the discovered main-object-log tail, protecting their copies from overwrite. A record not yet
repaired still carries its snapshot reference. Object bytes become durable before the corresponding
main-log repointing, so an interrupted repair can resume with either kind of reference.

FoldOver instead recovers through `T` entirely from the working logs and restores its read-only
boundary at `T`. Version 7 object-log recovery requires up-conversion when an object-log device is
configured; Garnet supplies the conversion device through `--upgrade`. That path scans/converts the
whole valid log, not just the resident window. See [object-log serialization](objectlog-serialization.md)
for the capture, object-loading, and conversion details.

## Consecutive snapshots and the shared main log

Consider two successive Snapshot checkpoints, with addresses shown as half-open ranges:

| Checkpoint | Snapshot start `S` | Main-log end `M` | Snapshot/main overlap |
| --- | --- | --- | --- |
| Snapshot 1 | 1000 | 2000 | `[1000, 2000)` |
| Snapshot 2 | 2000 | 2500 | `[2000, 2500)` |

Suppose recovery selects Snapshot 2, its tail is above 2500, and the final head is 500. The resident
immutable main-log region `[500, 2500)` includes **both** overlap ranges. Recovery loads
`[1000, 2000)` and `[2000, 2500)` from the shared main log, then overlays Snapshot 2 only from 2500
to its recovered tail. It does not open Snapshot 1 to interpret the earlier overlap.

The earlier overlap does not require opening Snapshot 1. A record superseded while taking Snapshot 1
is needed in its old form only by Snapshot 1; Snapshot 2 contains its `(v+1)` replacement. A marked,
superseded image outside Snapshot 2's `[L, M)` is obsolete and is rejected before reading even an
overflow key. Positions inside Snapshot 2's ownership range refer to Snapshot 2, not to an arbitrary
earlier snapshot.

If recovery of Snapshot 1 restores the old value as current, it first copies that value into the main
object log and durably repoints its record. A subsequent Snapshot 2 can then use ordinary main-log
references even for immutable records it does not reflush. Deleting Snapshot 1 no longer affects
that restored value. Thus a marker plus checkpoint-level bounds suffices; no capture table or index
is required.

The implementation entry points for this flow are:

- `GarnetServerOptions.GetSettings`, `GetStoreCheckpointDirectory`, and `GarnetServer.CreateStore`:
  device names and checkpoint roots.
- `DatabaseManagerFactory`, `MultiDatabaseManager.RecoverCheckpointAsync`, and
  `SingleDatabaseManager.RecoverCheckpointAsync`: host discovery and recovery entry points.
- Tsavorite's `Index/Recovery/Recovery.cs`: `FindRecoveryInfo`, `GetClosestHybridLogCheckpointInfo`,
  `GetClosestIndexCheckpointInfo`, `InternalRecoverAsync`, and `GetSnapshotPageRangesToRead`.
- `DefaultCheckpointNamingScheme`, `DeviceLogCommitCheckpointManager`, and
  `LocalStorageNamedDeviceFactory`: token enumeration, metadata reads, and snapshot device paths.
- `SnapshotCheckpointSMTask`: sampling `S`, `M`, and `T`; `ObjectAllocator.InspectRecoveryObjectLogReference`
  and `CompleteMainObjectLogRecovery`: append-tail discovery and durable reference repair.

## Checkpoint metadata

The remaining sections describe `cpr-checkpoints/<token>/info.dat`, represented in memory by
`HybridLogRecoveryInfo`.

### File layout

```text
[int32 payload length][payload]
```

The length prefix is written by `DeviceLogCommitCheckpointManager.CommitLogCheckpointMetadata`, which
writes both parts in a single device write so the file is never observed half-updated. The payload
itself is produced by `HybridLogRecoveryInfo.ToByteArray` and consumed by
`HybridLogRecoveryInfo.Initialize(StreamReader)`.

The payload is **line-oriented text**: each field is one `WriteLine`, parsed back in the same order.
Variable-length fields are written as a count followed by that many lines. This makes the format
trivially appendable, which is the basis for the versioning rule below.

### Field order

| Order | Field | Notes |
| --- | --- | --- |
| 1 | checkpoint version | See [Versioning](#versioning) |
| 2 | checksum | See [Checksum](#checksum) |
| 3 | `guid` | Checkpoint token |
| 4 | `useSnapshotFile` | 1 for a snapshot checkpoint, 0 for fold-over |
| 5 | `version`, `nextVersion` | Store version at, and after, the checkpoint |
| 6 | `mainLogRecoveryEndAddress`, `snapshotFileLogicalStartAddress` | |
| 7 | `fuzzyRegionStartAddress`, `recoveredTailAddress` | Bounds of the fuzzy region and the snapshot |
| 8 | fifth address slot | `pageSize` from v8; a duplicate of `recoveredTailAddress` in v7 |
| 9 | `headAddress`, `beginAddress` | |
| 10 | `objectLogBeginAddress` | v7 records the lowest object-log segment in use instead |
| 11 | `hlogEndObjectLogTail`, `snapshotEndObjectLogTail`, `snapshotObjectLogStartAddress` | Object-log tails and the hybrid-log lower bound `L` of snapshot-owned main-log images. v7 records three segment-split position words, `hlogEndObjectLogTail`, `snapshotStartObjectLogTail` and `snapshotEndObjectLogTail`; its ownership range is empty |
| 12 | `segmentSize`, `objectLogSegmentSize`, `objectLogPageSize` | v8 only; written by the store, not the host. Recovery fails if any differs from the store's configuration; the object log's memory size is not recorded and may change between runs |
| 13 | `cookie` | Host-supplied; length, then one line per byte |
| 14 | `databaseMapping` | Host-supplied; length, then one line per entry |
| 15 | `swapEpoch` | Host-supplied |

Fields 13–15 are supplied by the host rather than by the store, and are the only fields a host can
influence. **New host-supplied fields are appended at the end**, so that the host-supplied tail written by an
older build is a strict prefix of the one written by a newer build. Store-written fields are not bound by
that rule: field 12 was inserted ahead of the cookie in v8, so a reader selects the layout from the
checkpoint version rather than reading to the end of the payload.

### Versioning

```csharp
public const int CheckpointVersion = 8;
public const int MinRecoverableCheckpointVersion = 7;
```

`ToByteArray` always stamps `CheckpointVersion`. `Initialize` accepts any version in
`[MinRecoverableCheckpointVersion, CheckpointVersion]` and throws `TsavoriteException` otherwise, so a
store written by a recent-but-older build still recovers. The version that was read is retained in
`hybridLogRecoveryVersion`, letting callers branch on the age of a checkpoint — Garnet uses this to
detect stores written before it gave each logical database its own hybrid log devices.

Fields introduced after `MinRecoverableCheckpointVersion` are read only when the payload is newer than
that version; otherwise they keep the defaults assigned by `Initialize(Guid, long)`. For
`databaseMapping` and `swapEpoch` those defaults — `null` and `0` — mean "identity mapping, no swaps",
which is exactly what a checkpoint predating them implies.

**When adding a field:** append it after the current last field, gate the read on the version, give it
a default that matches how older checkpoints behave, and raise `CheckpointVersion`. Raise
`MinRecoverableCheckpointVersion` only when deliberately dropping the ability to read older
checkpoints.

### Checksum

`Checksum(int cversion)` always covers `guid`, the log addresses, and the object log tail positions,
xored together. What it does with the host-supplied fields (`cookie`, `databaseMapping`, `swapEpoch`)
depends on the version being checksummed:

| Version | Host-supplied fields |
| --- | --- |
| `MinRecoverableCheckpointVersion` (7) | **Excluded.** Checkpoints at this version were written before the fields joined the checksum, so including them would make every one of them fail to verify. |
| Above 7 | **Included**, by xoring in `HostSuppliedFieldsChecksum()`. |

The version gate is what allows one formula to validate payloads at more than one version: each
payload is checksummed the way its own version was written.

`HostSuppliedFieldsChecksum()` folds the fields with FNV-1a rather than xoring them, because they are
sequences rather than scalars — under a plain xor, two entries swapping places, or a value repeated
twice, would leave the checksum unchanged. A null sequence folds identically to an empty one, matching
the serializer, which writes both as a zero length and reads both back as null.

The checksum establishes only that the fields are intact, not that they are meaningful. Semantic
validation stays with the host at the point of use — a mapping that is not a permutation is rejected
by its consumer, not by the checksum.

### Host hooks

`ICheckpointManager` exposes two read hooks, both called from `WriteHybridLogMetaInfo` immediately
before the metadata is committed:

```csharp
byte[] GetCookie();
int[] GetDatabaseMapping(out long swapEpoch);
```

A host that relabels databases supplies the mapping through a third member:

```csharp
void SetDatabaseMappingProvider(Func<(int[] Mapping, long Epoch)> provider);
```

The provider is *queried* while the checkpoint runs rather than pushed on each relabelling, so a
checkpoint always records the mapping that was in force at the moment it ran, with no window in which
the two disagree.

`DeviceLogCommitCheckpointManager` implements `GetCookie` as a virtual no-op returning `null`, and
implements `SetDatabaseMappingProvider` as a virtual no-op that *discards* the provider — a checkpoint
manager whose host does not relabel databases records no mapping, and its metadata is unchanged.
`GetDatabaseMapping` is not virtual: it returns whatever provider a subclass accepted, or `null` and
`0` when none was. Subclasses opt in by overriding the setter to store the provider, which is what
Garnet's `GarnetCheckpointManager` does.

Reading needs no hook: `HybridLogRecoveryInfo.Recover(token, checkpointManager)` deserializes the whole
struct, so every field including the host-supplied ones is available to the caller. An overload also
returns the cookie directly, for convenience.

#### `databaseMapping` and `swapEpoch`

These support hosts that expose several logical databases over one store each, and allow those
databases to be relabelled at runtime (Garnet's `SWAPDB`).

- `databaseMapping[slot] = logicalDatabaseId`, where *slot* is the host's stable, on-disk identity for
  a store and *logical database id* is the index a client sees. `null` or empty means the identity
  mapping.
- The **whole** mapping is recorded in every database's checkpoint, not just that database's own id,
  so the most recent checkpoint describes the entire permutation on its own.
- `swapEpoch` is a monotonic counter the host increments on each relabel. Because checkpoints are
  taken per database and can be taken individually, two checkpoints may carry different mappings; the
  one with the highest epoch is authoritative. Without it, a relabel followed by a partial checkpoint
  would leave two databases claiming the same logical id.
