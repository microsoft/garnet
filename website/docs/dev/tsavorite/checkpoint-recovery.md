---
id: checkpoint-recovery
sidebar_label: Checkpoint Metadata
title: Checkpoint Metadata
---

# Checkpoint Metadata

A Tsavorite checkpoint is a set of files under a checkpoint directory, laid out by an
`ICheckpointNamingScheme`. `DefaultCheckpointNamingScheme` produces:

```text
<base>/
├── index-checkpoints/<token>/
│   ├── info.dat        IndexRecoveryInfo
│   └── ht.dat          hash table
└── cpr-checkpoints/<token>/
    ├── info.dat        HybridLogRecoveryInfo  (described below)
    ├── snapshot.dat    log snapshot, for CheckpointType.Snapshot
    └── snapshot.obj.dat object log snapshot
```

`<token>` is a `Guid`. Every name above is a logical device name; the device layer appends a segment
index, so `info.dat` is stored as `info.dat.0`.

This page describes `cpr-checkpoints/<token>/info.dat` — the log checkpoint metadata, represented in
memory by `HybridLogRecoveryInfo`.

## File layout

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

## Field order

| Order | Field | Notes |
| --- | --- | --- |
| 1 | checkpoint version | See [Versioning](#versioning) |
| 2 | checksum | See [Checksum](#checksum) |
| 3 | `guid` | Checkpoint token |
| 4 | `useSnapshotFile` | 1 for a snapshot checkpoint, 0 for fold-over |
| 5 | `version`, `nextVersion` | Store version at, and after, the checkpoint |
| 6 | `flushedLogicalAddress`, `snapshotStartFlushedLogicalAddress` | |
| 7 | `startLogicalAddress`, `finalLogicalAddress`, `snapshotFinalLogicalAddress` | Bounds of the fuzzy region and the snapshot |
| 8 | `headAddress`, `beginAddress` | |
| 9 | `beginAddressObjectLogSegment` | |
| 10 | `hlogEndObjectLogTail`, `snapshotStartObjectLogTail`, `snapshotEndObjectLogTail` | Object log positions |
| 11 | `cookie` | Host-supplied; length, then one line per byte |
| 12 | `databaseMapping` | Host-supplied; length, then one line per entry |
| 13 | `swapEpoch` | Host-supplied |

Fields 11–13 are supplied by the host rather than by the store, and are the only fields a host can
influence. **New host-supplied fields are appended at the end**, so that a payload written by an older
build is a strict prefix of one written by a newer build.

## Versioning

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

## Checksum

`Checksum()` covers `guid`, the log addresses, and the object log tail positions. It deliberately
**excludes** the host-supplied fields (`cookie`, `databaseMapping`, `swapEpoch`).

That exclusion is what allows one checksum formula to validate payloads at more than one version: a
downlevel payload and a current one describing the same checkpoint produce the same checksum. Host
fields are validated by the host at the point of use instead — for example, a mapping that is not a
permutation is rejected by its consumer rather than by the checksum.

## Host hooks

`ICheckpointManager` exposes two hooks, both called from `WriteHybridLogMetaInfo` immediately before
the metadata is committed:

```csharp
byte[] GetCookie();
int[] GetDatabaseMapping(out long swapEpoch);
```

`DeviceLogCommitCheckpointManager` implements both as virtual no-ops, returning `null` and `0`, so a
host that does not care is unaffected and its metadata is unchanged.

Reading needs no hook: `HybridLogRecoveryInfo.Recover(token, checkpointManager)` deserializes the whole
struct, so every field including the host-supplied ones is available to the caller. An overload also
returns the cookie directly, for convenience.

### `databaseMapping` and `swapEpoch`

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
