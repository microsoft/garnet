# HANDOFF - Garnet data plane under Redis/Valkey Sentinel

**Status:** standalone Garnet-to-Garnet replication is functional and pushed.
This is a temporary engineering handoff; remove it before an upstream merge.

**Implementation HEAD before this handoff:** `f2c59fe78`
(`[Resp] Pool standalone replication frame buffers`)

## 1. Goal and scope

The target topology is:

- One standalone Garnet primary.
- One or more standalone Garnet replicas.
- Stock Redis or Valkey Sentinel as the control plane.
- Garnet's native checkpoint and AOF formats as the data plane.

Sentinel monitors nodes, selects a promotion candidate, sends
`REPLICAOF NO ONE`, and reconfigures the remaining replicas. Sentinel does not
carry replication data.

Both ends of the data plane must be Garnet. A stock Redis replica cannot decode
Garnet's physical AOF records and is not a target for this implementation.

The feature remains opt-in through:

```text
--sentinel-replication
```

## 2. Current capabilities

The following work is implemented:

- Redis-compatible `REPLCONF`, `PSYNC`, `REPLICAOF`/`SECONDARYOF`, `ROLE`, and
  standalone `INFO replication` surfaces needed by Sentinel.
- Replica discovery through real `connected_slaves` and `slave<N>` entries.
- Garnet replica attach and detach.
- Stable primary replication identity.
- Live Garnet AOF streaming and replay.
- Fixed and moving AOF retention leases.
- A lease-aware standalone checkpoint catalogue.
- Checkpoint catalogue restoration after restart.
- Shared checkpoint source and receiver infrastructure in `Garnet.server`.
- Typed private checkpoint and AOF frames negotiated with
  `REPLCONF capa garnet-snapshot`.
- Full checkpoint transfer, replica recovery, and ordered AOF catch-up.
- Atomic checkpoint-to-AOF handoff.
- Idle-stream readiness notification.
- Bounded AOF batches preserving complete records and logical addresses.
- Pooled replica frame buffers.
- Sentinel promotion and replica reconfiguration surfaces.

A fresh replica can attach to a non-empty primary, receive a checkpoint, recover
it, replay retained AOF records written after the checkpoint boundary, and then
continue receiving live writes on the same connection.

## 3. Important limitations

### AOF is mandatory

Standalone Sentinel replication requires AOF on both primary and replicas.

The primary needs AOF for:

- The ordered mutation stream.
- Logical replication addresses.
- The checkpoint coverage boundary.
- Retention while a snapshot is in flight.
- Catch-up after snapshot recovery.

The replica currently requires AOF so that a promoted replica has durable
history and can recover or become a primary for other replicas.

Without AOF:

- Sentinel can still poll `PING`, `INFO`, and role information.
- `REPLICAOF host port` fails with
  `ERR standalone replication requires AOF`.
- `PSYNC` on a standalone-replication primary fails with the same error.
- There is no functional replication or failover data plane.

Do not describe an AOF-disabled benchmark as a supported Sentinel deployment.
It measures the underlying engine, not this replication mode.

### No partial resynchronization

`PSYNC` always establishes a full synchronization. There is no Redis-like
bounded backlog and no `+CONTINUE` path yet.

The batched wire format preserves complete-record boundaries and original AOF
addresses so future partial resync does not need a new record representation.

### No shared replication hub

Each standalone replica owns:

- One `StandaloneSyncDriver`.
- One AOF iterator.
- One batch writer.
- One network stream.

This deliberately remains the initial implementation. CPU profiles do not show
the iterator or batch-construction path as a leading cost with two replicas.

Cluster replication also remains per-replica/per-physical-sublog. The existing
`AofSyncDriverStore` is a lifecycle registry, not a shared fan-out backlog.

### Single physical AOF log only

Standalone replication rejects multi-log AOF. Cluster replication continues to
own multi-sublog orchestration.

### Garnet-to-Garnet only

Garnet AOF records contain Garnet-specific headers and typed inputs, not the
original RESP command bytes. A stock Redis replica cannot consume this stream.

## 4. Consistency model

Initial synchronization follows this sequence:

1. Acquire a checkpoint lease.
2. Acquire a fixed AOF retention lease at the checkpoint's covered address.
3. Send checkpoint files and metadata.
4. Recover the checkpoint on the replica.
5. Create the live AOF iterator at the covered address.
6. Install the moving retention lease associated with replica progress.
7. Replay catch-up records and continue streaming live records.
8. Release the checkpoint and fixed snapshot retention resources.

This ordering is essential. Releasing the fixed AOF pin before the live iterator
and moving lease are active can create an unrecoverable gap between snapshot and
stream.

The checkpoint metadata unifies:

- Store and index checkpoint tokens.
- Store version.
- Covered AOF address.
- Primary replication ID.

The same identity and coverage data feed checkpoint recovery, `INFO`,
`FULLRESYNC`, and restart restoration.

## 5. Wire protocol

The initial handshake remains recognizable to Redis/Sentinel:

```text
PING
REPLCONF listening-port <port>
REPLCONF capa eof capa psync2 capa garnet-snapshot
PSYNC ? -1
```

After `+FULLRESYNC`, the primary sends a valid length-delimited empty RDB
preamble. When `garnet-snapshot` was negotiated, Garnet-private typed frames
follow it.

Frame types are:

- `CheckpointFile`
- `CheckpointMetadata`
- `CheckpointFileEnd`
- `CheckpointComplete`
- `StreamReady`
- `AofRecord` (accepted for compatibility)
- `AofBatch`

An AOF batch is a sequence of:

```text
long currentAddress
int recordLength
byte[recordLength] record
```

Batch policy:

- Up to 256 complete records.
- Approximately 256 KiB per batch.
- Drain only records immediately available from the iterator.
- Flush a partial batch before waiting for more data.

An isolated write is therefore not delayed while waiting for a batch to fill.

## 6. Key code

### Standalone control and data plane

| File | Responsibility |
|---|---|
| `libs/server/Resp/ReplConfCommands.cs` | Replication capability and listening-port negotiation. |
| `libs/server/Resp/PsyncCommands.cs` | Full-sync response, snapshot selection, retention acquisition, and primary driver creation. |
| `libs/server/Resp/AdminCommands.cs` | Standalone `REPLICAOF`, promotion, and role handling. |
| `libs/server/StandaloneReplicaClient.cs` | Outbound attach, checkpoint receive/recovery, pooled frame receive, and AOF replay. |
| `libs/server/StandaloneSyncDriver.cs` | Checkpoint transfer followed by batched catch-up and live AOF streaming. |
| `libs/server/StandaloneReplicationWireFormat.cs` | Frame validation and AOF batch encoding/decoding. |
| `libs/server/LocalReplicationState.cs` | Local role, upstream endpoint, and link state. |
| `libs/server/ReplicaRegistry.cs` | Primary-side attached replica discovery for `INFO`. |

### Snapshot and retention infrastructure

| File | Responsibility |
|---|---|
| `libs/server/Replication/AofRetentionManager.cs` | Fixed and moving retention leases. |
| `libs/server/Replication/Snapshot/TsavoriteCheckpointDataSourceReader.cs` | Enumerates and reads checkpoint files and metadata. |
| `libs/server/Replication/Snapshot/CheckpointFileReceiveHandler.cs` | Receives checkpoint files and metadata. |
| `libs/server/Replication/Snapshot/CheckpointFileTransferProvider.cs` | Shared checkpoint device access. |
| `libs/server/StandaloneCheckpointStore.cs` | Lease-aware catalogue and restart restoration. |
| `libs/server/AOF/GarnetAppendOnlyFile.cs` | Owns the AOF and retention manager. |

### Cluster code that informed the design

| File | Note |
|---|---|
| `libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs` | Driver lifecycle and retention; not a shared buffer. |
| `libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs` | One cluster driver per replica. |
| `libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs` | One iterator per replica and physical sublog; already batches up to about 1 MiB. |
| `libs/server/AOF/AofProcessor.cs` | Applies Garnet AOF records on replicas and during recovery. |

## 7. Tests

The main targeted suites are:

- `test/standalone/Garnet.test/SentinelReplicationInfoTests.cs`
- `test/standalone/Garnet.test/ReplicationLeaseTests.cs`

They cover:

- Primary and replica `INFO`/`ROLE` behavior.
- Feature gating.
- Attach, detach, and live writes.
- Multiple attached replicas.
- Checkpoint transfer and recovery.
- Writes made while checkpoint synchronization is active.
- Checkpoint and AOF lease lifetime.
- Replication identity restoration.
- Frame headers and malformed input.
- Multi-record AOF batch round trips.
- Truncated batch record rejection.

Latest validation after pooled receive buffers:

| Validation | Result |
|---|---|
| Release `net10.0` server build | Passed, zero warnings |
| Targeted standalone replication tests, `net8.0` | 16/16 passed |
| Targeted standalone replication tests, `net10.0` | 16/16 passed |
| Formatting verification | Passed |
| `git diff --check` | Passed |

Before the final batching and pooling commits, the broader replication work was
also checked against selected cluster checkpoint, RangeIndex, and injected
checkpoint-failure tests. Keep those areas in the regression set when changing
shared snapshot infrastructure.

Useful commands:

```bash
dotnet build main/GarnetServer/GarnetServer.csproj -c Release -f net10.0

dotnet test test/standalone/Garnet.test/Garnet.test.csproj \
  -f net8.0 -c Debug \
  --filter "FullyQualifiedName~SentinelReplicationInfoTests|FullyQualifiedName~ReplicationLeaseTests"

dotnet test test/standalone/Garnet.test/Garnet.test.csproj \
  -f net10.0 -c Debug \
  --filter "FullyQualifiedName~SentinelReplicationInfoTests|FullyQualifiedName~ReplicationLeaseTests"

dotnet format Garnet.slnx --verify-no-changes \
  --include libs/server/StandaloneReplicaClient.cs
```

## 8. Performance findings

The repeatable local workload used:

- Garnet Release on .NET 10.
- Redis 7.4.11.
- AOF enabled on every node.
- 500,000 `SET` operations.
- 50 clients.
- Pipeline depth 16.
- 256-byte values.
- Three fresh trials per topology.
- One six-core host for primary, replicas, and client.

Median scaling results with default behavior:

| System | Replicas | Requests/s | Incremental throughput loss |
|---|---:|---:|---:|
| Redis | 0 | 303,767 | - |
| Redis | 1 | 270,124 | 11.1% |
| Redis | 2 | 249,875 | 7.5% |
| Garnet | 0 | 150,602 | - |
| Garnet | 1 | 80,026 | 46.9% |
| Garnet | 2 | 63,743 | 20.4% |

This comparison is not raw in-memory throughput:

- Redis used `appendfsync no`.
- Garnet used its default immediate AOF commit policy.
- All processes contended for six cores.

A diagnostic Garnet run with `--aof-commit-freq 1000` produced:

| Replicas | Requests/s |
|---:|---:|
| 0 | 481,232 |
| 1 | 289,687 |
| 2 | 210,349 |

Do not change the default based only on this result. AOF commit frequency has
durability and recovery implications that require an explicit contract.

CPU traces with immediate commit showed:

- `TsavoriteLog.Commit`: about 35% inclusive primary samples.
- `LightEpoch.BumpCurrentEpoch`: about 25% inclusive and 21% exclusive.
- `StandaloneSyncDriver.StreamAsync`: about 1.75% inclusive.
- AOF iterator `GetNext`: under 1% inclusive.
- Replica `StandaloneReplicaClient.RunAttachAsync`: about 3.65% inclusive and
  0.03% exclusive in a traced run.

Conclusions:

- Immediate AOF commit/epoch coordination is a larger bottleneck than batch
  construction.
- Batching improved two-replica drain lag by about 98% and throughput by about
  7% in the original before/after experiment.
- Pooled receive buffers are allocation/GC hygiene, not a proven throughput
  improvement.
- A shared hub should not be added to the initial PR based on current evidence.
- Production-style measurements should place primary and replicas on different
  hosts or CPU sets.

Performance artifacts are outside the repository:

```text
/home/skyline/.copilot/session-state/e15dc936-5726-4ab1-a7ca-ef107fcaa4cd/files/
```

Important files there include:

- `sentinel_perf_compare.py`
- `sentinel_perf_results.json`
- `sentinel_perf_batched_results.json`
- `aof_batching_perf_report.md`
- `replica_scaling_perf.py`
- `replica_scaling_perf_results.json`
- `replica_scaling_commit_1000ms_results.json`
- `replica_scaling_pooled_results.json`
- `standalone_replication_scaling_report.md`
- `garnet_two_replica_primary.nettrace`
- `garnet_two_replica_receiver.nettrace`
- `garnet_two_replica_commit_1000ms_primary.nettrace`

## 9. Commit sequence

Core standalone data-plane commits:

```text
aea85c3bc [Resp] Add REPLCONF and PSYNC for Redis Sentinel / stock-replica support (Phase 1)
ba417d1f3 [Resp] Fix REPLCONF/PSYNC review findings (round 1)
ed8dce346 [Resp] Report attached replicas in INFO replication for Sentinel (phase 2)
2bc3b6ae3 [Docs] Document Sentinel support status and add failover verification script
2e66fc123 [Resp] Add standalone REPLICAOF so a Garnet node can be a replica (Phase 3)
86addb5c7 [Resp] Stream standalone AOF replication data (Phase 4)
7bd5a7da5 [Cluster] Decouple checkpoint transfer from ClusterProvider
f0bf6786b [Cluster] Extract checkpoint and AOF retention leases
2c879bb9b [Resp] Protect standalone AOF streams from checkpoint truncation
ffb10af63 [Resp] Add lease-aware standalone checkpoint catalogue
8166553cb [Cluster] Move checkpoint transfer adapter to server
503752b55 [Resp] Unify standalone checkpoint replication identity
9c9d338b4 [Cluster] Extract reusable checkpoint data reader
a7b7e4047 [Cluster] Extract reusable checkpoint file receiver
268e39e74 [Resp] Add typed standalone replication frames
7c82564a8 [Resp] Stream standalone checkpoints before AOF catch-up
05887aea8 [Resp] Batch standalone AOF replication frames
f2c59fe78 [Resp] Pool standalone replication frame buffers
```

## 10. Recommended future work

### First: prepare the initial PR

Before adding more architecture:

1. Review the full branch diff against its merge base.
2. Ensure user-facing documentation matches current snapshot support.
3. Correct the stale `--sentinel-replication` help text in
   `libs/host/Configuration/Options.cs`; it still says initial snapshots are not
   implemented.
4. Run the broad standalone, ACL, command metadata, and selected cluster
   regression suites expected by the PR.
5. Write a precise PR description separating implemented behavior from partial
   resync and other future work.

### Then: partial resynchronization

The next substantial feature should be reconnect without full checkpoint
transfer:

1. Persist the replica's primary ID and last completely applied AOF address.
2. Send that identity and address in `PSYNC`.
3. Validate identity and retained AOF range on the primary.
4. Reply `+CONTINUE` when the requested complete-record boundary is retained.
5. Fall back to the existing full snapshot flow otherwise.
6. Add reconnect tests for retained and truncated ranges.

Reuse `AofRetentionManager`; do not invent an unrelated offset lifetime model.

### Separate performance investigation

Investigate AOF commit policy separately from Sentinel replication:

- Define acknowledged durability semantics for each commit mode.
- Compare equivalent Redis and Garnet durability settings.
- Profile epoch/commit contention with no replicas and with isolated replicas.
- Avoid silently weakening defaults for benchmark results.

### Defer the shared hub

Only revisit a shared hub if measurements at higher replica counts show
per-replica iterator and copying work becoming dominant. If pursued:

- Place a generic `AofReplicationHub` in `Garnet.server`.
- Use one publisher per physical sublog.
- Share immutable pooled batches.
- Give each subscriber a bounded queue and applied-address progress.
- Join shared live fan-out only after private catch-up to a captured barrier.
- Adapt standalone first; migrate cluster transport later.

Do not combine a full cluster migration with the initial Sentinel PR.

## 11. Traps

- `parseState.Count` excludes the command name.
- The replication RDB transfer has no trailing RESP CRLF after its body.
- `REPLCONF ACK` is a no-reply command.
- `ReplicaRegistry` entries must be pruned when sessions end.
- Primary replication ID must be stable and checkpoint-restored.
- Retention leases must cover the entire snapshot-to-live transition.
- `ScratchBufferBuilder` slices cannot escape or survive reallocation.
- Pooled replication payloads cannot be retained after the frame handler
  returns them to `ArrayPool<byte>`.
- Drain-list callbacks and epoch actions may run synchronously on arbitrary
  protected threads.
- Do not assume asynchronous replication is free; primary publication, AOF
  reads, copying, sockets, replica replay, storage I/O, and scheduling still
  consume resources.
- Do not use an AOF-disabled benchmark to evaluate the supported Sentinel
  topology.

If this handoff conflicts with code or fresh measurements, trust the code and
measurements, then update this document.
