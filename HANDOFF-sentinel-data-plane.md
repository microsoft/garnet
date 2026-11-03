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

### Operator-facing Sentinel knobs that stock Redis exposes but Garnet does not

The following options exist in stock Redis 7.4 and are read by stock Sentinel.
They are **not** wire-protocol compatibility: Sentinel does not fail or fall
back if they are absent. They are operator-facing levers that change how
Sentinel selects a failover target or how a replica behaves during outage.

| Option | Redis default | Purpose | Sentinel impact | Garnet status |
|---|---|---|---|---|
| `replica-announce-ip` | unset | Replica advertises an IP for `REPLCONF ip-address` when behind NAT. | Lets Sentinel reach replicas that are not at the inbound peer address. | **Shipped in `a01a8c4`.** |
| `replica-announce-port` | 0 | Replica advertises a different port for `REPLCONF listening-port`. | Lets Sentinel reach a replica that is port-forwarded. | **Shipped in `a01a8c4`.** |
| `replica-priority` (`slave-priority`) | 100 | Operator-set failover selection priority. | Sentinel promotes the replica with the **lowest** non-zero priority (`sentinel.c:5063` skips `priority == 0`). Used for AZ-aware failover and to exclude replicas (`priority 0`) from promotion. | Not implemented. |
| `replica-serve-stale-data` | `yes` | Replica serves reads even when the link is down. | Read-replica availability during failover. | Not implemented. |
| `min-replicas-to-write` | 0 | Primary refuses writes when fewer replicas are connected. | Write safety. | Not implemented. |
| `min-replicas-max-lag` | 10 | Primary refuses writes when all replicas are laggier than this many seconds. | Write safety. | Not implemented. |

### Replica `INFO replication` shape compared to Dragonfly

Dragonfly (the other well-known Sentinel-compatible server) is the closest
peer for this surface. Comparing what each server emits on the replica side of
`INFO replication`:

| Field | Stock Redis 7.4 | Dragonfly (`flags_info_replication_valkey_compatible`) | Garnet (this branch) |
|---|---|---|---|
| `role` | `slave` | `slave` (with the flag) | `slave` |
| `master_host` | yes | yes | yes |
| `master_port` | yes | yes | yes |
| `master_link_status` | yes | yes | yes |
| `master_last_io_seconds_ago` | yes | yes | yes |
| `master_sync_in_progress` | yes | yes | yes |
| `master_replid` | yes | yes | **missing** |
| `slave_repl_offset` | yes | yes | **missing** |
| `slave_priority` | yes | yes | **missing** (no option to set it) |
| `slave_read_only` | yes (`1`) | yes (`1`) | **missing** |

Dragonfly's source is clear about how it models these (`server_family.cc:3014-3033`):

- `master_replid` is captured from `+FULLRESYNC <replid> <offset>` and exposed
  to the replica's own INFO output, not to the primary's `slave<N>` lines.
- `slave_repl_offset` is the sum of journal-executed LSNs across all flows.
  For Garnet this would be the AOF address of the most recently applied
  record, captured from `currentAddress` in
  `StandaloneReplicaClient.cs:303` and reported to `LocalReplicationState`
  after each `AofProcessor.ProcessAofRecordInternal` call.
- `slave_priority` is an `absl::GetFlag` with default `100`. Dragonfly does
  not change it at runtime; the Redis default (`CONFIG SET slave-priority`) is
  not implemented either.
- `slave_read_only` is hard-coded to `1`.
- `psync_attempts` and `psync_successes` are debug-only counters Dragonfly
  emits for monitoring; Redis does not emit them.

Dragonfly does **not** implement `+CONTINUE` in the Redis-compatible control
plane path either. `replica.cc:1378-1381` parses the `+CONTINUE` reply, logs
`Partial replication not supported yet`, and returns
`errc::not_supported`. The `replica_partial_sync` flag exists but only
controls the private Dragonfly-to-Dragonfly DFLY protocol via
`experimental_cascaded_partial_sync`. This confirms that `+CONTINUE` on the
Redis-compatible control plane is a genuinely hard problem and not a quick
port from Dragonfly.

### Operational consequence of the gaps above

- **`replica-priority`**: without it, Sentinel promotes whichever replica has
  the lowest `PING` latency. In multi-AZ deployments this can pick a cross-AZ
  replica when the same-AZ replica is available, increasing RTO.
- **Missing replica INFO fields**: a Redis-compatible client that introspects
  `INFO replication` against a Garnet replica sees a slightly-incomplete
  picture. Sentinel itself does not require any of them (it monitors primaries,
  not replicas, and reads primary-side `slave<N>` lines which Garnet already
  emits correctly).
- **`+CONTINUE`**: a replica disconnect longer than the primary's last-changed
  AOF tail triggers a full resync. Same behavior as stock Redis without
  `repl-backlog-size`. Acceptable for now.

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

### 8.1 Cross-system comparison under the same Sentinel control plane

The numbers above use direct `REPLICAOF` and bypass Sentinel. A follow-up
measurement drove the same workload through a stock `redis-sentinel-7.4.11`
control plane with the same workload and host class, including Dragonfly as a
third system. The comparison is not like-for-like in durability mode (see
caveats below); its purpose is to give a directional read of where Garnet's
data-plane cost sits between Redis and Dragonfly on this host class.

| System | Replicas | Requests/s | avg latency | p99 | Primary CPU cores |
|---|---:|---:|---:|---:|---:|
| Redis 7.4.11 (`appendfsync no`) | 0 | 290,192 | 2.54 ms | 4.24 ms | 0.98 |
| Redis 7.4.11 (`appendfsync no`) | 1 | 262,881 | 2.84 ms | 4.86 ms | 0.98 |
| Redis 7.4.11 (`appendfsync no`) | 2 | 257,202 | 2.92 ms | 4.82 ms | 0.98 |
| Dragonfly dev (`build-opt`, journal) | 0 | **697,350** | 0.88 ms | 2.69 ms | 2.74 |
| Dragonfly dev (`build-opt`, journal) | 1 | **400,000** | 1.79 ms | 4.42 ms | 2.03 |
| Dragonfly dev (`build-opt`, journal) | 2 | **294,118** | 2.54 ms | 6.24 ms | 1.57 |
| **Garnet (default immediate commit)** | 0 | 133,941 | 5.85 ms | 27.65 ms | 4.25 |
| **Garnet (default immediate commit)** | 1 | 85,121 | 9.30 ms | 45.50 ms | 3.80 |
| **Garnet (default immediate commit)** | 2 | 60,503 | 13.08 ms | 80.19 ms | 3.40 |
| **Garnet (`--aof-commit-freq 1000`)** | 0 | **374,813** | 2.04 ms | 5.93 ms | 4.12 |
| **Garnet (`--aof-commit-freq 1000`)** | 1 | **268,817** | 2.97 ms | 10.83 ms | 3.21 |
| **Garnet (`--aof-commit-freq 1000`)** | 2 | **201,045** | 3.97 ms | 11.89 ms | 2.85 |

Method:

- Same workload: `redis-benchmark -t set -n 500000 -c 50 -P 16 -d 256 -r 5000000 --csv`.
- 3 fresh trials per (system, replica count); median reported.
- Sentinel drives topology via `SENTINEL MONITOR` + replicas issued `REPLICAOF`.
- Single 6-core / 15 GiB host, all processes contend.
- Garnet uses `--aof` + `--sentinel-replication`. Dragonfly uses
  `--dbfilename=""` (journal streaming, no AOF/RDB).
- Replication drain watermark (SET watermark on primary, poll replicas until visible):
  Garnet ~12 ms, Dragonfly ~8 ms, Redis ~4,500 ms (Redis was busy with full sync at
  watermark time on the first trial, not a steady-state gap).

Caveats (do not collapse these into a single ranking):

- Garnet's standalone Sentinel mode mandates AOF (see Section 3). With
  immediate commit the primary pays `TsavoriteLog.Commit` per op. Dragonfly
  uses journal buffering with no per-op fsync; Redis was configured with
  `appendfsync no` per Section 8. The Garnet-default column therefore pays
  for synchronous durability the others do not.
- Garnet's standalone primary is single-threaded for SET writes. Dragonfly's
  default is `--proactor_threads=4 --num_shards=3` (12 shard threads).
  Single-thread ingest alone explains a large fraction of the default-commit
  gap.
- All processes share 6 cores. Production isolation would change absolute
  numbers but likely preserve ranking.

Headline finding for the data plane specifically:

- **With batched commit (`--aof-commit-freq 1000`), Garnet closes roughly
  two-thirds of the default-commit gap.** The lift is 2.8–3.3× across all
  replica counts (180–232%). p99 latency collapses from 27.7 ms → 5.9 ms at
  N=0. Drain watermark is unchanged at ~12 ms regardless of commit policy,
  which isolates the gap to primary ingest rather than replication delivery.
- **The remaining ~30–46% gap to Dragonfly (post-commit-fix) is
  architectural, not durability.** Candidate causes in order of likelihood:
  single-thread primary for SET; AOF record-marshalling cost; Sentinel
  handshake path. Drain-watermark parity argues against wire-format overhead.

Implication for the roadmap (Section 10):

- The "investigate AOF commit policy separately" item in Section 10 should be
  elevated from a diagnostic to an explicit default-policy decision. The
  numbers above are the concrete cost of the current default.
- The remaining gap after a commit-policy change is the actual question
  multi-thread primary would need to answer. It is not a wire-format question
  and not a shared-hub question.
- Drain-watermark parity with Dragonfly (8–12 ms) means the data plane
  itself is healthy on this host class; the architectural lever, if any, is
  on the primary ingest side.

Full report (with TLDR, methodology, per-trial JSON, and CPU breakdowns):
`~/Projects/perf-artifacts/dragonfly-vs-garnet-sentinel/reports/README.md`.
Raw data:
`~/Projects/perf-artifacts/dragonfly-vs-garnet-sentinel/sentinel_perf_results.json`
and
`~/Projects/perf-artifacts/dragonfly-vs-garnet-sentinel/sentinel_perf_garnet_1000ms_results.json`.

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
d4509a0bd [Docs] Update Sentinel data-plane handoff
36b2fc824 [Docs] Update stale --sentinel-replication help text and PSYNC docstring
a01a8c4a1 [Resp] Add replica-announce-ip/port for standalone replication
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

**Realistic scope warning (Dragonfly comparison):** Dragonfly, which has been
Sentinel-compatible for years, does not implement `+CONTINUE` on the
Redis-compatible control plane. Their `replica.cc:1378-1381` explicitly errors
out with `not_supported` and a `TODO: part sync` comment. Their
`replica_partial_sync` flag only affects the private Dragonfly-to-Dragonfly
DFLY protocol. A faithful `+CONTINUE` implementation on Garnet requires:

- `LocalReplicationState` to capture `master_replid` from `+FULLRESYNC`
  (currently the `<replid>` token is parsed but discarded in
  `StandaloneReplicaClient.cs:207`).
- `LocalReplicationState` to track `slave_repl_offset` (the latest replicated
  AOF address) as records are replayed.
- `ReplicaRegistry` already updates `ReplicaEntry.AckOffset` from
  `REPLCONF ACK` (`ReplConfCommands.cs:77`) and `ReplicaRegistry.SetAckOffset`
  (`ReplicaRegistry.cs:175`), so the per-replica ack tracking is already in
  place.
- The `PSYNC` handler to compute whether `(<replid>, <offset>)` is within the
  retention window via `AofRetentionManager.GetTruncationLimit()`.
- `ReplicaRegistry` to expose per-replica `ack_offset` so the primary can do
  the comparison.

This is ~200 LOC of correctness-sensitive code. Plan for off-by-one bugs at
the retained/truncated boundary and a wire-byte-level test that reproduces
Redis's response for `+CONTINUE replid offset` vs. `+FULLRESYNC replid
offset`.

### Then: `replica-priority` (highest-value operator knob)

Add `--replica-priority <int>` (default 100, range 0..INT_MAX) and emit
`slave_priority:<n>` on the replica-side `INFO replication`. Sentinel reads
this from each replica's `INFO` and uses the lowest non-zero value to pick
the failover target. Stock Redis behavior: `priority 0` means "never
promote." ~50 LOC, mirrors the `--replica-announce-port` plumbing already in
`a01a8c4`. Useful in multi-AZ deployments to pin failover to the same AZ as
the primary, and to exclude read-only replicas from promotion.

### Optional: complete replica-side `INFO replication` parity

Add `master_replid`, `slave_repl_offset`, and `slave_read_only` to the
replica-side INFO output. Pure Redis compatibility, no Sentinel behavior
change. ~30 LOC. Lower priority than `replica-priority` because no current
operator dashboard reads them on Garnet replicas.

### Optional: `replica-serve-stale-data` and `min-replicas-*`

`replica-serve-stale-data yes` is a per-replica option; `min-replicas-to-write`
and `min-replicas-max-lag` are per-primary options. All three are stock
Redis options; none are read by Sentinel. They matter for read-replica
deployments and write-safety deployments respectively. ~30-100 LOC each,
following the established `--replica-announce-*` plumbing pattern. Defer
unless explicitly requested.

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
