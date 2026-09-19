# HANDOFF — Garnet as a data plane under stock Redis/Valkey Sentinel

**Status:** work in progress, saved and pushed. This document is a temporary
handoff note; delete it before merging anything upstream.

**Branch:** `sentinel-support` (on fork `skyline75489/garnet`)
**HEAD:** `2e66fc123` — `[Resp] Add standalone REPLICAOF so a Garnet node can be a replica (Phase 3)`
**Tree:** clean, builds with 0 warnings.

---

## 1. The goal, stated precisely

Make Garnet usable as the **data plane** underneath a **stock Redis/Valkey
Sentinel control plane**, for a "somewhat workable" branch that upstream will
review.

Concretely:

* **Topology:** 1 Garnet primary + N Garnet replicas, watched by stock Sentinel
  (the normal deployment is 1 primary + 2..3 replicas + 3 Sentinels).
* **Sentinel's only job:** monitor, detect failure, pick a replica, promote it,
  and reconfigure the others. It never touches replication bytes.
* **The data plane is entirely Garnet-to-Garnet.** Sentinel is the orchestrator,
  not a participant.
* **A stock Redis replica is explicitly a non-goal / stretch goal.** Do not
  design for it. It was considered early on and drives the design in the wrong
  direction (see §4).

**Two proven empirical facts that shape everything:**

1. Sentinel discovers a primary's replicas *only* from the `connected_slaves`
   count and the `slave<N>:` lines in the primary's `INFO replication` output.
   Prove it yourself: run a real Redis primary with a replica attached, point
   Sentinel at it, and watch `SENTINEL MASTERS`. With `connected_slaves:0`,
   `num-slaves` is 0 and Sentinel has no promotion candidate — failover can
   never start.
2. Sentinel tolerates a primary that only speaks a subset of Redis. It issues
   `PING`, `INFO`, `REPLICAOF` and (rarely) `CLIENT LIST`. It does not care what
   the primary's write command set is.

---

## 2. Current state — what works today

The Sentinel **control plane** is complete end-to-end, and a Garnet node can now
**attach as a replica**. What is missing is the **data flow**.

### Works

* `REPLCONF` and `PSYNC` implemented on the primary, wire-compatible with Redis
  7.4.11 (verified byte-for-byte against a live server).
* `INFO replication` on a standalone primary reports real `connected_slaves` and
  `slave<N>:` lines sourced from a registry, so Sentinel **can** discover
  replicas.
* `ROLE` reports `master` or `slave` correctly in standalone mode.
* A standalone Garnet node accepts `REPLICAOF <host> <port>` and opens an
  outbound link to the named primary, running the stock handshake.
* `REPLICAOF NO ONE` detaches and promotes back to primary.
* Registry entries are pruned when a replica's connection ends.
* **End-to-end Sentinel failover was demonstrated** with a real Sentinel binary:
  full state machine `+slave → +sdown → +odown → +try-failover → +selected-slave
  → +send-slaveof-noone → +promoted-slave → +failover-end → +switch-master`,
  with the promoted node ending as `role:master`. (This demo used a stock Redis
  replica for the attach side, because Garnet-as-replica did not exist yet.)

### Does NOT work — the whole of the remaining work

* **No data is replicated.** `SET k v` on the primary does not appear on the
  replica. `PSYNC` replies with a valid but **empty 56-byte RDB body**, and
  nothing is streamed afterwards.
* The replication link is **silently idle** after the handshake
  (`master_link_status:up` while `master_last_io_seconds_ago` climbs forever).
* No partial resync / no `+CONTINUE` (no replication backlog).
* `master_repl_offset` does not advance meaningfully.

**Say this plainly in any PR description or README.** The branch is honest about
being control-plane-complete and data-plane-empty. Claiming otherwise would be
the easiest way to lose reviewer trust.

---

## 3. Where the code is

### Phase 1 + Round 1 — the stock handshake (primary side)

| File | What |
|---|---|
| `libs/server/Resp/ReplConfCommands.cs` | `REPLCONF` handler. Handles `listening-port`, `ip-address`, `capa`, `rdb-only`; `ACK`/`GETACK` are **no-reply** (verified against Redis). |
| `libs/server/Resp/PsyncCommands.cs` | `PSYNC` handler. Replies `+FULLRESYNC <replid> 0` then a length-delimited RDB body with **no trailing CRLF**. |
| `libs/resources/RespCommands{Info,Docs}.json` | Generated command metadata. Regenerate via `playground/CommandInfoUpdater/`; do not hand-edit. |
| `playground/CommandInfoUpdater/SupportedCommand.cs` | Registers `REPLCONF` + `PSYNC`. |

### Phase 2 — replica discovery (primary side)

| File | What |
|---|---|
| `libs/server/ReplicaRegistry.cs` | Tracks attached replicas keyed by **source port** (not ip-address, which is optional). Produces the `slave<N>` line text. |
| `libs/server/Metrics/Info/GarnetInfoMetrics.cs` | `PopulateReplicationInfo` — emits the master layout (or the slave layout when this node is a replica). |
| `libs/server/StoreWrapper.cs` | `GetOrCreatePrimaryReplId()` (stable per process — must not be re-minted per call), `PrimaryReplOffset`. |

### Phase 3 — Garnet as a replica (this is the newest, least-reviewed part)

| File | What |
|---|---|
| `libs/server/LocalReplicationState.cs` | **NEW.** This node's own role: `master`/`slave`, primary endpoint, link status. API: `Snapshot()`, `BecomeReplica()`, `BecomePrimary()`, `ReportLinkUp/Down()`, `MarkSyncCompleted()`. |
| `libs/server/StandaloneReplicaClient.cs` | **NEW.** The outbound link. `Start(host, port)` opens a `GarnetClientSession` and runs the handshake; `Detach()` tears it down. One per node. |
| `libs/server/Resp/AdminCommands.cs` | `NetworkProcessClusterCommand` (line 717) routes `REPLICAOF`/`SECONDARYOF`/`FAILOVER` to `NetworkREPLICAOF_Standalone` (line 756) when not cluster mode and the flag is set. Also `ROLE` (line 964). |
| `libs/server/Resp/RespServerSession.cs` | `Dispose()` (line 418) prunes `replicaRegistry` — **this was a real bug**, `Remove` existed but was never called. |
| `libs/server/Servers/GarnetServerOptions.cs` | `EnableStandaloneReplication` (line 71), default `false`. |
| `libs/host/Configuration/Options.cs` | `--sentinel-replication` (line 159, mapped ~920). |
| `libs/host/Configuration/Redis/RedisOptions.cs` | Redis-config alias (line 54). |
| `libs/host/defaults.conf` | **Required** default (line 108) — see the trap in §6. |

### Tests

| Path | What |
|---|---|
| `test/standalone/Garnet.test/SentinelReplicationInfoTests.cs` | **13 ungated in-process tests.** 8 for the primary surface, 5 for `SentinelStandaloneReplicationTests` (attach, multi-replica, ROLE shape, promotion, flag gate). These run in default CI — the deliberate design decision was that the core surface must not be gate-only. |
| `test/standalone/Garnet.test/TestUtils.cs` | `CreateGarnetServer` gained `port:` and `enableStandaloneReplication:`. Also exposes `SendRawAsync` / `InfoValue` publicly (shared by the replication tests). |
| `test/standalone/Garnet.test.sentinel/` | External-process suite, **doubly gated** (compile-time `-p:EnableSentinelTests=true` AND runtime `GARNET_SENTINEL_TESTS=1`). |
| `test/standalone/Garnet.test.sentinel/verify-sentinel-failover.sh` | Runnable end-to-end demo: Garnet primary + replica + real Sentinel, then kills the primary to show the failover sequence. |
| `test/standalone/Garnet.test.sentinel/README.md` | Gap table with severities. |

---

## 4. THE central technical finding — read this before designing anything

**Garnet's AOF is a physical mutation log, not a Redis command log. It cannot be
sent to a stock Redis replica.**

`GarnetLog.Enqueue` (libs/server/AOF/GarnetLog.cs:637) writes
`AofEntryType + version + sessionId + key + value + typed input struct`, behind a
Garnet-specific `AofHeader` (six variants: basic/sharded/transaction/chunked).
The original RESP command text is **gone** by the time the record is written —
argv has been parsed into typed fields.

Consequences:

* You **cannot** pipe AOF bytes to stock Redis. It has no decoder for those
  headers or opcodes.
* You **can** read the AOF cheaply: `GarnetLog.Scan(sublogIdx, beginAddress,
  endAddress, ...)` returns a seekable `TsavoriteLogScanIterator`. This is the
  streaming primitive, and it already exists.
* The cluster replication path (`libs/cluster/Server/Replication/`) ships these
  Garnet AOF bytes over the wire and re-applies them via `AofProcessor`. **Both
  ends must be Garnet.** It also drives a proprietary handshake
  (`ExecuteClusterInitiateReplicaSync`, `CLUSTER FLUSHALL`, …) that stock Redis
  does not speak.

**This is why the goal narrowed to Garnet-to-Garnet.** Once both ends are Garnet,
the AOF byte stream is *fine* — it is Garnet's native format and `AofProcessor`
already knows how to apply it. The entire "translate to RESP" problem disappears.
Do not re-open the stock-Redis-replica path; it forces you to build a RESP
re-encoder for every command family, which is where the bugs live.

### Reuse map for the data plane

| Need | Existing code | Notes |
|---|---|---|
| Iterate the AOF from an offset | `GarnetLog.Scan` | Works today; used by recovery. |
| Ship records to a replica | `AofSyncDriver` / `AofSyncTask` (cluster) | Coupled to `IClusterProvider`; will need a stripped standalone variant. |
| Apply records on the replica | `AofProcessor` (`ProcessAofRecordInternal`) | Reusable as-is. |
| Receive checkpoint on the replica | `ReceiveCheckpointHandler` (cluster) | Reusable, but wired through cluster types. |
| Cluster sync entry point | `PrimarySync.TryBeginDiskbasedSyncAsync` | Takes typed `SyncMetadata`; the cluster-specific part is *how the address was learned*, not the sync itself. |

**Recommended shape:** two new classes in `libs/server/`, with **no edits to
`libs/cluster/**`**:

* `StandaloneSyncDriver` (primary side) — one streamer task per attached replica,
  reads via `GarnetLog.Scan`, writes to the socket.
* Extend `StandaloneReplicaClient` (replica side) — after the handshake, read
  records off the socket and apply via `AofProcessor`.

---

## 5. Suggested next steps, in order

Each step is independently demonstrable. Do not skip ahead; each one proves the
prerequisite for the next.

### Step A — stream data one-way (highest value)

* Primary: after `PSYNC`, keep the connection and stream AOF records for every
  mutation, via `GarnetLog.Scan` from the current tail forward.
* Replica: read the records and apply them.
* **Proof:** `SET k v` on the primary → `GET k` on the replica returns `v`.
* This alone converts the branch from "attaches but empty" to "actually
  replicates".

### Step B — offset + keepalives

* Track a real byte offset; report it in `INFO` and `+FULLRESYNC`.
* Send periodic pings so `master_last_io_seconds_ago` stops climbing.
* **Proof:** `master_repl_offset` advances; `master_last_io_seconds_ago` stays
  low while idle.

### Step C — initial snapshot

* Replace the empty 56-byte RDB body with a real snapshot so a replica that
  attaches to a **non-empty** primary converges.
* **Proof:** write 1000 keys, then attach a fresh replica; it ends with the same
  1000 keys.

### Step D — backlog + `+CONTINUE`

* In-memory ring of recent bytes; on reconnect, if the replica's offset is still
  in the ring, reply `+CONTINUE` and skip the snapshot.
* **Proof:** kill and restart the replica; its log says partial resync, not full.

### Step E — polish

* `replica-announce-ip` / `replica-announce-port` (NAT/proxy).
* `min-replicas-to-write`, `replica-serve-stale-data`.
* Multi-replica end-to-end test (1 primary + 3 replicas + 3 Sentinels, and
  ideally **two consecutive failovers** to prove the reconfigure path works
  with more than one replica).

---

## 6. Traps and gotchas

These are all things that actually cost time during this work.

* **`defaults.conf` is mandatory.** There is an enforced invariant
  (`GarnetServerConfigTests.DefaultConfigurationOptionsCoverage`) that every
  `[Option]`-attributed property on `Options` has a default there. Adding an
  option without it fails **45 config tests**. `defaults.conf` is an embedded
  resource in both the host and test projects, so it needs a rebuild.
* **`parseState.Count` excludes the command name.** `REPLICAOF host port` is
  `Count == 2`, not 3. This cost a debugging cycle.
* **`ReplicaRegistry` was never pruned.** Fixed in this branch, but if you touch
  session lifecycle, re-check that pruning still happens — a stale entry is a
  dead replica that Sentinel will happily consider for promotion.
* **`GarnetClientSession` needs a real `NetworkBufferSettings`.** Passing `null`
  throws `NullReferenceException` inside the constructor (it calls
  `CreateBufferPool` on it). Use `new NetworkBufferSettings()`.
* **`AdminCommands` ROLE reply mixes types.** `["master", 0, []]` is a bulk
  string, an **integer**, and a nested array — not three bulk strings. Test
  helpers that only parse `$` will break.
* **`GetOrCreatePrimaryReplId` must not use `Generator.DefaultHexId()`** — that
  returns an all-zero placeholder and makes the node look like a different
  primary on every poll. It uses `CreateHexId()`.
* **The test helper `useTestLogger` is a `const`-ish static** in `TestUtils.cs`.
  It was flipped to `true` temporarily for debugging and set back to `false`.
  Do not commit it as `true`.

---

## 7. How to build and test

```bash
export PATH=/home/skyline/.dotnet:$PATH

# Build everything
dotnet build Garnet.slnx -c Debug -nologo

# The 13 ungated replication tests (fast, no external processes)
dotnet test test/standalone/Garnet.test/Garnet.test.csproj --framework net10.0 \
  --filter "FullyQualifiedName~Sentinel"

# ACL suite — must stay green; enforces that every command has ACL coverage (468 tests)
dotnet test test/standalone/Garnet.test.acl/Garnet.test.acl.csproj --framework net10.0

# Command metadata / enum stability (91 tests)
dotnet test test/standalone/Garnet.test/Garnet.test.csproj --framework net10.0 \
  --filter "FullyQualifiedName~RespCommand|FullyQualifiedName~RespInfo|FullyQualifiedName~RespDocs|FullyQualifiedName~PersistedEnumStability"

# Gated external-process suite (needs BOTH gates)
dotnet build test/standalone/Garnet.test.sentinel/Garnet.test.sentinel.csproj \
  -p:EnableSentinelTests=true
GARNET_SENTINEL_TESTS=1 dotnet test test/standalone/Garnet.test.sentinel/Garnet.test.sentinel.csproj \
  --framework net10.0

# End-to-end demo (Garnet + real Sentinel, then kills the primary)
test/standalone/Garnet.test.sentinel/verify-sentinel-failover.sh
```

### Verified state at this commit

| Suite | Result |
|---|---|
| Build | succeeded, **0 warnings** |
| `Sentinel*` in-process | **13/13** |
| ACL | **468/468** |
| RespCommand/Info/Docs/EnumStability | **91/91** |
| Full `Garnet.test` | only **4 failures, all pre-existing + environmental** |

**Those 4 failures are not yours.** `ConnectionProtectionTest` ×3 and
`MultiTcpSocketTest` call `Dns.GetHostAddresses` on the machine's own hostname,
which does not resolve in this environment (`getent hosts opensuse` returns
nothing). The stack trace shows the throw occurs before any project code runs.
Confirm this before spending time on them.

---

## 8. Working agreements that were followed

* **Empirical verification over memory.** Redis 7.4.11 is built at
  `~/.cache/redis-bin/7.4.11/` and is used as an oracle. Claims about wire
  behaviour were proven by capturing actual bytes.
* **Strict Redis compatibility** is preferred over documenting a divergence.
  The one deliberate divergence (Garnet counts a replica as early as `REPLCONF`,
  where Redis waits for `PSYNC`) is documented in code and pinned by a test.
* **Do not disturb existing test structure.** The sentinel suite is a sibling
  project; only `Garnet.slnx` gained a line. Do not edit existing csproj/test
  files gratuitously.
* **Do not hand-edit generated resources.** `RespCommands{Info,Docs}.json` come
  from `playground/CommandInfoUpdater/`.
* **Two-layer gating for external tests** (compile-time symbol **and** runtime
  env var). In-process tests stay ungated so CI actually exercises the surface.
* **Honesty about gaps.** The README and commit messages state plainly what is
  not implemented. Keep it that way.

---

## 9. Immediate next action

**Step A in §5.** Stream AOF records from the primary to an attached replica and
apply them, so that `SET k v` on the primary shows up on the replica.

Start by reading `libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs`
and `libs/server/AOF/AofProcessor.cs` — between them they contain almost
everything needed; the work is stripping the cluster coupling, not inventing a
mechanism.

If anything in this document turns out to be wrong, **trust the code and the
measurements over this document**, and update it.
