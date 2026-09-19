# Garnet.test.sentinel

End-to-end tests that drive a stock `redis-server` and `redis-sentinel` against Garnet,
plus an ungated in-process suite for the replication surface that an orchestrator reads.

For the design context and the current list of gaps, see
[Sentinel support status](#sentinel-support-status) below.

## Gating

The project is wired into `Garnet.slnx` and compiled by default, but **no external
processes are spawned unless both of these gates are open**:

| Gate | How to open | Default |
|---|---|---|
| **Compile-time** (`ENABLE_SENTINEL_TESTS_FULL` symbol) | `dotnet build -p:EnableSentinelTests=true` | **off** |
| **Runtime** (`GARNET_SENTINEL_TESTS=1` env var) | `export GARNET_SENTINEL_TESTS=1` | **off** |

With both gates closed, every test in this project appears as `Ignored` with the gate
reason printed. Nothing external is launched; CI is unaffected.

Note that the in-process replication tests live in the main `Garnet.test` project as
`SentinelReplicationInfoTests` and are **not** gated — they need only Garnet, so they run
in the default CI configuration. Keeping that surface ungated is deliberate: an earlier
revision put every replication test behind the external-process gate, which left the whole
feature unexercised in CI.

## Redis binary

Tests use a stock `redis-server` and `redis-sentinel` binary. Discovery order:

1. `GARNET_TEST_REDIS_SERVER` / `GARNET_TEST_REDIS_SENTINEL` environment variables.
2. A per-user cache path, `<home>/.cache/redis-bin/7.4.11/{redis-server,redis-sentinel}`
   resolved from the current user's home directory (not a hard-coded absolute path).

The repository does **not** ship Redis. Build once:

```bash
mkdir -p /tmp/redis-build && cd /tmp/redis-build
curl -sSL https://github.com/redis/redis/archive/refs/tags/7.4.11.tar.gz | tar xz
cd redis-7.4.11
make -j4 OPT=-O2 redis-server redis-sentinel redis-cli redis-check-rdb
mkdir -p ~/.cache/redis-bin/7.4.11
cp src/redis-{server,sentinel,cli,check-rdb} ~/.cache/redis-bin/7.4.11/
```

`OPT=-O2` is required on newer GCC (Leap 16, Fedora 40+, etc.) because the
default `-O3` enables `-flto=auto` which fails to link.

## Running

```bash
# Gates closed -> everything Ignored, nothing spawned:
dotnet test test/standalone/Garnet.test.sentinel/Garnet.test.sentinel.csproj

# Gates open:
dotnet build test/standalone/Garnet.test.sentinel/Garnet.test.sentinel.csproj -p:EnableSentinelTests=true
GARNET_SENTINEL_TESTS=1 dotnet test test/standalone/Garnet.test.sentinel/Garnet.test.sentinel.csproj

# In-process replication tests (ungated, no Redis binary needed):
dotnet test test/standalone/Garnet.test/Garnet.test.csproj \
    --filter "FullyQualifiedName~SentinelReplicationInfoTests"
```

## Test layout

```
Fixtures/
├── ProcessWrapper.cs           base class: spawn, capture, ready-line, dispose
├── GarnetServerProcess.cs      Garnet server subprocess
├── RedisServerProcess.cs       stock redis-server subprocess
├── RedisSentinelProcess.cs     stock redis-sentinel subprocess
└── TestPorts.cs                free-port allocator
Tests/
├── GarnetServerProcessTests.cs harness smoke tests
├── ReplConfCommandTests.cs     REPLCONF wire contract (differential vs Redis 7.4.11)
└── PsyncCommandTests.cs        PSYNC framing, RDB body, stock-replica handshake
SentinelGate.cs                 compile-time + runtime gate logic
TestBase.cs                     running-test tracking (mirrors Garnet.test)
```

## Sentinel support status

### What works

* **Sentinel monitors a Garnet primary.** `SENTINEL master <name>` reports
  `flags=master`, `role-reported=master`, and PINGs succeed.
* **Sentinel discovers Garnet's replicas.** Sentinel learns a primary's replica set
  *only* from `INFO replication`, specifically the `connected_slaves` count and the
  `slave<N>:ip=..,port=..,state=..,offset=..,lag=..` lines. Garnet emits both, so
  `num-slaves` reports 1 where it previously reported 0.
* **A full Sentinel failover completes** when the Garnet primary is killed:
  `+slave` → `+sdown` → `+odown` → `+try-failover` → `+selected-slave` →
  `+failover-state-send-slaveof-noone` → `+promoted-slave` → `+failover-end` →
  `+switch-master`, with the promoted replica ending as `role:master`.
* **The handshake works**: `PING` / `REPLCONF` / `PSYNC`, `+FULLRESYNC`, and a valid
  empty-database RDB body that a stock replica loads successfully.
* `master_replid` is a real, stable, non-zero ID, shared between `INFO` and `PSYNC`.

### What does not work yet

These are measured, not inferred. The critical ones mean a failover currently
produces an **empty** new primary.

| # | Gap | Severity | Notes |
|---|---|---|---|
| 1 | **No data is replicated** | Critical | Garnet replies `+FULLRESYNC` with an always-empty RDB and then sends no command stream. Keys written on the primary never reach the replica. A promoted replica is empty. |
| 2 | **The replication link is silently idle** | Critical | `master_link_status` stays `up` while `master_last_io_seconds_ago` climbs indefinitely and nothing is transferred. The socket is open but no keepalives or data flow. |
| 3 | **No partial resync (`+CONTINUE`)** | High | Every reconnect forces a full resync, and the replica logs `Discarding previously cached master state`. Needs a replication backlog. |
| 4 | **`master_repl_offset` does not advance** | High | Stays 0 through writes. Sentinel uses lag when ranking replicas, so this can affect promotion choice. |
| 5 | **`REPLCONF ACK` offsets stay 0** | Medium | The field is wired into the registry; there is simply no stream to acknowledge yet. |
| 6 | **Registry entries are not pruned** | Medium | A disconnect does not remove a replica, so `connected_slaves` can over-report. |
| 7 | No `SENTINEL` command / `INFO sentinel` | Low | Does not block failover: only sentinels serve `SENTINEL`, and a monitored primary is not required to. Matters only for clients that probe it for discovery. |
| 8 | Garnet cannot be a replica of a stock Redis primary | Low | Outbound replication speaks a proprietary `CLUSTER_*` protocol, and `REPLICAOF` only accepts known cluster workers. Also needs an RDB reader, which Garnet does not have. |
| 9 | No `replica-announce-ip` / `-port` | Low | Needed for NAT/proxied deployments. |
| 10 | No `min-replicas-to-write` / `replica-serve-stale-data` | Low | Common write-safety guards alongside Sentinel. |

**Summary:** the control plane is complete — Sentinel can monitor, discover, and fail
over. The data plane is not started beyond the handshake, so gaps 1–4 need to be closed
before this is safe to use for real data. The cheapest credible path to gap 1 is to stream
the existing AOF/command log rather than write a full RDB serializer; a real replication
backlog (gap 3) and a true byte offset (gap 4) follow from that.

## Divergences from Redis

Pinned by test rather than left implicit:

* **Replica registration timing.** Redis counts nothing until `PSYNC` completes; Garnet
  registers a replica as soon as it identifies itself via `REPLCONF`, reporting it as
  `state=sync` until `PSYNC` succeeds. This lets an orchestrator observe a replica that is
  mid-handshake. Verified against Redis 7.4.11, where a `REPLCONF`-only connection leaves
  `connected_slaves` at 0.
* **`REPLCONF` option set** is strict, matching Redis: an unrecognised option returns
  `-ERR Unrecognized REPLCONF option: <key>` rather than being accepted.
  `no-one-connects` and `psync2` are *not* valid option names (`psync2`/`eof` are `CAPA`
  values).
