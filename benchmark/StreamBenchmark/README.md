# Garnet Streams Benchmark Suite

A reproducible, publication-grade methodology and harness for benchmarking Garnet's
Streams implementation against Redis (and other RESP servers). The design goal is
**fair, transparent, reproducible numbers** that can be published without an asterisk.

> Status: Streams are **experimental** in Garnet. These benchmarks are intended to
> track Garnet's stream performance over time and to produce an apples-to-apples
> comparison against Redis. Treat all results as preliminary until the feature is
> declared stable.

---

## 1. Principles (why this is publishable)

Published system comparisons live or die on fairness and reproducibility. This suite
follows the same disclosure bar as TechEmpower and the Redis/Valkey/memtier community:

1. **Identical client, identical workload.** Both servers are driven by the *same*
   load generator over the *same* protocol (RESP) so the client is never a variable.
2. **Configuration parity.** Persistence, memory limits, and networking are matched
   between Garnet and Redis, and every flag is published (see [`config/`](config/)).
3. **Pinned, disclosed environment.** Exact versions (Garnet commit hash, Redis tag),
   hardware, OS/kernel, and CPU pinning are captured automatically per run.
4. **Steady-state + repetition + variance.** Warmup is discarded; each point is run
   multiple times and reported with median and coefficient of variation (CV%).
5. **Coordinated-omission-aware latency.** Latency is measured under controlled offered
   load, not just at saturation, and reported as a throughput–latency curve.
6. **Raw artifacts published.** Every run emits machine-readable results (JSON/CSV) plus
   the captured environment, so third parties can re-derive every chart.
7. **No cherry-picking.** The full workload matrix is published, not the best cell.

---

## 2. System under test and comparison targets

| | Garnet | Redis |
|---|---|---|
| Build | pinned commit hash (captured) | pinned release tag, e.g. `7.4.x` (and/or Valkey `8.x`) |
| Threading | multi-threaded command execution | single-threaded execution (one shard) |
| Config | [`config/`](config/) Garnet flags | [`config/redis.conf`](config/redis.conf) |

**Fairness note on threading (must be disclosed in any publication).** Redis executes
commands on a single thread; Garnet is multi-threaded. A single-instance comparison is
the honest "out of the box" story but favors Garnet on multi-core hosts. To avoid an
apples-to-oranges claim, the suite supports **two comparison framings**, and results
should report both where relevant:

- **Single instance vs single instance** — the default, simplest story.
- **Resource-matched** — Redis run as *N* instances (cluster or sharded by the client)
  with `N ≈ Garnet worker threads`, so both saturate the same number of cores. This is
  the framing Redis itself recommends for multi-core comparisons.

---

## 3. Tooling

We deliberately use **two independent load generators** so a result is only trusted
when both agree (removes single-tool bias):

### 3a. Primary — `memtier_benchmark` (vendor-neutral)

[`memtier_benchmark`](https://github.com/RedisLabs/memtier_benchmark) is the de-facto
industry standard for Redis load testing. It is external to both projects, drives any
RESP server, and supports arbitrary commands, pipelining, multiple threads/connections,
percentile latencies, and JSON output. Using a Redis-ecosystem tool for the headline
numbers preempts "you used your own biased client" criticism.

- Stateless stream workloads (XADD ingest, XLEN, XRANGE, XREAD-tail) are expressed with
  `--command` using `__key__`/`__data__` placeholders.
- Pin version (e.g. `memtier_benchmark 2.x`) and record it.

### 3b. Cross-check — Garnet `Resp.benchmark` with `--client SERedis`

The in-repo [`Resp.benchmark`](../Resp.benchmark/README.md) tool already targets any
RESP server over TCP and its `SERedis` (StackExchange.Redis) client is explicitly
intended "for apples-to-apples comparisons with Redis." It provides:

- **Offline** (`--op`, `-b`, `-t`, `--runtime`) — saturation **throughput**.
- **Online** (`--online`, `--itp`) — per-op **latency** in an HdrHistogram
  (min/p50/p95/p99/p99.9), the path for latency curves.

Today it only wires `XADD` for streams (`OpType.XADD`). Extending it for the read/group
ops is tracked in the roadmap (§9). When both memtier and Resp.benchmark agree within a
few percent, the result is robust.

### 3c. Consumer-group closed loop — purpose-built driver (planned)

`XREADGROUP > … ` → process → `XACK` is **stateful**: the IDs to ack come from the prior
read, which neither memtier nor stock Resp.benchmark expresses well. The suite defines a
small closed-loop driver (a new `Resp.benchmark` mode or a standalone .NET console; see
§9) that maintains per-consumer in-flight state and measures end-to-end delivery
throughput and ack latency.

---

## 4. Workloads

The full matrix lives in [`config/workloads.json`](config/workloads.json). Each workload
sweeps payload size, pipeline depth, and connection/thread count. Summary:

| ID | Name | Ops | Measures | Tool |
|----|------|-----|----------|------|
| W1 | Ingest | `XADD key * f v` (single + multi-field; with/without `MAXLEN ~`) | append throughput & latency | memtier + Resp.benchmark |
| W2 | Range read | `XRANGE key - + COUNT n` / `XREVRANGE` over a preloaded stream | range-scan throughput & latency | memtier + Resp.benchmark |
| W3 | Tail read | `XREAD COUNT n STREAMS key $`-style follow | tailing throughput & latency | memtier + Resp.benchmark |
| W4 | Consumer group | `XREADGROUP > ` + `XACK` closed loop, N consumers/group | end-to-end delivery throughput & ack latency | group driver (§3c) |
| W5 | Mixed | 80% XADD / 15% XREADGROUP / 5% XACK | realistic blend | group driver |
| W6 | Metadata | `XLEN`, `XINFO STREAM` | cheap-op ceiling / baseline | memtier + Resp.benchmark |
| W7 | Trim under load | continuous `XADD` + periodic `XTRIM MAXLEN` | steady-state ingest with trimming | group driver |

**Parameter sweeps** (per workload, see JSON):

- Entry size: 1 field × 16 B; 10 fields; 100 B; 1 KB value.
- Pipeline / batch depth: 1 (latency), 16, 64, 256 (throughput).
- Connections / threads: 1, 2, 4, 8, 16, 32, 64.
- Preload size for read workloads: 1 M and 10 M entries (fixed RNG seed).

---

## 5. Metrics

- **Throughput** — ops/sec at saturation (offline / high pipeline). Report median over
  runs + CV%.
- **Latency** — p50/p90/p99/p99.9/max under **controlled offered load** (open-loop /
  `--itp` sweep), not only at saturation. The headline artifact is the
  **throughput–latency curve** (the "knee"), not a single number.
- **Efficiency** — server-side CPU% and peak RSS (captured via `pidstat`/`/proc`),
  yielding **ops/sec/core** and **bytes/entry** — the fairest cross-engine metric on
  asymmetric-threading hosts.
- **Durability cost** — each scenario is run in two configs (§8): in-memory ceiling and
  durable AOF, to show the persistence trade-off symmetrically.

> **Coordinated omission:** closed-loop "one in-flight per connection" under-reports tail
> latency when the server stalls. Mitigate by (a) reporting latency at a *fixed* offered
> rate below saturation, and (b) using memtier's rate control / HdrHistogram so a stall
> inflates the recorded latency of all would-be requests in that window.

---

## 6. Run protocol

1. **Isolate.** Client and server on separate machines over a dedicated link is ideal;
   loopback/same-host is acceptable if disclosed and CPU sets are disjoint.
2. **Pin.** Server pinned to one CPU set, client to another (`taskset`/affinity).
   `cpupower frequency-set -g performance`; disable turbo variability; record NUMA.
3. **Warm up.** Discard the first N seconds; only steady-state counts.
4. **Repeat.** ≥ 5 repetitions per cell; drop the run if CV% exceeds a stated threshold
   (e.g. 5%) and re-run; report median.
5. **Preload deterministically.** Read workloads load a fixed dataset (fixed seed) onto
   *both* servers before measurement.
6. **Capture everything.** Per run: tool stdout/JSON, server resource samples, the
   environment snapshot, and exact command lines → `results/<timestamp>/`.

---

## 7. Quickstart

```bash
# On the benchmark host (Linux), with memtier_benchmark, redis-server, and the .NET SDK
# installed, and Garnet built in Release.

# 1. Capture the environment (versions, CPU, kernel, NUMA) for reproducibility.
scripts/env-capture.sh results/$(date +%Y%m%dT%H%M%S)

# 2. Run the full suite against Garnet, then against Redis (in-memory ceiling scenario).
scripts/run-suite.sh --target garnet --scenario memory --out results/<ts>/garnet
scripts/run-suite.sh --target redis  --scenario memory --out results/<ts>/redis

# 3. Repeat with the durable scenario.
scripts/run-suite.sh --target garnet --scenario aof --out results/<ts>/garnet-aof
scripts/run-suite.sh --target redis  --scenario aof --out results/<ts>/redis-aof

# 4. Summarize → tables + throughput-latency plots.
python3 analysis/summarize.py results/<ts>
```

See [`scripts/run-suite.sh`](scripts/run-suite.sh) for all flags.

---

## 8. Configuration parity (scenarios)

Two scenarios, run symmetrically on both servers:

| Scenario | Garnet | Redis | Purpose |
|----------|--------|-------|---------|
| `memory` | AOF off, no checkpoint, `--stream-log-dir` unset (NullDevice) | `appendonly no`, `save ""` | Raw in-memory throughput ceiling |
| `aof` | `--aof`, `--aof-commit-freq` matched, `--stream-log-dir` set | `appendonly yes`, `appendfsync everysec` | Durable-path comparison |

Both: eviction/maxmemory disabled, TCP nodelay on, same OS socket buffers. Garnet flags
are documented in [`config/`](config/); Redis flags in [`config/redis.conf`](config/redis.conf).

---

## 9. Implementation roadmap

The methodology above is tool-agnostic. Concrete build steps to make the *whole* matrix
runnable in-repo:

- [x] Methodology, workload matrix, Redis config parity, orchestration + analysis scaffold (this directory).
- [ ] **memtier command templates** for W1–W3, W6 (stateless) — committed under `config/`.
- [ ] **Extend `Resp.benchmark`** `OpType` with `XLEN`, `XRANGE`, `XREVRANGE`, `XREAD`
      (plus request generation in `Common`/`OnlineBench`) so the in-repo cross-check
      covers the stateless read surface, not just `XADD`.
- [ ] **Consumer-group closed-loop driver** (W4/W5/W7): a new `Resp.benchmark --online`
      mode (or standalone `playground/StreamGroupBench`) that, per connection, keeps a
      consumer cursor, issues `XREADGROUP > COUNT n`, and acks the returned IDs; records
      delivery throughput and read→ack latency.
- [ ] **CI regression mode**: a reduced single-host matrix run on a fixed runner that
      tracks Garnet stream throughput/latency over time (regression guardrail, *not* a
      Redis comparison — same caveats as the existing `BDN.benchmark`).
- [ ] **Result publication**: `analysis/summarize.py` emits the tables/plots; a short
      methodology + results page can be linked from `website/docs/benchmarking/`.

---

## 10. Disclosure checklist (attach to any published result)

- [ ] Garnet commit hash + build config; Redis/Valkey exact version.
- [ ] Full hardware spec, OS/kernel, NIC, NUMA topology; client vs server placement and CPU pinning.
- [ ] Exact server configs (link the files) and the comparison framing (single vs resource-matched).
- [ ] Exact client command lines and tool versions (memtier, Resp.benchmark commit).
- [ ] Warmup, run count, steady-state duration, outlier policy; median + CV% reported.
- [ ] Raw result artifacts (JSON/CSV) published alongside the charts.
- [ ] Known caveats stated (experimental feature; single-thread Redis; coordinated omission handling).
