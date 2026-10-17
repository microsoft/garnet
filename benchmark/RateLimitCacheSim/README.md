# RateLimitCacheSim — token-bucket rate-limiter cache workload simulator

Models the cache workload of a distributed rate limiter: a fleet of service instances that keep
per-endpoint and per-tenant token buckets in a shared cache, read and refill them from Lua, and
heartbeat a liveness probe. The access pattern — overwhelmingly `EVALSHA`, each script a single
`HGET` or a small `HGET`/`HSET`/`EXPIRE` triple, at low pipeline depth — is typical of admission
control, quota enforcement and throttling tiers.

Reproduces that workload so Garnet, OSS Redis and Valkey can be compared on it directly, and so
that a two-VM comparison of them can be checked for client-side measurement error.

## Summary

All three servers run on an identical 4-vCPU footprint, one at a time on the same cores, over TLS,
measured interleaved in a single session. Redis and Valkey are each tuned to the best of eight
`io-threads` configurations for that footprint; they do not share a best configuration, so each was
swept separately. Garnet figures use inlined socket completions — two environment variables,
described in "Single-connection deficit and socket completions"; stock Garnet is shown alongside it.
Latency is service latency (send → reply) in ms.

| Measure | Garnet | Redis, best config | Valkey, best config | vs Redis | vs Valkey |
|---|---|---|---|---|---|
| **Max rate at p99 < 5 ms**, 64 conns | **400,000 ops/s** | 85,000 ops/s | 130,000 ops/s | 4.7x | 3.1x |
| Saturation ceiling, 64 conns | **653,300 ops/s** | 99,000 ops/s | 142,300 ops/s | 6.6x | 4.6x |
| Max rate, one operation in flight per connection | **151,000 ops/s** | 90,300 ops/s | 144,300 ops/s | 1.67x | 1.05x |
| Single connection, one operation at a time | **16,146 ops/s** | 15,668 ops/s | 15,772 ops/s | 1.03x | 1.02x |
| Four connections, one operation at a time | **79,118 ops/s** | 51,725 ops/s | 54,120 ops/s | 1.53x | 1.46x |
| Single connection, dynamic depth | **~300,000 ops/s** | ~40,000 ops/s | ~50,000 ops/s | 7.5x | 6x |
| p50 / p99 at 90,000 ops/s | 0.061 / 0.292 | 0.997 / 8.399 | 0.129 / 0.425 | — | — |
| p50 / p99 at 108,000 ops/s *(the modelled fleet)* | 0.070 / 0.364 | collapsed, 99,686 achieved | 0.134 / 0.518 | — | — |
| p50 / p99 at 130,000 ops/s | 0.096 / 0.473 | collapsed, 100,153 achieved | 0.202 / 0.846 | — | — |
| p50 / p99 at 250,000 ops/s | 0.488 / 1.452 | >ceiling | >ceiling | — | — |

Valkey 8.1 is a substantially stronger baseline than Redis 7.4 on this workload — its reworked
`io-threads` scale to all 4 vCPUs where Redis's stop paying off after 2 — and it closes much of the
gap. Garnet still sustains 4.6x Valkey's throughput ceiling and 3.1x its clean-latency rate on the
same cores. Valkey's gain is specific to TLS: without encryption it falls behind Redis (see
"Saturation").

- **One 4-vCPU Garnet node absorbs the entire modelled fleet load** (~108,000 ops/s, spread in
  production across roughly a dozen cache instances) at 0.070 ms p50 and 0.364 ms p99, and holds
  p99 under 5 ms to 400,000 ops/s, 3.7x that load. Tuned Valkey also absorbs the fleet load
  (0.134 / 0.518); tuned Redis does not.
- **The cliff matters more than the ratio.** Redis is usable at 85,000 ops/s and collapsed by
  108,000, where it sheds load and sits at ~1,450 ms. Valkey moves the cliff to ~135,000–140,000
  and is shedding by 160,000. Garnet shows no such edge until ~600,000.
- **The lead does not depend on batching.** With 64 connections, 50,000–108,000 ops/s keeps under
  0.15 operations outstanding per connection. At a fixed 85,000 ops/s Garnet has the lowest p50 and
  p99 at every connection count from 1 to 200. With strictly one operation per connection it
  leads Valkey at 1 connection and from 4 to 64 (2.1x Valkey and 1.8x Redis at 8), and is level
  with it at 2 and at 128, where that workload saturates all four vCPUs on every server; Garnet is
  1.6x ahead of Redis at 128.
- **Inlined completions cost up to ~0.08 ms of p50 below ~35,000 ops/s.** At 15,000–22,000 ops/s
  over 50 connections inlined Garnet's p50 is 0.12–0.15 ms against 0.07–0.08 for Redis and Valkey,
  while stock Garnet is within 0.03 ms at p50 and has the lowest p99 at 22,000 (0.122 against 0.258
  and 0.249). From 50,000 ops/s up the inlined configuration leads. See "Low offered rates".
- **One connection scales on Garnet and not on the baselines.** A single connection carries
  ~300,000 ops/s on Garnet against ~40,000 on Redis and ~50,000 on Valkey, because both baselines
  read, decrypt, execute and write a connection on one thread.
- **A two-VM test of the reported shape offers ~235 ops/s**, about 1% of one production
  instance's load. All three servers answer it in under 0.3 ms, so the test cannot rank them. Its
  4–160 ms figures are reproduced on either server by constraining the client, so they are
  client-side.
- **Script state follows concurrency, not connections.** Lua executors are pooled, so the
  working set does not grow with the connection count: at 128 connections strict
  request/response throughput rises from 123,219 to 142,670 ops/s and L2 misses per operation fall
  from 237 to 162. See "Server-side hot path".
- **Scripted commands are cheaper.** `redis.call` now has direct fast paths for `HGET`, `HSET` and
  `EXPIRE`, worth 17% of server CPU on the write script (1.21x throughput) and ~11% on the read
  script. See "Server-side hot path".

## Workload model

The modelled fleet drives roughly 100,000 ops/s spread across about a dozen cache instances.
`EVALSHA`/`EVAL` is ~97% of traffic. Within the Lua scripts, ~20% do `HGET` + `HSET` + `EXPIRE` and
almost all the rest do only `HGET`:

```lua
-- read (80% of scripted ops)
return redis.call('HGET', KEYS[1], ARGV[1])

-- read-write (20% of scripted ops)
local v = redis.call('HGET', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return v
```

The remaining 3% is a plain `GET` against a separate string key space. The read/write split is
applied per operation.

## Modes

| Mode | Shape | Pipeline depth | Use |
|---|---|---|---|
| `Periodic` | N mock service instances, each running the three documented background jobs (probe 2s, endpoint sync 250ms, tenant sync 5s) | emergent, ~0 | Reproduces the two-VM experiment |
| `Rate` | Open loop at `--ops` aggregate ops/s | emergent from load, like a real multiplexer | Latency at a fixed offered load; the production-realistic measurement |
| `Saturate --itp 1` | Closed loop, one operation outstanding per connection | exactly 1 | Lower bound: no pipelining |
| `Saturate --itp N` | Closed loop, N operations in flight per connection | exactly N | Upper bound: deep explicit pipelining |

The service uses StackExchange.Redis multiplexed connections with no configured batch size, so its
pipeline depth is dynamic. `Rate` mode has the same property: it emits each operation at its ideal
arrival time regardless of what is outstanding, so depth is an outcome and is reported, never a
setting. The two `Saturate` forms bracket it from below and above.

## Latency decomposition

Every operation is stamped with the time it was *due*, *sent*, and when the reply arrived, giving
three histograms:

- **schedule** (due → send): the client's own delay.
- **service** (send → reply): the server's share.
- **end-to-end** (due → reply): their sum.

Separating them distinguishes a slow server from a client that cannot keep up. **A result where
`schedule` is a significant fraction of `end-to-end` measures the client, not the server.** `Rate`
mode also reports `shed` (operations never issued because the in-flight cap was hit); a non-zero
`shed` invalidates the run.

## Running

Run the servers in containers on the same cpuset, one at a time, and pin the client to different
cores on another NUMA node:

```bash
CPUS=4,6,84,86                      # 2 physical cores + their HT siblings = a 4-vCPU VM
CLIENT=$(seq -s, 101 2 159)         # 30 cores on the other NUMA node

# TLS material, shared by all three servers. Garnet reads the PFX; the others read the PEM pair.
mkdir -p /tmp/rlcerts && cd /tmp/rlcerts
openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 365 \
    -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
openssl pkcs12 -export -out cert.pfx -inkey key.pem -in cert.pem -passout pass:rltest
chmod 644 /tmp/rlcerts/*          # the container users must be able to read these

# Garnet (publish first: dotnet publish main/GarnetServer -c Release -o /tmp/garnet-pub)
docker run -d --name garnet --network host --cpuset-cpus $CPUS \
    -v /tmp/garnet-pub:/app:ro -v /tmp/rlcerts:/certs:ro \
    mcr.microsoft.com/dotnet/runtime:10.0 \
    dotnet /app/GarnetServer.dll --port 7011 --bind 127.0.0.1 --lua -m 8g -i 512m \
    --tls --cert-file-name /certs/cert.pfx --cert-password rltest --client-certificate-required false

# OSS Redis: best configuration for this footprint (see the io-threads tuning below)
docker run -d --name redis --network host --cpuset-cpus $CPUS -v /tmp/rlcerts:/certs:ro \
    redis:7.4 redis-server --port 0 --tls-port 7012 \
    --tls-cert-file /certs/cert.pem --tls-key-file /certs/key.pem \
    --tls-ca-cert-file /certs/cert.pem --tls-auth-clients no \
    --save '' --appendonly no --io-threads 2 --io-threads-do-reads yes

# Valkey: its best io-threads setting differs from Redis's and is swept separately
docker run -d --name valkey --network host --cpuset-cpus $CPUS -v /tmp/rlcerts:/certs:ro \
    valkey/valkey:8.1 valkey-server --port 0 --tls-port 7013 \
    --tls-cert-file /certs/cert.pem --tls-key-file /certs/key.pem \
    --tls-ca-cert-file /certs/cert.pem --tls-auth-clients no \
    --save '' --appendonly no --io-threads 4 --io-threads-do-reads yes

# Client, always pinned away from the server
taskset -c $CLIENT dotnet RateLimitCacheSim.dll --mode Rate --tls --port 7011 --ops 85000 \
    --instances 64 --runtime 25 --warmup 5 --csv

# Same point with one operation in flight per connection
taskset -c $CLIENT dotnet RateLimitCacheSim.dll --mode Saturate --itp 1 --tls --port 7011 \
    --instances 16 --runtime 15 --warmup 5 --csv

# Sweeps
TLS=1 GARNET_PORT=7011 REDIS_PORT=7012 VALKEY_PORT=7013 \
    ./scripts/rl-ramp.sh /tmp/rl-ramp 50000 90000 108000 130000
SERVER=redis  ./scripts/rl-iothreads.sh    # best io-threads / io-threads-do-reads under TLS
SERVER=valkey ./scripts/rl-iothreads.sh

# Restrict the ramp to the servers still scaling in a range
TARGETS="garnet:7011 valkey:7013" TLS=1 ./scripts/rl-ramp.sh /tmp/rl-hi 160000 250000 400000
```

`rl-ramp.sh` runs one server at a time: before each measurement it `docker unpause`s the target and
`docker pause`s the others, since a paused container consumes no CPU from the shared cpuset. When
driving the simulator by hand, do the same with an explicit `if`/`else`.

Add `--tls` (and `TLS=1` for the scripts) to measure over TLS. A managed cache enforces TLS, and TLS
changes the result substantially, including the ranking of the two baselines against each other.

## Test setup and fairness

2x Xeon Platinum 8380. Each server is pinned to 4 vCPUs (2 physical cores + HT siblings); the
client runs on a separate NUMA node with 30 cores; deep C-states are disabled.

**All three servers run as Docker containers with `--network host --cpuset-cpus 4,6,84,86`** — the
same four CPUs, one server at a time. `--network host` avoids NAT in the path of any one server.
The cpuset is 2 physical cores plus their hyperthread siblings (4/84 and 6/86), which is what a
4-vCPU cloud VM physically is:

```bash
cat /sys/devices/system/cpu/cpu4/topology/thread_siblings_list   # -> 4,84
```

- **The confinement binds.** `Cpus_allowed_list` for the container's host PID reads `4,6,84,86`, and
  `cpuset.cpus.effective` in its cgroup agrees.
- **The runtime sees it.** .NET sizes its thread pool from `Environment.ProcessorCount`, which
  reports 4 under both cgroup cpuset and `taskset`; an unconstrained process reports 160.
- **The cap is reached.** Under load `docker stats` tops out at ~400%: Garnet reaches 400.2%,
  Redis 400.5% at `io-threads 4`.
- **Native pinning agrees.** Garnet under `taskset` on separate cores gives 0.057 ms p50 at 50,000
  ops/s against 0.058 ms containerised, so the two confinement mechanisms are interchangeable.
- **The client is pinned to different cores.** An unpinned client schedules onto the server's
  cores and steals CPU from it: Redis's p99 at 85,000 ops/s goes from 1.1 ms to 88 ms.

Three choices favour the baselines, so the margins reported are a floor:

- **The baselines are tuned.** Redis and Valkey run their best `io-threads` /
  `io-threads-do-reads` combination from the full sweep, worth up to 60% of Redis's ceiling over its
  default; default-config Redis collapses at 85,000 ops/s (60,758 achieved). Garnet runs with
  default server settings.
- **Redis is given the whole footprint although it cannot use it.** Its best configuration runs
  fewer threads than the cpuset has vCPUs; more threads land on hyperthread siblings and cost more
  in handoff than they return.
- **Neither pays for persistence.** Garnet has none enabled by default; Redis and Valkey run with
  `--save '' --appendonly no`.

## Results

### Head to head, TLS

64 connections, each baseline in its best configuration (Redis `io-threads 2` +
`io-threads-do-reads yes`; Valkey `io-threads 4` + `io-threads-do-reads yes`), run alternately.
Garnet is shown stock and with socket completions inlined. `>ceiling`
means the server had already collapsed at a lower rate.

| offered ops/s | Garnet stock p50 / p99 | Garnet inlined p50 / p99 | Redis p50 / p99 | Valkey p50 / p99 |
|---|---|---|---|---|
| 50,000 | 0.056 / 0.376 | 0.052 / 0.279 | 0.084 / 0.688 | 0.105 / 0.360 |
| 85,000 | 0.058 / 0.470 | 0.059 / 0.274 | 0.246 / 1.130 | 0.132 / 0.386 |
| 90,000 | 0.064 / 0.380 | 0.061 / 0.292 | 0.997 / 8.399 | 0.129 / 0.425 |
| 108,000 | 0.080 / 0.561 | 0.070 / 0.364 | collapsed (99,686 achieved, p50 1,445) | 0.134 / 0.518 |
| 130,000 | 0.372 / 1.250 | 0.096 / 0.473 | collapsed (100,153 achieved, p50 1,982) | 0.202 / 0.846 |
| 160,000 | 0.388 / 7.483 | 0.340 / 0.914 | >ceiling | collapsed (145,629 achieved, p50 1,033) |
| 250,000 | 0.670 / 2.751 | 0.488 / 1.452 | >ceiling | >ceiling |
| 350,000 | 1.086 / 35.936 | 0.902 / 2.835 | >ceiling | >ceiling |
| 400,000 | 3.073 / 273.151 | 1.360 / 4.211 | >ceiling | >ceiling |
| 450,000 | 5.731 / 412.671 | 2.131 / 7.123 | >ceiling | >ceiling |

Median of three interleaved 20 s runs per point. The highest rate each sustains with p99 under 5 ms
is **400,000 ops/s for inlined Garnet, 130,000 for Valkey and 85,000 for Redis**: 3.1x and 4.7x.
Stock Garnet's p99 is under 1.3 ms to 130,000 ops/s and 36 ms at 350,000.

At 90,000 ops/s, where Redis is at its edge, inlined Garnet's p50 is 16x lower than Redis's and its
p99 29x lower; against Valkey the margins are 2.1x and 1.5x. By 108,000 Redis sheds load and sits
near 1,450 ms while Garnet and Valkey are under 0.15 ms p50.

**Valkey's cliff is at ~142,000 ops/s and Redis's at ~100,000.** Redis is clean at 85,000 (p99
1.1 ms), has p99 of 8.4 ms at 90,000 and collapsed at 108,000, so ~85,000 is its edge. Valkey is
clean through 130,000 (p99 0.85 ms), sits on its knee at 135,000 (three runs: p99 of 594, 1.3 and
1.2 ms), and collapsed at 140,000 (138,872 achieved) and above. Inlined Garnet's p99 is 13 ms at
500,000 and it reaches 653,300 ops/s. A single p99 near a knee is provisional unless it reproduces.

### Pipeline depth

Depth is a measurement: depth is `throughput x median service latency`, and the harness also
reports the peak it observed (median of three runs).

| offered ops/s | Garnet depth (total / per conn) | peak | Redis depth (total / per conn) | peak |
|---|---|---|---|---|
| 50,000 | 2.6 / **0.04** | 192 | 4.1 / 0.06 | 219 |
| 85,000 | 4.9 / **0.08** | 151 | 22.2 / 0.35 | 198 |
| 108,000 | 7.9 / **0.12** | 309 | collapsed | 200,000 |
| 250,000 | 122 / 1.9 | 1,246 | — | — |

**Across the workload's operating range the harness carries well under one operation per
connection.** At 85,000 ops/s Garnet averages 5 operations outstanding across 64 connections, so
there is essentially nothing for the multiplexer to coalesce: the head-to-head table is, in effect,
request/response. Only the 250,000 row reaches a depth above 1 per connection (1.9).

At 108,000 ops/s Redis's peak is 200,000 outstanding operations: its queue grows without bound.

### Strict request/response

`Saturate --itp 1` holds exactly one operation outstanding per connection: a connection cannot issue
again until its reply arrives. This is stricter than the real client and is a lower bound for it.

Throughput in ops/s, with (p50 / p99) in ms. Median of three or more interleaved runs per point
(15 s each after a 5 s warmup); the servers not under test are paused.

| connections | Garnet stock | Garnet inlined | Redis | Valkey |
|---|---|---|---|---|
| 1 | 14,076 (0.070 / 0.082) | **16,146 (0.061 / 0.077)** | 15,668 (0.062 / 0.082) | 15,772 (0.061 / 0.079) |
| 2 | 32,334 (0.059 / 0.084) | **38,809 (0.049 / 0.077)** | 38,006 (0.049 / 0.083) | 38,366 (0.049 / 0.083) |
| 4 | 71,251 (0.053 / 0.077) | **79,118 (0.048 / 0.069)** | 51,725 (0.078 / 0.105) | 54,120 (0.075 / 0.100) |
| 8 | 115,828 (0.062 / 0.134) | **142,037 (0.053 / 0.074)** | 77,359 (0.096 / 0.148) | 66,925 (0.114 / 0.166) |
| 16 | 119,164 (0.125 / 0.212) | **145,803 (0.103 / 0.130)** | 85,163 (0.182 / 0.242) | 127,519 (0.121 / 0.184) |
| 32 | 123,613 (0.251 / 0.345) | **150,956 (0.206 / 0.254)** | 90,286 (0.351 / 0.613) | 143,117 (0.229 / 0.288) |
| 64 | 122,169 (0.514 / 0.775) | **149,464 (0.418 / 0.513)** | 88,451 (0.703 / 1.345) | 144,263 (0.453 / 0.606) |
| 128 | 120,711 (1.042 / 1.855) | 143,054 (0.883 / 1.000) | 88,250 (1.434 / 1.749) | **143,727 (0.880 / 1.747)** |

- **Stock Garnet trails at one and two connections.** It reaches 14,076 and 32,334 ops/s against
  Valkey's 15,772 and 38,366 (11% and 16% lower). With one operation in flight, throughput is
  `1 / latency`, so at one connection stock Garnet's per-operation path is ~9 µs longer than
  inlined Garnet's (71.0 against 61.9 µs). That is a single .NET thread-pool dispatch, removed by
  inlining completions (see "Single-connection deficit and socket completions"). Inlined, Garnet is
  ahead at one connection, 16,146 against 15,772 (+2.4%) with every inlined run above every Valkey
  run, and level at two (38,809 against 38,366).
- **Garnet pulls away from 4 connections.** At 4 connections inlined Garnet is 46% ahead of Valkey
  and 53% ahead of Redis; at 8 it is 2.1x Valkey and 1.8x Redis. Its p99 is the lowest of the three
  at every connection count, and so is its p50 apart from ties with Valkey at 1, 2 and 128. Both
  baselines stop scaling at 4 connections (52–54K, ~1.4x their 2-connection figure); Garnet is
  still scaling (79.1K, 2.0x).
- **Garnet leads Valkey from 4 to 64 connections and is level at 128.** At 64 connections it is
  3.6% ahead (149,464 against 144,263) and 1.7x Redis; at 128 every server is at its CPU limit and
  the three-run ranges nearly touch (Garnet 141.9–143.6K, Valkey 143.1–145.1K), with Garnet's p99
  at 1.000 ms against Valkey's 1.747. From 8 to 128 connections Garnet's throughput stays between
  142.0K and 151.0K; Valkey's, from 32 to 128, between 143.1K and 144.3K. Valkey's 8-connection figure (66,925) is its weakest: four io-threads on four vCPUs cost more in
  handoff than they return until there are enough sockets to fill them.
- **With pipelining removed, Garnet still clears the fleet load and tuned Redis does not.** Garnet
  peaks at ~151,000 ops/s (32 connections) against Redis's ~90,300 (32) and Valkey's ~144,300 (64).
  The advantage over Redis falls from 4.7x to 1.67x and over Valkey from 3.1x to 1.05x; one
  4-vCPU node must cover 108,000 ops/s, and Garnet and Valkey do in both regimes.

### Connection count, TLS

Connection count matters more than any other variable, because both baselines serialise a single
connection onto a single thread while Garnet does not. Offered load is 85,000 ops/s, varying the
number of mock service instances (one connection each). Median of three interleaved 20 s runs per
point:

| Service instances | Garnet p50 / p99 | Redis p50 / p99 | Valkey p50 / p99 |
|---|---|---|---|
| 1 | 0.079 / 1.827 | 715.78 / 1129.47 (collapsed) | 723.46 / 1080.32 (collapsed) |
| 4 | 0.050 / 0.188 | 39.94 / 64.22 (degraded) | 1094.66 / 2420.74 (collapsed) |
| 10 | 0.051 / 0.282 | 0.154 / 1.264 | 0.122 / 0.757 |
| 20 | 0.055 / 0.229 | 0.125 / 0.593 | 0.125 / 0.413 |
| 50 | 0.056 / 0.205 | 0.193 / 0.885 | 0.125 / 0.324 |
| 100 | 0.061 / 0.481 | 0.161 / 1.394 | 0.123 / 0.565 |
| 200 | 0.064 / 0.760 | 0.264 / 2.415 | 0.137 / 0.802 |

Garnet holds the full 85,000 ops/s at every connection count, including one. Both baselines need at
least 10 connections to reach the same rate at a sane latency. **Valkey's extra io-threads buy
nothing here, because the limit is per connection, not aggregate**; Valkey collapses at 4
connections where Redis merely degrades. From 10 connections upward Garnet's p50 is 2.0–4.1x lower
than either baseline's, and its p99 is the lowest at every count: 2.6–4.5x below Redis's, and
2.7x, 1.8x, 1.6x and 1.2x below Valkey's at 10, 20, 50 and 100 connections, level at 200 (0.760 vs
0.802 ms). The large multiples belong to the low-connection regime.

Per-connection capacity ceilings:

| connections | Garnet ceiling | Redis ceiling | Valkey ceiling |
|---|---|---|---|
| 1 | ~300,000 ops/s | ~40,000 ops/s | ~50,000 ops/s |
| 64 | 653,279 ops/s | 98,958 ops/s | 142,332 ops/s |

A single connection to Redis is clean to 40,000 ops/s, degraded at 50,000 (p50 0.96 ms, p99 10 ms)
and collapses at 60,000 (p50 725 ms). Valkey is clean through 50,000 (p50 0.26 ms, p99 0.82 ms) and
collapses at 60,000 (p50 313 ms): one connection is read, decrypted, executed and written by one
thread, and `io-threads` cannot split it. Garnet's single connection is clean to 200,000 ops/s
(p50 0.189 ms, p99 1.0 ms), holds p50 at 0.24 ms through 300,000 (median p99 2.0 ms; one run in
three shows a 73 ms p99), and collapses at 400,000 (p50 214 ms). This is the sharpest structural difference in the comparison, and Valkey's
threading rework does not address it.

The strict request/response and dynamic-depth results bound the same axis:

| one connection | strictly 1 in flight | dynamic depth under load |
|---|---|---|
| Garnet, stock | 14,076 ops/s | ~150,000 ops/s |
| Garnet, completions inlined | 16,146 ops/s | ~300,000 ops/s |
| Redis | 15,668 ops/s | ~40,000 ops/s |
| Valkey | 15,772 ops/s | ~50,000 ops/s |
| winner | Garnet once inlined, by 2% | Garnet, by 6x |

Garnet converts additional concurrency on a connection into throughput and neither baseline does.
A comparison is therefore sensitive to how many connections the harness opens and how deep they
run: a harness with few shallow connections measures the connection rather than the server.

### Open-loop rate ramp, plaintext

Without TLS, 50 connections, median of three interleaved 20 s runs. Plaintext does not model a
managed deployment, but it isolates how much of the gap is crypto and how much is the engine.

| target ops/s | Garnet p50/p99 | Redis p50/p99 | Valkey p50/p99 |
|---|---|---|---|
| 9,000 | 0.114 / 0.166 | 0.106 / 0.206 | 0.102 / 0.215 |
| 22,000 | 0.106 / 0.252 | 0.063 / 0.232 | 0.054 / 0.172 |
| 50,000 | 0.043 / 0.172 | 0.049 / 0.488 | 0.054 / 0.218 |
| 108,000 | 0.051 / 0.177 | 0.180 / 0.743 | 0.098 / 0.288 |
| 200,000 | 0.144 / 0.484 | 1.359 / 2.177 | 0.617 / 3.517 |
| 300,000 | 0.267 / 0.921 | 41.343 / 58.079 (degraded) | collapsed (200,961 achieved) |
| 400,000 | 0.385 / 1.338 | collapsed (302,315 achieved) | collapsed (200,460 achieved) |

Garnet has the lowest p50 and p99 from 50,000 ops/s upward, and sustains 400,000 where Redis
tops out at ~302,000 and Valkey at ~200,000. At 9,000 and 22,000 ops/s the three servers are within
0.05 ms of each other; that regime is covered in the low-rate section below. Valkey's `io-threads 4`
configuration exists to parallelise crypto; without crypto it is four threads' worth of handoff on
two physical cores.

### Saturation

Capacity at each end of the pipelining range:

| | Garnet | Redis | Valkey |
|---|---|---|---|
| Closed loop, itp=1, no pipelining, TLS | 150,956 ops/s | 90,286 ops/s | 144,263 ops/s |
| TLS ceiling, dynamic depth (p99 under 5 ms) | 400,000 ops/s | 85,000 ops/s | 130,000 ops/s |
| TLS ceiling, dynamic depth (throughput only) | 653,279 ops/s | 98,958 ops/s | 142,332 ops/s |
| Closed loop, itp=8, 16 conns, plaintext | 652,547 ops/s (p99 0.286) | 234,109 ops/s (p99 0.816) | 188,151 ops/s (p99 0.874) |
| Closed loop, itp=32, 16 conns, plaintext | 979,181 ops/s (p99 1.144) | 276,126 ops/s (p99 4.603) | 195,804 ops/s (p99 3.375) |

Garnet's TLS throughput ceiling plateaus at **653,279 ops/s** (601,515, 635,516 and 653,279 at
offers of 600K, 700K and 800K): 6.6x Redis's 98,958 and 4.6x Valkey's 142,332.

**Valkey's advantage over Redis is specific to TLS.** It leads Redis by 1.44x on the TLS ceiling but
trails it in both plaintext rows (188,151 against 234,109 at depth 8; 195,804 against 276,126 at
depth 32), because its winning `io-threads 4` exists to parallelise TLS crypto. A plaintext
comparison would rank the two baselines in the opposite order, so the transport has to match the
deployment being modelled.

Garnet's margin over Valkey is widest where there is concurrency to exploit and narrowest where
there is none: 5.0x at depth 32 without TLS, 4.6x at the TLS throughput ceiling, 3.1x at the
p99 < 5 ms rate, 1.05x with one operation in flight. Both baselines' TLS ceilings barely move across
the depth range (Redis 90.3K–99.0K, Valkey 142.3K–144.3K) because their limit is the single
execution thread per connection, not the arrival pattern. Garnet's moves by more than 4x.

### Tuning the baselines' io-threads under TLS

Both baselines do TLS crypto on whichever thread owns the connection, so `io-threads` and
`io-threads-do-reads` decide how much of the box each can use, and the two interact. The full grid
of `io-threads` 1–4 against `io-threads-do-reads` on/off is swept for each on the same 4-vCPU
cpuset (`SERVER=redis ./scripts/rl-iothreads.sh`, `SERVER=valkey ./scripts/rl-iothreads.sh`); the
response is sharply non-monotonic. Every baseline figure in this document comes from the winner:

| | io-threads | do-reads | 50K p50/p99 | 85K p50/p99 | max throughput |
|---|---|---|---|---|---|
| **Best Redis config** | **2** | **yes** | **0.080 / 0.703** | **0.256 / 1.181** | **99,729 ops/s** |
| **Best Valkey config** | **4** | **yes** | **0.105 / 0.315** | **0.132 / 0.400** | **145,800 ops/s** |

Throughputs in this section are from the sweep itself; the interleaved head-to-head runs reproduce
them within 3% (98,958 and 142,332 ops/s).

**The two do not share a best configuration.** Redis's winner (`io-threads 2`) caps Valkey at
~77,000 ops/s, below tuned Redis; Valkey's winner (`io-threads 4`) caps Redis at ~92,000.

- **`io-threads-do-reads yes` is the most important setting under TLS**, and it defaults to `no`.
  At 4 threads and 85K ops/s, Redis with `no` collapses (66,804 ops/s, p50 2,300 ms) and with `yes`
  is comfortable (85,000 ops/s, p50 0.447 ms). For Valkey the two variants reach the same ceiling.
- **For Redis, io-threads should track physical cores.** Threads 3 and 4 land on hyperthread
  siblings and cost more in handoff than they return: the ceiling falls from 99,729 to 92,270
  ops/s, and at 6 and 8 threads to ~10K ops/s, below having io-threads off.
- **Valkey scales to 4 io-threads on the same two physical cores**: 1 thread gives ~64,900 ops/s,
  2 gives ~77,100, 3 gives ~120,100, and 4 gives 145,800. The
  hyperthread siblings that hurt Redis help Valkey, which is why its ceiling is 1.46x Redis's on
  identical hardware.

### Where Garnet's multi-threading shows

With one operation per connection Garnet and Valkey land within ~15% of each other (145K against
129K at 16 connections, parity by 64). Throughput at 16 connections, TLS, as pipeline depth rises:

| depth | Garnet | Redis | Valkey |
|---|---|---|---|
| 1 | 144,568 | 85,186 | 128,908 |
| 2 | 203,386 | 91,653 | 143,794 |
| 4 | 332,682 | 92,498 | 144,092 |
| 8 | 488,111 | 92,990 | 130,124 |
| 16 | 665,946 | 94,050 | 147,405 |
| 32 | 801,598 | 94,679 | 147,588 |

Garnet scales 5.5x with depth; Redis is flat from depth 2 and Valkey stays within 130–148K. Three
measurements explain it.

**1. All three servers use all of their cores.** Per-thread CPU over a steady window: Garnet's four
socket threads sit at 95–98%; Valkey's main thread and three io-threads at 97–100%; Redis's main
thread and its one io-thread at 98–100%. Valkey and Redis include io-thread spin-polling, so their
CPU per operation is not comparable. The difference is what the cores are doing, not idle cores.

**2. At depth 1 the work is almost entirely network and TLS, not storage.** Replacing `HGET` with
`HGET`+`HSET`+`EXPIRE` triples the storage operations per call while leaving the bytes on the wire
nearly unchanged. At depth 1 that costs Garnet 11% of its throughput, so storage is about 6% of its
per-operation CPU:

| Garnet, TLS, 16 conns | depth 1 | depth 32 |
|---|---|---|
| CPU per operation | 26.3 µs | 4.7 µs |
| of which storage execution | ~1.7 µs | ~1.8 µs |
| of which network, TLS, wakeups | ~24.6 µs | ~2.9 µs |
| throughput drop when storage work triples | 11% | 43% |

Storage costs ~1.7 µs at either depth. Syscalls, TLS records and thread wakeups amortise across the
batch, falling 8.4x, so the share that multi-threaded execution can attack grows from 6% to 38%
and the throughput gain follows.

**3. Valkey's io-threads parallelise the term that dominates at depth 1.** Its tuned configuration
spreads socket and TLS work across four threads, so it spends all four vCPUs on the ~25 µs fixed
cost, as Garnet does. The two reach similar throughput through different limiters: Garnet because
four vCPUs of fixed cost give ~4 / 26 µs ≈ 150K, Valkey because its single command-execution
thread tops out near 147K. Garnet's ceiling moves with depth and Valkey's does not: the first is a
cost that amortises, the second a serial resource, which its flat ceiling and its main thread at
97–100% indicate is the main thread.

For the modelled workload: below roughly 108,000 ops/s, where per-connection depth stays near one,
the storage engine is not the bottleneck and Garnet's lead over tuned Valkey ranges from parity to
~15%, from a cheaper per-operation network path. Above it, operations begin to queue, depth rises
as an outcome of load, and the single-threaded baselines hit a wall that Garnet's multi-threaded
execution does not.

## Low rates and the two-VM experiment

The three documented jobs offer `0.5 + 4 + 0.2 = 4.7` ops/s per mock instance, so 50 instances
offer ~235 ops/s — about 1% of the peak rate of a single production instance:

| instances | offered ops/s | Garnet p50/p99 | Redis p50/p99 | Valkey p50/p99 |
|---|---|---|---|---|
| 1 | 4.7 | 0.170 / 2.107 | 0.173 / 0.583 | 0.181 / 0.597 |
| 10 | 47 | 0.137 / 0.256 | 0.138 / 0.508 | 0.138 / 0.267 |
| 20 | 94 | 0.131 / 0.214 | 0.138 / 0.256 | 0.136 / 0.243 |
| 50 | 235 | 0.129 / 0.202 | 0.134 / 0.348 | 0.134 / 0.237 |

All three servers are sub-millisecond at every point, with schedule delay of 0.003–0.007 ms, and the
p50 spread across them is 0.005 ms. **At 235 ops/s no server is near a ceiling, so the harness
cannot rank them.** Garnet's 2.107 ms p99 at a single instance is a 30-second run of about 140
operations, where one scheduling blip sets p99.

Below every server's ceiling, latency is dominated by the network round trip and the three are
indistinguishable. Garnet's advantage shows in two forms — much higher throughput at equal latency,
and much lower latency at a load that saturates the baselines — and neither is visible far below
saturation.

The structural reason is how each server uses cores. Both baselines execute commands on a single
thread and offload only socket I/O and crypto to `io-threads`, which cannot be split within a
connection. Garnet executes across all available cores and scales within a connection as well as
across them. The gap therefore widens with load and with pipeline depth, and is invisible at idle.

A two-VM test runs client and server on separate cloud VMs, with a VM-to-VM RTT on the order of
100–500 µs. Garnet's depth-1 deficit of ~7.7 µs is a sub-percent effect on every number in that
table, so a null result is the expected outcome whether or not the effect is present. A two-VM
experiment discriminates by raising the offered rate until a server fails to hold it.

## Single-connection deficit and socket completions

Stock Garnet is ~7 µs per operation slower than the baselines when exactly one operation is
outstanding (one and two connections). The cause is not Lua — script overhead is ~3.5 µs on Garnet
against ~2.7 µs on Redis — and not TLS, since the gap is present in plaintext.

**The cause is a thread-pool hop on every receive.** Garnet reads with raw `SocketAsyncEventArgs`
and processes the request inline in the `Completed` handler
(`libs/common/Networking/TcpNetworkHandlerBase.cs`). On Linux, .NET does not raise that callback on
the epoll thread that observed readiness: `SocketAsyncEngine` queues the continuation to the thread
pool, so that a slow continuation cannot block event handling for other sockets. Every Garnet
request therefore pays one queue-and-wake before any work begins. Redis does the read, the command
and the write on the thread that woke from `epoll_wait`.

The cost is paid once per arriving request, is largest when the server is otherwise idle (a worker
must be woken rather than found spinning), and is flat across 8, 32, 64 and 128 connections at a
fixed rate. It dominates the depth-1 number and is lost in the noise at 400K ops/s. Thread-pool
sizing (`--minthreads` / `--maxthreads`) does not change it, since the pool is not throttled; only
inlining removes the park/wake.

`DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS=1` sets `PreferInlineCompletions`, running the
continuation on the epoll thread. A bare `GET` at one connection, microseconds per operation:

| server | client on core 8 (same NUMA node) | client on core 101 (other node) |
|---|---|---|
| Garnet stock | 27.3 | 32.6 |
| Garnet, thread-pool spin limit raised to 200 | 24.0 | 31.8 |
| **Garnet, completions inlined** | **19.6** | **29.2** |
| Redis, best config | 19.9 | 30.1 |

Inlining recovers 7.7 µs — the whole deficit — and puts Garnet level with or ahead of Redis. Raising
the spin limit recovers about a third, which confirms the mechanism: part of the cost is a worker
parking and having to be woken.

### The two settings move together

Enabling inline completions also changes the number of epoll engine threads .NET creates: the
default is `max(1, round(ProcessorCount / 30))`, which is one on a 4-vCPU box, and becomes
`ProcessorCount` when inlining is on. With inlining on, the engine thread executes every request
rather than dispatching it:

| variant | 1 conn | 64 conns | Rate 350K, p50 / p99 |
|---|---|---|---|
| stock (inline off, 1 engine) | 14,199 | 122,295 | 1.617 / 57.855 |
| 4 engines only, inline off | 14,488 | 117,993 | 5.827 / 236.4 |
| inline on, **1 engine** | 16,193 | **49,785** | **collapsed**, 636 ms p50 |
| inline on, 4 engines (the default pairing) | 16,328 | 148,514 | 0.893 / 2.669 |

At one connection inlining is the whole fix and engine count is irrelevant. At 64 connections,
inlining with a single engine is worse than stock because one thread serialises all request
processing, and more engines without inlining destabilise the tail. Only the pairing works, which is
why .NET couples them by default.

### Effect on the modelled workload

From 108,000 ops/s up, over TLS with 64 connections, the fix helps well beyond the depth-1 case:

| offered ops/s | stock p50 / p99 | inlined p50 / p99 |
|---|---|---|
| 108,000 | 0.080 / 0.561 | 0.070 / 0.364 |
| 130,000 | 0.372 / 1.250 | 0.096 / 0.473 |
| 250,000 | 0.670 / 2.751 | 0.488 / 1.452 |
| 350,000 | 1.086 / 35.936 | 0.902 / 2.835 |
| 400,000 | 3.073 / 273.151 | 1.360 / 4.211 |
| 450,000 | 5.731 / 412.671 | 2.131 / 7.123 |

The p99 < 5 ms ceiling moves from 250,000 to 400,000 ops/s: the hop costs both latency and
throughput.

### Low offered rates

Over TLS with 50 connections, median of three interleaved 20 s runs, p50 / p99 in ms:

| offered ops/s | Garnet stock | Garnet inlined | Redis | Valkey |
|---|---|---|---|---|
| 9,000 | 0.114 / 0.173 | 0.157 / 0.240 | 0.116 / 0.200 | 0.120 / 0.239 |
| 15,000 | 0.098 / 0.166 | 0.154 / 0.281 | 0.076 / 0.173 | 0.072 / 0.163 |
| 22,000 | 0.072 / 0.122 | 0.121 / 0.278 | 0.074 / 0.258 | 0.072 / 0.249 |
| 35,000 | 0.057 / 0.139 | 0.066 / 0.169 | 0.068 / 0.474 | 0.062 / 0.502 |
| 50,000 | 0.057 / 0.357 | 0.054 / 0.230 | 0.087 / 0.551 | 0.090 / 0.297 |

From 9,000 to 22,000 ops/s inlined Garnet's p50 is 0.04–0.08 ms above the baselines', widest at
15,000 (0.154 against 0.072–0.076); at 35,000 it is level. Stock Garnet is within 0.026 ms of them
at p50 and has the lowest p99 at every rate through 35,000 except 15,000, where Valkey's is 0.003 ms
lower. The same
pattern appears without TLS (22,000 ops/s: inlined 0.108 / 0.245, stock 0.059 / 0.117). The
crossover is near 50,000 ops/s, above which inlined Garnet leads (see "Head to head, TLS").

The socket engine count moves the two ends of the range in opposite directions. Inlined, TLS:

| engines | 22,000 ops/s p50 / p99 | 108,000 ops/s p50 / p99 | one op per connection, 32 conns |
|---|---|---|---|
| 1 | 0.069 / 0.452 | 1.129 / 3.079 | 51,619 ops/s |
| 2 | 0.087 / 0.205 | 0.320 / 0.718 | 107,235 ops/s |
| 4 | 0.121 / 0.282 | 0.076 / 0.304 | 150,062 ops/s |

Fewer engines give lower p50 at 22,000 ops/s; at 108,000 one engine's p50 is 15x that of four.
Four engines on four vCPUs is the setting used for every Garnet figure above 50,000 ops/s.

### Deployment

```bash
DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS=1
DOTNET_SYSTEM_NET_SOCKETS_THREAD_COUNT=<number of vCPUs>
```

The Garnet standalone suite passes unchanged under these settings (386 tests, including the TLS and
Lua fixtures).

Inlined completions pay off from ~50,000 ops/s; at 9,000–22,000 ops/s they add 0.04–0.08 ms of
p50 (see "Low offered rates"), so a node that runs mostly below that range is better left stock.

With completions inlined, anything that blocks inside request processing blocks an epoll thread and
stalls the other sockets assigned to it. For a memory-resident workload that does not arise, and an
engine count equal to the core count bounds the blast radius. Disk-resident and long-script
workloads deserve care.

`Socket.PreferInlineCompletions` is a per-socket property, kept reachable by a `DynamicDependency`
on its setter so that servers can set it by reflection or `UnsafeAccessor`. Garnet could set it on
accepted sockets behind a server option, scoping the behaviour to the RESP listener rather than the
whole process. The engine count would still need the environment variable, as it is process-global
and read once.

Garnet's steady state does not allocate: across the saturated workloads there are zero collections
and ~0.04 bytes allocated per operation.

## Server-side hot path: scripts versus bare commands

The workload is 97% `EVALSHA`, so the cost of a `redis.call` is the cost of the workload. At depth
1 it is invisible: `PING`, `GET`, `HGET`, an empty `EVALSHA` and the full script all land within
noise of ~42 µs, because the fixed network cost swamps every per-operation difference. Under load
it separates. Fast-path versus fallback commands, issued directly versus through one `redis.call`
(saturated, 64 connections, 100K-key spread, 4-vCPU cpuset), in microseconds of server-side
processing per operation:

| command | issued directly | inside `redis.call` | Lua overhead |
|---|---|---|---|
| `GET` | 1.93 | 3.59 | +1.66 |
| `SET` | 1.74 | 3.74 | +2.00 |
| `HGET` | 2.16 | 4.91 | **+2.75** |
| `HSET` | 2.10 | 5.15 | **+3.05** |
| `EXPIRE` | 2.31 | 4.66 | +2.35 |

`GET` and `HGET` differ by 0.23 µs issued directly and by 1.32 µs from Lua. The extra ~1.1 µs is
the command dispatch path Lua uses, not the interpreter or the storage engine.

### Dispatch

`ProcessCommandFromScripting` (`libs/server/Lua/LuaRunner.Functions.cs`) calls the storage API
directly for commands with a fast path. Any other command takes a fallback that formats a complete
RESP command into a scratch buffer, re-enters `TryConsumeMessages()`, has the reply
RESP-serialised into a capture buffer, and re-parses that text into Lua values: a full
encode/decode round trip, in-process, per `redis.call`. Before the fast paths described below, only
`GET` and `SET` were direct, so the workload's scripts took the fallback for every command:

| script shape | cost | calls on fast path |
|---|---|---|
| `GET` + `SET` | 4.48 µs/op | 2 of 2 |
| `HGET` + `HSET` + `EXPIRE` *(the write script)* | **8.46 µs/op** | 0 of 3 |

### Where the time goes

Sampled profiles, excluding the `PollGCWorker` frame (a stack-walk artifact; there is no GC work).
Shares are solid; absolute µs are apportioned from the totals above:

| | `HGET` direct | `EVALSHA` `HGET` | `EVALSHA` `HGET`+`HSET`+`EXPIRE` |
|---|---|---|---|
| | 2.16 µs/op | 4.91 µs/op | 8.46 µs/op |
| Lua interpreter + `RunCommon` | — | 21.9% | 27.8% |
| `redis.call` RESP round-trip | — | 4.7% | 20.5% |
| Socket send | 77.2% | 30.8% | 15.8% |
| `PushBuffer` argument marshalling | — | 16.5% | 11.5% |
| `KEYS`/`ARGV` preamble | — | 15.3% | 9.9% |
| **Storage engine** | 13.6% | 2.6% | **6.2%** |
| RESP parse / socket recv | 5.9% | — | 5.1% |

For the write script, the three data operations are ~6% of request-processing time and the Lua
machinery ~70%. The native interpreter (`lua_*`/KeraLua frames) is 0.03%, so this is a marshalling
cost, not an interpretation cost.

### Decomposing the `EVALSHA` tax

A five-step ladder holds the command fixed at `HGET` and adds one layer of Lua at a time. Server
pinned to one core, client to four others, `-c 4 -P 32`, N=1M, 2M-op warmup, tiered compilation off,
best of five, in microseconds of server CPU per operation:

| step | µs-cpu/op | ops/s | adds |
|---|---|---|---|
| 1. direct `HGET` | 0.740 | 1,342,282 | — |
| 2. `EVALSHA "return 1"`, 0 keys | 0.910 | 1,094,092 | script invocation |
| 3. `EVALSHA "return 1"`, 1 key + 1 argv | 1.150 | 867,303 | `KEYS`/`ARGV` marshalling |
| 4. `EVALSHA "return redis.call('HGET','myhash','field1')"` | 1.900 | 526,870 | **`redis.call` dispatch** |
| 5. `EVALSHA "return redis.call('HGET',KEYS[1],ARGV[1])"` | 2.100 | 474,158 | reading the tables |

Wrapping an `HGET` in a script costs 2.84x, or 1.360 µs. The ladder deltas sum exactly:

| term | µs | share of the tax |
|---|---|---|
| script invocation (2−1) | 0.170 | 12.5% |
| `KEYS`/`ARGV` marshalling by the server (3−2) | 0.240 | 17.6% |
| **`redis.call` dispatch (4−3)** | **0.750** | **55.1%** |
| reading the `KEYS`/`ARGV` tables in-script (5−4) | 0.200 | 14.7% |

The in-process RESP round trip is the largest term, larger than the other three combined (0.610
µs), and the only one avoidable without changing the scripting contract. All `KEYS`/`ARGV` handling
together is 0.440 µs (32.4%).

### Changes and their measured effect

**1. Direct fast paths for `HGET`, `HSET` and `EXPIRE`.** They remove the request half of the RESP
round trip (the encode and the `TryConsumeMessages` re-parse). On the read script, interleaved A/B
over four rounds of 1M operations:

| round | without fast path | with fast path |
|---|---|---|
| 1 | 2.03 µs-cpu/op | 1.82 |
| 2 | 2.09 | 1.81 |
| 3 | 2.24 | 1.82 |
| 4 | 2.32 | 1.82 |
| | min 2.03, median 2.24 | min 1.81, median 1.82 |

That is 0.22 µs-cpu/op: ~11% by min against min, ~19% by median, and ~29% of the 0.750 µs dispatch
term. The no-fast-path arm reproduces step 5 of the ladder (2.100) from an independent harness. On
the write script (`HGET` + `HSET` + `EXPIRE`), three rounds of best-of-three:

| fast paths enabled | µs-cpu/op (min / median) | ops/s |
|---|---|---|
| none | 4.690 / 4.730 | ~203,000 |
| `HGET` only | 4.410 / 4.450 | ~223,000 |
| `HGET` + `HSET` + `EXPIRE` | **3.890 / 3.930** | **~254,000** |

That is 0.800 µs-cpu/op off the write script — 17.1%, or 1.21x the throughput at a fixed vCPU cap
— or 0.267 µs per `redis.call` eliminated, in line with the 0.22 µs measured for `HGET` on the read
script. Weighted over the mix (~80% read script with one `redis.call`, ~20% write script with
three), this removes about 0.34 µs of server CPU per script. This workload runs at low pipeline
depth, where the server is network-bound, so the saving is throughput headroom at saturation, which
is what the hottest instance's transient peak consumes.

Each fast path returns early only on the statuses it is certain of — `OK` and `NOTFOUND` for `GET`,
`HGET` and `EXPIRE`, `OK` alone for `HSET` — and lets every other status fall through to the
fallback, which already produces the right answer. It therefore implements no error semantics of its
own and cannot get them wrong. `EXPIRE` is the most conservative: it fast-paths only the
two-argument form with a plainly in-range, non-negative integer TTL, and a malformed, negative,
extreme or option-bearing `EXPIRE` falls through to the RESP path.

**2. No pooled buffer rental on `IGarnetApi` hash reads.** `RespServerSession.GetObjectOutput()`
hands the object store `ObjectOutput.FromPinnedPointer(dcurr, …)`, so a network `HGET` serialises
its reply straight into the socket output buffer. The programmatic surface constructed
`new ObjectOutput()` with no buffer, so the first `WriteBulkString` fell into
`RespMemoryWriter.ReallocateOutput`: a `MemoryPool<byte>.Shared.Rent(512)` plus `Memory.Pin()`,
copied out and disposed, once per call. The hash reads now pass a stack buffer the reply normally
fits in (the idiom already used in `SortedSetOps.cs:663`); a reply larger than the buffer
reallocates onto the pool as before, covered by a test that round-trips a 4 KB value. Rentals per
100 calls, measured with `RespMemoryWriter.TotalOutputRentals`:

| `IGarnetApi` op | before | after |
|---|---|---|
| `HashGet` | 100 | **0** |
| `HashGetAll` | 100 | **0** |
| `HashRandomField` (both overloads) | 100 | **0** |
| `HashLength`, `HashExists` | 0 | 0 — these only set `result1`, never emitting RESP |

The `HGET` fast path depends on this change: with the rental in place it trades the RESP encode for
a rent, pin, copy and dispose of equal cost and gains nothing. Every stored procedure, custom
command, extension and transaction that reads via `IGarnetApi` benefits; the network command path
was already optimal.

**3. Correctness fixes.**

- `IGarnetApi.HashGet(key, field, out value)` distinguishes a missing field from a field holding an
  empty string. `ProcessRespSingleTokenOutput` mapped the `$-1` null reply to `default`, so any
  caller — a stored procedure, a custom command, an extension — read a cache miss as a hit on `""`.
  `TryProcessRespSingleTokenOutput` now reports a null token as such and `HashGet` returns
  `NOTFOUND`, covered in `TestProcedureHash`.
- Null replies follow the script's RESP version: `redis.call('GET', missing)` returns Lua `nil`
  under `redis.setresp(3)` and `false` under RESP2.
- The `GET` fast path returns early only on `OK` and `NOTFOUND`; every other status, such as
  `WRONGTYPE`, falls through to the fallback.

**4. A pooled script executor.** An executor — the Lua VM, its script cache and the RESP processor
that `redis.call` dispatches through — is rented from a shared pool for one `EVAL`, `EVALSHA` or
`SCRIPT LOAD` and returned, so the Lua and processor working set follows the number of scripts
executing at once, not the number of connections. The pool holds `max(16, ProcessorCount)`
executors. The calling session's user applies on every rent, executor command metrics are
aggregated into `INFO` and the monitor, and each executor keeps a bounded script cache
(`ScriptCacheSize`) and trims its scratch buffers as it is returned.

With one executor per session, 64 sessions per physical core exceed the per-core 1.25 MB L2, and
L2 misses per operation grow with connection count while the instruction count stays flat
(strict request/response over TLS, `perf stat` on the server's four vCPUs, median of two runs):

| | connections | cycles/op | instructions/op | L2 misses/op |
|---|---|---|---|---|
| executor per session | 32 | 74.5K | 58.0K | 82 |
| pooled executor | 32 | 73.3K | 58.3K | 73 |
| executor per session | 128 | 78.2K | 58.6K | 237 |
| pooled executor | 128 | 75.5K | 58.4K | 162 |

Valkey runs at ~130 L2 misses per operation at 128 connections. Strict request/response
throughput in ops/s, interleaved A/B, median of three:

| connections | executor per session | pooled executor |
|---|---|---|
| 1 | 16,115 | 16,146 |
| 4 | 78,095 | 79,118 |
| 16 | 141,772 | 145,803 |
| 32 | 146,489 | 150,956 |
| 64 | 139,635 | 149,096 |
| 128 | 123,219 | 142,670 |

The gain is confined to high connection counts, where the per-session working set does not fit
in cache; per-session runs at 128 connections range from 88.0K to 131.4K ops/s. Pipelined
throughput and latency at fixed offered rates are equal or better.

### Further opportunities

1. **Hand the parse state directly to dispatch.** `redis.call` formats a complete RESP command into
   a scratch buffer and `TryConsumeMessages()` parses those same bytes straight back before
   dispatching. Populating `respServerSession.parseState` from the Lua stack directly — one
   `PinnedSpanByte` per argument, no buffer — removes the encode/decode for all commands rather than
   a hand-picked set, reaches the same handlers as the RESP path, and cannot diverge from them in
   error or nil-versus-empty semantics.
2. `PushBuffer` argument marshalling: 16.5% of the read script.
3. `KEYS`/`ARGV` preamble: 15.3% of the read script.
4. The per-call rental in (2) above also exists at ~35 `new ObjectOutput()` sites across `ListOps`,
   `SetOps` and `SortedSetOps`; the same stack-buffer change applies mechanically.

## Measurement guidelines

Each of these fails silently, returning plausible wrong numbers:

1. **Confine containers with `--cpuset-cpus`.** `taskset -c … docker run` pins the Docker CLI, not
   the container. `taskset` directly on a non-containerised process is fine.
2. **Parse `redis-benchmark --csv` with a real CSV reader.** Test names contain commas (the Lua
   script text is in the field), so `cut -d,` shifts every column.
3. **Check that the listening socket's PID is the server just started.** A stale server holding the
   port absorbs the experiment and every result describes the old binary.
4. **Start Garnet with `--lua`.** Otherwise every script returns `ERR This instance has Lua
   scripting support disabled`, which `redis-benchmark` times as a fast reply. Probe each variant
   for its expected reply before timing it.
5. **Measure server-side deltas pipelined.** At `-P 1` a fixed ~14 µs per-request network cost
   swamps everything: the same 0.22 µs saving is 2% unpipelined (7.340 → 7.146 µs-cpu/op) and 11%
   at `-P 32`, where the server is processing-bound.
6. **Hold the server for the whole measurement.** Restarting it per measurement injects JIT warmup
   into every sample (2.86–5.49 µs spread against an effect of 0.22 µs). Use
   `DOTNET_TieredCompilation=0 DOTNET_TieredPGO=0`, a large warmup, and best-of-N repetitions within
   one server lifetime. Tiered compilation off inflates absolute numbers 15–25%, so absolutes and
   A/B deltas want different settings. At low connection counts, discard a priming run first.
7. **Isolate servers with an explicit `if`/`else`.** `[ "$c" = "$keep" ] && docker unpause "$c" ||
   docker pause "$c"` pauses the server under test: `unpause` returns non-zero on an
   already-running container, so the `||` branch fires on the one container it was meant to spare.

## Interpreting a comparison

Four independent failure modes produce "the systems are identical" results:

1. **The offered load is far below every ceiling**, as in the `Periodic` table.
2. **The client is the bottleneck**, so every server reports the client's queuing delay. A client
   pinned to 4 CPUs at 22K ops/s yields Garnet 10.023 ms and Redis 10.207 ms p50 with 7.3 ms of
   schedule delay; the same run on a wide client yields 0.056 ms and 0.054 ms. An identical client
   for every arm does not address this: an identically bottlenecked client produces identical
   numbers for every server.
3. **Pipeline depth is left unstated.** It moves Garnet's ceiling by 2x and the baselines' barely at
   all, so report the depth a comparison ran at as a measurement.
4. **The baseline is untuned, or tuned by copying another server's settings.** Redis and Valkey do
   not share a best configuration, and applying Redis's winner to Valkey caps it below tuned Redis.

A blocking client is a sharp version of (2). At the two-VM experiment's offered load of 235 ops/s,
with 50 mock instances on a 4-CPU client:

| harness | throughput | service p50 | schedule p50 | end-to-end p50 |
|---|---|---|---|---|
| async (`default`) | 235 ops/s | 0.180 | 0.004 | 0.185 |
| blocking (`--sync`) | 19 ops/s | 1002 | 2327 | 3975 |

The blocking harness inflates service latency too, because completion processing is queued behind
the starved thread pool, so a harness cannot escape this by timing only the server.
