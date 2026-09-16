# Garnet.test.sentinel

End-to-end tests that drive a stock `redis-server` and `redis-sentinel` against Garnet.
Phase 1 of the "Garnet as Sentinel-controlled Redis" work.

## Gating

The project is wired into `Garnet.slnx` and compiled by default, but **no external
processes are spawned unless both of these gates are open**:

| Gate | How to open | Default |
|---|---|---|
| **Compile-time** (`ENABLE_SENTINEL_TESTS_FULL` symbol) | `dotnet build -p:EnableSentinelTests=true` | **off** |
| **Runtime** (`GARNET_SENTINEL_TESTS=1` env var) | `export GARNET_SENTINEL_TESTS=1` | **off** |

With both gates closed, the three smoke tests in `Tests/GarnetServerProcessTests.cs`
appear in the test list as `Ignored` with the gate reason printed. Nothing
external is launched; CI is unaffected.

## Redis binary

Tests use a stock `redis-server` and `redis-sentinel` binary. Discovery order:

1. `GARNET_TEST_REDIS_SERVER` / `GARNET_TEST_REDIS_SENTINEL` environment variables.
2. The hard-coded path `/home/skyline/.cache/redis-bin/7.4.11/{redis-server,redis-sentinel}`
   (the path used during development).

The repository does **not** ship Redis. Build once:

```bash
mkdir -p /tmp/redis-build && cd /tmp/redis-build
curl -sSL https://github.com/redis/redis/archive/refs/tags/7.4.11.tar.gz | tar xz
cd redis-7.4.11
make -j4 OPT=-O2 redis-server redis-sentinel
mkdir -p /home/skyline/.cache/redis-bin/7.4.11
cp src/redis-server src/redis-sentinel /home/skyline/.cache/redis-bin/7.4.11/
```

`OPT=-O2` is required on newer GCC (Leap 16, Fedora 40+, etc.) because the
default `-O3` enables `-flto=auto` which fails to link.

## Running

```bash
# Smoke tests only (gates closed → all tests Ignored):
dotnet test test/standalone/Garnet.test.sentinel/Garnet.test.sentinel.csproj

# Smoke tests actually executing:
dotnet build test/standalone/Garnet.test.sentinel/Garnet.test.sentinel.csproj -p:EnableSentinelTests=true
GARNET_SENTINEL_TESTS=1 dotnet test test/standalone/Garnet.test.sentinel/Garnet.test.sentinel.csproj \
    --filter "FullyQualifiedName~GarnetServerProcessTests"
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
└── GarnetServerProcessTests.cs harness smoke tests (Phase 0)
SentinelGate.cs                 compile-time + runtime gate logic
TestBase.cs                     running-test tracking (mirrors Garnet.test)
```

## Phase 1 server-side changes

`REPLCONF`, `PSYNC`, and `replica-announce-*` config will live under
`libs/server/Resp/` (commands) and `libs/host/Configuration/` (config).
Gated tests under `Tests/` will exercise them against the spawned subprocess.
