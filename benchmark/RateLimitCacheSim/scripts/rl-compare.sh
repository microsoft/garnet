#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
#
# Drives the cache comparison between Garnet, OSS Redis and Valkey.
# All servers must already be running, each pinned to the same number of vCPUs.
#
# Usage: rl-compare.sh <results-dir>

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SIM="$ROOT/benchmark/RateLimitCacheSim/bin/Release/net10.0/RateLimitCacheSim.dll"
OUT="${1:-/tmp/rl-results}"

GARNET_PORT="${GARNET_PORT:-7001}"
REDIS_PORT="${REDIS_PORT:-7002}"
VALKEY_PORT="${VALKEY_PORT:-7003}"
# The load modes need a client wide enough that it is never the bottleneck; a client pinned to
# as few cores as the server produces client-bound numbers in which both servers look identical.
CLIENT_CPUS="${CLIENT_CPUS:-$(seq -s, 101 2 159)}"
RUNTIME="${RUNTIME:-60}"
ENDPOINTS="${ENDPOINTS:-10000}"
TENANTS="${TENANTS:-1000}"

mkdir -p "$OUT"
: > "$OUT/summary.csv"

run() {
    local name=$1 port=$2
    shift 2
    echo "--- $name ---"
    taskset -c "$CLIENT_CPUS" dotnet "$SIM" --port "$port" --runtime "$RUNTIME" \
        --endpoints "$ENDPOINTS" --tenants "$TENANTS" --csv --label "$name" "$@" \
        2>&1 | tee "$OUT/$name.txt" | grep -E '^csv,' >> "$OUT/summary.csv" || true
}

# Load each key space once, then reuse it.
for port in "$GARNET_PORT" "$REDIS_PORT" "$VALKEY_PORT"; do
    taskset -c "$CLIENT_CPUS" dotnet "$SIM" --port "$port" --runtime 1 --warmup 0 \
        --instances 1 --endpoints "$ENDPOINTS" --tenants "$TENANTS" > /dev/null
done

# Experiment 1: the reported periodic harness, which offers only 4.7 ops/sec per instance.
for n in 1 10 20 50; do
    run "periodic-garnet-$n"  "$GARNET_PORT" --mode Periodic --instances "$n" --skip-load
    run "periodic-redis-$n"   "$REDIS_PORT"  --mode Periodic --instances "$n" --skip-load
    run "periodic-valkey-$n"  "$VALKEY_PORT" --mode Periodic --instances "$n" --skip-load
done

# Experiment 2: production rates on one instance (fleet average ~9 K, hottest ~22 K ops/sec).
for ops in 9000 22000 50000; do
    run "rate-garnet-$ops" "$GARNET_PORT" --mode Rate --ops "$ops" --instances 50 --skip-load
    run "rate-redis-$ops"  "$REDIS_PORT"  --mode Rate --ops "$ops" --instances 50 --skip-load
    run "rate-valkey-$ops" "$VALKEY_PORT" --mode Rate --ops "$ops" --instances 50 --skip-load
done

# Experiment 3: closed-loop saturation, to locate the capacity knee of each server.
for itp in 1 8 32; do
    run "sat-garnet-$itp" "$GARNET_PORT" --mode Saturate --itp "$itp" --instances 16 --skip-load
    run "sat-redis-$itp"  "$REDIS_PORT"  --mode Saturate --itp "$itp" --instances 16 --skip-load
    run "sat-valkey-$itp" "$VALKEY_PORT" --mode Saturate --itp "$itp" --instances 16 --skip-load
done

echo
echo "Results in $OUT/summary.csv"
