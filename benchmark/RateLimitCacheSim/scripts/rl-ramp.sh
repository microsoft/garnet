#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
#
# Open-loop rate ramp of the modelled workload against Garnet, OSS Redis and Valkey.
# Both servers must already be running, pinned to the same number of vCPUs, and the client
# must have enough spare cores that it is never the bottleneck - check that the reported
# schedule delay stays far below the service latency.
#
# Usage: rl-ramp.sh <results-dir> [ops ...]

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SIM="$ROOT/benchmark/RateLimitCacheSim/bin/Release/net10.0/RateLimitCacheSim.dll"
OUT="${1:-/tmp/rl-ramp}"
shift || true
RATES=("$@")
[ ${#RATES[@]} -eq 0 ] && RATES=(9000 22000 50000 108000 200000 300000)

GARNET_PORT="${GARNET_PORT:-7001}"
REDIS_PORT="${REDIS_PORT:-7002}"
VALKEY_PORT="${VALKEY_PORT:-7003}"
# Which servers to ramp, as name:port pairs. Narrow this to skip a server that has already
# been shown to collapse below the range being swept.
TARGETS="${TARGETS:-garnet:$GARNET_PORT redis:$REDIS_PORT valkey:$VALKEY_PORT}"
CLIENT_CPUS="${CLIENT_CPUS:-$(seq -s, 101 2 159)}"
RUNTIME="${RUNTIME:-20}"
CONNECTIONS="${CONNECTIONS:-50}"
# Set TLS=1 to negotiate TLS, which is what a managed cache generally enforces.
EXTRA=()
[ "${TLS:-0}" = "1" ] && EXTRA+=(--tls)

mkdir -p "$OUT"
HEADER_WRITTEN=0
: > "$OUT/ramp.csv"

# Only the server under measurement may hold the shared cpuset; a paused container
# consumes no CPU from it. Containers named here that do not exist are ignored.
read -r -a SERVERS <<< "${SERVERS:-garnet redis valkey}"

isolate() {
    local keep=$1 c
    for c in "${SERVERS[@]}"; do
        if [ "$c" = "$keep" ]; then
            docker unpause "$c" >/dev/null 2>&1 || true
        else
            docker pause "$c" >/dev/null 2>&1 || true
        fi
    done
}

run() {
    local name=$1 port=$2
    shift 2
    taskset -c "$CLIENT_CPUS" dotnet "$SIM" --port "$port" --runtime "$RUNTIME" \
        --instances "$CONNECTIONS" --skip-load --csv --label "$name" "${EXTRA[@]+"${EXTRA[@]}"}" "$@" > "$OUT/$name.txt" 2>&1
    if [ "$HEADER_WRITTEN" -eq 0 ]; then
        grep -E '^csv,label' "$OUT/$name.txt" >> "$OUT/ramp.csv"
        HEADER_WRITTEN=1
    fi
    grep -E "^csv,$name," "$OUT/$name.txt" >> "$OUT/ramp.csv"
    grep -E 'completed=' "$OUT/$name.txt"
}

for ops in "${RATES[@]}"; do
    for pair in $TARGETS; do
        name="${pair%%:*}"
        port="${pair##*:}"
        echo "--- $name @ $ops ops/sec ---"
        isolate "$name"
        run "$name-$ops" "$port" --mode Rate --ops "$ops"
    done
done

for c in "${SERVERS[@]}"; do docker unpause "$c" >/dev/null 2>&1 || true; done

echo
echo "Results in $OUT/ramp.csv"
