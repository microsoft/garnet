#!/usr/bin/env bash
#
# Reproduces the end-to-end Sentinel failover against a Garnet primary, and probes
# the data-plane gaps documented in test/standalone/Garnet.test.sentinel/README.md.
#
# What it does:
#   1. starts Garnet as the primary
#   2. starts a stock redis-server as a replica of that Garnet node
#   3. starts a stock redis-sentinel monitoring the Garnet primary
#   4. writes keys on the Garnet primary and checks whether the replica received them
#   5. kills the Garnet primary and watches Sentinel perform a failover
#
# Expected current results (see README "Sentinel support status"):
#   - Sentinel DOES discover the replica and DOES complete the failover
#   - the replica DOES NOT receive any of the written keys (gap 1)
#
# Usage:
#   test/standalone/Garnet.test.sentinel/verify-sentinel-failover.sh
#
# Env:
#   GARNET_SERVER   path to the GarnetServer binary (default: built Debug net10.0)
#   REDIS_SERVER    path to redis-server          (default: per-user cache)
#   REDIS_SENTINEL  path to redis-sentinel        (default: per-user cache)

set -uo pipefail

HOME_DIR="${HOME:-/root}"
CACHE_DIR="${HOME_DIR}/.cache/redis-bin/7.4.11"

GARNET_SERVER="${GARNET_SERVER:-$(dirname "$0")/../../../main/GarnetServer/bin/Debug/net10.0/GarnetServer}"
REDIS_SERVER="${REDIS_SERVER:-${GARNET_TEST_REDIS_SERVER:-${CACHE_DIR}/redis-server}}"
REDIS_SENTINEL="${REDIS_SENTINEL:-${GARNET_TEST_REDIS_SENTINEL:-${CACHE_DIR}/redis-sentinel}}"

GPORT=8100
RPORT=8101
SPORT=28100
WORKDIR="$(mktemp -d /tmp/garnet-sentinel-verify.XXXXXX)"

PIDS=()

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT

fail() { echo "ERROR: $*" >&2; exit 1; }

# The GarnetServer apphost needs DOTNET_ROOT when .NET is installed outside the
# default /usr/share/dotnet location. Derive it from the dotnet on PATH unless the
# caller already set it.
if [ -z "${DOTNET_ROOT:-}" ]; then
    if command -v dotnet >/dev/null 2>&1; then
        DOTNET_ROOT="$(dirname "$(readlink -f "$(command -v dotnet)")")"
        export DOTNET_ROOT
    fi
fi

for f in "$GARNET_SERVER" "$REDIS_SERVER" "$REDIS_SENTINEL"; do
    [ -x "$f" ] || fail "not executable: $f"
done

# Issues a command as a RESP array of bulk strings and prints the raw reply.
resp() {
    local port="$1"; shift
    local out="*$#\r\n"
    for a in "$@"; do
        out+="\$$(printf '%s' "$a" | wc -c)\r\n${a}\r\n"
    done
    printf '%b' "$out" | timeout 3 nc -q1 127.0.0.1 "$port" 2>/dev/null | tr -d '\r'
}

echo "=== workdir: $WORKDIR ==="
echo "garnet:   $GARNET_SERVER"
echo "redis:    $REDIS_SERVER"
echo "sentinel: $REDIS_SENTINEL"
echo "DOTNET_ROOT: ${DOTNET_ROOT:-<unset>}"

echo
echo "--- starting Garnet primary on :$GPORT ---"
"$GARNET_SERVER" --port "$GPORT" --no-obj >"$WORKDIR/garnet.log" 2>&1 &
PIDS+=($!)

for _ in $(seq 1 60); do
    resp "$GPORT" PING | grep -q PONG && break
    sleep 0.5
done
resp "$GPORT" PING | grep -q PONG || fail "Garnet did not start; see $WORKDIR/garnet.log"

echo "--- writing keys on the Garnet primary BEFORE the replica attaches ---"
for i in 0 1 2 3 4; do
    resp "$GPORT" SET "pre$i" "value$i" >/dev/null
done
echo "wrote pre0..pre4"

echo
echo "--- starting stock redis replica on :$RPORT ---"
"$REDIS_SERVER" --port "$RPORT" --replicaof 127.0.0.1 "$GPORT" \
    --save '' --appendonly no --dir "$WORKDIR" \
    --logfile "$WORKDIR/replica.log" >/dev/null 2>&1 &
PIDS+=($!)

for _ in $(seq 1 60); do
    resp "$RPORT" PING | grep -q PONG && break
    sleep 0.5
done

echo "--- starting stock redis-sentinel on :$SPORT monitoring the Garnet primary ---"
cat >"$WORKDIR/sentinel.conf" <<EOF
port $SPORT
dir $WORKDIR
logfile $WORKDIR/sentinel.log
sentinel monitor mymaster 127.0.0.1 $GPORT 1
sentinel down-after-milliseconds mymaster 3000
sentinel failover-timeout mymaster 10000
sentinel parallel-syncs mymaster 1
EOF
"$REDIS_SENTINEL" "$WORKDIR/sentinel.conf" --sentinel >/dev/null 2>&1 &
PIDS+=($!)

echo "waiting for Sentinel to discover the replica..."
sleep 12

echo
echo "=== [1] Does Sentinel see the Garnet primary? ==="
resp "$SPORT" SENTINEL master mymaster | grep -E '^(flags|role-reported|num-slaves)$' -A1 || true

echo
echo "=== [2] Does Sentinel see the replica? (expect num-slaves 1) ==="
resp "$SPORT" SENTINEL master mymaster | grep -A1 '^num-slaves$' || true

echo
echo "=== [3] GAP 1: did any data reach the replica? ==="
echo -n "replica KEYS *      -> "; resp "$RPORT" KEYS '*' | tr '\n' ' '; echo
echo -n "replica GET pre0    -> "; resp "$RPORT" GET pre0 | tr '\n' ' '; echo
echo "(empty / nil while the primary holds pre0..pre4 means NO data is replicated)"

echo
echo "=== [4] GAP 2: is the link actually alive? ==="
resp "$RPORT" INFO replication | grep -E 'master_link_status|master_last_io_seconds_ago' || true
echo "(link=up with a growing last_io and no data means the link is silently idle)"

echo
echo "=== [5] GAP 4: does master_repl_offset advance? ==="
resp "$GPORT" INFO replication | grep -E 'master_repl_offset|slave0' || true

echo
echo "--- killing the Garnet primary to trigger failover ---"
kill -9 "${PIDS[0]}" 2>/dev/null || true
sleep 20

echo
echo "=== [6] Sentinel failover log ==="
grep -E 'slave|sdown|odown|failover|promot|switch-master|reconfig' "$WORKDIR/sentinel.log" || true

echo
echo "=== [7] Role of the promoted replica ==="
resp "$RPORT" INFO replication | grep -E '^role:' || true

echo
echo "=== Interpretation ==="
echo "A complete failover sequence above (through +switch-master) with the replica"
echo "ending as role:master means the CONTROL PLANE works."
echo "Empty data in step [3] means the DATA PLANE does not: the promoted primary is empty."
echo
echo "Artifacts kept in $WORKDIR"
echo "  garnet.log, replica.log, sentinel.log"
