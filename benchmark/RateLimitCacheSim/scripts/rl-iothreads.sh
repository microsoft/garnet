#!/usr/bin/env bash
# Sweeps an OSS baseline's io-threads settings under TLS against the modelled workload.
# Set SERVER=redis (default) or SERVER=valkey to tune either baseline the same way.
#
# Both perform TLS crypto on the thread that owns the connection, so the io-threads count and
# io-threads-do-reads both change how much of a multi-core box the server can actually use. This
# sweep measures that directly: for each setting it walks a rate ladder and stops as soon as the
# server can no longer keep up, which brackets that setting's ceiling.
set -u

SERVER=${SERVER:-redis}
case "$SERVER" in
    redis)  DEFAULT_IMAGE=redis:7.4;         SERVER_BIN=redis-server;  CLI_BIN=redis-cli;  DEFAULT_PORT=7012 ;;
    valkey) DEFAULT_IMAGE=valkey/valkey:8.1; SERVER_BIN=valkey-server; CLI_BIN=valkey-cli; DEFAULT_PORT=7013 ;;
    *) echo "SERVER must be redis or valkey" >&2; exit 1 ;;
esac
CONTAINER="$SERVER-it"

OUT=${OUT:-/tmp/rl-iothreads-$SERVER}
PORT=${PORT:-$DEFAULT_PORT}
SERVER_CPUS=${SERVER_CPUS:-4,6,84,86}
CLIENT_CPUS=${CLIENT_CPUS:-$(seq -s, 101 2 159)}
RUNTIME=${RUNTIME:-25}
WARMUP=${WARMUP:-5}
CONN=${CONN:-64}
CERTS=${CERTS:-/tmp/rlcerts}
IMAGE=${IMAGE:-$DEFAULT_IMAGE}
SIM=${SIM:-/home/badrishc/git/garnet1/benchmark/RateLimitCacheSim/bin/Release/net10.0/RateLimitCacheSim.dll}
RATES=${RATES:-"50000 85000 108000 130000"}

mkdir -p "$OUT"
CSV="$OUT/results.csv"
echo "config,io_threads,do_reads,target,throughput,svc_p50,svc_p95,svc_p99,svc_p999,sched_p50,sched_p99,errors" > "$CSV"

stop_server() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1
    # Wait for the port to actually clear so the next container can bind it.
    for _ in $(seq 1 30); do
        ss -ltn 2>/dev/null | grep -q ":$PORT " || return 0
        sleep 0.3
    done
}

start_server() { # io_threads do_reads
    local threads=$1 doreads=$2
    stop_server
    docker run -d --name "$CONTAINER" --network host \
        --cpuset-cpus "$SERVER_CPUS" \
        -v "$CERTS":/certs:ro \
        "$IMAGE" "$SERVER_BIN" \
        --port 0 \
        --tls-port "$PORT" \
        --tls-cert-file /certs/cert.pem \
        --tls-key-file /certs/key.pem \
        --tls-ca-cert-file /certs/cert.pem \
        --tls-auth-clients no \
        --save '' --appendonly no \
        --io-threads "$threads" \
        --io-threads-do-reads "$doreads" >/dev/null 2>&1

    for _ in $(seq 1 40); do
        if docker exec "$CONTAINER" "$CLI_BIN" --tls --cert /certs/cert.pem --key /certs/key.pem \
             --cacert /certs/cert.pem -p "$PORT" PING 2>/dev/null | grep -q PONG; then
            # Confirm the setting actually took effect rather than being silently ignored.
            local got
            got=$(docker exec "$CONTAINER" "$CLI_BIN" --tls --cert /certs/cert.pem --key /certs/key.pem \
                    --cacert /certs/cert.pem -p "$PORT" CONFIG GET io-threads 2>/dev/null | tail -1)
            echo "    $SERVER up: io-threads=$got do-reads=$doreads"
            return 0
        fi
        sleep 0.3
    done
    echo "    FAILED to start $SERVER io-threads=$threads do-reads=$doreads"
    docker logs "$CONTAINER" 2>&1 | tail -5
    return 1
}

run_one() { # label rate skipflag -> echoes csv fields
    local label=$1 rate=$2 skip=$3
    taskset -c "$CLIENT_CPUS" dotnet "$SIM" \
        --mode Rate --tls --port "$PORT" \
        --ops "$rate" --instances "$CONN" \
        --runtime "$RUNTIME" --warmup "$WARMUP" \
        --label "$label" --csv $skip 2>&1 | tee "$OUT/$label-$rate.txt" | grep '^csv,' | tail -1
}

CONFIGS=${CONFIGS:-"1:no 1:yes 2:no 2:yes 3:no 3:yes 4:no 4:yes"}

for cfg in $CONFIGS; do
    threads=${cfg%%:*}
    doreads=${cfg##*:}
    name="it${threads}-dr${doreads}"
    echo "=== $name ==="
    start_server "$threads" "$doreads" || continue

    skip=""
    for rate in $RATES; do
        line=$(run_one "$name" "$rate" "$skip")
        skip="--skip-load"
        if [ -z "$line" ]; then
            echo "    $rate: NO RESULT"
            continue
        fi
        # csv,label,mode,client,instances,offered,throughput,svc_p50,svc_p95,svc_p99,svc_p999,sched_p50,sched_p99,e2e...,errors
        thr=$(echo "$line" | cut -d, -f7)
        p50=$(echo "$line" | cut -d, -f8)
        p95=$(echo "$line" | cut -d, -f9)
        p99=$(echo "$line" | cut -d, -f10)
        p999=$(echo "$line" | cut -d, -f11)
        sp50=$(echo "$line" | cut -d, -f12)
        sp99=$(echo "$line" | cut -d, -f13)
        err=$(echo "$line" | awk -F, '{print $NF}')
        echo "$name,$threads,$doreads,$rate,$thr,$p50,$p95,$p99,$p999,$sp50,$sp99,$err" >> "$CSV"
        echo "    rate=$rate achieved=$thr svc_p50=$p50 svc_p99=$p99 sched_p99=$sp99"

        # Stop climbing once the server cannot hold the offered rate.
        keep=$(awk -v t="$thr" -v r="$rate" 'BEGIN{print (t < r*0.95) ? 0 : 1}')
        if [ "$keep" = "0" ]; then
            echo "    -> collapsed at $rate; ceiling is below this. Stopping ladder."
            break
        fi
    done
done

stop_server
echo
echo "=== results: $CSV ==="
column -s, -t "$CSV"
