#!/usr/bin/env bash
# Shared helpers for the Garnet Streams benchmark harness.
# Sourced by run-suite.sh and env-capture.sh. Linux bench host assumed.

set -euo pipefail

# --- Defaults (override via run-suite.sh flags / environment) ---
: "${GARNET_DLL:=$REPO_ROOT/main/GarnetServer/bin/Release/net10.0/GarnetServer.dll}"
: "${RESP_BENCH_DLL:=$REPO_ROOT/benchmark/Resp.benchmark/bin/Release/net10.0/Resp.benchmark.dll}"
: "${REDIS_SERVER:=redis-server}"
: "${MEMTIER:=memtier_benchmark}"

: "${GARNET_PORT:=6379}"
: "${REDIS_PORT:=6380}"

# CPU pinning: server cores vs client cores must be disjoint (README section 6).
: "${SERVER_CPUS:=0-3}"
: "${CLIENT_CPUS:=4-7}"

log() { printf '[%(%H:%M:%S)T] %s\n' -1 "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

require() { command -v "$1" >/dev/null 2>&1 || die "required tool not found on PATH: $1"; }

# Pin a command to the server CPU set.
on_server_cpus() { taskset -c "$SERVER_CPUS" "$@"; }
# Pin a command to the client CPU set.
on_client_cpus() { taskset -c "$CLIENT_CPUS" "$@"; }

wait_for_port() {
  local host="$1" port="$2" tries="${3:-60}"
  for ((i = 0; i < tries; i++)); do
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then exec 3>&- 3<&-; return 0; fi
    sleep 0.25
  done
  die "server did not start listening on $host:$port"
}

# --- Server lifecycle -------------------------------------------------------
# start_server <target> <scenario> <data_dir>  -> echoes the server PID
start_server() {
  local target="$1" scenario="$2" data_dir="$3"
  mkdir -p "$data_dir"
  local pid
  case "$target" in
    garnet)
      local args=(--port "$GARNET_PORT")
      if [[ "$scenario" == "aof" ]]; then
        # Durable scenario: AOF on + per-stream log on disk. Confirm exact flag names
        # against `dotnet "$GARNET_DLL" --help` for your Garnet version.
        args+=(--aof --aof-commit-freq 1000 --checkpointdir "$data_dir/ckpt" --stream-log-dir "$data_dir/streams")
      else
        # memory scenario: streams stay on NullDevice (no --stream-log-dir), AOF off.
        :
      fi
      on_server_cpus dotnet "$GARNET_DLL" "${args[@]}" >"$data_dir/server.log" 2>&1 &
      pid=$!
      wait_for_port 127.0.0.1 "$GARNET_PORT"
      ;;
    redis)
      local args=("$SCRIPT_DIR/../config/redis.conf" --port "$REDIS_PORT" --dir "$data_dir")
      if [[ "$scenario" == "aof" ]]; then
        args+=(--appendonly yes --appendfsync everysec)
      else
        args+=(--appendonly no --save "")
      fi
      on_server_cpus "$REDIS_SERVER" "${args[@]}" >"$data_dir/server.log" 2>&1 &
      pid=$!
      wait_for_port 127.0.0.1 "$REDIS_PORT"
      ;;
    *) die "unknown target: $target (expected garnet|redis)" ;;
  esac
  echo "$pid"
}

stop_server() {
  local pid="$1"
  [[ -n "$pid" ]] || return 0
  kill "$pid" 2>/dev/null || true
  for ((i = 0; i < 40; i++)); do kill -0 "$pid" 2>/dev/null || return 0; sleep 0.25; done
  kill -9 "$pid" 2>/dev/null || true
}

# Sample server CPU% and RSS every second into a CSV until the named pidfile is removed.
# capture_stats <pid> <out_csv> &   ... then `rm -f <flagfile>` to stop via the caller.
start_stat_sampler() {
  local pid="$1" out="$2" flag="$3"
  echo "ts,cpu_pct,rss_kb" >"$out"
  ( while [[ -e "$flag" ]] && kill -0 "$pid" 2>/dev/null; do
      # %cpu and rss from ps; pidstat is preferred if available.
      read -r cpu rss < <(ps -o %cpu=,rss= -p "$pid" 2>/dev/null || echo "0 0")
      printf '%s,%s,%s\n' "$(date +%s)" "${cpu:-0}" "${rss:-0}" >>"$out"
      sleep 1
    done ) &
  echo $!
}
