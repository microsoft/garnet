#!/usr/bin/env bash
# Garnet Streams benchmark orchestrator.
#
# Starts a target server (Garnet or Redis) under a pinned config, optionally preloads a
# dataset, runs the stateless stream workloads with memtier_benchmark (primary) and the
# in-repo Resp.benchmark XADD cross-check, captures server-side CPU/RSS, and writes
# machine-readable results. Stateful consumer-group workloads (W4/W5/W7) are reported as
# "planned" until the closed-loop driver lands (see README section 9).
#
# Usage:
#   scripts/run-suite.sh --target garnet|redis --scenario memory|aof --out <dir> \
#       [--workloads W1-ingest,W2-range,...] [--connections 1,8,32] \
#       [--data-sizes 16,100,1024] [--runtime 30] [--reps 5] [--dry-run]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CFG_DIR="$SCRIPT_DIR/../config"
WORKLOADS_JSON="$CFG_DIR/workloads.json"

# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

TARGET="" SCENARIO="memory" OUT="" DRY_RUN=0
WORKLOADS="W1-ingest,W2-range,W3-tail,W6-meta"
CONNECTIONS="" DATA_SIZES="" RUNTIME="" REPS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="$2"; shift 2 ;;
    --scenario) SCENARIO="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --workloads) WORKLOADS="$2"; shift 2 ;;
    --connections) CONNECTIONS="$2"; shift 2 ;;
    --data-sizes) DATA_SIZES="$2"; shift 2 ;;
    --runtime) RUNTIME="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) die "unknown arg: $1" ;;
  esac
done

[[ -n "$TARGET" ]] || die "--target is required (garnet|redis)"
[[ -n "$OUT" ]] || die "--out is required"
require jq
[[ "$DRY_RUN" == 1 ]] || require "$MEMTIER"

# Resolve target host/port and apply defaults from workloads.json.
HOST=127.0.0.1
if [[ "$TARGET" == garnet ]]; then PORT="$GARNET_PORT"; else PORT="$REDIS_PORT"; fi
def() { jq -r ".defaults.$1" "$WORKLOADS_JSON"; }
: "${RUNTIME:=$(def runtime_seconds)}"
: "${REPS:=$(def repetitions)}"
WARMUP="$(def warmup_seconds)"
KEY_PREFIX="$(def key_prefix)"
KEY_MIN="$(def key_minimum)"
KEY_MAX="$(def key_maximum)"
KEY_PATTERN="$(def command_key_pattern)"
[[ -n "$CONNECTIONS" ]] || CONNECTIONS="1,8,32"
[[ -n "$DATA_SIZES" ]] || DATA_SIZES="16,100,1024"

mkdir -p "$OUT"
DATA_DIR="$OUT/data"

run() { # echo + (unless dry-run) execute, pinned to client CPUs
  log "+ $*"
  [[ "$DRY_RUN" == 1 ]] || on_client_cpus "$@"
}

# memtier one cell -> JSON file. Args: workload_id cmd conns datasize rep out_json
run_memtier() {
  local wid="$1" cmd="$2" conns="$3" dsize="$4" rep="$5" out_json="$6"
  run "$MEMTIER" -s "$HOST" -p "$PORT" \
    --command="$cmd" --command-key-pattern="$KEY_PATTERN" \
    --key-prefix="$KEY_PREFIX" --key-minimum="$KEY_MIN" --key-maximum="$KEY_MAX" \
    --hide-histogram --random-data --data-size="$dsize" \
    -t 1 -c "$conns" --test-time="$RUNTIME" \
    --json-out-file="$out_json"
}

# Preload N entries into a single stream key so read workloads have data on both servers.
preload_stream() {
  local key="$1" entries="$2" dsize="$3"
  log "preload: $entries entries into $key (data-size=$dsize)"
  run "$MEMTIER" -s "$HOST" -p "$PORT" \
    --command="XADD $key * field __data__" --command-key-pattern=P \
    --random-data --data-size="$dsize" \
    -t 1 -c 1 -n "$entries" --pipeline=256 --hide-histogram >/dev/null
}

# --- Start server ----------------------------------------------------------
log "Starting $TARGET ($SCENARIO scenario) on $HOST:$PORT"
SERVER_PID=""
if [[ "$DRY_RUN" != 1 ]]; then
  SERVER_PID="$(start_server "$TARGET" "$SCENARIO" "$DATA_DIR")"
  trap 'stop_server "$SERVER_PID"' EXIT
  log "$TARGET pid=$SERVER_PID"
fi

IFS=',' read -ra WL <<<"$WORKLOADS"
IFS=',' read -ra CONNS <<<"$CONNECTIONS"
IFS=',' read -ra DSIZES <<<"$DATA_SIZES"

for wid in "${WL[@]}"; do
  wl_json="$(jq -c --arg id "$wid" '.workloads[] | select(.id==$id)' "$WORKLOADS_JSON")"
  [[ -n "$wl_json" ]] || { log "skip: unknown workload $wid"; continue; }

  # Stateful workloads need the planned closed-loop driver.
  if [[ "$(jq -r 'has("tools") and (.tools|has("group-driver"))' <<<"$wl_json")" == "true" ]]; then
    log "SKIP $wid: requires the consumer-group closed-loop driver (README section 9, not yet implemented)."
    continue
  fi

  mtcmd="$(jq -r '.tools.memtier.command // empty' <<<"$wl_json")"
  [[ -n "$mtcmd" ]] || { log "skip $wid: no memtier command defined"; continue; }

  # Optional preload for read workloads (use the smaller preload size by default).
  if [[ "$(jq -r '.preload != null' <<<"$wl_json")" == "true" ]]; then
    pcount="$(jq -r '.preload.entries // empty' <<<"$wl_json")"
    [[ -n "$pcount" ]] || pcount="$(jq -r '.sweeps.preload_entries[0] // 1000000' "$WORKLOADS_JSON")"
    pds="$(jq -r '.preload.data_size_bytes // 100' <<<"$wl_json")"
    [[ "$DRY_RUN" == 1 ]] && log "(dry-run) would preload $pcount entries" || preload_stream "${KEY_PREFIX}preload" "$pcount" "$pds"
  fi

  for conns in "${CONNS[@]}"; do
    for dsize in "${DSIZES[@]}"; do
      for ((rep = 1; rep <= REPS; rep++)); do
        cell="$OUT/${wid}_c${conns}_d${dsize}_r${rep}"
        flag="$cell.running"; : >"$flag"
        sampler_pid=""
        [[ -n "$SERVER_PID" ]] && sampler_pid="$(start_stat_sampler "$SERVER_PID" "$cell.stats.csv" "$flag")"
        run_memtier "$wid" "$mtcmd" "$conns" "$dsize" "$rep" "$cell.memtier.json" || log "memtier cell failed: $cell"
        rm -f "$flag"; [[ -n "$sampler_pid" ]] && wait "$sampler_pid" 2>/dev/null || true
      done
    done
  done

  # In-repo cross-check for ops Resp.benchmark supports today (XADD only).
  resp_op="$(jq -r '.tools["resp.benchmark"].op // empty' <<<"$wl_json")"
  resp_status="$(jq -r '.tools["resp.benchmark"].status // "ready"' <<<"$wl_json")"
  if [[ -n "$resp_op" && "$resp_status" == "ready" ]]; then
    run dotnet "$RESP_BENCH_DLL" --host "$HOST" --port "$PORT" \
      --op "$resp_op" --client SERedis -t 8 -b 256 --runtime "$RUNTIME" \
      >"$OUT/${wid}.respbench.txt" 2>&1 || log "resp.benchmark cross-check failed for $wid"
  elif [[ -n "$resp_op" ]]; then
    log "note: Resp.benchmark op $resp_op is $resp_status (see README section 9)"
  fi
done

log "Done. Results in $OUT"
