#!/usr/bin/env bash
#
# nvme-raid0-matrix.sh — reproducible RESP GET throughput matrix for the NVMe
# storage-bound scenario (Resp.benchmark README scenario 2).
#
# Sweeps {Libaio, Uring} x {NUMA-pinned, no-pin} x thread-count, driving a real
# GarnetServer whose 100 M x 128 B dataset is tiered onto an NVMe device so every
# GET is a random disk read. Prints a Markdown table of median-of-N throughput.
#
# This is the generator behind the "Sample results — 8x NVMe SSD RAID-0" table in
# the Resp.benchmark README. Re-run it to regression-check device-serving perf.
#
# Requirements: Release builds of GarnetServer + Resp.benchmark, numactl, an NVMe
# (or other O_DIRECT-capable) mount for the tiered log, and Linux (Native device).
#
# Usage:
#   benchmark/Resp.benchmark/scripts/nvme-raid0-matrix.sh
#
# Override any of the environment variables below, e.g.:
#   DATA=/mnt/nvme/garnet THREADS="8 32 64" PASSES=3 \
#     benchmark/Resp.benchmark/scripts/nvme-raid0-matrix.sh
#
set -uo pipefail

# ---- configuration (override via environment) --------------------------------
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TFM="${TFM:-net10.0}"
GS="${GS:-$ROOT/main/GarnetServer/bin/Release/$TFM/GarnetServer.dll}"
RB="${RB:-$ROOT/benchmark/Resp.benchmark/bin/Release/$TFM/Resp.benchmark.dll}"
DATA="${DATA:-/raid/garnet-nvme-matrix}"          # tiered-log dir on an NVMe mount
PORT="${PORT:-6379}"
DBSIZE="${DBSIZE:-100000000}"                      # 100 M keys
KEYLEN="${KEYLEN:-16}"; VALLEN="${VALLEN:-96}"     # 128 B records
REQB="${REQB:-1024}"                               # client pipeline depth (-b)
RUNTIME="${RUNTIME:-12}"                           # seconds per GET cell
THREADS="${THREADS:-8 32 48 64}"                   # client thread-count sweep
PASSES="${PASSES:-3}"                              # passes per cell (median taken)
BACKENDS="${BACKENDS:-Libaio Uring}"
PINMODES="${PINMODES:-pin nopin}"
CT="${CT:-}"                                       # --device-completion-threads (empty = server default 4)
THROTTLE="${THROTTLE:-}"                            # --device-throttle-limit    (empty = server default 4096)
URING_IOCTX="${URING_IOCTX:-}"                      # Uring --device-io-contexts (empty = smart default, ~64 rings)
SRVNODE="${SRVNODE:-0}"; CLINODE="${CLINODE:-1}"   # NUMA nodes for pinned mode
OUT="${OUT:-/tmp/nvme-raid0-matrix.tsv}"

# ---- helpers -----------------------------------------------------------------
have() { command -v "$1" >/dev/null 2>&1; }
NUMACTL="numactl"; have numactl || NUMACTL=""

srv_pid() { pgrep -f "dotnet .*GarnetServer.dll .*--port $PORT" | head -1; }

wait_ready() {  # wait until the server accepts connections
  for _ in $(seq 1 60); do
    if have redis-cli && redis-cli -p "$PORT" PING >/dev/null 2>&1; then return 0; fi
    if (exec 3<>/dev/tcp/127.0.0.1/"$PORT") 2>/dev/null; then exec 3>&- 3<&-; return 0; fi
    sleep 1
  done
  return 1
}

dbsize() {      # current key count, or empty if it cannot be read
  if have redis-cli; then
    redis-cli -p "$PORT" DBSIZE 2>/dev/null | grep -oE '^[0-9]+'
    return
  fi
  # Raw RESP: send DBSIZE, read the single ":<n>\r\n" integer reply line. Read with a
  # timeout and stop at the line terminator — a fixed-size read would block waiting for
  # bytes the server never sends.
  (exec 3<>/dev/tcp/127.0.0.1/"$PORT" || return 0
   printf '*1\r\n$6\r\nDBSIZE\r\n' >&3
   IFS= read -r -t 10 reply <&3
   exec 3>&- 3<&-
   printf '%s' "${reply:-}" | grep -oE ':[0-9]+' | head -1 | tr -d ':') 2>/dev/null
}

stop_srv() {    # stop by real GarnetServer.dll PID (not the dotnet launcher)
  local p; p="$(srv_pid)"
  [ -n "$p" ] && kill "$p" 2>/dev/null
  for _ in $(seq 1 30); do [ -z "$(srv_pid)" ] && break; sleep 1; done
  p="$(srv_pid)"; [ -n "$p" ] && kill -9 "$p" 2>/dev/null; sleep 1
}

median() {      # median of stdin numbers
  sort -n | awk '{a[NR]=$1} END{if(NR==0){print 0;exit} m=int((NR+1)/2);
    if(NR%2)printf "%.3f",a[m]; else printf "%.3f",(a[m]+a[m+1])/2}'
}

# ---- run one (backend, pinmode) server + its thread/pass sweep ---------------
run_config() {
  local backend="$1" pinmode="$2"
  local srv_pin="" cli_pin=""
  if [ "$pinmode" = "pin" ] && [ -n "$NUMACTL" ]; then
    srv_pin="$NUMACTL --cpunodebind=$SRVNODE --membind=$SRVNODE"
    cli_pin="$NUMACTL --cpunodebind=$CLINODE --membind=$CLINODE"
  fi

  # Out-of-box device config: only the backend is forced. All capacity knobs
  # (completion-threads, throttle-limit, io-contexts) are left at the server
  # defaults unless explicitly overridden via the environment — this is the
  # config an external user gets with just `--storage-tier`. Set CT/THROTTLE/
  # URING_IOCTX to reproduce the hand-tuned configuration instead.
  local devflags="--device-type Native --device-io-backend $backend"
  [ -n "$CT" ] && devflags="$devflags --device-completion-threads $CT"
  [ -n "$THROTTLE" ] && devflags="$devflags --device-throttle-limit $THROTTLE"
  [ "$backend" = "Uring" ] && [ -n "$URING_IOCTX" ] && devflags="$devflags --device-io-contexts $URING_IOCTX"

  rm -rf "$DATA"; mkdir -p "$DATA"
  stop_srv
  # shellcheck disable=SC2086
  $srv_pin dotnet "$GS" --port "$PORT" --bind 127.0.0.1 \
    --memory 16m --page 4m --segment 1g --index 8g --storage-tier --logdir "$DATA" \
    $devflags --logger-level Warning >/tmp/nvme-matrix-srv.log 2>&1 &
  if ! wait_ready; then echo "!! server failed to start ($backend/$pinmode)"; cat /tmp/nvme-matrix-srv.log; return 1; fi

  # load the 100 M dataset (writes tier to the device; no run phase)
  # shellcheck disable=SC2086
  if ! $cli_pin dotnet "$RB" --port "$PORT" --op MSET --dbsize "$DBSIZE" --keylength "$KEYLEN" --valuelength "$VALLEN" \
    --client LightClient --load-threads 32 -b 4096 --runtime 0 >/tmp/nvme-matrix-load.log 2>&1; then
    echo "!! dataset load failed ($backend/$pinmode)"; tail -20 /tmp/nvme-matrix-load.log; stop_srv; return 1
  fi

  # Guard against a silently short load: GETs against a mostly-empty store are served as
  # in-memory misses, which would publish a high but meaningless "NVMe" number.
  local loaded; loaded="$(dbsize)"
  if [ -z "$loaded" ] || [ "$loaded" -lt "$((DBSIZE / 10 * 9))" ]; then
    echo "!! dataset load short ($backend/$pinmode): DBSIZE=${loaded:-unknown}, expected >= $((DBSIZE / 10 * 9))"
    stop_srv; return 1
  fi

  for t in $THREADS; do
    local vals=()
    for _ in $(seq 1 "$PASSES"); do
      # shellcheck disable=SC2086
      local out; out="$($cli_pin dotnet "$RB" -s --port "$PORT" --op GET --dbsize "$DBSIZE" \
        --keylength "$KEYLEN" --valuelength "$VALLEN" --client LightClient \
        -t "$t" -b "$REQB" --runtime "$RUNTIME" 2>/dev/null)"
      # Throughput line: "[Throughput]: 1,843,118.13 ops/sec" (comma-separated)
      local tp; tp="$(echo "$out" | grep -iE '^\[Throughput\]' | tr -d ',' | grep -oE '[0-9]+\.[0-9]+' | head -1)"
      [ -z "$tp" ] && tp=0
      # convert ops/sec -> Mops/sec
      local m; m="$(awk -v x="$tp" 'BEGIN{printf "%.3f", x/1000000}')"
      vals+=("$m")
      printf '%s\t%s\t%s\t%s\n' "$backend" "$pinmode" "$t" "$m" >> "$OUT"
    done
    local med; med="$(printf '%s\n' "${vals[@]}" | median)"
    printf '  %-7s %-5s t=%-3s -> median %s M/s   (passes: %s)\n' "$backend" "$pinmode" "$t" "$med" "${vals[*]}"
  done
  stop_srv
  rm -rf "$DATA"
}

# ---- main --------------------------------------------------------------------
[ -f "$GS" ] || { echo "GarnetServer.dll not found: $GS (build Release first)"; exit 1; }
[ -f "$RB" ] || { echo "Resp.benchmark.dll not found: $RB (build Release first)"; exit 1; }
: > "$OUT"
echo "== NVMe RAID-0 RESP GET matrix =="
echo "   GS=$GS"
echo "   RB=$RB"
echo "   DATA=$DATA  dbsize=$DBSIZE  record=$((KEYLEN+VALLEN))B  reqb=$REQB  runtime=${RUNTIME}s  passes=$PASSES"
echo "   ct=${CT:-default} throttle=${THROTTLE:-default} uring-io-contexts=${URING_IOCTX:-default(smart)} threads='$THREADS'"
echo

for backend in $BACKENDS; do
  for pinmode in $PINMODES; do
    echo "-- $backend / $pinmode --"
    run_config "$backend" "$pinmode"
    echo
  done
done

# ---- emit the Markdown table -------------------------------------------------
echo "==== Markdown table (median-of-$PASSES, Mops/sec) ===="
{
  printf '| backend | NUMA |'; for t in $THREADS; do printf ' t=%s |' "$t"; done; printf '\n'
  printf '|---|---|'; for _ in $THREADS; do printf '%s' '---|'; done; printf '\n'
  for backend in $BACKENDS; do
    for pinmode in $PINMODES; do
      local_label="no pin"; [ "$pinmode" = "pin" ] && local_label="srv node-$SRVNODE / cli node-$CLINODE"
      printf '| %s | %s |' "$backend" "$local_label"
      for t in $THREADS; do
        med="$(awk -F'\t' -v b="$backend" -v p="$pinmode" -v t="$t" \
          '$1==b&&$2==p&&$3==t{print $4}' "$OUT" | median)"
        printf ' %s M |' "$med"
      done
      printf '\n'
    done
  done
} | tee /tmp/nvme-raid0-matrix.md
echo
echo "raw samples: $OUT ; table: /tmp/nvme-raid0-matrix.md"
