#!/usr/bin/env bash
# Capture the benchmark environment for reproducibility (README section 1 & 10).
# Usage: scripts/env-capture.sh <out_dir>
set -euo pipefail

OUT="${1:?usage: env-capture.sh <out_dir>}"
mkdir -p "$OUT"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

{
  echo "## Captured: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "### Garnet"
  echo "commit: $(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "branch: $(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
  echo "dotnet: $(dotnet --version 2>/dev/null || echo n/a)"
  echo
  echo "### Comparison targets"
  echo "redis-server: $({ redis-server --version 2>/dev/null; } || echo 'not installed')"
  echo "memtier_benchmark: $({ memtier_benchmark --version 2>/dev/null | head -1; } || echo 'not installed')"
  echo
  echo "### Host"
  echo "kernel: $(uname -srmo 2>/dev/null || uname -a)"
  echo "cpu: $(LC_ALL=C lscpu 2>/dev/null | sed -n 's/^Model name:[[:space:]]*//p')"
  echo "cores/threads: $(nproc 2>/dev/null) online"
  echo "governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)"
  echo "mem: $(free -h 2>/dev/null | sed -n '2p')"
  echo
  echo "### NUMA"
  numactl --hardware 2>/dev/null || echo "numactl not available"
} >"$OUT/environment.md"

# Full machine-readable dumps for the archive.
lscpu >"$OUT/lscpu.txt" 2>/dev/null || true
cat /proc/meminfo >"$OUT/meminfo.txt" 2>/dev/null || true

echo "Environment captured to $OUT/environment.md"
