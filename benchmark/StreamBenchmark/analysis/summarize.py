#!/usr/bin/env python3
"""Summarize Garnet Streams benchmark results into tables + throughput-latency plots.

Reads the per-cell artifacts written by run-suite.sh:
  <wid>_c<conns>_d<datasize>_r<rep>.memtier.json   (memtier_benchmark --json-out-file)
  <wid>_c<conns>_d<datasize>_r<rep>.stats.csv       (server CPU%/RSS samples)

Emits, under <results_dir>/summary/:
  summary.csv   one row per (workload, connections, data_size) with median + CV%
  summary.md    a Markdown table for pasting into a report
  *.png         throughput-vs-connections and latency-vs-throughput plots (if matplotlib)

Usage: python3 analysis/summarize.py <results_dir> [<results_dir> ...]
"""
from __future__ import annotations
import csv
import json
import re
import statistics
import sys
from pathlib import Path

CELL_RE = re.compile(r"^(?P<wid>.+?)_c(?P<conns>\d+)_d(?P<dsize>\d+)_r(?P<rep>\d+)\.memtier\.json$")


def _find(obj, *names):
    """Depth-first search for the first key in `names` anywhere in a nested dict/list."""
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for k, v in cur.items():
                if k in names and isinstance(v, (int, float)):
                    return float(v)
                stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)
    return None


def parse_memtier(path: Path) -> dict | None:
    try:
        data = json.loads(path.read_text())
    except Exception as e:  # noqa: BLE001
        print(f"  warn: cannot parse {path.name}: {e}", file=sys.stderr)
        return None
    return {
        "ops_per_sec": _find(data, "Ops/sec", "Ops/Sec", "ops_per_sec"),
        "p50": _find(data, "p50.00", "Percentile 50.000000", "p50"),
        "p99": _find(data, "p99.00", "Percentile 99.000000", "p99"),
        "p999": _find(data, "p99.90", "Percentile 99.900000", "p99.9"),
        "avg_latency": _find(data, "Average Latency", "avg_latency"),
    }


def server_stats(cell_base: Path) -> dict:
    csv_path = cell_base.with_suffix(".stats.csv")
    if not csv_path.exists():
        return {"cpu_pct_mean": None, "rss_kb_peak": None}
    cpus, rss = [], []
    with csv_path.open() as f:
        for row in csv.DictReader(f):
            try:
                cpus.append(float(row["cpu_pct"]))
                rss.append(float(row["rss_kb"]))
            except (KeyError, ValueError):
                continue
    return {
        "cpu_pct_mean": round(statistics.mean(cpus), 1) if cpus else None,
        "rss_kb_peak": max(rss) if rss else None,
    }


def cv_pct(values: list[float]) -> float | None:
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return None
    m = statistics.mean(vals)
    return round(100.0 * statistics.pstdev(vals) / m, 1) if m else None


def median(values: list[float]) -> float | None:
    vals = [v for v in values if v is not None]
    return round(statistics.median(vals), 3) if vals else None


def collect(results_dir: Path) -> list[dict]:
    # group reps by (wid, conns, dsize)
    groups: dict[tuple, list[dict]] = {}
    for jf in results_dir.rglob("*.memtier.json"):
        m = CELL_RE.match(jf.name)
        if not m:
            continue
        parsed = parse_memtier(jf)
        if not parsed:
            continue
        parsed.update(server_stats(jf.with_name(jf.name[: -len(".memtier.json")])))
        key = (m["wid"], int(m["conns"]), int(m["dsize"]))
        groups.setdefault(key, []).append(parsed)

    rows = []
    for (wid, conns, dsize), reps in sorted(groups.items()):
        rows.append({
            "workload": wid,
            "connections": conns,
            "data_size": dsize,
            "reps": len(reps),
            "ops_per_sec_median": median([r["ops_per_sec"] for r in reps]),
            "ops_per_sec_cv_pct": cv_pct([r["ops_per_sec"] for r in reps]),
            "p50_ms_median": median([r["p50"] for r in reps]),
            "p99_ms_median": median([r["p99"] for r in reps]),
            "p999_ms_median": median([r["p999"] for r in reps]),
            "cpu_pct_mean": median([r["cpu_pct_mean"] for r in reps]),
            "rss_kb_peak": median([r["rss_kb_peak"] for r in reps]),
        })
    return rows


def write_csv(rows: list[dict], out: Path) -> None:
    if not rows:
        return
    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def write_md(rows: list[dict], out: Path) -> None:
    cols = ["workload", "connections", "data_size", "reps", "ops_per_sec_median",
            "ops_per_sec_cv_pct", "p50_ms_median", "p99_ms_median", "p999_ms_median",
            "cpu_pct_mean", "rss_kb_peak"]
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join(["---"] * len(cols)) + "|"]
    for r in rows:
        lines.append("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |")
    out.write_text("\n".join(lines) + "\n")


def make_plots(rows: list[dict], out_dir: Path) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("  note: matplotlib not installed; skipping plots (pip install matplotlib)", file=sys.stderr)
        return
    workloads = sorted({r["workload"] for r in rows})
    # Throughput vs connections (one line per workload, at the largest data size present).
    plt.figure()
    for wid in workloads:
        pts = sorted([r for r in rows if r["workload"] == wid and r["ops_per_sec_median"]],
                     key=lambda r: r["connections"])
        if pts:
            plt.plot([p["connections"] for p in pts], [p["ops_per_sec_median"] for p in pts], marker="o", label=wid)
    plt.xlabel("connections"); plt.ylabel("ops/sec (median)"); plt.legend(); plt.title("Throughput vs connections")
    plt.savefig(out_dir / "throughput_vs_connections.png", dpi=120, bbox_inches="tight")
    plt.close()
    # Latency-throughput curve (p99 vs ops/sec).
    plt.figure()
    for wid in workloads:
        pts = [r for r in rows if r["workload"] == wid and r["ops_per_sec_median"] and r["p99_ms_median"]]
        if pts:
            pts.sort(key=lambda r: r["ops_per_sec_median"])
            plt.plot([p["ops_per_sec_median"] for p in pts], [p["p99_ms_median"] for p in pts], marker="o", label=wid)
    plt.xlabel("ops/sec (median)"); plt.ylabel("p99 latency (ms)"); plt.legend(); plt.title("Latency vs throughput")
    plt.savefig(out_dir / "latency_vs_throughput.png", dpi=120, bbox_inches="tight")
    plt.close()


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 2
    for d in argv[1:]:
        results_dir = Path(d)
        if not results_dir.is_dir():
            print(f"skip: not a directory: {results_dir}", file=sys.stderr)
            continue
        rows = collect(results_dir)
        out_dir = results_dir / "summary"
        out_dir.mkdir(exist_ok=True)
        write_csv(rows, out_dir / "summary.csv")
        write_md(rows, out_dir / "summary.md")
        make_plots(rows, out_dir)
        print(f"{results_dir}: {len(rows)} cells -> {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
