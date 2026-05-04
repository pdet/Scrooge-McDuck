#!/usr/bin/env python3
"""
Benchmark Scrooge indicator throughput on synthetic data.

Generates a 1M-row OHLCV table, runs each benchmark query 3 times via
the local DuckDB binary with scrooge loaded, and reports best wall-clock
times. Designed for tracking regression across commits, not for absolute
comparison against other libraries.

Usage:
    python3 scripts/benchmarks/run_benchmarks.py [--rows 1000000] [--runs 3]

Output:
    benchmark                   median_ms   best_ms
    -----------                 ---------   -------
    ema_window                       ...       ...
"""

from __future__ import annotations

import argparse
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import List


HERE = Path(__file__).resolve().parents[2]


def run_one(duckdb_bin: Path, ext_path: Path, sql: str) -> float:
    script = "\n".join([
        ".bail on",
        "SET autoload_known_extensions=1;",
        "SET autoinstall_known_extensions=1;",
        f"LOAD '{ext_path.resolve()}';",
        ".timer off",
        ".mode csv",
        ".headers off",
        sql + ";",
    ])
    start = time.perf_counter()
    out = subprocess.run(
        [str(duckdb_bin.resolve()), "-unsigned"],
        input=script, capture_output=True, text=True, check=False,
    )
    elapsed = (time.perf_counter() - start) * 1000.0
    if out.returncode != 0:
        raise RuntimeError(f"benchmark failed: {out.stderr.strip()}")
    return elapsed


BENCHMARKS = [
    # Window indicators on a generated price series.
    ("ema_window",
     "WITH t AS (SELECT '2020-01-01'::TIMESTAMP + (INTERVAL 1 MINUTE) * i AS ts, "
     "100 + sin(i / 100.0) * 5 AS px FROM range({rows}) r(i)) "
     "SELECT count(*) FROM ("
     "  SELECT ema(px, ts, 20) OVER (ORDER BY ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) FROM t"
     ")"),
    ("rsi_window",
     "WITH t AS (SELECT '2020-01-01'::TIMESTAMP + (INTERVAL 1 MINUTE) * i AS ts, "
     "100 + sin(i / 100.0) * 5 AS px FROM range({rows}) r(i)) "
     "SELECT count(*) FROM ("
     "  SELECT rsi(px, ts, 14) OVER (ORDER BY ts ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) FROM t"
     ")"),
    ("monte_carlo_paths",
     # Use a smaller horizon to keep runtime reasonable; this exercises the
     # generator throughput rather than indicator math.
     "SELECT count(*) FROM monte_carlo(100.0, 0.05, 0.20, 100, {paths})"),
    ("bond_pricing_scalar",
     # Scalar fan-out — measure raw scalar throughput.
     "SELECT sum(bond_price(1000.0, 0.05, 0.04 + i * 0.0001, 20, 2.0)) "
     "FROM range({rows}) r(i)"),
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--paths", type=int, default=1000)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--duckdb", default=HERE / "build" / "release" / "duckdb")
    parser.add_argument("--extension",
                        default=HERE / "build" / "release" / "extension" / "scrooge" / "scrooge.duckdb_extension")
    args = parser.parse_args()

    duckdb_bin = Path(args.duckdb)
    ext_path = Path(args.extension)
    if not duckdb_bin.exists() or not ext_path.exists():
        print("missing duckdb binary or scrooge extension; build the project first",
              file=sys.stderr)
        return 2

    print(f"{'benchmark':30} {'median_ms':>12} {'best_ms':>12}")
    print(f"{'-' * 30} {'-' * 12} {'-' * 12}")

    fail = 0
    for name, sql_template in BENCHMARKS:
        sql = sql_template.format(rows=args.rows, paths=args.paths)
        timings: List[float] = []
        try:
            for _ in range(args.runs):
                timings.append(run_one(duckdb_bin, ext_path, sql))
        except RuntimeError as e:
            print(f"{name:30} FAILED: {e}", file=sys.stderr)
            fail += 1
            continue
        med = statistics.median(timings)
        best = min(timings)
        print(f"{name:30} {med:12.1f} {best:12.1f}")

    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
