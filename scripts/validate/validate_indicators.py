#!/usr/bin/env python3
"""
Validate Scrooge technical indicators against independent reference
implementations (pandas-only, no third-party TA libraries required).

Usage:
    python3 scripts/validate/validate_indicators.py [--duckdb path/to/duckdb]

Exit code is 0 if every indicator matches its reference within tolerance,
1 otherwise. Prints a one-line summary per indicator.

Run after building the extension; the script invokes the local DuckDB
binary with the loadable scrooge extension. By default it expects:
  ./build/release/duckdb
  ./build/release/extension/scrooge/scrooge.duckdb_extension
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Tuple


# ---------------------------------------------------------------------------
# Reference implementations.
# These are intentionally simple, vectorised pandas equivalents so the
# comparison is pure-Python and easy to audit. We avoid pandas-ta /
# TA-Lib so the validator stays installable in CI without extra deps.
# ---------------------------------------------------------------------------

try:
    import pandas as pd  # noqa: F401
except ImportError:
    print("pandas required; pip install pandas", file=sys.stderr)
    sys.exit(2)


def ref_ema(series: "pd.Series", period: int) -> float:
    alpha = 2.0 / (period + 1)
    ema = series.iloc[0]
    for v in series.iloc[1:]:
        ema = alpha * v + (1 - alpha) * ema
    return float(ema)


def ref_sma(series: "pd.Series", period: int) -> float:
    return float(series.tail(period).mean())


def ref_rsi(series: "pd.Series", period: int) -> float:
    diff = series.diff().dropna()
    gain = diff.clip(lower=0)
    loss = -diff.clip(upper=0)
    avg_g = gain.iloc[:period].mean()
    avg_l = loss.iloc[:period].mean()
    for i in range(period, len(diff)):
        avg_g = (avg_g * (period - 1) + gain.iloc[i]) / period
        avg_l = (avg_l * (period - 1) + loss.iloc[i]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return float(100 - 100 / (1 + rs))


def ref_williams_r(highs, lows, closes, period: int) -> float:
    hh = highs.tail(period).max()
    ll = lows.tail(period).min()
    return float(-100.0 * (hh - closes.iloc[-1]) / (hh - ll))


def ref_bond_price(face: float, c: float, y: float, n: int, freq: float) -> float:
    cpn = face * c / freq
    yp = y / freq
    return sum(cpn / (1 + yp) ** t for t in range(1, n + 1)) + face / (1 + yp) ** n


# ---------------------------------------------------------------------------
# DuckDB driver.
# ---------------------------------------------------------------------------


@dataclass
class DuckDB:
    binary: Path
    extension: Path

    def query(self, sql: str) -> List[Tuple]:
        # Multi-line script: shell metacommands on their own lines, SQL last.
        script = "\n".join([
            ".bail on",
            "SET autoload_known_extensions=1;",
            "SET autoinstall_known_extensions=1;",
            f"LOAD '{self.extension.resolve()}';",
            ".mode csv",
            ".headers off",
            f"{sql};",
        ])
        out = subprocess.run(
            [str(self.binary.resolve()), "-unsigned"],
            input=script, capture_output=True, text=True, check=False,
        )
        if out.returncode != 0:
            raise RuntimeError(f"duckdb failed: {out.stderr.strip()}")
        rows: List[Tuple] = []
        for line in out.stdout.splitlines():
            if not line.strip():
                continue
            rows.append(tuple(line.split(",")))
        return rows


# ---------------------------------------------------------------------------
# Test cases.
# ---------------------------------------------------------------------------


def make_series(n: int = 60, seed: int = 42):
    import pandas as pd
    rng = [(seed * (i + 1)) % 1000 for i in range(n)]
    closes = pd.Series([100 + (r - 500) / 50 + i * 0.05 for i, r in enumerate(rng)])
    highs = closes + 0.5
    lows = closes - 0.5
    return highs, lows, closes


@dataclass
class Case:
    name: str
    sql: Callable[[List[float]], str]
    reference: Callable[..., float]
    tol: float = 1e-6


def main() -> int:
    parser = argparse.ArgumentParser()
    here = Path(__file__).resolve().parents[2]
    parser.add_argument("--duckdb", default=here / "build" / "release" / "duckdb")
    parser.add_argument("--extension", default=here / "build" / "release" / "extension" / "scrooge" / "scrooge.duckdb_extension")
    args = parser.parse_args()

    db = DuckDB(Path(args.duckdb), Path(args.extension))
    if not db.binary.exists():
        print(f"missing {db.binary}: build the project first", file=sys.stderr)
        return 2
    if not db.extension.exists():
        print(f"missing {db.extension}: build the project first", file=sys.stderr)
        return 2

    highs, lows, closes = make_series()

    def values_clause(values, ts_offset_days=True):
        rows = []
        for i, v in enumerate(values):
            if ts_offset_days:
                rows.append(f"('2024-01-01'::TIMESTAMP + INTERVAL {i} DAY, {v})")
            else:
                rows.append(f"({v})")
        return ", ".join(rows)

    failures = 0
    total = 0

    # EMA(20) over closes.
    period = 20
    sql = (
        "WITH t(ts, c) AS (VALUES " + values_clause(list(closes)) + ") "
        f"SELECT ema(c, ts, {period}) FROM t"
    )
    expected = ref_ema(closes, period)
    actual = float(db.query(sql)[0][0])
    diff = abs(actual - expected)
    ok = diff < 1e-3
    print(f"{'OK ' if ok else 'FAIL'} ema(period={period})  duckdb={actual:.6f} ref={expected:.6f} diff={diff:.2e}")
    failures += 0 if ok else 1
    total += 1

    # RSI(14) over closes.
    period = 14
    sql = (
        "WITH t(ts, c) AS (VALUES " + values_clause(list(closes)) + ") "
        f"SELECT rsi(c, ts, {period}) FROM t"
    )
    expected = ref_rsi(closes, period)
    actual = float(db.query(sql)[0][0])
    diff = abs(actual - expected)
    ok = diff < 1e-3
    print(f"{'OK ' if ok else 'FAIL'} rsi(period={period})  duckdb={actual:.6f} ref={expected:.6f} diff={diff:.2e}")
    failures += 0 if ok else 1
    total += 1

    # Williams %R over closes/highs/lows (period 14).
    period = 14
    rows = []
    for i in range(len(closes)):
        rows.append(f"('2024-01-01'::TIMESTAMP + INTERVAL {i} DAY, {highs.iloc[i]}, {lows.iloc[i]}, {closes.iloc[i]})")
    sql = (
        "WITH t(ts, h, l, c) AS (VALUES " + ", ".join(rows) + ") "
        f"SELECT williams_r(h, l, c, ts, {period}) FROM t"
    )
    expected = ref_williams_r(highs, lows, closes, period)
    actual = float(db.query(sql)[0][0])
    diff = abs(actual - expected)
    ok = diff < 1e-3
    print(f"{'OK ' if ok else 'FAIL'} williams_r(period={period})  duckdb={actual:.6f} ref={expected:.6f} diff={diff:.2e}")
    failures += 0 if ok else 1
    total += 1

    # Bond price round-trip.
    face, c, y, n, freq = 1000.0, 0.05, 0.04, 20, 2.0
    sql = f"SELECT bond_price({face}, {c}, {y}, {n}, {freq})"
    expected = ref_bond_price(face, c, y, n, freq)
    actual = float(db.query(sql)[0][0])
    diff = abs(actual - expected)
    ok = diff < 1e-3
    print(f"{'OK ' if ok else 'FAIL'} bond_price             duckdb={actual:.6f} ref={expected:.6f} diff={diff:.2e}")
    failures += 0 if ok else 1
    total += 1

    print(f"\n{total - failures}/{total} validations passed")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
