#include "functions/macros.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// Built-in SQL macros for common analytical idioms.
//
// Scalar:
//   pct_change(curr, prev)          (curr - prev) / prev
//   safe_div(a, b)                  a / nullif(b, 0)
//   clip(x, lo, hi)                 greatest(lo, least(hi, x))
//   zscore(x, mean, stddev)         (x - mean) / stddev
//   pct_to_bps(p)                   p * 10000
//   bps_to_pct(b)                   b / 10000.0
//
// Table:
//   resample_ohlc(query, interval)  bucket OHLCV bars to a coarser interval.
//                                    `query` must yield (ts, open, high, low,
//                                    close, volume).
//   with_returns(query)             append simple_return and log_return
//                                    columns to a (ts, price) series.
// ──────────────────────────────────────────────────────────────

static const char *kMacros[] = {
    // Scalar macros.
    "CREATE OR REPLACE MACRO pct_change(curr, prev) AS (curr - prev) / NULLIF(prev, 0)",
    "CREATE OR REPLACE MACRO safe_div(a, b) AS a / NULLIF(b, 0)",
    "CREATE OR REPLACE MACRO clip(x, lo, hi) AS greatest(lo, least(hi, x))",
    "CREATE OR REPLACE MACRO zscore(x, mean, stddev) AS (x - mean) / NULLIF(stddev, 0)",
    "CREATE OR REPLACE MACRO pct_to_bps(p) AS p * 10000",
    "CREATE OR REPLACE MACRO bps_to_pct(b) AS b / 10000.0",

    // Table macros.
    "CREATE OR REPLACE MACRO resample_ohlc(input_query, bucket) AS TABLE "
    "  SELECT time_bucket(bucket::INTERVAL, ts) AS ts, "
    "         arg_min(open, ts) AS open, "
    "         max(high) AS high, "
    "         min(low) AS low, "
    "         arg_max(close, ts) AS close, "
    "         sum(volume) AS volume "
    "  FROM query_table(input_query::VARCHAR) "
    "  GROUP BY 1 "
    "  ORDER BY 1",

    "CREATE OR REPLACE MACRO with_returns(input_query) AS TABLE "
    "  SELECT ts, price, "
    "         (price - lag(price) OVER (ORDER BY ts)) / "
    "             NULLIF(lag(price) OVER (ORDER BY ts), 0) AS simple_return, "
    "         ln(price / NULLIF(lag(price) OVER (ORDER BY ts), 0)) AS log_return "
    "  FROM query_table(input_query::VARCHAR) "
    "  ORDER BY ts",
};

void RegisterScroogeMacros(Connection &conn) {
	// We tolerate failures (e.g. greatest/least missing because
	// core_functions hasn't been loaded yet) so a static-link load of
	// scrooge before core_functions still works.
	for (auto sql : kMacros) {
		auto result = conn.Query(sql);
		(void)result; // ignored on purpose
	}
}

} // namespace scrooge
} // namespace duckdb
