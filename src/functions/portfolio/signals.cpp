#include "functions/portfolio.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/helper.hpp"
#include <cmath>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// ma_crossover_signal(fast_ma, slow_ma)
//
// Returns 'BUY' when fast > slow, 'SELL' when fast < slow,
// 'HOLD' when equal. Simple but effective trend signal.
// ──────────────────────────────────────────────────────────────

static void MaCrossoverFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &fast = args.data[0];
	auto &slow = args.data[1];

	BinaryExecutor::Execute<double, double, string_t>(
	    fast, slow, result, args.size(),
	    [&](double f, double s) -> string_t {
		    if (f > s) return StringVector::AddString(result, "BUY");
		    if (f < s) return StringVector::AddString(result, "SELL");
		    return StringVector::AddString(result, "HOLD");
	    });
}

// ──────────────────────────────────────────────────────────────
// rsi_signal(rsi_value [, overbought, oversold])
//
// Returns 'SELL' when RSI > overbought (default 70),
// 'BUY' when RSI < oversold (default 30), 'HOLD' otherwise.
// ──────────────────────────────────────────────────────────────

static void RsiSignal2Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &rsi = args.data[0];

	UnaryExecutor::Execute<double, string_t>(
	    rsi, result, args.size(),
	    [&](double r) -> string_t {
		    if (r >= 70.0) return StringVector::AddString(result, "SELL");
		    if (r <= 30.0) return StringVector::AddString(result, "BUY");
		    return StringVector::AddString(result, "HOLD");
	    });
}

static void RsiSignal4Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &rsi = args.data[0];
	auto &overbought = args.data[1];
	auto &oversold = args.data[2];

	TernaryExecutor::Execute<double, double, double, string_t>(
	    rsi, overbought, oversold, result, args.size(),
	    [&](double r, double ob, double os) -> string_t {
		    if (r >= ob) return StringVector::AddString(result, "SELL");
		    if (r <= os) return StringVector::AddString(result, "BUY");
		    return StringVector::AddString(result, "HOLD");
	    });
}

// ──────────────────────────────────────────────────────────────
// bollinger_signal(close, upper, lower)
//
// Returns 'SELL' when close > upper band,
// 'BUY' when close < lower band, 'HOLD' otherwise.
// ──────────────────────────────────────────────────────────────

static void BollingerSignalFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &close = args.data[0];
	auto &upper = args.data[1];
	auto &lower = args.data[2];

	TernaryExecutor::Execute<double, double, double, string_t>(
	    close, upper, lower, result, args.size(),
	    [&](double c, double u, double l) -> string_t {
		    if (c > u) return StringVector::AddString(result, "SELL");
		    if (c < l) return StringVector::AddString(result, "BUY");
		    return StringVector::AddString(result, "HOLD");
	    });
}

// ──────────────────────────────────────────────────────────────
// kelly_fraction(win_rate, avg_win, avg_loss)
//
// Kelly Criterion: f* = (p * b - q) / b
// where p = win_rate, q = 1-p, b = avg_win/avg_loss
// Returns optimal fraction of capital to risk.
// ──────────────────────────────────────────────────────────────

static void KellyFractionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &win_rate = args.data[0];
	auto &avg_win = args.data[1];
	auto &avg_loss = args.data[2];

	TernaryExecutor::Execute<double, double, double, double>(
	    win_rate, avg_win, avg_loss, result, args.size(),
	    [&](double p, double w, double l) -> double {
		    if (l == 0.0 || p < 0.0 || p > 1.0) return 0.0;
		    double b = w / l;
		    double q = 1.0 - p;
		    double f = (p * b - q) / b;
		    // Clamp to [0, 1] — never recommend shorting or >100%
		    return std::max(0.0, std::min(1.0, f));
	    });
}

void RegisterSignalFunctions(Connection &conn, Catalog &catalog) {
	// ma_crossover_signal(fast_ma, slow_ma)
	ScalarFunctionSet ma_set("ma_crossover_signal");
	ma_set.AddFunction(ScalarFunction(
	    {LogicalType::DOUBLE, LogicalType::DOUBLE},
	    LogicalType::VARCHAR, MaCrossoverFunction));
	CreateScalarFunctionInfo ma_info(ma_set);
	catalog.CreateFunction(*conn.context, ma_info);

	// rsi_signal(rsi_value) — defaults 70/30
	// rsi_signal(rsi_value, overbought, oversold)
	ScalarFunctionSet rsi_sig_set("rsi_signal");
	rsi_sig_set.AddFunction(ScalarFunction(
	    {LogicalType::DOUBLE},
	    LogicalType::VARCHAR, RsiSignal2Function));
	rsi_sig_set.AddFunction(ScalarFunction(
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	    LogicalType::VARCHAR, RsiSignal4Function));
	CreateScalarFunctionInfo rsi_sig_info(rsi_sig_set);
	catalog.CreateFunction(*conn.context, rsi_sig_info);

	// bollinger_signal(close, upper, lower)
	ScalarFunctionSet boll_set("bollinger_signal");
	boll_set.AddFunction(ScalarFunction(
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	    LogicalType::VARCHAR, BollingerSignalFunction));
	CreateScalarFunctionInfo boll_info(boll_set);
	catalog.CreateFunction(*conn.context, boll_info);

	// kelly_fraction(win_rate, avg_win, avg_loss)
	ScalarFunctionSet kelly_set("kelly_fraction");
	kelly_set.AddFunction(ScalarFunction(
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	    LogicalType::DOUBLE, KellyFractionFunction));
	CreateScalarFunctionInfo kelly_info(kelly_set);
	catalog.CreateFunction(*conn.context, kelly_info);
}

} // namespace scrooge
} // namespace duckdb
