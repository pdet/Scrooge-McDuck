#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/types.hpp"
#include <cmath>
#include <string>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// Candlestick Pattern Detection — Pure Scalar Functions
//
// These operate on OHLC data to detect common candlestick patterns.
// Designed to be used with LAG() window functions for multi-candle patterns.
// ──────────────────────────────────────────────────────────────

// Helper: body size and direction
static inline double CandleBody(double open, double close) {
	return std::abs(close - open);
}
static inline double CandleRange(double high, double low) {
	return high - low;
}
static inline double UpperWick(double open, double high, double close) {
	return high - std::max(open, close);
}
static inline double LowerWick(double open, double low, double close) {
	return std::min(open, close) - low;
}
static inline bool IsBullish(double open, double close) {
	return close > open;
}

// ── is_doji(open, high, low, close [, threshold]) ──
// Body is very small relative to range
static void IsDoji(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	auto &open_vec = args.data[0];
	auto &high_vec = args.data[1];
	auto &low_vec = args.data[2];
	auto &close_vec = args.data[3];

	double threshold = 0.05; // default: body < 5% of range
	if (args.ColumnCount() >= 5) {
		// Constant threshold from 5th argument
		auto &thresh_vec = args.data[4];
		if (thresh_vec.GetVectorType() == VectorType::CONSTANT_VECTOR && ConstantVector::IsNull(thresh_vec) == false) {
			threshold = *ConstantVector::GetData<double>(thresh_vec);
		}
	}

	BinaryExecutor::Execute<double, double, bool>(open_vec, close_vec, result, count,
	                                               [&](double o, double c) {
		// Need high and low too — fall back to generic
		return false;
	});

	// Use generic executor for 4 inputs
	auto opens = FlatVector::GetData<double>(open_vec);
	auto highs = FlatVector::GetData<double>(high_vec);
	auto lows = FlatVector::GetData<double>(low_vec);
	auto closes = FlatVector::GetData<double>(close_vec);
	auto result_data = FlatVector::GetData<bool>(result);

	for (idx_t i = 0; i < count; i++) {
		double range = CandleRange(highs[i], lows[i]);
		if (range == 0.0) {
			result_data[i] = true; // Flat line is a doji
		} else {
			result_data[i] = (CandleBody(opens[i], closes[i]) / range) < threshold;
		}
	}
}

// ── is_hammer(open, high, low, close) ──
// Small body at top, long lower wick (≥2x body), tiny upper wick
static void IsHammer(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	auto opens = FlatVector::GetData<double>(args.data[0]);
	auto highs = FlatVector::GetData<double>(args.data[1]);
	auto lows = FlatVector::GetData<double>(args.data[2]);
	auto closes = FlatVector::GetData<double>(args.data[3]);
	auto result_data = FlatVector::GetData<bool>(result);

	for (idx_t i = 0; i < count; i++) {
		double body = CandleBody(opens[i], closes[i]);
		double lower = LowerWick(opens[i], lows[i], closes[i]);
		double upper = UpperWick(opens[i], highs[i], closes[i]);
		double range = CandleRange(highs[i], lows[i]);

		// Hammer: lower wick >= 2x body, upper wick <= 10% of range, body > 0
		result_data[i] = (range > 0 && body > 0 && lower >= 2.0 * body && upper <= 0.1 * range);
	}
}

// ── is_shooting_star(open, high, low, close) ──
// Inverse hammer: long upper wick, small body at bottom
static void IsShootingStar(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	auto opens = FlatVector::GetData<double>(args.data[0]);
	auto highs = FlatVector::GetData<double>(args.data[1]);
	auto lows = FlatVector::GetData<double>(args.data[2]);
	auto closes = FlatVector::GetData<double>(args.data[3]);
	auto result_data = FlatVector::GetData<bool>(result);

	for (idx_t i = 0; i < count; i++) {
		double body = CandleBody(opens[i], closes[i]);
		double upper = UpperWick(opens[i], highs[i], closes[i]);
		double lower = LowerWick(opens[i], lows[i], closes[i]);
		double range = CandleRange(highs[i], lows[i]);

		result_data[i] = (range > 0 && body > 0 && upper >= 2.0 * body && lower <= 0.1 * range);
	}
}

// ── is_engulfing(open, high, low, close, prev_open, prev_close) ──
// Returns VARCHAR: 'BULLISH', 'BEARISH', or NULL
static void IsEngulfing(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	auto opens = FlatVector::GetData<double>(args.data[0]);
	auto closes = FlatVector::GetData<double>(args.data[3]);
	auto prev_opens = FlatVector::GetData<double>(args.data[4]);
	auto prev_closes = FlatVector::GetData<double>(args.data[5]);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		double o = opens[i], c = closes[i];
		double po = prev_opens[i], pc = prev_closes[i];

		// Bullish engulfing: prev bearish, current bullish, current body engulfs prev body
		if (pc < po && c > o && o <= pc && c >= po) {
			result.SetValue(i, Value("BULLISH"));
		}
		// Bearish engulfing: prev bullish, current bearish, current body engulfs prev body
		else if (pc > po && c < o && o >= pc && c <= po) {
			result.SetValue(i, Value("BEARISH"));
		} else {
			result_validity.SetInvalid(i);
		}
	}
}

// ── is_morning_star(o1, c1, o2, c2, o3, c3) ──
// Three-candle bullish reversal
static void IsMorningStar(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	auto o1s = FlatVector::GetData<double>(args.data[0]);
	auto c1s = FlatVector::GetData<double>(args.data[1]);
	auto o2s = FlatVector::GetData<double>(args.data[2]);
	auto c2s = FlatVector::GetData<double>(args.data[3]);
	auto o3s = FlatVector::GetData<double>(args.data[4]);
	auto c3s = FlatVector::GetData<double>(args.data[5]);
	auto result_data = FlatVector::GetData<bool>(result);

	for (idx_t i = 0; i < count; i++) {
		double o1 = o1s[i], c1 = c1s[i]; // First candle (bearish)
		double o2 = o2s[i], c2 = c2s[i]; // Second candle (small body, gap down)
		double o3 = o3s[i], c3 = c3s[i]; // Third candle (bullish)

		double body1 = CandleBody(o1, c1);
		double body2 = CandleBody(o2, c2);
		double body3 = CandleBody(o3, c3);

		// Day 1: bearish with significant body
		// Day 2: small body (star), gaps below day 1 close
		// Day 3: bullish, closes above midpoint of day 1
		bool day1_bearish = c1 < o1 && body1 > 0;
		bool day2_small = body2 < body1 * 0.5;
		bool day2_gap = std::max(o2, c2) < c1;
		bool day3_bullish = c3 > o3 && body3 > 0;
		bool day3_recovery = c3 > (o1 + c1) / 2.0;

		result_data[i] = day1_bearish && day2_small && day2_gap && day3_bullish && day3_recovery;
	}
}

// ── is_evening_star(o1, c1, o2, c2, o3, c3) ──
// Three-candle bearish reversal (mirror of morning star)
static void IsEveningStar(DataChunk &args, ExpressionState &, Vector &result) {
	auto count = args.size();
	auto o1s = FlatVector::GetData<double>(args.data[0]);
	auto c1s = FlatVector::GetData<double>(args.data[1]);
	auto o2s = FlatVector::GetData<double>(args.data[2]);
	auto c2s = FlatVector::GetData<double>(args.data[3]);
	auto o3s = FlatVector::GetData<double>(args.data[4]);
	auto c3s = FlatVector::GetData<double>(args.data[5]);
	auto result_data = FlatVector::GetData<bool>(result);

	for (idx_t i = 0; i < count; i++) {
		double o1 = o1s[i], c1 = c1s[i];
		double o2 = o2s[i], c2 = c2s[i];
		double o3 = o3s[i], c3 = c3s[i];

		double body1 = CandleBody(o1, c1);
		double body2 = CandleBody(o2, c2);
		double body3 = CandleBody(o3, c3);

		bool day1_bullish = c1 > o1 && body1 > 0;
		bool day2_small = body2 < body1 * 0.5;
		bool day2_gap = std::min(o2, c2) > c1;
		bool day3_bearish = c3 < o3 && body3 > 0;
		bool day3_decline = c3 < (o1 + c1) / 2.0;

		result_data[i] = day1_bullish && day2_small && day2_gap && day3_bearish && day3_decline;
	}
}

// ── Registration ──

void RegisterCandlestickFunctions(Connection &conn, Catalog &catalog) {
	// is_doji(open, high, low, close) and is_doji(open, high, low, close, threshold)
	{
		ScalarFunctionSet doji_set("is_doji");
		doji_set.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
		                                      LogicalType::DOUBLE},
		                                     LogicalType::BOOLEAN, IsDoji));
		doji_set.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
		                                      LogicalType::DOUBLE, LogicalType::DOUBLE},
		                                     LogicalType::BOOLEAN, IsDoji));
		CreateScalarFunctionInfo info(doji_set);
		catalog.CreateFunction(*conn.context, info);
	}

	// is_hammer(open, high, low, close)
	{
		ScalarFunction func("is_hammer",
		                     {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
		                     LogicalType::BOOLEAN, IsHammer);
		CreateScalarFunctionInfo info(func);
		catalog.CreateFunction(*conn.context, info);
	}

	// is_shooting_star(open, high, low, close)
	{
		ScalarFunction func("is_shooting_star",
		                     {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
		                     LogicalType::BOOLEAN, IsShootingStar);
		CreateScalarFunctionInfo info(func);
		catalog.CreateFunction(*conn.context, info);
	}

	// is_engulfing(open, high, low, close, prev_open, prev_close)
	{
		ScalarFunction func("is_engulfing",
		                     {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
		                      LogicalType::DOUBLE, LogicalType::DOUBLE},
		                     LogicalType::VARCHAR, IsEngulfing);
		CreateScalarFunctionInfo info(func);
		catalog.CreateFunction(*conn.context, info);
	}

	// is_morning_star(o1, c1, o2, c2, o3, c3)
	{
		ScalarFunction func("is_morning_star",
		                     {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
		                      LogicalType::DOUBLE, LogicalType::DOUBLE},
		                     LogicalType::BOOLEAN, IsMorningStar);
		CreateScalarFunctionInfo info(func);
		catalog.CreateFunction(*conn.context, info);
	}

	// is_evening_star(o1, c1, o2, c2, o3, c3)
	{
		ScalarFunction func("is_evening_star",
		                     {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
		                      LogicalType::DOUBLE, LogicalType::DOUBLE},
		                     LogicalType::BOOLEAN, IsEveningStar);
		CreateScalarFunctionInfo info(func);
		catalog.CreateFunction(*conn.context, info);
	}
}

} // namespace scrooge
} // namespace duckdb
