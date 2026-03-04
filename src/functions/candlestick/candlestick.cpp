#include "functions/candlestick.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/helper.hpp"
#include <cmath>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// Candlestick Pattern Detection — scalar functions
//
// Each takes (open, high, low, close) and returns BOOLEAN.
// These detect single-bar patterns.
// ──────────────────────────────────────────────────────────────

// Doji: open ≈ close (body < 10% of range)
static void DojiFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &open_vec = args.data[0];
	auto &high_vec = args.data[1];
	auto &low_vec = args.data[2];
	auto &close_vec = args.data[3];
	idx_t count = args.size();

	UnifiedVectorFormat open_data, high_data, low_data, close_data;
	open_vec.ToUnifiedFormat(count, open_data);
	high_vec.ToUnifiedFormat(count, high_data);
	low_vec.ToUnifiedFormat(count, low_data);
	close_vec.ToUnifiedFormat(count, close_data);

	auto opens = UnifiedVectorFormat::GetData<double>(open_data);
	auto highs = UnifiedVectorFormat::GetData<double>(high_data);
	auto lows = UnifiedVectorFormat::GetData<double>(low_data);
	auto closes = UnifiedVectorFormat::GetData<double>(close_data);

	auto result_data = FlatVector::GetData<bool>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto oidx = open_data.sel->get_index(i);
		auto hidx = high_data.sel->get_index(i);
		auto lidx = low_data.sel->get_index(i);
		auto cidx = close_data.sel->get_index(i);

		if (!open_data.validity.RowIsValid(oidx) || !high_data.validity.RowIsValid(hidx) ||
		    !low_data.validity.RowIsValid(lidx) || !close_data.validity.RowIsValid(cidx)) {
			result_validity.SetInvalid(i);
			continue;
		}
		double o = opens[oidx], h = highs[hidx], l = lows[lidx], c = closes[cidx];
		double body = std::abs(c - o);
		double range = h - l;
		result_data[i] = (range > 0) && (body / range < 0.1);
	}
}

// Hammer: small body at top, long lower shadow (>=2x body), tiny upper shadow
static void HammerFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &open_vec = args.data[0];
	auto &high_vec = args.data[1];
	auto &low_vec = args.data[2];
	auto &close_vec = args.data[3];
	idx_t count = args.size();

	UnifiedVectorFormat open_data, high_data, low_data, close_data;
	open_vec.ToUnifiedFormat(count, open_data);
	high_vec.ToUnifiedFormat(count, high_data);
	low_vec.ToUnifiedFormat(count, low_data);
	close_vec.ToUnifiedFormat(count, close_data);

	auto opens = UnifiedVectorFormat::GetData<double>(open_data);
	auto highs = UnifiedVectorFormat::GetData<double>(high_data);
	auto lows = UnifiedVectorFormat::GetData<double>(low_data);
	auto closes = UnifiedVectorFormat::GetData<double>(close_data);

	auto result_data = FlatVector::GetData<bool>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto oidx = open_data.sel->get_index(i);
		auto hidx = high_data.sel->get_index(i);
		auto lidx = low_data.sel->get_index(i);
		auto cidx = close_data.sel->get_index(i);

		if (!open_data.validity.RowIsValid(oidx) || !high_data.validity.RowIsValid(hidx) ||
		    !low_data.validity.RowIsValid(lidx) || !close_data.validity.RowIsValid(cidx)) {
			result_validity.SetInvalid(i);
			continue;
		}
		double o = opens[oidx], h = highs[hidx], l = lows[lidx], c = closes[cidx];
		double body = std::abs(c - o);
		double body_top = std::max(o, c);
		double body_bottom = std::min(o, c);
		double upper_shadow = h - body_top;
		double lower_shadow = body_bottom - l;

		// Hammer: lower shadow >= 2x body, upper shadow <= 30% of body, body > 0
		result_data[i] = (body > 0) && (lower_shadow >= 2.0 * body) && (upper_shadow <= 0.3 * body);
	}
}

// Shooting Star: small body at bottom, long upper shadow (>=2x body), tiny lower shadow
static void ShootingStarFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &open_vec = args.data[0];
	auto &high_vec = args.data[1];
	auto &low_vec = args.data[2];
	auto &close_vec = args.data[3];
	idx_t count = args.size();

	UnifiedVectorFormat open_data, high_data, low_data, close_data;
	open_vec.ToUnifiedFormat(count, open_data);
	high_vec.ToUnifiedFormat(count, high_data);
	low_vec.ToUnifiedFormat(count, low_data);
	close_vec.ToUnifiedFormat(count, close_data);

	auto opens = UnifiedVectorFormat::GetData<double>(open_data);
	auto highs = UnifiedVectorFormat::GetData<double>(high_data);
	auto lows = UnifiedVectorFormat::GetData<double>(low_data);
	auto closes = UnifiedVectorFormat::GetData<double>(close_data);

	auto result_data = FlatVector::GetData<bool>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto oidx = open_data.sel->get_index(i);
		auto hidx = high_data.sel->get_index(i);
		auto lidx = low_data.sel->get_index(i);
		auto cidx = close_data.sel->get_index(i);

		if (!open_data.validity.RowIsValid(oidx) || !high_data.validity.RowIsValid(hidx) ||
		    !low_data.validity.RowIsValid(lidx) || !close_data.validity.RowIsValid(cidx)) {
			result_validity.SetInvalid(i);
			continue;
		}
		double o = opens[oidx], h = highs[hidx], l = lows[lidx], c = closes[cidx];
		double body = std::abs(c - o);
		double body_top = std::max(o, c);
		double body_bottom = std::min(o, c);
		double upper_shadow = h - body_top;
		double lower_shadow = body_bottom - l;

		result_data[i] = (body > 0) && (upper_shadow >= 2.0 * body) && (lower_shadow <= 0.3 * body);
	}
}

// Marubozu: full body, no/tiny shadows (both < 5% of body)
static void MarubozuFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &open_vec = args.data[0];
	auto &high_vec = args.data[1];
	auto &low_vec = args.data[2];
	auto &close_vec = args.data[3];
	idx_t count = args.size();

	UnifiedVectorFormat open_data, high_data, low_data, close_data;
	open_vec.ToUnifiedFormat(count, open_data);
	high_vec.ToUnifiedFormat(count, high_data);
	low_vec.ToUnifiedFormat(count, low_data);
	close_vec.ToUnifiedFormat(count, close_data);

	auto opens = UnifiedVectorFormat::GetData<double>(open_data);
	auto highs = UnifiedVectorFormat::GetData<double>(high_data);
	auto lows = UnifiedVectorFormat::GetData<double>(low_data);
	auto closes = UnifiedVectorFormat::GetData<double>(close_data);

	auto result_data = FlatVector::GetData<bool>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto oidx = open_data.sel->get_index(i);
		auto hidx = high_data.sel->get_index(i);
		auto lidx = low_data.sel->get_index(i);
		auto cidx = close_data.sel->get_index(i);

		if (!open_data.validity.RowIsValid(oidx) || !high_data.validity.RowIsValid(hidx) ||
		    !low_data.validity.RowIsValid(lidx) || !close_data.validity.RowIsValid(cidx)) {
			result_validity.SetInvalid(i);
			continue;
		}
		double o = opens[oidx], h = highs[hidx], l = lows[lidx], c = closes[cidx];
		double body = std::abs(c - o);
		double body_top = std::max(o, c);
		double body_bottom = std::min(o, c);
		double upper_shadow = h - body_top;
		double lower_shadow = body_bottom - l;

		result_data[i] = (body > 0) && (upper_shadow <= 0.05 * body) && (lower_shadow <= 0.05 * body);
	}
}

void RegisterCandlestickFunctions(Connection &conn, Catalog &catalog) {
	// is_doji(open, high, low, close) → BOOLEAN
	{
		ScalarFunctionSet func_set("is_doji");
		func_set.AddFunction(ScalarFunction(
		    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
		    LogicalType::BOOLEAN, DojiFunction));
		CreateScalarFunctionInfo info(func_set);
		catalog.CreateFunction(*conn.context, info);
	}
	// is_hammer(open, high, low, close) → BOOLEAN
	{
		ScalarFunctionSet func_set("is_hammer");
		func_set.AddFunction(ScalarFunction(
		    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
		    LogicalType::BOOLEAN, HammerFunction));
		CreateScalarFunctionInfo info(func_set);
		catalog.CreateFunction(*conn.context, info);
	}
	// is_shooting_star(open, high, low, close) → BOOLEAN
	{
		ScalarFunctionSet func_set("is_shooting_star");
		func_set.AddFunction(ScalarFunction(
		    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
		    LogicalType::BOOLEAN, ShootingStarFunction));
		CreateScalarFunctionInfo info(func_set);
		catalog.CreateFunction(*conn.context, info);
	}
	// is_marubozu(open, high, low, close) → BOOLEAN
	{
		ScalarFunctionSet func_set("is_marubozu");
		func_set.AddFunction(ScalarFunction(
		    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
		    LogicalType::BOOLEAN, MarubozuFunction));
		CreateScalarFunctionInfo info(func_set);
		catalog.CreateFunction(*conn.context, info);
	}
}

} // namespace scrooge
} // namespace duckdb
