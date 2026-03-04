#include "functions/portfolio.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/helper.hpp"
#include <cmath>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// composite_score(rsi, macd, bollinger_pct, obv_pct)
//
// Combines multiple indicator signals into a single score [-100, 100].
// Positive = bullish conviction, negative = bearish.
//
// Components (equally weighted):
//   RSI:     score = (50 - rsi) * -1  → maps 0-100 to bullish/bearish
//   MACD:    score = sign(macd) * min(|macd| * 10, 25)
//   BB%:     score = (bollinger_pct - 0.5) * 50
//   OBV%:    score = clamp(obv_pct * 25, -25, 25)
//
// bollinger_pct = (close - lower) / (upper - lower)  [0-1 range]
// obv_pct = OBV change as fraction (e.g., 0.05 = 5% increase)
// ──────────────────────────────────────────────────────────────

static void CompositeScoreFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &rsi_vec = args.data[0];
	auto &macd_vec = args.data[1];
	auto &bbpct_vec = args.data[2];
	auto &obvpct_vec = args.data[3];

	UnifiedVectorFormat rsi_data, macd_data, bb_data, obv_data;
	rsi_vec.ToUnifiedFormat(args.size(), rsi_data);
	macd_vec.ToUnifiedFormat(args.size(), macd_data);
	bbpct_vec.ToUnifiedFormat(args.size(), bb_data);
	obvpct_vec.ToUnifiedFormat(args.size(), obv_data);

	auto rsi_vals = UnifiedVectorFormat::GetData<double>(rsi_data);
	auto macd_vals = UnifiedVectorFormat::GetData<double>(macd_data);
	auto bb_vals = UnifiedVectorFormat::GetData<double>(bb_data);
	auto obv_vals = UnifiedVectorFormat::GetData<double>(obv_data);

	auto result_data = FlatVector::GetData<double>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < args.size(); i++) {
		auto ri = rsi_data.sel->get_index(i);
		auto mi = macd_data.sel->get_index(i);
		auto bi = bb_data.sel->get_index(i);
		auto oi = obv_data.sel->get_index(i);

		if (!rsi_data.validity.RowIsValid(ri) || !macd_data.validity.RowIsValid(mi) ||
		    !bb_data.validity.RowIsValid(bi) || !obv_data.validity.RowIsValid(oi)) {
			result_validity.SetInvalid(i);
			continue;
		}

		double rsi = rsi_vals[ri];
		double macd = macd_vals[mi];
		double bb_pct = bb_vals[bi];
		double obv_pct = obv_vals[oi];

		// RSI component: 50 is neutral, >50 is bearish pressure, <50 is bullish
		double rsi_score = (50.0 - rsi) * -1.0;  // maps to [-50, 50]
		rsi_score = std::max(-25.0, std::min(25.0, rsi_score));

		// MACD component: positive = bullish
		double macd_score = (macd > 0 ? 1.0 : -1.0) * std::min(std::abs(macd) * 10.0, 25.0);

		// Bollinger %B: 0 = at lower band (bullish), 1 = at upper (bearish), 0.5 = middle
		double bb_score = (bb_pct - 0.5) * 50.0;
		bb_score = std::max(-25.0, std::min(25.0, bb_score));

		// OBV momentum: positive change = bullish
		double obv_score = std::max(-25.0, std::min(25.0, obv_pct * 25.0));

		result_data[i] = rsi_score + macd_score + bb_score + obv_score;
	}
}

void RegisterCompositeFunctions(Connection &conn, Catalog &catalog) {
	ScalarFunctionSet comp_set("composite_score");
	comp_set.AddFunction(ScalarFunction(
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	    LogicalType::DOUBLE, CompositeScoreFunction));
	CreateScalarFunctionInfo comp_info(comp_set);
	catalog.CreateFunction(*conn.context, comp_info);
}

} // namespace scrooge
} // namespace duckdb
