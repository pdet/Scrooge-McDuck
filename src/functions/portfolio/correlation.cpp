#include "functions/portfolio.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/helper.hpp"
#include <cmath>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// portfolio_correlation(returns_a, returns_b)
//
// Pearson correlation coefficient between two return series.
// No timestamp needed — assumes rows are aligned.
// Returns value between -1 and 1.
// ──────────────────────────────────────────────────────────────

struct CorrState {
	double sum_a;
	double sum_b;
	double sum_a2;
	double sum_b2;
	double sum_ab;
	uint64_t count;
};

static void CorrInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<CorrState *>(state_p);
	state.sum_a = 0;
	state.sum_b = 0;
	state.sum_a2 = 0;
	state.sum_b2 = 0;
	state.sum_ab = 0;
	state.count = 0;
}

static void CorrUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat a_data, b_data, sdata;
	inputs[0].ToUnifiedFormat(count, a_data);
	inputs[1].ToUnifiedFormat(count, b_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto a_vals = UnifiedVectorFormat::GetData<double>(a_data);
	auto b_vals = UnifiedVectorFormat::GetData<double>(b_data);
	auto states = (CorrState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto ai = a_data.sel->get_index(i);
		auto bi = b_data.sel->get_index(i);
		auto si = sdata.sel->get_index(i);
		if (a_data.validity.RowIsValid(ai) && b_data.validity.RowIsValid(bi)) {
			auto &state = *states[si];
			double a = a_vals[ai];
			double b = b_vals[bi];
			state.sum_a += a;
			state.sum_b += b;
			state.sum_a2 += a * a;
			state.sum_b2 += b * b;
			state.sum_ab += a * b;
			state.count++;
		}
	}
}

static void CorrCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (CorrState **)src_data.data;
	auto targets = (CorrState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[src_data.sel->get_index(i)];
		auto &tgt = *targets[tgt_data.sel->get_index(i)];
		tgt.sum_a += src.sum_a;
		tgt.sum_b += src.sum_b;
		tgt.sum_a2 += src.sum_a2;
		tgt.sum_b2 += src.sum_b2;
		tgt.sum_ab += src.sum_ab;
		tgt.count += src.count;
	}
}

static void CorrFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (CorrState **)sdata.data;
	auto result_data = FlatVector::GetData<double>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		auto ridx = i + offset;
		if (state.count < 2) {
			result_validity.SetInvalid(ridx);
			continue;
		}
		double n = (double)state.count;
		double num = n * state.sum_ab - state.sum_a * state.sum_b;
		double den_a = n * state.sum_a2 - state.sum_a * state.sum_a;
		double den_b = n * state.sum_b2 - state.sum_b * state.sum_b;
		double den = std::sqrt(den_a * den_b);
		if (den == 0.0) {
			result_data[ridx] = 0.0;
		} else {
			result_data[ridx] = num / den;
		}
	}
}

// ──────────────────────────────────────────────────────────────
// portfolio_beta(asset_returns, benchmark_returns)
//
// Beta = Cov(asset, benchmark) / Var(benchmark)
// Measures systematic risk relative to a benchmark.
// ──────────────────────────────────────────────────────────────

static void BetaFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (CorrState **)sdata.data;
	auto result_data = FlatVector::GetData<double>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		auto ridx = i + offset;
		if (state.count < 2) {
			result_validity.SetInvalid(ridx);
			continue;
		}
		double n = (double)state.count;
		// Cov(a, b) = E[ab] - E[a]*E[b] = (sum_ab/n) - (sum_a/n)*(sum_b/n)
		double cov = (state.sum_ab / n) - (state.sum_a / n) * (state.sum_b / n);
		// Var(b) = E[b^2] - E[b]^2
		double var_b = (state.sum_b2 / n) - (state.sum_b / n) * (state.sum_b / n);
		if (var_b == 0.0) {
			result_validity.SetInvalid(ridx);
		} else {
			result_data[ridx] = cov / var_b;
		}
	}
}

void RegisterCorrelationFunctions(Connection &conn, Catalog &catalog) {
	// portfolio_correlation(returns_a, returns_b)
	AggregateFunction corr_func(
	    "portfolio_correlation",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE},
	    LogicalType::DOUBLE,
	    AggregateFunction::StateSize<CorrState>,
	    CorrInitialize, CorrUpdate, CorrCombine, CorrFinalize);
	CreateAggregateFunctionInfo corr_info(corr_func);
	catalog.CreateFunction(*conn.context, corr_info);

	// portfolio_beta(asset_returns, benchmark_returns)
	AggregateFunction beta_func(
	    "portfolio_beta",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE},
	    LogicalType::DOUBLE,
	    AggregateFunction::StateSize<CorrState>,
	    CorrInitialize, CorrUpdate, CorrCombine, BetaFinalize);
	CreateAggregateFunctionInfo beta_info(beta_func);
	catalog.CreateFunction(*conn.context, beta_info);
}

} // namespace scrooge
} // namespace duckdb
