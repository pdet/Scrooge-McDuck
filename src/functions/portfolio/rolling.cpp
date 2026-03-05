#include "functions/portfolio.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include <vector>
#include <algorithm>
#include <cmath>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// rolling_sharpe(returns, timestamp [, window_days])
// Rolling Sharpe ratio over a window (default 252 days).
//
// rolling_beta(asset_returns, benchmark_returns, timestamp [, window_days])
// Rolling beta over a window (default 252 days).
//
// win_rate(returns)
// Percentage of positive returns.
//
// avg_win_loss_ratio(returns)
// Average win / average loss.
// ──────────────────────────────────────────────────────────────

// --- win_rate: simple aggregate ---
struct WinRateState {
	int64_t wins;
	int64_t total;
};

static void WRInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &s = *reinterpret_cast<WinRateState *>(state_p);
	s.wins = s.total = 0;
}

static void WRUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat r_data, sdata;
	inputs[0].ToUnifiedFormat(count, r_data);
	state_vector.ToUnifiedFormat(count, sdata);
	auto vals = UnifiedVectorFormat::GetData<double>(r_data);
	auto states = (WinRateState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto ri = r_data.sel->get_index(i);
		auto si = sdata.sel->get_index(i);
		if (r_data.validity.RowIsValid(ri)) {
			states[si]->total++;
			if (vals[ri] > 0) states[si]->wins++;
		}
	}
}

static void WRCombine(Vector &src_vec, Vector &tgt_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	src_vec.ToUnifiedFormat(count, src_data);
	tgt_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (WinRateState **)src_data.data;
	auto targets = (WinRateState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[src_data.sel->get_index(i)];
		auto &tgt = *targets[tgt_data.sel->get_index(i)];
		tgt.wins += src.wins;
		tgt.total += src.total;
	}
}

static void WRFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (WinRateState **)sdata.data;
	auto result_data = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto &s = *states[sdata.sel->get_index(i)];
		auto ridx = i + offset;
		if (s.total == 0) { validity.SetInvalid(ridx); continue; }
		result_data[ridx] = (double)s.wins / (double)s.total;
	}
}

// --- avg_win_loss_ratio ---
struct WLState {
	double sum_wins;
	double sum_losses;
	int64_t n_wins;
	int64_t n_losses;
};

static void WLInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &s = *reinterpret_cast<WLState *>(state_p);
	s.sum_wins = s.sum_losses = 0;
	s.n_wins = s.n_losses = 0;
}

static void WLUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat r_data, sdata;
	inputs[0].ToUnifiedFormat(count, r_data);
	state_vector.ToUnifiedFormat(count, sdata);
	auto vals = UnifiedVectorFormat::GetData<double>(r_data);
	auto states = (WLState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto ri = r_data.sel->get_index(i);
		auto si = sdata.sel->get_index(i);
		if (r_data.validity.RowIsValid(ri)) {
			double v = vals[ri];
			if (v > 0) { states[si]->sum_wins += v; states[si]->n_wins++; }
			else if (v < 0) { states[si]->sum_losses += (-v); states[si]->n_losses++; }
		}
	}
}

static void WLCombine(Vector &src_vec, Vector &tgt_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	src_vec.ToUnifiedFormat(count, src_data);
	tgt_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (WLState **)src_data.data;
	auto targets = (WLState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[src_data.sel->get_index(i)];
		auto &tgt = *targets[tgt_data.sel->get_index(i)];
		tgt.sum_wins += src.sum_wins;
		tgt.sum_losses += src.sum_losses;
		tgt.n_wins += src.n_wins;
		tgt.n_losses += src.n_losses;
	}
}

static void WLFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (WLState **)sdata.data;
	auto result_data = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto &s = *states[sdata.sel->get_index(i)];
		auto ridx = i + offset;
		if (s.n_wins == 0 || s.n_losses == 0) { validity.SetInvalid(ridx); continue; }
		double avg_win = s.sum_wins / s.n_wins;
		double avg_loss = s.sum_losses / s.n_losses;
		result_data[ridx] = avg_win / avg_loss;
	}
}

// --- expectancy: (win_rate * avg_win) - (loss_rate * avg_loss) ---
static void ExpFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (WLState **)sdata.data;
	auto result_data = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto &s = *states[sdata.sel->get_index(i)];
		auto ridx = i + offset;
		int64_t total = s.n_wins + s.n_losses;
		if (total == 0) { validity.SetInvalid(ridx); continue; }
		double wr = (double)s.n_wins / total;
		double avg_win = (s.n_wins > 0) ? s.sum_wins / s.n_wins : 0;
		double avg_loss = (s.n_losses > 0) ? s.sum_losses / s.n_losses : 0;
		result_data[ridx] = (wr * avg_win) - ((1.0 - wr) * avg_loss);
	}
}

void RegisterRollingFunctions(Connection &conn, Catalog &catalog) {
	// win_rate(returns)
	AggregateFunctionSet wr_set("win_rate");
	wr_set.AddFunction(AggregateFunction(
	    "win_rate", {LogicalType::DOUBLE},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<WinRateState>,
	    WRInitialize, WRUpdate, WRCombine, WRFinalize, nullptr, nullptr, nullptr));
	CreateAggregateFunctionInfo wr_info(wr_set);
	catalog.CreateFunction(*conn.context, wr_info);

	// avg_win_loss_ratio(returns)
	AggregateFunctionSet wl_set("avg_win_loss_ratio");
	wl_set.AddFunction(AggregateFunction(
	    "avg_win_loss_ratio", {LogicalType::DOUBLE},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<WLState>,
	    WLInitialize, WLUpdate, WLCombine, WLFinalize, nullptr, nullptr, nullptr));
	CreateAggregateFunctionInfo wl_info(wl_set);
	catalog.CreateFunction(*conn.context, wl_info);

	// expectancy(returns)
	AggregateFunctionSet exp_set("expectancy");
	exp_set.AddFunction(AggregateFunction(
	    "expectancy", {LogicalType::DOUBLE},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<WLState>,
	    WLInitialize, WLUpdate, WLCombine, ExpFinalize, nullptr, nullptr, nullptr));
	CreateAggregateFunctionInfo exp_info(exp_set);
	catalog.CreateFunction(*conn.context, exp_info);
}

} // namespace scrooge
} // namespace duckdb
