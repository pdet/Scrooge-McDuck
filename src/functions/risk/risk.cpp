#include "functions/risk.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include <cmath>
#include <algorithm>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// sharpe_ratio(returns, risk_free_rate) — simple aggregate
//
//   (mean(returns) - risk_free_rate) / stddev(returns)
//
// Collects all return values; risk_free_rate is taken from the
// first row (constant across the group).
// ──────────────────────────────────────────────────────────────

struct SharpeState {
	std::vector<double> *returns;
	double risk_free_rate;
	bool rfr_set;
};

static void SharpeInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<SharpeState *>(state_p);
	state.returns = nullptr;
	state.risk_free_rate = 0.0;
	state.rfr_set = false;
}

static void SharpeUpdate(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector, idx_t count) {
	auto &return_vec = inputs[0];
	auto &rfr_vec = inputs[1];

	UnifiedVectorFormat return_data, rfr_data, sdata;
	return_vec.ToUnifiedFormat(count, return_data);
	rfr_vec.ToUnifiedFormat(count, rfr_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto returns = UnifiedVectorFormat::GetData<double>(return_data);
	auto rfrs = UnifiedVectorFormat::GetData<double>(rfr_data);
	auto states = (SharpeState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];

		if (!state.returns) {
			state.returns = new std::vector<double>();
		}

		auto vidx = return_data.sel->get_index(i);
		auto ridx = rfr_data.sel->get_index(i);

		if (!state.rfr_set && rfr_data.validity.RowIsValid(ridx)) {
			state.risk_free_rate = rfrs[ridx];
			state.rfr_set = true;
		}

		if (return_data.validity.RowIsValid(vidx)) {
			state.returns->push_back(returns[vidx]);
		}
	}
}

static void SharpeCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (SharpeState **)src_data.data;
	auto targets = (SharpeState **)tgt_data.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = src_data.sel->get_index(i);
		auto tidx = tgt_data.sel->get_index(i);
		auto &source = *sources[sidx];
		auto &target = *targets[tidx];

		if (source.returns) {
			if (!target.returns) {
				target.returns = new std::vector<double>();
			}
			target.returns->insert(target.returns->end(), source.returns->begin(), source.returns->end());
			delete source.returns;
			source.returns = nullptr;
		}
		if (source.rfr_set && !target.rfr_set) {
			target.risk_free_rate = source.risk_free_rate;
			target.rfr_set = true;
		}
	}
}

static void SharpeFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (SharpeState **)sdata.data;

	auto result_data = FlatVector::GetData<double>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		auto ridx = i + offset;

		if (!state.returns || state.returns->size() < 2) {
			result_validity.SetInvalid(ridx);
			if (state.returns) {
				delete state.returns;
				state.returns = nullptr;
			}
			continue;
		}

		auto &rets = *state.returns;
		idx_t n = rets.size();

		// Mean
		double sum = 0.0;
		for (auto &r : rets) {
			sum += r;
		}
		double mean = sum / n;

		// Population stddev
		double sq_sum = 0.0;
		for (auto &r : rets) {
			double diff = r - mean;
			sq_sum += diff * diff;
		}
		double stddev = std::sqrt(sq_sum / n);

		if (stddev == 0.0) {
			result_validity.SetInvalid(ridx);
		} else {
			result_data[ridx] = (mean - state.risk_free_rate) / stddev;
		}

		delete state.returns;
		state.returns = nullptr;
	}
}

static void SharpeDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (SharpeState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (state.returns) {
			delete state.returns;
			state.returns = nullptr;
		}
	}
}

// ──────────────────────────────────────────────────────────────
// sortino_ratio(returns, risk_free_rate) — simple aggregate
//
//   (mean(returns) - risk_free_rate) / downside_deviation
//
// Downside deviation = sqrt(mean(min(r - risk_free_rate, 0)^2))
// Only negative excess returns contribute.
// ──────────────────────────────────────────────────────────────

// Reuses SharpeState — same data needed

static void SortinoFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (SharpeState **)sdata.data;

	auto result_data = FlatVector::GetData<double>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		auto ridx = i + offset;

		if (!state.returns || state.returns->size() < 2) {
			result_validity.SetInvalid(ridx);
			if (state.returns) {
				delete state.returns;
				state.returns = nullptr;
			}
			continue;
		}

		auto &rets = *state.returns;
		idx_t n = rets.size();
		double rfr = state.risk_free_rate;

		// Mean
		double sum = 0.0;
		for (auto &r : rets) {
			sum += r;
		}
		double mean = sum / n;

		// Downside deviation: sqrt(mean of squared negative excess returns)
		double downside_sq_sum = 0.0;
		for (auto &r : rets) {
			double excess = r - rfr;
			if (excess < 0.0) {
				downside_sq_sum += excess * excess;
			}
		}
		double downside_dev = std::sqrt(downside_sq_sum / n);

		if (downside_dev == 0.0) {
			result_validity.SetInvalid(ridx);
		} else {
			result_data[ridx] = (mean - rfr) / downside_dev;
		}

		delete state.returns;
		state.returns = nullptr;
	}
}

// ──────────────────────────────────────────────────────────────
// max_drawdown(price, timestamp) — ordered aggregate
//
// Maximum peak-to-trough decline: (trough - peak) / peak
// Returns a negative value (the decline fraction).
// Collects (price, ts), sorts by ts in Finalize, then scans
// for the largest drawdown.
//
// Follows the same collect-sort pattern as EMA/RSI.
// ──────────────────────────────────────────────────────────────

struct MaxDrawdownState {
	struct Entry {
		double price;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void MaxDrawdownInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<MaxDrawdownState *>(state_p);
	state.entries = nullptr;
}

static void MaxDrawdownUpdate(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector,
                               idx_t count) {
	auto &price_vec = inputs[0];
	auto &ts_vec = inputs[1];

	UnifiedVectorFormat price_data, ts_data, sdata;
	price_vec.ToUnifiedFormat(count, price_data);
	ts_vec.ToUnifiedFormat(count, ts_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto prices = UnifiedVectorFormat::GetData<double>(price_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto states = (MaxDrawdownState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];

		if (!state.entries) {
			state.entries = new std::vector<MaxDrawdownState::Entry>();
		}

		auto pidx = price_data.sel->get_index(i);
		auto tidx = ts_data.sel->get_index(i);

		if (price_data.validity.RowIsValid(pidx) && ts_data.validity.RowIsValid(tidx)) {
			state.entries->push_back({prices[pidx], timestamps[tidx]});
		}
	}
}

static void MaxDrawdownCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (MaxDrawdownState **)src_data.data;
	auto targets = (MaxDrawdownState **)tgt_data.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = src_data.sel->get_index(i);
		auto tidx = tgt_data.sel->get_index(i);
		auto &source = *sources[sidx];
		auto &target = *targets[tidx];

		if (source.entries) {
			if (!target.entries) {
				target.entries = new std::vector<MaxDrawdownState::Entry>();
			}
			target.entries->insert(target.entries->end(), source.entries->begin(), source.entries->end());
			delete source.entries;
			source.entries = nullptr;
		}
	}
}

static void MaxDrawdownFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count,
                                 idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (MaxDrawdownState **)sdata.data;

	auto result_data = FlatVector::GetData<double>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		auto ridx = i + offset;

		if (!state.entries || state.entries->size() < 2) {
			result_validity.SetInvalid(ridx);
			if (state.entries) {
				delete state.entries;
				state.entries = nullptr;
			}
			continue;
		}

		auto &entries = *state.entries;
		// Sort by timestamp
		std::sort(entries.begin(), entries.end(),
		          [](const MaxDrawdownState::Entry &a, const MaxDrawdownState::Entry &b) { return a.ts < b.ts; });

		double peak = entries[0].price;
		double max_dd = 0.0; // Worst drawdown (most negative)

		for (size_t j = 1; j < entries.size(); j++) {
			if (entries[j].price > peak) {
				peak = entries[j].price;
			}
			if (peak > 0.0) {
				double dd = (entries[j].price - peak) / peak;
				if (dd < max_dd) {
					max_dd = dd;
				}
			}
		}

		result_data[ridx] = max_dd;

		delete state.entries;
		state.entries = nullptr;
	}
}

static void MaxDrawdownDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (MaxDrawdownState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

// ── Registration ─────────────────────────────────────────────

void RegisterRiskFunctions(Connection &conn, Catalog &catalog) {
	// sharpe_ratio(returns, risk_free_rate) → double
	{
		AggregateFunction sharpe_func(
		    "sharpe_ratio", {LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
		    AggregateFunction::StateSize<SharpeState>, SharpeInitialize, SharpeUpdate, SharpeCombine, SharpeFinalize,
		    nullptr, nullptr, SharpeDestructor);

		CreateAggregateFunctionInfo info(sharpe_func);
		catalog.CreateFunction(*conn.context, info);
	}

	// sortino_ratio(returns, risk_free_rate) → double
	// Reuses SharpeState and update/combine — only Finalize differs
	{
		AggregateFunction sortino_func(
		    "sortino_ratio", {LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
		    AggregateFunction::StateSize<SharpeState>, SharpeInitialize, SharpeUpdate, SharpeCombine, SortinoFinalize,
		    nullptr, nullptr, SharpeDestructor);

		CreateAggregateFunctionInfo info(sortino_func);
		catalog.CreateFunction(*conn.context, info);
	}

	// max_drawdown(price, timestamp) → double
	{
		AggregateFunctionSet mdd_set("max_drawdown");

		// TIMESTAMP_TZ variant
		{
			AggregateFunction mdd_func(
			    "max_drawdown", {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ}, LogicalType::DOUBLE,
			    AggregateFunction::StateSize<MaxDrawdownState>, MaxDrawdownInitialize, MaxDrawdownUpdate,
			    MaxDrawdownCombine, MaxDrawdownFinalize, nullptr, nullptr, MaxDrawdownDestructor);
			mdd_set.AddFunction(mdd_func);
		}

		// TIMESTAMP variant
		{
			AggregateFunction mdd_func(
			    "max_drawdown", {LogicalType::DOUBLE, LogicalType::TIMESTAMP}, LogicalType::DOUBLE,
			    AggregateFunction::StateSize<MaxDrawdownState>, MaxDrawdownInitialize, MaxDrawdownUpdate,
			    MaxDrawdownCombine, MaxDrawdownFinalize, nullptr, nullptr, MaxDrawdownDestructor);
			mdd_set.AddFunction(mdd_func);
		}

		CreateAggregateFunctionInfo info(mdd_set);
		catalog.CreateFunction(*conn.context, info);
	}
}

} // namespace scrooge
} // namespace duckdb
