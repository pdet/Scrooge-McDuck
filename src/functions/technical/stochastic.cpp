#include "functions/technical.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/helper.hpp"
#include <algorithm>
#include <vector>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// Stochastic Oscillator (%K) — ordered aggregate
//
// Usage:  stochastic_k(close, high, low, timestamp [, period])
//
// %K = 100 * (close - lowest_low(period)) / (highest_high(period) - lowest_low(period))
// Default period: 14
// Returns the final %K value of the series.
// ──────────────────────────────────────────────────────────────

struct StochFunctionData : public FunctionData {
	int32_t period;
	explicit StochFunctionData(int32_t p) : period(p) {}
	unique_ptr<FunctionData> Copy() const override { return make_uniq<StochFunctionData>(period); }
	bool Equals(const FunctionData &other) const override { return period == other.Cast<StochFunctionData>().period; }
};

struct StochListState {
	struct Entry {
		double close;
		double high;
		double low;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void StochInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<StochListState *>(state_p);
	state.entries = nullptr;
}

static void StochUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat close_data, high_data, low_data, ts_data, sdata;
	inputs[0].ToUnifiedFormat(count, close_data);
	inputs[1].ToUnifiedFormat(count, high_data);
	inputs[2].ToUnifiedFormat(count, low_data);
	inputs[3].ToUnifiedFormat(count, ts_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto closes = UnifiedVectorFormat::GetData<double>(close_data);
	auto highs = UnifiedVectorFormat::GetData<double>(high_data);
	auto lows = UnifiedVectorFormat::GetData<double>(low_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto states = (StochListState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (!state.entries) {
			state.entries = new std::vector<StochListState::Entry>();
		}
		auto cidx = close_data.sel->get_index(i);
		auto hidx = high_data.sel->get_index(i);
		auto lidx = low_data.sel->get_index(i);
		auto tidx = ts_data.sel->get_index(i);
		if (close_data.validity.RowIsValid(cidx) && high_data.validity.RowIsValid(hidx) &&
		    low_data.validity.RowIsValid(lidx) && ts_data.validity.RowIsValid(tidx)) {
			state.entries->push_back({closes[cidx], highs[hidx], lows[lidx], timestamps[tidx]});
		}
	}
}

static void StochCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (StochListState **)src_data.data;
	auto targets = (StochListState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = src_data.sel->get_index(i);
		auto tidx = tgt_data.sel->get_index(i);
		auto &source = *sources[sidx];
		auto &target = *targets[tidx];
		if (source.entries) {
			if (!target.entries) {
				target.entries = new std::vector<StochListState::Entry>();
			}
			target.entries->insert(target.entries->end(), source.entries->begin(), source.entries->end());
			delete source.entries;
			source.entries = nullptr;
		}
	}
}

static void StochFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count,
                           idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (StochListState **)sdata.data;
	auto &bind_data = aggr_input.bind_data->Cast<StochFunctionData>();
	auto result_data = FlatVector::GetData<double>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		auto ridx = i + offset;

		if (!state.entries || state.entries->empty()) {
			result_validity.SetInvalid(ridx);
			continue;
		}

		auto &entries = *state.entries;
		std::sort(entries.begin(), entries.end(),
		          [](const StochListState::Entry &a, const StochListState::Entry &b) { return a.ts < b.ts; });

		int32_t period = bind_data.period > 0 ? bind_data.period : 14;
		idx_t n = entries.size();

		// Look back `period` bars from the end
		idx_t start = (n > (idx_t)period) ? n - period : 0;
		double highest_high = entries[start].high;
		double lowest_low = entries[start].low;
		for (idx_t j = start + 1; j < n; j++) {
			if (entries[j].high > highest_high) highest_high = entries[j].high;
			if (entries[j].low < lowest_low) lowest_low = entries[j].low;
		}

		double range = highest_high - lowest_low;
		if (range == 0.0) {
			result_data[ridx] = 50.0; // midpoint when no range
		} else {
			double close = entries[n - 1].close;
			result_data[ridx] = 100.0 * (close - lowest_low) / range;
		}

		delete state.entries;
		state.entries = nullptr;
	}
}

static void StochDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (StochListState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

static unique_ptr<FunctionData> StochBind(ClientContext &context, AggregateFunction &,
                                           vector<unique_ptr<Expression>> &arguments) {
	int32_t period = 14;
	if (arguments.size() >= 5 && arguments[4]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[4]);
		if (!val.IsNull()) period = val.GetValue<int32_t>();
	}
	return make_uniq<StochFunctionData>(period);
}

void RegisterStochasticFunction(Connection &conn, Catalog &catalog) {
	AggregateFunctionSet stoch_set("stochastic_k");

	// (close, high, low, timestamp) — default period 14
	stoch_set.AddFunction(AggregateFunction(
	    "stochastic_k",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<StochListState>, StochInitialize, StochUpdate, StochCombine,
	    StochFinalize, nullptr, StochBind, StochDestructor));

	// (close, high, low, timestamp, period)
	stoch_set.AddFunction(AggregateFunction(
	    "stochastic_k",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ,
	     LogicalType::INTEGER},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<StochListState>, StochInitialize, StochUpdate, StochCombine,
	    StochFinalize, nullptr, StochBind, StochDestructor));

	// TIMESTAMP variant
	stoch_set.AddFunction(AggregateFunction(
	    "stochastic_k",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP}, LogicalType::DOUBLE,
	    AggregateFunction::StateSize<StochListState>, StochInitialize, StochUpdate, StochCombine, StochFinalize,
	    nullptr, StochBind, StochDestructor));

	CreateAggregateFunctionInfo stoch_info(stoch_set);
	catalog.CreateFunction(*conn.context, stoch_info);
}

} // namespace scrooge
} // namespace duckdb
