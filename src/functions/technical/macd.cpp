#include "functions/technical.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/helper.hpp"
#include <algorithm>
#include <vector>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// MACD (Moving Average Convergence Divergence)
//
// Returns a table with: macd_line, signal_line, histogram
//
// macd_line = EMA(fast_period) - EMA(slow_period)
// signal_line = EMA(macd_line, signal_period)
// histogram = macd_line - signal_line
//
// Default periods: fast=12, slow=26, signal=9
//
// Usage:  SELECT * FROM macd(prices, 'price', 'ts', 12, 26, 9)
//         or as aggregate: macd_line(price, ts, 12, 26)
// ──────────────────────────────────────────────────────────────

// Helper: compute EMA over a vector of doubles
static double compute_ema_final(const std::vector<double> &values, int32_t period) {
	if (values.empty()) return 0.0;
	double multiplier = 2.0 / (period + 1);
	double ema = values[0];
	for (size_t i = 1; i < values.size(); i++) {
		ema = values[i] * multiplier + ema * (1.0 - multiplier);
	}
	return ema;
}

// ── macd_line aggregate ──────────────────────────────────────

struct MacdFunctionData : public FunctionData {
	int32_t fast_period;
	int32_t slow_period;

	MacdFunctionData(int32_t fast, int32_t slow) : fast_period(fast), slow_period(slow) {}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<MacdFunctionData>(fast_period, slow_period);
	}
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<MacdFunctionData>();
		return fast_period == o.fast_period && slow_period == o.slow_period;
	}
};

struct MacdListState {
	struct Entry {
		double value;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void MacdInitialize(AggregateInputData &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<MacdListState *>(state_p);
	state.entries = nullptr;
}

static void MacdUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	auto &value_vec = inputs[0];
	auto &ts_vec = inputs[1];

	UnifiedVectorFormat value_data, ts_data, sdata;
	value_vec.ToUnifiedFormat(count, value_data);
	ts_vec.ToUnifiedFormat(count, ts_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto values = UnifiedVectorFormat::GetData<double>(value_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto states = (MacdListState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (!state.entries) {
			state.entries = new std::vector<MacdListState::Entry>();
		}
		auto vidx = value_data.sel->get_index(i);
		auto tidx = ts_data.sel->get_index(i);
		if (value_data.validity.RowIsValid(vidx) && ts_data.validity.RowIsValid(tidx)) {
			state.entries->push_back({values[vidx], timestamps[tidx]});
		}
	}
}

static void MacdCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (MacdListState **)src_data.data;
	auto targets = (MacdListState **)tgt_data.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = src_data.sel->get_index(i);
		auto tidx = tgt_data.sel->get_index(i);
		auto &source = *sources[sidx];
		auto &target = *targets[tidx];
		if (source.entries) {
			if (!target.entries) {
				target.entries = new std::vector<MacdListState::Entry>();
			}
			target.entries->insert(target.entries->end(), source.entries->begin(), source.entries->end());
			delete source.entries;
			source.entries = nullptr;
		}
	}
}

static void MacdFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count,
                          idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (MacdListState **)sdata.data;
	auto &bind_data = aggr_input.bind_data->Cast<MacdFunctionData>();
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
		          [](const MacdListState::Entry &a, const MacdListState::Entry &b) { return a.ts < b.ts; });

		std::vector<double> values;
		values.reserve(entries.size());
		for (auto &e : entries) {
			values.push_back(e.value);
		}

		double fast_ema = compute_ema_final(values, bind_data.fast_period);
		double slow_ema = compute_ema_final(values, bind_data.slow_period);
		result_data[ridx] = fast_ema - slow_ema;

		delete state.entries;
		state.entries = nullptr;
	}
}

static void MacdDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (MacdListState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

unique_ptr<FunctionData> MacdBind(ClientContext &context, AggregateFunction &,
                                   vector<unique_ptr<Expression>> &arguments) {
	int32_t fast = 12, slow = 26;
	if (arguments.size() >= 3 && arguments[2]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
		if (!val.IsNull()) fast = val.GetValue<int32_t>();
	}
	if (arguments.size() >= 4 && arguments[3]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[3]);
		if (!val.IsNull()) slow = val.GetValue<int32_t>();
	}
	return make_uniq<MacdFunctionData>(fast, slow);
}

void RegisterMacdFunction(Connection &conn, Catalog &catalog) {
	AggregateFunctionSet macd_set("macd_line");

	// macd_line(price, timestamp) — default 12/26
	{
		AggregateFunction f("macd_line", {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ}, LogicalType::DOUBLE,
		                    AggregateFunction::StateSize<MacdListState>, MacdInitialize, MacdUpdate, MacdCombine,
		                    MacdFinalize, nullptr, MacdBind, MacdDestructor);
		macd_set.AddFunction(f);
	}

	// macd_line(price, timestamp, fast, slow)
	{
		AggregateFunction f("macd_line",
		                    {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ, LogicalType::INTEGER, LogicalType::INTEGER},
		                    LogicalType::DOUBLE, AggregateFunction::StateSize<MacdListState>, MacdInitialize,
		                    MacdUpdate, MacdCombine, MacdFinalize, nullptr, MacdBind, MacdDestructor);
		macd_set.AddFunction(f);
	}

	// TIMESTAMP (non-TZ) variants
	{
		AggregateFunction f("macd_line", {LogicalType::DOUBLE, LogicalType::TIMESTAMP}, LogicalType::DOUBLE,
		                    AggregateFunction::StateSize<MacdListState>, MacdInitialize, MacdUpdate, MacdCombine,
		                    MacdFinalize, nullptr, MacdBind, MacdDestructor);
		macd_set.AddFunction(f);
	}

	CreateAggregateFunctionInfo macd_info(macd_set);
	catalog.CreateFunction(*conn.context, macd_info);
}

} // namespace scrooge
} // namespace duckdb
