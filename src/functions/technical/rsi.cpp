#include "functions/technical.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// RSI (Relative Strength Index) — ordered aggregate
//
// Usage:  rsi(price, period ORDER BY timestamp)
//
// RSI = 100 - (100 / (1 + RS))
// RS  = avg_gain / avg_loss over `period`
//
// Uses Wilder's smoothing (exponential): after initial SMA,
//   avg_gain = (prev_avg_gain * (period-1) + current_gain) / period
//   avg_loss = (prev_avg_loss * (period-1) + current_loss) / period
// ──────────────────────────────────────────────────────────────

struct RsiFunctionData : public FunctionData {
	int32_t period;

	explicit RsiFunctionData(int32_t period_p) : period(period_p) {}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<RsiFunctionData>(period);
	}
	bool Equals(const FunctionData &other) const override {
		return period == other.Cast<RsiFunctionData>().period;
	}
};

struct RsiListState {
	struct Entry {
		double value;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void RsiInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<RsiListState *>(state_p);
	state.entries = nullptr;
}

static void RsiUpdate(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector, idx_t count) {
	auto &value_vec = inputs[0];
	auto &ts_vec = inputs[1];

	UnifiedVectorFormat value_data, ts_data, sdata;
	value_vec.ToUnifiedFormat(count, value_data);
	ts_vec.ToUnifiedFormat(count, ts_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto values = UnifiedVectorFormat::GetData<double>(value_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto states = (RsiListState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (!state.entries) {
			state.entries = new std::vector<RsiListState::Entry>();
		}
		auto vidx = value_data.sel->get_index(i);
		auto tidx = ts_data.sel->get_index(i);
		if (value_data.validity.RowIsValid(vidx) && ts_data.validity.RowIsValid(tidx)) {
			state.entries->push_back({values[vidx], timestamps[tidx]});
		}
	}
}

static void RsiCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (RsiListState **)src_data.data;
	auto targets = (RsiListState **)tgt_data.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = src_data.sel->get_index(i);
		auto tidx = tgt_data.sel->get_index(i);
		auto &source = *sources[sidx];
		auto &target = *targets[tidx];
		if (source.entries) {
			if (!target.entries) {
				target.entries = new std::vector<RsiListState::Entry>();
			}
			target.entries->insert(target.entries->end(), source.entries->begin(), source.entries->end());
			delete source.entries;
			source.entries = nullptr;
		}
	}
}

static void RsiFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count,
                         idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (RsiListState **)sdata.data;

	auto &bind_data = aggr_input.bind_data->Cast<RsiFunctionData>();
	auto result_data = FlatVector::GetData<double>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		auto ridx = i + offset;

		if (!state.entries || state.entries->size() < 2) {
			result_validity.SetInvalid(ridx);
			continue;
		}

		auto &entries = *state.entries;
		std::sort(entries.begin(), entries.end(),
		          [](const RsiListState::Entry &a, const RsiListState::Entry &b) { return a.ts < b.ts; });

		int32_t period = bind_data.period > 0 ? bind_data.period : 14;
		idx_t n = entries.size();

		if (n < (idx_t)(period + 1)) {
			// Not enough data for RSI calculation
			result_validity.SetInvalid(ridx);
			continue;
		}

		// Calculate initial average gain/loss (SMA over first `period` changes)
		double avg_gain = 0.0;
		double avg_loss = 0.0;
		for (idx_t j = 1; j <= (idx_t)period; j++) {
			double change = entries[j].value - entries[j - 1].value;
			if (change > 0) {
				avg_gain += change;
			} else {
				avg_loss += (-change);
			}
		}
		avg_gain /= period;
		avg_loss /= period;

		// Apply Wilder's smoothing for remaining values
		for (idx_t j = period + 1; j < n; j++) {
			double change = entries[j].value - entries[j - 1].value;
			double gain = change > 0 ? change : 0.0;
			double loss = change < 0 ? (-change) : 0.0;
			avg_gain = (avg_gain * (period - 1) + gain) / period;
			avg_loss = (avg_loss * (period - 1) + loss) / period;
		}

		if (avg_loss == 0.0) {
			result_data[ridx] = 100.0; // No losses = RSI 100
		} else {
			double rs = avg_gain / avg_loss;
			result_data[ridx] = 100.0 - (100.0 / (1.0 + rs));
		}

		delete state.entries;
		state.entries = nullptr;
	}
}

static void RsiDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (RsiListState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

unique_ptr<FunctionData> RsiBind(ClientContext &context, AggregateFunction &function,
                                  vector<unique_ptr<Expression>> &arguments) {
	int32_t period = 14; // Default RSI period
	if (arguments.size() >= 3 && arguments[2]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
		if (!val.IsNull()) {
			period = val.GetValue<int32_t>();
		}
	}
	return make_uniq<RsiFunctionData>(period);
}

void RegisterRsiFunction(Connection &conn, Catalog &catalog) {
	AggregateFunctionSet rsi_set("rsi");

	// rsi(price, timestamp) — uses default period 14
	{
		AggregateFunction rsi_func(
		    "rsi", {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ}, LogicalType::DOUBLE,
		    AggregateFunction::StateSize<RsiListState>, RsiInitialize, RsiUpdate, RsiCombine, RsiFinalize, nullptr,
		    RsiBind, RsiDestructor);
		rsi_set.AddFunction(rsi_func);
	}

	// rsi(price, timestamp) with TIMESTAMP (non-TZ)
	{
		AggregateFunction rsi_func(
		    "rsi", {LogicalType::DOUBLE, LogicalType::TIMESTAMP}, LogicalType::DOUBLE,
		    AggregateFunction::StateSize<RsiListState>, RsiInitialize, RsiUpdate, RsiCombine, RsiFinalize, nullptr,
		    RsiBind, RsiDestructor);
		rsi_set.AddFunction(rsi_func);
	}

	// rsi(price, timestamp, period) — explicit period
	{
		AggregateFunction rsi_func(
		    "rsi", {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ, LogicalType::INTEGER}, LogicalType::DOUBLE,
		    AggregateFunction::StateSize<RsiListState>, RsiInitialize, RsiUpdate, RsiCombine, RsiFinalize, nullptr,
		    RsiBind, RsiDestructor);
		rsi_set.AddFunction(rsi_func);
	}

	CreateAggregateFunctionInfo rsi_info(rsi_set);
	catalog.CreateFunction(*conn.context, rsi_info);
}

} // namespace scrooge
} // namespace duckdb
