#include "functions/technical.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/helper.hpp"
#include <algorithm>
#include <cmath>
#include <vector>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// ATR (Average True Range) — ordered aggregate
//
// Usage:  atr(high, low, close, timestamp [, period])
//
// True Range = max(high-low, |high-prev_close|, |low-prev_close|)
// ATR = Wilder's smoothed average of True Range over `period`
// Default period: 14
// ──────────────────────────────────────────────────────────────

struct AtrFunctionData : public FunctionData {
	int32_t period;
	explicit AtrFunctionData(int32_t p) : period(p) {}
	unique_ptr<FunctionData> Copy() const override { return make_uniq<AtrFunctionData>(period); }
	bool Equals(const FunctionData &other) const override { return period == other.Cast<AtrFunctionData>().period; }
};

struct AtrListState {
	struct Entry {
		double high;
		double low;
		double close;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void AtrInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<AtrListState *>(state_p);
	state.entries = nullptr;
}

static void AtrUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat high_data, low_data, close_data, ts_data, sdata;
	inputs[0].ToUnifiedFormat(count, high_data);
	inputs[1].ToUnifiedFormat(count, low_data);
	inputs[2].ToUnifiedFormat(count, close_data);
	inputs[3].ToUnifiedFormat(count, ts_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto highs = UnifiedVectorFormat::GetData<double>(high_data);
	auto lows = UnifiedVectorFormat::GetData<double>(low_data);
	auto closes = UnifiedVectorFormat::GetData<double>(close_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto states = (AtrListState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (!state.entries) {
			state.entries = new std::vector<AtrListState::Entry>();
		}
		auto hidx = high_data.sel->get_index(i);
		auto lidx = low_data.sel->get_index(i);
		auto cidx = close_data.sel->get_index(i);
		auto tidx = ts_data.sel->get_index(i);
		if (high_data.validity.RowIsValid(hidx) && low_data.validity.RowIsValid(lidx) &&
		    close_data.validity.RowIsValid(cidx) && ts_data.validity.RowIsValid(tidx)) {
			state.entries->push_back({highs[hidx], lows[lidx], closes[cidx], timestamps[tidx]});
		}
	}
}

static void AtrCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (AtrListState **)src_data.data;
	auto targets = (AtrListState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = src_data.sel->get_index(i);
		auto tidx = tgt_data.sel->get_index(i);
		auto &source = *sources[sidx];
		auto &target = *targets[tidx];
		if (source.entries) {
			if (!target.entries) {
				target.entries = new std::vector<AtrListState::Entry>();
			}
			target.entries->insert(target.entries->end(), source.entries->begin(), source.entries->end());
			delete source.entries;
			source.entries = nullptr;
		}
	}
}

static void AtrFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count,
                         idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (AtrListState **)sdata.data;
	auto &bind_data = aggr_input.bind_data->Cast<AtrFunctionData>();
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
		          [](const AtrListState::Entry &a, const AtrListState::Entry &b) { return a.ts < b.ts; });

		int32_t period = bind_data.period > 0 ? bind_data.period : 14;
		idx_t n = entries.size();

		// Compute True Range for each bar (starting from index 1)
		std::vector<double> tr_values;
		tr_values.reserve(n - 1);
		for (idx_t j = 1; j < n; j++) {
			double hl = entries[j].high - entries[j].low;
			double hpc = std::abs(entries[j].high - entries[j - 1].close);
			double lpc = std::abs(entries[j].low - entries[j - 1].close);
			tr_values.push_back(std::max({hl, hpc, lpc}));
		}

		if (tr_values.size() < (size_t)period) {
			// Not enough data — return simple average of available TR
			double sum = 0;
			for (auto &v : tr_values) sum += v;
			result_data[ridx] = sum / tr_values.size();
		} else {
			// Initial ATR = SMA of first `period` TRs
			double atr = 0;
			for (idx_t j = 0; j < (idx_t)period; j++) {
				atr += tr_values[j];
			}
			atr /= period;

			// Wilder's smoothing
			for (size_t j = period; j < tr_values.size(); j++) {
				atr = (atr * (period - 1) + tr_values[j]) / period;
			}
			result_data[ridx] = atr;
		}

		delete state.entries;
		state.entries = nullptr;
	}
}

static void AtrDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (AtrListState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

static unique_ptr<FunctionData> AtrBind(ClientContext &context, AggregateFunction &,
                                         vector<unique_ptr<Expression>> &arguments) {
	int32_t period = 14;
	if (arguments.size() >= 5 && arguments[4]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[4]);
		if (!val.IsNull()) period = val.GetValue<int32_t>();
	}
	return make_uniq<AtrFunctionData>(period);
}

void RegisterAtrFunction(Connection &conn, Catalog &catalog) {
	AggregateFunctionSet atr_set("atr");

	// atr(high, low, close, timestamp) — default period 14
	atr_set.AddFunction(AggregateFunction(
	    "atr", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<AtrListState>, AtrInitialize, AtrUpdate, AtrCombine,
	    AtrFinalize, nullptr, AtrBind, AtrDestructor));

	// atr(high, low, close, timestamp, period)
	atr_set.AddFunction(AggregateFunction(
	    "atr",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ,
	     LogicalType::INTEGER},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<AtrListState>, AtrInitialize, AtrUpdate, AtrCombine,
	    AtrFinalize, nullptr, AtrBind, AtrDestructor));

	// TIMESTAMP variants
	atr_set.AddFunction(AggregateFunction(
	    "atr", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<AtrListState>, AtrInitialize, AtrUpdate, AtrCombine,
	    AtrFinalize, nullptr, AtrBind, AtrDestructor));

	CreateAggregateFunctionInfo atr_info(atr_set);
	catalog.CreateFunction(*conn.context, atr_info);
}

} // namespace scrooge
} // namespace duckdb
