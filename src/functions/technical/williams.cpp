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
// Williams %R — momentum oscillator that mirrors stochastic %K.
//
//   williams_r(high, low, close, ts [, period])
//
// %R = -100 * (highest_high - close) / (highest_high - lowest_low)
// Default period = 14. Range: -100 (oversold) to 0 (overbought).
// Conventionally treated as overbought above -20, oversold below -80.
// ──────────────────────────────────────────────────────────────

namespace {

struct PeriodFunctionData : public FunctionData {
	int32_t period;
	explicit PeriodFunctionData(int32_t p) : period(p) {}
	unique_ptr<FunctionData> Copy() const override { return make_uniq<PeriodFunctionData>(period); }
	bool Equals(const FunctionData &other) const override {
		return period == other.Cast<PeriodFunctionData>().period;
	}
};

struct WRState {
	struct Entry {
		double high, low, close;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void WRInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<WRState *>(state_p);
	state.entries = nullptr;
}

static void WRUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat hd, ld, cd, td, sd;
	inputs[0].ToUnifiedFormat(count, hd);
	inputs[1].ToUnifiedFormat(count, ld);
	inputs[2].ToUnifiedFormat(count, cd);
	inputs[3].ToUnifiedFormat(count, td);
	state_vector.ToUnifiedFormat(count, sd);
	auto highs = UnifiedVectorFormat::GetData<double>(hd);
	auto lows = UnifiedVectorFormat::GetData<double>(ld);
	auto closes = UnifiedVectorFormat::GetData<double>(cd);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(td);
	auto states = (WRState **)sd.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sd.sel->get_index(i);
		auto &state = *states[sidx];
		if (!state.entries) {
			state.entries = new std::vector<WRState::Entry>();
		}
		auto hi = hd.sel->get_index(i);
		auto li = ld.sel->get_index(i);
		auto ci = cd.sel->get_index(i);
		auto ti = td.sel->get_index(i);
		if (hd.validity.RowIsValid(hi) && ld.validity.RowIsValid(li) &&
		    cd.validity.RowIsValid(ci) && td.validity.RowIsValid(ti)) {
			state.entries->push_back({highs[hi], lows[li], closes[ci], timestamps[ti]});
		}
	}
}

static void WRCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sd, td;
	source_vec.ToUnifiedFormat(count, sd);
	target_vec.ToUnifiedFormat(count, td);
	auto sources = (WRState **)sd.data;
	auto targets = (WRState **)td.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[sd.sel->get_index(i)];
		auto &tgt = *targets[td.sel->get_index(i)];
		if (src.entries) {
			if (!tgt.entries) {
				tgt.entries = new std::vector<WRState::Entry>();
			}
			tgt.entries->insert(tgt.entries->end(), src.entries->begin(), src.entries->end());
			delete src.entries;
			src.entries = nullptr;
		}
	}
}

static void WRDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sd;
	state_vector.ToUnifiedFormat(count, sd);
	auto states = (WRState **)sd.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sd.sel->get_index(i)];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

static void WRFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sd;
	state_vector.ToUnifiedFormat(count, sd);
	auto states = (WRState **)sd.data;
	auto out = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	auto &bind = aggr_input.bind_data->Cast<PeriodFunctionData>();
	int32_t period = bind.period > 0 ? bind.period : 14;

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sd.sel->get_index(i)];
		auto ridx = i + offset;
		if (!state.entries || state.entries->empty()) {
			validity.SetInvalid(ridx);
			if (state.entries) {
				delete state.entries;
				state.entries = nullptr;
			}
			continue;
		}
		auto &entries = *state.entries;
		std::sort(entries.begin(), entries.end(),
		          [](const WRState::Entry &a, const WRState::Entry &b) { return a.ts < b.ts; });
		idx_t n = entries.size();
		idx_t start = n > (idx_t)period ? n - (idx_t)period : 0;
		double hh = entries[start].high;
		double ll = entries[start].low;
		for (idx_t j = start + 1; j < n; j++) {
			if (entries[j].high > hh) hh = entries[j].high;
			if (entries[j].low < ll) ll = entries[j].low;
		}
		double close = entries.back().close;
		double range = hh - ll;
		if (range <= 0) {
			validity.SetInvalid(ridx);
		} else {
			out[ridx] = -100.0 * (hh - close) / range;
		}
		delete state.entries;
		state.entries = nullptr;
	}
}

static unique_ptr<FunctionData> WRBind(ClientContext &context, AggregateFunction &,
                                          vector<unique_ptr<Expression>> &arguments) {
	int32_t period = 14;
	if (arguments.size() >= 5 && arguments[4]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[4]);
		if (!val.IsNull()) period = val.GetValue<int32_t>();
	}
	return make_uniq<PeriodFunctionData>(period);
}

} // namespace

void RegisterWilliamsR(Connection &conn, Catalog &catalog) {
	AggregateFunctionSet set("williams_r");
	auto base = vector<LogicalType>{LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                                  LogicalType::TIMESTAMP_TZ};
	auto with_period = vector<LogicalType>{LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                                          LogicalType::TIMESTAMP_TZ, LogicalType::INTEGER};
	auto base_ts = vector<LogicalType>{LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                                     LogicalType::TIMESTAMP};
	auto with_period_ts = vector<LogicalType>{LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                                             LogicalType::TIMESTAMP, LogicalType::INTEGER};
	for (auto &sig : {base, with_period, base_ts, with_period_ts}) {
		set.AddFunction(AggregateFunction(
		    "williams_r", sig, LogicalType::DOUBLE,
		    AggregateFunction::StateSize<WRState>,
		    WRInit, WRUpdate, WRCombine, WRFinalize, nullptr, WRBind, WRDestructor));
	}
	CreateAggregateFunctionInfo info(set);
	catalog.CreateFunction(*conn.context, info);
}

} // namespace scrooge
} // namespace duckdb
