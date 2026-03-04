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
// Chaikin Money Flow (CMF) — ordered aggregate
//
// Usage:  cmf(high, low, close, volume, timestamp [, period])
//
// Money Flow Multiplier = ((close - low) - (high - close)) / (high - low)
// Money Flow Volume = MFM * volume
// CMF = SUM(MFV over period) / SUM(volume over period)
// Default period: 20
// ──────────────────────────────────────────────────────────────

// ──────────────────────────────────────────────────────────────
// Accumulation/Distribution Line (AD Line) — ordered aggregate
//
// Usage:  ad_line(high, low, close, volume, timestamp)
//
// MFM = ((close - low) - (high - close)) / (high - low)
// AD = cumulative sum of MFM * volume
// ──────────────────────────────────────────────────────────────

struct CmfFunctionData : public FunctionData {
	int32_t period;
	explicit CmfFunctionData(int32_t p) : period(p) {}
	unique_ptr<FunctionData> Copy() const override { return make_uniq<CmfFunctionData>(period); }
	bool Equals(const FunctionData &other) const override { return period == other.Cast<CmfFunctionData>().period; }
};

struct MoneyFlowListState {
	struct Entry {
		double high;
		double low;
		double close;
		double volume;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void MfInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<MoneyFlowListState *>(state_p);
	state.entries = nullptr;
}

static void MfUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat h_data, l_data, c_data, v_data, ts_data, sdata;
	inputs[0].ToUnifiedFormat(count, h_data);
	inputs[1].ToUnifiedFormat(count, l_data);
	inputs[2].ToUnifiedFormat(count, c_data);
	inputs[3].ToUnifiedFormat(count, v_data);
	inputs[4].ToUnifiedFormat(count, ts_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto highs = UnifiedVectorFormat::GetData<double>(h_data);
	auto lows = UnifiedVectorFormat::GetData<double>(l_data);
	auto closes = UnifiedVectorFormat::GetData<double>(c_data);
	auto volumes = UnifiedVectorFormat::GetData<double>(v_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto states = (MoneyFlowListState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (!state.entries) {
			state.entries = new std::vector<MoneyFlowListState::Entry>();
		}
		auto hi = h_data.sel->get_index(i);
		auto li = l_data.sel->get_index(i);
		auto ci = c_data.sel->get_index(i);
		auto vi = v_data.sel->get_index(i);
		auto ti = ts_data.sel->get_index(i);
		if (h_data.validity.RowIsValid(hi) && l_data.validity.RowIsValid(li) && c_data.validity.RowIsValid(ci) &&
		    v_data.validity.RowIsValid(vi) && ts_data.validity.RowIsValid(ti)) {
			state.entries->push_back({highs[hi], lows[li], closes[ci], volumes[vi], timestamps[ti]});
		}
	}
}

static void MfCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (MoneyFlowListState **)src_data.data;
	auto targets = (MoneyFlowListState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = src_data.sel->get_index(i);
		auto tidx = tgt_data.sel->get_index(i);
		auto &source = *sources[sidx];
		auto &target = *targets[tidx];
		if (source.entries) {
			if (!target.entries) {
				target.entries = new std::vector<MoneyFlowListState::Entry>();
			}
			target.entries->insert(target.entries->end(), source.entries->begin(), source.entries->end());
			delete source.entries;
			source.entries = nullptr;
		}
	}
}

static double ComputeMFM(double high, double low, double close) {
	double range = high - low;
	if (range == 0.0) return 0.0;
	return ((close - low) - (high - close)) / range;
}

// ─── CMF Finalize ────────────────────────────────────────────
static void CmfFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count,
                         idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (MoneyFlowListState **)sdata.data;
	auto &bind_data = aggr_input.bind_data->Cast<CmfFunctionData>();
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
		          [](const MoneyFlowListState::Entry &a, const MoneyFlowListState::Entry &b) { return a.ts < b.ts; });

		int32_t period = bind_data.period > 0 ? bind_data.period : 20;
		idx_t n = entries.size();
		idx_t start = (n > (idx_t)period) ? n - period : 0;

		double sum_mfv = 0, sum_vol = 0;
		for (idx_t j = start; j < n; j++) {
			double mfm = ComputeMFM(entries[j].high, entries[j].low, entries[j].close);
			sum_mfv += mfm * entries[j].volume;
			sum_vol += entries[j].volume;
		}

		if (sum_vol == 0.0) {
			result_data[ridx] = 0.0;
		} else {
			result_data[ridx] = sum_mfv / sum_vol;
		}

		delete state.entries;
		state.entries = nullptr;
	}
}

// ─── AD Line Finalize ────────────────────────────────────────
static void AdLineFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count,
                            idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (MoneyFlowListState **)sdata.data;
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
		          [](const MoneyFlowListState::Entry &a, const MoneyFlowListState::Entry &b) { return a.ts < b.ts; });

		double ad = 0.0;
		for (auto &e : entries) {
			double mfm = ComputeMFM(e.high, e.low, e.close);
			ad += mfm * e.volume;
		}

		result_data[ridx] = ad;

		delete state.entries;
		state.entries = nullptr;
	}
}

static void MfDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (MoneyFlowListState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

static unique_ptr<FunctionData> CmfBind(ClientContext &context, AggregateFunction &,
                                         vector<unique_ptr<Expression>> &arguments) {
	int32_t period = 20;
	if (arguments.size() >= 6 && arguments[5]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[5]);
		if (!val.IsNull()) period = val.GetValue<int32_t>();
	}
	return make_uniq<CmfFunctionData>(period);
}

static unique_ptr<FunctionData> AdLineBind(ClientContext &, AggregateFunction &,
                                            vector<unique_ptr<Expression>> &) {
	return make_uniq<CmfFunctionData>(0); // period unused for AD Line
}

void RegisterCmfFunction(Connection &conn, Catalog &catalog) {
	// ─── CMF ─────────────────────────────────────────────────
	AggregateFunctionSet cmf_set("cmf");

	// cmf(high, low, close, volume, timestamp) — default period 20
	cmf_set.AddFunction(AggregateFunction(
	    "cmf",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	     LogicalType::TIMESTAMP_TZ},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<MoneyFlowListState>, MfInitialize, MfUpdate, MfCombine,
	    CmfFinalize, nullptr, CmfBind, MfDestructor));

	// cmf(high, low, close, volume, timestamp, period)
	cmf_set.AddFunction(AggregateFunction(
	    "cmf",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	     LogicalType::TIMESTAMP_TZ, LogicalType::INTEGER},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<MoneyFlowListState>, MfInitialize, MfUpdate, MfCombine,
	    CmfFinalize, nullptr, CmfBind, MfDestructor));

	// TIMESTAMP variant
	cmf_set.AddFunction(AggregateFunction(
	    "cmf",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<MoneyFlowListState>, MfInitialize, MfUpdate, MfCombine,
	    CmfFinalize, nullptr, CmfBind, MfDestructor));

	CreateAggregateFunctionInfo cmf_info(cmf_set);
	catalog.CreateFunction(*conn.context, cmf_info);

	// ─── AD Line ─────────────────────────────────────────────
	AggregateFunctionSet ad_set("ad_line");

	ad_set.AddFunction(AggregateFunction(
	    "ad_line",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	     LogicalType::TIMESTAMP_TZ},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<MoneyFlowListState>, MfInitialize, MfUpdate, MfCombine,
	    AdLineFinalize, nullptr, AdLineBind, MfDestructor));

	ad_set.AddFunction(AggregateFunction(
	    "ad_line",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<MoneyFlowListState>, MfInitialize, MfUpdate, MfCombine,
	    AdLineFinalize, nullptr, AdLineBind, MfDestructor));

	CreateAggregateFunctionInfo ad_info(ad_set);
	catalog.CreateFunction(*conn.context, ad_info);
}

} // namespace scrooge
} // namespace duckdb
