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
// MFI (Money Flow Index) — ordered aggregate
//
// Usage:  mfi(high, low, close, volume, timestamp [, period])
//
// Typical Price = (H+L+C)/3
// Raw Money Flow = TP * Volume
// Positive MF = sum of flows where TP > prev_TP
// Negative MF = sum of flows where TP < prev_TP
// MFI = 100 - 100/(1 + positive_flow/negative_flow)
// Default period: 14
// ──────────────────────────────────────────────────────────────

struct MfiFunctionData : public FunctionData {
	int32_t period;
	explicit MfiFunctionData(int32_t p) : period(p) {}
	unique_ptr<FunctionData> Copy() const override { return make_uniq<MfiFunctionData>(period); }
	bool Equals(const FunctionData &other) const override { return period == other.Cast<MfiFunctionData>().period; }
};

struct MfiListState {
	struct Entry {
		double high;
		double low;
		double close;
		double volume;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void MfiInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<MfiListState *>(state_p);
	state.entries = nullptr;
}

static void MfiUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
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
	auto states = (MfiListState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (!state.entries) {
			state.entries = new std::vector<MfiListState::Entry>();
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

static void MfiCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (MfiListState **)src_data.data;
	auto targets = (MfiListState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = src_data.sel->get_index(i);
		auto tidx = tgt_data.sel->get_index(i);
		auto &source = *sources[sidx];
		auto &target = *targets[tidx];
		if (source.entries) {
			if (!target.entries) {
				target.entries = new std::vector<MfiListState::Entry>();
			}
			target.entries->insert(target.entries->end(), source.entries->begin(), source.entries->end());
			delete source.entries;
			source.entries = nullptr;
		}
	}
}

static void MfiFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count,
                         idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (MfiListState **)sdata.data;
	auto &bind_data = aggr_input.bind_data->Cast<MfiFunctionData>();
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
		          [](const MfiListState::Entry &a, const MfiListState::Entry &b) { return a.ts < b.ts; });

		int32_t period = bind_data.period > 0 ? bind_data.period : 14;
		idx_t n = entries.size();

		// Compute typical prices
		std::vector<double> tp(n);
		for (idx_t j = 0; j < n; j++) {
			tp[j] = (entries[j].high + entries[j].low + entries[j].close) / 3.0;
		}

		// Look at last `period` bars (need period+1 for comparison)
		idx_t start = (n > (idx_t)(period + 1)) ? n - period - 1 : 0;
		double pos_flow = 0, neg_flow = 0;

		for (idx_t j = start + 1; j < n; j++) {
			double raw_mf = tp[j] * entries[j].volume;
			if (tp[j] > tp[j - 1]) {
				pos_flow += raw_mf;
			} else if (tp[j] < tp[j - 1]) {
				neg_flow += raw_mf;
			}
		}

		if (neg_flow == 0.0) {
			result_data[ridx] = 100.0;
		} else {
			double mf_ratio = pos_flow / neg_flow;
			result_data[ridx] = 100.0 - (100.0 / (1.0 + mf_ratio));
		}

		delete state.entries;
		state.entries = nullptr;
	}
}

static void MfiDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (MfiListState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

static unique_ptr<FunctionData> MfiBind(ClientContext &context, AggregateFunction &,
                                         vector<unique_ptr<Expression>> &arguments) {
	int32_t period = 14;
	if (arguments.size() >= 6 && arguments[5]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[5]);
		if (!val.IsNull()) period = val.GetValue<int32_t>();
	}
	return make_uniq<MfiFunctionData>(period);
}

void RegisterMfiFunction(Connection &conn, Catalog &catalog) {
	AggregateFunctionSet mfi_set("mfi");

	// mfi(high, low, close, volume, timestamp) — default period 14
	mfi_set.AddFunction(AggregateFunction(
	    "mfi",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	     LogicalType::TIMESTAMP_TZ},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<MfiListState>, MfiInitialize, MfiUpdate, MfiCombine,
	    MfiFinalize, nullptr, MfiBind, MfiDestructor));

	// mfi(high, low, close, volume, timestamp, period)
	mfi_set.AddFunction(AggregateFunction(
	    "mfi",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	     LogicalType::TIMESTAMP_TZ, LogicalType::INTEGER},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<MfiListState>, MfiInitialize, MfiUpdate, MfiCombine,
	    MfiFinalize, nullptr, MfiBind, MfiDestructor));

	// TIMESTAMP variant
	mfi_set.AddFunction(AggregateFunction(
	    "mfi",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<MfiListState>, MfiInitialize, MfiUpdate, MfiCombine,
	    MfiFinalize, nullptr, MfiBind, MfiDestructor));

	CreateAggregateFunctionInfo mfi_info(mfi_set);
	catalog.CreateFunction(*conn.context, mfi_info);
}

} // namespace scrooge
} // namespace duckdb
