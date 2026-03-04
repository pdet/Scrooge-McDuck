#include "functions/technical.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/helper.hpp"
#include <algorithm>
#include <vector>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// OBV (On-Balance Volume) — ordered aggregate
//
// Usage:  obv(close, volume, timestamp)
//
// OBV tracks cumulative volume flow:
//   if close > prev_close: OBV += volume
//   if close < prev_close: OBV -= volume
//   if close == prev_close: OBV unchanged
// ──────────────────────────────────────────────────────────────

struct ObvListState {
	struct Entry {
		double close;
		double volume;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void ObvInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<ObvListState *>(state_p);
	state.entries = nullptr;
}

static void ObvUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	auto &close_vec = inputs[0];
	auto &vol_vec = inputs[1];
	auto &ts_vec = inputs[2];

	UnifiedVectorFormat close_data, vol_data, ts_data, sdata;
	close_vec.ToUnifiedFormat(count, close_data);
	vol_vec.ToUnifiedFormat(count, vol_data);
	ts_vec.ToUnifiedFormat(count, ts_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto closes = UnifiedVectorFormat::GetData<double>(close_data);
	auto volumes = UnifiedVectorFormat::GetData<double>(vol_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto states = (ObvListState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (!state.entries) {
			state.entries = new std::vector<ObvListState::Entry>();
		}
		auto cidx = close_data.sel->get_index(i);
		auto vidx = vol_data.sel->get_index(i);
		auto tidx = ts_data.sel->get_index(i);
		if (close_data.validity.RowIsValid(cidx) && vol_data.validity.RowIsValid(vidx) &&
		    ts_data.validity.RowIsValid(tidx)) {
			state.entries->push_back({closes[cidx], volumes[vidx], timestamps[tidx]});
		}
	}
}

static void ObvCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (ObvListState **)src_data.data;
	auto targets = (ObvListState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = src_data.sel->get_index(i);
		auto tidx = tgt_data.sel->get_index(i);
		auto &source = *sources[sidx];
		auto &target = *targets[tidx];
		if (source.entries) {
			if (!target.entries) {
				target.entries = new std::vector<ObvListState::Entry>();
			}
			target.entries->insert(target.entries->end(), source.entries->begin(), source.entries->end());
			delete source.entries;
			source.entries = nullptr;
		}
	}
}

static void ObvFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (ObvListState **)sdata.data;
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
		          [](const ObvListState::Entry &a, const ObvListState::Entry &b) { return a.ts < b.ts; });

		double obv = 0.0;
		for (size_t j = 1; j < entries.size(); j++) {
			if (entries[j].close > entries[j - 1].close) {
				obv += entries[j].volume;
			} else if (entries[j].close < entries[j - 1].close) {
				obv -= entries[j].volume;
			}
		}
		result_data[ridx] = obv;

		delete state.entries;
		state.entries = nullptr;
	}
}

static void ObvDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (ObvListState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

void RegisterObvFunction(Connection &conn, Catalog &catalog) {
	AggregateFunctionSet obv_set("obv");

	obv_set.AddFunction(AggregateFunction(
	    "obv", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ}, LogicalType::DOUBLE,
	    AggregateFunction::StateSize<ObvListState>, ObvInitialize, ObvUpdate, ObvCombine, ObvFinalize, nullptr, nullptr,
	    ObvDestructor));

	obv_set.AddFunction(AggregateFunction(
	    "obv", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP}, LogicalType::DOUBLE,
	    AggregateFunction::StateSize<ObvListState>, ObvInitialize, ObvUpdate, ObvCombine, ObvFinalize, nullptr, nullptr,
	    ObvDestructor));

	CreateAggregateFunctionInfo obv_info(obv_set);
	catalog.CreateFunction(*conn.context, obv_info);
}

} // namespace scrooge
} // namespace duckdb
