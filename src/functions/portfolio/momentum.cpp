#include "functions/portfolio.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/helper.hpp"
#include <algorithm>
#include <vector>
#include <cmath>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// momentum_score(close, timestamp [, period_days])
//
// Rate of change over the period: (close_last - close_first) / close_first
// Default period: 252 (1 trading year)
// Higher score = stronger momentum.
// ──────────────────────────────────────────────────────────────

struct MomentumFunctionData : public FunctionData {
	int32_t period_days;
	explicit MomentumFunctionData(int32_t p) : period_days(p) {}
	unique_ptr<FunctionData> Copy() const override { return make_uniq<MomentumFunctionData>(period_days); }
	bool Equals(const FunctionData &other) const override {
		return period_days == other.Cast<MomentumFunctionData>().period_days;
	}
};

struct MomentumListState {
	struct Entry {
		double close;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void MomInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<MomentumListState *>(state_p);
	state.entries = nullptr;
}

static void MomUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat c_data, ts_data, sdata;
	inputs[0].ToUnifiedFormat(count, c_data);
	inputs[1].ToUnifiedFormat(count, ts_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto closes = UnifiedVectorFormat::GetData<double>(c_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto states = (MomentumListState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto si = sdata.sel->get_index(i);
		auto ci = c_data.sel->get_index(i);
		auto ti = ts_data.sel->get_index(i);
		auto &state = *states[si];
		if (!state.entries) {
			state.entries = new std::vector<MomentumListState::Entry>();
		}
		if (c_data.validity.RowIsValid(ci) && ts_data.validity.RowIsValid(ti)) {
			state.entries->push_back({closes[ci], timestamps[ti]});
		}
	}
}

static void MomCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (MomentumListState **)src_data.data;
	auto targets = (MomentumListState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[src_data.sel->get_index(i)];
		auto &tgt = *targets[tgt_data.sel->get_index(i)];
		if (src.entries) {
			if (!tgt.entries) tgt.entries = new std::vector<MomentumListState::Entry>();
			tgt.entries->insert(tgt.entries->end(), src.entries->begin(), src.entries->end());
			delete src.entries;
			src.entries = nullptr;
		}
	}
}

static void MomFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count,
                         idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (MomentumListState **)sdata.data;
	auto &bind_data = aggr_input.bind_data->Cast<MomentumFunctionData>();
	auto result_data = FlatVector::GetData<double>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		auto ridx = i + offset;
		if (!state.entries || state.entries->size() < 2) {
			result_validity.SetInvalid(ridx);
			continue;
		}
		auto &entries = *state.entries;
		std::sort(entries.begin(), entries.end(),
		          [](const MomentumListState::Entry &a, const MomentumListState::Entry &b) { return a.ts < b.ts; });

		// Find first entry within period_days of the last entry
		int64_t period_micros = (int64_t)bind_data.period_days * 24LL * 3600LL * 1000000LL;
		int64_t last_ts = entries.back().ts;
		int64_t cutoff = last_ts - period_micros;

		// Find the entry closest to the cutoff
		double first_close = entries.front().close;
		for (auto &e : entries) {
			if (e.ts >= cutoff) {
				first_close = e.close;
				break;
			}
		}

		double last_close = entries.back().close;
		if (first_close == 0.0) {
			result_validity.SetInvalid(ridx);
		} else {
			result_data[ridx] = (last_close - first_close) / first_close;
		}

		delete state.entries;
		state.entries = nullptr;
	}
}

// ──────────────────────────────────────────────────────────────
// relative_strength(asset_close, benchmark_close, timestamp)
//
// RS = (asset_return over period) / (benchmark_return over period)
// > 1 means outperforming, < 1 means underperforming
// ──────────────────────────────────────────────────────────────

struct RelStrengthListState {
	struct Entry {
		double asset;
		double benchmark;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void RSInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<RelStrengthListState *>(state_p);
	state.entries = nullptr;
}

static void RSUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat a_data, b_data, ts_data, sdata;
	inputs[0].ToUnifiedFormat(count, a_data);
	inputs[1].ToUnifiedFormat(count, b_data);
	inputs[2].ToUnifiedFormat(count, ts_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto assets = UnifiedVectorFormat::GetData<double>(a_data);
	auto benchmarks = UnifiedVectorFormat::GetData<double>(b_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto states = (RelStrengthListState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto si = sdata.sel->get_index(i);
		auto ai = a_data.sel->get_index(i);
		auto bi = b_data.sel->get_index(i);
		auto ti = ts_data.sel->get_index(i);
		auto &state = *states[si];
		if (!state.entries) state.entries = new std::vector<RelStrengthListState::Entry>();
		if (a_data.validity.RowIsValid(ai) && b_data.validity.RowIsValid(bi) && ts_data.validity.RowIsValid(ti)) {
			state.entries->push_back({assets[ai], benchmarks[bi], timestamps[ti]});
		}
	}
}

static void RSCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (RelStrengthListState **)src_data.data;
	auto targets = (RelStrengthListState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[src_data.sel->get_index(i)];
		auto &tgt = *targets[tgt_data.sel->get_index(i)];
		if (src.entries) {
			if (!tgt.entries) tgt.entries = new std::vector<RelStrengthListState::Entry>();
			tgt.entries->insert(tgt.entries->end(), src.entries->begin(), src.entries->end());
			delete src.entries;
			src.entries = nullptr;
		}
	}
}

static void RSFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (RelStrengthListState **)sdata.data;
	auto result_data = FlatVector::GetData<double>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		auto ridx = i + offset;
		if (!state.entries || state.entries->size() < 2) {
			result_validity.SetInvalid(ridx);
			continue;
		}
		auto &entries = *state.entries;
		std::sort(entries.begin(), entries.end(),
		          [](const RelStrengthListState::Entry &a, const RelStrengthListState::Entry &b) { return a.ts < b.ts; });

		double asset_first = entries.front().asset;
		double asset_last = entries.back().asset;
		double bench_first = entries.front().benchmark;
		double bench_last = entries.back().benchmark;

		if (asset_first == 0.0 || bench_first == 0.0) {
			result_validity.SetInvalid(ridx);
		} else {
			double asset_return = (asset_last - asset_first) / asset_first;
			double bench_return = (bench_last - bench_first) / bench_first;
			if (bench_return == -1.0) {
				result_validity.SetInvalid(ridx);
			} else {
				// RS = (1 + asset_return) / (1 + bench_return)
				result_data[ridx] = (1.0 + asset_return) / (1.0 + bench_return);
			}
		}

		delete state.entries;
		state.entries = nullptr;
	}
}

static void MomDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (MomentumListState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		if (state.entries) { delete state.entries; state.entries = nullptr; }
	}
}

static void RSDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (RelStrengthListState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		if (state.entries) { delete state.entries; state.entries = nullptr; }
	}
}

static unique_ptr<FunctionData> MomBind(ClientContext &context, AggregateFunction &,
                                         vector<unique_ptr<Expression>> &arguments) {
	int32_t period = 252; // 1 trading year
	if (arguments.size() >= 3 && arguments[2]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
		if (!val.IsNull()) period = val.GetValue<int32_t>();
	}
	return make_uniq<MomentumFunctionData>(period);
}

void RegisterMomentumFunctions(Connection &conn, Catalog &catalog) {
	// momentum_score(close, timestamp) — default 252 days
	AggregateFunctionSet mom_set("momentum_score");

	mom_set.AddFunction(AggregateFunction(
	    "momentum_score",
	    {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<MomentumListState>,
	    MomInitialize, MomUpdate, MomCombine, MomFinalize, nullptr, MomBind, MomDestructor));

	mom_set.AddFunction(AggregateFunction(
	    "momentum_score",
	    {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ, LogicalType::INTEGER},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<MomentumListState>,
	    MomInitialize, MomUpdate, MomCombine, MomFinalize, nullptr, MomBind, MomDestructor));

	mom_set.AddFunction(AggregateFunction(
	    "momentum_score",
	    {LogicalType::DOUBLE, LogicalType::TIMESTAMP},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<MomentumListState>,
	    MomInitialize, MomUpdate, MomCombine, MomFinalize, nullptr, MomBind, MomDestructor));

	CreateAggregateFunctionInfo mom_info(mom_set);
	catalog.CreateFunction(*conn.context, mom_info);

	// relative_strength(asset_close, benchmark_close, timestamp)
	AggregateFunctionSet rs_set("relative_strength");

	rs_set.AddFunction(AggregateFunction(
	    "relative_strength",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<RelStrengthListState>,
	    RSInitialize, RSUpdate, RSCombine, RSFinalize, nullptr, nullptr, RSDestructor));

	rs_set.AddFunction(AggregateFunction(
	    "relative_strength",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<RelStrengthListState>,
	    RSInitialize, RSUpdate, RSCombine, RSFinalize, nullptr, nullptr, RSDestructor));

	CreateAggregateFunctionInfo rs_info(rs_set);
	catalog.CreateFunction(*conn.context, rs_info);
}

} // namespace scrooge
} // namespace duckdb
