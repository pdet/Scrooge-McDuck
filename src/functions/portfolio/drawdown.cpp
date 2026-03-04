#include "functions/portfolio.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/helper.hpp"
#include <algorithm>
#include <vector>
#include <cmath>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// drawdown_duration(close, timestamp)
//
// Returns the number of days from peak to recovery (or to latest
// if still in drawdown). Measures how long the worst drawdown lasted.
// ──────────────────────────────────────────────────────────────

struct DrawdownDurState {
	struct Entry { double close; int64_t ts; };
	std::vector<Entry> *entries;
};

static void DDInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<DrawdownDurState *>(state_p);
	state.entries = nullptr;
}

static void DDUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat c_data, ts_data, sdata;
	inputs[0].ToUnifiedFormat(count, c_data);
	inputs[1].ToUnifiedFormat(count, ts_data);
	state_vector.ToUnifiedFormat(count, sdata);
	auto closes = UnifiedVectorFormat::GetData<double>(c_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto states = (DrawdownDurState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto si = sdata.sel->get_index(i);
		auto ci = c_data.sel->get_index(i);
		auto ti = ts_data.sel->get_index(i);
		auto &state = *states[si];
		if (!state.entries) state.entries = new std::vector<DrawdownDurState::Entry>();
		if (c_data.validity.RowIsValid(ci) && ts_data.validity.RowIsValid(ti)) {
			state.entries->push_back({closes[ci], timestamps[ti]});
		}
	}
}

static void DDCombine(Vector &src_vec, Vector &tgt_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	src_vec.ToUnifiedFormat(count, src_data);
	tgt_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (DrawdownDurState **)src_data.data;
	auto targets = (DrawdownDurState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[src_data.sel->get_index(i)];
		auto &tgt = *targets[tgt_data.sel->get_index(i)];
		if (src.entries) {
			if (!tgt.entries) tgt.entries = new std::vector<DrawdownDurState::Entry>();
			tgt.entries->insert(tgt.entries->end(), src.entries->begin(), src.entries->end());
			delete src.entries; src.entries = nullptr;
		}
	}
}

static void DDFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (DrawdownDurState **)sdata.data;
	auto result_data = FlatVector::GetData<int64_t>(result);
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
		          [](const DrawdownDurState::Entry &a, const DrawdownDurState::Entry &b) { return a.ts < b.ts; });

		// Find longest drawdown duration in days
		double peak = entries[0].close;
		int64_t dd_start_ts = entries[0].ts;
		int64_t max_duration_days = 0;
		bool in_drawdown = false;

		for (auto &e : entries) {
			if (e.close >= peak) {
				if (in_drawdown) {
					int64_t dur = (e.ts - dd_start_ts) / (86400LL * 1000000LL); // micros to days
					max_duration_days = std::max(max_duration_days, dur);
					in_drawdown = false;
				}
				peak = e.close;
				dd_start_ts = e.ts;
			} else if (!in_drawdown) {
				in_drawdown = true;
				dd_start_ts = entries[0].ts; // from last peak
				// Find actual peak timestamp
				for (auto &p : entries) {
					if (p.close == peak) { dd_start_ts = p.ts; break; }
				}
			}
		}
		// If still in drawdown at end
		if (in_drawdown) {
			int64_t dur = (entries.back().ts - dd_start_ts) / (86400LL * 1000000LL);
			max_duration_days = std::max(max_duration_days, dur);
		}

		result_data[ridx] = max_duration_days;
		delete state.entries; state.entries = nullptr;
	}
}

static void DDDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (DrawdownDurState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		if (state.entries) { delete state.entries; state.entries = nullptr; }
	}
}

// ──────────────────────────────────────────────────────────────
// profit_factor(returns)
//
// Sum of positive returns / abs(sum of negative returns)
// > 1 means profitable, > 2 is very good
// ──────────────────────────────────────────────────────────────

struct ProfitFactorState {
	double sum_wins;
	double sum_losses;
};

static void PFInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<ProfitFactorState *>(state_p);
	state.sum_wins = 0;
	state.sum_losses = 0;
}

static void PFUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat r_data, sdata;
	inputs[0].ToUnifiedFormat(count, r_data);
	state_vector.ToUnifiedFormat(count, sdata);
	auto returns = UnifiedVectorFormat::GetData<double>(r_data);
	auto states = (ProfitFactorState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto ri = r_data.sel->get_index(i);
		auto si = sdata.sel->get_index(i);
		if (r_data.validity.RowIsValid(ri)) {
			double r = returns[ri];
			if (r > 0) states[si]->sum_wins += r;
			else if (r < 0) states[si]->sum_losses += (-r);
		}
	}
}

static void PFCombine(Vector &src_vec, Vector &tgt_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	src_vec.ToUnifiedFormat(count, src_data);
	tgt_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (ProfitFactorState **)src_data.data;
	auto targets = (ProfitFactorState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[src_data.sel->get_index(i)];
		auto &tgt = *targets[tgt_data.sel->get_index(i)];
		tgt.sum_wins += src.sum_wins;
		tgt.sum_losses += src.sum_losses;
	}
}

static void PFFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (ProfitFactorState **)sdata.data;
	auto result_data = FlatVector::GetData<double>(result);
	auto &result_validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		auto ridx = i + offset;
		if (state.sum_losses == 0.0) {
			if (state.sum_wins == 0.0) result_validity.SetInvalid(ridx);
			else result_data[ridx] = std::numeric_limits<double>::infinity();
		} else {
			result_data[ridx] = state.sum_wins / state.sum_losses;
		}
	}
}

void RegisterDrawdownFunctions(Connection &conn, Catalog &catalog) {
	// drawdown_duration(close, timestamp) → INTEGER (days)
	AggregateFunctionSet dd_set("drawdown_duration");
	dd_set.AddFunction(AggregateFunction(
	    "drawdown_duration", {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ},
	    LogicalType::BIGINT, AggregateFunction::StateSize<DrawdownDurState>,
	    DDInitialize, DDUpdate, DDCombine, DDFinalize, nullptr, nullptr, DDDestructor));
	dd_set.AddFunction(AggregateFunction(
	    "drawdown_duration", {LogicalType::DOUBLE, LogicalType::TIMESTAMP},
	    LogicalType::BIGINT, AggregateFunction::StateSize<DrawdownDurState>,
	    DDInitialize, DDUpdate, DDCombine, DDFinalize, nullptr, nullptr, DDDestructor));
	CreateAggregateFunctionInfo dd_info(dd_set);
	catalog.CreateFunction(*conn.context, dd_info);

	// profit_factor(returns) → DOUBLE
	AggregateFunctionSet pf_set("profit_factor");
	pf_set.AddFunction(AggregateFunction(
	    "profit_factor", {LogicalType::DOUBLE},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<ProfitFactorState>,
	    PFInitialize, PFUpdate, PFCombine, PFFinalize, nullptr, nullptr, nullptr));
	CreateAggregateFunctionInfo pf_info(pf_set);
	catalog.CreateFunction(*conn.context, pf_info);
}

} // namespace scrooge
} // namespace duckdb
