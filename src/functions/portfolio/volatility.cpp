#include "functions/portfolio.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include <vector>
#include <algorithm>
#include <cmath>
#include <numeric>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// annualized_volatility(returns) → DOUBLE
//
// σ_annual = σ_daily × √252
// Standard measure of risk used in modern portfolio theory.
// ──────────────────────────────────────────────────────────────

struct VolState {
	double sum;
	double sum_sq;
	int64_t count;
};

static void VolInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &s = *reinterpret_cast<VolState *>(state_p);
	s.sum = s.sum_sq = 0;
	s.count = 0;
}

static void VolUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat r_data, sdata;
	inputs[0].ToUnifiedFormat(count, r_data);
	state_vector.ToUnifiedFormat(count, sdata);
	auto vals = UnifiedVectorFormat::GetData<double>(r_data);
	auto states = (VolState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto ri = r_data.sel->get_index(i);
		auto si = sdata.sel->get_index(i);
		if (r_data.validity.RowIsValid(ri)) {
			double v = vals[ri];
			states[si]->sum += v;
			states[si]->sum_sq += v * v;
			states[si]->count++;
		}
	}
}

static void VolCombine(Vector &src_vec, Vector &tgt_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	src_vec.ToUnifiedFormat(count, src_data);
	tgt_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (VolState **)src_data.data;
	auto targets = (VolState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[src_data.sel->get_index(i)];
		auto &tgt = *targets[tgt_data.sel->get_index(i)];
		tgt.sum += src.sum;
		tgt.sum_sq += src.sum_sq;
		tgt.count += src.count;
	}
}

static void VolFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (VolState **)sdata.data;
	auto result_data = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto &s = *states[sdata.sel->get_index(i)];
		auto ridx = i + offset;
		if (s.count < 2) { validity.SetInvalid(ridx); continue; }
		double mean = s.sum / s.count;
		double var = (s.sum_sq / s.count) - (mean * mean);
		// Annualize: daily vol × √252
		result_data[ridx] = std::sqrt(var) * std::sqrt(252.0);
	}
}

// ──────────────────────────────────────────────────────────────
// calmar_ratio(returns, close, timestamp) → DOUBLE
//
// Annualized return / max drawdown
// Higher = better risk-adjusted return relative to worst peak-to-trough.
// ──────────────────────────────────────────────────────────────

struct CalmarState {
	struct Entry { double close; double ret; int64_t ts; };
	std::vector<Entry> *entries;
};

static void CalInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &s = *reinterpret_cast<CalmarState *>(state_p);
	s.entries = nullptr;
}

static void CalUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat r_data, c_data, ts_data, sdata;
	inputs[0].ToUnifiedFormat(count, r_data);
	inputs[1].ToUnifiedFormat(count, c_data);
	inputs[2].ToUnifiedFormat(count, ts_data);
	state_vector.ToUnifiedFormat(count, sdata);
	auto returns = UnifiedVectorFormat::GetData<double>(r_data);
	auto closes = UnifiedVectorFormat::GetData<double>(c_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto states = (CalmarState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto ri = r_data.sel->get_index(i);
		auto ci = c_data.sel->get_index(i);
		auto ti = ts_data.sel->get_index(i);
		auto si = sdata.sel->get_index(i);
		auto &state = *states[si];
		if (!state.entries) state.entries = new std::vector<CalmarState::Entry>();
		if (r_data.validity.RowIsValid(ri) && c_data.validity.RowIsValid(ci) && ts_data.validity.RowIsValid(ti)) {
			state.entries->push_back({closes[ci], returns[ri], timestamps[ti]});
		}
	}
}

static void CalCombine(Vector &src_vec, Vector &tgt_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	src_vec.ToUnifiedFormat(count, src_data);
	tgt_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (CalmarState **)src_data.data;
	auto targets = (CalmarState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[src_data.sel->get_index(i)];
		auto &tgt = *targets[tgt_data.sel->get_index(i)];
		if (src.entries) {
			if (!tgt.entries) tgt.entries = new std::vector<CalmarState::Entry>();
			tgt.entries->insert(tgt.entries->end(), src.entries->begin(), src.entries->end());
			delete src.entries; src.entries = nullptr;
		}
	}
}

static void CalFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (CalmarState **)sdata.data;
	auto result_data = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		auto ridx = i + offset;
		if (!state.entries || state.entries->size() < 2) { validity.SetInvalid(ridx); continue; }
		auto &entries = *state.entries;
		std::sort(entries.begin(), entries.end(),
		          [](const CalmarState::Entry &a, const CalmarState::Entry &b) { return a.ts < b.ts; });

		// Calculate annualized return
		double total_return = 0;
		for (auto &e : entries) total_return += e.ret;
		int64_t days = (entries.back().ts - entries[0].ts) / (86400LL * 1000000LL);
		double ann_return = (days > 0) ? total_return * (252.0 / days) : 0;

		// Calculate max drawdown
		double peak = entries[0].close;
		double max_dd = 0;
		for (auto &e : entries) {
			if (e.close > peak) peak = e.close;
			double dd = (peak - e.close) / peak;
			if (dd > max_dd) max_dd = dd;
		}

		result_data[ridx] = (max_dd > 0) ? ann_return / max_dd : std::numeric_limits<double>::infinity();
		delete state.entries; state.entries = nullptr;
	}
}

static void CalDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (CalmarState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		if (state.entries) { delete state.entries; state.entries = nullptr; }
	}
}

// ──────────────────────────────────────────────────────────────
// information_ratio(asset_returns, benchmark_returns) → DOUBLE
//
// (mean(excess) / std(excess)) × √252
// Measures skill of active management vs benchmark.
// ──────────────────────────────────────────────────────────────

struct IRState {
	double sum_excess;
	double sum_excess_sq;
	int64_t count;
};

static void IRInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &s = *reinterpret_cast<IRState *>(state_p);
	s.sum_excess = s.sum_excess_sq = 0;
	s.count = 0;
}

static void IRUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat a_data, b_data, sdata;
	inputs[0].ToUnifiedFormat(count, a_data);
	inputs[1].ToUnifiedFormat(count, b_data);
	state_vector.ToUnifiedFormat(count, sdata);
	auto asset = UnifiedVectorFormat::GetData<double>(a_data);
	auto bench = UnifiedVectorFormat::GetData<double>(b_data);
	auto states = (IRState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto ai = a_data.sel->get_index(i);
		auto bi = b_data.sel->get_index(i);
		auto si = sdata.sel->get_index(i);
		if (a_data.validity.RowIsValid(ai) && b_data.validity.RowIsValid(bi)) {
			double excess = asset[ai] - bench[bi];
			states[si]->sum_excess += excess;
			states[si]->sum_excess_sq += excess * excess;
			states[si]->count++;
		}
	}
}

static void IRCombine(Vector &src_vec, Vector &tgt_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	src_vec.ToUnifiedFormat(count, src_data);
	tgt_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (IRState **)src_data.data;
	auto targets = (IRState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[src_data.sel->get_index(i)];
		auto &tgt = *targets[tgt_data.sel->get_index(i)];
		tgt.sum_excess += src.sum_excess;
		tgt.sum_excess_sq += src.sum_excess_sq;
		tgt.count += src.count;
	}
}

static void IRFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (IRState **)sdata.data;
	auto result_data = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto &s = *states[sdata.sel->get_index(i)];
		auto ridx = i + offset;
		if (s.count < 2) { validity.SetInvalid(ridx); continue; }
		double mean = s.sum_excess / s.count;
		double var = (s.sum_excess_sq / s.count) - (mean * mean);
		double std = std::sqrt(var);
		result_data[ridx] = (std > 0) ? (mean / std) * std::sqrt(252.0) : 0.0;
	}
}

void RegisterVolatilityFunctions(Connection &conn, Catalog &catalog) {
	// annualized_volatility(returns)
	AggregateFunctionSet vol_set("annualized_volatility");
	vol_set.AddFunction(AggregateFunction(
	    "annualized_volatility", {LogicalType::DOUBLE},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<VolState>,
	    VolInitialize, VolUpdate, VolCombine, VolFinalize, nullptr, nullptr, nullptr));
	CreateAggregateFunctionInfo vol_info(vol_set);
	catalog.CreateFunction(*conn.context, vol_info);

	// calmar_ratio(returns, close, timestamp)
	AggregateFunctionSet cal_set("calmar_ratio");
	cal_set.AddFunction(AggregateFunction(
	    "calmar_ratio", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<CalmarState>,
	    CalInitialize, CalUpdate, CalCombine, CalFinalize, nullptr, nullptr, CalDestructor));
	cal_set.AddFunction(AggregateFunction(
	    "calmar_ratio", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<CalmarState>,
	    CalInitialize, CalUpdate, CalCombine, CalFinalize, nullptr, nullptr, CalDestructor));
	CreateAggregateFunctionInfo cal_info(cal_set);
	catalog.CreateFunction(*conn.context, cal_info);

	// information_ratio(asset_returns, benchmark_returns)
	AggregateFunctionSet ir_set("information_ratio");
	ir_set.AddFunction(AggregateFunction(
	    "information_ratio", {LogicalType::DOUBLE, LogicalType::DOUBLE},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<IRState>,
	    IRInitialize, IRUpdate, IRCombine, IRFinalize, nullptr, nullptr, nullptr));
	CreateAggregateFunctionInfo ir_info(ir_set);
	catalog.CreateFunction(*conn.context, ir_info);
}

} // namespace scrooge
} // namespace duckdb
