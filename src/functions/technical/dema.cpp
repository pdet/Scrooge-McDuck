#include "functions/technical.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include <vector>
#include <algorithm>
#include <cmath>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// dema(close, timestamp, period) — Double Exponential Moving Average
//
// DEMA = 2×EMA(n) - EMA(EMA(n))
// Reduces lag compared to standard EMA.
//
// tema(close, timestamp, period) — Triple Exponential Moving Average
//
// TEMA = 3×EMA(n) - 3×EMA(EMA(n)) + EMA(EMA(EMA(n)))
// Even less lag than DEMA.
// ──────────────────────────────────────────────────────────────

struct DEMAState {
	struct Entry { double close; int64_t ts; };
	std::vector<Entry> *entries;
	int32_t period;
};

static void DEMAInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &s = *reinterpret_cast<DEMAState *>(state_p);
	s.entries = nullptr;
	s.period = 0;
}

static void DEMAUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat c_data, ts_data, p_data, sdata;
	inputs[0].ToUnifiedFormat(count, c_data);
	inputs[1].ToUnifiedFormat(count, ts_data);
	inputs[2].ToUnifiedFormat(count, p_data);
	state_vector.ToUnifiedFormat(count, sdata);
	auto closes = UnifiedVectorFormat::GetData<double>(c_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto periods = UnifiedVectorFormat::GetData<int32_t>(p_data);
	auto states = (DEMAState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto ci = c_data.sel->get_index(i);
		auto ti = ts_data.sel->get_index(i);
		auto pi = p_data.sel->get_index(i);
		auto si = sdata.sel->get_index(i);
		auto &state = *states[si];
		if (!state.entries) state.entries = new std::vector<DEMAState::Entry>();
		if (c_data.validity.RowIsValid(ci) && ts_data.validity.RowIsValid(ti)) {
			state.entries->push_back({closes[ci], timestamps[ti]});
			state.period = periods[pi];
		}
	}
}

static void DEMACombine(Vector &src_vec, Vector &tgt_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	src_vec.ToUnifiedFormat(count, src_data);
	tgt_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (DEMAState **)src_data.data;
	auto targets = (DEMAState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[src_data.sel->get_index(i)];
		auto &tgt = *targets[tgt_data.sel->get_index(i)];
		if (src.entries) {
			if (!tgt.entries) tgt.entries = new std::vector<DEMAState::Entry>();
			tgt.entries->insert(tgt.entries->end(), src.entries->begin(), src.entries->end());
			if (src.period > 0) tgt.period = src.period;
			delete src.entries; src.entries = nullptr;
		}
	}
}

// Compute EMA series from price series
static std::vector<double> ComputeEMA(const std::vector<double> &prices, int period) {
	std::vector<double> ema(prices.size());
	if (prices.empty() || period <= 0) return ema;
	double k = 2.0 / (period + 1.0);
	ema[0] = prices[0];
	for (size_t i = 1; i < prices.size(); i++) {
		ema[i] = prices[i] * k + ema[i - 1] * (1.0 - k);
	}
	return ema;
}

static void DEMAFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (DEMAState **)sdata.data;
	auto result_data = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		auto ridx = i + offset;
		if (!state.entries || state.entries->empty() || state.period <= 0) {
			validity.SetInvalid(ridx); continue;
		}
		auto &entries = *state.entries;
		std::sort(entries.begin(), entries.end(),
		          [](const DEMAState::Entry &a, const DEMAState::Entry &b) { return a.ts < b.ts; });
		std::vector<double> prices;
		for (auto &e : entries) prices.push_back(e.close);
		auto ema1 = ComputeEMA(prices, state.period);
		auto ema2 = ComputeEMA(ema1, state.period);
		// DEMA = 2*EMA - EMA(EMA)
		result_data[ridx] = 2.0 * ema1.back() - ema2.back();
		delete state.entries; state.entries = nullptr;
	}
}

static void TEMAFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (DEMAState **)sdata.data;
	auto result_data = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		auto ridx = i + offset;
		if (!state.entries || state.entries->empty() || state.period <= 0) {
			validity.SetInvalid(ridx); continue;
		}
		auto &entries = *state.entries;
		std::sort(entries.begin(), entries.end(),
		          [](const DEMAState::Entry &a, const DEMAState::Entry &b) { return a.ts < b.ts; });
		std::vector<double> prices;
		for (auto &e : entries) prices.push_back(e.close);
		auto ema1 = ComputeEMA(prices, state.period);
		auto ema2 = ComputeEMA(ema1, state.period);
		auto ema3 = ComputeEMA(ema2, state.period);
		// TEMA = 3*EMA - 3*EMA(EMA) + EMA(EMA(EMA))
		result_data[ridx] = 3.0 * ema1.back() - 3.0 * ema2.back() + ema3.back();
		delete state.entries; state.entries = nullptr;
	}
}

static void DEMADestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (DEMAState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		if (state.entries) { delete state.entries; state.entries = nullptr; }
	}
}

void RegisterDEMAFunctions(Connection &conn, Catalog &catalog) {
	auto make_fn = [](const string &name, aggregate_finalize_t finalize) {
		AggregateFunctionSet set(name);
		set.AddFunction(AggregateFunction(
		    name, {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ, LogicalType::INTEGER},
		    LogicalType::DOUBLE, AggregateFunction::StateSize<DEMAState>,
		    DEMAInitialize, DEMAUpdate, DEMACombine, finalize, nullptr, nullptr, DEMADestructor));
		set.AddFunction(AggregateFunction(
		    name, {LogicalType::DOUBLE, LogicalType::TIMESTAMP, LogicalType::INTEGER},
		    LogicalType::DOUBLE, AggregateFunction::StateSize<DEMAState>,
		    DEMAInitialize, DEMAUpdate, DEMACombine, finalize, nullptr, nullptr, DEMADestructor));
		return set;
	};

	auto dema_set = make_fn("dema", DEMAFinalize);
	CreateAggregateFunctionInfo dema_info(dema_set);
	catalog.CreateFunction(*conn.context, dema_info);

	auto tema_set = make_fn("tema", TEMAFinalize);
	CreateAggregateFunctionInfo tema_info(tema_set);
	catalog.CreateFunction(*conn.context, tema_info);
}

} // namespace scrooge
} // namespace duckdb
