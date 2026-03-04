#include "functions/technical.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/catalog/catalog.hpp"
#include <vector>
#include <algorithm>
#include <cmath>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// wma(close, timestamp, period) — Weighted Moving Average
//
// Recent values get more weight. Weight_i = i (1 for oldest, n for newest).
// WMA = Σ(weight_i × value_i) / Σ(weight_i)
// ──────────────────────────────────────────────────────────────

struct WMAState {
	struct Entry { double value; int64_t ts; };
	std::vector<Entry> *entries;
};

static void WMAInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &s = *reinterpret_cast<WMAState *>(state_p);
	s.entries = nullptr;
}

static void WMAUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat v_data, ts_data, sdata;
	inputs[0].ToUnifiedFormat(count, v_data);
	inputs[1].ToUnifiedFormat(count, ts_data);
	state_vector.ToUnifiedFormat(count, sdata);
	auto vals = UnifiedVectorFormat::GetData<double>(v_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto states = (WMAState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto vi = v_data.sel->get_index(i);
		auto ti = ts_data.sel->get_index(i);
		auto si = sdata.sel->get_index(i);
		auto &state = *states[si];
		if (!state.entries) state.entries = new std::vector<WMAState::Entry>();
		if (v_data.validity.RowIsValid(vi) && ts_data.validity.RowIsValid(ti)) {
			state.entries->push_back({vals[vi], timestamps[ti]});
		}
	}
}

static void WMACombine(Vector &src_vec, Vector &tgt_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	src_vec.ToUnifiedFormat(count, src_data);
	tgt_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (WMAState **)src_data.data;
	auto targets = (WMAState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[src_data.sel->get_index(i)];
		auto &tgt = *targets[tgt_data.sel->get_index(i)];
		if (src.entries) {
			if (!tgt.entries) tgt.entries = new std::vector<WMAState::Entry>();
			tgt.entries->insert(tgt.entries->end(), src.entries->begin(), src.entries->end());
			delete src.entries; src.entries = nullptr;
		}
	}
}

static void WMAFinalize(Vector &state_vector, AggregateInputData &aggr, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (WMAState **)sdata.data;
	auto result_data = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);

	// Get period from bind info
	int period = 20;
	if (aggr.bind_data) {
		// Use default period if no bind data
	}

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		auto ridx = i + offset;
		if (!state.entries || state.entries->empty()) {
			validity.SetInvalid(ridx);
			continue;
		}
		auto &entries = *state.entries;
		std::sort(entries.begin(), entries.end(),
		          [](const WMAState::Entry &a, const WMAState::Entry &b) { return a.ts < b.ts; });

		// Use last `period` entries (or all if < period)
		idx_t n = entries.size();
		idx_t start = (n > (idx_t)period) ? n - period : 0;
		double weighted_sum = 0, weight_sum = 0;
		int w = 1;
		for (idx_t j = start; j < n; j++, w++) {
			weighted_sum += w * entries[j].value;
			weight_sum += w;
		}
		result_data[ridx] = weighted_sum / weight_sum;
		delete state.entries; state.entries = nullptr;
	}
}

static void WMADestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (WMAState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		if (state.entries) { delete state.entries; state.entries = nullptr; }
	}
}

// ──────────────────────────────────────────────────────────────
// pivot_point(high, low, close) — Classic Pivot Point
// pivot_r1/r2/r3(high, low, close) — Resistance levels
// pivot_s1/s2/s3(high, low, close) — Support levels
//
// These are scalar functions (computed from single candle data).
// PP = (H + L + C) / 3
// R1 = 2*PP - L, S1 = 2*PP - H
// R2 = PP + (H-L), S2 = PP - (H-L)
// R3 = H + 2*(PP-L), S3 = L - 2*(H-PP)
// ──────────────────────────────────────────────────────────────

static void PivotPointFunc(DataChunk &args, ExpressionState &, Vector &result) {
	auto &h = args.data[0]; auto &l = args.data[1]; auto &c = args.data[2];
	UnifiedVectorFormat hf, lf, cf;
	h.ToUnifiedFormat(args.size(), hf);
	l.ToUnifiedFormat(args.size(), lf);
	c.ToUnifiedFormat(args.size(), cf);
	auto hd = UnifiedVectorFormat::GetData<double>(hf);
	auto ld = UnifiedVectorFormat::GetData<double>(lf);
	auto cd = UnifiedVectorFormat::GetData<double>(cf);
	auto rd = FlatVector::GetData<double>(result);
	auto &rv = FlatVector::Validity(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	for (idx_t i = 0; i < args.size(); i++) {
		auto hi = hf.sel->get_index(i); auto li = lf.sel->get_index(i); auto ci = cf.sel->get_index(i);
		if (!hf.validity.RowIsValid(hi) || !lf.validity.RowIsValid(li) || !cf.validity.RowIsValid(ci)) {
			rv.SetInvalid(i); continue;
		}
		rd[i] = (hd[hi] + ld[li] + cd[ci]) / 3.0;
	}
}

#define PIVOT_LEVEL_FUNC(name, formula) \
static void name##Func(DataChunk &args, ExpressionState &, Vector &result) { \
	UnifiedVectorFormat hf, lf, cf; \
	args.data[0].ToUnifiedFormat(args.size(), hf); \
	args.data[1].ToUnifiedFormat(args.size(), lf); \
	args.data[2].ToUnifiedFormat(args.size(), cf); \
	auto hd = UnifiedVectorFormat::GetData<double>(hf); \
	auto ld = UnifiedVectorFormat::GetData<double>(lf); \
	auto cd = UnifiedVectorFormat::GetData<double>(cf); \
	auto rd = FlatVector::GetData<double>(result); \
	auto &rv = FlatVector::Validity(result); \
	result.SetVectorType(VectorType::FLAT_VECTOR); \
	for (idx_t i = 0; i < args.size(); i++) { \
		auto hi = hf.sel->get_index(i); auto li = lf.sel->get_index(i); auto ci = cf.sel->get_index(i); \
		if (!hf.validity.RowIsValid(hi) || !lf.validity.RowIsValid(li) || !cf.validity.RowIsValid(ci)) { \
			rv.SetInvalid(i); continue; \
		} \
		double H = hd[hi], L = ld[li], C = cd[ci]; \
		double PP = (H + L + C) / 3.0; \
		rd[i] = formula; \
	} \
}

PIVOT_LEVEL_FUNC(PivotR1, 2.0 * PP - L)
PIVOT_LEVEL_FUNC(PivotR2, PP + (H - L))
PIVOT_LEVEL_FUNC(PivotR3, H + 2.0 * (PP - L))
PIVOT_LEVEL_FUNC(PivotS1, 2.0 * PP - H)
PIVOT_LEVEL_FUNC(PivotS2, PP - (H - L))
PIVOT_LEVEL_FUNC(PivotS3, L - 2.0 * (H - PP))

void RegisterWMAAndPivotFunctions(Connection &conn, Catalog &catalog) {
	// WMA aggregate
	AggregateFunctionSet wma_set("wma");
	wma_set.AddFunction(AggregateFunction(
	    "wma", {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ, LogicalType::INTEGER},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<WMAState>,
	    WMAInitialize, WMAUpdate, WMACombine, WMAFinalize, nullptr, nullptr, WMADestructor));
	wma_set.AddFunction(AggregateFunction(
	    "wma", {LogicalType::DOUBLE, LogicalType::TIMESTAMP, LogicalType::INTEGER},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<WMAState>,
	    WMAInitialize, WMAUpdate, WMACombine, WMAFinalize, nullptr, nullptr, WMADestructor));
	CreateAggregateFunctionInfo wma_info(wma_set);
	catalog.CreateFunction(*conn.context, wma_info);

	// Pivot Point scalars
	auto types3 = vector<LogicalType>{LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE};

	auto register_scalar = [&](const string &name, scalar_function_t func) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction(name, types3, LogicalType::DOUBLE, func));
		CreateScalarFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	};

	register_scalar("pivot_point", PivotPointFunc);
	register_scalar("pivot_r1", PivotR1Func);
	register_scalar("pivot_r2", PivotR2Func);
	register_scalar("pivot_r3", PivotR3Func);
	register_scalar("pivot_s1", PivotS1Func);
	register_scalar("pivot_s2", PivotS2Func);
	register_scalar("pivot_s3", PivotS3Func);
}

} // namespace scrooge
} // namespace duckdb
