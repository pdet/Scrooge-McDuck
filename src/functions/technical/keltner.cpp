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
// Keltner Channels.
//
//   keltner_middle(high, low, close, ts [, period])
//   keltner_upper(high, low, close, ts [, period [, multiplier]])
//   keltner_lower(high, low, close, ts [, period [, multiplier]])
//
// Middle = EMA(close, period).  Upper/Lower = Middle ± multiplier * ATR.
// Defaults: period = 20, multiplier = 2.0.
// ──────────────────────────────────────────────────────────────

namespace {

enum class KCBand { MIDDLE, UPPER, LOWER };

struct KCFunctionData : public FunctionData {
	int32_t period;
	double multiplier;
	KCBand band;
	KCFunctionData(int32_t p, double m, KCBand b) : period(p), multiplier(m), band(b) {}
	unique_ptr<FunctionData> Copy() const override { return make_uniq<KCFunctionData>(period, multiplier, band); }
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<KCFunctionData>();
		return period == o.period && multiplier == o.multiplier && band == o.band;
	}
};

struct KCState {
	struct Entry {
		double h, l, c;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void KCInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<KCState *>(state_p);
	state.entries = nullptr;
}

static void KCUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
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
	auto states = (KCState **)sd.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sd.sel->get_index(i)];
		if (!state.entries) {
			state.entries = new std::vector<KCState::Entry>();
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

static void KCCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sd, td;
	source_vec.ToUnifiedFormat(count, sd);
	target_vec.ToUnifiedFormat(count, td);
	auto sources = (KCState **)sd.data;
	auto targets = (KCState **)td.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[sd.sel->get_index(i)];
		auto &tgt = *targets[td.sel->get_index(i)];
		if (src.entries) {
			if (!tgt.entries) {
				tgt.entries = new std::vector<KCState::Entry>();
			}
			tgt.entries->insert(tgt.entries->end(), src.entries->begin(), src.entries->end());
			delete src.entries;
			src.entries = nullptr;
		}
	}
}

static void KCDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sd;
	state_vector.ToUnifiedFormat(count, sd);
	auto states = (KCState **)sd.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sd.sel->get_index(i)];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

static double EMA(const std::vector<KCState::Entry> &entries, int32_t period) {
	double alpha = 2.0 / (double)(period + 1);
	double ema = entries[0].c;
	for (idx_t i = 1; i < entries.size(); i++) {
		ema = alpha * entries[i].c + (1.0 - alpha) * ema;
	}
	return ema;
}

static double ATR(const std::vector<KCState::Entry> &entries, int32_t period) {
	if (entries.size() < 2) {
		return 0.0;
	}
	std::vector<double> tr;
	tr.reserve(entries.size() - 1);
	for (idx_t i = 1; i < entries.size(); i++) {
		double hl = entries[i].h - entries[i].l;
		double hpc = std::fabs(entries[i].h - entries[i - 1].c);
		double lpc = std::fabs(entries[i].l - entries[i - 1].c);
		tr.push_back(std::max({hl, hpc, lpc}));
	}
	if ((int32_t)tr.size() < period) {
		double s = 0;
		for (auto v : tr) s += v;
		return s / (double)tr.size();
	}
	double atr = 0;
	for (int32_t i = 0; i < period; i++) atr += tr[i];
	atr /= period;
	for (idx_t i = period; i < tr.size(); i++) {
		atr = (atr * (period - 1) + tr[i]) / period;
	}
	return atr;
}

static void KCFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sd;
	state_vector.ToUnifiedFormat(count, sd);
	auto states = (KCState **)sd.data;
	auto out = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	auto &bind = aggr_input.bind_data->Cast<KCFunctionData>();
	int32_t period = bind.period > 0 ? bind.period : 20;
	double mult = bind.multiplier; // bind already supplies the default
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
		          [](const KCState::Entry &a, const KCState::Entry &b) { return a.ts < b.ts; });
		double mid = EMA(entries, period);
		double atr = ATR(entries, period);
		switch (bind.band) {
		case KCBand::MIDDLE:
			out[ridx] = mid;
			break;
		case KCBand::UPPER:
			out[ridx] = mid + mult * atr;
			break;
		case KCBand::LOWER:
			out[ridx] = mid - mult * atr;
			break;
		}
		delete state.entries;
		state.entries = nullptr;
	}
}

template <KCBand B>
static unique_ptr<FunctionData> KCBind(ClientContext &context, AggregateFunction &,
                                          vector<unique_ptr<Expression>> &arguments) {
	int32_t period = 20;
	double mult = 2.0;
	if (arguments.size() >= 5 && arguments[4]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[4]);
		if (!val.IsNull()) period = val.GetValue<int32_t>();
	}
	if (arguments.size() >= 6 && arguments[5]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[5]);
		if (!val.IsNull()) mult = val.GetValue<double>();
	}
	return make_uniq<KCFunctionData>(period, mult, B);
}

template <KCBand B>
static void RegisterOne(Connection &conn, Catalog &catalog, const string &name) {
	AggregateFunctionSet set(name);
	auto add_sig = [&](vector<LogicalType> sig) {
		set.AddFunction(AggregateFunction(
		    name, sig, LogicalType::DOUBLE, AggregateFunction::StateSize<KCState>,
		    KCInit, KCUpdate, KCCombine, KCFinalize, nullptr, KCBind<B>, KCDestructor));
	};
	for (auto ts_type : {LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP}) {
		add_sig({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, ts_type});
		add_sig({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, ts_type, LogicalType::INTEGER});
		add_sig({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, ts_type, LogicalType::INTEGER,
		         LogicalType::DOUBLE});
	}
	CreateAggregateFunctionInfo info(set);
	catalog.CreateFunction(*conn.context, info);
}

} // namespace

void RegisterKeltner(Connection &conn, Catalog &catalog) {
	RegisterOne<KCBand::MIDDLE>(conn, catalog, "keltner_middle");
	RegisterOne<KCBand::UPPER>(conn, catalog, "keltner_upper");
	RegisterOne<KCBand::LOWER>(conn, catalog, "keltner_lower");
}

} // namespace scrooge
} // namespace duckdb
