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
// ADX / DMI (Directional Movement Index)
//
//   adx(high, low, close, ts [, period])
//   plus_di(high, low, close, ts [, period])
//   minus_di(high, low, close, ts [, period])
//
// All use Wilder's smoothing over `period` (default 14). ADX is the
// smoothed average of |+DI - -DI| / (+DI + -DI) * 100. Values
// roughly: < 20 weak trend; > 25 strong trend.
// ──────────────────────────────────────────────────────────────

namespace {

enum class AdxOutput { ADX, PLUS_DI, MINUS_DI };

struct AdxFunctionData : public FunctionData {
	int32_t period;
	AdxOutput output;
	AdxFunctionData(int32_t p, AdxOutput o) : period(p), output(o) {}
	unique_ptr<FunctionData> Copy() const override { return make_uniq<AdxFunctionData>(period, output); }
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<AdxFunctionData>();
		return period == o.period && output == o.output;
	}
};

struct AdxState {
	struct Entry {
		double high, low, close;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void AdxInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<AdxState *>(state_p);
	state.entries = nullptr;
}

static void AdxUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
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
	auto states = (AdxState **)sd.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sd.sel->get_index(i)];
		if (!state.entries) {
			state.entries = new std::vector<AdxState::Entry>();
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

static void AdxCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sd, td;
	source_vec.ToUnifiedFormat(count, sd);
	target_vec.ToUnifiedFormat(count, td);
	auto sources = (AdxState **)sd.data;
	auto targets = (AdxState **)td.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[sd.sel->get_index(i)];
		auto &tgt = *targets[td.sel->get_index(i)];
		if (src.entries) {
			if (!tgt.entries) {
				tgt.entries = new std::vector<AdxState::Entry>();
			}
			tgt.entries->insert(tgt.entries->end(), src.entries->begin(), src.entries->end());
			delete src.entries;
			src.entries = nullptr;
		}
	}
}

static void AdxDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sd;
	state_vector.ToUnifiedFormat(count, sd);
	auto states = (AdxState **)sd.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sd.sel->get_index(i)];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

struct AdxRollup {
	double adx;
	double plus_di;
	double minus_di;
	bool valid;
};

static AdxRollup ComputeAdx(std::vector<AdxState::Entry> &entries, int32_t period) {
	std::sort(entries.begin(), entries.end(),
	          [](const AdxState::Entry &a, const AdxState::Entry &b) { return a.ts < b.ts; });
	idx_t n = entries.size();
	if (n < (idx_t)(2 * period + 1)) {
		return {0, 0, 0, false};
	}
	std::vector<double> tr(n - 1), pdm(n - 1), mdm(n - 1);
	for (idx_t i = 1; i < n; i++) {
		double up = entries[i].high - entries[i - 1].high;
		double dn = entries[i - 1].low - entries[i].low;
		pdm[i - 1] = (up > dn && up > 0) ? up : 0.0;
		mdm[i - 1] = (dn > up && dn > 0) ? dn : 0.0;
		double hl = entries[i].high - entries[i].low;
		double hpc = std::fabs(entries[i].high - entries[i - 1].close);
		double lpc = std::fabs(entries[i].low - entries[i - 1].close);
		tr[i - 1] = std::max({hl, hpc, lpc});
	}
	// Initial Wilder smoothing: sum first `period` values.
	double atr = 0, sp = 0, sm = 0;
	for (int32_t k = 0; k < period; k++) {
		atr += tr[k];
		sp += pdm[k];
		sm += mdm[k];
	}
	std::vector<double> dx;
	dx.reserve(n - 1 - period);
	auto push_dx = [&]() {
		if (atr <= 0) {
			return;
		}
		double pdi = 100.0 * sp / atr;
		double mdi = 100.0 * sm / atr;
		double sum = pdi + mdi;
		if (sum > 0) {
			dx.push_back(100.0 * std::fabs(pdi - mdi) / sum);
		} else {
			dx.push_back(0.0);
		}
	};
	push_dx();
	for (idx_t k = period; k < tr.size(); k++) {
		atr = atr - atr / period + tr[k];
		sp = sp - sp / period + pdm[k];
		sm = sm - sm / period + mdm[k];
		push_dx();
	}
	if (dx.size() < (size_t)period) {
		return {0, 0, 0, false};
	}
	double adx = 0;
	for (int32_t k = 0; k < period; k++) {
		adx += dx[k];
	}
	adx /= period;
	for (idx_t k = period; k < dx.size(); k++) {
		adx = (adx * (period - 1) + dx[k]) / period;
	}
	double final_pdi = atr > 0 ? 100.0 * sp / atr : 0;
	double final_mdi = atr > 0 ? 100.0 * sm / atr : 0;
	return {adx, final_pdi, final_mdi, true};
}

static void AdxFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sd;
	state_vector.ToUnifiedFormat(count, sd);
	auto states = (AdxState **)sd.data;
	auto out = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	auto &bind = aggr_input.bind_data->Cast<AdxFunctionData>();
	int32_t period = bind.period > 0 ? bind.period : 14;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sd.sel->get_index(i)];
		auto ridx = i + offset;
		if (!state.entries || state.entries->size() < (size_t)(2 * period + 1)) {
			validity.SetInvalid(ridx);
			if (state.entries) {
				delete state.entries;
				state.entries = nullptr;
			}
			continue;
		}
		auto rs = ComputeAdx(*state.entries, period);
		if (!rs.valid) {
			validity.SetInvalid(ridx);
		} else {
			switch (bind.output) {
			case AdxOutput::ADX:
				out[ridx] = rs.adx;
				break;
			case AdxOutput::PLUS_DI:
				out[ridx] = rs.plus_di;
				break;
			case AdxOutput::MINUS_DI:
				out[ridx] = rs.minus_di;
				break;
			}
		}
		delete state.entries;
		state.entries = nullptr;
	}
}

template <AdxOutput OUT>
static unique_ptr<FunctionData> AdxBind(ClientContext &context, AggregateFunction &,
                                          vector<unique_ptr<Expression>> &arguments) {
	int32_t period = 14;
	if (arguments.size() >= 5 && arguments[4]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[4]);
		if (!val.IsNull()) period = val.GetValue<int32_t>();
	}
	return make_uniq<AdxFunctionData>(period, OUT);
}

template <AdxOutput OUT>
static void RegisterOne(Connection &conn, Catalog &catalog, const string &name) {
	AggregateFunctionSet set(name);
	auto add = [&](vector<LogicalType> sig) {
		set.AddFunction(AggregateFunction(
		    name, sig, LogicalType::DOUBLE,
		    AggregateFunction::StateSize<AdxState>,
		    AdxInit, AdxUpdate, AdxCombine, AdxFinalize, nullptr,
		    AdxBind<OUT>, AdxDestructor));
	};
	add({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ});
	add({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ, LogicalType::INTEGER});
	add({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP});
	add({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP, LogicalType::INTEGER});
	CreateAggregateFunctionInfo info(set);
	catalog.CreateFunction(*conn.context, info);
}

} // namespace

void RegisterADX(Connection &conn, Catalog &catalog) {
	RegisterOne<AdxOutput::ADX>(conn, catalog, "adx");
	RegisterOne<AdxOutput::PLUS_DI>(conn, catalog, "plus_di");
	RegisterOne<AdxOutput::MINUS_DI>(conn, catalog, "minus_di");
}

} // namespace scrooge
} // namespace duckdb
