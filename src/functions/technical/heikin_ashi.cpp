#include "functions/technical.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/helper.hpp"

#include <algorithm>
#include <vector>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// Heikin-Ashi candles — derived OHLC for trend smoothing.
//
//   ha_open(o, h, l, c, ts)
//   ha_high(o, h, l, c, ts)
//   ha_low(o, h, l, c, ts)
//   ha_close(o, h, l, c, ts)
//
// Each component returns the value of the *last* HA candle in the
// group; combine with `OVER (ORDER BY ts)` to produce a series.
//
// Recurrence:
//   HA_close = (O + H + L + C) / 4
//   HA_open  = (prev HA_open + prev HA_close) / 2,    seed = (O + C) / 2
//   HA_high  = max(H, HA_open, HA_close)
//   HA_low   = min(L, HA_open, HA_close)
// ──────────────────────────────────────────────────────────────

namespace {

enum class HAField { OPEN, HIGH, LOW, CLOSE };

struct HAFunctionData : public FunctionData {
	HAField field;
	explicit HAFunctionData(HAField f) : field(f) {}
	unique_ptr<FunctionData> Copy() const override { return make_uniq<HAFunctionData>(field); }
	bool Equals(const FunctionData &other) const override { return field == other.Cast<HAFunctionData>().field; }
};

struct HAState {
	struct Entry {
		double o, h, l, c;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void HAInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<HAState *>(state_p);
	state.entries = nullptr;
}

static void HAUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat od, hd, ld, cd, td, sd;
	inputs[0].ToUnifiedFormat(count, od);
	inputs[1].ToUnifiedFormat(count, hd);
	inputs[2].ToUnifiedFormat(count, ld);
	inputs[3].ToUnifiedFormat(count, cd);
	inputs[4].ToUnifiedFormat(count, td);
	state_vector.ToUnifiedFormat(count, sd);
	auto opens = UnifiedVectorFormat::GetData<double>(od);
	auto highs = UnifiedVectorFormat::GetData<double>(hd);
	auto lows = UnifiedVectorFormat::GetData<double>(ld);
	auto closes = UnifiedVectorFormat::GetData<double>(cd);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(td);
	auto states = (HAState **)sd.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sd.sel->get_index(i)];
		if (!state.entries) {
			state.entries = new std::vector<HAState::Entry>();
		}
		auto oi = od.sel->get_index(i);
		auto hi = hd.sel->get_index(i);
		auto li = ld.sel->get_index(i);
		auto ci = cd.sel->get_index(i);
		auto ti = td.sel->get_index(i);
		if (od.validity.RowIsValid(oi) && hd.validity.RowIsValid(hi) &&
		    ld.validity.RowIsValid(li) && cd.validity.RowIsValid(ci) &&
		    td.validity.RowIsValid(ti)) {
			state.entries->push_back({opens[oi], highs[hi], lows[li], closes[ci], timestamps[ti]});
		}
	}
}

static void HACombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sd, td;
	source_vec.ToUnifiedFormat(count, sd);
	target_vec.ToUnifiedFormat(count, td);
	auto sources = (HAState **)sd.data;
	auto targets = (HAState **)td.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[sd.sel->get_index(i)];
		auto &tgt = *targets[td.sel->get_index(i)];
		if (src.entries) {
			if (!tgt.entries) {
				tgt.entries = new std::vector<HAState::Entry>();
			}
			tgt.entries->insert(tgt.entries->end(), src.entries->begin(), src.entries->end());
			delete src.entries;
			src.entries = nullptr;
		}
	}
}

static void HADestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sd;
	state_vector.ToUnifiedFormat(count, sd);
	auto states = (HAState **)sd.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sd.sel->get_index(i)];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

static void HAFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sd;
	state_vector.ToUnifiedFormat(count, sd);
	auto states = (HAState **)sd.data;
	auto out = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	auto &bind = aggr_input.bind_data->Cast<HAFunctionData>();
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
		          [](const HAState::Entry &a, const HAState::Entry &b) { return a.ts < b.ts; });

		double prev_open = (entries[0].o + entries[0].c) / 2.0;
		double prev_close = (entries[0].o + entries[0].h + entries[0].l + entries[0].c) / 4.0;
		double ha_open = prev_open;
		double ha_close = prev_close;
		double ha_high = std::max({entries[0].h, ha_open, ha_close});
		double ha_low = std::min({entries[0].l, ha_open, ha_close});
		for (idx_t j = 1; j < entries.size(); j++) {
			ha_close = (entries[j].o + entries[j].h + entries[j].l + entries[j].c) / 4.0;
			ha_open = (prev_open + prev_close) / 2.0;
			ha_high = std::max({entries[j].h, ha_open, ha_close});
			ha_low = std::min({entries[j].l, ha_open, ha_close});
			prev_open = ha_open;
			prev_close = ha_close;
		}
		switch (bind.field) {
		case HAField::OPEN:
			out[ridx] = ha_open;
			break;
		case HAField::HIGH:
			out[ridx] = ha_high;
			break;
		case HAField::LOW:
			out[ridx] = ha_low;
			break;
		case HAField::CLOSE:
			out[ridx] = ha_close;
			break;
		}
		delete state.entries;
		state.entries = nullptr;
	}
}

template <HAField F>
static unique_ptr<FunctionData> HABind(ClientContext &, AggregateFunction &,
                                          vector<unique_ptr<Expression>> &) {
	return make_uniq<HAFunctionData>(F);
}

template <HAField F>
static void RegisterOne(Connection &conn, Catalog &catalog, const string &name) {
	AggregateFunctionSet set(name);
	auto add = [&](LogicalType ts_type) {
		set.AddFunction(AggregateFunction(
		    name,
		    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, ts_type},
		    LogicalType::DOUBLE, AggregateFunction::StateSize<HAState>,
		    HAInit, HAUpdate, HACombine, HAFinalize, nullptr, HABind<F>, HADestructor));
	};
	add(LogicalType::TIMESTAMP_TZ);
	add(LogicalType::TIMESTAMP);
	CreateAggregateFunctionInfo info(set);
	catalog.CreateFunction(*conn.context, info);
}

} // namespace

void RegisterHeikinAshi(Connection &conn, Catalog &catalog) {
	RegisterOne<HAField::OPEN>(conn, catalog, "ha_open");
	RegisterOne<HAField::HIGH>(conn, catalog, "ha_high");
	RegisterOne<HAField::LOW>(conn, catalog, "ha_low");
	RegisterOne<HAField::CLOSE>(conn, catalog, "ha_close");
}

} // namespace scrooge
} // namespace duckdb
