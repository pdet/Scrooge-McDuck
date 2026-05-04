#include "functions/technical.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/helper.hpp"

#include <algorithm>
#include <vector>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// Ichimoku Kinko Hyo — five derived lines.
//
//   ichimoku_tenkan(high, low, ts)            (default 9-period midpoint)
//   ichimoku_kijun(high, low, ts)             (default 26-period midpoint)
//   ichimoku_senkou_a(high, low, ts)          (avg of tenkan + kijun, no shift)
//   ichimoku_senkou_b(high, low, ts)          (default 52-period midpoint)
//
// Senkou A/B are projected 26 bars forward in classical use; the
// shift is left to the caller (use a window LAG/LEAD if desired).
// Chikou (close shifted -26) is similarly omitted; LAG handles it.
// ──────────────────────────────────────────────────────────────

namespace {

enum class IchiKind { TENKAN, KIJUN, SENKOU_A, SENKOU_B };

struct IchiState {
	struct Entry {
		double h, l;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

struct IchiFunctionData : public FunctionData {
	IchiKind kind;
	explicit IchiFunctionData(IchiKind k) : kind(k) {}
	unique_ptr<FunctionData> Copy() const override { return make_uniq<IchiFunctionData>(kind); }
	bool Equals(const FunctionData &other) const override { return kind == other.Cast<IchiFunctionData>().kind; }
};

static void IchiInit(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<IchiState *>(state_p);
	state.entries = nullptr;
}

static void IchiUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat hd, ld, td, sd;
	inputs[0].ToUnifiedFormat(count, hd);
	inputs[1].ToUnifiedFormat(count, ld);
	inputs[2].ToUnifiedFormat(count, td);
	state_vector.ToUnifiedFormat(count, sd);
	auto highs = UnifiedVectorFormat::GetData<double>(hd);
	auto lows = UnifiedVectorFormat::GetData<double>(ld);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(td);
	auto states = (IchiState **)sd.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sd.sel->get_index(i)];
		if (!state.entries) {
			state.entries = new std::vector<IchiState::Entry>();
		}
		auto hi = hd.sel->get_index(i);
		auto li = ld.sel->get_index(i);
		auto ti = td.sel->get_index(i);
		if (hd.validity.RowIsValid(hi) && ld.validity.RowIsValid(li) && td.validity.RowIsValid(ti)) {
			state.entries->push_back({highs[hi], lows[li], timestamps[ti]});
		}
	}
}

static void IchiCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sd, td;
	source_vec.ToUnifiedFormat(count, sd);
	target_vec.ToUnifiedFormat(count, td);
	auto sources = (IchiState **)sd.data;
	auto targets = (IchiState **)td.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[sd.sel->get_index(i)];
		auto &tgt = *targets[td.sel->get_index(i)];
		if (src.entries) {
			if (!tgt.entries) {
				tgt.entries = new std::vector<IchiState::Entry>();
			}
			tgt.entries->insert(tgt.entries->end(), src.entries->begin(), src.entries->end());
			delete src.entries;
			src.entries = nullptr;
		}
	}
}

static void IchiDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sd;
	state_vector.ToUnifiedFormat(count, sd);
	auto states = (IchiState **)sd.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sd.sel->get_index(i)];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

static double Midpoint(const std::vector<IchiState::Entry> &entries, idx_t period) {
	idx_t n = entries.size();
	idx_t start = n > period ? n - period : 0;
	double hh = entries[start].h;
	double ll = entries[start].l;
	for (idx_t j = start + 1; j < n; j++) {
		if (entries[j].h > hh) hh = entries[j].h;
		if (entries[j].l < ll) ll = entries[j].l;
	}
	return (hh + ll) / 2.0;
}

static void IchiFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sd;
	state_vector.ToUnifiedFormat(count, sd);
	auto states = (IchiState **)sd.data;
	auto out = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	auto &bind = aggr_input.bind_data->Cast<IchiFunctionData>();
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
		          [](const IchiState::Entry &a, const IchiState::Entry &b) { return a.ts < b.ts; });
		switch (bind.kind) {
		case IchiKind::TENKAN:
			out[ridx] = Midpoint(entries, 9);
			break;
		case IchiKind::KIJUN:
			out[ridx] = Midpoint(entries, 26);
			break;
		case IchiKind::SENKOU_A:
			out[ridx] = (Midpoint(entries, 9) + Midpoint(entries, 26)) / 2.0;
			break;
		case IchiKind::SENKOU_B:
			out[ridx] = Midpoint(entries, 52);
			break;
		}
		delete state.entries;
		state.entries = nullptr;
	}
}

template <IchiKind K>
static unique_ptr<FunctionData> IchiBind(ClientContext &, AggregateFunction &,
                                            vector<unique_ptr<Expression>> &) {
	return make_uniq<IchiFunctionData>(K);
}

template <IchiKind K>
static void RegisterOne(Connection &conn, Catalog &catalog, const string &name) {
	AggregateFunctionSet set(name);
	auto add = [&](LogicalType ts_type) {
		set.AddFunction(AggregateFunction(
		    name, {LogicalType::DOUBLE, LogicalType::DOUBLE, ts_type},
		    LogicalType::DOUBLE, AggregateFunction::StateSize<IchiState>,
		    IchiInit, IchiUpdate, IchiCombine, IchiFinalize, nullptr, IchiBind<K>, IchiDestructor));
	};
	add(LogicalType::TIMESTAMP_TZ);
	add(LogicalType::TIMESTAMP);
	CreateAggregateFunctionInfo info(set);
	catalog.CreateFunction(*conn.context, info);
}

} // namespace

void RegisterIchimoku(Connection &conn, Catalog &catalog) {
	RegisterOne<IchiKind::TENKAN>(conn, catalog, "ichimoku_tenkan");
	RegisterOne<IchiKind::KIJUN>(conn, catalog, "ichimoku_kijun");
	RegisterOne<IchiKind::SENKOU_A>(conn, catalog, "ichimoku_senkou_a");
	RegisterOne<IchiKind::SENKOU_B>(conn, catalog, "ichimoku_senkou_b");
}

} // namespace scrooge
} // namespace duckdb
