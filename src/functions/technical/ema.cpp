#include "functions/technical.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// EMA (Exponential Moving Average) — ordered aggregate
//
// Usage:  ema(price, period ORDER BY timestamp)
//
// The EMA is computed as:
//   multiplier = 2.0 / (period + 1)
//   ema[0] = price[0]
//   ema[i] = price[i] * multiplier + ema[i-1] * (1 - multiplier)
//
// Since DuckDB aggregates don't guarantee order within Update(),
// we collect all (value, timestamp) pairs in a list, sort in
// Finalize, and compute the EMA there. This is correct for any
// parallelism level.
//
// For a streaming/window version, use ema_w() (future).
// ──────────────────────────────────────────────────────────────

struct EmaState {
	struct Entry {
		double value;
		int64_t timestamp;
	};
	std::vector<Entry> entries;
	int32_t period;
	bool period_set;
};

struct EmaOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.period = 0;
		state.period_set = false;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		state.entries.~vector();
	}

	static void Operation(EmaState &state, const double &value,
	                       const int64_t &timestamp, const int32_t &period) {
		if (!state.period_set) {
			state.period = period;
			state.period_set = true;
		}
		state.entries.push_back({value, timestamp});
	}

	static void Combine(const EmaState &source, EmaState &target,
	                     AggregateInputData &) {
		if (!target.period_set && source.period_set) {
			target.period = source.period;
			target.period_set = true;
		}
		target.entries.insert(target.entries.end(), source.entries.begin(),
		                      source.entries.end());
	}

	static void Finalize(EmaState &state, double &target,
	                      AggregateFinalizeData &finalize_data) {
		if (state.entries.empty()) {
			finalize_data.ReturnNull();
			return;
		}
		// Sort by timestamp
		std::sort(state.entries.begin(), state.entries.end(),
		          [](const EmaState::Entry &a, const EmaState::Entry &b) {
			          return a.timestamp < b.timestamp;
		          });

		int32_t period = state.period > 0 ? state.period : 1;
		double multiplier = 2.0 / (period + 1);
		double ema = state.entries[0].value;
		for (size_t i = 1; i < state.entries.size(); i++) {
			ema = state.entries[i].value * multiplier + ema * (1.0 - multiplier);
		}
		target = ema;
	}
};

// A simpler approach: use a regular aggregate with custom update
// This requires manual handling of the function binding
struct EmaFunctionData : public FunctionData {
	int32_t period;

	explicit EmaFunctionData(int32_t period_p) : period(period_p) {}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<EmaFunctionData>(period);
	}
	bool Equals(const FunctionData &other) const override {
		return period == other.Cast<EmaFunctionData>().period;
	}
};

struct EmaListState {
	struct Entry {
		double value;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void EmaUpdate(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count, Vector &state_vector,
                       idx_t count) {
	auto &value_vec = inputs[0];
	auto &ts_vec = inputs[1];
	auto &period_vec = inputs[2];

	UnifiedVectorFormat value_data, ts_data, period_data, sdata;
	value_vec.ToUnifiedFormat(count, value_data);
	ts_vec.ToUnifiedFormat(count, ts_data);
	period_vec.ToUnifiedFormat(count, period_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto values = UnifiedVectorFormat::GetData<double>(value_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto periods = UnifiedVectorFormat::GetData<int32_t>(period_data);
	auto states = (EmaListState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];

		if (!state.entries) {
			state.entries = new std::vector<EmaListState::Entry>();
		}

		auto vidx = value_data.sel->get_index(i);
		auto tidx = ts_data.sel->get_index(i);

		if (value_data.validity.RowIsValid(vidx) && ts_data.validity.RowIsValid(tidx)) {
			state.entries->push_back({values[vidx], timestamps[tidx]});
		}
	}
}

static void EmaInitialize(AggregateInputData &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<EmaListState *>(state_p);
	state.entries = nullptr;
}

static void EmaCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);

	auto sources = (EmaListState **)src_data.data;
	auto targets = (EmaListState **)tgt_data.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = src_data.sel->get_index(i);
		auto tidx = tgt_data.sel->get_index(i);
		auto &source = *sources[sidx];
		auto &target = *targets[tidx];

		if (source.entries) {
			if (!target.entries) {
				target.entries = new std::vector<EmaListState::Entry>();
			}
			target.entries->insert(target.entries->end(), source.entries->begin(), source.entries->end());
			delete source.entries;
			source.entries = nullptr;
		}
	}
}

static void EmaFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count,
                         idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (EmaListState **)sdata.data;

	auto &bind_data = aggr_input.bind_data->Cast<EmaFunctionData>();
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

		// Sort by timestamp
		auto &entries = *state.entries;
		std::sort(entries.begin(), entries.end(),
		          [](const EmaListState::Entry &a, const EmaListState::Entry &b) {
			          return a.ts < b.ts;
		          });

		int32_t period = bind_data.period > 0 ? bind_data.period : 1;
		double multiplier = 2.0 / (period + 1);
		double ema = entries[0].value;
		for (size_t j = 1; j < entries.size(); j++) {
			ema = entries[j].value * multiplier + ema * (1.0 - multiplier);
		}
		result_data[ridx] = ema;

		delete state.entries;
		state.entries = nullptr;
	}
}

static void EmaDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (EmaListState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

unique_ptr<FunctionData> EmaBind(ClientContext &context, AggregateFunction &function,
                                  vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 3) {
		throw BinderException("ema requires 3 arguments: ema(value, timestamp, period)");
	}
	// Try to extract period as constant
	if (arguments[2]->IsFoldable()) {
		auto period_val = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
		if (!period_val.IsNull()) {
			return make_uniq<EmaFunctionData>(period_val.GetValue<int32_t>());
		}
	}
	return make_uniq<EmaFunctionData>(12); // Default period
}

void RegisterEmaFunction(Connection &conn, Catalog &catalog) {
	AggregateFunction ema_func(
	    "ema",
	    {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ, LogicalType::INTEGER},
	    LogicalType::DOUBLE,
	    AggregateFunction::StateSize<EmaListState>,
	    EmaInitialize,
	    EmaUpdate,
	    EmaCombine,
	    EmaFinalize,
	    nullptr, // simple_update
	    EmaBind,
	    EmaDestructor);

	CreateAggregateFunctionInfo ema_info(ema_func);
	catalog.CreateFunction(*conn.context, ema_info);
}

} // namespace scrooge
} // namespace duckdb
