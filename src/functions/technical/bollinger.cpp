#include "functions/technical.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/helper.hpp"
#include <algorithm>
#include <cmath>
#include <vector>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// Bollinger Bands — ordered aggregate returning band width
//
// Upper Band = SMA(period) + num_std * stddev(period)
// Lower Band = SMA(period) - num_std * stddev(period)
// Band Width = (Upper - Lower) / Middle
//
// bollinger_upper(price, ts, period, num_std) → upper band value
// bollinger_lower(price, ts, period, num_std) → lower band value
// bollinger_width(price, ts, period, num_std) → bandwidth %
//
// All use the last `period` values (sorted by timestamp) to compute
// the SMA and standard deviation.
// ──────────────────────────────────────────────────────────────

struct BollingerFunctionData : public FunctionData {
	int32_t period;
	double num_std;
	enum class Band { UPPER, LOWER, WIDTH, MIDDLE } band;

	BollingerFunctionData(int32_t p, double s, Band b) : period(p), num_std(s), band(b) {}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<BollingerFunctionData>(period, num_std, band);
	}
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<BollingerFunctionData>();
		return period == o.period && num_std == o.num_std && band == o.band;
	}
};

struct BollingerListState {
	struct Entry {
		double value;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void BollingerInitialize(AggregateInputData &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<BollingerListState *>(state_p);
	state.entries = nullptr;
}

static void BollingerUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	auto &value_vec = inputs[0];
	auto &ts_vec = inputs[1];

	UnifiedVectorFormat value_data, ts_data, sdata;
	value_vec.ToUnifiedFormat(count, value_data);
	ts_vec.ToUnifiedFormat(count, ts_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto values = UnifiedVectorFormat::GetData<double>(value_data);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(ts_data);
	auto states = (BollingerListState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (!state.entries) {
			state.entries = new std::vector<BollingerListState::Entry>();
		}
		auto vidx = value_data.sel->get_index(i);
		auto tidx = ts_data.sel->get_index(i);
		if (value_data.validity.RowIsValid(vidx) && ts_data.validity.RowIsValid(tidx)) {
			state.entries->push_back({values[vidx], timestamps[tidx]});
		}
	}
}

static void BollingerCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (BollingerListState **)src_data.data;
	auto targets = (BollingerListState **)tgt_data.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = src_data.sel->get_index(i);
		auto tidx = tgt_data.sel->get_index(i);
		auto &source = *sources[sidx];
		auto &target = *targets[tidx];
		if (source.entries) {
			if (!target.entries) {
				target.entries = new std::vector<BollingerListState::Entry>();
			}
			target.entries->insert(target.entries->end(), source.entries->begin(), source.entries->end());
			delete source.entries;
			source.entries = nullptr;
		}
	}
}

static void BollingerFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count,
                               idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (BollingerListState **)sdata.data;
	auto &bind_data = aggr_input.bind_data->Cast<BollingerFunctionData>();
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

		auto &entries = *state.entries;
		std::sort(entries.begin(), entries.end(),
		          [](const BollingerListState::Entry &a, const BollingerListState::Entry &b) { return a.ts < b.ts; });

		int32_t period = bind_data.period;
		idx_t n = entries.size();
		idx_t window_start = (n > (idx_t)period) ? n - period : 0;

		// Compute SMA and stddev over the last `period` values
		double sum = 0.0;
		idx_t window_count = 0;
		for (idx_t j = window_start; j < n; j++) {
			sum += entries[j].value;
			window_count++;
		}
		double sma = sum / window_count;

		double var_sum = 0.0;
		for (idx_t j = window_start; j < n; j++) {
			double diff = entries[j].value - sma;
			var_sum += diff * diff;
		}
		double stddev = std::sqrt(var_sum / window_count);

		double upper = sma + bind_data.num_std * stddev;
		double lower = sma - bind_data.num_std * stddev;

		switch (bind_data.band) {
		case BollingerFunctionData::Band::UPPER:
			result_data[ridx] = upper;
			break;
		case BollingerFunctionData::Band::LOWER:
			result_data[ridx] = lower;
			break;
		case BollingerFunctionData::Band::MIDDLE:
			result_data[ridx] = sma;
			break;
		case BollingerFunctionData::Band::WIDTH:
			result_data[ridx] = sma > 0 ? (upper - lower) / sma : 0.0;
			break;
		}

		delete state.entries;
		state.entries = nullptr;
	}
}

static void BollingerDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (BollingerListState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

static unique_ptr<FunctionData> BollingerBindFactory(ClientContext &context, AggregateFunction &,
                                                      vector<unique_ptr<Expression>> &arguments,
                                                      BollingerFunctionData::Band band) {
	int32_t period = 20;
	double num_std = 2.0;
	if (arguments.size() >= 3 && arguments[2]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
		if (!val.IsNull()) period = val.GetValue<int32_t>();
	}
	if (arguments.size() >= 4 && arguments[3]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[3]);
		if (!val.IsNull()) num_std = val.GetValue<double>();
	}
	return make_uniq<BollingerFunctionData>(period, num_std, band);
}

static void RegisterBollingerVariant(const string &name, BollingerFunctionData::Band band, Connection &conn,
                                      Catalog &catalog) {
	AggregateFunctionSet func_set(name);

	auto bind_fn = [band](ClientContext &ctx, AggregateFunction &func, vector<unique_ptr<Expression>> &args) {
		return BollingerBindFactory(ctx, func, args, band);
	};

	// (price, timestamp) — defaults
	func_set.AddFunction(AggregateFunction(name, {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ}, LogicalType::DOUBLE,
	                                       AggregateFunction::StateSize<BollingerListState>, BollingerInitialize,
	                                       BollingerUpdate, BollingerCombine, BollingerFinalize, nullptr, bind_fn,
	                                       BollingerDestructor));

	// (price, timestamp, period)
	func_set.AddFunction(
	    AggregateFunction(name, {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ, LogicalType::INTEGER},
	                      LogicalType::DOUBLE, AggregateFunction::StateSize<BollingerListState>, BollingerInitialize,
	                      BollingerUpdate, BollingerCombine, BollingerFinalize, nullptr, bind_fn, BollingerDestructor));

	// (price, timestamp, period, num_std)
	func_set.AddFunction(AggregateFunction(
	    name, {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ, LogicalType::INTEGER, LogicalType::DOUBLE},
	    LogicalType::DOUBLE, AggregateFunction::StateSize<BollingerListState>, BollingerInitialize, BollingerUpdate,
	    BollingerCombine, BollingerFinalize, nullptr, bind_fn, BollingerDestructor));

	// TIMESTAMP (non-TZ) variant
	func_set.AddFunction(AggregateFunction(name, {LogicalType::DOUBLE, LogicalType::TIMESTAMP}, LogicalType::DOUBLE,
	                                       AggregateFunction::StateSize<BollingerListState>, BollingerInitialize,
	                                       BollingerUpdate, BollingerCombine, BollingerFinalize, nullptr, bind_fn,
	                                       BollingerDestructor));

	CreateAggregateFunctionInfo info(func_set);
	catalog.CreateFunction(*conn.context, info);
}

void RegisterBollingerFunctions(Connection &conn, Catalog &catalog) {
	RegisterBollingerVariant("bollinger_upper", BollingerFunctionData::Band::UPPER, conn, catalog);
	RegisterBollingerVariant("bollinger_lower", BollingerFunctionData::Band::LOWER, conn, catalog);
	RegisterBollingerVariant("bollinger_middle", BollingerFunctionData::Band::MIDDLE, conn, catalog);
	RegisterBollingerVariant("bollinger_width", BollingerFunctionData::Band::WIDTH, conn, catalog);
}

} // namespace scrooge
} // namespace duckdb
