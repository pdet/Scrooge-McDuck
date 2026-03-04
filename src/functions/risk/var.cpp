#include "functions/risk.hpp"
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
// VaR (Value at Risk) — Historical simulation method
//
// Usage:  var(returns [, confidence_level])
//
// Returns the loss threshold at the given confidence level.
// Default confidence: 0.95 (95%)
// VaR = negative of the (1 - confidence) percentile of returns
// ──────────────────────────────────────────────────────────────

struct VaRFunctionData : public FunctionData {
	double confidence;
	explicit VaRFunctionData(double c) : confidence(c) {}
	unique_ptr<FunctionData> Copy() const override { return make_uniq<VaRFunctionData>(confidence); }
	bool Equals(const FunctionData &other) const override {
		return confidence == other.Cast<VaRFunctionData>().confidence;
	}
};

struct VaRListState {
	std::vector<double> *returns;
};

static void VaRInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<VaRListState *>(state_p);
	state.returns = nullptr;
}

static void VaRUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat input_data, sdata;
	inputs[0].ToUnifiedFormat(count, input_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto values = UnifiedVectorFormat::GetData<double>(input_data);
	auto states = (VaRListState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto vidx = input_data.sel->get_index(i);
		auto &state = *states[sidx];
		if (!input_data.validity.RowIsValid(vidx)) continue;
		if (!state.returns) {
			state.returns = new std::vector<double>();
		}
		state.returns->push_back(values[vidx]);
	}
}

static void VaRCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat src_data, tgt_data;
	source_vec.ToUnifiedFormat(count, src_data);
	target_vec.ToUnifiedFormat(count, tgt_data);
	auto sources = (VaRListState **)src_data.data;
	auto targets = (VaRListState **)tgt_data.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = src_data.sel->get_index(i);
		auto tidx = tgt_data.sel->get_index(i);
		auto &source = *sources[sidx];
		auto &target = *targets[tidx];
		if (source.returns) {
			if (!target.returns) {
				target.returns = new std::vector<double>();
			}
			target.returns->insert(target.returns->end(), source.returns->begin(), source.returns->end());
			delete source.returns;
			source.returns = nullptr;
		}
	}
}

static void VaRFinalize(Vector &state_vector, AggregateInputData &aggr_input, Vector &result, idx_t count,
                         idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (VaRListState **)sdata.data;
	auto &bind_data = aggr_input.bind_data->Cast<VaRFunctionData>();
	auto result_data = FlatVector::GetData<double>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		auto ridx = i + offset;

		if (!state.returns || state.returns->size() < 2) {
			result_validity.SetInvalid(ridx);
			continue;
		}

		auto &returns = *state.returns;
		std::sort(returns.begin(), returns.end());

		double alpha = 1.0 - bind_data.confidence;
		// Index for the percentile
		double idx_d = alpha * (returns.size() - 1);
		idx_t lower = (idx_t)std::floor(idx_d);
		idx_t upper = (idx_t)std::ceil(idx_d);
		double frac = idx_d - lower;

		double percentile_val;
		if (lower == upper || upper >= returns.size()) {
			percentile_val = returns[lower];
		} else {
			percentile_val = returns[lower] * (1.0 - frac) + returns[upper] * frac;
		}

		// VaR is the negative of the percentile (loss is positive)
		result_data[ridx] = -percentile_val;

		delete state.returns;
		state.returns = nullptr;
	}
}

static void VaRDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (VaRListState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (state.returns) {
			delete state.returns;
			state.returns = nullptr;
		}
	}
}

static unique_ptr<FunctionData> VaRBind(ClientContext &context, AggregateFunction &,
                                         vector<unique_ptr<Expression>> &arguments) {
	double confidence = 0.95;
	if (arguments.size() >= 2 && arguments[1]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		if (!val.IsNull()) confidence = val.GetValue<double>();
	}
	return make_uniq<VaRFunctionData>(confidence);
}

void RegisterVaRFunction(Connection &conn, Catalog &catalog) {
	AggregateFunctionSet var_set("value_at_risk");

	// value_at_risk(returns) — default 95% confidence
	var_set.AddFunction(AggregateFunction("value_at_risk", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                                      AggregateFunction::StateSize<VaRListState>, VaRInitialize, VaRUpdate,
	                                      VaRCombine, VaRFinalize, nullptr, VaRBind, VaRDestructor));

	// value_at_risk(returns, confidence)
	var_set.AddFunction(AggregateFunction("value_at_risk", {LogicalType::DOUBLE, LogicalType::DOUBLE},
	                                      LogicalType::DOUBLE, AggregateFunction::StateSize<VaRListState>,
	                                      VaRInitialize, VaRUpdate, VaRCombine, VaRFinalize, nullptr, VaRBind,
	                                      VaRDestructor));

	CreateAggregateFunctionInfo var_info(var_set);
	catalog.CreateFunction(*conn.context, var_info);
}

} // namespace scrooge
} // namespace duckdb
