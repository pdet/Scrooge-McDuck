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
// Usage:  value_at_risk(returns [, confidence_level])
//
// Returns the loss threshold at the given confidence level.
// Default confidence: 0.95 (95%)
// Uses the historical/percentile method: sort returns, pick the
// (1-confidence) quantile.
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
	std::vector<double> *returns_list;
};

static void VaRInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<VaRListState *>(state_p);
	state.returns_list = nullptr;
}

static void VaRUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat ret_data, sdata;
	inputs[0].ToUnifiedFormat(count, ret_data);
	state_vector.ToUnifiedFormat(count, sdata);

	auto returns = UnifiedVectorFormat::GetData<double>(ret_data);
	auto states = (VaRListState **)sdata.data;

	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (!state.returns_list) {
			state.returns_list = new std::vector<double>();
		}
		auto ridx = ret_data.sel->get_index(i);
		if (ret_data.validity.RowIsValid(ridx)) {
			state.returns_list->push_back(returns[ridx]);
		}
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
		if (source.returns_list) {
			if (!target.returns_list) {
				target.returns_list = new std::vector<double>();
			}
			target.returns_list->insert(target.returns_list->end(), source.returns_list->begin(),
			                            source.returns_list->end());
			delete source.returns_list;
			source.returns_list = nullptr;
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

		if (!state.returns_list || state.returns_list->empty()) {
			result_validity.SetInvalid(ridx);
			continue;
		}

		auto &rets = *state.returns_list;
		std::sort(rets.begin(), rets.end());

		// VaR at confidence level: pick the (1-confidence) percentile
		double alpha = 1.0 - bind_data.confidence;
		idx_t var_idx = (idx_t)std::floor(alpha * rets.size());
		if (var_idx >= rets.size()) var_idx = rets.size() - 1;

		// VaR is typically reported as a positive number (loss)
		result_data[ridx] = -rets[var_idx];

		delete state.returns_list;
		state.returns_list = nullptr;
	}
}

static void VaRDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (VaRListState **)sdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sdata.sel->get_index(i);
		auto &state = *states[sidx];
		if (state.returns_list) {
			delete state.returns_list;
			state.returns_list = nullptr;
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

	// value_at_risk(returns) — default 95%
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
