#include "functions/technical.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// VWAP (Volume Weighted Average Price) — aggregate
//
// Usage:  vwap(price, volume)
//
// VWAP = SUM(price * volume) / SUM(volume)
//
// This is a simple aggregate — no ordering needed.
// ──────────────────────────────────────────────────────────────

struct VwapState {
	double sum_pv;   // sum of price * volume
	double sum_vol;  // sum of volume
	bool executed;
};

struct VwapOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.sum_pv = 0.0;
		state.sum_vol = 0.0;
		state.executed = false;
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &price, const B_TYPE &volume, AggregateBinaryInput &) {
		state.sum_pv += static_cast<double>(price) * static_cast<double>(volume);
		state.sum_vol += static_cast<double>(volume);
		state.executed = true;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.executed) {
			target.sum_pv += source.sum_pv;
			target.sum_vol += source.sum_vol;
			target.executed = true;
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.executed || state.sum_vol == 0.0) {
			finalize_data.ReturnNull();
		} else {
			target = state.sum_pv / state.sum_vol;
		}
	}

	static bool IgnoreNull() { return true; }
};

void RegisterVwapFunction(Connection &conn, Catalog &catalog) {
	AggregateFunctionSet vwap_set("vwap");

	// vwap(price, volume) — DOUBLE, DOUBLE
	vwap_set.AddFunction(AggregateFunction::BinaryAggregate<VwapState, double, double, double, VwapOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));

	// vwap(price, volume) — DOUBLE, BIGINT
	vwap_set.AddFunction(AggregateFunction::BinaryAggregate<VwapState, double, int64_t, double, VwapOperation>(
	    LogicalType::DOUBLE, LogicalType::BIGINT, LogicalType::DOUBLE));

	// vwap(price, volume) — DOUBLE, INTEGER
	vwap_set.AddFunction(AggregateFunction::BinaryAggregate<VwapState, double, int32_t, double, VwapOperation>(
	    LogicalType::DOUBLE, LogicalType::INTEGER, LogicalType::DOUBLE));

	CreateAggregateFunctionInfo vwap_info(vwap_set);
	catalog.CreateFunction(*conn.context, vwap_info);
}

} // namespace scrooge
} // namespace duckdb
