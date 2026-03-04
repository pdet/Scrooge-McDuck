#include "functions/returns.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/helper.hpp"
#include <cmath>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// Simple Return: (current - previous) / previous
// Log Return: ln(current / previous)
// Cumulative Return: (final - initial) / initial
//
// simple_return(current, previous) → scalar
// log_return(current, previous)    → scalar
// cumulative_return(price, ts)     → aggregate (first to last)
// ──────────────────────────────────────────────────────────────

// ── simple_return scalar ─────────────────────────────────────

static void SimpleReturnFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &current_vec = args.data[0];
	auto &previous_vec = args.data[1];

	BinaryExecutor::Execute<double, double, double>(
	    current_vec, previous_vec, result, args.size(),
	    [](double current, double previous) -> double {
		    if (previous == 0.0) return 0.0;
		    return (current - previous) / previous;
	    });
}

// ── log_return scalar ────────────────────────────────────────

static void LogReturnFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &current_vec = args.data[0];
	auto &previous_vec = args.data[1];

	BinaryExecutor::Execute<double, double, double>(
	    current_vec, previous_vec, result, args.size(),
	    [](double current, double previous) -> double {
		    if (previous <= 0.0 || current <= 0.0) return 0.0;
		    return std::log(current / previous);
	    });
}

// ── cumulative_return aggregate ──────────────────────────────

struct CumulativeReturnState {
	double first_value;
	double last_value;
	int64_t first_ts;
	int64_t last_ts;
	bool executed;
};

struct CumulativeReturnOp {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.first_value = 0.0;
		state.last_value = 0.0;
		state.first_ts = NumericLimits<int64_t>::Maximum();
		state.last_ts = NumericLimits<int64_t>::Minimum();
		state.executed = false;
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &price, const B_TYPE &timestamp, AggregateBinaryInput &) {
		int64_t ts = static_cast<int64_t>(timestamp);
		if (!state.executed || ts < state.first_ts) {
			state.first_ts = ts;
			state.first_value = static_cast<double>(price);
		}
		if (!state.executed || ts > state.last_ts) {
			state.last_ts = ts;
			state.last_value = static_cast<double>(price);
		}
		state.executed = true;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.executed) return;
		if (!target.executed || source.first_ts < target.first_ts) {
			target.first_ts = source.first_ts;
			target.first_value = source.first_value;
		}
		if (!target.executed || source.last_ts > target.last_ts) {
			target.last_ts = source.last_ts;
			target.last_value = source.last_value;
		}
		target.executed = true;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.executed || state.first_value == 0.0) {
			finalize_data.ReturnNull();
		} else {
			target = (state.last_value - state.first_value) / state.first_value;
		}
	}

	static bool IgnoreNull() { return true; }
};

// ── Registration ─────────────────────────────────────────────

void RegisterReturnFunctions(Connection &conn, Catalog &catalog) {
	// simple_return(current, previous) → double
	{
		ScalarFunctionSet set("simple_return");
		set.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
		                                SimpleReturnFunction));
		CreateScalarFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	}

	// log_return(current, previous) → double
	{
		ScalarFunctionSet set("log_return");
		set.AddFunction(
		    ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE, LogReturnFunction));
		CreateScalarFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	}

	// cumulative_return(price, timestamp) → double
	{
		AggregateFunctionSet set("cumulative_return");
		set.AddFunction(
		    AggregateFunction::BinaryAggregate<CumulativeReturnState, double, int64_t, double, CumulativeReturnOp>(
		        LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ, LogicalType::DOUBLE));
		set.AddFunction(
		    AggregateFunction::BinaryAggregate<CumulativeReturnState, double, int64_t, double, CumulativeReturnOp>(
		        LogicalType::DOUBLE, LogicalType::TIMESTAMP, LogicalType::DOUBLE));
		CreateAggregateFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	}
}

} // namespace scrooge
} // namespace duckdb
