#include "functions/functions.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

namespace scrooge {

template <class T> struct FirstScroogeState {
  T first;
  int64_t earliest_time;
  bool executed;
};

struct FirstScroogeOperation {
  template <class STATE> static void Initialize(STATE &state) {
    state.earliest_time = duckdb::NumericLimits<int64_t>::Maximum();
    state.executed = false;
  }

  template <class A_TYPE, class B_TYPE, class STATE, class OP>
  static void
  Operation(STATE &state,
            const A_TYPE &x_data, const B_TYPE &y_data, duckdb::AggregateBinaryInput &idata) {
    if (!state.executed || y_data < state.earliest_time) {
      state.earliest_time = y_data;
      state.first = x_data;
      state.executed = true;
    }
  }

  template <class STATE, class OP>
  static void Combine(const STATE &source, STATE &target,
                      duckdb::AggregateInputData &aggr_input_data) {
    if (!target.executed) {
      target = source;
    } else if (source.executed) {
      if (target.earliest_time > source.earliest_time) {
        target.earliest_time = source.earliest_time;
        target.first = source.first;
      }
    }
  }

  template <class T, class STATE>
  static void Finalize(STATE &state, T &target, duckdb::AggregateFinalizeData &finalize_data) {
    if (!state.executed) {
      finalize_data.ReturnNull();
    } else {
      target = state.first;
    }
  }

  static bool IgnoreNull() { return true; }
};

duckdb::unique_ptr<duckdb::FunctionData> BindDoubleFirst(duckdb::ClientContext &context, duckdb::AggregateFunction &function,
                                                              duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments) {
  auto &decimal_type = arguments[0]->return_type;
  switch (decimal_type.InternalType()) {
  case duckdb::PhysicalType::INT16: {
    function =
        duckdb::AggregateFunction::BinaryAggregate<FirstScroogeState<int16_t>,
                                                   int16_t, int64_t, int16_t,
                                                   FirstScroogeOperation>(
            decimal_type, duckdb::LogicalType::TIMESTAMP_TZ, decimal_type);
    break;
  }
  case duckdb::PhysicalType::INT32: {
    function =
        duckdb::AggregateFunction::BinaryAggregate<FirstScroogeState<int32_t>,
                                                   int32_t, int64_t, int32_t,
                                                   FirstScroogeOperation>(
            decimal_type, duckdb::LogicalType::TIMESTAMP_TZ, decimal_type);
    break;
  }
  case duckdb::PhysicalType::INT64: {
    function =
        duckdb::AggregateFunction::BinaryAggregate<FirstScroogeState<int64_t>,
                                                   int64_t, int64_t, int64_t,
                                                   FirstScroogeOperation>(
            decimal_type, duckdb::LogicalType::TIMESTAMP_TZ, decimal_type);
    break;
  }
  default:
    function = duckdb::AggregateFunction::BinaryAggregate<
        FirstScroogeState<duckdb::Hugeint>, duckdb::Hugeint, int64_t,
        duckdb::Hugeint, FirstScroogeOperation>(
        decimal_type, duckdb::LogicalType::TIMESTAMP_TZ, decimal_type);
  }
  function.name = "first_s";
  return nullptr;
}

duckdb::AggregateFunction
GetFirstScroogeFunction(const duckdb::LogicalType &timestamp_type,
                        const duckdb::LogicalType &type) {
  switch (type.id()) {
  case duckdb::LogicalTypeId::SMALLINT:
    return duckdb::AggregateFunction::BinaryAggregate<
        FirstScroogeState<int16_t>, int16_t, int64_t, int16_t,
        FirstScroogeOperation>(type, timestamp_type, type);
  case duckdb::LogicalTypeId::TINYINT:
    return duckdb::AggregateFunction::BinaryAggregate<FirstScroogeState<int8_t>,
                                                      int8_t, int64_t, int8_t,
                                                      FirstScroogeOperation>(
        type, timestamp_type, type);
  case duckdb::LogicalTypeId::INTEGER:
    return duckdb::AggregateFunction::BinaryAggregate<
        FirstScroogeState<int32_t>, int32_t, int64_t, int32_t,
        FirstScroogeOperation>(type, timestamp_type, type);
  case duckdb::LogicalTypeId::BIGINT:
    return duckdb::AggregateFunction::BinaryAggregate<
        FirstScroogeState<int64_t>, int64_t, int64_t, int64_t,
        FirstScroogeOperation>(type, timestamp_type, type);
  case duckdb::LogicalTypeId::DECIMAL: {
    auto decimal_aggregate = duckdb::AggregateFunction::BinaryAggregate<
        FirstScroogeState<duckdb::hugeint_t>, duckdb::hugeint_t, int64_t,
        duckdb::hugeint_t, FirstScroogeOperation>(type, timestamp_type, type);
    decimal_aggregate.bind = BindDoubleFirst;
    return decimal_aggregate;
  }
  case duckdb::LogicalTypeId::FLOAT:
    return duckdb::AggregateFunction::BinaryAggregate<
        FirstScroogeState<float>, float, int64_t, float, FirstScroogeOperation>(
        type, timestamp_type, type);
  case duckdb::LogicalTypeId::DOUBLE:
    return duckdb::AggregateFunction::BinaryAggregate<FirstScroogeState<double>,
                                                      double, int64_t, double,
                                                      FirstScroogeOperation>(
        type, timestamp_type, type);
  case duckdb::LogicalTypeId::UTINYINT:
    return duckdb::AggregateFunction::BinaryAggregate<
        FirstScroogeState<uint8_t>, uint8_t, int64_t, uint8_t,
        FirstScroogeOperation>(type, timestamp_type, type);
  case duckdb::LogicalTypeId::USMALLINT:
    return duckdb::AggregateFunction::BinaryAggregate<
        FirstScroogeState<uint16_t>, uint16_t, int64_t, uint16_t,
        FirstScroogeOperation>(type, timestamp_type, type);
  case duckdb::LogicalTypeId::UINTEGER:
    return duckdb::AggregateFunction::BinaryAggregate<
        FirstScroogeState<uint32_t>, uint32_t, int64_t, uint32_t,
        FirstScroogeOperation>(type, timestamp_type, type);
  case duckdb::LogicalTypeId::UBIGINT:
    return duckdb::AggregateFunction::BinaryAggregate<
        FirstScroogeState<uint64_t>, uint64_t, int64_t, uint64_t,
        FirstScroogeOperation>(type, timestamp_type, type);
  case duckdb::LogicalTypeId::HUGEINT:
    return duckdb::AggregateFunction::BinaryAggregate<
        FirstScroogeState<duckdb::hugeint_t>, duckdb::hugeint_t, int64_t,
        duckdb::hugeint_t, FirstScroogeOperation>(type, timestamp_type, type);
  case duckdb::LogicalTypeId::UHUGEINT:
    return duckdb::AggregateFunction::BinaryAggregate<
        FirstScroogeState<duckdb::uhugeint_t>, duckdb::uhugeint_t, int64_t,
        duckdb::uhugeint_t, FirstScroogeOperation>(type, timestamp_type, type);
  default:
    throw duckdb::InternalException(
        "Scrooge First Function only accept Numeric Inputs");
  }
}

void FirstScrooge::RegisterFunction(duckdb::Connection &conn,
                                    duckdb::Catalog &catalog) {
  // The first aggregate allows you to get the first value of one column as
  // ordered by another e.g., first(temperature, time) returns the earliest
  // temperature value based on time within an aggregate group.
  duckdb::AggregateFunctionSet first("first_s");
  for (auto &type : duckdb::LogicalType::Numeric()) {
    first.AddFunction(
        GetFirstScroogeFunction(duckdb::LogicalType::TIMESTAMP_TZ, type));
    first.AddFunction(
        GetFirstScroogeFunction(duckdb::LogicalType::TIMESTAMP, type));
  }
  duckdb::CreateAggregateFunctionInfo first_info(first);
  catalog.CreateFunction(*conn.context, first_info);
}

} // namespace scrooge