#include "functions/functions.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {
namespace scrooge {

template <class T> struct FirstScroogeState {
  T first;
  int64_t earliest_time;
  bool executed;
};

struct FirstScroogeOperation {
  template <class STATE> static void Initialize(STATE &state) {
    state.earliest_time = NumericLimits<int64_t>::Maximum();
    state.executed = false;
  }

  template <class A_TYPE, class B_TYPE, class STATE, class OP>
  static void Operation(STATE &state, const A_TYPE &x_data,
                        const B_TYPE &y_data, AggregateBinaryInput &idata) {
    if (!state.executed || y_data < state.earliest_time) {
      state.earliest_time = y_data;
      state.first = x_data;
      state.executed = true;
    }
  }

  template <class STATE, class OP>
  static void Combine(const STATE &source, STATE &target,
                      AggregateInputData &aggr_input_data) {
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
  static void Finalize(STATE &state, T &target,
                       AggregateFinalizeData &finalize_data) {
    if (!state.executed) {
      finalize_data.ReturnNull();
    } else {
      target = state.first;
    }
  }

  static bool IgnoreNull() { return true; }
};

unique_ptr<FunctionData>
BindDoubleFirst(ClientContext &context, AggregateFunction &function,
                vector<unique_ptr<Expression>> &arguments) {
  auto &decimal_type = arguments[0]->return_type;
  switch (decimal_type.InternalType()) {
  case PhysicalType::INT16: {
    function = AggregateFunction::BinaryAggregate<FirstScroogeState<int16_t>,
                                                  int16_t, int64_t, int16_t,
                                                  FirstScroogeOperation>(
        decimal_type, LogicalType::TIMESTAMP_TZ, decimal_type);
    break;
  }
  case PhysicalType::INT32: {
    function = AggregateFunction::BinaryAggregate<FirstScroogeState<int32_t>,
                                                  int32_t, int64_t, int32_t,
                                                  FirstScroogeOperation>(
        decimal_type, LogicalType::TIMESTAMP_TZ, decimal_type);
    break;
  }
  case PhysicalType::INT64: {
    function = AggregateFunction::BinaryAggregate<FirstScroogeState<int64_t>,
                                                  int64_t, int64_t, int64_t,
                                                  FirstScroogeOperation>(
        decimal_type, LogicalType::TIMESTAMP_TZ, decimal_type);
    break;
  }
  default:
    function = AggregateFunction::BinaryAggregate<FirstScroogeState<Hugeint>,
                                                  Hugeint, int64_t, Hugeint,
                                                  FirstScroogeOperation>(
        decimal_type, LogicalType::TIMESTAMP_TZ, decimal_type);
  }
  function.name = "first_s";
  return nullptr;
}

AggregateFunction GetFirstScroogeFunction(const LogicalType &timestamp_type,
                                          const LogicalType &type) {
  switch (type.id()) {
  case LogicalTypeId::SMALLINT:
    return AggregateFunction::BinaryAggregate<FirstScroogeState<int16_t>,
                                              int16_t, int64_t, int16_t,
                                              FirstScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::TINYINT:
    return AggregateFunction::BinaryAggregate<FirstScroogeState<int8_t>, int8_t,
                                              int64_t, int8_t,
                                              FirstScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::INTEGER:
    return AggregateFunction::BinaryAggregate<FirstScroogeState<int32_t>,
                                              int32_t, int64_t, int32_t,
                                              FirstScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::BIGINT:
    return AggregateFunction::BinaryAggregate<FirstScroogeState<int64_t>,
                                              int64_t, int64_t, int64_t,
                                              FirstScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::DECIMAL: {
    auto decimal_aggregate =
        AggregateFunction::BinaryAggregate<FirstScroogeState<hugeint_t>,
                                           hugeint_t, int64_t, hugeint_t,
                                           FirstScroogeOperation>(
            type, timestamp_type, type);
    decimal_aggregate.bind = BindDoubleFirst;
    return decimal_aggregate;
  }
  case LogicalTypeId::FLOAT:
    return AggregateFunction::BinaryAggregate<
        FirstScroogeState<float>, float, int64_t, float, FirstScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::DOUBLE:
    return AggregateFunction::BinaryAggregate<FirstScroogeState<double>, double,
                                              int64_t, double,
                                              FirstScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::UTINYINT:
    return AggregateFunction::BinaryAggregate<FirstScroogeState<uint8_t>,
                                              uint8_t, int64_t, uint8_t,
                                              FirstScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::USMALLINT:
    return AggregateFunction::BinaryAggregate<FirstScroogeState<uint16_t>,
                                              uint16_t, int64_t, uint16_t,
                                              FirstScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::UINTEGER:
    return AggregateFunction::BinaryAggregate<FirstScroogeState<uint32_t>,
                                              uint32_t, int64_t, uint32_t,
                                              FirstScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::UBIGINT:
    return AggregateFunction::BinaryAggregate<FirstScroogeState<uint64_t>,
                                              uint64_t, int64_t, uint64_t,
                                              FirstScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::HUGEINT:
    return AggregateFunction::BinaryAggregate<FirstScroogeState<hugeint_t>,
                                              hugeint_t, int64_t, hugeint_t,
                                              FirstScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::UHUGEINT:
    return AggregateFunction::BinaryAggregate<FirstScroogeState<uhugeint_t>,
                                              uhugeint_t, int64_t, uhugeint_t,
                                              FirstScroogeOperation>(
        type, timestamp_type, type);
  default:
    throw InternalException(
        "Scrooge First Function only accept Numeric Inputs");
  }
}

void FirstScrooge::RegisterFunction(Connection &conn, Catalog &catalog) {
  // The first aggregate allows you to get the first value of one column as
  // ordered by another e.g., first(temperature, time) returns the earliest
  // temperature value based on time within an aggregate group.
  AggregateFunctionSet first("first_s");
  for (auto &type : LogicalType::Numeric()) {
    first.AddFunction(GetFirstScroogeFunction(LogicalType::TIMESTAMP_TZ, type));
    first.AddFunction(GetFirstScroogeFunction(LogicalType::TIMESTAMP, type));
  }
  CreateAggregateFunctionInfo first_info(first);
  catalog.CreateFunction(*conn.context, first_info);
}

} // namespace scrooge
} // namespace duckdb