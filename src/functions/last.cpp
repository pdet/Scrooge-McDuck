#include "functions/functions.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {
namespace scrooge {

template <class T> struct LastScroogeState {
  T last;
  int64_t last_time;
  bool executed;
};

struct LastScroogeOperation {
  template <class STATE> static void Initialize(STATE &state) {
    state.last_time = NumericLimits<int64_t>::Minimum();
    state.executed = false;
  }

  template <class A_TYPE, class B_TYPE, class STATE, class OP>
  static void Operation(STATE &state, const A_TYPE &x_data,
                        const B_TYPE &y_data, AggregateBinaryInput &idata) {

    const auto time = y_data;
    if (!state.executed || time > state.last_time) {
      state.last_time = time;
      state.last = x_data;
      state.executed = true;
    }
  }

  template <class STATE, class OP>
  static void Combine(const STATE &source, STATE &target,
                      AggregateInputData &aggr_input_data) {
    if (!target.executed) {
      target = source;
    } else if (source.executed) {
      if (target.last_time > source.last_time) {
        target.last_time = source.last_time;
        target.last = source.last;
      }
    }
  }

  template <class T, class STATE>
  static void Finalize(STATE &state, T &target,
                       AggregateFinalizeData &finalize_data) {
    if (!state.executed) {
      finalize_data.ReturnNull();
    } else {
      target = state.last;
    }
  }

  static bool IgnoreNull() { return true; }
};

unique_ptr<FunctionData>
BindDoupleLastFunctionDecimal(ClientContext &context,
                              AggregateFunction &function,
                              vector<unique_ptr<Expression>> &arguments) {
  auto &decimal_type = arguments[0]->return_type;
  switch (decimal_type.InternalType()) {
  case PhysicalType::INT16: {
    function = AggregateFunction::BinaryAggregate<LastScroogeState<int16_t>,
                                                  int16_t, int64_t, int16_t,
                                                  LastScroogeOperation>(
        decimal_type, LogicalType::TIMESTAMP_TZ, decimal_type);
    break;
  }
  case PhysicalType::INT32: {
    function = AggregateFunction::BinaryAggregate<LastScroogeState<int32_t>,
                                                  int32_t, int64_t, int32_t,
                                                  LastScroogeOperation>(
        decimal_type, LogicalType::TIMESTAMP_TZ, decimal_type);
    break;
  }
  case PhysicalType::INT64: {
    function = AggregateFunction::BinaryAggregate<LastScroogeState<int64_t>,
                                                  int64_t, int64_t, int64_t,
                                                  LastScroogeOperation>(
        decimal_type, LogicalType::TIMESTAMP_TZ, decimal_type);
    break;
  }
  default:
    function = AggregateFunction::BinaryAggregate<LastScroogeState<Hugeint>,
                                                  Hugeint, int64_t, Hugeint,
                                                  LastScroogeOperation>(
        decimal_type, LogicalType::TIMESTAMP_TZ, decimal_type);
  }
  function.name = "last_s";
  return nullptr;
}

AggregateFunction GetLastScroogeFunction(const LogicalType &timestamp_type,
                                         const LogicalType &type) {
  switch (type.id()) {
  case LogicalTypeId::SMALLINT:
    return AggregateFunction::BinaryAggregate<LastScroogeState<int16_t>,
                                              int16_t, int64_t, int16_t,
                                              LastScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::TINYINT:
    return AggregateFunction::BinaryAggregate<LastScroogeState<int8_t>, int8_t,
                                              int64_t, int8_t,
                                              LastScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::INTEGER:
    return AggregateFunction::BinaryAggregate<LastScroogeState<int32_t>,
                                              int32_t, int64_t, int32_t,
                                              LastScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::BIGINT:
    return AggregateFunction::BinaryAggregate<LastScroogeState<int64_t>,
                                              int64_t, int64_t, int64_t,
                                              LastScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::DECIMAL: {
    auto decimal_aggregate =
        AggregateFunction::BinaryAggregate<LastScroogeState<hugeint_t>,
                                           hugeint_t, int64_t, hugeint_t,
                                           LastScroogeOperation>(
            type, timestamp_type, type);
    decimal_aggregate.bind = BindDoupleLastFunctionDecimal;
    return decimal_aggregate;
  }
    //    	corr.AddFunction(AggregateFunction::BinaryAggregate<CorrState,
    //    double, double, double, CorrOperation>(
    //	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
  case LogicalTypeId::FLOAT:
    return AggregateFunction::BinaryAggregate<
        LastScroogeState<float>, float, int64_t, float, LastScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::DOUBLE:
    return AggregateFunction::BinaryAggregate<LastScroogeState<double>, double,
                                              int64_t, double,
                                              LastScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::UTINYINT:
    return AggregateFunction::BinaryAggregate<LastScroogeState<uint8_t>,
                                              uint8_t, int64_t, uint8_t,
                                              LastScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::USMALLINT:
    return AggregateFunction::BinaryAggregate<LastScroogeState<uint16_t>,
                                              uint16_t, int64_t, uint16_t,
                                              LastScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::UINTEGER:
    return AggregateFunction::BinaryAggregate<LastScroogeState<uint32_t>,
                                              uint32_t, int64_t, uint32_t,
                                              LastScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::UBIGINT:
    return AggregateFunction::BinaryAggregate<LastScroogeState<uint64_t>,
                                              uint64_t, int64_t, uint64_t,
                                              LastScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::HUGEINT:
    return AggregateFunction::BinaryAggregate<LastScroogeState<hugeint_t>,
                                              hugeint_t, int64_t, hugeint_t,
                                              LastScroogeOperation>(
        type, timestamp_type, type);
  case LogicalTypeId::UHUGEINT:
    return AggregateFunction::BinaryAggregate<LastScroogeState<hugeint_t>,
                                              hugeint_t, int64_t, hugeint_t,
                                              LastScroogeOperation>(
        type, timestamp_type, type);
  default:
    throw InternalException(
        "Scrooge First Function only accept Numeric Inputs");
  }
}

void LastScrooge::RegisterFunction(Connection &conn, Catalog &catalog) {
  // The last aggregate allows you to get the value of one column as ordered by
  // another. For example, last(temperature, time) returns the latest
  // temperature value based on time within an aggregate group.

  AggregateFunctionSet last("last_s");
  for (auto &type : LogicalType::Numeric()) {
    last.AddFunction(GetLastScroogeFunction(LogicalType::TIMESTAMP_TZ, type));
    last.AddFunction(GetLastScroogeFunction(LogicalType::TIMESTAMP, type));
  }
  CreateAggregateFunctionInfo last_info(last);
  catalog.CreateFunction(*conn.context, last_info);
}

} // namespace scrooge
} // namespace duckdb