#include "functions/functions.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

namespace scrooge {

template <class T> struct LastScroogeState {
  T last;
  int64_t last_time;
  bool executed;
};

struct LastScroogeOperation {
  template <class STATE> static void Initialize(STATE *state) {
    state->last_time = duckdb::NumericLimits<int64_t>::Minimum();
    state->executed = false;
  }

  template <class A_TYPE, class B_TYPE, class STATE, class OP>
  static void Operation(STATE *state, duckdb::FunctionData *bind_data,
                        A_TYPE *x_data, B_TYPE *y_data,
                        duckdb::ValidityMask &amask,
                        duckdb::ValidityMask &bmask, idx_t xidx, idx_t yidx) {

    const auto time = y_data[yidx];
    if (!state->executed || time > state->last_time) {
      state->last_time = time;
      state->last = x_data[xidx];
      state->executed = true;
    }
  }

  template <class STATE, class OP>
  static void Combine(const STATE &source, STATE *target,
                      duckdb::FunctionData *bind_data) {
    if (!target->executed) {
      *target = source;
    } else if (source.executed) {
      if (target->last_time > source.last_time) {
        target->last_time = source.last_time;
        target->last = source.last;
      }
    }
  }

  template <class T, class STATE>
  static void Finalize(duckdb::Vector &result, duckdb::FunctionData *,
                       STATE *state, T *target, duckdb::ValidityMask &mask,
                       idx_t idx) {
    if (!state->executed) {
      mask.SetInvalid(idx);
    } else {
      target[idx] = state->last;
    }
  }

  static bool IgnoreNull() { return true; }
};

std::unique_ptr<duckdb::FunctionData> BindDoupleLastFunctionDecimal(
    duckdb::ClientContext &context, duckdb::AggregateFunction &bound_function,
    std::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments) {
  auto &decimal_type = arguments[0]->return_type;
  switch (decimal_type.InternalType()) {
  case duckdb::PhysicalType::INT16: {
    bound_function =
        duckdb::AggregateFunction::BinaryAggregate<LastScroogeState<int16_t>,
                                                   int16_t, int64_t, int16_t,
                                                   LastScroogeOperation>(
            decimal_type, duckdb::LogicalType::TIMESTAMP_TZ, decimal_type);
    break;
  }
  case duckdb::PhysicalType::INT32: {
    bound_function =
        duckdb::AggregateFunction::BinaryAggregate<LastScroogeState<int32_t>,
                                                   int32_t, int64_t, int32_t,
                                                   LastScroogeOperation>(
            decimal_type, duckdb::LogicalType::TIMESTAMP_TZ, decimal_type);
    break;
  }
  case duckdb::PhysicalType::INT64: {
    bound_function =
        duckdb::AggregateFunction::BinaryAggregate<LastScroogeState<int64_t>,
                                                   int64_t, int64_t, int64_t,
                                                   LastScroogeOperation>(
            decimal_type, duckdb::LogicalType::TIMESTAMP_TZ, decimal_type);
    break;
  }
  default:
    bound_function = duckdb::AggregateFunction::BinaryAggregate<
        LastScroogeState<duckdb::Hugeint>, duckdb::Hugeint, int64_t,
        duckdb::Hugeint, LastScroogeOperation>(
        decimal_type, duckdb::LogicalType::TIMESTAMP_TZ, decimal_type);
  }
  bound_function.name = "last_s";
  return nullptr;
}

duckdb::AggregateFunction
GetLastScroogeFunction(const duckdb::LogicalType &type) {
  switch (type.id()) {
  case duckdb::LogicalTypeId::SMALLINT:
    return duckdb::AggregateFunction::BinaryAggregate<LastScroogeState<int16_t>,
                                                      int16_t, int64_t, int16_t,
                                                      LastScroogeOperation>(
        type, duckdb::LogicalType::TIMESTAMP_TZ, type);
  case duckdb::LogicalTypeId::TINYINT:
    return duckdb::AggregateFunction::BinaryAggregate<LastScroogeState<int8_t>,
                                                      int8_t, int64_t, int8_t,
                                                      LastScroogeOperation>(
        type, duckdb::LogicalType::TIMESTAMP_TZ, type);
  case duckdb::LogicalTypeId::INTEGER:
    return duckdb::AggregateFunction::BinaryAggregate<LastScroogeState<int32_t>,
                                                      int32_t, int64_t, int32_t,
                                                      LastScroogeOperation>(
        type, duckdb::LogicalType::TIMESTAMP_TZ, type);
  case duckdb::LogicalTypeId::BIGINT:
    return duckdb::AggregateFunction::BinaryAggregate<LastScroogeState<int64_t>,
                                                      int64_t, int64_t, int64_t,
                                                      LastScroogeOperation>(
        type, duckdb::LogicalType::TIMESTAMP_TZ, type);
  case duckdb::LogicalTypeId::DECIMAL: {
    auto decimal_aggregate = duckdb::AggregateFunction::BinaryAggregate<
        LastScroogeState<duckdb::hugeint_t>, duckdb::hugeint_t, int64_t,
        duckdb::hugeint_t, LastScroogeOperation>(
        type, duckdb::LogicalType::TIMESTAMP_TZ, type);
    decimal_aggregate.bind = BindDoupleLastFunctionDecimal;
    return decimal_aggregate;
  }
  case duckdb::LogicalTypeId::FLOAT:
    return duckdb::AggregateFunction::BinaryAggregate<
        LastScroogeState<float>, float, int64_t, float, LastScroogeOperation>(
        type, duckdb::LogicalType::TIMESTAMP_TZ, type);
  case duckdb::LogicalTypeId::DOUBLE:
    return duckdb::AggregateFunction::BinaryAggregate<LastScroogeState<double>,
                                                      double, int64_t, double,
                                                      LastScroogeOperation>(
        type, duckdb::LogicalType::TIMESTAMP_TZ, type);
  case duckdb::LogicalTypeId::UTINYINT:
    return duckdb::AggregateFunction::BinaryAggregate<LastScroogeState<uint8_t>,
                                                      uint8_t, int64_t, uint8_t,
                                                      LastScroogeOperation>(
        type, duckdb::LogicalType::TIMESTAMP_TZ, type);
  case duckdb::LogicalTypeId::USMALLINT:
    return duckdb::AggregateFunction::BinaryAggregate<
        LastScroogeState<uint16_t>, uint16_t, int64_t, uint16_t,
        LastScroogeOperation>(type, duckdb::LogicalType::TIMESTAMP_TZ, type);
  case duckdb::LogicalTypeId::UINTEGER:
    return duckdb::AggregateFunction::BinaryAggregate<
        LastScroogeState<uint32_t>, uint32_t, int64_t, uint32_t,
        LastScroogeOperation>(type, duckdb::LogicalType::TIMESTAMP_TZ, type);
  case duckdb::LogicalTypeId::UBIGINT:
    return duckdb::AggregateFunction::BinaryAggregate<
        LastScroogeState<uint64_t>, uint64_t, int64_t, uint64_t,
        LastScroogeOperation>(type, duckdb::LogicalType::TIMESTAMP_TZ, type);
  case duckdb::LogicalTypeId::HUGEINT:
    return duckdb::AggregateFunction::BinaryAggregate<
        LastScroogeState<duckdb::hugeint_t>, duckdb::hugeint_t, int64_t,
        duckdb::hugeint_t, LastScroogeOperation>(
        type, duckdb::LogicalType::TIMESTAMP_TZ, type);
  default:
    throw duckdb::InternalException(
        "Scrooge First Function only accept Numeric Inputs");
  }
}

void LastScrooge::RegisterFunction(duckdb::Connection &conn,
                                   duckdb::Catalog &catalog) {
  // The last aggregate allows you to get the value of one column as ordered by
  // another. For example, last(temperature, time) returns the latest
  // temperature value based on time within an aggregate group.

  duckdb::AggregateFunctionSet last("last_s");
  for (auto &type : duckdb::LogicalType::Numeric()) {
    last.AddFunction(GetLastScroogeFunction(type));
  }
  duckdb::CreateAggregateFunctionInfo last_info(last);
  catalog.CreateFunction(*conn.context, &last_info);
}

} // namespace scrooge