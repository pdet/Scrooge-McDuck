#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "functions/functions.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {
namespace scrooge {

void TimeBucketFunction(DataChunk &args, ExpressionState &state,
                        Vector &result) {
  D_ASSERT(args.ColumnCount() == 2);
  auto &timestamp_vector = args.data[0];
  auto interval_value = args.data[1].GetValue(0);
  if (interval_value.IsNull()) {
    throw std::runtime_error("Timebucket interval can't be null");
  }
  auto interval = Interval::GetMicro(interval_value.GetValue<interval_t>());
  if (timestamp_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
    if (ConstantVector::IsNull(timestamp_vector)) {
      ConstantVector::SetNull(result, true);
    } else {
      auto timestamp_ptr =
          ConstantVector::GetData<timestamp_t>(timestamp_vector);
      result.SetValue(0, timestamp_ptr[0].value -
                             (timestamp_ptr[0].value % interval));
    }
  }
  if (timestamp_vector.GetVectorType() == VectorType::FLAT_VECTOR) {
    auto timestamp_ptr = FlatVector::GetData<timestamp_t>(timestamp_vector);
    auto timestamp_validity = FlatVector::Validity(timestamp_vector);

    auto result_ptr = FlatVector::GetData<timestamp_t>(result);
    if (timestamp_validity.AllValid()) {
      for (idx_t i = 0; i < args.size(); i++) {
        result_ptr[i] =
            timestamp_ptr[i].value - (timestamp_ptr[i].value % interval);
      }
    } else {
      auto &result_validity = FlatVector::Validity(result);
      for (idx_t i = 0; i < args.size(); i++) {
        if (timestamp_validity.RowIsValid(i)) {
          result_ptr[i] =
              timestamp_ptr[i].value - (timestamp_ptr[i].value % interval);
        } else {
          result_validity.SetInvalid(i);
        }
      }
    }
  } else {
    UnifiedVectorFormat timestamp_data;
    timestamp_vector.ToUnifiedFormat(args.size(), timestamp_data);
    auto timestamp_ptr = (const timestamp_t *)timestamp_data.data;

    auto result_ptr = FlatVector::GetData<timestamp_t>(result);

    if (timestamp_data.validity.AllValid()) {
      for (idx_t i = 0; i < args.size(); ++i) {
        const auto idx = timestamp_data.sel->get_index(i);
        result_ptr[i] =
            timestamp_ptr[idx].value - (timestamp_ptr[idx].value % interval);
      }
    } else {
      auto &result_validity = FlatVector::Validity(result);
      for (idx_t i = 0; i < args.size(); i++) {
        if (timestamp_data.validity.RowIsValid(i)) {
          result_ptr[i] =
              timestamp_ptr[i].value - (timestamp_ptr[i].value % interval);
        } else {
          result_validity.SetInvalid(i);
        }
      }
    }
  }
}

void TimeBucketScrooge::RegisterFunction(Connection &conn, Catalog &catalog) {
  // The time_bucket function is similar to the standard PostgreSQL date_trunc
  // function. Unlike date_trunc, it allows for arbitrary time intervals
  // instead of second, minute, and hour intervals. The return value is the
  // bucket's start time.

  ScalarFunctionSet timebucket("timebucket");
  timebucket.AddFunction(
      ScalarFunction({LogicalType::TIMESTAMP_TZ, LogicalType::INTERVAL},
                     LogicalType::TIMESTAMP_TZ, TimeBucketFunction));
  timebucket.AddFunction(
      ScalarFunction({LogicalType::TIMESTAMP, LogicalType::INTERVAL},
                     LogicalType::TIMESTAMP, TimeBucketFunction));
  CreateScalarFunctionInfo timebucket_info(timebucket);
  catalog.CreateFunction(*conn.context, timebucket_info);
}
} // namespace scrooge
} // namespace duckdb