#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "functions/functions.hpp"

namespace scrooge {

void TimeBucketFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state,
                        duckdb::Vector &result) {
  D_ASSERT(args.ColumnCount() == 2);
  auto &timestamp_vector = args.data[0];
  auto interval_value = args.data[1].GetValue(0);
  if (interval_value.IsNull()) {
    throw std::runtime_error("Timebucket interval can't be null");
  }
  auto interval =
      duckdb::Interval::GetMicro(interval_value.GetValue<duckdb::interval_t>());
  if (timestamp_vector.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
    result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
    if (duckdb::ConstantVector::IsNull(timestamp_vector)) {
      duckdb::ConstantVector::SetNull(result, true);
    } else {
      auto timestamp_ptr = duckdb::ConstantVector::GetData<duckdb::timestamp_t>(
          timestamp_vector);
      result.SetValue(0, timestamp_ptr[0].value -
                             (timestamp_ptr[0].value % interval));
    }
  }
  if (timestamp_vector.GetVectorType() == duckdb::VectorType::FLAT_VECTOR) {
    auto timestamp_ptr =
        duckdb::FlatVector::GetData<duckdb::timestamp_t>(timestamp_vector);
    auto timestamp_validity = duckdb::FlatVector::Validity(timestamp_vector);

    auto result_ptr = duckdb::FlatVector::GetData<duckdb::timestamp_t>(result);
    if (timestamp_validity.AllValid()) {
      for (idx_t i = 0; i < args.size(); i++) {
        result_ptr[i] =
            timestamp_ptr[i].value - (timestamp_ptr[i].value % interval);
      }
    } else {
      auto &result_validity = duckdb::FlatVector::Validity(result);
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
    duckdb::UnifiedVectorFormat timestamp_data;
    timestamp_vector.ToUnifiedFormat(args.size(), timestamp_data);
    auto timestamp_ptr = (const duckdb::timestamp_t *)timestamp_data.data;

    auto result_ptr = duckdb::FlatVector::GetData<duckdb::timestamp_t>(result);

    if (timestamp_data.validity.AllValid()) {
      for (idx_t i = 0; i < args.size(); ++i) {
        const auto idx = timestamp_data.sel->get_index(i);
        result_ptr[i] =
            timestamp_ptr[idx].value - (timestamp_ptr[idx].value % interval);
      }
    } else {
      auto &result_validity = duckdb::FlatVector::Validity(result);
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

void TimeBucketScrooge::RegisterFunction(duckdb::Connection &conn,
                                         duckdb::Catalog &catalog) {
  // The time_bucket function is similar to the standard PostgreSQL date_trunc
  // function. Unlike date_trunc, it allows for arbitrary time intervals
  // instead of second, minute, and hour intervals. The return value is the
  // bucket's start time.

  duckdb::ScalarFunctionSet timebucket("timebucket");
  timebucket.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::INTERVAL},
      duckdb::LogicalType::TIMESTAMP_TZ, TimeBucketFunction));
  timebucket.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::TIMESTAMP, duckdb::LogicalType::INTERVAL},
      duckdb::LogicalType::TIMESTAMP, TimeBucketFunction));
  duckdb::CreateScalarFunctionInfo timebucket_info(timebucket);
  catalog.CreateFunction(*conn.context, timebucket_info);
}
} // namespace scrooge