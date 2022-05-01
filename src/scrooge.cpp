#define DUCKDB_BUILD_LOADABLE_EXTENSION

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include <iostream>

namespace duckdb {
class ScroogeExtension : public Extension {
public:
  void Load(DuckDB &db) override;
  std::string Name() override;
};

struct FirstScroogeState {
  int64_t first;
  int64_t earliest_time;
  bool executed;
};

struct FirstScroogeOperation {
  template <class STATE> static void Initialize(STATE *state) {
    state->first = 0;
    state->earliest_time = NumericLimits<int64_t>::Maximum();
    state->executed = false;
  }

  template <class A_TYPE, class B_TYPE, class STATE, class OP>
  static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data,
                        B_TYPE *y_data, ValidityMask &amask,
                        ValidityMask &bmask, idx_t xidx, idx_t yidx) {

    const auto time = y_data[yidx];

    if (time < state->earliest_time) {
      state->earliest_time = time;
      state->first = x_data[xidx];
      state->executed = true;
    }
  }

  template <class STATE, class OP>
  static void Combine(const STATE &source, STATE *target,
                      FunctionData *bind_data) {
    if (!target->executed) {
      *target = source;
    } else if (source.executed) {
      if (target->earliest_time > source.earliest_time) {
        target->earliest_time = source.earliest_time;
        target->first = source.first;
      }
    }
  }

  template <class T, class STATE>
  static void Finalize(Vector &result, FunctionData *, STATE *state, T *target,
                       ValidityMask &mask, idx_t idx) {
    if (!state->executed) {
      mask.SetInvalid(idx);
    } else {
      target[idx] = state->first;
    }
  }

  static bool IgnoreNull() { return true; }
};

void ScroogeExtension::Load(DuckDB &db) {
  Connection con(db);
  con.BeginTransaction();
  auto &catalog = Catalog::GetCatalog(*con.context);

  // The first aggregate allows you to get the first value of one column as
  // ordered by another e.g., first(temperature, time) returns the earliest
  // temperature value based on time within an aggregate group.

  AggregateFunctionSet first("first_s");
  //	for (auto &type : LogicalType::AllTypes()) {

  first.AddFunction(
      AggregateFunction::BinaryAggregate<FirstScroogeState, int64_t, int64_t,
                                         int64_t, FirstScroogeOperation>(
          LogicalType::BIGINT, LogicalType::TIMESTAMP_TZ, LogicalType::BIGINT));
  //	}
  CreateAggregateFunctionInfo first_info(first);
  catalog.CreateFunction(*con.context, &first_info);
  con.Commit();
}

std::string ScroogeExtension::Name() { return "scrooge"; }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void scrooge_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::ScroogeExtension>();
}

DUCKDB_EXTENSION_API const char *scrooge_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}