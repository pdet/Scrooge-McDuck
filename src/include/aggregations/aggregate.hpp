//===----------------------------------------------------------------------===//
//                         Scrooge
//
// aggregations/aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace scrooge {

struct FirstScrooge {
  static void RegisterFunction(duckdb::Connection &conn,
                               duckdb::Catalog &catalog);
};

} // namespace scrooge
