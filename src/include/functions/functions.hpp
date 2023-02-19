//===----------------------------------------------------------------------===//
//                         Scrooge
//
// functions/functions.hpp
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

struct LastScrooge {
  static void RegisterFunction(duckdb::Connection &conn,
                               duckdb::Catalog &catalog);
};

struct TimeBucketScrooge {
  static void RegisterFunction(duckdb::Connection &conn,
                               duckdb::Catalog &catalog);
};

struct Aliases {
  static void Register(duckdb::Connection &conn, duckdb::Catalog &catalog);
};

} // namespace scrooge
