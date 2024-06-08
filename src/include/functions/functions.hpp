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

namespace duckdb {
namespace scrooge {

struct FirstScrooge {
  static void RegisterFunction(Connection &conn, Catalog &catalog);
};

struct LastScrooge {
  static void RegisterFunction(Connection &conn, Catalog &catalog);
};

struct TimeBucketScrooge {
  static void RegisterFunction(Connection &conn, Catalog &catalog);
};

struct Aliases {
  static void Register(Connection &conn, Catalog &catalog);
};

} // namespace scrooge
} // namespace duckdb
