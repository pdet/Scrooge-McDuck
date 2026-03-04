#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace scrooge {

void RegisterCandlestickFunctions(Connection &conn, Catalog &catalog);

} // namespace scrooge
} // namespace duckdb
