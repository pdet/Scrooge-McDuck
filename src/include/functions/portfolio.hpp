#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace scrooge {

void RegisterPortfolioFunctions(Connection &conn, Catalog &catalog);

} // namespace scrooge
} // namespace duckdb
