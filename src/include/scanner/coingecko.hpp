#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {
namespace scrooge {

void RegisterCoinGeckoScanner(Connection &conn, Catalog &catalog);

} // namespace scrooge
} // namespace duckdb
