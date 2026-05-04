#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {
namespace scrooge {

void RegisterPolygonScanner(Connection &conn, Catalog &catalog);
void RegisterBinanceScanner(Connection &conn, Catalog &catalog);
void RegisterJsonRpcScanner(Connection &conn, Catalog &catalog);
void RegisterAlphaVantageNewsScanner(Connection &conn, Catalog &catalog);

} // namespace scrooge
} // namespace duckdb
