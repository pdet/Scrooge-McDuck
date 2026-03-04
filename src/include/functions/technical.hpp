#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {
namespace scrooge {

//! Register all technical indicator functions
void RegisterTechnicalFunctions(Connection &conn, Catalog &catalog);

} // namespace scrooge
} // namespace duckdb
