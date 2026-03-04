#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {
namespace scrooge {

//! Register risk metric functions (sharpe_ratio, sortino_ratio, max_drawdown, value_at_risk)
void RegisterRiskFunctions(Connection &conn, Catalog &catalog);
void RegisterVaRFunction(Connection &conn, Catalog &catalog);

} // namespace scrooge
} // namespace duckdb
