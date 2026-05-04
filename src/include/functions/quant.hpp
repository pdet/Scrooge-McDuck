#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {
namespace scrooge {

void RegisterMonteCarlo(Connection &conn, Catalog &catalog);
void RegisterPortfolioOptimization(Connection &conn, Catalog &catalog);
void RegisterBacktest(Connection &conn, Catalog &catalog);
void RegisterFixedIncome(Connection &conn, Catalog &catalog);
void RegisterAmericanOptions(Connection &conn, Catalog &catalog);
void RegisterTimeSeriesStats(Connection &conn, Catalog &catalog);

} // namespace scrooge
} // namespace duckdb
