#include "functions/functions.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

namespace scrooge {
void Aliases::Register(duckdb::Connection &conn, duckdb::Catalog &catalog) {
  // Register Volatility
  auto &stddev = (duckdb::AggregateFunctionCatalogEntry &)catalog.GetEntry(
      *conn.context, duckdb::CatalogType::AGGREGATE_FUNCTION_ENTRY,
      DEFAULT_SCHEMA, "stddev_pop");
  auto volatility = stddev.functions;
  volatility.name = "volatility";
  duckdb::CreateAggregateFunctionInfo volatility_info(volatility);
  catalog.CreateFunction(*conn.context, volatility_info);

  // Register SMA
  auto &avg = (duckdb::AggregateFunctionCatalogEntry &)catalog.GetEntry(
      *conn.context, duckdb::CatalogType::AGGREGATE_FUNCTION_ENTRY,
      DEFAULT_SCHEMA, "avg");
  auto sma = avg.functions;
  sma.name = "sma";
  duckdb::CreateAggregateFunctionInfo sma_info(sma);
  catalog.CreateFunction(*conn.context, sma_info);
}
} // namespace scrooge
