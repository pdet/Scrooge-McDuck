#include "functions/functions.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

namespace scrooge {
void Aliases::Register(Connection &conn, Catalog &catalog) {
  // Register Volatility
  auto &stddev =
      catalog
          .GetEntry(*conn.context, CatalogType::AGGREGATE_FUNCTION_ENTRY,
                    DEFAULT_SCHEMA, "stddev_pop")
          .Cast<AggregateFunctionCatalogEntry>();
  auto volatility = stddev.functions;
  volatility.name = "volatility";
  CreateAggregateFunctionInfo volatility_info(volatility);
  catalog.CreateFunction(*conn.context, volatility_info);

  // Register SMA
  auto &avg =
      catalog
          .GetEntry(*conn.context, CatalogType::AGGREGATE_FUNCTION_ENTRY,
                    DEFAULT_SCHEMA, "avg")
          .Cast<AggregateFunctionCatalogEntry>();
  auto sma = avg.functions;
  sma.name = "sma";
  CreateAggregateFunctionInfo sma_info(sma);
  catalog.CreateFunction(*conn.context, sma_info);
}
} // namespace scrooge
} // namespace duckdb