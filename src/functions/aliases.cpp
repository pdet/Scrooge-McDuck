#include "functions/functions.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

namespace scrooge {

// Aliases for builtin core_functions aggregates. We register these only
// if the underlying functions are already in the catalog — otherwise a
// scrooge load that races ahead of core_functions throws CatalogError.
static void RegisterAlias(Connection &conn, Catalog &catalog,
                            const string &source, const string &alias) {
	try {
		auto &entry = catalog.GetEntry(*conn.context, CatalogType::AGGREGATE_FUNCTION_ENTRY,
		                                 DEFAULT_SCHEMA, source).Cast<AggregateFunctionCatalogEntry>();
		auto fns = entry.functions;
		fns.name = alias;
		CreateAggregateFunctionInfo info(fns);
		catalog.CreateFunction(*conn.context, info);
	} catch (...) {
		// core_functions not yet loaded — skip silently. The user can
		// reload scrooge after `LOAD core_functions` to pick up the alias.
	}
}

void Aliases::Register(Connection &conn, Catalog &catalog) {
	RegisterAlias(conn, catalog, "stddev_pop", "volatility");
	RegisterAlias(conn, catalog, "avg", "sma");
}

} // namespace scrooge
} // namespace duckdb