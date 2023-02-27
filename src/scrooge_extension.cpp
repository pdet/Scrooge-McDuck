#define DUCKDB_EXTENSION_MAIN

#include "scrooge_extension.hpp"
#include "functions/functions.hpp"
#include "functions/scanner.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include <iostream>

namespace duckdb {

void ScroogeExtension::Load(DuckDB &db) {
  Connection con(db);
  con.BeginTransaction();
  auto &catalog = Catalog::GetSystemCatalog(*con.context);
  scrooge::FirstScrooge::RegisterFunction(con, catalog);
  scrooge::LastScrooge::RegisterFunction(con, catalog);
  scrooge::TimeBucketScrooge::RegisterFunction(con, catalog);
  scrooge::Aliases::Register(con, catalog);

  // Create Yahoo Scanner Function
  TableFunction yahoo_scanner("yahoo_finance",
                              {LogicalType::ANY, LogicalType::ANY,
                               LogicalType::ANY, LogicalType::VARCHAR},
                              scrooge::YahooScanner::Scan,
                              scrooge::YahooScanner::Bind);
  CreateTableFunctionInfo yahoo_scanner_info(yahoo_scanner);
  catalog.CreateTableFunction(*con.context, &yahoo_scanner_info);

  // Create Yahoo Scanner Function
  TableFunction portfolio_frontier(
      "portfolio_frontier",
      {duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
       LogicalType::ANY, LogicalType::ANY, LogicalType::INTEGER},
      scrooge::PortfolioFrontier::Scan, scrooge::PortfolioFrontier::Bind);
  CreateTableFunctionInfo portfolio_frontier_info(portfolio_frontier);
  catalog.CreateTableFunction(*con.context, &portfolio_frontier_info);

  con.Commit();
}

std::string ScroogeExtension::Name() { return "scrooge"; }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void scrooge_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::ScroogeExtension>();
}

DUCKDB_EXTENSION_API const char *scrooge_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}