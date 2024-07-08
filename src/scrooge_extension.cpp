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
#include "duckdb/parser/parsed_data/create_type_info.hpp"

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

  // Create Portfolio Frontier Function
  TableFunction portfolio_frontier(
      "portfolio_frontier",
      {duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
       LogicalType::ANY, LogicalType::ANY, LogicalType::INTEGER},
      scrooge::PortfolioFrontier::Scan, scrooge::PortfolioFrontier::Bind);
  CreateTableFunctionInfo portfolio_frontier_info(portfolio_frontier);
  catalog.CreateTableFunction(*con.context, &portfolio_frontier_info);

  // Create Ethereum Scanner Function
  TableFunction ethereum_rpc_scanner(
      "read_eth",
      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
       LogicalType::BIGINT},
      scrooge::EthRPC::Scan, scrooge::EthRPC::Bind, scrooge::EthRPC::InitGlobal,
      scrooge::EthRPC::InitLocal);
  ethereum_rpc_scanner.table_scan_progress = scrooge::EthRPC::ProgressBar;
  ethereum_rpc_scanner.projection_pushdown = true;
  ethereum_rpc_scanner.named_parameters["blocks_per_thread"] =
      LogicalType::BIGINT;
  ethereum_rpc_scanner.named_parameters["strict"] = LogicalType::BOOLEAN;

  CreateTableFunctionInfo ethereum_rpc_scanner_info(ethereum_rpc_scanner);
  catalog.CreateTableFunction(*con.context, &ethereum_rpc_scanner_info);

  auto &config = DBConfig::GetConfig(*db.instance);

  config.AddExtensionOption("eth_node_url",
                            "URL of Ethereum node to be queried",
                            LogicalType::VARCHAR, "http://127.0.0.1:8545");

  auto &temp_catalog = Catalog::GetCatalog(*con.context, TEMP_CATALOG);
  // Create CSV_ERROR_TYPE ENUM
  string enum_name = "ETH_EVENT";
  Vector order_errors(LogicalType::VARCHAR, 7);
  order_errors.SetValue(0, "Transfer");
  order_errors.SetValue(1, "Approval");
  order_errors.SetValue(2, "Sync");
  order_errors.SetValue(3, "TransferSingle");
  order_errors.SetValue(4, "TransferBatch");
  order_errors.SetValue(5, "ApprovalForAll");
  order_errors.SetValue(6, "Unknown");
  LogicalType enum_type = LogicalType::ENUM(enum_name, order_errors, 7);
  auto type_info = make_uniq<CreateTypeInfo>(enum_name, enum_type);
  type_info->temporary = true;
  type_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
  temp_catalog.CreateType(*con.context, *type_info);
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