#define DUCKDB_EXTENSION_MAIN

#include "scrooge_extension.hpp"
#include "functions/functions.hpp"
#include "functions/scanner.hpp"
#include "scanner/fred.hpp"
#include "functions/technical.hpp"
#include "functions/returns.hpp"
#include "functions/risk.hpp"
#include "functions/candlestick.hpp"
#include "functions/portfolio.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include <iostream>

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	auto &db_instance = loader.GetDatabaseInstance();
	DuckDB db(db_instance);
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// Core functions (first_s, last_s, timebucket, aliases)
	scrooge::FirstScrooge::RegisterFunction(con, catalog);
	scrooge::LastScrooge::RegisterFunction(con, catalog);
	scrooge::TimeBucketScrooge::RegisterFunction(con, catalog);
	scrooge::Aliases::Register(con, catalog);

	// Phase 1: Technical indicators
	scrooge::RegisterTechnicalFunctions(con, catalog);

	// Phase 2: Return calculations
	scrooge::RegisterReturnFunctions(con, catalog);

	// Phase 2: Risk metrics
	scrooge::RegisterRiskFunctions(con, catalog);

	// Phase 4: Candlestick patterns
	scrooge::RegisterCandlestickFunctions(con, catalog);

	// Portfolio analytics
	scrooge::RegisterPortfolioFunctions(con, catalog);

	// Yahoo Finance Scanner
	TableFunction yahoo_scanner("yahoo_finance",
	                            {LogicalType::ANY, LogicalType::ANY, LogicalType::ANY, LogicalType::VARCHAR},
	                            scrooge::YahooScanner::Scan, scrooge::YahooScanner::Bind);
	CreateTableFunctionInfo yahoo_scanner_info(yahoo_scanner);
	catalog.CreateTableFunction(*con.context, &yahoo_scanner_info);

	// FRED Economic Data Scanner
	TableFunctionSet fred_set("fred_series");
	// 2-arg: fred_series(series_id, api_key)
	fred_set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                   scrooge::FredScanner::Scan, scrooge::FredScanner::Bind));
	// 4-arg: fred_series(series_id, api_key, start_date, end_date)
	fred_set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY},
	                                   scrooge::FredScanner::Scan, scrooge::FredScanner::Bind));
	CreateTableFunctionInfo fred_scanner_info(fred_set);
	catalog.CreateTableFunction(*con.context, &fred_scanner_info);

	// Ethereum Scanner
	TableFunction ethereum_rpc_scanner("read_eth",
	                                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
	                                    LogicalType::BIGINT},
	                                   scrooge::EthRPC::Scan, scrooge::EthRPC::Bind, scrooge::EthRPC::InitGlobal,
	                                   scrooge::EthRPC::InitLocal);
	ethereum_rpc_scanner.table_scan_progress = scrooge::EthRPC::ProgressBar;
	ethereum_rpc_scanner.projection_pushdown = true;
	ethereum_rpc_scanner.named_parameters["blocks_per_thread"] = LogicalType::BIGINT;
	ethereum_rpc_scanner.named_parameters["strict"] = LogicalType::BOOLEAN;

	CreateTableFunctionInfo ethereum_rpc_scanner_info(ethereum_rpc_scanner);
	catalog.CreateTableFunction(*con.context, &ethereum_rpc_scanner_info);

	auto &config = DBConfig::GetConfig(db_instance);
	config.AddExtensionOption("eth_node_url", "URL of Ethereum node to be queried", LogicalType::VARCHAR,
	                          "http://127.0.0.1:8545");

	// ETH_EVENT enum type
	auto &temp_catalog = Catalog::GetCatalog(*con.context, TEMP_CATALOG);
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

void ScroogeExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string ScroogeExtension::Name() {
	return "scrooge";
}

std::string ScroogeExtension::Version() const {
#ifdef EXT_VERSION_SCROOGE
	return EXT_VERSION_SCROOGE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(scrooge, loader) {
	duckdb::LoadInternal(loader);
}
}
