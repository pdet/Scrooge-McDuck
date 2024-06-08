//===----------------------------------------------------------------------===//
//                         Scrooge
//
// functions/scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace scrooge {
struct YahooScanner {
  static unique_ptr<FunctionData> Bind(ClientContext &context,
                                       TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types,
                                       vector<string> &names);
  static void Scan(ClientContext &context, TableFunctionInput &data_p,
                   DataChunk &output);
};

struct PortfolioFrontier {
  static unique_ptr<FunctionData> Bind(ClientContext &context,
                                       TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types,
                                       vector<string> &names);
  static void Scan(ClientContext &context, TableFunctionInput &data_p,
                   DataChunk &output);
};

struct EthRPC {
  static unique_ptr<FunctionData> Bind(ClientContext &context,
                                       TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types,
                                       vector<string> &names);
  static void Scan(ClientContext &context, TableFunctionInput &data_p,
                   DataChunk &output);
  static unique_ptr<LocalTableFunctionState>

  InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
            GlobalTableFunctionState *global_state_p);

  static unique_ptr<GlobalTableFunctionState>
  InitGlobal(ClientContext &context, TableFunctionInitInput &input);
};
} // namespace scrooge

} // namespace duckdb
