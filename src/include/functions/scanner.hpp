//===----------------------------------------------------------------------===//
//                         Scrooge
//
// functions/scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace scrooge {
struct YahooScanner {
  static duckdb::unique_ptr<duckdb::FunctionData>
  Bind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
       std::vector<duckdb::LogicalType> &return_types,
       std::vector<std::string> &names);
  static void Scan(duckdb::ClientContext &context,
                   duckdb::TableFunctionInput &data_p,
                   duckdb::DataChunk &output);
};

struct PortfolioFrontier {
  static duckdb::unique_ptr<duckdb::FunctionData>
  Bind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
       std::vector<duckdb::LogicalType> &return_types,
       std::vector<std::string> &names);
  static void Scan(duckdb::ClientContext &context,
                   duckdb::TableFunctionInput &data_p,
                   duckdb::DataChunk &output);
};
} // namespace scrooge
