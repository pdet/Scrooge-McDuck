//===----------------------------------------------------------------------===//
//                         Scrooge
//
// scanner/fred.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace scrooge {

struct FredScanner {
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);
	static void Scan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
};

} // namespace scrooge
} // namespace duckdb
