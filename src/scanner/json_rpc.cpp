#include "scanner/extra_scanners.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"
#include "util/http_util.hpp"
#include "json.hpp"

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// json_rpc(url, method [, params_json])
//
// Generic JSON-RPC 2.0 client. Works with any node that speaks the
// standard format: Bitcoin, Solana (with adapters), BSC, Avalanche,
// Polygon RPC, Optimism, Arbitrum, etc.
//
// `params_json` is a VARCHAR holding a JSON value (typically an array).
// Defaults to '[]' when omitted.
//
// Returns a single row:
//   (jsonrpc VARCHAR, id BIGINT, result VARCHAR, error VARCHAR)
//
// `result` and `error` are returned as raw JSON strings — parse with
// DuckDB's json extension.
// ──────────────────────────────────────────────────────────────

namespace {

using nlohmann::json;

struct JsonRpcData : public TableFunctionData {
	string url;
	string method;
	string params_json;
	bool done = false;
	string jsonrpc;
	int64_t id = 1;
	string result_str;
	string error_str;
	bool has_error = false;
};

static unique_ptr<FunctionData> JsonRpcBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<JsonRpcData>();
	data->url = input.inputs[0].GetValue<string>();
	data->method = input.inputs[1].GetValue<string>();
	data->params_json = (input.inputs.size() > 2 && !input.inputs[2].IsNull())
	                        ? input.inputs[2].GetValue<string>() : "[]";
	if (data->url.empty()) {
		throw InvalidInputException("json_rpc: url required");
	}
	if (data->method.empty()) {
		throw InvalidInputException("json_rpc: method required");
	}
	// Validate params JSON eagerly so the user gets a parse error at bind.
	try {
		(void)json::parse(data->params_json);
	} catch (const std::exception &e) {
		throw InvalidInputException("json_rpc: params is not valid JSON: %s", e.what());
	}

	names = {"jsonrpc", "id", "result", "error"};
	return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(data);
}

static void JsonRpcScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &d = (JsonRpcData &)*input.bind_data;
	if (d.done) {
		output.SetCardinality(0);
		return;
	}

	// Build the JSON-RPC envelope.
	json envelope = {
	    {"jsonrpc", "2.0"},
	    {"method", d.method},
	    {"params", json::parse(d.params_json)},
	    {"id", d.id},
	};
	auto body = envelope.dump();

	auto res = HTTPUtil::Request(d.url, body);
	auto reply = json::parse(res->body);

	d.jsonrpc = reply.value("jsonrpc", string("2.0"));
	if (reply.contains("id") && reply["id"].is_number_integer()) {
		d.id = reply["id"].get<int64_t>();
	}
	if (reply.contains("result")) {
		d.result_str = reply["result"].dump();
	}
	if (reply.contains("error") && !reply["error"].is_null()) {
		d.error_str = reply["error"].dump();
		d.has_error = true;
	}

	auto rpc_v = FlatVector::GetData<string_t>(output.data[0]);
	auto id_v = FlatVector::GetData<int64_t>(output.data[1]);
	auto res_v = FlatVector::GetData<string_t>(output.data[2]);
	auto &res_valid = FlatVector::Validity(output.data[2]);
	auto err_v = FlatVector::GetData<string_t>(output.data[3]);
	auto &err_valid = FlatVector::Validity(output.data[3]);

	rpc_v[0] = StringVector::AddString(output.data[0], d.jsonrpc);
	id_v[0] = d.id;
	if (d.result_str.empty()) {
		res_valid.SetInvalid(0);
	} else {
		res_v[0] = StringVector::AddString(output.data[2], d.result_str);
	}
	if (d.has_error) {
		err_v[0] = StringVector::AddString(output.data[3], d.error_str);
	} else {
		err_valid.SetInvalid(0);
	}
	output.SetCardinality(1);
	d.done = true;
}

} // namespace

void RegisterJsonRpcScanner(Connection &conn, Catalog &catalog) {
	TableFunctionSet set("json_rpc");
	set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                JsonRpcScan, JsonRpcBind));
	set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                JsonRpcScan, JsonRpcBind));
	CreateTableFunctionInfo info(set);
	catalog.CreateFunction(*conn.context, info);
}

} // namespace scrooge
} // namespace duckdb
