#include "scanner/extra_scanners.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {
namespace scrooge {

static string ResolvePolygonKey(ClientContext &context, const string &arg) {
	if (!arg.empty()) {
		return arg;
	}
	Value v;
	if (context.TryGetCurrentSetting("polygon_api_key", v) && !v.IsNull()) {
		auto s = v.GetValue<string>();
		if (!s.empty()) {
			return s;
		}
	}
	throw InvalidInputException(
	    "Polygon API key is required: pass it as an argument or `SET polygon_api_key = '...'`");
}

// ──────────────────────────────────────────────────────────────
// polygon_aggs(symbol, multiplier, timespan, from_date, to_date, api_key)
//
// Polygon.io aggregate (OHLCV) bars endpoint.
//   timespan ∈ minute|hour|day|week|month|quarter|year
//   from/to_date: 'YYYY-MM-DD'
// Free tier requires an API key but allows historical data.
// ──────────────────────────────────────────────────────────────

struct PolygonData : public TableFunctionData {
	unique_ptr<Connection> conn;
	string symbol;
	int64_t multiplier;
	string timespan;
	string from_date;
	string to_date;
	string api_key;
	bool done = false;
	unique_ptr<QueryResult> result;
};

static unique_ptr<FunctionData> PolygonBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<PolygonData>();
	data->conn = make_uniq<Connection>(*context.db);
	data->symbol = input.inputs[0].GetValue<string>();
	data->multiplier = input.inputs[1].GetValue<int64_t>();
	data->timespan = input.inputs[2].GetValue<string>();
	data->from_date = input.inputs[3].GetValue<string>();
	data->to_date = input.inputs[4].GetValue<string>();
	data->api_key = ResolvePolygonKey(context, input.inputs[5].GetValue<string>());

	if (data->symbol.empty()) {
		throw InvalidInputException("polygon_aggs: symbol required");
	}
	if (data->multiplier <= 0) {
		throw InvalidInputException("polygon_aggs: multiplier must be positive");
	}

	names = {"timestamp", "open", "high", "low", "close", "volume", "vwap", "transactions"};
	return_types = {LogicalType::TIMESTAMP, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::BIGINT};
	return std::move(data);
}

static void PolygonScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &d = (PolygonData &)*input.bind_data;
	if (d.done) {
		output.SetCardinality(0);
		return;
	}
	if (!d.result) {
		string url = "https://api.polygon.io/v2/aggs/ticker/" + d.symbol + "/range/" +
		              std::to_string(d.multiplier) + "/" + d.timespan + "/" +
		              d.from_date + "/" + d.to_date +
		              "?adjusted=true&sort=asc&limit=50000&apiKey=" + d.api_key;
		// Polygon shape: {"results": [{"t": ms, "o", "h", "l", "c", "v", "vw", "n"}, ...]}
		string q =
		    "SELECT epoch_ms(r.t::BIGINT) AS timestamp, "
		    "r.o::DOUBLE AS open, r.h::DOUBLE AS high, r.l::DOUBLE AS low, "
		    "r.c::DOUBLE AS close, r.v::DOUBLE AS volume, "
		    "TRY_CAST(r.vw AS DOUBLE) AS vwap, "
		    "TRY_CAST(r.n AS BIGINT) AS transactions "
		    "FROM (SELECT unnest(results) AS r FROM read_json('" + url + "')) sub";
		d.result = d.conn->Query(q);
		if (d.result->HasError()) {
			throw InvalidInputException("polygon_aggs query failed: %s", d.result->GetError());
		}
	}
	auto chunk = d.result->Fetch();
	if (!chunk || chunk->size() == 0) {
		d.done = true;
		output.SetCardinality(0);
		return;
	}
	output.Move(*chunk);
}

// 5-arg overload: pulls api_key from `polygon_api_key` setting.
struct PolygonNoKeyData : public PolygonData {};

static unique_ptr<FunctionData> PolygonBindNoKey(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<PolygonData>();
	data->conn = make_uniq<Connection>(*context.db);
	data->symbol = input.inputs[0].GetValue<string>();
	data->multiplier = input.inputs[1].GetValue<int64_t>();
	data->timespan = input.inputs[2].GetValue<string>();
	data->from_date = input.inputs[3].GetValue<string>();
	data->to_date = input.inputs[4].GetValue<string>();
	data->api_key = ResolvePolygonKey(context, "");
	if (data->symbol.empty()) {
		throw InvalidInputException("polygon_aggs: symbol required");
	}
	if (data->multiplier <= 0) {
		throw InvalidInputException("polygon_aggs: multiplier must be positive");
	}
	names = {"timestamp", "open", "high", "low", "close", "volume", "vwap", "transactions"};
	return_types = {LogicalType::TIMESTAMP, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::BIGINT};
	return std::move(data);
}

void RegisterPolygonScanner(Connection &conn, Catalog &catalog) {
	auto &config = DBConfig::GetConfig(*conn.context->db);
	config.AddExtensionOption(
	    "polygon_api_key",
	    "Polygon.io API key used by polygon_aggs() when none is passed as an argument",
	    LogicalType::VARCHAR, "");

	TableFunctionSet set("polygon_aggs");
	set.AddFunction(TableFunction(
	    {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR,
	     LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    PolygonScan, PolygonBind));
	set.AddFunction(TableFunction(
	    {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR,
	     LogicalType::VARCHAR, LogicalType::VARCHAR},
	    PolygonScan, PolygonBindNoKey));
	CreateTableFunctionInfo info(set);
	catalog.CreateFunction(*conn.context, info);
}

} // namespace scrooge
} // namespace duckdb
