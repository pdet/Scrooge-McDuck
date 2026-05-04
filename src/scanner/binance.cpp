#include "scanner/extra_scanners.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// binance_klines(symbol, interval [, limit])
//
// Public Binance spot klines endpoint (no auth required).
//   symbol: 'BTCUSDT', 'ETHUSDT', etc.
//   interval: '1m','5m','15m','1h','4h','1d','1w','1M'
//   limit:   1..1000 (default 500)
// ──────────────────────────────────────────────────────────────

struct BinanceData : public TableFunctionData {
	unique_ptr<Connection> conn;
	string symbol;
	string interval;
	int64_t limit;
	bool done = false;
	unique_ptr<QueryResult> result;
};

static unique_ptr<FunctionData> BinanceBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<BinanceData>();
	data->conn = make_uniq<Connection>(*context.db);
	data->symbol = input.inputs[0].GetValue<string>();
	data->interval = input.inputs[1].GetValue<string>();
	data->limit = input.inputs.size() > 2 && !input.inputs[2].IsNull()
	                  ? input.inputs[2].GetValue<int64_t>() : 500;
	if (data->symbol.empty() || data->interval.empty()) {
		throw InvalidInputException("binance_klines: symbol and interval required");
	}
	if (data->limit <= 0 || data->limit > 1000) {
		throw InvalidInputException("binance_klines: limit must be in [1, 1000]");
	}

	names = {"open_time", "open", "high", "low", "close", "volume",
	         "close_time", "quote_volume", "trades"};
	return_types = {LogicalType::TIMESTAMP, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::TIMESTAMP, LogicalType::DOUBLE, LogicalType::BIGINT};
	return std::move(data);
}

static void BinanceScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &d = (BinanceData &)*input.bind_data;
	if (d.done) {
		output.SetCardinality(0);
		return;
	}
	if (!d.result) {
		string url = "https://api.binance.com/api/v3/klines?symbol=" + d.symbol +
		              "&interval=" + d.interval + "&limit=" + std::to_string(d.limit);
		// Binance returns an array of arrays:
		// [[open_time, "open", "high", "low", "close", "volume", close_time,
		//   "quote_volume", trades, ...], ...]
		string q =
		    "SELECT epoch_ms(el[1]::BIGINT) AS open_time, "
		    "el[2]::DOUBLE AS open, el[3]::DOUBLE AS high, el[4]::DOUBLE AS low, "
		    "el[5]::DOUBLE AS close, el[6]::DOUBLE AS volume, "
		    "epoch_ms(el[7]::BIGINT) AS close_time, "
		    "el[8]::DOUBLE AS quote_volume, el[9]::BIGINT AS trades "
		    "FROM (SELECT unnest(data) AS el FROM "
		    "read_json_auto('" + url + "', format='array')) sub";
		d.result = d.conn->Query(q);
		if (d.result->HasError()) {
			throw InvalidInputException("binance_klines query failed: %s", d.result->GetError());
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

void RegisterBinanceScanner(Connection &conn, Catalog &catalog) {
	TableFunctionSet set("binance_klines");
	set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                BinanceScan, BinanceBind));
	set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
	                                BinanceScan, BinanceBind));
	CreateTableFunctionInfo info(set);
	catalog.CreateFunction(*conn.context, info);
}

} // namespace scrooge
} // namespace duckdb
