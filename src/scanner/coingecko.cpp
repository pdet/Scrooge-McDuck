#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/exception.hpp"
#include "scanner/coingecko.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/main/connection.hpp"
#include "duckdb/catalog/catalog.hpp"
#endif

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// coingecko(coin_id, vs_currency, days)
//
// Fetches historical OHLC data from CoinGecko (free, no API key).
// Example: SELECT * FROM coingecko('bitcoin', 'usd', 365);
// ──────────────────────────────────────────────────────────────

struct CoinGeckoData : public TableFunctionData {
	string coin_id;
	string vs_currency;
	int64_t days;
	bool done;
};

static unique_ptr<FunctionData> CoinGeckoBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<CoinGeckoData>();
	data->coin_id = input.inputs[0].GetValue<string>();
	data->vs_currency = input.inputs[1].GetValue<string>();
	data->days = input.inputs[2].GetValue<int64_t>();
	data->done = false;

	names = {"timestamp", "open", "high", "low", "close"};
	return_types = {LogicalType::TIMESTAMP, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::DOUBLE};
	return std::move(data);
}

static void CoinGeckoScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = (CoinGeckoData &)*input.bind_data;
	if (data.done) return;
	data.done = true;

	// Use DuckDB's httpfs to fetch from CoinGecko
	string url = "https://api.coingecko.com/api/v3/coins/" + data.coin_id +
	             "/ohlc?vs_currency=" + data.vs_currency +
	             "&days=" + to_string(data.days);

	auto &db = DatabaseInstance::GetDatabase(context);
	Connection conn(db);

	// CoinGecko returns JSON: [[timestamp_ms, open, high, low, close], ...]
	string query = "SELECT "
	               "epoch_ms(el[1]::BIGINT) as timestamp, "
	               "el[2]::DOUBLE as open, "
	               "el[3]::DOUBLE as high, "
	               "el[4]::DOUBLE as low, "
	               "el[5]::DOUBLE as close "
	               "FROM (SELECT unnest(data) as el FROM "
	               "read_json_auto('" + url + "', format='array'))";

	auto result = conn.Query(query);
	if (result->HasError()) {
		throw InvalidInputException("CoinGecko query failed: %s", result->GetError());
	}

	idx_t row_count = 0;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) break;
		for (idx_t col = 0; col < chunk->ColumnCount() && col < output.ColumnCount(); col++) {
			VectorOperations::Copy(chunk->data[col], output.data[col], chunk->size(), 0, row_count);
		}
		row_count += chunk->size();
	}
	output.SetCardinality(row_count);
}

void RegisterCoinGeckoScanner(Connection &conn, Catalog &catalog) {
	TableFunctionSet set("coingecko");
	TableFunction func("coingecko",
	                    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
	                    CoinGeckoScan, CoinGeckoBind);
	set.AddFunction(func);
	CreateTableFunctionInfo info(set);
	catalog.CreateFunction(*conn.context, info);
}

} // namespace scrooge
} // namespace duckdb
