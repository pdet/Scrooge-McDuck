#include "scanner/extra_scanners.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// alpha_vantage_news(tickers [, time_from [, time_to [, limit]]])
//
// Wraps Alpha Vantage's NEWS_SENTIMENT API. `tickers` is a comma-separated
// string ('AAPL,MSFT'); `time_from`/`time_to` are 'YYYYMMDDTHHMM' strings
// (Alpha Vantage's required format) or empty for no bound; `limit` defaults
// to 50 (max 1000).
//
// Requires the `alpha_vantage_api_key` setting.
//
// Returns one row per article:
//   (time_published TIMESTAMP, title VARCHAR, url VARCHAR, source VARCHAR,
//    summary VARCHAR, overall_sentiment_score DOUBLE,
//    overall_sentiment_label VARCHAR, tickers VARCHAR[])
// ──────────────────────────────────────────────────────────────

namespace {

static string ResolveAVKey(ClientContext &context) {
	Value v;
	if (context.TryGetCurrentSetting("alpha_vantage_api_key", v) && !v.IsNull()) {
		auto s = v.GetValue<string>();
		if (!s.empty()) {
			return s;
		}
	}
	throw InvalidInputException(
	    "Alpha Vantage API key required: SET alpha_vantage_api_key = '...'");
}

struct AVNewsData : public TableFunctionData {
	unique_ptr<Connection> conn;
	string url;
	bool done = false;
	unique_ptr<QueryResult> result;
};

static unique_ptr<FunctionData> AVNewsBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<AVNewsData>();
	data->conn = make_uniq<Connection>(*context.db);
	auto tickers = input.inputs[0].GetValue<string>();
	if (tickers.empty()) {
		throw InvalidInputException("alpha_vantage_news: tickers required");
	}
	string time_from, time_to;
	int64_t limit = 50;
	if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		time_from = input.inputs[1].GetValue<string>();
	}
	if (input.inputs.size() > 2 && !input.inputs[2].IsNull()) {
		time_to = input.inputs[2].GetValue<string>();
	}
	if (input.inputs.size() > 3 && !input.inputs[3].IsNull()) {
		limit = input.inputs[3].GetValue<int64_t>();
		if (limit <= 0 || limit > 1000) {
			throw InvalidInputException("alpha_vantage_news: limit must be in [1, 1000]");
		}
	}

	auto api_key = ResolveAVKey(context);
	data->url = "https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=" + tickers +
	             "&apikey=" + api_key + "&limit=" + std::to_string(limit);
	if (!time_from.empty()) {
		data->url += "&time_from=" + time_from;
	}
	if (!time_to.empty()) {
		data->url += "&time_to=" + time_to;
	}

	names = {"time_published", "title", "url", "source", "summary",
	         "overall_sentiment_score", "overall_sentiment_label", "tickers"};
	return_types = {LogicalType::TIMESTAMP, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DOUBLE,
	                LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)};
	return std::move(data);
}

static void AVNewsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &d = (AVNewsData &)*input.bind_data;
	if (d.done) {
		output.SetCardinality(0);
		return;
	}
	if (!d.result) {
		// Alpha Vantage shape:
		//   { "feed": [ { "title", "url", "time_published": "20240101T120000",
		//                 "source", "summary", "overall_sentiment_score",
		//                 "overall_sentiment_label",
		//                 "ticker_sentiment": [ {"ticker": ...}, ... ] }, ... ] }
		string q =
		    "WITH raw AS (SELECT feed FROM read_json('" + d.url + "')) "
		    "SELECT "
		    "  strptime(art.time_published::VARCHAR, '%Y%m%dT%H%M%S')::TIMESTAMP AS time_published, "
		    "  art.title::VARCHAR AS title, "
		    "  art.url::VARCHAR AS url, "
		    "  art.source::VARCHAR AS source, "
		    "  art.summary::VARCHAR AS summary, "
		    "  art.overall_sentiment_score::DOUBLE AS overall_sentiment_score, "
		    "  art.overall_sentiment_label::VARCHAR AS overall_sentiment_label, "
		    "  list_transform(art.ticker_sentiment, ts -> ts.ticker::VARCHAR) AS tickers "
		    "FROM raw, LATERAL (SELECT unnest(feed) AS art) sub "
		    "ORDER BY time_published DESC";
		d.result = d.conn->Query(q);
		if (d.result->HasError()) {
			throw InvalidInputException("alpha_vantage_news query failed: %s", d.result->GetError());
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

} // namespace

void RegisterAlphaVantageNewsScanner(Connection &conn, Catalog &catalog) {
	auto &config = DBConfig::GetConfig(*conn.context->db);
	config.AddExtensionOption(
	    "alpha_vantage_api_key",
	    "Alpha Vantage API key used by alpha_vantage_news()",
	    LogicalType::VARCHAR, "");

	TableFunctionSet set("alpha_vantage_news");
	set.AddFunction(TableFunction({LogicalType::VARCHAR}, AVNewsScan, AVNewsBind));
	set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                AVNewsScan, AVNewsBind));
	set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                AVNewsScan, AVNewsBind));
	set.AddFunction(TableFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
	    AVNewsScan, AVNewsBind));
	CreateTableFunctionInfo info(set);
	catalog.CreateFunction(*conn.context, info);
}

} // namespace scrooge
} // namespace duckdb
