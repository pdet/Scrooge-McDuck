#include "scanner/fred.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {
namespace scrooge {

static string ResolveApiKey(ClientContext &context, const string &arg, const string &setting_name,
                              const string &display_name) {
	if (!arg.empty()) {
		return arg;
	}
	Value v;
	if (context.TryGetCurrentSetting(setting_name, v) && !v.IsNull()) {
		auto s = v.GetValue<string>();
		if (!s.empty()) {
			return s;
		}
	}
	throw InvalidInputException("%s is required: pass it as an argument or `SET %s = '...'`",
	                              display_name, setting_name);
}

struct FredFunctionData : public TableFunctionData {
	FredFunctionData(unique_ptr<Connection> conn_p, string series_id_p, string api_key_p, string start_date_p,
	                 string end_date_p)
	    : conn(std::move(conn_p)), series_id(std::move(series_id_p)), api_key(std::move(api_key_p)),
	      start_date(std::move(start_date_p)), end_date(std::move(end_date_p)), done(false) {
	}
	unique_ptr<Connection> conn;
	string series_id;
	string api_key;
	string start_date;
	string end_date;
	bool done;
	unique_ptr<QueryResult> result;
};

// Two binding shapes:
//   FredBindWithKey:    (series_id, api_key [, start, end])
//   FredBindFromSetting:(series_id [, start, end])  — pulls api_key from `fred_api_key` setting
static unique_ptr<FunctionData> FredBindShared(ClientContext &context, const string &series_id,
                                                 const string &api_key_arg, const string &start_date,
                                                 const string &end_date) {
	if (series_id.empty()) {
		throw InvalidInputException("series_id cannot be empty");
	}
	auto api_key = ResolveApiKey(context, api_key_arg, "fred_api_key", "FRED API key");
	return make_uniq<FredFunctionData>(make_uniq<Connection>(*context.db), series_id, api_key,
	                                     start_date, end_date);
}

static unique_ptr<FunctionData> FredBindWithKey(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto series_id = input.inputs[0].GetValue<string>();
	auto api_key = input.inputs[1].GetValue<string>();
	string start_date, end_date;
	if (input.inputs.size() > 2 && !input.inputs[2].IsNull()) {
		start_date = input.inputs[2].GetValue<string>();
	}
	if (input.inputs.size() > 3 && !input.inputs[3].IsNull()) {
		end_date = input.inputs[3].GetValue<string>();
	}
	auto result = FredBindShared(context, series_id, api_key, start_date, end_date);

	// Define output columns
	return_types.emplace_back(LogicalType::DATE);
	names.emplace_back("date");

	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("value");

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("series_id");

	return std::move(result);
}

static unique_ptr<FunctionData> FredBindFromSetting(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto series_id = input.inputs[0].GetValue<string>();
	auto result = FredBindShared(context, series_id, "", "", "");
	return_types.emplace_back(LogicalType::DATE);
	names.emplace_back("date");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("value");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("series_id");
	return std::move(result);
}

static void FredScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (FredFunctionData &)*data_p.bind_data;
	if (data.done) {
		return;
	}

	// Execute the query on first call
	if (!data.result) {
		string url = "https://api.stlouisfed.org/fred/series/observations?series_id=" + data.series_id +
		             "&api_key=" + data.api_key + "&file_type=json";
		if (!data.start_date.empty()) {
			url += "&observation_start=" + data.start_date;
		}
		if (!data.end_date.empty()) {
			url += "&observation_end=" + data.end_date;
		}

		// FRED returns: {"observations": [{"date": "2024-01-01", "value": "123.45"}, ...]}
		// The value "." means missing data in FRED → we convert to NULL
		string query = "SELECT obs.date::DATE AS date, "
		               "CASE WHEN obs.value = '.' THEN NULL ELSE obs.value::DOUBLE END AS value, "
		               "'" +
		               data.series_id +
		               "' AS series_id "
		               "FROM (SELECT unnest(observations) AS obs FROM read_json('" +
		               url + "')) sub;";

		auto rel = data.conn->RelationFromQuery(query);
		data.result = rel->Execute();
	}

	auto result_chunk = data.result->Fetch();
	if (!result_chunk || result_chunk->size() == 0) {
		data.done = true;
		return;
	}
	output.Move(*result_chunk);
}

void RegisterFredScanner(Connection &conn, Catalog &catalog) {
	auto &config = DBConfig::GetConfig(*conn.context->db);
	config.AddExtensionOption(
	    "fred_api_key",
	    "FRED API key used by fred_series() when none is passed as an argument",
	    LogicalType::VARCHAR, "");

	TableFunctionSet fred_set("fred_series");
	// With explicit api_key (existing).
	fred_set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, FredScan, FredBindWithKey));
	fred_set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                     LogicalType::VARCHAR, LogicalType::VARCHAR}, FredScan, FredBindWithKey));
	// 1-arg form pulls api_key from the `fred_api_key` setting. For date
	// filtering with the setting, pass an empty string for api_key:
	//   fred_series('GDP', '', '2020-01-01', '2024-01-01')
	fred_set.AddFunction(TableFunction({LogicalType::VARCHAR}, FredScan, FredBindFromSetting));
	CreateTableFunctionInfo fred_info(fred_set);
	catalog.CreateFunction(*conn.context, fred_info);
}

} // namespace scrooge
} // namespace duckdb
