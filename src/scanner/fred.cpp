#include "scanner/fred.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {
namespace scrooge {

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

unique_ptr<FunctionData> FredScanner::Bind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].type() != LogicalType::VARCHAR) {
		throw InvalidInputException("series_id must be a VARCHAR");
	}
	if (input.inputs[1].type() != LogicalType::VARCHAR) {
		throw InvalidInputException("api_key must be a VARCHAR");
	}

	auto series_id = input.inputs[0].GetValue<string>();
	auto api_key = input.inputs[1].GetValue<string>();

	if (series_id.empty()) {
		throw InvalidInputException("series_id cannot be empty");
	}
	if (api_key.empty()) {
		throw InvalidInputException("api_key cannot be empty");
	}

	string start_date;
	string end_date;

	if (input.inputs.size() > 2 && !input.inputs[2].IsNull()) {
		start_date = input.inputs[2].GetValue<string>();
	}
	if (input.inputs.size() > 3 && !input.inputs[3].IsNull()) {
		end_date = input.inputs[3].GetValue<string>();
	}

	auto result =
	    make_uniq<FredFunctionData>(make_uniq<Connection>(*context.db), series_id, api_key, start_date, end_date);

	// Define output columns
	return_types.emplace_back(LogicalType::DATE);
	names.emplace_back("date");

	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("value");

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("series_id");

	return std::move(result);
}

void FredScanner::Scan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
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

} // namespace scrooge
} // namespace duckdb
