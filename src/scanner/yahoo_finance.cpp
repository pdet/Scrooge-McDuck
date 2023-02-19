#include "functions/scanner.hpp"
#include "duckdb/execution/operator/persistent/csv_reader_options.hpp"
#include "duckdb/main/relation/read_csv_relation.hpp"

namespace scrooge {

struct YahooFunctionData : public duckdb::TableFunctionData {
  YahooFunctionData() = default;
  std::shared_ptr<duckdb::Relation> plan;
  std::unique_ptr<duckdb::QueryResult> res;
  std::unique_ptr<duckdb::Connection> conn;
};

void ValidInterval(std::string &interval) {
  std::unordered_set<std::string> valid_interval{"1d", "5d", "1wk", "1mo",
                                                 "3mo"};
  if (valid_interval.find(interval) == valid_interval.end()) {
    std::string accepted_intervals =
        "1d: 1 day interval\n5d: 5 day interval\n1wk: 1 week interval\n1mo: 1 "
        "month interval\n3mo: 3 month interval\n";
    throw duckdb::InvalidInputException(
        "Interval is not valid, you should use one of the following valid "
        "intervals: \n" +
        accepted_intervals);
  }
}

std::unique_ptr<duckdb::FunctionData>
YahooScanner::Bind(duckdb::ClientContext &context,
                   duckdb::TableFunctionBindInput &input,
                   std::vector<duckdb::LogicalType> &return_types,
                   std::vector<std::string> &names) {
  if (input.inputs[1].type() != duckdb::LogicalType::VARCHAR &&
      input.inputs[1].type() != duckdb::LogicalType::DATE) {
    throw duckdb::InvalidInputException(
        "Start Period must be a Date or a Date-VARCHAR ");
  }
  if (input.inputs[2].type() != duckdb::LogicalType::VARCHAR &&
      input.inputs[2].type() != duckdb::LogicalType::DATE) {
    throw duckdb::InvalidInputException(
        "Start Period must be a Date or a Date-VARCHAR ");
  }
  auto result = duckdb::make_unique<YahooFunctionData>();
  result->conn = duckdb::make_unique<duckdb::Connection>(*context.db);
  auto symbol = input.inputs[0].GetValueUnsafe<std::string>();
  auto from_date = input.inputs[1].GetValue<duckdb::date_t>();
  auto to_date = input.inputs[2].GetValue<duckdb::date_t>();
  auto from = std::to_string(
      duckdb::Date::Epoch(input.inputs[1].GetValue<duckdb::date_t>()));
  auto to = std::to_string(
      duckdb::Date::Epoch(input.inputs[2].GetValue<duckdb::date_t>()));
  auto interval = input.inputs[3].GetValue<std::string>();
  ValidInterval(interval);
  if (to_date <= from_date) {
    throw duckdb::InvalidInputException(
        "The End period must be higher than the start period");
  }
  std::string url = "https://query1.finance.yahoo.com/v7/finance/download/" +
                    symbol + "?period1=" + from + "&period2=" + to +
                    "&interval=" + interval + "&events=history";
  std::vector<duckdb::ColumnDefinition> column_def;
  column_def.emplace_back("Date", duckdb::LogicalType::DATE);
  column_def.emplace_back("Open", duckdb::LogicalType::DOUBLE);
  column_def.emplace_back("High", duckdb::LogicalType::DOUBLE);
  column_def.emplace_back("Low", duckdb::LogicalType::DOUBLE);
  column_def.emplace_back("Close", duckdb::LogicalType::DOUBLE);
  column_def.emplace_back("Adj Close", duckdb::LogicalType::DOUBLE);
  column_def.emplace_back("Volume", duckdb::LogicalType::HUGEINT);
  auto csv_rel = duckdb::make_shared<duckdb::ReadCSVRelation>(
      result->conn->context, url, std::move(column_def));
  csv_rel->AddNamedParameter("HEADER", true);
  csv_rel->AddNamedParameter("NULLSTR", "null");
  result->plan = csv_rel;

  for (auto &column : result->plan->Columns()) {
    return_types.emplace_back(column.Type());
    names.emplace_back(column.Name());
  }
  return std::move(result);
}
void YahooScanner::Scan(duckdb::ClientContext &context,
                        duckdb::TableFunctionInput &data_p,
                        duckdb::DataChunk &output) {
  auto &data = (YahooFunctionData &)*data_p.bind_data;
  if (!data.res) {
    data.res = data.plan->Execute();
  }
  auto result_chunk = data.res->Fetch();
  if (!result_chunk) {
    return;
  }
  output.Move(*result_chunk);
}
} // namespace scrooge