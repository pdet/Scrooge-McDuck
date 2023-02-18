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

std::unique_ptr<duckdb::FunctionData>
YahooScanner::Bind(duckdb::ClientContext &context,
                   duckdb::TableFunctionBindInput &input,
                   std::vector<duckdb::LogicalType> &return_types,
                   std::vector<std::string> &names) {
  auto result = duckdb::make_unique<YahooFunctionData>();
  result->conn = duckdb::make_unique<duckdb::Connection>(*context.db);
  auto symbol = input.inputs[0].GetValueUnsafe<std::string>();
  auto from = std::to_string(
      duckdb::Date::Epoch(input.inputs[1].GetValue<duckdb::date_t>()));
  auto to = std::to_string(
      duckdb::Date::Epoch(input.inputs[2].GetValue<duckdb::date_t>()));
  auto interval =
      std::to_string(input.inputs[3].GetValue<duckdb::interval_t>().days) + 'd';
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