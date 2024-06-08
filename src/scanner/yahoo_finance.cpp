#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/read_csv_relation.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "functions/scanner.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {
namespace scrooge {

int64_t IntervalInEpoch(string &interval) {
  // ble string checkaroo
  if (interval == "1d") {
    return Interval::SECS_PER_DAY;
  }
  if (interval == "5d") {
    return 5 * Interval::SECS_PER_DAY;
  }
  if (interval == "1wk") {
    return 7 * Interval::SECS_PER_DAY;
  }
  if (interval == "1mo") {
    return 30 * Interval::SECS_PER_DAY;
  }
  if (interval == "3mo") {
    return 90 * Interval::SECS_PER_DAY;
  }
  return 0;
}

struct YahooFunctionData : public TableFunctionData {
  YahooFunctionData(unique_ptr<Connection> conn_p, vector<string> &symbol_p,
                    int64_t from_epoch_p, int64_t to_epoch_p,
                    string &interval_p)
      : conn(std::move(conn_p)), symbols(symbol_p), from_epoch(from_epoch_p),
        to_epoch(to_epoch_p), interval(interval_p) {
    auto interval_epoch = IntervalInEpoch(interval);
    // We have to do this hacky thing to keep yahoo finance requests happy
    int expected_tuples = (to_epoch - from_epoch) / interval_epoch + 1;
    if (expected_tuples > 60) {
      increment_epoch = (to_epoch - from_epoch) / (expected_tuples / 60 + 1);
    } else {
      increment_epoch = (to_epoch - from_epoch);
    }
    from_epoch_og = from_epoch;
    cur_to_epoch = from_epoch + increment_epoch < to_epoch
                       ? from_epoch + increment_epoch
                       : to_epoch;
    symbol = symbols[0];
  }
  shared_ptr<Relation> plan;
  unique_ptr<Connection> conn;
  vector<string> symbols;
  string symbol;
  idx_t cur_symbol_idx = 0;
  int64_t from_epoch;
  int64_t from_epoch_og;
  int64_t cur_to_epoch;
  int64_t to_epoch;
  string interval;
  int64_t increment_epoch;
};

shared_ptr<ProjectionRelation> GeneratePlan(YahooFunctionData &bind_data) {
  if (bind_data.cur_to_epoch > bind_data.to_epoch) {
    if (bind_data.cur_symbol_idx + 1 == bind_data.symbols.size()) {
      // we are done
      return nullptr;
    }
    bind_data.cur_symbol_idx++;
    bind_data.symbol = bind_data.symbols[bind_data.cur_symbol_idx];
    bind_data.from_epoch = bind_data.from_epoch_og;
    bind_data.cur_to_epoch =
        bind_data.from_epoch + bind_data.increment_epoch < bind_data.to_epoch
            ? bind_data.from_epoch + bind_data.increment_epoch
            : bind_data.to_epoch;
  }
  auto from = to_string(bind_data.from_epoch);
  auto to = to_string(bind_data.cur_to_epoch);
  // Increment start
  bind_data.from_epoch += bind_data.increment_epoch;
  bind_data.cur_to_epoch += bind_data.increment_epoch;

  string url = "https://query1.finance.yahoo.com/v7/finance/download/" +
               bind_data.symbol + "?period1=" + from + "&period2=" + to +
               "&interval=" + bind_data.interval + "&events=history";
  vector<ColumnDefinition> column_def;
  column_def.emplace_back("Date", LogicalType::DATE);
  column_def.emplace_back("Open", LogicalType::DOUBLE);
  column_def.emplace_back("High", LogicalType::DOUBLE);
  column_def.emplace_back("Low", LogicalType::DOUBLE);
  column_def.emplace_back("Close", LogicalType::DOUBLE);
  column_def.emplace_back("Adj Close", LogicalType::DOUBLE);
  column_def.emplace_back("Volume", LogicalType::HUGEINT);
  named_parameter_map_t options;
  vector<string> urls{url};
  auto csv_rel = make_shared_ptr<ReadCSVRelation>(bind_data.conn->context, urls,
                                                  duckdb::move(options));
  csv_rel->AddNamedParameter("HEADER", true);
  csv_rel->AddNamedParameter("NULLSTR", "null");
  vector<unique_ptr<ParsedExpression>> expressions;
  auto star_exp = make_uniq<StarExpression>(csv_rel->name);
  vector<string> aliases;
  if (bind_data.symbols.size() > 1) {
    auto constant_expression = make_uniq<ConstantExpression>(bind_data.symbol);
    expressions.emplace_back(std::move(constant_expression));
    aliases.emplace_back("symbol");
  }
  expressions.emplace_back(std::move(star_exp));
  aliases.emplace_back("star");

  auto proj_rel = make_shared_ptr<ProjectionRelation>(
      csv_rel, std::move(expressions), aliases);
  return proj_rel;
}

void ValidInterval(string &interval) {
  unordered_set<string> valid_interval{"1d", "5d", "1wk", "1mo", "3mo"};
  if (valid_interval.find(interval) == valid_interval.end()) {
    string accepted_intervals =
        "1d: 1 day interval\n5d: 5 day interval\n1wk: 1 week interval\n1mo: 1 "
        "month interval\n3mo: 3 month interval\n";
    throw InvalidInputException(
        "Interval is not valid, you should use one of the following valid "
        "intervals: \n" +
        accepted_intervals);
  }
}

unique_ptr<FunctionData> YahooScanner::Bind(ClientContext &context,
                                            TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types,
                                            vector<string> &names) {
  if (input.inputs[0].type() != LogicalType::VARCHAR &&
      input.inputs[0].type() != LogicalType::LIST(LogicalType::VARCHAR)) {
    throw InvalidInputException(
        "Symbol must be either a String or a List of strings");
  }
  if (input.inputs[1].type() != LogicalType::VARCHAR &&
      input.inputs[1].type() != LogicalType::DATE) {
    throw InvalidInputException(
        "Start Period must be a Date or a Date-VARCHAR ");
  }
  if (input.inputs[2].type() != LogicalType::VARCHAR &&
      input.inputs[2].type() != LogicalType::DATE) {
    throw InvalidInputException(
        "Start Period must be a Date or a Date-VARCHAR ");
  }
  vector<string> symbols;
  if (input.inputs[0].type() == LogicalType::VARCHAR) {
    symbols.emplace_back(input.inputs[0].GetValueUnsafe<string>());
  } else {
    auto values = ListValue::GetChildren(input.inputs[0]);
    for (auto &value : values) {
      symbols.emplace_back(value.GetValueUnsafe<string>());
    }
  }
  auto from_date = input.inputs[1].GetValue<date_t>();
  auto to_date = input.inputs[2].GetValue<date_t>();
  auto from = Date::Epoch(input.inputs[1].GetValue<date_t>());
  auto to = Date::Epoch(input.inputs[2].GetValue<date_t>());
  auto interval = input.inputs[3].GetValue<string>();
  ValidInterval(interval);
  if (to_date <= from_date) {
    throw InvalidInputException(
        "The End period must be higher than the start period");
  }
  auto result = make_uniq<YahooFunctionData>(make_uniq<Connection>(*context.db),
                                             symbols, from, to, interval);
  result->plan = GeneratePlan(*result);
  for (auto &column : result->plan->Columns()) {
    return_types.emplace_back(column.Type());
    names.emplace_back(column.Name());
  }
  return std::move(result);
}
void YahooScanner::Scan(ClientContext &context, TableFunctionInput &data_p,
                        DataChunk &output) {

  auto &data = (YahooFunctionData &)*data_p.bind_data;
  if (!data.plan) {
    return;
  }
  unique_ptr<QueryResult> res = data.plan->Execute();
  auto result_chunk = res->Fetch();
  if (!result_chunk) {
    return;
  }
  output.Move(*result_chunk);
  data.plan = GeneratePlan(data);
}
} // namespace scrooge
} // namespace duckdb