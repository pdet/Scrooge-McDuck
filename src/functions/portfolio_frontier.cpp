#include <cmath>
#include <vector>
#include "functions/scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb/main/relation/read_csv_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {
namespace scrooge {

struct Asset {
  string symbol;
  double volatility;
  double expected_return;
};

struct Portfolio {
  vector<Asset> assets;
  vector<vector<double>> weights;
};

double calculate_portfolio_return(Portfolio &portfolio) {
  double portfolio_return = 0.0;
  for (int i = 0; i < portfolio.assets.size(); i++) {
    portfolio_return +=
        portfolio.weights.back()[i] * portfolio.assets[i].expected_return;
  }
  return portfolio_return;
}

double calculate_portfolio_volatility(Portfolio &portfolio) {
  double portfolio_volatility = 0.0;
  for (int i = 0; i < portfolio.assets.size(); i++) {
    portfolio_volatility +=
        pow(portfolio.weights.back()[i] * portfolio.assets[i].volatility, 2.0);
  }
  return sqrt(portfolio_volatility);
}

vector<double> generate_random_weights(int n) {
  vector<double> weights(n);
  double sum = 0.0;
  for (int i = 0; i < n; i++) {
    weights[i] = (double)rand() / RAND_MAX;
    sum += weights[i];
  }
  for (int i = 0; i < n; i++) {
    weights[i] /= sum;
  }
  return weights;
}

vector<pair<double, double>> calculate_efficient_frontier(Portfolio &portfolio,
                                                          int n) {
  vector<pair<double, double>> efficient_frontier(n);
  for (int i = 0; i < n; i++) {
    portfolio.weights.emplace_back(
        generate_random_weights(portfolio.assets.size()));
    double portfolio_return = calculate_portfolio_return(portfolio);
    double portfolio_volatility = calculate_portfolio_volatility(portfolio);
    efficient_frontier[i] = make_pair(portfolio_volatility, portfolio_return);
  }
  //  sort(efficient_frontier.begin(), efficient_frontier.end());
  return efficient_frontier;
}

struct PortfolioFrontierData : public TableFunctionData {
  Portfolio portfolio;
  vector<pair<double, double>> portfolio_stats;
  int n;
  int cur = 0;
};

unique_ptr<FunctionData>
PortfolioFrontier::Bind(ClientContext &context, TableFunctionBindInput &input,
                        vector<LogicalType> &return_types,
                        vector<string> &names) {
  auto result = make_uniq<PortfolioFrontierData>();
  auto conn = make_uniq<Connection>(*context.db);
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
  result->n = input.inputs[3].GetValue<int>();
  vector<Value> parameters{input.inputs[0], input.inputs[1], input.inputs[2],
                           "1d"};

  auto tbl_rel = make_shared_ptr<TableFunctionRelation>(
      conn->context, "yahoo_finance", std::move(parameters));
  vector<unique_ptr<ParsedExpression>> expressions;
  vector<unique_ptr<ParsedExpression>> groups;
  auto group_column = make_uniq<ColumnRefExpression>("symbol");

  auto value_column = make_uniq<ColumnRefExpression>("Adj Close");
  vector<unique_ptr<ParsedExpression>> children;
  children.emplace_back(std::move(value_column));
  auto volatility =
      make_uniq<FunctionExpression>("stddev_pop", std::move(children));

  auto date_column = make_uniq<ColumnRefExpression>("Date");
  value_column = make_uniq<ColumnRefExpression>("Adj Close");
  vector<unique_ptr<ParsedExpression>> children_min;
  children_min.emplace_back(std::move(value_column));
  children_min.emplace_back(std::move(date_column));
  auto arg_min =
      make_uniq<FunctionExpression>("arg_min", std::move(children_min));

  date_column = make_uniq<ColumnRefExpression>("Date");
  value_column = make_uniq<ColumnRefExpression>("Adj Close");
  vector<unique_ptr<ParsedExpression>> children_min_2;
  children_min_2.emplace_back(std::move(value_column));
  children_min_2.emplace_back(std::move(date_column));
  auto arg_min_2 =
      make_uniq<FunctionExpression>("arg_min", std::move(children_min_2));

  date_column = make_uniq<ColumnRefExpression>("Date");
  value_column = make_uniq<ColumnRefExpression>("Adj Close");
  vector<unique_ptr<ParsedExpression>> children_max;
  children_max.emplace_back(std::move(value_column));
  children_max.emplace_back(std::move(date_column));
  auto arg_max =
      make_uniq<FunctionExpression>("arg_max", std::move(children_max));

  vector<unique_ptr<ParsedExpression>> substract_children;
  substract_children.emplace_back(std::move(arg_max));
  substract_children.emplace_back(std::move(arg_min));
  auto subtract =
      make_uniq<FunctionExpression>("-", std::move(substract_children));

  vector<unique_ptr<ParsedExpression>> expected_return_children;
  expected_return_children.emplace_back(std::move(subtract));
  expected_return_children.emplace_back(std::move(arg_min_2));
  auto expected_return =
      make_uniq<FunctionExpression>("/", std::move(expected_return_children));
  auto symbol_column = make_uniq<ColumnRefExpression>("symbol");
  vector<unique_ptr<ParsedExpression>> aggr_expression;
  aggr_expression.emplace_back(std::move(symbol_column));
  aggr_expression.emplace_back(std::move(volatility));
  aggr_expression.emplace_back(std::move(expected_return));
  auto aggr_rel = make_shared_ptr<AggregateRelation>(
      tbl_rel, std::move(aggr_expression), std::move(groups));
  auto plan = std::move(aggr_rel);

  child_list_t<LogicalType> children_struct;
  children_struct.emplace_back(make_pair("symbol", LogicalType::VARCHAR));
  children_struct.emplace_back(make_pair("weight", LogicalType::DOUBLE));
  return_types.emplace_back(
      LogicalType::LIST(LogicalType::STRUCT(children_struct)));
  return_types.emplace_back(LogicalType::DOUBLE);
  return_types.emplace_back(LogicalType::DOUBLE);

  names.emplace_back("Portfolio");
  names.emplace_back("ExpectedReturn");
  names.emplace_back("Volatility");

  auto res = plan->Execute();
  auto result_chunk = res->Fetch();
  while (result_chunk) {
    for (idx_t i = 0; i < result_chunk->size(); i++) {
      Asset asset;
      asset.symbol = result_chunk->data[0].GetValue(i).GetValueUnsafe<string>();
      asset.volatility =
          result_chunk->data[1].GetValue(i).GetValueUnsafe<double>();
      asset.expected_return =
          result_chunk->data[2].GetValue(i).GetValueUnsafe<double>();
      result->portfolio.assets.emplace_back(asset);
    }
    result_chunk = res->Fetch();
  }
  result->portfolio_stats =
      calculate_efficient_frontier(result->portfolio, result->n);
  return std::move(result);
}
void PortfolioFrontier::Scan(ClientContext &context, TableFunctionInput &data_p,
                             DataChunk &output) {

  auto &data = (PortfolioFrontierData &)*data_p.bind_data;
  idx_t cur_out = 0;
  for (; data.cur < data.portfolio_stats.size(); data.cur++) {
    if (cur_out == STANDARD_VECTOR_SIZE) {
      break;
    }
    vector<Value> list;
    for (idx_t j = 0; j < data.portfolio.assets.size(); j++) {
      child_list_t<Value> children_struct;
      children_struct.emplace_back(
          make_pair("symbol", data.portfolio.assets[j].symbol));
      children_struct.emplace_back(
          make_pair("weight", data.portfolio.weights[data.cur][j]));
      list.emplace_back(Value::STRUCT(children_struct));
    }
    output.SetValue(0, data.cur, Value::LIST(list));
    output.SetValue(1, data.cur, data.portfolio_stats[data.cur].second);
    output.SetValue(2, data.cur, data.portfolio_stats[data.cur].first);
    cur_out++;
  }
  output.SetCardinality(cur_out);
}
} // namespace scrooge
} // namespace duckdb