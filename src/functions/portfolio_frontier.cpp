#include <cmath>
#include <vector>
#include "functions/scanner.hpp"
#include "duckdb/execution/operator/persistent/csv_reader_options.hpp"
#include "duckdb/main/relation/read_csv_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace scrooge {

using namespace std;
using namespace duckdb;

struct Asset {
  std::string symbol;
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

struct PortfolioFrontierData : public duckdb::TableFunctionData {
  Portfolio portfolio;
  vector<pair<double, double>> portfolio_stats;
  int n;
  int cur = 0;
};

duckdb::unique_ptr<duckdb::FunctionData>
PortfolioFrontier::Bind(duckdb::ClientContext &context,
                        duckdb::TableFunctionBindInput &input,
                        std::vector<duckdb::LogicalType> &return_types,
                        std::vector<std::string> &names) {
  auto result = duckdb::make_unique<PortfolioFrontierData>();
  auto conn = duckdb::make_unique<duckdb::Connection>(*context.db);
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
  result->n = input.inputs[3].GetValue<int>();
  vector<Value> parameters{input.inputs[0], input.inputs[1], input.inputs[2],
                           "1d"};

  auto tbl_rel = duckdb::make_shared<duckdb::TableFunctionRelation>(
      conn->context, "yahoo_finance", std::move(parameters));
  std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> expressions;
  std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> groups;
  auto group_column = duckdb::make_unique<ColumnRefExpression>("symbol");

  auto value_column = duckdb::make_unique<ColumnRefExpression>("Adj Close");
  vector<duckdb::unique_ptr<ParsedExpression>> children;
  children.emplace_back(std::move(value_column));
  auto volatility =
      duckdb::make_unique<FunctionExpression>("stddev_pop", std::move(children));

  auto date_column = duckdb::make_unique<ColumnRefExpression>("Date");
  value_column = duckdb::make_unique<ColumnRefExpression>("Adj Close");
  vector<duckdb::unique_ptr<ParsedExpression>> children_min;
  children_min.emplace_back(std::move(value_column));
  children_min.emplace_back(std::move(date_column));
  auto arg_min =
      duckdb::make_unique<FunctionExpression>("arg_min", std::move(children_min));

  date_column = duckdb::make_unique<ColumnRefExpression>("Date");
  value_column = duckdb::make_unique<ColumnRefExpression>("Adj Close");
  vector<duckdb::unique_ptr<ParsedExpression>> children_min_2;
  children_min_2.emplace_back(std::move(value_column));
  children_min_2.emplace_back(std::move(date_column));
  auto arg_min_2 = duckdb::make_unique<FunctionExpression>(
      "arg_min", std::move(children_min_2));

  date_column = duckdb::make_unique<ColumnRefExpression>("Date");
  value_column = duckdb::make_unique<ColumnRefExpression>("Adj Close");
  vector<duckdb::unique_ptr<ParsedExpression>> children_max;
  children_max.emplace_back(std::move(value_column));
  children_max.emplace_back(std::move(date_column));
  auto arg_max =
      duckdb::make_unique<FunctionExpression>("arg_max", std::move(children_max));

  vector<duckdb::unique_ptr<ParsedExpression>> substract_children;
  substract_children.emplace_back(std::move(arg_max));
  substract_children.emplace_back(std::move(arg_min));
  auto subtract =
      duckdb::make_unique<FunctionExpression>("-", std::move(substract_children));

  vector<duckdb::unique_ptr<ParsedExpression>> expected_return_children;
  expected_return_children.emplace_back(std::move(subtract));
  expected_return_children.emplace_back(std::move(arg_min_2));
  auto expected_return = duckdb::make_unique<FunctionExpression>(
      "/", std::move(expected_return_children));
  auto symbol_column = duckdb::make_unique<ColumnRefExpression>("symbol");
  vector<duckdb::unique_ptr<ParsedExpression>> aggr_expression;
  aggr_expression.emplace_back(std::move(symbol_column));
  aggr_expression.emplace_back(std::move(volatility));
  aggr_expression.emplace_back(std::move(expected_return));
  auto aggr_rel = duckdb::make_shared<duckdb::AggregateRelation>(
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
      asset.symbol =
          result_chunk->data[0].GetValue(i).GetValueUnsafe<std::string>();
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
void PortfolioFrontier::Scan(duckdb::ClientContext &context,
                             duckdb::TableFunctionInput &data_p,
                             duckdb::DataChunk &output) {

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