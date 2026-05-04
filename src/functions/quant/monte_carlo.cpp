#include "functions/quant.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#define _USE_MATH_DEFINES
#include <cmath>
#include <random>
#include <vector>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// monte_carlo(start_price, mu, sigma, horizon_days, num_paths [, seed])
//
// Geometric Brownian Motion simulation.
//   S_{t+1} = S_t * exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)
// where mu and sigma are *annualized* drift and volatility.
// dt = 1/252 (trading days).
//
// Returns: (path_id BIGINT, day INTEGER, price DOUBLE)
// ──────────────────────────────────────────────────────────────

struct MonteCarloData : public TableFunctionData {
	double start_price;
	double mu;
	double sigma;
	int64_t horizon;
	int64_t num_paths;
	uint64_t seed;
	bool seeded;

	// Iteration state
	int64_t cur_path = 0;
	int64_t cur_day = 0;
	double cur_price = 0.0;
	std::mt19937_64 rng;
	std::normal_distribution<double> normal;

	MonteCarloData() : normal(0.0, 1.0) {}
};

static unique_ptr<FunctionData> MCBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<MonteCarloData>();
	data->start_price = input.inputs[0].GetValue<double>();
	data->mu = input.inputs[1].GetValue<double>();
	data->sigma = input.inputs[2].GetValue<double>();
	data->horizon = input.inputs[3].GetValue<int64_t>();
	data->num_paths = input.inputs[4].GetValue<int64_t>();

	if (data->start_price <= 0) {
		throw InvalidInputException("start_price must be positive");
	}
	if (data->sigma < 0) {
		throw InvalidInputException("sigma must be non-negative");
	}
	if (data->horizon <= 0 || data->num_paths <= 0) {
		throw InvalidInputException("horizon_days and num_paths must be positive");
	}

	data->seeded = false;
	if (input.inputs.size() > 5 && !input.inputs[5].IsNull()) {
		data->seed = (uint64_t)input.inputs[5].GetValue<int64_t>();
		data->seeded = true;
	}
	if (data->seeded) {
		data->rng.seed(data->seed);
	} else {
		std::random_device rd;
		data->rng.seed(((uint64_t)rd() << 32) | rd());
	}
	data->cur_price = data->start_price;

	names = {"path_id", "day", "price"};
	return_types = {LogicalType::BIGINT, LogicalType::INTEGER, LogicalType::DOUBLE};
	return std::move(data);
}

static void MCScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = (MonteCarloData &)*input.bind_data;
	if (data.cur_path >= data.num_paths) {
		output.SetCardinality(0);
		return;
	}

	auto path_data = FlatVector::GetData<int64_t>(output.data[0]);
	auto day_data = FlatVector::GetData<int32_t>(output.data[1]);
	auto price_data = FlatVector::GetData<double>(output.data[2]);

	const double dt = 1.0 / 252.0;
	const double drift = (data.mu - 0.5 * data.sigma * data.sigma) * dt;
	const double diff_coef = data.sigma * std::sqrt(dt);

	idx_t out = 0;
	while (out < STANDARD_VECTOR_SIZE && data.cur_path < data.num_paths) {
		// Emit day 0 (start price) when day=0 just begun
		if (data.cur_day == 0) {
			path_data[out] = data.cur_path;
			day_data[out] = 0;
			price_data[out] = data.start_price;
			data.cur_price = data.start_price;
			data.cur_day = 1;
			out++;
			continue;
		}

		double z = data.normal(data.rng);
		data.cur_price = data.cur_price * std::exp(drift + diff_coef * z);
		path_data[out] = data.cur_path;
		day_data[out] = (int32_t)data.cur_day;
		price_data[out] = data.cur_price;
		out++;
		data.cur_day++;

		if (data.cur_day > data.horizon) {
			data.cur_path++;
			data.cur_day = 0;
		}
	}
	output.SetCardinality(out);
}

void RegisterMonteCarlo(Connection &conn, Catalog &catalog) {
	TableFunctionSet set("monte_carlo");
	// Without seed
	set.AddFunction(TableFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                                LogicalType::BIGINT, LogicalType::BIGINT},
	                               MCScan, MCBind));
	// With seed
	set.AddFunction(TableFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                                LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                               MCScan, MCBind));
	CreateTableFunctionInfo info(set);
	catalog.CreateFunction(*conn.context, info);
}

} // namespace scrooge
} // namespace duckdb
