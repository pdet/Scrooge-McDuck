#include "functions/quant.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/helper.hpp"

#include <algorithm>
#include <cmath>
#include <vector>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// Time-series statistics — ordered aggregates over (value, ts).
//
//   hurst_exponent(value, ts)
//     R/S analysis. H ≈ 0.5 → random walk; H > 0.5 → trending;
//     H < 0.5 → mean-reverting.
//
//   adf_test_stat(value, ts)
//     Augmented Dickey-Fuller test statistic on Δy_t = α + β·y_{t-1} + ε.
//     More negative → stronger evidence of stationarity.
//     Critical values (no constant, no trend, large N): 1% ≈ -3.43,
//     5% ≈ -2.86, 10% ≈ -2.57. Returned as a raw t-statistic; users
//     compare against published tables.
// ──────────────────────────────────────────────────────────────

namespace {

struct OrderedSeriesState {
	struct Entry {
		double v;
		int64_t ts;
	};
	std::vector<Entry> *entries;
};

static void TSInitialize(const AggregateFunction &, data_ptr_t state_p) {
	auto &state = *reinterpret_cast<OrderedSeriesState *>(state_p);
	state.entries = nullptr;
}

static void TSUpdate(Vector inputs[], AggregateInputData &, idx_t, Vector &state_vector, idx_t count) {
	UnifiedVectorFormat vd, td, sd;
	inputs[0].ToUnifiedFormat(count, vd);
	inputs[1].ToUnifiedFormat(count, td);
	state_vector.ToUnifiedFormat(count, sd);
	auto values = UnifiedVectorFormat::GetData<double>(vd);
	auto timestamps = UnifiedVectorFormat::GetData<int64_t>(td);
	auto states = (OrderedSeriesState **)sd.data;
	for (idx_t i = 0; i < count; i++) {
		auto sidx = sd.sel->get_index(i);
		auto &state = *states[sidx];
		if (!state.entries) {
			state.entries = new std::vector<OrderedSeriesState::Entry>();
		}
		auto vi = vd.sel->get_index(i);
		auto ti = td.sel->get_index(i);
		if (vd.validity.RowIsValid(vi) && td.validity.RowIsValid(ti)) {
			state.entries->push_back({values[vi], timestamps[ti]});
		}
	}
}

static void TSCombine(Vector &source_vec, Vector &target_vec, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sd, td;
	source_vec.ToUnifiedFormat(count, sd);
	target_vec.ToUnifiedFormat(count, td);
	auto sources = (OrderedSeriesState **)sd.data;
	auto targets = (OrderedSeriesState **)td.data;
	for (idx_t i = 0; i < count; i++) {
		auto &src = *sources[sd.sel->get_index(i)];
		auto &tgt = *targets[td.sel->get_index(i)];
		if (src.entries) {
			if (!tgt.entries) {
				tgt.entries = new std::vector<OrderedSeriesState::Entry>();
			}
			tgt.entries->insert(tgt.entries->end(), src.entries->begin(), src.entries->end());
			delete src.entries;
			src.entries = nullptr;
		}
	}
}

static void TSDestructor(Vector &state_vector, AggregateInputData &, idx_t count) {
	UnifiedVectorFormat sd;
	state_vector.ToUnifiedFormat(count, sd);
	auto states = (OrderedSeriesState **)sd.data;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sd.sel->get_index(i)];
		if (state.entries) {
			delete state.entries;
			state.entries = nullptr;
		}
	}
}

static std::vector<double> SortedValues(OrderedSeriesState &state) {
	auto &entries = *state.entries;
	std::sort(entries.begin(), entries.end(),
	          [](const OrderedSeriesState::Entry &a, const OrderedSeriesState::Entry &b) { return a.ts < b.ts; });
	std::vector<double> out;
	out.reserve(entries.size());
	for (auto &e : entries) {
		out.push_back(e.v);
	}
	return out;
}

// ─── Hurst (R/S analysis) ────────────────────────────────────────
//
// Compute log(R/S) for several chunk sizes n and regress on log(n).
// The slope is the Hurst exponent.

static double HurstFromSeries(const std::vector<double> &x) {
	idx_t N = x.size();
	if (N < 16) {
		return std::numeric_limits<double>::quiet_NaN();
	}
	std::vector<idx_t> sizes;
	for (idx_t n = 8; n * 2 <= N; n *= 2) {
		sizes.push_back(n);
	}
	if (sizes.size() < 3) {
		return std::numeric_limits<double>::quiet_NaN();
	}

	std::vector<double> log_n;
	std::vector<double> log_rs;
	for (idx_t n : sizes) {
		idx_t chunks = N / n;
		double rs_sum = 0.0;
		idx_t rs_count = 0;
		for (idx_t c = 0; c < chunks; c++) {
			double mean = 0.0;
			for (idx_t i = 0; i < n; i++) {
				mean += x[c * n + i];
			}
			mean /= (double)n;
			double sd2 = 0.0;
			double y = 0.0;
			double y_min = 0.0, y_max = 0.0;
			for (idx_t i = 0; i < n; i++) {
				double d = x[c * n + i] - mean;
				sd2 += d * d;
				y += d;
				if (i == 0) {
					y_min = y_max = y;
				} else {
					if (y < y_min) y_min = y;
					if (y > y_max) y_max = y;
				}
			}
			double sd = std::sqrt(sd2 / (double)n);
			if (sd > 0) {
				double R = y_max - y_min;
				rs_sum += R / sd;
				rs_count++;
			}
		}
		if (rs_count > 0) {
			log_n.push_back(std::log((double)n));
			log_rs.push_back(std::log(rs_sum / (double)rs_count));
		}
	}
	if (log_n.size() < 3) {
		return std::numeric_limits<double>::quiet_NaN();
	}
	// OLS slope.
	double mx = 0.0, my = 0.0;
	for (idx_t i = 0; i < log_n.size(); i++) {
		mx += log_n[i];
		my += log_rs[i];
	}
	mx /= (double)log_n.size();
	my /= (double)log_n.size();
	double num = 0.0, den = 0.0;
	for (idx_t i = 0; i < log_n.size(); i++) {
		num += (log_n[i] - mx) * (log_rs[i] - my);
		den += (log_n[i] - mx) * (log_n[i] - mx);
	}
	if (den <= 0) {
		return std::numeric_limits<double>::quiet_NaN();
	}
	return num / den;
}

static void HurstFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sd;
	state_vector.ToUnifiedFormat(count, sd);
	auto states = (OrderedSeriesState **)sd.data;
	auto out = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sd.sel->get_index(i)];
		auto ridx = i + offset;
		if (!state.entries || state.entries->size() < 16) {
			validity.SetInvalid(ridx);
			if (state.entries) {
				delete state.entries;
				state.entries = nullptr;
			}
			continue;
		}
		auto x = SortedValues(state);
		double h = HurstFromSeries(x);
		if (!std::isfinite(h)) {
			validity.SetInvalid(ridx);
		} else {
			out[ridx] = h;
		}
		delete state.entries;
		state.entries = nullptr;
	}
}

// ─── ADF test statistic (no constant, lag 0) ─────────────────────
//
// Regress Δy_t = β·y_{t-1} (+ α intercept) and report t-stat for β.

static double ADFFromSeries(const std::vector<double> &y) {
	idx_t N = y.size();
	if (N < 10) {
		return std::numeric_limits<double>::quiet_NaN();
	}
	// Build x = y_{t-1}, dy = y_t - y_{t-1} for t = 1..N-1.
	idx_t n = N - 1;
	double sum_x = 0.0, sum_dy = 0.0, sum_xx = 0.0, sum_xdy = 0.0;
	for (idx_t t = 1; t < N; t++) {
		double xt = y[t - 1];
		double dyt = y[t] - y[t - 1];
		sum_x += xt;
		sum_dy += dyt;
		sum_xx += xt * xt;
		sum_xdy += xt * dyt;
	}
	double mean_x = sum_x / (double)n;
	double mean_dy = sum_dy / (double)n;
	double sxx = sum_xx - (double)n * mean_x * mean_x;
	double sxy = sum_xdy - (double)n * mean_x * mean_dy;
	if (sxx <= 0) {
		return std::numeric_limits<double>::quiet_NaN();
	}
	double beta = sxy / sxx;
	double alpha = mean_dy - beta * mean_x;
	// Residuals.
	double rss = 0.0;
	for (idx_t t = 1; t < N; t++) {
		double xt = y[t - 1];
		double dyt = y[t] - y[t - 1];
		double resid = dyt - alpha - beta * xt;
		rss += resid * resid;
	}
	double s2 = rss / (double)(n - 2);
	double se_beta = std::sqrt(s2 / sxx);
	if (se_beta <= 0) {
		return std::numeric_limits<double>::quiet_NaN();
	}
	return beta / se_beta;
}

static void ADFFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sd;
	state_vector.ToUnifiedFormat(count, sd);
	auto states = (OrderedSeriesState **)sd.data;
	auto out = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sd.sel->get_index(i)];
		auto ridx = i + offset;
		if (!state.entries || state.entries->size() < 10) {
			validity.SetInvalid(ridx);
			if (state.entries) {
				delete state.entries;
				state.entries = nullptr;
			}
			continue;
		}
		auto x = SortedValues(state);
		double t = ADFFromSeries(x);
		if (!std::isfinite(t)) {
			validity.SetInvalid(ridx);
		} else {
			out[ridx] = t;
		}
		delete state.entries;
		state.entries = nullptr;
	}
}

static void Register(Connection &conn, Catalog &catalog, const string &name,
                       aggregate_finalize_t finalize) {
	AggregateFunctionSet set(name);
	set.AddFunction(AggregateFunction(
	    name, {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ},
	    LogicalType::DOUBLE,
	    AggregateFunction::StateSize<OrderedSeriesState>,
	    TSInitialize, TSUpdate, TSCombine, finalize, nullptr, nullptr, TSDestructor));
	set.AddFunction(AggregateFunction(
	    name, {LogicalType::DOUBLE, LogicalType::TIMESTAMP},
	    LogicalType::DOUBLE,
	    AggregateFunction::StateSize<OrderedSeriesState>,
	    TSInitialize, TSUpdate, TSCombine, finalize, nullptr, nullptr, TSDestructor));
	CreateAggregateFunctionInfo info(set);
	catalog.CreateFunction(*conn.context, info);
}

} // namespace

void RegisterTimeSeriesStats(Connection &conn, Catalog &catalog) {
	Register(conn, catalog, "hurst_exponent", HurstFinalize);
	Register(conn, catalog, "adf_test_stat", ADFFinalize);
}

} // namespace scrooge
} // namespace duckdb
