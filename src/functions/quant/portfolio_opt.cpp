#include "functions/quant.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"

#include <algorithm>
#include <cmath>
#include <map>
#include <random>
#include <vector>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// Portfolio optimization on a returns table.
//
// Common contract: input is the name of a query/table that yields
//   (symbol VARCHAR, return DOUBLE)
// with N observations per symbol (ordered or not — covariance is
// invariant to ordering of paired observations *only if symbols are
// aligned*; we therefore expect callers to pre-align via JOIN).
//
// Functions:
//   min_variance_portfolio(returns_query VARCHAR)
//   max_sharpe_portfolio(returns_query VARCHAR [, risk_free_rate DOUBLE])
//   efficient_frontier(returns_query VARCHAR, num_points INTEGER
//                      [, risk_free_rate DOUBLE])
//
// All assume rows are aligned across symbols by row position.
// ──────────────────────────────────────────────────────────────

namespace {

struct ReturnsMatrix {
	vector<string> symbols;
	// rows = observations, cols = symbols
	vector<vector<double>> data;
};

// Load (symbol, return) pairs and pivot into a matrix.
// We read symbols in first-seen order. We assume the same number of
// observations per symbol (validated at the end).
static ReturnsMatrix LoadReturns(ClientContext &context, const string &query) {
	Connection conn(*context.db);
	auto pulled = conn.Query("SELECT symbol::VARCHAR, return::DOUBLE FROM (" + query + ")");
	if (pulled->HasError()) {
		throw InvalidInputException("returns query failed: %s", pulled->GetError());
	}

	ReturnsMatrix m;
	std::map<string, idx_t> sym_idx;
	std::map<string, vector<double>> per_symbol;

	while (true) {
		auto chunk = pulled->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto sym_val = chunk->GetValue(0, i);
			auto ret_val = chunk->GetValue(1, i);
			if (sym_val.IsNull() || ret_val.IsNull()) {
				continue;
			}
			auto sym = sym_val.GetValue<string>();
			auto r = ret_val.GetValue<double>();
			if (sym_idx.find(sym) == sym_idx.end()) {
				sym_idx[sym] = m.symbols.size();
				m.symbols.push_back(sym);
			}
			per_symbol[sym].push_back(r);
		}
	}
	if (m.symbols.size() < 2) {
		throw InvalidInputException("portfolio optimization requires at least 2 symbols");
	}
	idx_t n_obs = per_symbol[m.symbols[0]].size();
	for (auto &s : m.symbols) {
		if (per_symbol[s].size() != n_obs) {
			throw InvalidInputException(
			    "all symbols must have the same number of observations (symbol '%s' has %llu, expected %llu)",
			    s, (unsigned long long)per_symbol[s].size(), (unsigned long long)n_obs);
		}
	}
	if (n_obs < 2) {
		throw InvalidInputException("at least 2 observations per symbol are required");
	}
	m.data.assign(n_obs, vector<double>(m.symbols.size(), 0.0));
	for (idx_t c = 0; c < m.symbols.size(); c++) {
		auto &col = per_symbol[m.symbols[c]];
		for (idx_t r = 0; r < n_obs; r++) {
			m.data[r][c] = col[r];
		}
	}
	return m;
}

static vector<double> MeanVector(const ReturnsMatrix &m) {
	vector<double> mean(m.symbols.size(), 0.0);
	for (auto &row : m.data) {
		for (idx_t c = 0; c < row.size(); c++) {
			mean[c] += row[c];
		}
	}
	for (auto &v : mean) {
		v /= (double)m.data.size();
	}
	return mean;
}

// Sample covariance matrix (n-1 in denominator).
static vector<vector<double>> CovMatrix(const ReturnsMatrix &m, const vector<double> &mean) {
	idx_t k = m.symbols.size();
	idx_t n = m.data.size();
	vector<vector<double>> cov(k, vector<double>(k, 0.0));
	for (auto &row : m.data) {
		for (idx_t i = 0; i < k; i++) {
			for (idx_t j = 0; j < k; j++) {
				cov[i][j] += (row[i] - mean[i]) * (row[j] - mean[j]);
			}
		}
	}
	double denom = (double)(n - 1);
	for (idx_t i = 0; i < k; i++) {
		for (idx_t j = 0; j < k; j++) {
			cov[i][j] /= denom;
		}
	}
	return cov;
}

// Gauss-Jordan inversion with partial pivoting and a small ridge for stability.
static vector<vector<double>> Invert(vector<vector<double>> A) {
	idx_t n = A.size();
	// Add small ridge to diagonal for numerical stability (Tikhonov).
	double trace = 0.0;
	for (idx_t i = 0; i < n; i++) {
		trace += A[i][i];
	}
	double ridge = std::max(1e-10, std::fabs(trace) * 1e-10 / (double)n);
	for (idx_t i = 0; i < n; i++) {
		A[i][i] += ridge;
	}

	vector<vector<double>> I(n, vector<double>(n, 0.0));
	for (idx_t i = 0; i < n; i++) {
		I[i][i] = 1.0;
	}
	for (idx_t col = 0; col < n; col++) {
		idx_t pivot = col;
		double best = std::fabs(A[col][col]);
		for (idx_t r = col + 1; r < n; r++) {
			if (std::fabs(A[r][col]) > best) {
				best = std::fabs(A[r][col]);
				pivot = r;
			}
		}
		if (best < 1e-14) {
			throw InvalidInputException("covariance matrix is singular — symbols may be linearly dependent");
		}
		if (pivot != col) {
			std::swap(A[col], A[pivot]);
			std::swap(I[col], I[pivot]);
		}
		double pv = A[col][col];
		for (idx_t j = 0; j < n; j++) {
			A[col][j] /= pv;
			I[col][j] /= pv;
		}
		for (idx_t r = 0; r < n; r++) {
			if (r == col) {
				continue;
			}
			double factor = A[r][col];
			if (factor == 0.0) {
				continue;
			}
			for (idx_t j = 0; j < n; j++) {
				A[r][j] -= factor * A[col][j];
				I[r][j] -= factor * I[col][j];
			}
		}
	}
	return I;
}

static vector<double> MatVec(const vector<vector<double>> &A, const vector<double> &x) {
	vector<double> y(A.size(), 0.0);
	for (idx_t i = 0; i < A.size(); i++) {
		for (idx_t j = 0; j < x.size(); j++) {
			y[i] += A[i][j] * x[j];
		}
	}
	return y;
}

static double Dot(const vector<double> &a, const vector<double> &b) {
	double s = 0.0;
	for (idx_t i = 0; i < a.size(); i++) {
		s += a[i] * b[i];
	}
	return s;
}

// Min-variance portfolio with sum(w)=1, no other constraints:
//   w = inv(Sigma) * 1 / (1' inv(Sigma) 1)
static vector<double> MinVarianceWeights(const vector<vector<double>> &cov_inv) {
	idx_t n = cov_inv.size();
	vector<double> ones(n, 1.0);
	vector<double> u = MatVec(cov_inv, ones);
	double denom = 0.0;
	for (auto &v : u) {
		denom += v;
	}
	for (auto &v : u) {
		v /= denom;
	}
	return u;
}

// Tangency / max-Sharpe portfolio (excess returns):
//   w = inv(Sigma) * (mu - rf*1) / (1' inv(Sigma) (mu - rf*1))
static vector<double> MaxSharpeWeights(const vector<vector<double>> &cov_inv,
                                        const vector<double> &mu, double rf) {
	idx_t n = cov_inv.size();
	vector<double> excess(n);
	for (idx_t i = 0; i < n; i++) {
		excess[i] = mu[i] - rf;
	}
	vector<double> u = MatVec(cov_inv, excess);
	double denom = 0.0;
	for (auto &v : u) {
		denom += v;
	}
	if (std::fabs(denom) < 1e-14) {
		throw InvalidInputException("max_sharpe_portfolio: degenerate excess returns");
	}
	for (auto &v : u) {
		v /= denom;
	}
	return u;
}

static double PortfolioReturn(const vector<double> &w, const vector<double> &mu) {
	return Dot(w, mu);
}

static double PortfolioVariance(const vector<double> &w, const vector<vector<double>> &cov) {
	auto y = MatVec(cov, w);
	return Dot(w, y);
}

} // namespace

// ──────────────────────────────────────────────────────────────
// Common bind for "TABLE(symbol, weight, expected_return, volatility)" returns
// ──────────────────────────────────────────────────────────────

struct OptResult {
	vector<string> symbols;
	vector<double> weights;
	double expected_return;
	double volatility;
};

struct OptScanData : public TableFunctionData {
	OptResult res;
	idx_t cur = 0;
};

static unique_ptr<FunctionData> BuildSingleResult(ClientContext &context, const string &query,
                                                    bool max_sharpe, double rf,
                                                    vector<LogicalType> &return_types,
                                                    vector<string> &names) {
	auto m = LoadReturns(context, query);
	auto mu = MeanVector(m);
	auto cov = CovMatrix(m, mu);
	auto cov_inv = Invert(cov);

	vector<double> w = max_sharpe ? MaxSharpeWeights(cov_inv, mu, rf) : MinVarianceWeights(cov_inv);
	double pret = PortfolioReturn(w, mu);
	double pvar = PortfolioVariance(w, cov);
	double pvol = pvar > 0 ? std::sqrt(pvar) : 0.0;

	auto data = make_uniq<OptScanData>();
	data->res.symbols = m.symbols;
	data->res.weights = std::move(w);
	data->res.expected_return = pret;
	data->res.volatility = pvol;

	names = {"symbol", "weight", "expected_return", "volatility"};
	return_types = {LogicalType::VARCHAR, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE};
	return std::move(data);
}

static unique_ptr<FunctionData> MinVarBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto query = input.inputs[0].GetValue<string>();
	return BuildSingleResult(context, query, false, 0.0, return_types, names);
}

static unique_ptr<FunctionData> MaxSharpeBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto query = input.inputs[0].GetValue<string>();
	double rf = 0.0;
	if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		rf = input.inputs[1].GetValue<double>();
	}
	return BuildSingleResult(context, query, true, rf, return_types, names);
}

static void OptScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = (OptScanData &)*input.bind_data;
	auto &res = data.res;
	idx_t out = 0;
	auto sym_data = FlatVector::GetData<string_t>(output.data[0]);
	auto w_data = FlatVector::GetData<double>(output.data[1]);
	auto er_data = FlatVector::GetData<double>(output.data[2]);
	auto vol_data = FlatVector::GetData<double>(output.data[3]);
	while (out < STANDARD_VECTOR_SIZE && data.cur < res.symbols.size()) {
		sym_data[out] = StringVector::AddString(output.data[0], res.symbols[data.cur]);
		w_data[out] = res.weights[data.cur];
		er_data[out] = res.expected_return;
		vol_data[out] = res.volatility;
		data.cur++;
		out++;
	}
	output.SetCardinality(out);
}

// ──────────────────────────────────────────────────────────────
// efficient_frontier(returns_query, num_points [, risk_free_rate])
// Generates portfolios spanning the frontier between min-variance
// and a high-return target.
// Returns: (point_id INTEGER, symbol VARCHAR, weight DOUBLE,
//           expected_return DOUBLE, volatility DOUBLE, sharpe_ratio DOUBLE)
// ──────────────────────────────────────────────────────────────

struct FrontierData : public TableFunctionData {
	vector<string> symbols;
	vector<vector<double>> weights; // [point][symbol]
	vector<double> rets;
	vector<double> vols;
	vector<double> sharpes;
	idx_t cur_point = 0;
	idx_t cur_sym = 0;
};

// Solve mean-variance optimization for a target return:
//   min w'Σw  s.t.  1'w = 1, μ'w = target
// Closed form via 2x2 Lagrangian system.
static vector<double> FrontierWeightsForTarget(const vector<vector<double>> &cov_inv,
                                                 const vector<double> &mu, double target) {
	idx_t n = mu.size();
	vector<double> ones(n, 1.0);
	vector<double> u = MatVec(cov_inv, ones); // Σ⁻¹ 1
	vector<double> v = MatVec(cov_inv, mu);   // Σ⁻¹ μ
	double A = Dot(ones, u);
	double B = Dot(mu, u);
	double C = Dot(mu, v);
	double D = A * C - B * B;
	if (std::fabs(D) < 1e-18) {
		throw InvalidInputException("efficient_frontier: degenerate problem");
	}
	double lam = (C - B * target) / D;
	double gam = (A * target - B) / D;
	vector<double> w(n);
	for (idx_t i = 0; i < n; i++) {
		w[i] = lam * u[i] + gam * v[i];
	}
	return w;
}

static unique_ptr<FunctionData> FrontierBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto query = input.inputs[0].GetValue<string>();
	int32_t num_points = input.inputs[1].GetValue<int32_t>();
	if (num_points < 2) {
		throw InvalidInputException("num_points must be >= 2");
	}
	double rf = 0.0;
	if (input.inputs.size() > 2 && !input.inputs[2].IsNull()) {
		rf = input.inputs[2].GetValue<double>();
	}

	auto m = LoadReturns(context, query);
	auto mu = MeanVector(m);
	auto cov = CovMatrix(m, mu);
	auto cov_inv = Invert(cov);

	double mu_min = *std::min_element(mu.begin(), mu.end());
	double mu_max = *std::max_element(mu.begin(), mu.end());

	// Anchor low end at min-variance portfolio's return; high end at max-mean asset.
	auto mvp = MinVarianceWeights(cov_inv);
	double mvp_ret = PortfolioReturn(mvp, mu);
	double lo = std::min(mvp_ret, mu_min);
	double hi = mu_max;
	if (hi - lo < 1e-12) {
		hi = lo + 1e-6;
	}

	auto data = make_uniq<FrontierData>();
	data->symbols = m.symbols;
	for (int32_t p = 0; p < num_points; p++) {
		double target = lo + (hi - lo) * ((double)p / (double)(num_points - 1));
		vector<double> w;
		try {
			w = FrontierWeightsForTarget(cov_inv, mu, target);
		} catch (...) {
			continue;
		}
		double pret = PortfolioReturn(w, mu);
		double pvar = PortfolioVariance(w, cov);
		double pvol = pvar > 0 ? std::sqrt(pvar) : 0.0;
		double sharpe = pvol > 0 ? (pret - rf) / pvol : 0.0;
		data->weights.push_back(std::move(w));
		data->rets.push_back(pret);
		data->vols.push_back(pvol);
		data->sharpes.push_back(sharpe);
	}

	names = {"point_id", "symbol", "weight", "expected_return", "volatility", "sharpe_ratio"};
	return_types = {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE};
	return std::move(data);
}

static void FrontierScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = (FrontierData &)*input.bind_data;
	auto pid_data = FlatVector::GetData<int32_t>(output.data[0]);
	auto sym_data = FlatVector::GetData<string_t>(output.data[1]);
	auto w_data = FlatVector::GetData<double>(output.data[2]);
	auto er_data = FlatVector::GetData<double>(output.data[3]);
	auto vol_data = FlatVector::GetData<double>(output.data[4]);
	auto sh_data = FlatVector::GetData<double>(output.data[5]);

	idx_t out = 0;
	while (out < STANDARD_VECTOR_SIZE && data.cur_point < data.weights.size()) {
		pid_data[out] = (int32_t)data.cur_point;
		sym_data[out] = StringVector::AddString(output.data[1], data.symbols[data.cur_sym]);
		w_data[out] = data.weights[data.cur_point][data.cur_sym];
		er_data[out] = data.rets[data.cur_point];
		vol_data[out] = data.vols[data.cur_point];
		sh_data[out] = data.sharpes[data.cur_point];
		out++;
		data.cur_sym++;
		if (data.cur_sym >= data.symbols.size()) {
			data.cur_sym = 0;
			data.cur_point++;
		}
	}
	output.SetCardinality(out);
}

void RegisterPortfolioOptimization(Connection &conn, Catalog &catalog) {
	{
		TableFunctionSet set("min_variance_portfolio");
		set.AddFunction(TableFunction({LogicalType::VARCHAR}, OptScan, MinVarBind));
		CreateTableFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	}
	{
		TableFunctionSet set("max_sharpe_portfolio");
		set.AddFunction(TableFunction({LogicalType::VARCHAR}, OptScan, MaxSharpeBind));
		set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::DOUBLE}, OptScan, MaxSharpeBind));
		CreateTableFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	}
	{
		TableFunctionSet set("efficient_frontier");
		set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::INTEGER},
		                               FrontierScan, FrontierBind));
		set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::DOUBLE},
		                               FrontierScan, FrontierBind));
		CreateTableFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	}
}

} // namespace scrooge
} // namespace duckdb
