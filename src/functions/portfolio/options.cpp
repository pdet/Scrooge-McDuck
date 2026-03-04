#include "functions/portfolio.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/catalog/catalog.hpp"
#define _USE_MATH_DEFINES
#include <cmath>
#include <functional>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// Black-Scholes-Merton option pricing + Greeks
//
// All functions take: S (spot), K (strike), T (time to expiry in years),
//                     r (risk-free rate), sigma (volatility)
//
// bs_call(S, K, T, r, sigma) → call price
// bs_put(S, K, T, r, sigma) → put price
// bs_delta_call(S, K, T, r, sigma) → call delta
// bs_delta_put(S, K, T, r, sigma) → put delta
// bs_gamma(S, K, T, r, sigma) → gamma (same for call/put)
// bs_theta_call(S, K, T, r, sigma) → call theta (per day)
// bs_theta_put(S, K, T, r, sigma) → put theta (per day)
// bs_vega(S, K, T, r, sigma) → vega (same for call/put)
// bs_implied_vol(option_price, S, K, T, r, is_call) → implied vol
// ──────────────────────────────────────────────────────────────

// Standard normal CDF (Abramowitz & Stegun approximation)
static double NormCDF(double x) {
	if (x > 6.0) return 1.0;
	if (x < -6.0) return 0.0;
	static const double a1 = 0.254829592, a2 = -0.284496736;
	static const double a3 = 1.421413741, a4 = -1.453152027;
	static const double a5 = 1.061405429, p = 0.3275911;
	int sign = (x < 0) ? -1 : 1;
	x = std::fabs(x);
	double t = 1.0 / (1.0 + p * x);
	double y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * std::exp(-x * x / 2.0);
	return 0.5 * (1.0 + sign * y);
}

// Standard normal PDF
static double NormPDF(double x) {
	return std::exp(-0.5 * x * x) / std::sqrt(2.0 * M_PI);
}

struct BSParams {
	double S, K, T, r, sigma, d1, d2;
	bool valid;
};

static BSParams ComputeBS(double S, double K, double T, double r, double sigma) {
	BSParams p;
	p.S = S; p.K = K; p.T = T; p.r = r; p.sigma = sigma;
	p.valid = (S > 0 && K > 0 && T > 0 && sigma > 0);
	if (p.valid) {
		double sqrtT = std::sqrt(T);
		p.d1 = (std::log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrtT);
		p.d2 = p.d1 - sigma * sqrtT;
	} else {
		p.d1 = p.d2 = 0;
	}
	return p;
}

// Helper to iterate over 5 input vectors and compute BS result
static void BSIterate(DataChunk &args, Vector &result,
                      std::function<double(const BSParams &)> compute) {
	UnifiedVectorFormat sf, kf, tf, rf, sigf;
	args.data[0].ToUnifiedFormat(args.size(), sf);
	args.data[1].ToUnifiedFormat(args.size(), kf);
	args.data[2].ToUnifiedFormat(args.size(), tf);
	args.data[3].ToUnifiedFormat(args.size(), rf);
	args.data[4].ToUnifiedFormat(args.size(), sigf);
	auto s_data = UnifiedVectorFormat::GetData<double>(sf);
	auto k_data = UnifiedVectorFormat::GetData<double>(kf);
	auto t_data = UnifiedVectorFormat::GetData<double>(tf);
	auto r_data = UnifiedVectorFormat::GetData<double>(rf);
	auto sig_data = UnifiedVectorFormat::GetData<double>(sigf);
	auto result_data = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	for (idx_t i = 0; i < args.size(); i++) {
		auto si = sf.sel->get_index(i);
		auto ki = kf.sel->get_index(i);
		auto ti = tf.sel->get_index(i);
		auto ri = rf.sel->get_index(i);
		auto sigi = sigf.sel->get_index(i);
		if (!sf.validity.RowIsValid(si) || !kf.validity.RowIsValid(ki) ||
		    !tf.validity.RowIsValid(ti) || !rf.validity.RowIsValid(ri) ||
		    !sigf.validity.RowIsValid(sigi)) {
			validity.SetInvalid(i); continue;
		}
		auto p = ComputeBS(s_data[si], k_data[ki], t_data[ti], r_data[ri], sig_data[sigi]);
		if (!p.valid) { validity.SetInvalid(i); continue; }
		result_data[i] = compute(p);
	}
}

#define DEFINE_BS_FUNC(name, body) \
static void name##Func(DataChunk &args, ExpressionState &, Vector &result) { \
	BSIterate(args, result, [](const BSParams &p) -> double { body }); \
}

DEFINE_BS_FUNC(BSCall, {
	return p.S * NormCDF(p.d1) - p.K * std::exp(-p.r * p.T) * NormCDF(p.d2);
})

DEFINE_BS_FUNC(BSPut, {
	return p.K * std::exp(-p.r * p.T) * NormCDF(-p.d2) - p.S * NormCDF(-p.d1);
})

DEFINE_BS_FUNC(BSDeltaCall, {
	return NormCDF(p.d1);
})

DEFINE_BS_FUNC(BSDeltaPut, {
	return NormCDF(p.d1) - 1.0;
})

DEFINE_BS_FUNC(BSGamma, {
	return NormPDF(p.d1) / (p.S * p.sigma * std::sqrt(p.T));
})

DEFINE_BS_FUNC(BSThetaCall, {
	double term1 = -(p.S * NormPDF(p.d1) * p.sigma) / (2.0 * std::sqrt(p.T));
	double term2 = -p.r * p.K * std::exp(-p.r * p.T) * NormCDF(p.d2);
	return (term1 + term2) / 365.0;
})

DEFINE_BS_FUNC(BSThetaPut, {
	double term1 = -(p.S * NormPDF(p.d1) * p.sigma) / (2.0 * std::sqrt(p.T));
	double term2 = p.r * p.K * std::exp(-p.r * p.T) * NormCDF(-p.d2);
	return (term1 + term2) / 365.0;
})

DEFINE_BS_FUNC(BSVega, {
	return p.S * NormPDF(p.d1) * std::sqrt(p.T) / 100.0;
})

// bs_implied_vol: Newton-Raphson to find sigma given option price
// Args: price, S, K, T, r, is_call (BOOLEAN)
static void BSImpliedVolFunc(DataChunk &args, ExpressionState &, Vector &result) {
	UnifiedVectorFormat pf, sf, kf, tf, rf, cf;
	args.data[0].ToUnifiedFormat(args.size(), pf);
	args.data[1].ToUnifiedFormat(args.size(), sf);
	args.data[2].ToUnifiedFormat(args.size(), kf);
	args.data[3].ToUnifiedFormat(args.size(), tf);
	args.data[4].ToUnifiedFormat(args.size(), rf);
	args.data[5].ToUnifiedFormat(args.size(), cf);
	auto prices = UnifiedVectorFormat::GetData<double>(pf);
	auto spots = UnifiedVectorFormat::GetData<double>(sf);
	auto strikes = UnifiedVectorFormat::GetData<double>(kf);
	auto times = UnifiedVectorFormat::GetData<double>(tf);
	auto rates = UnifiedVectorFormat::GetData<double>(rf);
	auto calls = UnifiedVectorFormat::GetData<bool>(cf);
	auto result_data = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < args.size(); i++) {
		auto pi = pf.sel->get_index(i);
		auto si = sf.sel->get_index(i);
		auto ki = kf.sel->get_index(i);
		auto ti = tf.sel->get_index(i);
		auto ri = rf.sel->get_index(i);
		auto ci = cf.sel->get_index(i);
		if (!pf.validity.RowIsValid(pi) || !sf.validity.RowIsValid(si)) {
			validity.SetInvalid(i); continue;
		}
		double price = prices[pi];
		double S = spots[si], K = strikes[ki], T = times[ti], r = rates[ri];
		bool is_call = calls[ci];

		// Newton-Raphson: start at 0.3, iterate up to 100 times
		double sigma = 0.3;
		for (int iter = 0; iter < 100; iter++) {
			auto p = ComputeBS(S, K, T, r, sigma);
			if (!p.valid) break;
			double model_price = is_call
			    ? S * NormCDF(p.d1) - K * std::exp(-r * T) * NormCDF(p.d2)
			    : K * std::exp(-r * T) * NormCDF(-p.d2) - S * NormCDF(-p.d1);
			double vega = S * NormPDF(p.d1) * std::sqrt(T);
			if (vega < 1e-12) break;
			double diff = model_price - price;
			if (std::fabs(diff) < 1e-8) break;
			sigma -= diff / vega;
			if (sigma <= 0) sigma = 0.001;
		}
		result_data[i] = sigma;
	}
}

void RegisterOptionsFunctions(Connection &conn, Catalog &catalog) {
	auto types5 = vector<LogicalType>{
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	    LogicalType::DOUBLE, LogicalType::DOUBLE};

	auto register_fn = [&](const string &name, scalar_function_t func) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction(name, types5, LogicalType::DOUBLE, func));
		CreateScalarFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	};

	register_fn("bs_call", BSCallFunc);
	register_fn("bs_put", BSPutFunc);
	register_fn("bs_delta_call", BSDeltaCallFunc);
	register_fn("bs_delta_put", BSDeltaPutFunc);
	register_fn("bs_gamma", BSGammaFunc);
	register_fn("bs_theta_call", BSThetaCallFunc);
	register_fn("bs_theta_put", BSThetaPutFunc);
	register_fn("bs_vega", BSVegaFunc);

	// bs_implied_vol(price, S, K, T, r, is_call)
	ScalarFunctionSet iv_set("bs_implied_vol");
	iv_set.AddFunction(ScalarFunction(
	    "bs_implied_vol",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	     LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::BOOLEAN},
	    LogicalType::DOUBLE, BSImpliedVolFunc));
	CreateScalarFunctionInfo iv_info(iv_set);
	catalog.CreateFunction(*conn.context, iv_info);
}

} // namespace scrooge
} // namespace duckdb
