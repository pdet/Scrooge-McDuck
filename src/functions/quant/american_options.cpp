#include "functions/quant.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include <algorithm>
#include <cmath>
#include <vector>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// American option pricing via Cox-Ross-Rubinstein binomial tree.
//
//   am_call(S, K, T, r, sigma [, steps])
//   am_put(S, K, T, r, sigma [, steps])
//
// `steps` defaults to 200 (a reasonable accuracy/speed trade-off).
// All dimensions are continuous: T in years, r and sigma annualized.
// ──────────────────────────────────────────────────────────────

namespace {

static double CRR(double S, double K, double T, double r, double sigma, int32_t steps, bool is_call) {
	if (S <= 0 || K <= 0 || T <= 0 || sigma <= 0 || steps <= 0) {
		return std::numeric_limits<double>::quiet_NaN();
	}
	double dt = T / (double)steps;
	double u = std::exp(sigma * std::sqrt(dt));
	double d = 1.0 / u;
	double a = std::exp(r * dt);
	double p = (a - d) / (u - d);
	if (p <= 0 || p >= 1) {
		// Tree no longer risk-neutral with these parameters.
		return std::numeric_limits<double>::quiet_NaN();
	}
	double disc = std::exp(-r * dt);

	std::vector<double> values((size_t)steps + 1);
	for (int32_t i = 0; i <= steps; i++) {
		double price = S * std::pow(u, (double)(steps - i)) * std::pow(d, (double)i);
		double payoff = is_call ? std::max(price - K, 0.0) : std::max(K - price, 0.0);
		values[i] = payoff;
	}
	for (int32_t step = steps - 1; step >= 0; step--) {
		for (int32_t i = 0; i <= step; i++) {
			double cont = disc * (p * values[i] + (1.0 - p) * values[i + 1]);
			double price = S * std::pow(u, (double)(step - i)) * std::pow(d, (double)i);
			double exer = is_call ? price - K : K - price;
			values[i] = std::max(cont, exer);
		}
	}
	return values[0];
}

template <bool IS_CALL>
static void AmFunc(DataChunk &args, ExpressionState &, Vector &result) {
	idx_t n_args = args.ColumnCount();
	UnifiedVectorFormat sf, kf, tf, rf, sigf, stf;
	args.data[0].ToUnifiedFormat(args.size(), sf);
	args.data[1].ToUnifiedFormat(args.size(), kf);
	args.data[2].ToUnifiedFormat(args.size(), tf);
	args.data[3].ToUnifiedFormat(args.size(), rf);
	args.data[4].ToUnifiedFormat(args.size(), sigf);
	if (n_args > 5) {
		args.data[5].ToUnifiedFormat(args.size(), stf);
	}
	auto sd = UnifiedVectorFormat::GetData<double>(sf);
	auto kd = UnifiedVectorFormat::GetData<double>(kf);
	auto td = UnifiedVectorFormat::GetData<double>(tf);
	auto rd = UnifiedVectorFormat::GetData<double>(rf);
	auto sigd = UnifiedVectorFormat::GetData<double>(sigf);
	const int32_t *std_steps = nullptr;
	if (n_args > 5) {
		std_steps = UnifiedVectorFormat::GetData<int32_t>(stf);
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < args.size(); i++) {
		auto si = sf.sel->get_index(i);
		auto ki = kf.sel->get_index(i);
		auto ti = tf.sel->get_index(i);
		auto ri = rf.sel->get_index(i);
		auto sigi = sigf.sel->get_index(i);
		if (!sf.validity.RowIsValid(si) || !kf.validity.RowIsValid(ki) ||
		    !tf.validity.RowIsValid(ti) || !rf.validity.RowIsValid(ri) ||
		    !sigf.validity.RowIsValid(sigi)) {
			validity.SetInvalid(i);
			continue;
		}
		int32_t steps = 200;
		if (std_steps) {
			auto sti = stf.sel->get_index(i);
			if (stf.validity.RowIsValid(sti)) {
				steps = std_steps[sti];
			}
		}
		double v = CRR(sd[si], kd[ki], td[ti], rd[ri], sigd[sigi], steps, IS_CALL);
		if (!std::isfinite(v)) {
			validity.SetInvalid(i);
		} else {
			out[i] = v;
		}
	}
}

} // namespace

void RegisterAmericanOptions(Connection &conn, Catalog &catalog) {
	auto t5 = vector<LogicalType>{LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                                LogicalType::DOUBLE, LogicalType::DOUBLE};
	auto t6 = vector<LogicalType>{LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                                LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::INTEGER};

	{
		ScalarFunctionSet set("am_call");
		set.AddFunction(ScalarFunction("am_call", t5, LogicalType::DOUBLE, AmFunc<true>));
		set.AddFunction(ScalarFunction("am_call", t6, LogicalType::DOUBLE, AmFunc<true>));
		CreateScalarFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	}
	{
		ScalarFunctionSet set("am_put");
		set.AddFunction(ScalarFunction("am_put", t5, LogicalType::DOUBLE, AmFunc<false>));
		set.AddFunction(ScalarFunction("am_put", t6, LogicalType::DOUBLE, AmFunc<false>));
		CreateScalarFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	}
}

} // namespace scrooge
} // namespace duckdb
