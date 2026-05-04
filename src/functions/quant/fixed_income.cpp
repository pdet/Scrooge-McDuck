#include "functions/quant.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include <cmath>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// Fixed-income pricing on plain coupon bonds.
//
// Conventions: `face` is principal, `coupon_rate` is annual decimal
// (0.05 = 5%), `ytm` is annual decimal yield, `n_periods` is the
// total number of coupon periods, `freq` is coupons per year (typ. 2).
// All cash flows are equally spaced; settlement is on a coupon date.
//
//   bond_price(face, coupon_rate, ytm, n_periods, freq) → DOUBLE
//   bond_ytm(price, face, coupon_rate, n_periods, freq) → DOUBLE
//   bond_macaulay_duration(face, coupon_rate, ytm, n_periods, freq) → DOUBLE
//   bond_modified_duration(face, coupon_rate, ytm, n_periods, freq) → DOUBLE
//   bond_convexity(face, coupon_rate, ytm, n_periods, freq) → DOUBLE
// ──────────────────────────────────────────────────────────────

namespace {

static double BondPrice(double face, double coupon_rate, double ytm, int32_t n, double freq) {
	if (n <= 0 || freq <= 0) {
		return std::numeric_limits<double>::quiet_NaN();
	}
	double c = face * coupon_rate / freq; // periodic coupon
	double y = ytm / freq;                // periodic yield
	double price = 0.0;
	for (int32_t t = 1; t <= n; t++) {
		price += c / std::pow(1.0 + y, (double)t);
	}
	price += face / std::pow(1.0 + y, (double)n);
	return price;
}

static double BondYTM(double price, double face, double coupon_rate, int32_t n, double freq) {
	if (price <= 0 || n <= 0 || freq <= 0) {
		return std::numeric_limits<double>::quiet_NaN();
	}
	// Newton-Raphson; seed with coupon_rate as a reasonable guess.
	double y_annual = coupon_rate > 0 ? coupon_rate : 0.05;
	for (int iter = 0; iter < 100; iter++) {
		double y = y_annual / freq;
		double p = 0.0, dp = 0.0;
		double c = face * coupon_rate / freq;
		for (int32_t t = 1; t <= n; t++) {
			double disc = std::pow(1.0 + y, (double)t);
			p += c / disc;
			dp -= (double)t * c / (disc * (1.0 + y));
		}
		double disc_n = std::pow(1.0 + y, (double)n);
		p += face / disc_n;
		dp -= (double)n * face / (disc_n * (1.0 + y));
		// dp is dPrice/dy (per-period y); convert to per-annual.
		double dp_annual = dp / freq;
		double diff = p - price;
		if (std::fabs(diff) < 1e-9) {
			return y_annual;
		}
		if (std::fabs(dp_annual) < 1e-14) {
			break;
		}
		y_annual -= diff / dp_annual;
		if (y_annual <= -0.999) {
			y_annual = -0.99; // keep 1+y > 0
		}
	}
	return y_annual;
}

static double MacaulayDuration(double face, double coupon_rate, double ytm, int32_t n, double freq) {
	if (n <= 0 || freq <= 0) {
		return std::numeric_limits<double>::quiet_NaN();
	}
	double c = face * coupon_rate / freq;
	double y = ytm / freq;
	double price = BondPrice(face, coupon_rate, ytm, n, freq);
	if (price <= 0) {
		return std::numeric_limits<double>::quiet_NaN();
	}
	double weighted = 0.0;
	for (int32_t t = 1; t <= n; t++) {
		weighted += (double)t * c / std::pow(1.0 + y, (double)t);
	}
	weighted += (double)n * face / std::pow(1.0 + y, (double)n);
	// In years: divide by freq.
	return weighted / price / freq;
}

static double ModifiedDuration(double face, double coupon_rate, double ytm, int32_t n, double freq) {
	double mac = MacaulayDuration(face, coupon_rate, ytm, n, freq);
	return mac / (1.0 + ytm / freq);
}

static double Convexity(double face, double coupon_rate, double ytm, int32_t n, double freq) {
	if (n <= 0 || freq <= 0) {
		return std::numeric_limits<double>::quiet_NaN();
	}
	double c = face * coupon_rate / freq;
	double y = ytm / freq;
	double price = BondPrice(face, coupon_rate, ytm, n, freq);
	if (price <= 0) {
		return std::numeric_limits<double>::quiet_NaN();
	}
	double sum = 0.0;
	for (int32_t t = 1; t <= n; t++) {
		double tt = (double)t * (double)(t + 1);
		sum += tt * c / std::pow(1.0 + y, (double)(t + 2));
	}
	double tt_n = (double)n * (double)(n + 1);
	sum += tt_n * face / std::pow(1.0 + y, (double)(n + 2));
	// Convexity in years^2.
	return sum / price / (freq * freq);
}

template <double (*FN)(double, double, double, int32_t, double)>
static void BondScalarFunc(DataChunk &args, ExpressionState &, Vector &result) {
	UnifiedVectorFormat a, b, c, d, e;
	args.data[0].ToUnifiedFormat(args.size(), a);
	args.data[1].ToUnifiedFormat(args.size(), b);
	args.data[2].ToUnifiedFormat(args.size(), c);
	args.data[3].ToUnifiedFormat(args.size(), d);
	args.data[4].ToUnifiedFormat(args.size(), e);
	auto av = UnifiedVectorFormat::GetData<double>(a);
	auto bv = UnifiedVectorFormat::GetData<double>(b);
	auto cv = UnifiedVectorFormat::GetData<double>(c);
	auto dv = UnifiedVectorFormat::GetData<int32_t>(d);
	auto ev = UnifiedVectorFormat::GetData<double>(e);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetData<double>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < args.size(); i++) {
		auto ai = a.sel->get_index(i);
		auto bi = b.sel->get_index(i);
		auto ci = c.sel->get_index(i);
		auto di = d.sel->get_index(i);
		auto ei = e.sel->get_index(i);
		if (!a.validity.RowIsValid(ai) || !b.validity.RowIsValid(bi) ||
		    !c.validity.RowIsValid(ci) || !d.validity.RowIsValid(di) ||
		    !e.validity.RowIsValid(ei)) {
			validity.SetInvalid(i);
			continue;
		}
		double v = FN(av[ai], bv[bi], cv[ci], dv[di], ev[ei]);
		if (!std::isfinite(v)) {
			validity.SetInvalid(i);
		} else {
			out[i] = v;
		}
	}
}

} // namespace

void RegisterFixedIncome(Connection &conn, Catalog &catalog) {
	auto types5 = vector<LogicalType>{LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                                    LogicalType::INTEGER, LogicalType::DOUBLE};

	auto reg = [&](const string &name, scalar_function_t fn) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction(name, types5, LogicalType::DOUBLE, fn));
		CreateScalarFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	};

	reg("bond_price", BondScalarFunc<BondPrice>);
	reg("bond_ytm", BondScalarFunc<BondYTM>);
	reg("bond_macaulay_duration", BondScalarFunc<MacaulayDuration>);
	reg("bond_modified_duration", BondScalarFunc<ModifiedDuration>);
	reg("bond_convexity", BondScalarFunc<Convexity>);
}

} // namespace scrooge
} // namespace duckdb
