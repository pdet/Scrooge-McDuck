#include "functions/quant.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <limits>
#include <vector>

namespace duckdb {
namespace scrooge {

// ──────────────────────────────────────────────────────────────
// Long-only signal-driven backtester.
//
// All three table functions take a query that yields:
//   (ts TIMESTAMP, price DOUBLE, signal VARCHAR)
// where signal ∈ {'BUY', 'SELL', 'HOLD'} (case-insensitive, NULL = HOLD).
//
// The simulator: starts with `capital` cash. On 'BUY' (when flat),
// invest all cash at next bar's price (using current bar — close-of-bar
// fill model). On 'SELL' (when long), liquidate at the current bar's
// price. Commission & slippage are charged as a fraction per fill
// (e.g. 0.001 = 10 bps).
//
// Functions:
//   backtest_equity(query, capital, commission, slippage)
//      → (ts TIMESTAMP, price DOUBLE, position DOUBLE,
//         cash DOUBLE, equity DOUBLE)
//   backtest_trades(query, capital, commission, slippage)
//      → (entry_ts TIMESTAMP, exit_ts TIMESTAMP,
//         entry_price DOUBLE, exit_price DOUBLE, pnl DOUBLE,
//         return DOUBLE, bars_held BIGINT)
//   backtest_stats(query, capital, commission, slippage)
//      → (metric VARCHAR, value DOUBLE)
// ──────────────────────────────────────────────────────────────

namespace {

struct Bar {
	int64_t ts; // TIMESTAMP micros
	double price;
	int8_t signal; // 1 = BUY, -1 = SELL, 0 = HOLD
};

struct Trade {
	int64_t entry_ts;
	int64_t exit_ts;
	double entry_price;
	double exit_price;
	double pnl;
	double ret;
	int64_t bars_held;
};

struct EquityRow {
	int64_t ts;
	double price;
	double position; // shares
	double cash;
	double equity;
};

struct SimResult {
	vector<EquityRow> equity;
	vector<Trade> trades;
	double final_equity;
	double initial_capital;
};

static int8_t ParseSignal(const string &s) {
	if (s.empty()) {
		return 0;
	}
	// Case-insensitive comparison
	string up;
	up.reserve(s.size());
	for (auto c : s) {
		up.push_back((char)std::toupper((unsigned char)c));
	}
	if (up == "BUY") {
		return 1;
	}
	if (up == "SELL") {
		return -1;
	}
	return 0;
}

static vector<Bar> LoadBars(ClientContext &context, const string &query) {
	Connection conn(*context.db);
	// Cast columns and order by ts to ensure deterministic execution.
	auto pulled = conn.Query(
	    "SELECT ts::TIMESTAMP, price::DOUBLE, signal::VARCHAR FROM (" + query + ") ORDER BY ts");
	if (pulled->HasError()) {
		throw InvalidInputException("backtest input query failed: %s", pulled->GetError());
	}
	vector<Bar> bars;
	while (true) {
		auto chunk = pulled->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto ts_v = chunk->GetValue(0, i);
			auto px_v = chunk->GetValue(1, i);
			auto sg_v = chunk->GetValue(2, i);
			if (ts_v.IsNull() || px_v.IsNull()) {
				continue;
			}
			Bar b;
			b.ts = ts_v.GetValue<timestamp_t>().value;
			b.price = px_v.GetValue<double>();
			b.signal = sg_v.IsNull() ? 0 : ParseSignal(sg_v.GetValue<string>());
			bars.push_back(b);
		}
	}
	return bars;
}

static SimResult Simulate(const vector<Bar> &bars, double capital, double commission, double slippage) {
	SimResult r;
	r.initial_capital = capital;
	double cash = capital;
	double position = 0.0;
	double entry_price = 0.0;
	double entry_cost = 0.0; // cash actually spent on the open position
	int64_t entry_ts = 0;
	int64_t entry_idx = 0;

	r.equity.reserve(bars.size());

	for (idx_t i = 0; i < bars.size(); i++) {
		const auto &b = bars[i];
		if (b.signal == 1 && position == 0.0 && cash > 0) {
			// Buy: pay (1 + slippage) * price per share, then commission on notional.
			double fill_price = b.price * (1.0 + slippage);
			double notional = cash / (1.0 + commission);
			double shares = notional / fill_price;
			double cost = shares * fill_price * (1.0 + commission);
			position = shares;
			cash -= cost;
			entry_price = fill_price;
			entry_cost = cost;
			entry_ts = b.ts;
			entry_idx = (int64_t)i;
		} else if (b.signal == -1 && position > 0.0) {
			double fill_price = b.price * (1.0 - slippage);
			double proceeds = position * fill_price * (1.0 - commission);
			Trade t;
			t.entry_ts = entry_ts;
			t.exit_ts = b.ts;
			t.entry_price = entry_price;
			t.exit_price = fill_price;
			t.pnl = proceeds - entry_cost;
			t.ret = entry_cost > 0 ? t.pnl / entry_cost : 0.0;
			t.bars_held = (int64_t)i - entry_idx;
			r.trades.push_back(t);
			cash += proceeds;
			position = 0.0;
		}

		EquityRow er;
		er.ts = b.ts;
		er.price = b.price;
		er.position = position;
		er.cash = cash;
		er.equity = cash + position * b.price;
		r.equity.push_back(er);
	}

	r.final_equity = r.equity.empty() ? capital : r.equity.back().equity;
	return r;
}

struct StatRow {
	string name;
	double value;
};

// Common bind data
struct BTBindData : public TableFunctionData {
	SimResult sim;
	vector<StatRow> stats;
	idx_t cur = 0;
	int mode = 0; // 0 = equity, 1 = trades, 2 = stats
};

static SimResult BuildSim(ClientContext &context, TableFunctionBindInput &input) {
	auto query = input.inputs[0].GetValue<string>();
	double capital = input.inputs[1].GetValue<double>();
	double commission = input.inputs.size() > 2 && !input.inputs[2].IsNull() ? input.inputs[2].GetValue<double>() : 0.0;
	double slippage = input.inputs.size() > 3 && !input.inputs[3].IsNull() ? input.inputs[3].GetValue<double>() : 0.0;
	if (capital <= 0) {
		throw InvalidInputException("capital must be positive");
	}
	if (commission < 0 || slippage < 0) {
		throw InvalidInputException("commission and slippage must be non-negative");
	}
	auto bars = LoadBars(context, query);
	if (bars.empty()) {
		throw InvalidInputException("backtest: input query produced no rows");
	}
	return Simulate(bars, capital, commission, slippage);
}

static unique_ptr<FunctionData> EquityBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<BTBindData>();
	data->sim = BuildSim(context, input);
	data->mode = 0;
	names = {"ts", "price", "position", "cash", "equity"};
	return_types = {LogicalType::TIMESTAMP, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::DOUBLE};
	return std::move(data);
}

static unique_ptr<FunctionData> TradesBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<BTBindData>();
	data->sim = BuildSim(context, input);
	data->mode = 1;
	names = {"entry_ts", "exit_ts", "entry_price", "exit_price", "pnl", "return", "bars_held"};
	return_types = {LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::DOUBLE,
	                LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::BIGINT};
	return std::move(data);
}

static vector<StatRow> ComputeStats(const SimResult &sim);

static unique_ptr<FunctionData> StatsBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<BTBindData>();
	data->sim = BuildSim(context, input);
	data->stats = ComputeStats(data->sim);
	data->mode = 2;
	names = {"metric", "value"};
	return_types = {LogicalType::VARCHAR, LogicalType::DOUBLE};
	return std::move(data);
}

static void EquityScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &d = (BTBindData &)*input.bind_data;
	auto ts_data = FlatVector::GetData<timestamp_t>(output.data[0]);
	auto px_data = FlatVector::GetData<double>(output.data[1]);
	auto pos_data = FlatVector::GetData<double>(output.data[2]);
	auto cash_data = FlatVector::GetData<double>(output.data[3]);
	auto eq_data = FlatVector::GetData<double>(output.data[4]);
	idx_t out = 0;
	while (out < STANDARD_VECTOR_SIZE && d.cur < d.sim.equity.size()) {
		auto &row = d.sim.equity[d.cur];
		ts_data[out].value = row.ts;
		px_data[out] = row.price;
		pos_data[out] = row.position;
		cash_data[out] = row.cash;
		eq_data[out] = row.equity;
		out++;
		d.cur++;
	}
	output.SetCardinality(out);
}

static void TradesScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &d = (BTBindData &)*input.bind_data;
	auto ets = FlatVector::GetData<timestamp_t>(output.data[0]);
	auto xts = FlatVector::GetData<timestamp_t>(output.data[1]);
	auto epx = FlatVector::GetData<double>(output.data[2]);
	auto xpx = FlatVector::GetData<double>(output.data[3]);
	auto pnl = FlatVector::GetData<double>(output.data[4]);
	auto ret = FlatVector::GetData<double>(output.data[5]);
	auto held = FlatVector::GetData<int64_t>(output.data[6]);
	idx_t out = 0;
	while (out < STANDARD_VECTOR_SIZE && d.cur < d.sim.trades.size()) {
		auto &t = d.sim.trades[d.cur];
		ets[out].value = t.entry_ts;
		xts[out].value = t.exit_ts;
		epx[out] = t.entry_price;
		xpx[out] = t.exit_price;
		pnl[out] = t.pnl;
		ret[out] = t.ret;
		held[out] = t.bars_held;
		out++;
		d.cur++;
	}
	output.SetCardinality(out);
}

// Compute summary stats from the simulation.
static vector<StatRow> ComputeStats(const SimResult &sim) {
	vector<StatRow> s;
	double initial = sim.initial_capital;
	double final_eq = sim.final_equity;
	double total_return = (final_eq / initial) - 1.0;
	s.push_back({"initial_capital", initial});
	s.push_back({"final_equity", final_eq});
	s.push_back({"total_return", total_return});
	s.push_back({"num_bars", (double)sim.equity.size()});
	s.push_back({"num_trades", (double)sim.trades.size()});

	// Bar-by-bar returns.
	double mean_r = 0.0;
	double m2 = 0.0;
	idx_t n = 0;
	double prev = 0.0;
	bool have_prev = false;
	double peak = 0.0;
	double max_dd = 0.0;
	for (auto &row : sim.equity) {
		if (peak == 0.0 || row.equity > peak) {
			peak = row.equity;
		}
		if (peak > 0) {
			double dd = (row.equity / peak) - 1.0; // negative
			if (dd < max_dd) {
				max_dd = dd;
			}
		}
		if (have_prev && prev > 0) {
			double r = (row.equity / prev) - 1.0;
			n++;
			double delta = r - mean_r;
			mean_r += delta / (double)n;
			m2 += delta * (r - mean_r);
		}
		prev = row.equity;
		have_prev = true;
	}
	double var_r = n > 1 ? m2 / (double)(n - 1) : 0.0;
	double std_r = std::sqrt(var_r);
	double sharpe_daily = std_r > 0 ? mean_r / std_r : 0.0;
	double sharpe_ann = sharpe_daily * std::sqrt(252.0);

	// Annualized return assuming 252 trading bars/year.
	double years = sim.equity.size() > 1 ? (double)(sim.equity.size() - 1) / 252.0 : 0.0;
	double cagr = years > 0 && initial > 0 && final_eq > 0
	                  ? std::pow(final_eq / initial, 1.0 / years) - 1.0
	                  : 0.0;

	s.push_back({"cagr", cagr});
	s.push_back({"annualized_volatility", std_r * std::sqrt(252.0)});
	s.push_back({"sharpe_ratio", sharpe_ann});
	s.push_back({"max_drawdown", max_dd});

	// Trade stats.
	idx_t wins = 0;
	double sum_win = 0.0, sum_loss = 0.0;
	for (auto &t : sim.trades) {
		if (t.pnl > 0) {
			wins++;
			sum_win += t.pnl;
		} else {
			sum_loss += -t.pnl;
		}
	}
	double win_rate = sim.trades.empty() ? 0.0 : (double)wins / (double)sim.trades.size();
	double profit_factor = sum_loss > 0 ? sum_win / sum_loss : (sum_win > 0 ? std::numeric_limits<double>::infinity() : 0.0);
	s.push_back({"win_rate", win_rate});
	s.push_back({"profit_factor", profit_factor});
	return s;
}

static void StatsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &d = (BTBindData &)*input.bind_data;
	auto name_data = FlatVector::GetData<string_t>(output.data[0]);
	auto val_data = FlatVector::GetData<double>(output.data[1]);
	idx_t out = 0;
	while (out < STANDARD_VECTOR_SIZE && d.cur < d.stats.size()) {
		name_data[out] = StringVector::AddString(output.data[0], d.stats[d.cur].name);
		val_data[out] = d.stats[d.cur].value;
		out++;
		d.cur++;
	}
	output.SetCardinality(out);
}

} // namespace

void RegisterBacktest(Connection &conn, Catalog &catalog) {
	auto types_full = vector<LogicalType>{LogicalType::VARCHAR, LogicalType::DOUBLE,
	                                        LogicalType::DOUBLE, LogicalType::DOUBLE};
	auto types_min = vector<LogicalType>{LogicalType::VARCHAR, LogicalType::DOUBLE};
	auto types_mid = vector<LogicalType>{LogicalType::VARCHAR, LogicalType::DOUBLE, LogicalType::DOUBLE};

	{
		TableFunctionSet set("backtest_equity");
		set.AddFunction(TableFunction(types_min, EquityScan, EquityBind));
		set.AddFunction(TableFunction(types_mid, EquityScan, EquityBind));
		set.AddFunction(TableFunction(types_full, EquityScan, EquityBind));
		CreateTableFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	}
	{
		TableFunctionSet set("backtest_trades");
		set.AddFunction(TableFunction(types_min, TradesScan, TradesBind));
		set.AddFunction(TableFunction(types_mid, TradesScan, TradesBind));
		set.AddFunction(TableFunction(types_full, TradesScan, TradesBind));
		CreateTableFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	}
	{
		TableFunctionSet set("backtest_stats");
		set.AddFunction(TableFunction(types_min, StatsScan, StatsBind));
		set.AddFunction(TableFunction(types_mid, StatsScan, StatsBind));
		set.AddFunction(TableFunction(types_full, StatsScan, StatsBind));
		CreateTableFunctionInfo info(set);
		catalog.CreateFunction(*conn.context, info);
	}
}

} // namespace scrooge
} // namespace duckdb
