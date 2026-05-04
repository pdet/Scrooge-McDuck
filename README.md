# Scrooge McDuck

A financial analysis extension for [DuckDB](https://www.duckdb.org). 59+ SQL functions for technical analysis, risk management, portfolio analytics, options pricing, and market data access.

> **Note:** This codebase is currently maintained and updated by an AI agent running Claude Code. While all changes go through CI and automated testing, use with appropriate caution and review before relying on it for financial decisions.

> **Disclaimer:** This extension is not affiliated with the [DuckDB Foundation](https://duckdb.org/foundation/) or [DuckDB Labs](https://duckdblabs.com/). Binaries are unsigned.

```sql
INSTALL scrooge FROM community;
LOAD scrooge;

SELECT date, close,
    ema(close, date::TIMESTAMPTZ, 20) as ema_20,
    rsi(close, date::TIMESTAMPTZ, 14) as rsi_14,
    bollinger_signal(close,
        bollinger_upper(close, date::TIMESTAMPTZ, 20, 2.0),
        bollinger_lower(close, date::TIMESTAMPTZ, 20, 2.0)) as signal
FROM yahoo_finance('AAPL', '2024-01-01', '2025-01-01', '1d')
GROUP BY ALL;
```

## Quick Start

```sql
INSTALL scrooge FROM community;
LOAD scrooge;

-- Stock data
SELECT * FROM yahoo_finance('MSFT', '2024-01-01', '2025-01-01', '1d');

-- Crypto data (free, no API key)
SELECT * FROM coingecko('bitcoin', 'usd', 365);

-- Economic data (requires free FRED API key)
SELECT * FROM fred_series('GDP', 'your_api_key');
```

## Functions

### Technical Indicators

| Function | Description |
|----------|-------------|
| `ema(close, timestamp, period)` | Exponential Moving Average |
| `dema(close, timestamp, period)` | Double EMA (reduced lag) |
| `tema(close, timestamp, period)` | Triple EMA (minimal lag) |
| `wma(close, timestamp, period)` | Weighted Moving Average |
| `rsi(close, timestamp, period)` | Relative Strength Index |
| `macd(close, timestamp)` | MACD Line |
| `bollinger_upper/lower/middle/width(close, ts, period, stddev)` | Bollinger Bands |
| `vwap(price, volume)` | Volume Weighted Average Price |
| `obv(close, volume, timestamp)` | On-Balance Volume |
| `atr(high, low, close, timestamp, period)` | Average True Range |
| `stochastic_k(high, low, close, timestamp, period)` | Stochastic Oscillator %K |
| `mfi(high, low, close, volume, timestamp, period)` | Money Flow Index |
| `cmf(high, low, close, volume, timestamp, period)` | Chaikin Money Flow |
| `ad_line(high, low, close, volume, timestamp)` | Accumulation/Distribution Line |
| `pivot_point/pivot_r1/pivot_r2/pivot_r3/pivot_s1/pivot_s2/pivot_s3(h, l, c)` | Pivot Points |
| `williams_r(high, low, close, ts [, period])` | Williams %R momentum oscillator |
| `adx / plus_di / minus_di(high, low, close, ts [, period])` | Average Directional Index + DMI |
| `ichimoku_tenkan / ichimoku_kijun / ichimoku_senkou_a / ichimoku_senkou_b(high, low, ts)` | Ichimoku Cloud lines |
| `keltner_upper / keltner_middle / keltner_lower(high, low, close, ts [, period [, multiplier]])` | Keltner Channels |
| `ha_open / ha_high / ha_low / ha_close(open, high, low, close, ts)` | Heikin-Ashi candles |

### Returns and Risk

| Function | Description |
|----------|-------------|
| `simple_return(close, timestamp)` | Simple return |
| `log_return(close, timestamp)` | Logarithmic return |
| `cumulative_return(close, timestamp)` | Cumulative return |
| `sharpe_ratio(returns, timestamp)` | Sharpe ratio |
| `sortino_ratio(returns, timestamp)` | Sortino ratio |
| `max_drawdown(close, timestamp)` | Maximum drawdown |
| `value_at_risk(returns, timestamp, confidence)` | Value at Risk |
| `annualized_volatility(returns)` | Annualized volatility |
| `calmar_ratio(returns, close, timestamp)` | Calmar ratio |
| `information_ratio(asset_returns, benchmark_returns)` | Information ratio |
| `drawdown_duration(close, timestamp)` | Longest drawdown in days |
| `profit_factor(returns)` | Sum of wins / sum of losses |

### Candlestick Patterns

| Function | Returns |
|----------|---------|
| `is_doji(open, high, low, close)` | `BOOLEAN` |
| `is_hammer(open, high, low, close)` | `BOOLEAN` |
| `is_shooting_star(open, high, low, close)` | `BOOLEAN` |
| `is_engulfing(open, high, low, close, prev_open, prev_close)` | `'BULLISH'/'BEARISH'/NULL` |
| `is_morning_star(o1, c1, o2, h2, l2, c2, o3, c3)` | `BOOLEAN` |
| `is_evening_star(o1, c1, o2, h2, l2, c2, o3, c3)` | `BOOLEAN` |

### Portfolio Analytics

| Function | Description |
|----------|-------------|
| `portfolio_correlation(returns_a, returns_b)` | Pearson correlation |
| `portfolio_beta(asset_returns, benchmark_returns)` | Systematic risk (beta) |
| `momentum_score(close, timestamp, period_days)` | Rate-of-change momentum |
| `relative_strength(asset_close, benchmark_close, ts)` | Performance vs benchmark |

### Signal Generation

| Function | Description |
|----------|-------------|
| `ma_crossover_signal(fast_ma, slow_ma)` | `'BUY'/'SELL'/'HOLD'` |
| `rsi_signal(rsi [, overbought, oversold])` | `'BUY'/'SELL'/'HOLD'` |
| `bollinger_signal(close, upper, lower)` | `'BUY'/'SELL'/'HOLD'` |
| `kelly_fraction(win_rate, avg_win, avg_loss)` | Optimal position size [0,1] |
| `composite_score(rsi, macd, bb_pct, obv_pct)` | Multi-indicator conviction [-100,100] |

### Options Pricing (Black-Scholes + American)

| Function | Description |
|----------|-------------|
| `bs_call(S, K, T, r, sigma)` | European call price |
| `bs_put(S, K, T, r, sigma)` | European put price |
| `bs_delta_call / bs_delta_put` | Delta |
| `bs_gamma` | Gamma |
| `bs_theta_call / bs_theta_put` | Theta (per day) |
| `bs_vega` | Vega (per 1% vol) |
| `bs_implied_vol(price, S, K, T, r, is_call)` | Implied volatility |
| `am_call(S, K, T, r, sigma [, steps])` | American call (CRR binomial) |
| `am_put(S, K, T, r, sigma [, steps])` | American put (CRR binomial) |

### Fixed Income

| Function | Description |
|----------|-------------|
| `bond_price(face, coupon_rate, ytm, n_periods, freq)` | Coupon bond price |
| `bond_ytm(price, face, coupon_rate, n_periods, freq)` | Yield to maturity |
| `bond_macaulay_duration(face, coupon_rate, ytm, n_periods, freq)` | Macaulay duration (years) |
| `bond_modified_duration(face, coupon_rate, ytm, n_periods, freq)` | Modified duration (years) |
| `bond_convexity(face, coupon_rate, ytm, n_periods, freq)` | Convexity (years²) |

### Time-Series Statistics

| Function | Description |
|----------|-------------|
| `hurst_exponent(value, ts)` | Hurst exponent via R/S analysis |
| `adf_test_stat(value, ts)` | Augmented Dickey-Fuller t-statistic (lag 0) |

### Data Scanners

| Function | Description |
|----------|-------------|
| `yahoo_finance(symbol, from, to, interval)` | Yahoo Finance OHLCV data |
| `coingecko(coin_id, vs_currency, days)` | CoinGecko crypto OHLC data |
| `fred_series(series_id, api_key [, start, end])` | Federal Reserve economic data |
| `read_eth(url, method, from_block, to_block)` | Ethereum blockchain logs |
| `sec_filings(cik_or_ticker [, form [, limit]])` | SEC EDGAR filing index |
| `sec_facts(cik_or_ticker, concept [, taxonomy])` | SEC EDGAR XBRL fundamentals |
| `polygon_aggs(symbol, multiplier, timespan, from, to, api_key)` | Polygon.io OHLCV aggregates |
| `binance_klines(symbol, interval [, limit])` | Binance spot klines (no auth) |
| `json_rpc(url, method [, params_json])` | Generic JSON-RPC 2.0 client (any blockchain or RPC service) |
| `alpha_vantage_news(tickers [, time_from [, time_to [, limit]]])` | Alpha Vantage news + sentiment |

### Configuration

Set API keys and other defaults once per session via `SET`:

```sql
SET fred_api_key = 'YOUR_FRED_KEY';
SET polygon_api_key = 'YOUR_POLYGON_KEY';
SET alpha_vantage_api_key = 'YOUR_AV_KEY';
SET sec_user_agent = 'Your Name your.email@example.com';
```

When configured, scanners can be called without the api_key argument:

```sql
SELECT * FROM fred_series('GDP');                    -- uses fred_api_key
SELECT * FROM polygon_aggs('AAPL', 1, 'day',
                           '2024-01-01', '2024-12-31'); -- uses polygon_api_key
```

### Macros

A small library of analytical helpers ships preregistered:

| Macro | Description |
|-------|-------------|
| `pct_change(curr, prev)` | Percent change with zero-protection |
| `safe_div(a, b)` | `a / b` returning NULL when `b = 0` |
| `clip(x, lo, hi)` | Clamp a value into a range |
| `zscore(x, mean, stddev)` | Standard score |
| `pct_to_bps(p)` / `bps_to_pct(b)` | Percent ↔ basis-point conversions |
| `resample_ohlc(table_name, interval)` | Resample OHLCV to a coarser bucket |
| `with_returns(table_name)` | Append simple_return / log_return to a (ts, price) series |

### Quant Tooling

| Function | Description |
|----------|-------------|
| `monte_carlo(start_price, mu, sigma, horizon_days, num_paths [, seed])` | GBM price-path simulation |
| `min_variance_portfolio(returns_query)` | Closed-form min-variance allocation |
| `max_sharpe_portfolio(returns_query [, risk_free_rate])` | Tangency / max-Sharpe allocation |
| `efficient_frontier(returns_query, num_points [, risk_free_rate])` | Mean-variance frontier portfolios |
| `backtest_equity(query, capital [, commission [, slippage]])` | Per-bar equity curve from a signal stream |
| `backtest_trades(query, capital [, commission [, slippage]])` | Closed trades with PnL and return |
| `backtest_stats(query, capital [, commission [, slippage]])` | Summary stats: CAGR, Sharpe, max DD, win rate, profit factor |

The portfolio-optimization functions take a SQL query (as VARCHAR) returning
`(symbol VARCHAR, return DOUBLE)` aligned by row position. The backtest
functions take a query returning `(ts TIMESTAMP, price DOUBLE, signal VARCHAR)`
where `signal` ∈ `'BUY'`, `'SELL'`, `'HOLD'` (case-insensitive).

### Utility

| Function | Description |
|----------|-------------|
| `first_s(value, timestamp)` | First value by timestamp |
| `last_s(value, timestamp)` | Last value by timestamp |
| `timebucket(timestamp, interval)` | Time bucketing |

## Examples

### Analysis Pipeline

```sql
WITH prices AS (
    SELECT * FROM yahoo_finance('AAPL', '2024-01-01', '2025-01-01', '1d')
)
SELECT date, close,
    rsi(close, date::TIMESTAMPTZ, 14) as rsi_14,
    rsi_signal(rsi(close, date::TIMESTAMPTZ, 14)) as rsi_signal,
    sharpe_ratio(simple_return(close, date::TIMESTAMPTZ), date::TIMESTAMPTZ) as sharpe
FROM prices
GROUP BY ALL;
```

### Options Analysis

```sql
-- Price a call option and compute Greeks
SELECT
    bs_call(150, 145, 0.25, 0.05, 0.30) as call_price,
    bs_delta_call(150, 145, 0.25, 0.05, 0.30) as delta,
    bs_gamma(150, 145, 0.25, 0.05, 0.30) as gamma,
    bs_implied_vol(12.5, 150, 145, 0.25, 0.05, true) as implied_vol;
```

### Portfolio Diversification

```sql
SELECT
    portfolio_correlation(aapl_ret, msft_ret) as correlation,
    portfolio_beta(aapl_ret, spy_ret) as beta
FROM returns_table;
```

## Build

```bash
git clone --recurse-submodules https://github.com/pdet/Scrooge-McDuck.git
cd Scrooge-McDuck
GEN=ninja make
```

## Why Scrooge

1. **Pure SQL** -- no Python glue code needed for financial analysis
2. **Privacy** -- DuckDB runs locally, portfolio data stays on your machine
3. **Free** -- MIT licensed, no cloud costs
4. **Fast** -- columnar storage and vectorized execution
5. **Composable** -- combine indicators, signals, and data sources in a single query

## Cookbook

End-to-end recipes (momentum strategy + backtest, pairs trading,
efficient frontier, options screener, Monte Carlo retirement, EDGAR DCF
inputs, FRED macro overlay) live in [EXAMPLES.md](EXAMPLES.md).

## Validation and Benchmarks

`scripts/validate/validate_indicators.py` compares Scrooge outputs
against pure-Python reference implementations:

```bash
python3 scripts/validate/validate_indicators.py
```

`scripts/benchmarks/run_benchmarks.py` runs throughput micro-benchmarks
on synthetic data — useful for tracking regression across commits:

```bash
python3 scripts/benchmarks/run_benchmarks.py --rows 1000000 --runs 3
```

## Code style

`.clang-format` defines the canonical style (LLVM with 2-space indent
and 120-column lines). The Quality CI workflow runs `clang-format` in
advisory mode. To autoformat locally:

```bash
find src -name '*.cpp' -o -name '*.hpp' | xargs clang-format -i
```

## Roadmap

See [EXPANSION_PLAN.md](EXPANSION_PLAN.md) and [Discussions](https://github.com/pdet/Scrooge-McDuck/discussions/22).

## License

MIT
