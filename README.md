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

### Options Pricing (Black-Scholes)

| Function | Description |
|----------|-------------|
| `bs_call(S, K, T, r, sigma)` | Call option price |
| `bs_put(S, K, T, r, sigma)` | Put option price |
| `bs_delta_call / bs_delta_put` | Delta |
| `bs_gamma` | Gamma |
| `bs_theta_call / bs_theta_put` | Theta (per day) |
| `bs_vega` | Vega (per 1% vol) |
| `bs_implied_vol(price, S, K, T, r, is_call)` | Implied volatility |

### Data Scanners

| Function | Description |
|----------|-------------|
| `yahoo_finance(symbol, from, to, interval)` | Yahoo Finance OHLCV data |
| `coingecko(coin_id, vs_currency, days)` | CoinGecko crypto OHLC data |
| `fred_series(series_id, api_key [, start, end])` | Federal Reserve economic data |
| `read_eth(url, method, from_block, to_block)` | Ethereum blockchain logs |

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

## Roadmap

See [EXPANSION_PLAN.md](EXPANSION_PLAN.md) and [Discussions](https://github.com/pdet/Scrooge-McDuck/discussions/22).

## License

MIT
