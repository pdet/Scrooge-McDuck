# Scrooge McDuck 🦆💰

**The definitive financial analysis extension for [DuckDB](https://www.duckdb.org).**

35+ functions for technical analysis, risk management, portfolio analytics, and real-time market data — all in pure SQL.

```sql
INSTALL scrooge FROM community;
LOAD scrooge;

-- Get AAPL data and analyze
SELECT date, close,
    ema(close, date::TIMESTAMPTZ, 20) as ema_20,
    rsi(close, date::TIMESTAMPTZ, 14) as rsi_14,
    bollinger_signal(close, 
        bollinger_upper(close, date::TIMESTAMPTZ, 20, 2.0),
        bollinger_lower(close, date::TIMESTAMPTZ, 20, 2.0)) as signal
FROM yahoo_finance('AAPL', '2024-01-01', '2025-01-01', '1d')
GROUP BY ALL;
```

> **Disclaimer:** This extension is not affiliated with the [DuckDB Foundation](https://duckdb.org/foundation/) or [DuckDB Labs](https://duckdblabs.com/).

## Quick Start

```sql
-- Install and load
INSTALL scrooge FROM community;
LOAD scrooge;

-- Fetch stock data
SELECT * FROM yahoo_finance('MSFT', '2024-01-01', '2025-01-01', '1d');

-- Fetch economic data (requires free FRED API key)
SELECT * FROM fred_series('GDP', 'your_api_key');
```

## Functions

### 📈 Technical Indicators
| Function | Description |
|----------|-------------|
| `ema(close, timestamp, period)` | Exponential Moving Average |
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

### 💰 Returns & Risk
| Function | Description |
|----------|-------------|
| `simple_return(close, timestamp)` | Simple return |
| `log_return(close, timestamp)` | Logarithmic return |
| `cumulative_return(close, timestamp)` | Cumulative return |
| `sharpe_ratio(returns, timestamp)` | Sharpe ratio |
| `sortino_ratio(returns, timestamp)` | Sortino ratio |
| `max_drawdown(close, timestamp)` | Maximum drawdown |
| `value_at_risk(returns, timestamp, confidence)` | Value at Risk (VaR) |

### 🕯️ Candlestick Patterns
| Function | Returns |
|----------|---------|
| `is_doji(open, high, low, close)` | `BOOLEAN` |
| `is_hammer(open, high, low, close)` | `BOOLEAN` |
| `is_shooting_star(open, high, low, close)` | `BOOLEAN` |
| `is_engulfing(open, high, low, close, prev_open, prev_close)` | `'BULLISH'/'BEARISH'/NULL` |
| `is_morning_star(o1, c1, o2, h2, l2, c2, o3, c3)` | `BOOLEAN` |
| `is_evening_star(o1, c1, o2, h2, l2, c2, o3, c3)` | `BOOLEAN` |

### 📊 Portfolio Analytics
| Function | Description |
|----------|-------------|
| `portfolio_correlation(returns_a, returns_b)` | Pearson correlation between assets |
| `portfolio_beta(asset_returns, benchmark_returns)` | Systematic risk (beta) |
| `momentum_score(close, timestamp, period_days)` | Rate-of-change momentum |
| `relative_strength(asset_close, benchmark_close, ts)` | Performance vs benchmark |

### 🚦 Signal Generation
| Function | Description |
|----------|-------------|
| `ma_crossover_signal(fast_ma, slow_ma)` | `'BUY'/'SELL'/'HOLD'` |
| `rsi_signal(rsi [, overbought, oversold])` | `'BUY'/'SELL'/'HOLD'` |
| `bollinger_signal(close, upper, lower)` | `'BUY'/'SELL'/'HOLD'` |
| `kelly_fraction(win_rate, avg_win, avg_loss)` | Optimal position size [0,1] |
| `composite_score(rsi, macd, bb_pct, obv_pct)` | Multi-indicator conviction [-100,100] |

### 📡 Data Scanners
| Function | Description |
|----------|-------------|
| `yahoo_finance(symbol, from, to, interval)` | Yahoo Finance OHLCV data |
| `fred_series(series_id, api_key [, start, end])` | Federal Reserve economic data |
| `read_eth(url, method, from_block, to_block)` | Ethereum blockchain logs |

### ⏱️ Utility
| Function | Description |
|----------|-------------|
| `first_s(value, timestamp)` | First value by timestamp |
| `last_s(value, timestamp)` | Last value by timestamp |
| `timebucket(timestamp, interval)` | Time bucketing |

## Examples

### Full Analysis Pipeline
```sql
-- Combine technical indicators with signals
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

### Portfolio Diversification
```sql
-- Check if your positions are too correlated
SELECT 
    portfolio_correlation(aapl_ret, msft_ret) as aapl_msft_corr,
    portfolio_beta(aapl_ret, spy_ret) as aapl_beta
FROM returns_table;
```

### Macro + Stock Analysis
```sql
-- Compare stock performance vs Fed funds rate
SELECT p.date, p.close, f.value as fed_rate,
    momentum_score(p.close, p.date::TIMESTAMPTZ, 90) as momentum_3m
FROM yahoo_finance('SPY', '2023-01-01', '2025-01-01', '1d') p
JOIN fred_series('DFF', :api_key, '2023-01-01', '2025-01-01') f ON p.date = f.date
GROUP BY ALL;
```

## Build

```bash
git clone --recurse-submodules https://github.com/pdet/Scrooge-McDuck.git
cd Scrooge-McDuck
GEN=ninja make
```

## Why Scrooge?

1. **Pure SQL financial analysis** — no Python glue code needed
2. **Privacy** — DuckDB runs locally, your portfolio data stays private
3. **Free** — MIT licensed, no cloud costs
4. **Fast** — columnar storage + vectorized execution
5. **Composable** — combine indicators, signals, and data sources in a single query

## Roadmap

See [EXPANSION_PLAN.md](EXPANSION_PLAN.md) and [Discussions](https://github.com/pdet/Scrooge-McDuck/discussions/22).

## License

MIT
