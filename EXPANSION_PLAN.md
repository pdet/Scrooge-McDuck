# Scrooge McDuck — Expansion Plan

> Comprehensive roadmap for transforming Scrooge McDuck into the definitive DuckDB financial analysis toolkit.

## Table of Contents

1. [Current State](#current-state)
2. [Architecture & Code Organization](#architecture--code-organization)
3. [Phase 1: Core Technical Indicators](#phase-1-core-technical-indicators)
4. [Phase 2: Risk & Return Metrics](#phase-2-risk--return-metrics)
5. [Phase 3: Advanced Scanners & Portfolio Optimization](#phase-3-advanced-scanners--portfolio-optimization)
6. [Phase 4: Candlestick Patterns & Sentiment](#phase-4-candlestick-patterns--sentiment)
7. [Testing Strategy](#testing-strategy)
8. [CI/CD & Infrastructure](#cicd--infrastructure)
9. [Appendix: Full Function Reference](#appendix-full-function-reference)

---

## Current State

### Functions
| Name | Type | Description |
|------|------|-------------|
| `first_s(value, timestamp)` | Aggregate | First value ordered by timestamp |
| `last_s(value, timestamp)` | Aggregate | Last value ordered by timestamp |
| `timebucket(timestamp, interval)` | Scalar | Bucket timestamps by arbitrary interval |
| `volatility(value)` | Aggregate (alias) | Alias for `stddev_pop` |
| `sma(value)` | Aggregate (alias) | Alias for `avg` |
| `portfolio_frontier(...)` | Table function | Markowitz efficient frontier (commented out, depends on Yahoo scanner) |

### Scanners
| Name | Description |
|------|-------------|
| `yahoo_finance(symbol, from, to, interval)` | Stock/crypto OHLCV data from Yahoo Finance |
| `read_eth(address, topic, from_block, to_block)` | Ethereum blockchain event logs via JSON-RPC |

### Codebase Metrics
- ~1,500 lines of C++ (excluding auto-generated maps and `json.hpp`)
- 9 SQL test files (DuckDB sqllogictest format)
- Dependencies: OpenSSL, nlohmann/json (vendored), DuckDB httplib
- CI: DuckDB extension-ci-tools (`_extension_distribution.yml`), builds against DuckDB v1.2.2

### Roadmap v0.1 Items (from Discussion #22)
- Replace CURL with DuckDB HTTPFS ✅ (done — uses `httplib`)
- 256-bit integer support for Ether values
- Hex BLOB type with numeric casts
- ETH contract aliasing (token name → address maps) ✅ (done)
- Full `eth_getLogs` support: topics, cardinality estimation, progress bar ✅ (partial), filter pushdown, projection pushdown ✅

---

## Architecture & Code Organization

### Current Structure
```
src/
├── scrooge_extension.cpp         # Extension entry point, function registration
├── functions/
│   ├── aliases.cpp               # volatility, sma (alias registration)
│   ├── first.cpp                 # first_s aggregate
│   ├── last.cpp                  # last_s aggregate
│   ├── portfolio_frontier.cpp    # Markowitz frontier (commented out)
│   └── timebucket.cpp            # timebucket scalar
├── include/
│   ├── functions/
│   │   ├── functions.hpp         # Function struct declarations
│   │   └── scanner.hpp           # Scanner struct declarations
│   ├── scrooge_extension.hpp     # Extension class
│   └── util/
│       ├── eth_maps.hpp          # Event signature maps
│       ├── eth_tokens_map.hpp    # Token name → address (auto-generated)
│       ├── eth_uniswap_map.hpp   # Uniswap pool maps (auto-generated)
│       ├── hex_converter.hpp     # Hex ↔ numeric utilities
│       └── http_util.hpp         # HTTP POST helper
├── scanner/
│   ├── ethereum_blockchain.cpp   # read_eth table function
│   └── yahoo_finance.cpp         # yahoo_finance table function
└── test/sql/scrooge/             # sqllogictest files
```

### Proposed Structure (for 30+ functions)

Group functions into subdirectories by domain. Each domain gets its own header and registration function. The extension entry point calls a single `RegisterAllFunctions()` that dispatches to each domain.

```
src/
├── scrooge_extension.cpp
├── functions/
│   ├── registration.cpp           # RegisterAllFunctions() — single dispatch
│   ├── core/
│   │   ├── first.cpp
│   │   ├── last.cpp
│   │   ├── timebucket.cpp
│   │   └── aliases.cpp
│   ├── technical/                 # Technical indicators
│   │   ├── ema.cpp
│   │   ├── rsi.cpp
│   │   ├── macd.cpp
│   │   ├── bollinger.cpp
│   │   ├── vwap.cpp
│   │   ├── obv.cpp
│   │   ├── atr.cpp
│   │   └── stochastic.cpp
│   ├── returns/                   # Return calculations
│   │   ├── simple_returns.cpp
│   │   ├── log_returns.cpp
│   │   └── cumulative_returns.cpp
│   ├── risk/                      # Risk & portfolio metrics
│   │   ├── sharpe.cpp
│   │   ├── sortino.cpp
│   │   ├── max_drawdown.cpp
│   │   ├── cagr.cpp
│   │   ├── beta.cpp
│   │   ├── alpha.cpp
│   │   ├── var.cpp
│   │   ├── cvar.cpp
│   │   └── portfolio_frontier.cpp
│   ├── money_flow/                # Volume-based indicators
│   │   ├── mfi.cpp
│   │   ├── chaikin.cpp
│   │   └── accumulation_distribution.cpp
│   └── patterns/                  # Candlestick patterns
│       ├── doji.cpp
│       ├── hammer.cpp
│       ├── engulfing.cpp
│       └── pattern_scanner.cpp
├── scanner/
│   ├── yahoo_finance.cpp
│   ├── ethereum_blockchain.cpp
│   ├── alpha_vantage.cpp
│   ├── polygon_io.cpp
│   ├── fred.cpp
│   ├── coingecko.cpp
│   └── sec_edgar.cpp
├── include/
│   ├── functions/
│   │   ├── functions.hpp          # Core function declarations
│   │   ├── technical.hpp          # Technical indicator declarations
│   │   ├── returns.hpp            # Return function declarations
│   │   ├── risk.hpp               # Risk metric declarations
│   │   ├── money_flow.hpp         # Money flow declarations
│   │   ├── patterns.hpp           # Pattern declarations
│   │   └── scanner.hpp            # Scanner declarations
│   ├── scrooge_extension.hpp
│   └── util/
│       ├── eth_maps.hpp
│       ├── eth_tokens_map.hpp
│       ├── eth_uniswap_map.hpp
│       ├── hex_converter.hpp
│       ├── http_util.hpp
│       └── window_state.hpp       # Shared state for ordered window aggregates
└── test/sql/scrooge/
    ├── core/
    ├── technical/
    ├── returns/
    ├── risk/
    ├── money_flow/
    ├── patterns/
    └── scanners/
```

### Registration Pattern

Each domain exposes a single `RegisterXXXFunctions(Connection &conn, Catalog &catalog)`:

```cpp
// src/functions/registration.cpp
namespace duckdb { namespace scrooge {

void RegisterAllFunctions(Connection &conn, Catalog &catalog) {
    // Core (existing)
    FirstScrooge::RegisterFunction(conn, catalog);
    LastScrooge::RegisterFunction(conn, catalog);
    TimeBucketScrooge::RegisterFunction(conn, catalog);
    Aliases::Register(conn, catalog);

    // Phase 1: Technical Indicators
    RegisterTechnicalFunctions(conn, catalog);

    // Phase 2: Risk & Returns
    RegisterReturnFunctions(conn, catalog);
    RegisterRiskFunctions(conn, catalog);

    // Phase 3: Portfolio
    RegisterPortfolioFunctions(conn, catalog);

    // Phase 4: Patterns & Money Flow
    RegisterMoneyFlowFunctions(conn, catalog);
    RegisterPatternFunctions(conn, catalog);
}

}} // namespace
```

---

## Phase 1: Core Technical Indicators

**Priority: Highest — most requested by users, most immediately useful.**

All technical indicators operate on ordered time-series data. The key design question for each: **aggregate vs. scalar vs. window function?**

- **Aggregate**: computes a single value from a group (e.g., `SELECT ema(close, 20) FROM ...`)
- **Scalar**: row-level transform (e.g., `SELECT log_return(close) OVER (ORDER BY date)`)
- **Window-aware aggregate**: used with `OVER (ORDER BY ... ROWS BETWEEN ...)` clauses

Most technical indicators are best implemented as **aggregate functions** that users combine with DuckDB's window clauses for rolling calculations. This leverages DuckDB's existing window machinery and avoids reimplementing frame management.

### 1.1 EMA — Exponential Moving Average

**Type:** Aggregate function (window-compatible)

**Signature:**
```sql
ema(value DOUBLE, period INTEGER) → DOUBLE
```

**SQL Usage:**
```sql
-- 20-day EMA as a window function
SELECT date, close,
       ema(close, 20) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ema_20
FROM yahoo_finance('AAPL', '2024-01-01', '2024-06-01', '1d');
```

**Implementation:**

EMA requires ordered state — the smoothing factor `α = 2/(period+1)` and the running EMA value. Implemented as an aggregate with state:

```cpp
struct EMAState {
    double ema_value;
    double alpha;
    int64_t count;
    bool initialized;
};
```

The `Operation` method:
- If `count == 0`: `ema_value = input` (seed with first value)
- Otherwise: `ema_value = alpha * input + (1 - alpha) * ema_value`

**Note:** EMA is inherently order-dependent. When used as a plain aggregate (no window), it relies on ingestion order. When used with `OVER (ORDER BY ...)`, DuckDB feeds values in the correct order. Document that users should always use it with a window clause for correct results.

**Alternative approach:** Implement as a scalar function `ema(value, period) OVER (ORDER BY date)` using DuckDB's custom window function API if available. This would guarantee order-awareness. Investigate `WindowAggregateFunction` in DuckDB internals.

### 1.2 RSI — Relative Strength Index

**Type:** Aggregate function (window-compatible)

**Signature:**
```sql
rsi(value DOUBLE, period INTEGER) → DOUBLE
```

**SQL Usage:**
```sql
SELECT date, close,
       rsi(close, 14) OVER (ORDER BY date ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS rsi_14
FROM yahoo_finance('AAPL', '2024-01-01', '2024-06-01', '1d');
```

**Implementation:**

RSI tracks average gain and average loss over the period. Uses Wilder's smoothing (exponential):

```cpp
struct RSIState {
    double avg_gain;
    double avg_loss;
    double prev_value;
    int64_t count;
    int32_t period;
    bool initialized;
};
```

Formula: `RSI = 100 - (100 / (1 + avg_gain/avg_loss))`

First `period` values use simple average; subsequent values use:
- `avg_gain = (prev_avg_gain * (period-1) + current_gain) / period`
- `avg_loss = (prev_avg_loss * (period-1) + current_loss) / period`

### 1.3 MACD — Moving Average Convergence Divergence

**Type:** Table-valued macro or aggregate returning STRUCT

MACD produces three values (MACD line, signal line, histogram), making it a natural fit for a function returning a `STRUCT`.

**Signature:**
```sql
macd(value DOUBLE, fast INTEGER DEFAULT 12, slow INTEGER DEFAULT 26, signal INTEGER DEFAULT 9)
  → STRUCT(macd DOUBLE, signal DOUBLE, histogram DOUBLE)
```

**SQL Usage:**
```sql
SELECT date, close,
       (macd(close, 12, 26, 9) OVER (ORDER BY date)).macd AS macd_line,
       (macd(close, 12, 26, 9) OVER (ORDER BY date)).signal AS signal_line,
       (macd(close, 12, 26, 9) OVER (ORDER BY date)).histogram AS histogram
FROM yahoo_finance('AAPL', '2024-01-01', '2024-06-01', '1d');
```

**Implementation:**

Internally maintains three EMA states: fast EMA, slow EMA, and signal EMA:

```cpp
struct MACDState {
    EMAState fast_ema;    // default period 12
    EMAState slow_ema;    // default period 26
    EMAState signal_ema;  // default period 9
    int64_t count;
};
```

MACD line = fast_EMA - slow_EMA. Signal = EMA of MACD line. Histogram = MACD - Signal.

### 1.4 Bollinger Bands

**Type:** Aggregate returning STRUCT

**Signature:**
```sql
bollinger(value DOUBLE, period INTEGER DEFAULT 20, num_std DOUBLE DEFAULT 2.0)
  → STRUCT(upper DOUBLE, middle DOUBLE, lower DOUBLE, bandwidth DOUBLE, pct_b DOUBLE)
```

**SQL Usage:**
```sql
SELECT date, close,
       (bollinger(close, 20, 2.0) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)).*
FROM yahoo_finance('AAPL', '2024-01-01', '2024-06-01', '1d');
```

**Implementation:**

Middle band = SMA(period). Upper = SMA + num_std * stddev. Lower = SMA - num_std * stddev.

```cpp
struct BollingerState {
    double sum;
    double sum_sq;
    int64_t count;
    int32_t period;
    double num_std;
};
```

Bandwidth = (upper - lower) / middle. %B = (close - lower) / (upper - lower).

### 1.5 VWAP — Volume Weighted Average Price

**Type:** Aggregate

**Signature:**
```sql
vwap(price DOUBLE, volume DOUBLE) → DOUBLE
```

**SQL Usage:**
```sql
-- Daily VWAP
SELECT date,
       vwap(close, volume) OVER (PARTITION BY date ORDER BY time) AS vwap
FROM intraday_data;

-- Cumulative VWAP over session
SELECT date,
       vwap(close, volume) OVER (ORDER BY time) AS cum_vwap
FROM yahoo_finance('AAPL', '2024-01-01', '2024-06-01', '1d');
```

**Implementation:**

Simple: `VWAP = Σ(price * volume) / Σ(volume)`. Binary aggregate like `first_s`/`last_s`.

```cpp
struct VWAPState {
    double sum_pv;    // Σ(price * volume)
    double sum_vol;   // Σ(volume)
};
```

### 1.6 OBV — On Balance Volume

**Type:** Aggregate (window-compatible, cumulative)

**Signature:**
```sql
obv(close DOUBLE, volume DOUBLE) → BIGINT
```

**SQL Usage:**
```sql
SELECT date,
       obv(close, volume) OVER (ORDER BY date) AS obv
FROM yahoo_finance('AAPL', '2024-01-01', '2024-06-01', '1d');
```

**Implementation:**

Cumulative volume where sign depends on price direction:
- If close > prev_close: OBV += volume
- If close < prev_close: OBV -= volume
- If close == prev_close: OBV unchanged

```cpp
struct OBVState {
    double prev_close;
    int64_t obv;
    bool initialized;
};
```

### 1.7 ATR — Average True Range

**Type:** Aggregate (typically used with window)

**Signature:**
```sql
atr(high DOUBLE, low DOUBLE, close DOUBLE, period INTEGER DEFAULT 14) → DOUBLE
```

**SQL Usage:**
```sql
SELECT date,
       atr(high, low, close, 14) OVER (ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS atr_14
FROM yahoo_finance('AAPL', '2024-01-01', '2024-06-01', '1d');
```

**Implementation:**

True Range = max(high - low, |high - prev_close|, |low - prev_close|). ATR = EMA/SMA of True Range over `period`.

This is a **ternary+ aggregate** (takes 3 value columns + period parameter). Use bind function to capture `period`.

```cpp
struct ATRState {
    double prev_close;
    double atr_value;
    int64_t count;
    int32_t period;
    bool initialized;
};
```

### 1.8 Stochastic Oscillator

**Type:** Aggregate returning STRUCT

**Signature:**
```sql
stochastic(high DOUBLE, low DOUBLE, close DOUBLE, k_period INTEGER DEFAULT 14, d_period INTEGER DEFAULT 3)
  → STRUCT(k DOUBLE, d DOUBLE)
```

**SQL Usage:**
```sql
SELECT date,
       (stochastic(high, low, close, 14, 3) OVER (ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)).*
FROM yahoo_finance('AAPL', '2024-01-01', '2024-06-01', '1d');
```

**Implementation:**

%K = 100 * (close - lowest_low) / (highest_high - lowest_low) over k_period.
%D = SMA of %K over d_period.

Needs to track min/max over window — a ring buffer or rely on DuckDB's window frame.

---

## Phase 2: Risk & Return Metrics

### 2.1 Returns

#### Simple Returns

**Type:** Scalar (window function)

**Signature:**
```sql
simple_return(value DOUBLE) → DOUBLE
```

**SQL Usage:**
```sql
SELECT date, close,
       simple_return(close) OVER (ORDER BY date) AS daily_return
FROM yahoo_finance('AAPL', '2024-01-01', '2024-06-01', '1d');
```

**Implementation:** `(current - previous) / previous`. Scalar operating on window frame. First row returns NULL.

#### Log Returns

**Type:** Scalar (window function)

**Signature:**
```sql
log_return(value DOUBLE) → DOUBLE
```

**SQL Usage:**
```sql
SELECT date,
       log_return(close) OVER (ORDER BY date) AS log_ret
FROM yahoo_finance('AAPL', '2024-01-01', '2024-06-01', '1d');
```

**Implementation:** `ln(current / previous)`. Advantages: log returns are additive across time.

#### Cumulative Returns

**Type:** Aggregate (window-compatible)

**Signature:**
```sql
cumulative_return(value DOUBLE) → DOUBLE
```

**SQL Usage:**
```sql
SELECT date,
       cumulative_return(close) OVER (ORDER BY date) AS cum_ret
FROM yahoo_finance('AAPL', '2024-01-01', '2024-06-01', '1d');
```

**Implementation:** `(current / first_value) - 1`. Tracks the initial value and computes running return.

### 2.2 Sharpe Ratio

**Type:** Aggregate

**Signature:**
```sql
sharpe_ratio(returns DOUBLE, risk_free_rate DOUBLE DEFAULT 0.0) → DOUBLE
```

**SQL Usage:**
```sql
-- Annual Sharpe from daily returns
SELECT sharpe_ratio(daily_return, 0.0001) * sqrt(252) AS annual_sharpe
FROM (
    SELECT simple_return(close) OVER (ORDER BY date) AS daily_return
    FROM yahoo_finance('AAPL', '2024-01-01', '2024-12-31', '1d')
);

-- Rolling 30-day Sharpe
SELECT date,
       sharpe_ratio(daily_return) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) * sqrt(252) AS rolling_sharpe
FROM returns_table;
```

**Implementation:** `(mean(returns) - risk_free_rate) / stddev(returns)`. State tracks sum, sum_sq, count.

### 2.3 Sortino Ratio

**Type:** Aggregate

**Signature:**
```sql
sortino_ratio(returns DOUBLE, risk_free_rate DOUBLE DEFAULT 0.0) → DOUBLE
```

**Implementation:** Like Sharpe but only uses downside deviation (returns < target). State tracks sum, downside_sum_sq, count, downside_count.

### 2.4 Max Drawdown

**Type:** Aggregate

**Signature:**
```sql
max_drawdown(value DOUBLE) → DOUBLE
```

**SQL Usage:**
```sql
SELECT max_drawdown(close) OVER (ORDER BY date) AS running_max_dd
FROM yahoo_finance('AAPL', '2024-01-01', '2024-06-01', '1d');

-- Single value for full period
SELECT max_drawdown(close)
FROM yahoo_finance('AAPL', '2024-01-01', '2024-06-01', '1d');
```

**Implementation:**

```cpp
struct MaxDrawdownState {
    double peak;
    double max_drawdown;  // stored as negative fraction
    bool initialized;
};
```

Track running peak. At each value: `drawdown = (value - peak) / peak`. `max_drawdown = min(max_drawdown, drawdown)`.

### 2.5 CAGR — Compound Annual Growth Rate

**Type:** Aggregate

**Signature:**
```sql
cagr(value DOUBLE, date TIMESTAMP) → DOUBLE
```

**SQL Usage:**
```sql
SELECT cagr(close, date)
FROM yahoo_finance('AAPL', '2020-01-01', '2024-01-01', '1d');
```

**Implementation:** `(end_value / start_value)^(365.25 / days) - 1`. Binary aggregate tracking first value, last value, first date, last date (reuse first_s/last_s logic).

### 2.6 Beta

**Type:** Aggregate

**Signature:**
```sql
beta(asset_returns DOUBLE, market_returns DOUBLE) → DOUBLE
```

**SQL Usage:**
```sql
-- Compute beta of AAPL vs S&P500
WITH asset AS (
    SELECT date, simple_return(close) OVER (ORDER BY date) AS ret
    FROM yahoo_finance('AAPL', '2024-01-01', '2024-06-01', '1d')
),
market AS (
    SELECT date, simple_return(close) OVER (ORDER BY date) AS ret
    FROM yahoo_finance('^GSPC', '2024-01-01', '2024-06-01', '1d')
)
SELECT beta(a.ret, m.ret)
FROM asset a JOIN market m ON a.date = m.date;
```

**Implementation:** `covariance(asset, market) / variance(market)`. Binary aggregate tracking sums for covariance computation.

### 2.7 Alpha (Jensen's Alpha)

**Type:** Aggregate

**Signature:**
```sql
alpha(asset_returns DOUBLE, market_returns DOUBLE, risk_free_rate DOUBLE DEFAULT 0.0) → DOUBLE
```

**Implementation:** `mean(asset) - (risk_free + beta * (mean(market) - risk_free))`. Can share state with beta computation.

### 2.8 VaR — Value at Risk

**Type:** Aggregate

**Signature:**
```sql
-- Parametric (Gaussian) VaR
var_parametric(returns DOUBLE, confidence DOUBLE DEFAULT 0.95) → DOUBLE

-- Historical VaR
var_historical(returns DOUBLE, confidence DOUBLE DEFAULT 0.95) → DOUBLE
```

**SQL Usage:**
```sql
-- 95% daily VaR
SELECT var_parametric(daily_return, 0.95) AS var_95
FROM returns_table;
```

**Implementation:**
- Parametric: `mean - z_score * stddev` where z_score corresponds to confidence level.
- Historical: sort all returns, take the percentile value. Requires collecting all values — use a `vector<double>` in state, sort in Finalize.

### 2.9 CVaR — Conditional Value at Risk

**Type:** Aggregate

**Signature:**
```sql
cvar(returns DOUBLE, confidence DOUBLE DEFAULT 0.95) → DOUBLE
```

**Implementation:** Mean of all returns below the VaR threshold. Requires collecting returns (like historical VaR), then averaging the tail.

---

## Phase 3: Advanced Scanners & Portfolio Optimization

### 3.1 New Scanners

#### Alpha Vantage Scanner

```sql
alpha_vantage(symbol VARCHAR, function VARCHAR, api_key VARCHAR, interval VARCHAR DEFAULT '5min')
```

**Returns:** OHLCV data + additional indicators depending on `function` parameter.

**API Functions to Support:**
- `TIME_SERIES_DAILY`, `TIME_SERIES_INTRADAY`
- `SMA`, `EMA`, `RSI`, `MACD` (server-side computation)
- `OVERVIEW` (company fundamentals)

**Implementation:** HTTP GET to `https://www.alphavantage.co/query?...`, parse JSON response. Store API key via DuckDB extension option (`SET alpha_vantage_api_key = '...'`).

#### Polygon.io Scanner

```sql
polygon_stocks(symbol VARCHAR, from_date DATE, to_date DATE, timespan VARCHAR DEFAULT 'day',
               api_key VARCHAR DEFAULT NULL)
polygon_options(symbol VARCHAR, expiration DATE, api_key VARCHAR DEFAULT NULL)
```

**Implementation:** REST API, returns JSON. Supports aggregates, trades, quotes, options chains.

#### FRED Scanner — Federal Reserve Economic Data

```sql
fred(series_id VARCHAR, from_date DATE DEFAULT NULL, to_date DATE DEFAULT NULL, api_key VARCHAR DEFAULT NULL)
```

**SQL Usage:**
```sql
-- Get S&P 500 PE ratio from FRED
SELECT * FROM fred('SP500', '2024-01-01', '2024-06-01');

-- Federal funds rate
SELECT * FROM fred('FEDFUNDS', '2020-01-01', '2024-01-01');
```

**Returns:** `(date DATE, value DOUBLE, series_id VARCHAR)`

**Implementation:** HTTP GET to `https://api.stlouisfed.org/fred/series/observations?...`, JSON parsing.

#### CoinGecko Scanner

```sql
coingecko(coin_id VARCHAR, vs_currency VARCHAR DEFAULT 'usd', days INTEGER DEFAULT 30)
```

**SQL Usage:**
```sql
SELECT * FROM coingecko('bitcoin', 'usd', 365);
```

**Returns:** `(timestamp TIMESTAMP, price DOUBLE, market_cap DOUBLE, volume DOUBLE)`

**Implementation:** Free API (no key for basic tier), rate-limited.

#### SEC EDGAR Scanner

```sql
sec_filings(ticker VARCHAR, filing_type VARCHAR DEFAULT '10-K', limit INTEGER DEFAULT 10)
```

**Returns:** Filing metadata + links. Can parse XBRL for structured financial data.

**Implementation:** EDGAR FULL-TEXT search API (`https://efts.sec.gov/LATEST/search-index?...`). More complex — Phase 3+.

### 3.2 Portfolio Optimization (Reactivate & Extend)

**Fix the current `portfolio_frontier`:**
1. Decouple from Yahoo scanner — accept a table/subquery of returns instead
2. Use proper covariance matrix instead of independent volatilities
3. Add Markowitz mean-variance optimization (not just random sampling)

**New Signature:**
```sql
portfolio_frontier(
    returns_table TABLE,    -- Table with columns: symbol VARCHAR, date DATE, return DOUBLE
    num_portfolios INTEGER DEFAULT 1000,
    risk_free_rate DOUBLE DEFAULT 0.0
) → TABLE(portfolio LIST(STRUCT(symbol VARCHAR, weight DOUBLE)),
          expected_return DOUBLE, volatility DOUBLE, sharpe_ratio DOUBLE)
```

**Additional portfolio functions:**
```sql
-- Minimum variance portfolio
min_variance_portfolio(returns_table TABLE) → TABLE(symbol VARCHAR, weight DOUBLE)

-- Maximum Sharpe portfolio (tangency portfolio)
max_sharpe_portfolio(returns_table TABLE, risk_free_rate DOUBLE DEFAULT 0.0)
  → TABLE(symbol VARCHAR, weight DOUBLE, expected_return DOUBLE, volatility DOUBLE)

-- Portfolio variance given weights and covariance matrix
portfolio_variance(weights DOUBLE[], covariance_matrix DOUBLE[][]) → DOUBLE
```

---

## Phase 4: Candlestick Patterns & Sentiment

### 4.1 Candlestick Pattern Detection

Scrooge already supports OHLC aggregation via `first_s`/`last_s`/`min`/`max`. The next step is pattern detection on candle data.

**Type:** Scalar function on OHLC rows

**Signature:**
```sql
candle_pattern(open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
               prev_open DOUBLE, prev_high DOUBLE, prev_low DOUBLE, prev_close DOUBLE)
  → VARCHAR  -- pattern name or NULL
```

**Individual pattern detectors:**
```sql
is_doji(open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, threshold DOUBLE DEFAULT 0.001) → BOOLEAN
is_hammer(open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE) → BOOLEAN
is_engulfing(open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
             prev_open DOUBLE, prev_close DOUBLE) → VARCHAR  -- 'BULLISH'/'BEARISH'/NULL
is_morning_star(o1 DOUBLE, c1 DOUBLE, o2 DOUBLE, c2 DOUBLE, o3 DOUBLE, c3 DOUBLE) → BOOLEAN
is_evening_star(o1 DOUBLE, c1 DOUBLE, o2 DOUBLE, c2 DOUBLE, o3 DOUBLE, c3 DOUBLE) → BOOLEAN
is_shooting_star(open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE) → BOOLEAN
is_hanging_man(open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE) → BOOLEAN
is_three_white_soldiers(o1 DOUBLE, c1 DOUBLE, o2 DOUBLE, c2 DOUBLE, o3 DOUBLE, c3 DOUBLE) → BOOLEAN
is_three_black_crows(o1 DOUBLE, c1 DOUBLE, o2 DOUBLE, c2 DOUBLE, o3 DOUBLE, c3 DOUBLE) → BOOLEAN
```

**SQL Usage:**
```sql
-- Detect patterns using LAG window function for previous candle data
SELECT date, candle_pattern(
    open, high, low, close,
    LAG(open) OVER w, LAG(high) OVER w, LAG(low) OVER w, LAG(close) OVER w
) AS pattern
FROM candles
WINDOW w AS (ORDER BY date)
WHERE pattern IS NOT NULL;
```

**Implementation:** Pure scalar functions — no state needed. Each is a simple geometric test on the candle body/wick ratios.

### 4.2 Money Flow Indicators

#### MFI — Money Flow Index

**Type:** Aggregate (window-compatible)

**Signature:**
```sql
mfi(high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, period INTEGER DEFAULT 14) → DOUBLE
```

**Implementation:** Typical Price = (H+L+C)/3. Money Flow = TP * Volume. Positive/Negative flow tracking over period. MFI = 100 - 100/(1 + positive_flow/negative_flow).

#### Chaikin Money Flow

**Signature:**
```sql
chaikin_mf(high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, period INTEGER DEFAULT 20) → DOUBLE
```

**Implementation:** Money Flow Multiplier = ((C-L) - (H-C)) / (H-L). CMF = Σ(MFM * Volume) / Σ(Volume) over period.

#### Accumulation/Distribution Line

**Signature:**
```sql
ad_line(high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE) → DOUBLE
```

**Implementation:** Cumulative. AD = prev_AD + ((C-L) - (H-C))/(H-L) * Volume. Used as window aggregate.

### 4.3 News Sentiment Scanner (Exploratory)

```sql
news_sentiment(symbol VARCHAR, from_date DATE, to_date DATE, provider VARCHAR DEFAULT 'alphavantage')
```

**Returns:** `(date DATE, title VARCHAR, url VARCHAR, sentiment DOUBLE, relevance DOUBLE)`

**Implementation:** Alpha Vantage NEWS_SENTIMENT API or similar. Lower priority — API quality varies.

---

## Testing Strategy

### Test File Structure

Follow the existing DuckDB sqllogictest format. Each function gets its own test file:

```
test/sql/scrooge/
├── core/
│   ├── test_first.test          # (existing, move here)
│   ├── test_last.test           # (existing, move here)
│   └── test_timebucket.test     # (existing, move here)
├── technical/
│   ├── test_ema.test
│   ├── test_rsi.test
│   ├── test_macd.test
│   ├── test_bollinger.test
│   ├── test_vwap.test
│   ├── test_obv.test
│   ├── test_atr.test
│   └── test_stochastic.test
├── returns/
│   ├── test_simple_return.test
│   ├── test_log_return.test
│   └── test_cumulative_return.test
├── risk/
│   ├── test_sharpe.test
│   ├── test_sortino.test
│   ├── test_max_drawdown.test
│   ├── test_beta.test
│   ├── test_var.test
│   └── test_cvar.test
├── money_flow/
│   ├── test_mfi.test
│   ├── test_chaikin.test
│   └── test_ad_line.test
├── patterns/
│   ├── test_doji.test
│   ├── test_hammer.test
│   └── test_engulfing.test
└── scanners/
    ├── test_yahoo.test          # (existing, move here)
    ├── test_eth_scan.test       # (existing, move here)
    └── ...
```

### Test Pattern

Each test file should cover:
1. **Corner cases:** NULL inputs, empty tables, single-row tables
2. **Known values:** Hand-computed expected results for small datasets
3. **Type variants:** Test with different numeric types where applicable (using `foreach`)
4. **Window usage:** Test as both plain aggregate and window function
5. **Grouped usage:** Test with `GROUP BY`

**Example test (EMA):**

```sql
# name: test/sql/scrooge/technical/test_ema.test
# description: Test ema function
# group: [scrooge]

require scrooge

# Corner cases
statement error
select ema()
----

query I
select ema(NULL, 10)
----
NULL

# Known values - 3-period EMA
# alpha = 2/(3+1) = 0.5
# day1: ema = 10.0
# day2: ema = 0.5*11 + 0.5*10 = 10.5
# day3: ema = 0.5*12 + 0.5*10.5 = 11.25

statement ok
CREATE TABLE prices (dt DATE, price DOUBLE);

statement ok
INSERT INTO prices VALUES
    ('2024-01-01', 10.0),
    ('2024-01-02', 11.0),
    ('2024-01-03', 12.0),
    ('2024-01-04', 11.0),
    ('2024-01-05', 13.0);

query R
SELECT round(ema(price, 3) OVER (ORDER BY dt), 4)
FROM prices;
----
10.0
10.5
11.25
11.125
12.0625
```

### Validation Against Reference Implementations

For each indicator, validate against:
- **pandas-ta** (Python): widely used, well-tested
- **TA-Lib**: industry standard C library
- **TradingView**: manual spot checks

Create a `scripts/validate/` directory with Python scripts that compute expected values and generate `.test` files.

---

## CI/CD & Infrastructure

### Current CI

Already using `duckdb/extension-ci-tools` via `MainDistributionPipeline.yml`:
- Builds on all platforms (excluding `windows_amd64_rtools`)
- Deploys on tag/main push
- Uses DuckDB v1.2.2

### Improvements

1. **Add test step to CI:** The existing `extension_config.cmake` has `LOAD_TESTS` — ensure the test runner picks up the new test directory structure.

2. **Add linting:**
   ```yaml
   - name: clang-format check
     run: |
       find src -name '*.cpp' -o -name '*.hpp' | xargs clang-format --dry-run --Werror
   ```

3. **API integration tests:** Scanner tests that require API keys should be gated behind a `require scrooge_network` flag or only run in CI with secrets:
   ```sql
   # name: test/sql/scrooge/scanners/test_yahoo.test
   # description: Test yahoo scanner (requires network)
   # group: [scrooge]

   require scrooge

   require no_ci  # Skip in CI if no network
   ```

4. **Nightly builds:** Track DuckDB nightly to catch breaking changes early. Add a second workflow that builds against `duckdb/duckdb@main`.

5. **Benchmarks:** Add a `benchmarks/` directory with SQL scripts that time critical paths (e.g., EMA over 1M rows). Run in CI and track regression.

### CMakeLists.txt Changes

Update to include new source files. Use `file(GLOB_RECURSE ...)` or maintain explicit list (explicit is better for reproducibility):

```cmake
set(EXTENSION_SOURCES
    src/scrooge_extension.cpp
    src/functions/registration.cpp
    # Core
    src/functions/core/first.cpp
    src/functions/core/last.cpp
    src/functions/core/timebucket.cpp
    src/functions/core/aliases.cpp
    # Technical
    src/functions/technical/ema.cpp
    src/functions/technical/rsi.cpp
    src/functions/technical/macd.cpp
    src/functions/technical/bollinger.cpp
    src/functions/technical/vwap.cpp
    src/functions/technical/obv.cpp
    src/functions/technical/atr.cpp
    src/functions/technical/stochastic.cpp
    # Returns
    src/functions/returns/simple_returns.cpp
    src/functions/returns/log_returns.cpp
    src/functions/returns/cumulative_returns.cpp
    # Risk
    src/functions/risk/sharpe.cpp
    src/functions/risk/sortino.cpp
    src/functions/risk/max_drawdown.cpp
    src/functions/risk/cagr.cpp
    src/functions/risk/beta.cpp
    src/functions/risk/alpha.cpp
    src/functions/risk/var.cpp
    src/functions/risk/cvar.cpp
    src/functions/risk/portfolio_frontier.cpp
    # Money Flow
    src/functions/money_flow/mfi.cpp
    src/functions/money_flow/chaikin.cpp
    src/functions/money_flow/accumulation_distribution.cpp
    # Patterns
    src/functions/patterns/doji.cpp
    src/functions/patterns/hammer.cpp
    src/functions/patterns/engulfing.cpp
    src/functions/patterns/pattern_scanner.cpp
    # Scanners
    src/scanner/yahoo_finance.cpp
    src/scanner/ethereum_blockchain.cpp
    src/scanner/alpha_vantage.cpp
    src/scanner/polygon_io.cpp
    src/scanner/fred.cpp
    src/scanner/coingecko.cpp
    src/scanner/sec_edgar.cpp)
```

---

## Appendix: Full Function Reference

### Phase 1 — Technical Indicators

| Function | Type | Signature | DuckDB Usage Pattern |
|----------|------|-----------|---------------------|
| `ema` | Aggregate | `ema(value, period) → DOUBLE` | Window: `OVER (ORDER BY date)` |
| `rsi` | Aggregate | `rsi(value, period) → DOUBLE` | Window: `OVER (ORDER BY date)` |
| `macd` | Aggregate | `macd(value, fast, slow, signal) → STRUCT` | Window: `OVER (ORDER BY date)` |
| `bollinger` | Aggregate | `bollinger(value, period, num_std) → STRUCT` | Window: `OVER (ORDER BY date ROWS ...)` |
| `vwap` | Aggregate | `vwap(price, volume) → DOUBLE` | Window or GROUP BY |
| `obv` | Aggregate | `obv(close, volume) → BIGINT` | Window: cumulative |
| `atr` | Aggregate | `atr(high, low, close, period) → DOUBLE` | Window: `OVER (ORDER BY date ROWS ...)` |
| `stochastic` | Aggregate | `stochastic(high, low, close, k, d) → STRUCT` | Window: `OVER (ORDER BY date ROWS ...)` |

### Phase 2 — Risk & Returns

| Function | Type | Signature |
|----------|------|-----------|
| `simple_return` | Scalar/Window | `simple_return(value) → DOUBLE` |
| `log_return` | Scalar/Window | `log_return(value) → DOUBLE` |
| `cumulative_return` | Aggregate | `cumulative_return(value) → DOUBLE` |
| `sharpe_ratio` | Aggregate | `sharpe_ratio(returns, rfr) → DOUBLE` |
| `sortino_ratio` | Aggregate | `sortino_ratio(returns, rfr) → DOUBLE` |
| `max_drawdown` | Aggregate | `max_drawdown(value) → DOUBLE` |
| `cagr` | Aggregate | `cagr(value, date) → DOUBLE` |
| `beta` | Aggregate | `beta(asset_ret, market_ret) → DOUBLE` |
| `alpha` | Aggregate | `alpha(asset_ret, market_ret, rfr) → DOUBLE` |
| `var_parametric` | Aggregate | `var_parametric(returns, conf) → DOUBLE` |
| `var_historical` | Aggregate | `var_historical(returns, conf) → DOUBLE` |
| `cvar` | Aggregate | `cvar(returns, conf) → DOUBLE` |

### Phase 3 — Scanners & Portfolio

| Function | Type | Description |
|----------|------|-------------|
| `alpha_vantage` | Table | Stock/crypto/fundamentals from Alpha Vantage |
| `polygon_stocks` | Table | Stock aggregates from Polygon.io |
| `polygon_options` | Table | Options chains from Polygon.io |
| `fred` | Table | Economic data from Federal Reserve |
| `coingecko` | Table | Crypto prices from CoinGecko |
| `sec_filings` | Table | SEC EDGAR filings |
| `portfolio_frontier` | Table | Markowitz efficient frontier (rewritten) |
| `min_variance_portfolio` | Table | Minimum variance allocation |
| `max_sharpe_portfolio` | Table | Maximum Sharpe ratio allocation |

### Phase 4 — Patterns & Money Flow

| Function | Type | Signature |
|----------|------|-----------|
| `is_doji` | Scalar | `is_doji(O, H, L, C, threshold) → BOOLEAN` |
| `is_hammer` | Scalar | `is_hammer(O, H, L, C) → BOOLEAN` |
| `is_engulfing` | Scalar | `is_engulfing(O, H, L, C, prev_O, prev_C) → VARCHAR` |
| `is_morning_star` | Scalar | `is_morning_star(o1,c1,o2,c2,o3,c3) → BOOLEAN` |
| `is_evening_star` | Scalar | `is_evening_star(o1,c1,o2,c2,o3,c3) → BOOLEAN` |
| `is_shooting_star` | Scalar | `is_shooting_star(O, H, L, C) → BOOLEAN` |
| `candle_pattern` | Scalar | `candle_pattern(O,H,L,C,pO,pH,pL,pC) → VARCHAR` |
| `mfi` | Aggregate | `mfi(H, L, C, volume, period) → DOUBLE` |
| `chaikin_mf` | Aggregate | `chaikin_mf(H, L, C, volume, period) → DOUBLE` |
| `ad_line` | Aggregate | `ad_line(H, L, C, volume) → DOUBLE` |

### Total New Functions: ~35

---

## Implementation Notes

### Order Dependence

Many financial functions (EMA, RSI, MACD, OBV, returns) are order-dependent. DuckDB's aggregate functions don't guarantee input order unless used with a window clause. Two approaches:

1. **Recommended:** Implement as aggregates and document that users must use `OVER (ORDER BY ...)` for correct results. This is the simplest approach and leverages DuckDB's battle-tested window machinery.

2. **Advanced:** Use DuckDB's `WindowAggregateFunction` API (if accessible from extensions) to enforce ordering. Needs investigation into the extension API surface.

### Bind Functions for Parameters

Functions like `ema(value, period)` where `period` is a constant: use a **bind function** to capture the period parameter and bake it into the aggregate state. This follows the pattern already used in `first.cpp` with `BindDoubleFirst`. The bind function reads the second argument as a constant and stores it in `FunctionData`.

### STRUCT Return Types

MACD, Bollinger, and Stochastic return multiple values. Use DuckDB's `LogicalType::STRUCT(...)` as the return type. Users destructure with `(func(...)).field_name` or `(func(...)).*`.

### Performance Considerations

- All Phase 1 and Phase 2 functions are O(n) single-pass — no performance concerns.
- Historical VaR/CVaR require collecting all values (O(n) memory + O(n log n) sort). Acceptable for financial datasets (typically <100k rows per group).
- Scanners are I/O bound — consider connection pooling for batch requests.

### DuckDB Version Compatibility

Currently pinned to v1.2.2. Technical indicators are pure computation — minimal API surface dependency. Scanners use `TableFunction` which is stable API. Should be forward-compatible.

---

*Generated: 2026-03-03. Target: Scrooge McDuck v0.2+.*
