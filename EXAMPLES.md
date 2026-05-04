# Scrooge McDuck — Cookbook

End-to-end recipes for common quant workflows. All examples are pure SQL
and self-contained: paste them into a DuckDB session with `LOAD scrooge`.

## Setup

```sql
INSTALL scrooge FROM community;
LOAD scrooge;
LOAD httpfs;            -- needed for HTTP-based scanners
LOAD json;              -- needed for JSON parsing in scanners

-- Optional: configure API keys once for the session
SET fred_api_key = 'YOUR_FRED_KEY';
SET polygon_api_key = 'YOUR_POLYGON_KEY';
SET sec_user_agent = 'Your Name your.email@example.com';
```

---

## 1. Momentum strategy with backtest

Buy when 50-day SMA crosses above 200-day SMA, sell on the inverse cross.
Run a full backtest with realistic costs.

```sql
WITH prices AS (
    SELECT * FROM yahoo_finance('SPY', '2018-01-01', '2025-01-01', '1d')
),
signals AS (
    SELECT date::TIMESTAMP AS ts, close AS price,
           ma_crossover_signal(
               avg(close) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),
               avg(close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
           ) AS signal
    FROM prices
)
SELECT * FROM backtest_stats(
    'FROM signals WHERE signal IS NOT NULL',
    100000.0,    -- starting capital
    0.0005,      -- 5 bps commission
    0.0001       -- 1 bp slippage
);
```

Use `backtest_equity` for the daily equity curve or `backtest_trades`
for the closed-trade log.

---

## 2. Pairs trading screener

Find cointegrated pairs by computing the ADF test statistic on the
spread of two series. Strongly negative ADF → mean-reverting spread.

```sql
WITH koi AS (
    SELECT date::TIMESTAMP AS ts, close AS price
    FROM yahoo_finance('KO', '2022-01-01', '2025-01-01', '1d')
),
pep AS (
    SELECT date::TIMESTAMP AS ts, close AS price
    FROM yahoo_finance('PEP', '2022-01-01', '2025-01-01', '1d')
),
spread AS (
    SELECT k.ts, ln(k.price) - ln(p.price) AS s
    FROM koi k JOIN pep p USING (ts)
)
SELECT
    portfolio_correlation(k.price, p.price) AS price_corr,
    adf_test_stat(s, ts) AS adf_stat,
    CASE
        WHEN adf_test_stat(s, ts) < -2.86 THEN 'cointegrated (95%)'
        WHEN adf_test_stat(s, ts) < -2.57 THEN 'borderline (90%)'
        ELSE 'no evidence'
    END AS verdict
FROM spread, koi k, pep p
WHERE k.ts = p.ts
GROUP BY ALL;
```

---

## 3. Efficient-frontier portfolio construction

Build a 5-asset efficient frontier from daily returns, then pick the
maximum-Sharpe portfolio.

```sql
WITH px AS (
    SELECT 'SPY' AS symbol, date::TIMESTAMP AS ts, close FROM yahoo_finance('SPY', '2022-01-01', '2025-01-01', '1d')
    UNION ALL SELECT 'TLT',  date::TIMESTAMP, close FROM yahoo_finance('TLT', '2022-01-01', '2025-01-01', '1d')
    UNION ALL SELECT 'GLD',  date::TIMESTAMP, close FROM yahoo_finance('GLD', '2022-01-01', '2025-01-01', '1d')
    UNION ALL SELECT 'QQQ',  date::TIMESTAMP, close FROM yahoo_finance('QQQ', '2022-01-01', '2025-01-01', '1d')
    UNION ALL SELECT 'EFA',  date::TIMESTAMP, close FROM yahoo_finance('EFA', '2022-01-01', '2025-01-01', '1d')
),
rets AS (
    SELECT symbol, ts,
           (close / lag(close) OVER (PARTITION BY symbol ORDER BY ts)) - 1.0 AS return
    FROM px
)
CREATE OR REPLACE TABLE returns_aligned AS
    SELECT symbol, return FROM rets WHERE return IS NOT NULL;

-- Print frontier (20 portfolios) plus the tangency portfolio.
SELECT * FROM efficient_frontier('FROM returns_aligned', 20, 0.0)
ORDER BY point_id, symbol;

SELECT * FROM max_sharpe_portfolio('FROM returns_aligned', 0.0);
```

---

## 4. Options-covered-call screener

Scan an option chain for high-yield covered calls on a long stock
position. Compute strike-relative Greeks via Black-Scholes.

```sql
-- Synthetic chain: 30-day calls 0–20% OTM in $1 increments
WITH chain AS (
    SELECT 150.0 AS spot,
           150.0 + i AS strike,
           30 / 365.0 AS T,
           0.05 AS r,
           0.30 AS sigma
    FROM range(0, 31) t(i)
)
SELECT strike,
       round(bs_call(spot, strike, T, r, sigma), 3) AS call_price,
       round(bs_delta_call(spot, strike, T, r, sigma), 3) AS delta,
       round(bs_call(spot, strike, T, r, sigma) / spot * (365.0 / 30), 3) AS annualized_yield
FROM chain
WHERE bs_delta_call(spot, strike, T, r, sigma) BETWEEN 0.20 AND 0.40
ORDER BY annualized_yield DESC;
```

---

## 5. Bond portfolio convexity / duration

Compute portfolio-weighted modified duration and convexity for a
synthetic ladder.

```sql
WITH bonds AS (
    SELECT 1000.0 AS face, 0.04 AS coupon, 0.045 AS ytm, 5  AS years, 2.0 AS freq
    UNION ALL SELECT 1000.0, 0.045, 0.046, 10, 2.0
    UNION ALL SELECT 1000.0, 0.05, 0.047, 20, 2.0
    UNION ALL SELECT 1000.0, 0.05, 0.048, 30, 2.0
)
SELECT
    sum(bond_price(face, coupon, ytm, years * freq::INT, freq)) AS portfolio_value,
    sum(bond_price(face, coupon, ytm, years * freq::INT, freq) *
        bond_modified_duration(face, coupon, ytm, years * freq::INT, freq))
        / sum(bond_price(face, coupon, ytm, years * freq::INT, freq)) AS weighted_mod_duration,
    sum(bond_price(face, coupon, ytm, years * freq::INT, freq) *
        bond_convexity(face, coupon, ytm, years * freq::INT, freq))
        / sum(bond_price(face, coupon, ytm, years * freq::INT, freq)) AS weighted_convexity
FROM bonds;
```

---

## 6. Monte Carlo retirement projection

Simulate 1,000 30-year wealth paths, then look at the 5th/50th/95th
percentile terminal balances.

```sql
WITH sim AS (
    SELECT path_id, day, price
    FROM monte_carlo(
        100000.0,   -- starting balance
        0.07,       -- 7% expected annual return
        0.15,       -- 15% annual vol
        252 * 30,   -- 30 years of trading days
        1000,       -- paths
        42          -- seed
    )
),
terminal AS (
    SELECT path_id, last(price ORDER BY day) AS final_balance
    FROM sim GROUP BY path_id
)
SELECT
    quantile_cont(final_balance, 0.05) AS pessimistic_p5,
    quantile_cont(final_balance, 0.50) AS median,
    quantile_cont(final_balance, 0.95) AS optimistic_p95
FROM terminal;
```

---

## 7. Fundamentals → DCF inputs from EDGAR

Pull historical revenues + net income for a ticker straight from SEC
EDGAR. (Set `sec_user_agent` first.)

```sql
SELECT
    period_end::DATE AS quarter_end,
    fiscal_year,
    fiscal_period,
    rev.value AS revenue,
    ni.value  AS net_income,
    ni.value / NULLIF(rev.value, 0) AS net_margin
FROM (SELECT * FROM sec_facts('AAPL', 'Revenues') WHERE unit = 'USD' AND form = '10-Q') rev
JOIN (SELECT * FROM sec_facts('AAPL', 'NetIncomeLoss') WHERE unit = 'USD' AND form = '10-Q') ni
  USING (period_end, fiscal_year, fiscal_period)
ORDER BY period_end DESC
LIMIT 20;
```

---

## 8. Macro overlay using FRED + market data

Plot S&P 500 returns alongside the Fed Funds rate to see how the index
behaved across rate regimes.

```sql
SET fred_api_key = 'YOUR_KEY';

WITH spy AS (
    SELECT date::DATE AS d, close
    FROM yahoo_finance('SPY', '2018-01-01', '2025-01-01', '1d')
),
fed AS (
    SELECT date AS d, value AS fed_funds_rate
    FROM fred_series('FEDFUNDS')
    WHERE date BETWEEN '2018-01-01' AND '2025-01-01'
)
SELECT s.d,
       s.close,
       f.fed_funds_rate,
       (s.close / lag(s.close, 252) OVER (ORDER BY s.d)) - 1 AS twelve_month_return
FROM spy s ASOF JOIN fed f ON s.d >= f.d
ORDER BY s.d;
```

---

## 9. Higher-timeframe resampling with macros

Use the `resample_ohlc` macro to convert hourly Binance bars to daily.

```sql
CREATE OR REPLACE TABLE btc_hourly AS
SELECT * FROM binance_klines('BTCUSDT', '1h', 1000);

CREATE OR REPLACE TABLE btc_daily AS
SELECT open_time::TIMESTAMP AS ts, open, high, low, close, volume
FROM btc_hourly;

SELECT * FROM resample_ohlc('btc_daily', '1 day') LIMIT 10;
```

---

## 10. Heikin-Ashi trend filter

Mark days where the Heikin-Ashi candle is bullish (`ha_close > ha_open`)
and the underlying price closed up.

```sql
WITH bars AS (
    SELECT date::TIMESTAMP AS ts, open, high, low, close
    FROM yahoo_finance('AAPL', '2024-01-01', '2025-01-01', '1d')
)
SELECT ts,
       close,
       ha_open(open, high, low, close, ts)  AS ha_o,
       ha_close(open, high, low, close, ts) AS ha_c,
       (ha_close(open, high, low, close, ts) > ha_open(open, high, low, close, ts))
           AS bullish
FROM bars
WINDOW w AS (ORDER BY ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
ORDER BY ts DESC
LIMIT 20;
```
