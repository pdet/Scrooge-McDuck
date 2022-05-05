import duckdb
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime

con = duckdb.connect()

con.execute("LOAD 'build/release/scrooge.duckdb_extension';")


con.execute("""CREATE TABLE crypto_assets (
			    symbol TEXT UNIQUE,
			    name TEXT
			);""")

con.execute("""CREATE TABLE crypto_ticks (
			    "time" TIMESTAMPTZ,
			    symbol TEXT,
			    price DOUBLE PRECISION,
			    day_volume NUMERIC
			);""")

con.execute("COPY crypto_assets FROM 'tutorial_sample_assets.csv' CSV HEADER;")

con.execute("COPY crypto_ticks FROM 'tutorial_sample_tick.csv' CSV HEADER;")

df = con.execute(""" SELECT
					    timebucket(time,'1M'::INTERVAL) AS bucket,
					    symbol,
					    FIRST_S(price, time) AS "open",
					    MAX(price) AS high,
					    MIN(price) AS low,
					    LAST_S(price, time) AS "close",
					    LAST_S(day_volume, time) AS day_volume
					FROM crypto_ticks
					WHERE symbol = 'BTC/USD'
					GROUP BY bucket, symbol """).df()

fig = go.Figure(data=[go.Candlestick(x=df['bucket'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'])])
fig.show()

