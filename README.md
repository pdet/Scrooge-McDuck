# Scrooge McDuck
Scrooge McDuck is a financial extension to [DuckDB](https://www.duckdb.org).
The main goal of this extension is to support a set of aggregation functions and data scanners frequently used on financial data.

**Disclaimer**: This extension is in no way affiliated with the [DuckDB Foundation](https://duckdb.org/foundation/) or [DuckDB Labs](https://duckdblabs.com/). Therefore, any binaries produced and distributed of this extension are unsigned.


To build, type 
``` sh
make
```

To run, run the `duckdb` shell with the unsined flag:
``` sh
cd build/release/
 ./duckdb -unsigned
```

Then, load the Scrooge McDuck extension like so:
```SQL
LOAD 'extension/scrooge/scrooge.duckdb_extension';
```

## Available Scanner

### yahoo_finance(symbol,start_period,end_period,interval)
Directly Scans financial data from Yahoo into a DuckDB Table.
Usage:
```sql
select * FROM yahoo_finance("^GSPC", "2017-12-01"::DATE, "2017-12-10"::DATE, "1d")
----
2017-12-01	2645.100098	2650.620117	2605.52002	2642.219971	2642.219971	3950930000
2017-12-04	2657.189941	2665.189941	2639.030029	2639.439941	2639.439941	4025840000
2017-12-05	2639.780029	2648.719971	2627.72998	2629.570068	2629.570068	3547570000
2017-12-06	2626.23999	2634.409912	2624.75	2629.27002	2629.27002	3253080000
2017-12-07	2628.379883	2640.98999	2626.530029	2636.97998	2636.97998	3297060000
2017-12-08	2646.209961	2651.649902	2644.100098	2651.5	2651.5	3126750000
```

## Available Functions

### FIRST_S(A::{NUMERIC_VALUE}, B::{TIMESTAMPTZ})
It returns the value of column A based omn the earliest timestamp value of column B.

### LAST_S(A::{NUMERIC_VALUE}, B::{TIMESTAMPTZ})
It returns the value of column A based on the latest timestamp value of column B

### TIMEBUCKET(A::{TIMESTAMPTZ}, B::{INTERVAL})
Creates timestamp buckets on column A, with ranges on the interval of value B.

### VOLATILITY(A::{DOUBLE})
Returns the volatility of that financial instrument during the given period of time.

### SMA(A::{DOUBLE})
Returns the average price, during a period of time, of a financial instrument

## Demo
To reproduce this demo, you must have:
1. A build of Scrooge McDuck.
2. The latest python libraries of DuckDB and Plotly ``` pip install duckdb plotly```
3. Download and unzip the example [dataset.](https://github.com/pdet/Scrooge-McDuck/raw/main/crypto_sample.zip)

To start, we need to load the libraries we will be using in this tutorial.
```python
import duckdb
import plotly.graph_objects as go
```

### Loading Scrooge McDuck
To load Scrooge McDuck, we simply have to execute the ```LOAD``` command together with the compiled path of the library.
```python
con = duckdb.connect()
con.execute("LOAD 'build/release/scrooge.duckdb_extension';")
```
### Creating Table
We only have one table in our dataset, the crypto_ticks, that holds the price (to USD) of a cryptocurrency at a given time.
```python
con.execute("""CREATE TABLE crypto_ticks (
               "time" TIMESTAMP,
               symbol TEXT,
               price DOUBLE PRECISION,
               day_volume NUMERIC
            );""")
```

### Loading Data
Our dataset is a CSV file; hence to load it we simply need a ```COPY ... FROM... ``` statement.
```python
con.execute("COPY crypto_ticks FROM 'tutorial_sample_tick.csv' CSV HEADER;")
```

### Generating Candle-Stick Aggregations
Here we generate our main aggregation using the domain-specific functions from Scrooge McDuck. To generate these aggregations, we must take, from within our time bucket, the open and close values of a cryptocurrency and their high and low values. In this case, we focus only on the week from 10-17 of April 22.
Since DuckDB has a tight integration with Pandas, to facilitate plotting we export our query result to a Pandas Dataframe with the ```.df()``` function.
```python
df = con.execute("""SELECT TIMEBUCKET(time,'1M'::INTERVAL) AS bucket,
                           FIRST_S(price, time) AS "open",
                           MAX(price) AS high,
                           MIN(price) AS low,
                           LAST_S(price, time) AS "close",
                    FROM crypto_ticks
                    WHERE symbol = 'BTC/USD'
                    AND time >= '2022-04-10'::TIMESTAMPTZ
                    AND time <= '2022-04-17'::TIMESTAMPTZ
                    GROUP BY bucket""").df()
```
### Plotting
To plot, we use the Candlestick aggregation of Plotly on our generated dataframe.
```python

fig = go.Figure(data=[go.Candlestick(x=df['bucket'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'])])
fig.show()

```
The result of our Candle-Stick aggregation can be seen below:
<img src="images/scrooge-plot.png"
     alt="Scrooge-Candle-Stick"
     width=1000
     />

