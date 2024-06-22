# Scrooge McDuck Extension

Scrooge McDuck is a third-party financial extension for [DuckDB](https://www.duckdb.org). The main goal of this extension is to support a set of aggregation functions and data scanners for financial data. It currently supports access to the logs of Ethereum nodes and stock information from Yahoo Finance. 

This extension is still under development, with no official version released yet.

You can find more details on the [supported scanners](https://github.com/pdet/Scrooge-McDuck/wiki/Data-Scanners), [custom functions](https://github.com/pdet/Scrooge-McDuck/wiki/Custom-Functions), and [usage](https://github.com/pdet/Scrooge-McDuck/wiki/Usage) in the Scrooge wiki.

> **Disclaimer:**  This extension is in no way affiliated with the [DuckDB Foundation](https://duckdb.org/foundation/) or [DuckDB Labs](https://duckdblabs.com/). Therefore, any binaries produced and distributed of this extension are unsigned.

## Roadmap
A roadmap for the next version of Scrooge is currently maintained as a discussion. You can find it [here](https://github.com/pdet/Scrooge-McDuck/discussions/22).

## Why Scrooge?
1. DuckDB is an easy-to-use, fast system for analytics. Scrooge takes advantage of all the design decisions of DuckDB that make it a highly performant database system for analytics (e.g., columnar storage, vectorized execution).
2. Privacy/Security. Since DuckDB runs locally, all your queries are completely private and fully executed on your machine.
3. Cost-Efficiency. Both DuckDB and Scrooge are completely free and available under an MIT License. There are no costs associated with using them, unlike with a cloud-based engine; all you need is your own machine.
4. [Subquery Flattening](https://duckdb.org/2023/05/26/correlated-subqueries-in-sql.html). This is a DuckDB optimization that few systems implement. Financial queries (e.g., ROIs) can get extremely complex and will be efficiently executed by DuckDB.

## Build
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
## Blogposts
- [Candle-Stick Aggregation](https://pdet-blog.github.io/2022/08/16/scrooge.html)
- [Yahoo Scanner](https://pdet-blog.github.io/2023/02/25/yahoofinance.html)

