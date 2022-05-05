# Scrooge McDuck
Scrooge McDuck is a financial extension to [DuckDB](https://www.duckdb.org).
The main goal of this extension is to support a set of aggregation functions and data scanners frequently used on financial data.



To build, type 
```
make
```

To run, run the bundled `duckdb` shell:
```
 ./duckdb/build/release/duckdb 
```

Then, load the Scrooge McDuck extension like so:
```SQL
LOAD 'build/release/scrooge.duckdb_extension';
```