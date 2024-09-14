# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(scrooge
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Build the httpfs extension to test with s3/http
duckdb_extension_load(httpfs)
duckdb_extension_load(json)
