# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(scrooge
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Build json extension for Yahoo scanner JSON parsing
duckdb_extension_load(json)
# Note: httpfs is needed at runtime for Yahoo scanner HTTP URLs
# but is loaded dynamically, not compiled into the extension
