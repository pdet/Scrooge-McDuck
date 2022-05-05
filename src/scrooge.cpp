#define DUCKDB_BUILD_LOADABLE_EXTENSION

#include "aggregations/aggregate.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include <iostream>

namespace duckdb {
class ScroogeExtension : public Extension {
public:
  void Load(DuckDB &db) override;
  std::string Name() override;
};

void ScroogeExtension::Load(DuckDB &db) {
  Connection con(db);
  con.BeginTransaction();
  auto &catalog = Catalog::GetCatalog(*con.context);
  scrooge::FirstScrooge::RegisterFunction(con, catalog);
  scrooge::LastScrooge::RegisterFunction(con, catalog);

  con.Commit();
}

std::string ScroogeExtension::Name() { return "scrooge"; }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void scrooge_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::ScroogeExtension>();
}

DUCKDB_EXTENSION_API const char *scrooge_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}