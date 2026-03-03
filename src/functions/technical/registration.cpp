#include "functions/technical.hpp"

namespace duckdb {
namespace scrooge {

// Forward declarations for individual function registrations
void RegisterEmaFunction(Connection &conn, Catalog &catalog);
void RegisterRsiFunction(Connection &conn, Catalog &catalog);

void RegisterTechnicalFunctions(Connection &conn, Catalog &catalog) {
	RegisterEmaFunction(conn, catalog);
	RegisterRsiFunction(conn, catalog);
}

} // namespace scrooge
} // namespace duckdb
