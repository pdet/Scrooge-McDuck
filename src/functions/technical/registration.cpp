#include "functions/technical.hpp"

namespace duckdb {
namespace scrooge {

// Forward declarations for individual function registrations
void RegisterEmaFunction(Connection &conn, Catalog &catalog);
void RegisterRsiFunction(Connection &conn, Catalog &catalog);
void RegisterMacdFunction(Connection &conn, Catalog &catalog);
void RegisterBollingerFunctions(Connection &conn, Catalog &catalog);
void RegisterVwapFunction(Connection &conn, Catalog &catalog);

void RegisterTechnicalFunctions(Connection &conn, Catalog &catalog) {
	RegisterEmaFunction(conn, catalog);
	RegisterRsiFunction(conn, catalog);
	RegisterMacdFunction(conn, catalog);
	RegisterBollingerFunctions(conn, catalog);
	RegisterVwapFunction(conn, catalog);
}

} // namespace scrooge
} // namespace duckdb
