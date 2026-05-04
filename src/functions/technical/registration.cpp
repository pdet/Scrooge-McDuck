#include "functions/technical.hpp"

namespace duckdb {
namespace scrooge {

// Forward declarations for individual function registrations
void RegisterEmaFunction(Connection &conn, Catalog &catalog);
void RegisterRsiFunction(Connection &conn, Catalog &catalog);
void RegisterMacdFunction(Connection &conn, Catalog &catalog);
void RegisterBollingerFunctions(Connection &conn, Catalog &catalog);
void RegisterVwapFunction(Connection &conn, Catalog &catalog);
void RegisterObvFunction(Connection &conn, Catalog &catalog);
void RegisterAtrFunction(Connection &conn, Catalog &catalog);
void RegisterStochasticFunction(Connection &conn, Catalog &catalog);
void RegisterMfiFunction(Connection &conn, Catalog &catalog);
void RegisterCmfFunction(Connection &conn, Catalog &catalog);
void RegisterWMAAndPivotFunctions(Connection &conn, Catalog &catalog);
void RegisterDEMAFunctions(Connection &conn, Catalog &catalog);
void RegisterWilliamsR(Connection &conn, Catalog &catalog);
void RegisterADX(Connection &conn, Catalog &catalog);
void RegisterHeikinAshi(Connection &conn, Catalog &catalog);
void RegisterIchimoku(Connection &conn, Catalog &catalog);
void RegisterKeltner(Connection &conn, Catalog &catalog);

void RegisterTechnicalFunctions(Connection &conn, Catalog &catalog) {
	RegisterEmaFunction(conn, catalog);
	RegisterRsiFunction(conn, catalog);
	RegisterMacdFunction(conn, catalog);
	RegisterBollingerFunctions(conn, catalog);
	RegisterVwapFunction(conn, catalog);
	RegisterObvFunction(conn, catalog);
	RegisterAtrFunction(conn, catalog);
	RegisterStochasticFunction(conn, catalog);
	RegisterMfiFunction(conn, catalog);
	RegisterCmfFunction(conn, catalog);
	RegisterWMAAndPivotFunctions(conn, catalog);
	RegisterDEMAFunctions(conn, catalog);
	RegisterWilliamsR(conn, catalog);
	RegisterADX(conn, catalog);
	RegisterHeikinAshi(conn, catalog);
	RegisterIchimoku(conn, catalog);
	RegisterKeltner(conn, catalog);
}

} // namespace scrooge
} // namespace duckdb
