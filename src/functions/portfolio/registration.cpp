#include "functions/portfolio.hpp"

namespace duckdb {
namespace scrooge {

void RegisterCorrelationFunctions(Connection &conn, Catalog &catalog);
void RegisterMomentumFunctions(Connection &conn, Catalog &catalog);
void RegisterSignalFunctions(Connection &conn, Catalog &catalog);
void RegisterCompositeFunctions(Connection &conn, Catalog &catalog);

void RegisterPortfolioFunctions(Connection &conn, Catalog &catalog) {
	RegisterCorrelationFunctions(conn, catalog);
	RegisterMomentumFunctions(conn, catalog);
	RegisterSignalFunctions(conn, catalog);
	RegisterCompositeFunctions(conn, catalog);
}

} // namespace scrooge
} // namespace duckdb
